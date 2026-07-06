// Package faults implements the chaos fault-injection layer of the operator.
//
// The package is built around a small, explicit contract — the Injector
// interface — plus a Registry that maps a FaultType to the Injector that
// knows how to inject and roll back that fault. The controller talks only to
// the Executor (target selection + dispatch); the Executor talks only to the
// Registry. Adding a new fault means implementing Injector and registering it.
//
// Injectors fall into two families:
//
//   - Direct actions (PodKill, ContainerKill) mutate the target directly —
//     deleting the pod or exec'ing a signal into a container. They are
//     effectively irreversible, so their Rollback is a no-op.
//
//   - Sidecar actions (NetworkDelay, NetworkPartition, CPUStress,
//     MemoryStress, DNSFailure) create short-lived, privileged "chaos" pods
//     co-scheduled with the target. Every artifact is labelled with the owning
//     experiment so Rollback can delete exactly the pods it created — never
//     another experiment's.
package faults

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	chaosv1alpha1 "github.com/sharatvikas/chaos-engineering-operator/api/v1alpha1"
)

// Labels applied to every chaos artifact the operator creates. Rollback and
// reporting select on these, so they are part of the package's contract.
const (
	// LabelManaged marks a pod as created and owned by this operator.
	LabelManaged = "chaos.sharatvikas.io/managed"
	// LabelFaultType records which fault family created the artifact.
	LabelFaultType = "chaos.sharatvikas.io/fault-type"
	// LabelTargetPod records the pod the artifact is impairing.
	LabelTargetPod = "chaos.sharatvikas.io/target-pod"
	// LabelExperiment scopes an artifact to its owning ChaosExperiment so that
	// rollback of one experiment never disturbs another.
	LabelExperiment = "chaos.sharatvikas.io/experiment"
)

// Deps carries the shared dependencies every Injector needs. RESTConfig and
// ClientSet are only required by exec-based injectors (ContainerKill); the
// controller-runtime Client is sufficient for everything else, which keeps
// unit tests (fake client, no REST config) fully functional.
type Deps struct {
	Client     client.Client
	RESTConfig *rest.Config
	ClientSet  kubernetes.Interface
}

// InjectRequest is the input to a single injection.
type InjectRequest struct {
	Experiment *chaosv1alpha1.ChaosExperiment
	Targets    []corev1.Pod
}

// InjectResult reports what an injection actually affected.
type InjectResult struct {
	// AffectedPods are the target pods the fault was successfully applied to.
	AffectedPods []string
	// Artifacts are the names of chaos pods created to sustain the fault
	// (empty for direct-action faults like PodKill).
	Artifacts []string
}

// RollbackRequest is the input to cleaning up a fault.
type RollbackRequest struct {
	Experiment *chaosv1alpha1.ChaosExperiment
}

// Injector is the contract every fault implements.
type Injector interface {
	// Type returns the FaultType this injector handles.
	Type() chaosv1alpha1.FaultType
	// Inject applies the fault to the request's targets.
	Inject(ctx context.Context, req InjectRequest) (InjectResult, error)
	// Rollback removes any lingering effect of the fault. It must be
	// idempotent — the controller may call it more than once.
	Rollback(ctx context.Context, req RollbackRequest) error
	// Describe returns a human-readable, one-line summary of what the fault
	// would do, used by dry-run and reporting.
	Describe(exp *chaosv1alpha1.ChaosExperiment) string
}

// Registry maps fault types to their injectors.
type Registry struct {
	injectors map[chaosv1alpha1.FaultType]Injector
}

// NewRegistry builds a Registry with every built-in injector registered.
func NewRegistry(deps Deps) *Registry {
	r := &Registry{injectors: make(map[chaosv1alpha1.FaultType]Injector)}
	for _, inj := range []Injector{
		&PodKillInjector{deps: deps},
		&ContainerKillInjector{deps: deps},
		&NetworkDelayInjector{deps: deps},
		&NetworkPartitionInjector{deps: deps},
		&CPUStressInjector{deps: deps},
		&MemoryStressInjector{deps: deps},
		&DNSInjector{deps: deps},
	} {
		r.injectors[inj.Type()] = inj
	}
	return r
}

// Get returns the injector for a fault type, or an error if unregistered.
func (r *Registry) Get(t chaosv1alpha1.FaultType) (Injector, error) {
	inj, ok := r.injectors[t]
	if !ok {
		return nil, fmt.Errorf("no injector registered for fault type %q", t)
	}
	return inj, nil
}

// Types returns the registered fault types (order is not guaranteed).
func (r *Registry) Types() []chaosv1alpha1.FaultType {
	types := make([]chaosv1alpha1.FaultType, 0, len(r.injectors))
	for t := range r.injectors {
		types = append(types, t)
	}
	return types
}

// managedLabels returns the standard label set for a chaos artifact.
func managedLabels(exp *chaosv1alpha1.ChaosExperiment, faultType, targetPod string) map[string]string {
	return map[string]string{
		LabelManaged:    "true",
		LabelFaultType:  faultType,
		LabelTargetPod:  targetPod,
		LabelExperiment: exp.Name,
	}
}

// targetNamespace resolves the namespace the experiment acts in.
func targetNamespace(exp *chaosv1alpha1.ChaosExperiment) string {
	if exp.Spec.Target.Namespace != "" {
		return exp.Spec.Target.Namespace
	}
	return exp.Namespace
}

// rollbackManagedPods deletes every chaos artifact created for the given
// experiment. It is safe to call repeatedly and never touches artifacts owned
// by other experiments.
func rollbackManagedPods(ctx context.Context, c client.Client, exp *chaosv1alpha1.ChaosExperiment) error {
	logger := log.FromContext(ctx)
	ns := targetNamespace(exp)

	var podList corev1.PodList
	if err := c.List(ctx, &podList,
		client.InNamespace(ns),
		client.MatchingLabels{
			LabelManaged:    "true",
			LabelExperiment: exp.Name,
		},
	); err != nil {
		return fmt.Errorf("list managed chaos pods: %w", err)
	}

	grace := int64(0)
	var firstErr error
	for i := range podList.Items {
		p := &podList.Items[i]
		if err := c.Delete(ctx, p, &client.DeleteOptions{GracePeriodSeconds: &grace}); err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("delete chaos pod %s: %w", p.Name, err)
			}
			logger.Error(err, "failed to delete chaos artifact", "pod", p.Name)
			continue
		}
		logger.Info("rolled back chaos artifact", "pod", p.Name, "experiment", exp.Name)
	}
	return firstErr
}

// artifactHashLen is the number of hex characters of the disambiguating hash
// appended to every generated chaos-artifact name.
const artifactHashLen = 8

// maxArtifactNameLen is Kubernetes' DNS-1123 *label* limit — the tightest
// constraint on a pod name (pod names are also used as label values elsewhere,
// and must be <= 63 chars to remain valid selectors).
const maxArtifactNameLen = 63

// chaosArtifactName builds a deterministic, collision-free name for a chaos
// artifact created for (experiment, target). The result is
//
//	<prefix>-<truncated-target>-<hash>
//
// where <prefix> encodes the fault family (e.g. "chaos-cpu") and <hash> is
// derived from the experiment identity (namespace+name), the target pod, and
// the prefix. Two ChaosExperiments injecting the SAME fault into the SAME
// target therefore produce DISTINCT names (their experiment names differ), so
// their artifacts never collide on create. The name is a pure function of its
// inputs, so it is stable across reconciles — though rollback selects purely by
// label (see rollbackManagedPods) and does not depend on the name at all.
//
// The result is always a valid DNS-1123 label of at most 63 characters: the
// prefix and hash are lowercase alphanumerics/'-', pod names are already
// DNS-1123, and the trailing hash guarantees the last character is alphanumeric
// even when the target component is truncated.
func chaosArtifactName(prefix string, exp *chaosv1alpha1.ChaosExperiment, target *corev1.Pod) string {
	sum := sha1.Sum([]byte(strings.Join([]string{exp.Namespace, exp.Name, target.Name, prefix}, "/")))
	hash := hex.EncodeToString(sum[:])[:artifactHashLen]

	// Reserve room for the prefix, the hash, and the two "-" separators; the
	// remaining budget is filled by a truncated target name.
	budget := maxArtifactNameLen - len(prefix) - artifactHashLen - 2
	if budget < 0 {
		budget = 0
	}
	targetPart := target.Name
	if len(targetPart) > budget {
		targetPart = targetPart[:budget]
	}
	return fmt.Sprintf("%s-%s-%s", prefix, targetPart, hash)
}

// buildChaosPodMeta builds the ObjectMeta shared by all sidecar chaos pods.
func buildChaosPodMeta(name, namespace string, labels map[string]string) metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Name:      name,
		Namespace: namespace,
		Labels:    labels,
	}
}
