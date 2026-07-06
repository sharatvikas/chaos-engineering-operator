package faults

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	chaosv1alpha1 "github.com/sharatvikas/chaos-engineering-operator/api/v1alpha1"
)

// Executor selects target pods and dispatches fault injection through the
// Registry. It is the single entry point the controller uses.
type Executor struct {
	client   client.Client
	registry *Registry
}

// NewExecutor creates an Executor backed by the given controller-runtime
// client. Exec-based faults (ContainerKill) require additional dependencies —
// use NewExecutorWithDeps for those.
func NewExecutor(c client.Client) *Executor {
	return NewExecutorWithDeps(Deps{Client: c})
}

// NewExecutorWithDeps creates an Executor with full dependencies, enabling
// exec-based fault injectors.
func NewExecutorWithDeps(deps Deps) *Executor {
	return &Executor{
		client:   deps.Client,
		registry: NewRegistry(deps),
	}
}

// Registry exposes the underlying fault registry (used for introspection).
func (e *Executor) Registry() *Registry { return e.registry }

// Start selects target pods and injects the configured fault.
// Returns the number of pods the fault was applied to.
func (e *Executor) Start(ctx context.Context, exp *chaosv1alpha1.ChaosExperiment) (int, error) {
	injector, err := e.registry.Get(exp.Spec.Fault.Type)
	if err != nil {
		return 0, err
	}

	targets, err := e.SelectTargets(ctx, exp)
	if err != nil {
		return 0, fmt.Errorf("select targets: %w", err)
	}
	if len(targets) == 0 {
		return 0, fmt.Errorf("no pods matched label selector %v in namespace %s",
			exp.Spec.Target.Selector.MatchLabels, targetNamespace(exp))
	}

	result, err := injector.Inject(ctx, InjectRequest{Experiment: exp, Targets: targets})
	if err != nil {
		return 0, fmt.Errorf("%s injection failed: %w", exp.Spec.Fault.Type, err)
	}

	log.FromContext(ctx).Info("fault injected",
		"type", exp.Spec.Fault.Type,
		"affected", len(result.AffectedPods),
		"artifacts", len(result.Artifacts),
	)
	return len(result.AffectedPods), nil
}

// Stop rolls back the fault, removing any artifacts created for the experiment.
func (e *Executor) Stop(ctx context.Context, exp *chaosv1alpha1.ChaosExperiment) error {
	injector, err := e.registry.Get(exp.Spec.Fault.Type)
	if err != nil {
		return err
	}
	return injector.Rollback(ctx, RollbackRequest{Experiment: exp})
}

// SelectTargets lists Running pods matching the experiment's label selector
// and applies the percentage/mode selection strategy.
func (e *Executor) SelectTargets(ctx context.Context, exp *chaosv1alpha1.ChaosExperiment) ([]corev1.Pod, error) {
	targets, _, err := e.SelectTargetsWithTotal(ctx, exp)
	return targets, err
}

// SelectTargetsWithTotal returns both the selected targets and the total
// number of running candidate pods. The total is required by the
// min-healthy-replicas safety guard, which reasons about how many pods would
// be left unaffected.
func (e *Executor) SelectTargetsWithTotal(ctx context.Context, exp *chaosv1alpha1.ChaosExperiment) ([]corev1.Pod, int, error) {
	ns := targetNamespace(exp)

	var podList corev1.PodList
	if err := e.client.List(ctx, &podList,
		client.InNamespace(ns),
		client.MatchingLabels(exp.Spec.Target.Selector.MatchLabels),
	); err != nil {
		return nil, 0, fmt.Errorf("list pods: %w", err)
	}

	// Only Running pods that are not already terminating are valid targets.
	// Chaos artifacts created by the operator are explicitly excluded so an
	// experiment never targets its own sidecars.
	var running []corev1.Pod
	for _, pod := range podList.Items {
		if pod.Labels[LabelManaged] == "true" {
			continue
		}
		if pod.Status.Phase == corev1.PodRunning && pod.DeletionTimestamp == nil {
			running = append(running, pod)
		}
	}

	if len(running) == 0 {
		return nil, 0, nil
	}

	return e.applySelectionStrategy(running, exp.Spec.Target), len(running), nil
}

// applySelectionStrategy applies percentage + mode to candidate pods.
func (e *Executor) applySelectionStrategy(pods []corev1.Pod, target chaosv1alpha1.TargetSpec) []corev1.Pod {
	pct := target.Percentage
	if pct <= 0 {
		pct = 25
	}
	if pct > 100 {
		pct = 100
	}

	count := max(1, len(pods)*pct/100)

	// Mode is case-insensitive: the CRD enum uses Random/All/Fixed but earlier
	// samples used lowercase, so we normalise to avoid silently defaulting.
	switch strings.ToLower(target.Mode) {
	case "all":
		return pods

	case "fixed":
		// Fixed: always pick the same pods (sorted by name for determinism).
		sorted := make([]corev1.Pod, len(pods))
		copy(sorted, pods)
		sort.Slice(sorted, func(i, j int) bool {
			return sorted[i].Name < sorted[j].Name
		})
		if count > len(sorted) {
			return sorted
		}
		return sorted[:count]

	default: // "random" (and empty)
		shuffled := make([]corev1.Pod, len(pods))
		copy(shuffled, pods)
		rand.Shuffle(len(shuffled), func(i, j int) {
			shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
		})
		if count > len(shuffled) {
			return shuffled
		}
		return shuffled[:count]
	}
}
