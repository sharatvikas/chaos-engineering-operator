package faults

import (
	"context"
	"fmt"
	"math/rand"
	"sort"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	chaosv1alpha1 "github.com/sharatvikas/chaos-engineering-operator/api/v1alpha1"
)

// Executor selects target pods and dispatches fault injection by type.
type Executor struct {
	client client.Client
}

// NewExecutor creates an Executor backed by the given controller-runtime client.
func NewExecutor(c client.Client) *Executor {
	return &Executor{client: c}
}

// Start selects target pods and injects the configured fault.
// Returns the number of pods targeted.
func (e *Executor) Start(ctx context.Context, exp *chaosv1alpha1.ChaosExperiment) (int, error) {
	targets, err := e.SelectTargets(ctx, exp)
	if err != nil {
		return 0, fmt.Errorf("select targets: %w", err)
	}
	if len(targets) == 0 {
		return 0, fmt.Errorf("no pods matched label selector %v in namespace %s",
			exp.Spec.Target.Selector.MatchLabels, exp.Spec.Target.Namespace)
	}

	switch exp.Spec.Fault.Type {
	case "PodKill":
		return len(targets), e.startPodKill(ctx, targets, nil)
	case "CPUStress":
		return len(targets), e.startCPUStress(ctx, targets, exp.Spec.Fault.CPUStress)
	case "MemoryStress":
		return len(targets), e.startMemoryStress(ctx, targets, exp.Spec.Fault.MemoryStress)
	case "NetworkDelay":
		return len(targets), e.startNetworkDelay(ctx, targets, exp.Spec.Fault.NetworkDelay)
	case "DNSFailure":
		return len(targets), e.startDNSFailure(ctx, targets)
	default:
		return 0, fmt.Errorf("unknown fault type: %s", exp.Spec.Fault.Type)
	}
}

// Stop cleans up all fault artifacts created by this experiment.
func (e *Executor) Stop(ctx context.Context, exp *chaosv1alpha1.ChaosExperiment) error {
	ns := exp.Spec.Target.Namespace
	if ns == "" {
		ns = exp.Namespace
	}

	switch exp.Spec.Fault.Type {
	case "NetworkDelay", "NetworkLoss", "DNSFailure":
		// All network-family faults use managed sidecar pods — clean them up
		return e.stopNetworkFaults(ctx, ns)
	case "CPUStress", "MemoryStress":
		return e.stopStressPods(ctx, ns)
	case "PodKill":
		// Pod kills are irreversible — nothing to clean up
		return nil
	}
	return nil
}

// SelectTargets lists Running pods matching the experiment's label selector
// and applies the percentage/mode selection strategy.
func (e *Executor) SelectTargets(ctx context.Context, exp *chaosv1alpha1.ChaosExperiment) ([]corev1.Pod, error) {
	ns := exp.Spec.Target.Namespace
	if ns == "" {
		ns = exp.Namespace
	}

	var podList corev1.PodList
	if err := e.client.List(ctx, &podList,
		client.InNamespace(ns),
		client.MatchingLabels(exp.Spec.Target.Selector.MatchLabels),
	); err != nil {
		return nil, fmt.Errorf("list pods: %w", err)
	}

	// Only Running pods are valid targets
	var running []corev1.Pod
	for _, pod := range podList.Items {
		if pod.Status.Phase == corev1.PodRunning && pod.DeletionTimestamp == nil {
			running = append(running, pod)
		}
	}

	if len(running) == 0 {
		return nil, nil
	}

	return e.applySelectionStrategy(running, exp.Spec.Target), nil
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

	switch target.Mode {
	case "all":
		return pods

	case "fixed":
		// Fixed: always pick the same pods (sorted by name for determinism)
		sorted := make([]corev1.Pod, len(pods))
		copy(sorted, pods)
		sort.Slice(sorted, func(i, j int) bool {
			return sorted[i].Name < sorted[j].Name
		})
		if count > len(sorted) {
			return sorted
		}
		return sorted[:count]

	default: // "random"
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

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
