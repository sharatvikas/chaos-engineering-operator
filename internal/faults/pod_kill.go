package faults

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	chaosv1alpha1 "github.com/sharatvikas/chaos-engineering-operator/api/v1alpha1"
)

// Executor manages fault injection lifecycle
type Executor struct {
	client client.Client
}

// NewExecutor creates a new fault executor
func NewExecutor(c client.Client) *Executor {
	return &Executor{client: c}
}

// SelectTargets selects pods matching the experiment target spec
func SelectTargets(
	ctx context.Context,
	c client.Client,
	target chaosv1alpha1.TargetSpec,
) ([]string, error) {
	selector, err := metav1.LabelSelectorAsSelector(&target.Selector)
	if err != nil {
		return nil, fmt.Errorf("invalid label selector: %w", err)
	}

	var podList corev1.PodList
	if err := c.List(ctx, &podList,
		client.InNamespace(target.Namespace),
		client.MatchingLabelsSelector{Selector: selector},
	); err != nil {
		return nil, fmt.Errorf("failed to list pods: %w", err)
	}

	// Filter to only Running pods
	var running []corev1.Pod
	for _, pod := range podList.Items {
		if pod.Status.Phase == corev1.PodRunning {
			running = append(running, pod)
		}
	}

	if len(running) == 0 {
		return nil, fmt.Errorf("no running pods found matching selector in namespace %s", target.Namespace)
	}

	// Apply percentage / mode selection
	count := max(1, (len(running)*target.Percentage)/100)
	selected := selectPods(running, count, target.Mode)

	names := make([]string, len(selected))
	for i, p := range selected {
		names[i] = p.Name
	}
	return names, nil
}

func selectPods(pods []corev1.Pod, count int, mode string) []corev1.Pod {
	switch mode {
	case "All":
		return pods
	case "Fixed":
		if count > len(pods) {
			count = len(pods)
		}
		return pods[:count]
	default: // Random
		shuffled := make([]corev1.Pod, len(pods))
		copy(shuffled, pods)
		rand.Shuffle(len(shuffled), func(i, j int) {
			shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
		})
		if count > len(shuffled) {
			count = len(shuffled)
		}
		return shuffled[:count]
	}
}

// Start begins fault injection for the given experiment and targets
func (e *Executor) Start(
	ctx context.Context,
	exp *chaosv1alpha1.ChaosExperiment,
	targets []string,
) error {
	logger := log.FromContext(ctx)

	switch exp.Spec.Fault.Type {
	case chaosv1alpha1.FaultPodKill:
		return e.startPodKill(ctx, exp.Spec.Target.Namespace, targets, logger)
	case chaosv1alpha1.FaultCPUStress:
		return e.startCPUStress(ctx, exp, targets)
	case chaosv1alpha1.FaultMemoryStress:
		return e.startMemoryStress(ctx, exp, targets)
	default:
		return fmt.Errorf("unsupported fault type: %s", exp.Spec.Fault.Type)
	}
}

// Stop terminates any ongoing fault injection (best-effort cleanup)
func (e *Executor) Stop(ctx context.Context, exp *chaosv1alpha1.ChaosExperiment) {
	logger := log.FromContext(ctx)
	logger.Info("Stopping fault injection", "type", exp.Spec.Fault.Type)
	// For PodKill: pods self-recover via ReplicaSet controller
	// For stress faults: delete the stress pods/sidecars
	// Implementation depends on fault type
}

func (e *Executor) startPodKill(
	ctx context.Context,
	namespace string,
	targets []string,
	logger interface{ Info(string, ...any) },
) error {
	for _, podName := range targets {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      podName,
				Namespace: namespace,
			},
		}

		// Use graceful deletion with a short grace period
		gracePeriod := int64(0)
		if err := e.client.Delete(ctx, pod, &client.DeleteOptions{
			GracePeriodSeconds: &gracePeriod,
		}); err != nil {
			return fmt.Errorf("failed to kill pod %s: %w", podName, err)
		}

		logger.Info("Pod killed", "pod", podName, "namespace", namespace)
		// Small delay between kills to avoid thundering herd
		time.Sleep(500 * time.Millisecond)
	}
	return nil
}

func (e *Executor) startCPUStress(
	ctx context.Context,
	exp *chaosv1alpha1.ChaosExperiment,
	targets []string,
) error {
	params := exp.Spec.Fault.CPUStress
	if params == nil {
		return fmt.Errorf("CPUStress params required for CPUStress fault")
	}

	// Inject a stress sidecar pod for each target
	for _, target := range targets {
		stressPod := buildStressPod(
			exp.Namespace,
			fmt.Sprintf("chaos-cpu-%s", target),
			[]string{
				"stress-ng",
				"--cpu", fmt.Sprintf("%d", params.Cores),
				"--cpu-load", fmt.Sprintf("%d", params.Load),
				"--timeout", exp.Spec.Duration,
			},
			map[string]string{
				"chaos.sharatvikas.io/experiment": exp.Name,
				"chaos.sharatvikas.io/target-pod": target,
			},
		)

		if err := e.client.Create(ctx, stressPod); err != nil {
			return fmt.Errorf("failed to create CPU stress pod for %s: %w", target, err)
		}
	}
	return nil
}

func (e *Executor) startMemoryStress(
	ctx context.Context,
	exp *chaosv1alpha1.ChaosExperiment,
	targets []string,
) error {
	params := exp.Spec.Fault.MemoryStress
	if params == nil {
		return fmt.Errorf("MemoryStress params required for MemoryStress fault")
	}

	vmBytes := fmt.Sprintf("%dM", params.MemoryMB)
	for _, target := range targets {
		stressPod := buildStressPod(
			exp.Namespace,
			fmt.Sprintf("chaos-mem-%s", target),
			[]string{
				"stress-ng",
				"--vm", "1",
				"--vm-bytes", vmBytes,
				"--timeout", exp.Spec.Duration,
			},
			map[string]string{
				"chaos.sharatvikas.io/experiment": exp.Name,
				"chaos.sharatvikas.io/target-pod": target,
			},
		)

		if err := e.client.Create(ctx, stressPod); err != nil {
			return fmt.Errorf("failed to create memory stress pod for %s: %w", target, err)
		}
	}
	return nil
}

func buildStressPod(namespace, name string, args []string, labels map[string]string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    labels,
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers: []corev1.Container{
				{
					Name:    "stress",
					Image:   "alexeiled/stress-ng:latest",
					Args:    args,
					Resources: corev1.ResourceRequirements{
						Limits: corev1.ResourceList{
							corev1.ResourceCPU:    resource("2"),
							corev1.ResourceMemory: resource("2Gi"),
						},
					},
					SecurityContext: &corev1.SecurityContext{
						RunAsNonRoot: boolPtr(false), // stress-ng needs root
						Capabilities: &corev1.Capabilities{
							Add: []corev1.Capability{"SYS_RESOURCE"},
						},
					},
				},
			},
		},
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func boolPtr(b bool) *bool { return &b }

func resource(s string) resource_quantity {
	// Stub — in real code use resource.MustParse
	return resource_quantity{}
}

type resource_quantity = struct{}
