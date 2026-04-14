package faults

// pod_kill.go — PodKill, CPUStress, and MemoryStress fault methods on Executor.
// Executor struct and lifecycle (Start/Stop/SelectTargets) are in executor.go.

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	chaosv1alpha1 "github.com/sharatvikas/chaos-engineering-operator/api/v1alpha1"
)

func (e *Executor) startPodKill(ctx context.Context, targets []corev1.Pod, _ *struct{}) error {
	logger := log.FromContext(ctx)
	gracePeriod := int64(0)

	for _, pod := range targets {
		p := pod // capture
		if err := e.client.Delete(ctx, &p, &client.DeleteOptions{
			GracePeriodSeconds: &gracePeriod,
		}); err != nil {
			return fmt.Errorf("kill pod %s: %w", pod.Name, err)
		}
		logger.Info("pod killed", "pod", pod.Name, "namespace", pod.Namespace)
		time.Sleep(500 * time.Millisecond)
	}
	return nil
}

func (e *Executor) startCPUStress(ctx context.Context, targets []corev1.Pod, params *chaosv1alpha1.CPUStressParams) error {
	if params == nil {
		params = &chaosv1alpha1.CPUStressParams{Cores: 1, Load: 80}
	}

	for _, pod := range targets {
		stressPod := buildStressPod(
			pod.Namespace,
			fmt.Sprintf("chaos-cpu-%s", pod.Name[:min(8, len(pod.Name))]),
			[]string{
				"stress-ng",
				"--cpu", fmt.Sprintf("%d", params.Cores),
				"--cpu-load", fmt.Sprintf("%d", params.Load),
			},
			map[string]string{
				"chaos.sharatvikas.io/managed":    "true",
				"chaos.sharatvikas.io/fault-type": "cpu-stress",
				"chaos.sharatvikas.io/target-pod": pod.Name,
			},
		)
		stressPod.Spec.NodeName = pod.Spec.NodeName
		if err := e.client.Create(ctx, stressPod); err != nil {
			return fmt.Errorf("create CPU stress pod for %s: %w", pod.Name, err)
		}
	}
	return nil
}

func (e *Executor) startMemoryStress(ctx context.Context, targets []corev1.Pod, params *chaosv1alpha1.MemoryStressParams) error {
	if params == nil {
		params = &chaosv1alpha1.MemoryStressParams{FillPercentage: 80}
	}

	vmBytes := fmt.Sprintf("%dM", params.MemoryMB)
	if params.FillPercentage > 0 {
		vmBytes = fmt.Sprintf("%d%%", params.FillPercentage)
	}

	for _, pod := range targets {
		stressPod := buildStressPod(
			pod.Namespace,
			fmt.Sprintf("chaos-mem-%s", pod.Name[:min(8, len(pod.Name))]),
			[]string{"stress-ng", "--vm", "1", "--vm-bytes", vmBytes},
			map[string]string{
				"chaos.sharatvikas.io/managed":    "true",
				"chaos.sharatvikas.io/fault-type": "memory-stress",
				"chaos.sharatvikas.io/target-pod": pod.Name,
			},
		)
		stressPod.Spec.NodeName = pod.Spec.NodeName
		if err := e.client.Create(ctx, stressPod); err != nil {
			return fmt.Errorf("create memory stress pod for %s: %w", pod.Name, err)
		}
	}
	return nil
}

func (e *Executor) stopStressPods(ctx context.Context, namespace string) error {
	var podList corev1.PodList
	if err := e.client.List(ctx, &podList,
		client.InNamespace(namespace),
		client.MatchingLabels{"chaos.sharatvikas.io/managed": "true"},
	); err != nil {
		return err
	}

	gracePeriod := int64(0)
	for _, pod := range podList.Items {
		p := pod
		if err := e.client.Delete(ctx, &p, &client.DeleteOptions{
			GracePeriodSeconds: &gracePeriod,
		}); err != nil {
			return fmt.Errorf("delete stress pod %s: %w", pod.Name, err)
		}
	}
	return nil
}

// buildStressPod creates a stress-ng Pod spec.
func buildStressPod(namespace, name string, args []string, labels map[string]string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    labels,
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Tolerations:   []corev1.Toleration{{Operator: corev1.TolerationOpExists}},
			Containers: []corev1.Container{
				{
					Name:  "stress",
					Image: "alexeiled/stress-ng:latest",
					Args:  args,
					Resources: corev1.ResourceRequirements{
						Limits: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("2"),
							corev1.ResourceMemory: resource.MustParse("2Gi"),
						},
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("100m"),
							corev1.ResourceMemory: resource.MustParse("64Mi"),
						},
					},
				},
			},
		},
	}
}

// selectPods applies mode-based pod selection (used by SelectTargets in executor.go).
func selectPodsFromList(pods []corev1.Pod, count int, mode string) []corev1.Pod {
	switch mode {
	case "all", "All":
		return pods
	case "fixed", "Fixed":
		sorted := make([]corev1.Pod, len(pods))
		copy(sorted, pods)
		sort.Slice(sorted, func(i, j int) bool {
			return sorted[i].Name < sorted[j].Name
		})
		if count > len(sorted) {
			return sorted
		}
		return sorted[:count]
	default:
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

func boolPtr(b bool) *bool { return &b }

// Keep unused import warnings away
var _ = boolPtr
