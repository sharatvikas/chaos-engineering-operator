package faults

import (
	"context"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/controller-runtime/pkg/client"

	chaosv1alpha1 "github.com/sharatvikas/chaos-engineering-operator/api/v1alpha1"
)

// startNetworkDelay injects network latency using a tc-netem sidecar.
// The sidecar runs in the host network namespace of each target pod's node,
// targeting traffic from the pod's IP using `tc qdisc add`.
func (e *Executor) startNetworkDelay(ctx context.Context, targets []corev1.Pod, params *chaosv1alpha1.NetworkDelayParams) error {
	if params == nil {
		params = &chaosv1alpha1.NetworkDelayParams{
			LatencyMs:      200,
			JitterMs:       20,
			CorrelationPct: 25,
		}
	}

	for _, pod := range targets {
		if err := e.injectNetworkFault(ctx, pod, "delay", params); err != nil {
			return fmt.Errorf("inject network delay on %s: %w", pod.Name, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	return nil
}

// injectNetworkFault creates a privileged init+sidecar pod on the same node
// that uses `tc` (traffic control) to manipulate the target pod's network egress.
//
// Architecture:
//   - Sidecar pod runs as privileged with hostPID + hostNetwork
//   - init container: resolves target pod IP and applies tc qdisc rule
//   - main container: sleeps for experiment duration, then removes the rule
//
// Why tc/netem on the node rather than in-pod:
//   - Works for any container runtime (no need to exec into pod)
//   - Affects all traffic from that pod IP, including UDP and non-HTTP
//   - Clean removal on pod deletion via preStop hook
func (e *Executor) injectNetworkFault(
	ctx context.Context,
	target corev1.Pod,
	faultType string, // "delay"
	delayParams *chaosv1alpha1.NetworkDelayParams,
) error {
	tcCommand := buildTCCommand(target.Status.PodIP, delayParams)
	cleanupCommand := fmt.Sprintf(
		"tc qdisc del dev eth0 root 2>/dev/null; tc filter del dev eth0 2>/dev/null || true",
	)

	// Duration to hold the fault — default to 5m
	holdDuration := "300" // seconds

	sidecarName := fmt.Sprintf("chaos-net-%s-%s", faultType, target.Name[:min(8, len(target.Name))])

	privileged := true
	hostPID := true
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sidecarName,
			Namespace: target.Namespace,
			Labels: map[string]string{
				"chaos.sharatvikas.io/managed":    "true",
				"chaos.sharatvikas.io/fault-type": faultType,
				"chaos.sharatvikas.io/target-pod": target.Name,
			},
		},
		Spec: corev1.PodSpec{
			NodeName:      target.Spec.NodeName,
			HostPID:       hostPID,
			HostNetwork:   true,
			RestartPolicy: corev1.RestartPolicyNever,
			// Tolerate all taints to reach the target node
			Tolerations: []corev1.Toleration{{Operator: corev1.TolerationOpExists}},
			InitContainers: []corev1.Container{
				{
					Name:  "inject",
					Image: "ghcr.io/sharatvikas/chaos-tc:latest",
					Command: []string{"/bin/sh", "-c", strings.Join([]string{
						// Wait for pod IP to be routable
						fmt.Sprintf("until ping -c1 -W1 %s &>/dev/null; do sleep 1; done", target.Status.PodIP),
						tcCommand,
						fmt.Sprintf("echo 'Network %s injected on %s (%s)'", faultType, target.Name, target.Status.PodIP),
					}, " && ")},
					SecurityContext: &corev1.SecurityContext{
						Privileged: &privileged,
						Capabilities: &corev1.Capabilities{
							Add: []corev1.Capability{"NET_ADMIN", "SYS_ADMIN"},
						},
					},
				},
			},
			Containers: []corev1.Container{
				{
					Name:  "hold",
					Image: "ghcr.io/sharatvikas/chaos-tc:latest",
					Command: []string{"/bin/sh", "-c", fmt.Sprintf(
						"trap '%s' TERM INT; sleep %s; %s",
						cleanupCommand, holdDuration, cleanupCommand,
					)},
					SecurityContext: &corev1.SecurityContext{
						Privileged: &privileged,
						Capabilities: &corev1.Capabilities{
							Add: []corev1.Capability{"NET_ADMIN"},
						},
					},
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("10m"),
							corev1.ResourceMemory: resource.MustParse("16Mi"),
						},
					},
					Lifecycle: &corev1.Lifecycle{
						PreStop: &corev1.LifecycleHandler{
							Exec: &corev1.ExecAction{
								Command: []string{"/bin/sh", "-c", cleanupCommand},
							},
						},
					},
				},
			},
		},
	}

	return e.client.Create(ctx, pod)
}

// buildTCCommand builds the `tc` netem delay command string.
// Uses netem qdisc for realistic network impairment simulation.
func buildTCCommand(podIP string, delay *chaosv1alpha1.NetworkDelayParams) string {
	latency := 200
	jitter := 20
	correlation := 25
	if delay != nil {
		latency = delay.LatencyMs
		jitter = delay.JitterMs
		correlation = delay.CorrelationPct
	}
	// distribution normal gives more realistic latency distribution than uniform
	netemArgs := fmt.Sprintf("delay %dms %dms %d%% distribution normal", latency, jitter, correlation)

	// Apply netem only to traffic FROM the target pod IP using u32 filter:
	// 1. Add prio qdisc as root (allows multiple filters)
	// 2. Add netem as child on band 1
	// 3. Add u32 filter matching source IP → redirect to netem band
	return strings.Join([]string{
		"tc qdisc add dev eth0 root handle 1: prio priomap 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0",
		fmt.Sprintf("tc qdisc add dev eth0 parent 1:1 handle 10: netem %s", netemArgs),
		fmt.Sprintf("tc filter add dev eth0 parent 1:0 protocol ip u32 match ip src %s/32 flowid 1:1", podIP),
	}, " && ")
}

// stopNetworkFaults removes all chaos sidecar pods injecting network faults for an experiment.
func (e *Executor) stopNetworkFaults(ctx context.Context, namespace string) error {
	var podList corev1.PodList
	if err := e.client.List(ctx, &podList,
		client.InNamespace(namespace),
		client.MatchingLabels{"chaos.sharatvikas.io/managed": "true"},
	); err != nil {
		return err
	}

	gracePeriod := int64(0)
	for _, pod := range podList.Items {
		if err := e.client.Delete(ctx, &pod, &client.DeleteOptions{
			GracePeriodSeconds: &gracePeriod,
		}); err != nil {
			return fmt.Errorf("delete chaos pod %s: %w", pod.Name, err)
		}
	}
	return nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
