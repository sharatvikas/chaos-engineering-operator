package faults

// DNSFailure fault — simulates DNS resolution failures for target pods.
//
// Implementation strategy:
//   Uses iptables on the target node to DROP UDP/TCP packets destined for
//   port 53 originating from the target pod's IP address. This forces DNS
//   resolution to fail (SERVFAIL / timeout) for any lookup made by the pod.
//
//   A privileged "chaos-dns" sidecar pod runs on the same node as the target
//   with NET_ADMIN capability to manage iptables rules. The rule is inserted
//   into the FORWARD chain (for pod-to-pod traffic) and OUTPUT chain
//   (for traffic from node processes if needed).
//
//   Cleanup: Rules are removed via preStop hook and trap(TERM).
//   If the sidecar is force-killed, rules are removed by the operator's
//   Stop() call which deletes all pods labeled chaos.sharatvikas.io/managed=true.
//
// Compared to NetworkDelay (tc-netem), iptables DROP is preferred for DNS
// because tc doesn't reliably intercept UDP traffic to a specific port
// without kernel module support (xt_u32 may not be available in all images).

import (
	"context"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// startDNSFailure injects DNS failure for all target pods.
func (e *Executor) startDNSFailure(ctx context.Context, targets []corev1.Pod) error {
	for _, pod := range targets {
		if err := e.injectDNSFailure(ctx, pod); err != nil {
			return fmt.Errorf("inject DNS failure on %s: %w", pod.Name, err)
		}
		// Small delay between injections to avoid overwhelming node with iptables ops
		time.Sleep(100 * time.Millisecond)
	}
	return nil
}

// injectDNSFailure creates a privileged sidecar pod that blocks port 53
// traffic from the target pod's IP using iptables.
func (e *Executor) injectDNSFailure(ctx context.Context, target corev1.Pod) error {
	if target.Status.PodIP == "" {
		return fmt.Errorf("pod %s has no IP — not yet running", target.Name)
	}

	podIP := target.Status.PodIP
	sidecarName := fmt.Sprintf("chaos-dns-%s", target.Name[:min(16, len(target.Name))])

	// iptables rules to block DNS from this pod IP
	// DROP both UDP and TCP port 53 outbound traffic from the pod
	injectRules := strings.Join([]string{
		// Block UDP DNS (standard)
		fmt.Sprintf("iptables -I FORWARD -s %s -p udp --dport 53 -j DROP", podIP),
		// Block TCP DNS (for large responses that fall back to TCP)
		fmt.Sprintf("iptables -I FORWARD -s %s -p tcp --dport 53 -j DROP", podIP),
		// Also block DNS queries routed via OUTPUT (for cases using node's DNS)
		fmt.Sprintf("iptables -I OUTPUT -s %s -p udp --dport 53 -j DROP", podIP),
		fmt.Sprintf("iptables -I OUTPUT -s %s -p tcp --dport 53 -j DROP", podIP),
		fmt.Sprintf("echo 'DNS failure injected for pod %s (%s)'", target.Name, podIP),
	}, " && ")

	// Cleanup removes the four rules in reverse order
	cleanupRules := strings.Join([]string{
		fmt.Sprintf("iptables -D OUTPUT -s %s -p tcp --dport 53 -j DROP 2>/dev/null || true", podIP),
		fmt.Sprintf("iptables -D OUTPUT -s %s -p udp --dport 53 -j DROP 2>/dev/null || true", podIP),
		fmt.Sprintf("iptables -D FORWARD -s %s -p tcp --dport 53 -j DROP 2>/dev/null || true", podIP),
		fmt.Sprintf("iptables -D FORWARD -s %s -p udp --dport 53 -j DROP 2>/dev/null || true", podIP),
		fmt.Sprintf("echo 'DNS failure removed for %s'", target.Name),
	}, " && ")

	privileged := true
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sidecarName,
			Namespace: target.Namespace,
			Labels: map[string]string{
				"chaos.sharatvikas.io/managed":    "true",
				"chaos.sharatvikas.io/fault-type": "dns-failure",
				"chaos.sharatvikas.io/target-pod": target.Name,
			},
			// Owner reference cleanup handled by operator Stop() — not by GC,
			// since the sidecar is in a different "ownership" from the CRD.
		},
		Spec: corev1.PodSpec{
			NodeName:      target.Spec.NodeName,
			HostNetwork:   true,
			HostPID:       true,
			RestartPolicy: corev1.RestartPolicyNever,
			// Tolerate all taints (node may have chaos/experiment taints)
			Tolerations: []corev1.Toleration{
				{Operator: corev1.TolerationOpExists},
			},
			InitContainers: []corev1.Container{
				{
					Name:  "inject-dns-block",
					Image: "ghcr.io/sharatvikas/chaos-iptables:latest",
					Command: []string{"/bin/sh", "-c", injectRules},
					SecurityContext: &corev1.SecurityContext{
						Privileged: &privileged,
						Capabilities: &corev1.Capabilities{
							Add: []corev1.Capability{"NET_ADMIN", "NET_RAW"},
						},
					},
				},
			},
			Containers: []corev1.Container{
				{
					Name:  "hold",
					Image: "ghcr.io/sharatvikas/chaos-iptables:latest",
					// Hold the fault; trap SIGTERM/SIGINT for graceful cleanup
					Command: []string{"/bin/sh", "-c", fmt.Sprintf(
						"trap '%s' TERM INT; echo 'Holding DNS failure for %s'; while true; do sleep 30; done",
						cleanupRules,
						target.Name,
					)},
					SecurityContext: &corev1.SecurityContext{
						Privileged: &privileged,
						Capabilities: &corev1.Capabilities{
							Add: []corev1.Capability{"NET_ADMIN", "NET_RAW"},
						},
					},
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("5m"),
							corev1.ResourceMemory: resource.MustParse("8Mi"),
						},
					},
					Lifecycle: &corev1.Lifecycle{
						PreStop: &corev1.LifecycleHandler{
							Exec: &corev1.ExecAction{
								Command: []string{"/bin/sh", "-c", cleanupRules},
							},
						},
					},
				},
			},
		},
	}

	return e.client.Create(ctx, pod)
}
