package faults

// dns.go — DNSInjector simulates DNS resolution failures for target pods by
// DROPping port-53 traffic (UDP + TCP, FORWARD + OUTPUT chains) originating
// from the target pod's IP, via a privileged iptables sidecar on the node.
//
// iptables DROP is used rather than tc/netem because tc cannot reliably match
// UDP to a specific port without kernel modules that may be absent from node
// images. Rollback deletes the sidecar (scoped to the owning experiment).

import (
	"context"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"

	chaosv1alpha1 "github.com/sharatvikas/chaos-engineering-operator/api/v1alpha1"
)

const iptablesImage = "ghcr.io/sharatvikas/chaos-iptables:latest"

// DNSInjector implements Injector for the DNSFailure fault.
type DNSInjector struct {
	deps Deps
}

func (i *DNSInjector) Type() chaosv1alpha1.FaultType { return chaosv1alpha1.FaultDNSFailure }

func (i *DNSInjector) Inject(ctx context.Context, req InjectRequest) (InjectResult, error) {
	logger := log.FromContext(ctx)
	holdSec := durationSeconds(req.Experiment.Spec.Duration)

	result := InjectResult{}
	for idx := range req.Targets {
		target := &req.Targets[idx]
		if target.Status.PodIP == "" {
			return result, fmt.Errorf("pod %s has no IP — not yet running", target.Name)
		}
		sidecar := buildDNSSidecar(req.Experiment, target, holdSec)
		if err := i.deps.Client.Create(ctx, sidecar); err != nil {
			return result, fmt.Errorf("inject DNS failure on %s: %w", target.Name, err)
		}
		result.AffectedPods = append(result.AffectedPods, target.Name)
		result.Artifacts = append(result.Artifacts, sidecar.Name)
		logger.Info("dns failure injected", "target", target.Name)
		time.Sleep(100 * time.Millisecond)
	}
	return result, nil
}

func (i *DNSInjector) Rollback(ctx context.Context, req RollbackRequest) error {
	return rollbackManagedPods(ctx, i.deps.Client, req.Experiment)
}

func (i *DNSInjector) Describe(exp *chaosv1alpha1.ChaosExperiment) string {
	return fmt.Sprintf("DROP all port-53 (DNS) traffic from %d%% of matching pods; service discovery fails.",
		blastRadius(exp))
}

// buildDNSSidecar builds the iptables DNS-block sidecar for one target.
func buildDNSSidecar(exp *chaosv1alpha1.ChaosExperiment, target *corev1.Pod, holdSec int) *corev1.Pod {
	podIP := target.Status.PodIP

	inject := strings.Join([]string{
		fmt.Sprintf("iptables -I FORWARD -s %s -p udp --dport 53 -j DROP", podIP),
		fmt.Sprintf("iptables -I FORWARD -s %s -p tcp --dport 53 -j DROP", podIP),
		fmt.Sprintf("iptables -I OUTPUT -s %s -p udp --dport 53 -j DROP", podIP),
		fmt.Sprintf("iptables -I OUTPUT -s %s -p tcp --dport 53 -j DROP", podIP),
		fmt.Sprintf("echo 'DNS failure injected for %s'", podIP),
	}, " && ")

	cleanup := strings.Join([]string{
		fmt.Sprintf("iptables -D OUTPUT -s %s -p tcp --dport 53 -j DROP 2>/dev/null || true", podIP),
		fmt.Sprintf("iptables -D OUTPUT -s %s -p udp --dport 53 -j DROP 2>/dev/null || true", podIP),
		fmt.Sprintf("iptables -D FORWARD -s %s -p tcp --dport 53 -j DROP 2>/dev/null || true", podIP),
		fmt.Sprintf("iptables -D FORWARD -s %s -p udp --dport 53 -j DROP 2>/dev/null || true", podIP),
		fmt.Sprintf("echo 'DNS failure removed for %s'", podIP),
	}, " && ")

	sidecar := buildPrivilegedNodeSidecar(privilegedSidecarSpec{
		exp:        exp,
		target:     target,
		name:       fmt.Sprintf("chaos-dns-%s", shortName(target.Name)),
		faultType:  "dns-failure",
		injectCmd:  inject,
		cleanupCmd: cleanup,
		holdSec:    holdSec,
		caps:       []corev1.Capability{"NET_ADMIN", "NET_RAW"},
	})
	// DNS block uses the iptables tooling image rather than the tc image.
	sidecar.Spec.InitContainers[0].Image = iptablesImage
	sidecar.Spec.Containers[0].Image = iptablesImage
	return sidecar
}
