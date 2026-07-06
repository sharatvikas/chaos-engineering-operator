package faults

// network.go — network-family faults implemented with privileged "chaos"
// sidecar pods co-scheduled with each target on the same node.
//
//   - NetworkDelayInjector uses tc/netem to add latency + jitter to the
//     target pod's egress, matched on the pod's source IP.
//   - NetworkPartitionInjector uses iptables to DROP traffic to/from the
//     target pod, either wholesale (full isolation) or restricted to a set
//     of peer CIDRs (e.g. partition from the database subnet only).
//
// Running in the node's host network namespace (hostNetwork + NET_ADMIN)
// works for any CNI/runtime without exec'ing into the target, and cleans up
// via a preStop hook plus operator-driven Rollback.

import (
	"context"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/controller-runtime/pkg/log"

	chaosv1alpha1 "github.com/sharatvikas/chaos-engineering-operator/api/v1alpha1"
)

const netToolsImage = "ghcr.io/sharatvikas/chaos-tc:latest"

// ─── NetworkDelay ───────────────────────────────────────────────────────────

// NetworkDelayInjector implements Injector for the NetworkDelay fault.
type NetworkDelayInjector struct {
	deps Deps
}

func (i *NetworkDelayInjector) Type() chaosv1alpha1.FaultType {
	return chaosv1alpha1.FaultNetworkDelay
}

func (i *NetworkDelayInjector) Inject(ctx context.Context, req InjectRequest) (InjectResult, error) {
	logger := log.FromContext(ctx)
	params := req.Experiment.Spec.Fault.NetworkDelay
	if params == nil {
		params = &chaosv1alpha1.NetworkDelayParams{LatencyMs: 200, JitterMs: 20, CorrelationPct: 25}
	}

	holdSec := durationSeconds(req.Experiment.Spec.Duration)
	result := InjectResult{}
	for idx := range req.Targets {
		target := &req.Targets[idx]
		if target.Status.PodIP == "" {
			return result, fmt.Errorf("pod %s has no IP — not yet running", target.Name)
		}
		sidecar := buildNetemSidecar(req.Experiment, target, params, holdSec)
		if err := i.deps.Client.Create(ctx, sidecar); err != nil {
			return result, fmt.Errorf("inject network delay on %s: %w", target.Name, err)
		}
		result.AffectedPods = append(result.AffectedPods, target.Name)
		result.Artifacts = append(result.Artifacts, sidecar.Name)
		logger.Info("network delay injected", "target", target.Name, "latencyMs", params.LatencyMs)
		time.Sleep(200 * time.Millisecond)
	}
	return result, nil
}

func (i *NetworkDelayInjector) Rollback(ctx context.Context, req RollbackRequest) error {
	return rollbackManagedPods(ctx, i.deps.Client, req.Experiment)
}

func (i *NetworkDelayInjector) Describe(exp *chaosv1alpha1.ChaosExperiment) string {
	p := exp.Spec.Fault.NetworkDelay
	if p == nil {
		return "Add default network latency (200ms±20ms) to targeted pods' egress."
	}
	return fmt.Sprintf("Add %dms±%dms latency to egress of %d%% of matching pods (tc/netem).",
		p.LatencyMs, p.JitterMs, blastRadius(exp))
}

// buildNetemSidecar builds the privileged tc/netem sidecar for one target.
func buildNetemSidecar(exp *chaosv1alpha1.ChaosExperiment, target *corev1.Pod, params *chaosv1alpha1.NetworkDelayParams, holdSec int) *corev1.Pod {
	injectCmd := buildTCCommand(target.Status.PodIP, params)
	cleanupCmd := "tc qdisc del dev eth0 root 2>/dev/null; tc filter del dev eth0 2>/dev/null || true"
	name := fmt.Sprintf("chaos-net-delay-%s", shortName(target.Name))

	return buildPrivilegedNodeSidecar(privilegedSidecarSpec{
		exp:        exp,
		target:     target,
		name:       name,
		faultType:  "network-delay",
		injectCmd:  fmt.Sprintf("until ping -c1 -W1 %s >/dev/null 2>&1; do sleep 1; done && %s", target.Status.PodIP, injectCmd),
		cleanupCmd: cleanupCmd,
		holdSec:    holdSec,
		caps:       []corev1.Capability{"NET_ADMIN", "SYS_ADMIN"},
	})
}

// buildTCCommand builds the tc netem delay command targeting the pod's source IP.
func buildTCCommand(podIP string, delay *chaosv1alpha1.NetworkDelayParams) string {
	latency, jitter, correlation := 200, 20, 25
	if delay != nil {
		latency, jitter, correlation = delay.LatencyMs, delay.JitterMs, delay.CorrelationPct
	}
	netemArgs := fmt.Sprintf("delay %dms %dms %d%% distribution normal", latency, jitter, correlation)
	return strings.Join([]string{
		"tc qdisc add dev eth0 root handle 1: prio priomap 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0",
		fmt.Sprintf("tc qdisc add dev eth0 parent 1:1 handle 10: netem %s", netemArgs),
		fmt.Sprintf("tc filter add dev eth0 parent 1:0 protocol ip u32 match ip src %s/32 flowid 1:1", podIP),
	}, " && ")
}

// ─── NetworkPartition ───────────────────────────────────────────────────────

// NetworkPartitionInjector implements Injector for the NetworkPartition fault.
type NetworkPartitionInjector struct {
	deps Deps
}

func (i *NetworkPartitionInjector) Type() chaosv1alpha1.FaultType {
	return chaosv1alpha1.FaultNetworkPartition
}

func (i *NetworkPartitionInjector) Inject(ctx context.Context, req InjectRequest) (InjectResult, error) {
	logger := log.FromContext(ctx)
	params := req.Experiment.Spec.Fault.NetworkPartition
	if params == nil {
		params = &chaosv1alpha1.NetworkPartitionParams{Direction: "Both"}
	}

	holdSec := durationSeconds(req.Experiment.Spec.Duration)
	result := InjectResult{}
	for idx := range req.Targets {
		target := &req.Targets[idx]
		if target.Status.PodIP == "" {
			return result, fmt.Errorf("pod %s has no IP — not yet running", target.Name)
		}
		sidecar := buildPartitionSidecar(req.Experiment, target, params, holdSec)
		if err := i.deps.Client.Create(ctx, sidecar); err != nil {
			return result, fmt.Errorf("inject network partition on %s: %w", target.Name, err)
		}
		result.AffectedPods = append(result.AffectedPods, target.Name)
		result.Artifacts = append(result.Artifacts, sidecar.Name)
		logger.Info("network partition injected", "target", target.Name, "direction", params.Direction, "peers", params.PeerCIDRs)
		time.Sleep(200 * time.Millisecond)
	}
	return result, nil
}

func (i *NetworkPartitionInjector) Rollback(ctx context.Context, req RollbackRequest) error {
	return rollbackManagedPods(ctx, i.deps.Client, req.Experiment)
}

func (i *NetworkPartitionInjector) Describe(exp *chaosv1alpha1.ChaosExperiment) string {
	p := exp.Spec.Fault.NetworkPartition
	direction := "Both"
	scope := "all peers"
	if p != nil {
		if p.Direction != "" {
			direction = p.Direction
		}
		if len(p.PeerCIDRs) > 0 {
			scope = strings.Join(p.PeerCIDRs, ",")
		}
	}
	return fmt.Sprintf("DROP %s traffic between %d%% of matching pods and %s (iptables).",
		direction, blastRadius(exp), scope)
}

// buildPartitionSidecar builds the iptables partition sidecar for one target.
func buildPartitionSidecar(exp *chaosv1alpha1.ChaosExperiment, target *corev1.Pod, params *chaosv1alpha1.NetworkPartitionParams, holdSec int) *corev1.Pod {
	inject, cleanup := buildPartitionRules(target.Status.PodIP, params)
	name := fmt.Sprintf("chaos-net-part-%s", shortName(target.Name))

	return buildPrivilegedNodeSidecar(privilegedSidecarSpec{
		exp:        exp,
		target:     target,
		name:       name,
		faultType:  "network-partition",
		injectCmd:  inject,
		cleanupCmd: cleanup,
		holdSec:    holdSec,
		caps:       []corev1.Capability{"NET_ADMIN", "NET_RAW"},
	})
}

// buildPartitionRules returns the (inject, cleanup) iptables command strings
// for a partition. Egress rules match on source IP (FORWARD -s), ingress on
// destination IP (FORWARD -d); direction and optional peer CIDRs scope them.
func buildPartitionRules(podIP string, params *chaosv1alpha1.NetworkPartitionParams) (string, string) {
	direction := "Both"
	if params != nil && params.Direction != "" {
		direction = params.Direction
	}
	var peers []string
	if params != nil {
		peers = params.PeerCIDRs
	}

	egress := direction == "Egress" || direction == "Both"
	ingress := direction == "Ingress" || direction == "Both"

	var inject, cleanup []string
	addRule := func(chain, flag, peerFlag, peer string) {
		peerClause := ""
		if peer != "" {
			peerClause = fmt.Sprintf(" %s %s", peerFlag, peer)
		}
		inject = append(inject, fmt.Sprintf("iptables -I %s %s %s%s -j DROP", chain, flag, podIP, peerClause))
		cleanup = append(cleanup, fmt.Sprintf("iptables -D %s %s %s%s -j DROP 2>/dev/null || true", chain, flag, podIP, peerClause))
	}

	// When no peers are specified we partition against everything (peer "").
	targetPeers := peers
	if len(targetPeers) == 0 {
		targetPeers = []string{""}
	}
	for _, peer := range targetPeers {
		if egress {
			addRule("FORWARD", "-s", "-d", peer)
		}
		if ingress {
			addRule("FORWARD", "-d", "-s", peer)
		}
	}

	inject = append(inject, fmt.Sprintf("echo 'Network partition (%s) applied for %s'", direction, podIP))
	cleanup = append(cleanup, fmt.Sprintf("echo 'Network partition removed for %s'", podIP))
	return strings.Join(inject, " && "), strings.Join(cleanup, " && ")
}

// ─── shared sidecar builder ─────────────────────────────────────────────────

type privilegedSidecarSpec struct {
	exp        *chaosv1alpha1.ChaosExperiment
	target     *corev1.Pod
	name       string
	faultType  string
	injectCmd  string
	cleanupCmd string
	holdSec    int
	caps       []corev1.Capability
}

// buildPrivilegedNodeSidecar constructs a hostNetwork/privileged pod pinned to
// the target's node that injects a fault via an init container and holds it
// (with trap-based cleanup) for holdSec seconds in the main container.
func buildPrivilegedNodeSidecar(s privilegedSidecarSpec) *corev1.Pod {
	privileged := true
	return &corev1.Pod{
		ObjectMeta: buildChaosPodMeta(s.name, s.target.Namespace, managedLabels(s.exp, s.faultType, s.target.Name)),
		Spec: corev1.PodSpec{
			NodeName:      s.target.Spec.NodeName,
			HostPID:       true,
			HostNetwork:   true,
			RestartPolicy: corev1.RestartPolicyNever,
			Tolerations:   []corev1.Toleration{{Operator: corev1.TolerationOpExists}},
			InitContainers: []corev1.Container{
				{
					Name:            "inject",
					Image:           netToolsImage,
					Command:         []string{"/bin/sh", "-c", s.injectCmd},
					SecurityContext: privilegedSecurityContext(privileged, s.caps),
				},
			},
			Containers: []corev1.Container{
				{
					Name:  "hold",
					Image: netToolsImage,
					Command: []string{"/bin/sh", "-c", fmt.Sprintf(
						"trap '%s' TERM INT; echo 'holding %s for %ds'; sleep %d; %s",
						s.cleanupCmd, s.faultType, s.holdSec, s.holdSec, s.cleanupCmd,
					)},
					SecurityContext: privilegedSecurityContext(privileged, s.caps),
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("10m"),
							corev1.ResourceMemory: resource.MustParse("16Mi"),
						},
					},
					Lifecycle: &corev1.Lifecycle{
						PreStop: &corev1.LifecycleHandler{
							Exec: &corev1.ExecAction{
								Command: []string{"/bin/sh", "-c", s.cleanupCmd},
							},
						},
					},
				},
			},
		},
	}
}

func privilegedSecurityContext(privileged bool, caps []corev1.Capability) *corev1.SecurityContext {
	return &corev1.SecurityContext{
		Privileged:   &privileged,
		Capabilities: &corev1.Capabilities{Add: caps},
	}
}

// durationSeconds parses a Duration string into whole seconds, defaulting to
// 300s (5m) when unparseable — the sidecar hold time must never be zero.
func durationSeconds(d string) int {
	dur, err := time.ParseDuration(d)
	if err != nil || dur <= 0 {
		return 300
	}
	return int(dur.Seconds())
}
