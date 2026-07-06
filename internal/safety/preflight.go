package safety

// preflight.go — static, cluster-cheap safety guards evaluated before any
// fault is injected, complementing the Prometheus SLO gates in gates.go.
//
// Prometheus gates answer "is the service healthy enough right now?"; these
// preflight guards answer "is this experiment allowed to run at all?" — a
// protected namespace, or a blast radius that would leave too few replicas
// standing. They require no external calls, so they never fail open.

import (
	"fmt"
	"strings"
)

// PreflightChecker evaluates static admission-style guards for an experiment.
type PreflightChecker struct {
	// NamespaceDenylist is the set of namespaces experiments may never target.
	NamespaceDenylist []string
}

// DefaultNamespaceDenylist returns the control-plane and system namespaces
// that should never be chaos-tested by default.
func DefaultNamespaceDenylist() []string {
	return []string{
		"kube-system",
		"kube-public",
		"kube-node-lease",
		"chaos-system",
	}
}

// CheckNamespace reports whether the target namespace is permitted.
// Returns (allowed, reason). When allowed is false, reason explains why.
func (p PreflightChecker) CheckNamespace(namespace string) (bool, string) {
	ns := strings.TrimSpace(namespace)
	if ns == "" {
		return false, "target namespace is empty"
	}
	for _, denied := range p.NamespaceDenylist {
		if strings.EqualFold(ns, strings.TrimSpace(denied)) {
			return false, fmt.Sprintf("namespace %q is on the chaos denylist (protected)", ns)
		}
	}
	return true, ""
}

// CheckMinHealthyReplicas reports whether injecting the fault would still
// leave at least minHealthy running, unaffected pods. minHealthy <= 0
// disables the guard. Returns (allowed, reason).
func (p PreflightChecker) CheckMinHealthyReplicas(totalRunning, affected, minHealthy int) (bool, string) {
	if minHealthy <= 0 {
		return true, ""
	}
	remaining := totalRunning - affected
	if remaining < minHealthy {
		return false, fmt.Sprintf(
			"min-healthy-replicas guard: %d running, fault would affect %d, leaving %d < required %d",
			totalRunning, affected, remaining, minHealthy,
		)
	}
	return true, ""
}
