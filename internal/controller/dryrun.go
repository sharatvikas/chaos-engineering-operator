// Package controller provides dry-run simulation for ChaosExperiment.
//
// When spec.dryRun is set to true the operator:
//  1. Runs all preflight safety gate checks (real Prometheus queries)
//  2. Resolves target pods using the same selection logic as a live run
//  3. Logs what WOULD have happened without touching any resources
//  4. Writes a DryRunReport to the ChaosExperiment status
//  5. Sets Phase=Completed instead of injecting real faults
//
// This lets teams validate experiment configs, blast radius, and safety
// gate thresholds against real cluster state before running live.
package controller

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"

	chaosv1alpha1 "github.com/sharatvikas/chaos-engineering-operator/api/v1alpha1"
	"github.com/sharatvikas/chaos-engineering-operator/internal/faults"
	"github.com/sharatvikas/chaos-engineering-operator/internal/safety"
)

// DryRunReport is the result of a dry-run simulation.
type DryRunReport struct {
	SimulatedAt      time.Time         `json:"simulated_at"`
	FaultType        string            `json:"fault_type"`
	WouldTargetPods  []string          `json:"would_target_pods"`
	WouldBlastRadius int               `json:"would_blast_radius_pct"`
	WouldDurationSec int               `json:"would_duration_sec"`
	SafetyGateResults []GateCheckResult `json:"safety_gate_results"`
	WouldProceed     bool              `json:"would_proceed"`
	BlockedReason    string            `json:"blocked_reason,omitempty"`
	EstimatedImpact  string            `json:"estimated_impact"`
}

// GateCheckResult captures a single preflight safety gate evaluation.
type GateCheckResult struct {
	Name      string `json:"name"`
	Query     string `json:"query"`
	Threshold string `json:"threshold"`
	Observed  string `json:"observed"`
	Passed    bool   `json:"passed"`
}

// runDryRun performs a full dry-run simulation of the experiment.
// It mutates exp.Status with the DryRunReport and returns the report.
func (r *ChaosExperimentReconciler) runDryRun(
	ctx context.Context,
	exp *chaosv1alpha1.ChaosExperiment,
) (*DryRunReport, error) {
	logger := log.FromContext(ctx).WithValues("dryrun", exp.Name)
	logger.Info("starting dry-run simulation")

	dur, _ := parseDuration(exp.Spec.Duration)

	report := &DryRunReport{
		SimulatedAt:      time.Now().UTC(),
		FaultType:        string(exp.Spec.Fault.Type),
		WouldDurationSec: int(dur.Seconds()),
		WouldBlastRadius: exp.Spec.Target.Percentage,
	}

	// ── Step 1: Resolve target pods (read-only) ────────────────────────────
	executor := faults.NewExecutor(r.Client)
	targets, err := executor.SelectTargets(ctx, exp)
	if err != nil {
		report.BlockedReason = fmt.Sprintf("target selection failed: %v", err)
		report.WouldProceed = false
		return report, nil
	}

	report.WouldTargetPods = podNames(targets)
	logger.Info("dry-run: resolved targets", "count", len(targets))

	if len(targets) == 0 {
		report.BlockedReason = "no pods matched the target selector — experiment would be a no-op"
		report.WouldProceed = false
		return report, nil
	}

	// ── Step 2: Evaluate safety gates (real Prometheus queries) ───────────
	gateChecker := safety.NewGateChecker(r.PrometheusURL)
	var gateResults []GateCheckResult
	allPassed := true

	for _, gate := range exp.Spec.SafetyGates {
		passed, observed, err := gateChecker.Check(ctx, gate)
		result := GateCheckResult{
			Name:      gate.Type,
			Query:     gate.PrometheusQuery,
			Threshold: gate.Threshold,
			Passed:    passed,
		}
		if err != nil {
			result.Observed = fmt.Sprintf("error: %v", err)
			result.Passed = false
			allPassed = false
		} else {
			result.Observed = observed
			if !passed {
				allPassed = false
			}
		}
		gateResults = append(gateResults, result)
		logger.Info("dry-run gate check", "name", gate.Type, "passed", result.Passed, "observed", result.Observed)
	}

	report.SafetyGateResults = gateResults

	if !allPassed {
		blockedGates := []string{}
		for _, g := range gateResults {
			if !g.Passed {
				blockedGates = append(blockedGates, g.Name)
			}
		}
		report.WouldProceed = false
		report.BlockedReason = fmt.Sprintf("safety gates would block: %v", blockedGates)
	} else {
		report.WouldProceed = true
	}

	// ── Step 3: Estimate impact ───────────────────────────────────────────
	report.EstimatedImpact = estimateImpact(exp, report.WouldTargetPods)

	logger.Info("dry-run complete",
		"would_target", len(targets),
		"would_proceed", report.WouldProceed,
		"blocked_reason", report.BlockedReason,
	)

	return report, nil
}

// estimateImpact returns a human-readable impact summary for the dry-run report.
func estimateImpact(exp *chaosv1alpha1.ChaosExperiment, targets []string) string {
	switch exp.Spec.Fault.Type {
	case chaosv1alpha1.FaultPodKill:
		return fmt.Sprintf(
			"Would kill %d pod(s) over %s. If Deployment has <2 replicas, requests may fail during restart.",
			len(targets), exp.Spec.Duration,
		)
	case chaosv1alpha1.FaultCPUStress:
		params := exp.Spec.Fault.CPUStress
		if params != nil {
			return fmt.Sprintf(
				"Would stress CPU at %d%% load on %d core(s) across %d pod(s) for %s. HPA may scale out.",
				params.Load, params.Cores, len(targets), exp.Spec.Duration,
			)
		}
		return fmt.Sprintf("Would stress CPU on %d pod(s) for %s.", len(targets), exp.Spec.Duration)
	case chaosv1alpha1.FaultMemoryStress:
		params := exp.Spec.Fault.MemoryStress
		if params != nil {
			return fmt.Sprintf(
				"Would consume %dMB additional memory on %d pod(s) for %s. OOMKill risk if near limits.",
				params.MemoryMB, len(targets), exp.Spec.Duration,
			)
		}
		return fmt.Sprintf("Would stress memory on %d pod(s) for %s.", len(targets), exp.Spec.Duration)
	case chaosv1alpha1.FaultNetworkDelay:
		params := exp.Spec.Fault.NetworkDelay
		if params != nil {
			return fmt.Sprintf(
				"Would add %dms±%dms network delay on %d pod(s) for %s. p99 latency SLOs likely to breach.",
				params.LatencyMs, params.JitterMs, len(targets), exp.Spec.Duration,
			)
		}
		return fmt.Sprintf("Would inject network delay on %d pod(s) for %s.", len(targets), exp.Spec.Duration)
	case chaosv1alpha1.FaultDNSFailure:
		return fmt.Sprintf(
			"Would block DNS on %d pod(s) for %s. Service discovery and external API calls will fail.",
			len(targets), exp.Spec.Duration,
		)
	default:
		return fmt.Sprintf(
			"Would apply %s fault to %d pod(s) for %s.",
			exp.Spec.Fault.Type, len(targets), exp.Spec.Duration,
		)
	}
}

// podNames extracts pod names from a slice of corev1.Pod.
func podNames(pods []corev1.Pod) []string {
	names := make([]string, 0, len(pods))
	for _, p := range pods {
		names = append(names, p.Name)
	}
	return names
}
