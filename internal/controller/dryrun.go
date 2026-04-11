// Package controller provides dry-run simulation for ChaosExperiment.
//
// When spec.dryRun is set to true the operator:
//  1. Runs all preflight safety gate checks (real Prometheus queries)
//  2. Resolves target pods using the same selection logic as a live run
//  3. Logs what WOULD have happened without touching any resources
//  4. Writes a DryRunReport to the ChaosExperiment status
//  5. Sets Phase=DryRunCompleted instead of Completed
//
// This lets teams validate experiment configs, blast radius, and safety
// gate thresholds against real cluster state before running live.
package controller

import (
	"context"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/log"

	chaosv1alpha1 "github.com/sharatvikas/chaos-engineering-operator/api/v1alpha1"
	"github.com/sharatvikas/chaos-engineering-operator/internal/faults"
	"github.com/sharatvikas/chaos-engineering-operator/internal/safety"
)

// DryRunReport is the result of a dry-run simulation.
type DryRunReport struct {
	SimulatedAt       time.Time          `json:"simulated_at"`
	FaultType         string             `json:"fault_type"`
	WouldTargetPods   []string           `json:"would_target_pods"`
	WouldBlastRadius  int                `json:"would_blast_radius_pct"`
	WouldDurationSec  int                `json:"would_duration_sec"`
	SafetyGateResults []GateCheckResult  `json:"safety_gate_results"`
	WouldProceed      bool               `json:"would_proceed"`
	BlockedReason     string             `json:"blocked_reason,omitempty"`
	EstimatedImpact   string             `json:"estimated_impact"`
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

	report := &DryRunReport{
		SimulatedAt:      time.Now().UTC(),
		FaultType:        string(exp.Spec.FaultType),
		WouldDurationSec: int(exp.Spec.Duration.Seconds()),
		WouldBlastRadius: exp.Spec.BlastRadius.Percentage,
	}

	// ── Step 1: Resolve target pods (read-only) ────────────────────────────
	executor := faults.NewExecutor(r.Client)
	targets, err := executor.SelectTargets(ctx, exp)
	if err != nil {
		report.BlockedReason = fmt.Sprintf("target selection failed: %v", err)
		report.WouldProceed = false
		r.emitDryRunEvent(ctx, exp, "TargetSelectionFailed", report.BlockedReason)
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
			Name:      gate.Name,
			Query:     gate.Query,
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
		logger.Info("dry-run gate check", "name", gate.Name, "passed", result.Passed, "observed", result.Observed)
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

	// ── Step 4: Emit event and update status ──────────────────────────────
	eventMsg := fmt.Sprintf(
		"DryRun: would target %d pods, blast radius %d%%, proceed=%v",
		len(targets), report.WouldBlastRadius, report.WouldProceed,
	)
	r.emitDryRunEvent(ctx, exp, "DryRunCompleted", eventMsg)

	logger.Info("dry-run complete",
		"would_target", len(targets),
		"would_proceed", report.WouldProceed,
		"blocked_reason", report.BlockedReason,
	)

	return report, nil
}

// emitDryRunEvent records a Kubernetes Event on the ChaosExperiment.
func (r *ChaosExperimentReconciler) emitDryRunEvent(
	ctx context.Context,
	exp *chaosv1alpha1.ChaosExperiment,
	reason, message string,
) {
	r.Recorder.Event(exp, "Normal", reason, message)
}

// estimateImpact returns a human-readable impact summary for the dry-run report.
func estimateImpact(exp *chaosv1alpha1.ChaosExperiment, targets []string) string {
	switch exp.Spec.FaultType {
	case chaosv1alpha1.FaultTypePodKill:
		return fmt.Sprintf(
			"Would kill %d pod(s) over %s. If Deployment has <2 replicas, requests may fail during restart.",
			len(targets), exp.Spec.Duration,
		)
	case chaosv1alpha1.FaultTypeCPUStress:
		return fmt.Sprintf(
			"Would stress CPU at %d%% on %d pod(s) for %s. HPA may scale out; CPU-sensitive latency SLOs at risk.",
			exp.Spec.CPUStress.Percentage, len(targets), exp.Spec.Duration,
		)
	case chaosv1alpha1.FaultTypeMemoryStress:
		return fmt.Sprintf(
			"Would consume %dMB additional memory on %d pod(s) for %s. OOMKill risk if near limits.",
			exp.Spec.MemoryStress.SizeMB, len(targets), exp.Spec.Duration,
		)
	case chaosv1alpha1.FaultTypeNetworkDelay:
		return fmt.Sprintf(
			"Would add %dms±%dms network delay on %d pod(s) for %s. p99 latency SLOs likely to breach.",
			exp.Spec.NetworkDelay.LatencyMs, exp.Spec.NetworkDelay.JitterMs,
			len(targets), exp.Spec.Duration,
		)
	case chaosv1alpha1.FaultTypeNetworkLoss:
		return fmt.Sprintf(
			"Would drop %d%% of packets on %d pod(s) for %s. Connection timeouts and retry storms likely.",
			exp.Spec.NetworkLoss.Percentage, len(targets), exp.Spec.Duration,
		)
	default:
		return fmt.Sprintf(
			"Would apply %s fault to %d pod(s) for %s.",
			exp.Spec.FaultType, len(targets), exp.Spec.Duration,
		)
	}
}

// podNames extracts pod names from a list of pod objects.
func podNames(pods []interface{ GetName() string }) []string {
	names := make([]string, 0, len(pods))
	for _, p := range pods {
		names = append(names, p.GetName())
	}
	return names
}
