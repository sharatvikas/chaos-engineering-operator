package controller

import (
	"context"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	chaosv1alpha1 "github.com/sharatvikas/chaos-engineering-operator/api/v1alpha1"
	"github.com/sharatvikas/chaos-engineering-operator/internal/faults"
	"github.com/sharatvikas/chaos-engineering-operator/internal/safety"
)

// ChaosExperimentReconciler reconciles ChaosExperiment objects
type ChaosExperimentReconciler struct {
	client.Client
	Scheme         *runtime.Scheme
	PrometheusURL  string
	SlackWebhook   string
}

// +kubebuilder:rbac:groups=chaos.sharatvikas.io,resources=chaosexperiments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=chaos.sharatvikas.io,resources=chaosexperiments/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;delete
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

func (r *ChaosExperimentReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var experiment chaosv1alpha1.ChaosExperiment
	if err := r.Get(ctx, req.NamespacedName, &experiment); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	logger.Info("Reconciling ChaosExperiment",
		"name", experiment.Name,
		"phase", experiment.Status.Phase,
		"fault", experiment.Spec.Fault.Type,
	)

	switch experiment.Status.Phase {
	case "", chaosv1alpha1.PhasePending:
		return r.handlePending(ctx, &experiment)
	case chaosv1alpha1.PhasePreflightCheck:
		return r.handlePreflightCheck(ctx, &experiment)
	case chaosv1alpha1.PhaseRunning:
		return r.handleRunning(ctx, &experiment)
	case chaosv1alpha1.PhaseCompleted, chaosv1alpha1.PhaseAborted, chaosv1alpha1.PhaseFailed:
		// Terminal states — nothing to do
		return ctrl.Result{}, nil
	}

	return ctrl.Result{}, nil
}

func (r *ChaosExperimentReconciler) handlePending(
	ctx context.Context, exp *chaosv1alpha1.ChaosExperiment,
) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("Starting preflight check", "experiment", exp.Name)

	exp.Status.Phase = chaosv1alpha1.PhasePreflightCheck
	if err := r.Status().Update(ctx, exp); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{Requeue: true}, nil
}

func (r *ChaosExperimentReconciler) handlePreflightCheck(
	ctx context.Context, exp *chaosv1alpha1.ChaosExperiment,
) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Check that the target SLOs are healthy before we start
	if len(exp.Spec.SafetyGates) > 0 {
		checker := safety.NewGateChecker(r.PrometheusURL)
		for _, gate := range exp.Spec.SafetyGates {
			ok, reason, err := checker.Check(ctx, gate)
			if err != nil {
				logger.Error(err, "Safety gate check failed", "gate", gate.Type)
				continue
			}
			if !ok {
				logger.Info("Preflight safety gate not satisfied — postponing", "reason", reason)
				exp.Status.Phase = chaosv1alpha1.PhasePending
				if err := r.Status().Update(ctx, exp); err != nil {
					return ctrl.Result{}, err
				}
				// Retry in 5 minutes
				return ctrl.Result{RequeueAfter: 5 * time.Minute}, nil
			}
		}
	}

	// All gates passed — start the experiment
	now := metaTime()
	exp.Status.Phase = chaosv1alpha1.PhaseRunning
	exp.Status.StartTime = &now

	if exp.Spec.DryRun {
		logger.Info("DRY RUN — would start fault injection", "type", exp.Spec.Fault.Type)
		exp.Status.Phase = chaosv1alpha1.PhaseCompleted
		exp.Status.Report = fmt.Sprintf("Dry run completed. Would have injected %s fault.", exp.Spec.Fault.Type)
		return ctrl.Result{}, r.Status().Update(ctx, exp)
	}

	// Select target pods
	targets, err := faults.SelectTargets(ctx, r.Client, exp.Spec.Target)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to select targets: %w", err)
	}

	exp.Status.TargetedPods = targets
	if err := r.Status().Update(ctx, exp); err != nil {
		return ctrl.Result{}, err
	}

	// Start fault injection (non-blocking — controller will monitor via requeue)
	executor := faults.NewExecutor(r.Client)
	if err := executor.Start(ctx, exp, targets); err != nil {
		exp.Status.Phase = chaosv1alpha1.PhaseFailed
		r.Status().Update(ctx, exp)
		return ctrl.Result{}, fmt.Errorf("fault injection failed to start: %w", err)
	}

	logger.Info("Fault injection started",
		"type", exp.Spec.Fault.Type,
		"targets", len(targets),
		"duration", exp.Spec.Duration,
	)

	// Parse duration and requeue at experiment end
	duration, _ := time.ParseDuration(exp.Spec.Duration)
	return ctrl.Result{RequeueAfter: duration}, r.Status().Update(ctx, exp)
}

func (r *ChaosExperimentReconciler) handleRunning(
	ctx context.Context, exp *chaosv1alpha1.ChaosExperiment,
) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Check safety gates while running
	if len(exp.Spec.SafetyGates) > 0 {
		checker := safety.NewGateChecker(r.PrometheusURL)
		for _, gate := range exp.Spec.SafetyGates {
			ok, reason, err := checker.Check(ctx, gate)
			if err != nil {
				logger.Error(err, "Safety gate check error during experiment", "gate", gate.Type)
				continue
			}
			if !ok {
				logger.Info("Safety gate triggered — aborting experiment", "reason", reason)
				return r.abortExperiment(ctx, exp, reason)
			}
		}
	}

	// Check if experiment duration has elapsed
	if exp.Status.StartTime != nil {
		duration, err := time.ParseDuration(exp.Spec.Duration)
		if err == nil {
			elapsed := time.Since(exp.Status.StartTime.Time)
			if elapsed >= duration {
				return r.completeExperiment(ctx, exp)
			}
			// Check again in 10 seconds (for safety gate polling)
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
	}

	return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
}

func (r *ChaosExperimentReconciler) abortExperiment(
	ctx context.Context, exp *chaosv1alpha1.ChaosExperiment, reason string,
) (ctrl.Result, error) {
	log.FromContext(ctx).Info("Aborting experiment", "reason", reason)

	executor := faults.NewExecutor(r.Client)
	executor.Stop(ctx, exp)

	now := metaTime()
	exp.Status.Phase = chaosv1alpha1.PhaseAborted
	exp.Status.EndTime = &now
	exp.Status.AbortReason = reason

	return ctrl.Result{}, r.Status().Update(ctx, exp)
}

func (r *ChaosExperimentReconciler) completeExperiment(
	ctx context.Context, exp *chaosv1alpha1.ChaosExperiment,
) (ctrl.Result, error) {
	log.FromContext(ctx).Info("Experiment completed successfully")

	executor := faults.NewExecutor(r.Client)
	executor.Stop(ctx, exp)

	now := metaTime()
	exp.Status.Phase = chaosv1alpha1.PhaseCompleted
	exp.Status.EndTime = &now

	duration := now.Time.Sub(exp.Status.StartTime.Time)
	exp.Status.Report = fmt.Sprintf(
		"Experiment completed. Fault: %s. Duration: %s. Targets affected: %d.",
		exp.Spec.Fault.Type, duration.Round(time.Second), len(exp.Status.TargetedPods),
	)

	return ctrl.Result{}, r.Status().Update(ctx, exp)
}

func (r *ChaosExperimentReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&chaosv1alpha1.ChaosExperiment{}).
		Complete(r)
}

func metaTime() metav1Time {
	return metav1Time{Time: time.Now()}
}

// metav1Time is an alias to avoid import cycle in this snippet
type metav1Time = interface{ Time time.Time }
