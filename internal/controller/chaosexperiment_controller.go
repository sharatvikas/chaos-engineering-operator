package controller

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	chaosv1alpha1 "github.com/sharatvikas/chaos-engineering-operator/api/v1alpha1"
	"github.com/sharatvikas/chaos-engineering-operator/internal/faults"
	"github.com/sharatvikas/chaos-engineering-operator/internal/safety"
)

const (
	requeueRunning  = 15 * time.Second
	requeuePending  = 5 * time.Second
	requeueCooldown = 30 * time.Second
)

// ChaosExperimentReconciler reconciles ChaosExperiment objects
type ChaosExperimentReconciler struct {
	client.Client
	Scheme        *runtime.Scheme
	PrometheusURL string

	// RESTConfig and ClientSet enable exec-based faults (ContainerKill).
	// They are optional: when nil (e.g. unit tests), exec faults return a
	// clear error rather than crashing.
	RESTConfig *rest.Config
	ClientSet  kubernetes.Interface

	// NamespaceDenylist lists namespaces experiments may never target.
	// Empty means the built-in default set is used.
	NamespaceDenylist []string
}

// newExecutor builds a fault Executor wired with this reconciler's runtime
// dependencies (client + optional exec deps).
func (r *ChaosExperimentReconciler) newExecutor() *faults.Executor {
	return faults.NewExecutorWithDeps(faults.Deps{
		Client:     r.Client,
		RESTConfig: r.RESTConfig,
		ClientSet:  r.ClientSet,
	})
}

// preflight returns the configured static safety checker.
func (r *ChaosExperimentReconciler) preflight() safety.PreflightChecker {
	denylist := r.NamespaceDenylist
	if len(denylist) == 0 {
		denylist = safety.DefaultNamespaceDenylist()
	}
	return safety.PreflightChecker{NamespaceDenylist: denylist}
}

// +kubebuilder:rbac:groups=chaos.sharatvikas.io,resources=chaosexperiments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=chaos.sharatvikas.io,resources=chaosexperiments/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch;delete;create
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch

func (r *ChaosExperimentReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var exp chaosv1alpha1.ChaosExperiment
	if err := r.Get(ctx, req.NamespacedName, &exp); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	switch exp.Status.Phase {
	case "", "Pending":
		return r.handlePending(ctx, &exp)
	case "PreflightCheck":
		return r.handlePreflightCheck(ctx, &exp)
	case "Running":
		return r.handleRunning(ctx, &exp)
	case "Cooldown":
		return r.handleCooldown(ctx, &exp)
	case "Completed", "Aborted":
		logger.Info("experiment is terminal", "phase", exp.Status.Phase)
		return ctrl.Result{}, nil
	default:
		logger.Info("unknown phase, resetting", "phase", exp.Status.Phase)
		return r.setPhase(ctx, &exp, "Pending")
	}
}

func (r *ChaosExperimentReconciler) handlePending(ctx context.Context, exp *chaosv1alpha1.ChaosExperiment) (ctrl.Result, error) {
	log.FromContext(ctx).Info("experiment pending, starting preflight")
	return r.setPhase(ctx, exp, "PreflightCheck")
}

func (r *ChaosExperimentReconciler) handlePreflightCheck(ctx context.Context, exp *chaosv1alpha1.ChaosExperiment) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// ── Static preflight guards (no external calls, never fail open) ───────
	pf := r.preflight()

	ns := exp.Spec.Target.Namespace
	if ns == "" {
		ns = exp.Namespace
	}
	if ok, reason := pf.CheckNamespace(ns); !ok {
		logger.Info("preflight blocked experiment", "guard", "namespace-denylist", "reason", reason)
		return r.abort(ctx, exp, fmt.Sprintf("preflight namespace guard: %s", reason))
	}

	if exp.Spec.Target.MinHealthyReplicas > 0 {
		affected, total, err := r.newExecutor().SelectTargetsWithTotal(ctx, exp)
		if err != nil {
			logger.Error(err, "preflight target selection failed — failing safe (blocking)")
			return r.abort(ctx, exp, fmt.Sprintf("preflight target selection failed: %v", err))
		}
		if ok, reason := pf.CheckMinHealthyReplicas(total, len(affected), exp.Spec.Target.MinHealthyReplicas); !ok {
			logger.Info("preflight blocked experiment", "guard", "min-healthy-replicas", "reason", reason)
			return r.abort(ctx, exp, fmt.Sprintf("preflight replica guard: %s", reason))
		}
	}

	// ── Dynamic SLO gates (live Prometheus queries) ────────────────────────
	checker := safety.NewGateChecker(r.PrometheusURL)

	for _, gate := range exp.Spec.SafetyGates {
		safe, reason, err := checker.Check(ctx, gate)
		if err != nil {
			logger.Error(err, "gate check error — failing open", "gate", gate.Type)
			continue
		}
		if !safe {
			logger.Info("safety gate blocked experiment", "gate", gate.Type, "reason", reason)
			return r.abort(ctx, exp, fmt.Sprintf("gate %s blocked: %s", gate.Type, reason))
		}
	}

	if exp.Spec.DryRun {
		logger.Info("dry run — skipping fault injection")
		return r.setPhase(ctx, exp, "Completed")
	}
	return r.startFaults(ctx, exp)
}

func (r *ChaosExperimentReconciler) startFaults(ctx context.Context, exp *chaosv1alpha1.ChaosExperiment) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	executor := r.newExecutor()

	count, err := executor.Start(ctx, exp)
	if err != nil {
		return r.abort(ctx, exp, fmt.Sprintf("fault injection failed: %v", err))
	}

	logger.Info("faults injected", "type", exp.Spec.Fault.Type, "pods", count)

	patch := client.MergeFrom(exp.DeepCopy())
	now := metav1.Now()
	exp.Status.Phase = "Running"
	exp.Status.StartTime = &now
	// TargetedPods is []string — record count as a synthetic pod-count entry
	exp.Status.TargetedPods = make([]string, count)
	meta.SetStatusCondition(&exp.Status.Conditions, metav1.Condition{
		Type:               "Running",
		Status:             metav1.ConditionTrue,
		Reason:             "FaultInjected",
		Message:            fmt.Sprintf("%s on %d pods", exp.Spec.Fault.Type, count),
		LastTransitionTime: now,
	})

	duration, _ := parseDuration(exp.Spec.Duration)
	return ctrl.Result{RequeueAfter: duration}, r.Status().Patch(ctx, exp, patch)
}

func (r *ChaosExperimentReconciler) handleRunning(ctx context.Context, exp *chaosv1alpha1.ChaosExperiment) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Mid-experiment gate evaluation — abort if SLOs degrade
	checker := safety.NewGateChecker(r.PrometheusURL)
	for _, gate := range exp.Spec.SafetyGates {
		safe, reason, err := checker.Check(ctx, gate)
		if err != nil {
			continue
		}
		if !safe {
			logger.Info("mid-experiment gate tripped — auto-rolling back", "gate", gate.Type, "reason", reason)
			if err := r.newExecutor().Stop(ctx, exp); err != nil {
				logger.Error(err, "auto-rollback error — manual cleanup may be required")
			}
			return r.abort(ctx, exp, fmt.Sprintf("mid-experiment gate %s: %s", gate.Type, reason))
		}
	}

	duration, _ := parseDuration(exp.Spec.Duration)
	if exp.Status.StartTime != nil && time.Since(exp.Status.StartTime.Time) >= duration {
		logger.Info("duration elapsed — stopping faults")
		if err := r.newExecutor().Stop(ctx, exp); err != nil {
			logger.Error(err, "fault cleanup error — manual cleanup may be required")
		}
		return r.setPhase(ctx, exp, "Cooldown")
	}

	return ctrl.Result{RequeueAfter: requeueRunning}, nil
}

func (r *ChaosExperimentReconciler) handleCooldown(ctx context.Context, exp *chaosv1alpha1.ChaosExperiment) (ctrl.Result, error) {
	cooldown, _ := parseDuration(exp.Spec.Cooldown)
	if cooldown == 0 {
		cooldown = 5 * time.Minute
	}

	stopTime := time.Now()
	if exp.Status.EndTime != nil {
		stopTime = exp.Status.EndTime.Time
	}

	if time.Since(stopTime) < cooldown {
		return ctrl.Result{RequeueAfter: cooldown - time.Since(stopTime)}, nil
	}
	return r.setPhase(ctx, exp, "Completed")
}

func (r *ChaosExperimentReconciler) abort(ctx context.Context, exp *chaosv1alpha1.ChaosExperiment, reason string) (ctrl.Result, error) {
	patch := client.MergeFrom(exp.DeepCopy())
	now := metav1.Now()
	exp.Status.Phase = "Aborted"
	exp.Status.AbortReason = reason
	exp.Status.EndTime = &now
	meta.SetStatusCondition(&exp.Status.Conditions, metav1.Condition{
		Type:               "Running",
		Status:             metav1.ConditionFalse,
		Reason:             "Aborted",
		Message:            reason,
		LastTransitionTime: now,
	})
	return ctrl.Result{}, r.Status().Patch(ctx, exp, patch)
}

func (r *ChaosExperimentReconciler) setPhase(ctx context.Context, exp *chaosv1alpha1.ChaosExperiment, phase string) (ctrl.Result, error) {
	patch := client.MergeFrom(exp.DeepCopy())
	exp.Status.Phase = chaosv1alpha1.ExperimentPhase(phase)
	if phase == "Completed" {
		now := metav1.Now()
		exp.Status.EndTime = &now
	}
	requeue := map[string]time.Duration{
		"Pending": requeuePending, "PreflightCheck": requeuePending, "Cooldown": requeueCooldown,
	}[phase]
	return ctrl.Result{RequeueAfter: requeue}, r.Status().Patch(ctx, exp, patch)
}

func (r *ChaosExperimentReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).For(&chaosv1alpha1.ChaosExperiment{}).Complete(r)
}

func parseDuration(s string) (time.Duration, error) {
	if s == "" {
		return 5 * time.Minute, nil
	}
	return time.ParseDuration(s)
}

// Ensure corev1 is used (for EventTypeWarning in event recording)
var _ = corev1.EventTypeWarning
