package controller

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	chaosv1alpha1 "github.com/sharatvikas/chaos-engineering-operator/api/v1alpha1"
)

// ─── Test Helpers ─────────────────────────────────────────────────────────────

func newScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	_ = chaosv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	return scheme
}

func newReconciler(objs ...client.Object) *ChaosExperimentReconciler {
	fakeClient := fake.NewClientBuilder().
		WithScheme(newScheme()).
		WithObjects(objs...).
		WithStatusSubresource(&chaosv1alpha1.ChaosExperiment{}).
		Build()
	return &ChaosExperimentReconciler{
		Client:        fakeClient,
		Scheme:        newScheme(),
		PrometheusURL: "", // no Prometheus in unit tests
	}
}

func newExperiment(name, namespace, phase string) *chaosv1alpha1.ChaosExperiment {
	return &chaosv1alpha1.ChaosExperiment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: chaosv1alpha1.ChaosExperimentSpec{
			Target: chaosv1alpha1.TargetSpec{
				Namespace: namespace,
				Selector: metav1.LabelSelector{
					MatchLabels: map[string]string{
						"app": "test-service",
					},
				},
				Percentage: 50,
				Mode:       "random",
			},
			Fault: chaosv1alpha1.FaultSpec{
				Type: "PodKill",
			},
			Duration: "2m",
			Cooldown: "1m",
		},
		Status: chaosv1alpha1.ChaosExperimentStatus{
			Phase: chaosv1alpha1.ExperimentPhase(phase),
		},
	}
}

func newTargetPod(name, namespace string, phase corev1.PodPhase) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    map[string]string{"app": "test-service"},
		},
		Spec: corev1.PodSpec{
			NodeName: "node-1",
		},
		Status: corev1.PodStatus{
			Phase: phase,
			PodIP: "10.0.0.1",
		},
	}
}

func reconcileRequest(name, namespace string) ctrl.Request {
	return ctrl.Request{NamespacedName: types.NamespacedName{Name: name, Namespace: namespace}}
}

func getExperiment(t *testing.T, r *ChaosExperimentReconciler, name, namespace string) *chaosv1alpha1.ChaosExperiment {
	t.Helper()
	var exp chaosv1alpha1.ChaosExperiment
	if err := r.Get(context.Background(), types.NamespacedName{Name: name, Namespace: namespace}, &exp); err != nil {
		t.Fatalf("get experiment: %v", err)
	}
	return &exp
}

// ─── Reconcile State Machine Tests ───────────────────────────────────────────

func TestReconcile_NotFound(t *testing.T) {
	r := newReconciler() // No objects
	result, err := r.Reconcile(context.Background(), reconcileRequest("nonexistent", "default"))
	if err != nil {
		t.Fatalf("expected no error for missing resource, got %v", err)
	}
	if result.Requeue {
		t.Error("expected no requeue for missing resource")
	}
}

func TestReconcile_PendingTransitionsToPreflight(t *testing.T) {
	exp := newExperiment("test-exp", "default", "")
	r := newReconciler(exp)

	_, err := r.Reconcile(context.Background(), reconcileRequest("test-exp", "default"))
	if err != nil {
		t.Fatalf("reconcile error: %v", err)
	}

	got := getExperiment(t, r, "test-exp", "default")
	if got.Status.Phase != "PreflightCheck" {
		t.Errorf("expected PreflightCheck, got %q", got.Status.Phase)
	}
}

func TestReconcile_ExplicitPendingTransitionsToPreflight(t *testing.T) {
	exp := newExperiment("test-exp", "default", "Pending")
	r := newReconciler(exp)

	_, err := r.Reconcile(context.Background(), reconcileRequest("test-exp", "default"))
	if err != nil {
		t.Fatalf("reconcile error: %v", err)
	}

	got := getExperiment(t, r, "test-exp", "default")
	if got.Status.Phase != "PreflightCheck" {
		t.Errorf("expected PreflightCheck, got %q", got.Status.Phase)
	}
}

func TestReconcile_DryRunCompletesImmediately(t *testing.T) {
	exp := newExperiment("dry-exp", "default", "PreflightCheck")
	exp.Spec.DryRun = true
	r := newReconciler(exp)

	_, err := r.Reconcile(context.Background(), reconcileRequest("dry-exp", "default"))
	if err != nil {
		t.Fatalf("reconcile error: %v", err)
	}

	got := getExperiment(t, r, "dry-exp", "default")
	if got.Status.Phase != "Completed" {
		t.Errorf("expected Completed (dry run), got %q", got.Status.Phase)
	}
}

func TestReconcile_PreflightWithNoPodsAborts(t *testing.T) {
	exp := newExperiment("no-pods-exp", "default", "PreflightCheck")
	// No pods registered — executor.Start will fail
	r := newReconciler(exp)

	_, err := r.Reconcile(context.Background(), reconcileRequest("no-pods-exp", "default"))
	if err != nil {
		t.Fatalf("reconcile error: %v", err)
	}

	got := getExperiment(t, r, "no-pods-exp", "default")
	if got.Status.Phase != "Aborted" {
		t.Errorf("expected Aborted (no pods), got %q", got.Status.Phase)
	}
	if got.Status.AbortReason == "" {
		t.Error("expected AbortReason to be set")
	}
}

func TestReconcile_PreflightWithPodsProceedsToRunning(t *testing.T) {
	exp := newExperiment("run-exp", "default", "PreflightCheck")
	pod := newTargetPod("target-pod-1", "default", corev1.PodRunning)
	r := newReconciler(exp, pod)

	_, err := r.Reconcile(context.Background(), reconcileRequest("run-exp", "default"))
	if err != nil {
		t.Fatalf("reconcile error: %v", err)
	}

	got := getExperiment(t, r, "run-exp", "default")
	if got.Status.Phase != "Running" {
		t.Errorf("expected Running, got %q", got.Status.Phase)
	}
	if len(got.Status.TargetedPods) == 0 {
		t.Error("expected TargetedPods > 0")
	}
	if got.Status.StartTime == nil {
		t.Error("expected StartTime to be set")
	}
}

func TestReconcile_RunningBeforeDurationRequeues(t *testing.T) {
	exp := newExperiment("running-exp", "default", "Running")
	now := metav1.Now()
	exp.Status.StartTime = &now // Just started — duration not elapsed
	exp.Spec.Duration = "10m"
	r := newReconciler(exp)

	result, err := r.Reconcile(context.Background(), reconcileRequest("running-exp", "default"))
	if err != nil {
		t.Fatalf("reconcile error: %v", err)
	}

	if result.RequeueAfter == 0 {
		t.Error("expected RequeueAfter to be set while experiment is running")
	}

	// Phase should remain Running
	got := getExperiment(t, r, "running-exp", "default")
	if got.Status.Phase != "Running" {
		t.Errorf("expected Running, got %q", got.Status.Phase)
	}
}

func TestReconcile_RunningAfterDurationTransitionsToCooldown(t *testing.T) {
	exp := newExperiment("elapsed-exp", "default", "Running")
	// Start time 15 minutes ago, duration is 10m — elapsed
	past := metav1.NewTime(time.Now().Add(-15 * time.Minute))
	exp.Status.StartTime = &past
	exp.Spec.Duration = "10m"
	r := newReconciler(exp)

	_, err := r.Reconcile(context.Background(), reconcileRequest("elapsed-exp", "default"))
	if err != nil {
		t.Fatalf("reconcile error: %v", err)
	}

	got := getExperiment(t, r, "elapsed-exp", "default")
	if got.Status.Phase != "Cooldown" {
		t.Errorf("expected Cooldown after duration elapsed, got %q", got.Status.Phase)
	}
}

func TestReconcile_CooldownTransitionsToCompleted(t *testing.T) {
	exp := newExperiment("cooldown-exp", "default", "Cooldown")
	// End time 10 minutes ago, cooldown is 5m — elapsed
	past := metav1.NewTime(time.Now().Add(-10 * time.Minute))
	exp.Status.EndTime = &past
	exp.Spec.Cooldown = "5m"
	r := newReconciler(exp)

	_, err := r.Reconcile(context.Background(), reconcileRequest("cooldown-exp", "default"))
	if err != nil {
		t.Fatalf("reconcile error: %v", err)
	}

	got := getExperiment(t, r, "cooldown-exp", "default")
	if got.Status.Phase != "Completed" {
		t.Errorf("expected Completed after cooldown, got %q", got.Status.Phase)
	}
	if got.Status.EndTime == nil {
		t.Error("expected EndTime to be set on Completed")
	}
}

func TestReconcile_CooldownNotElapsedRequeues(t *testing.T) {
	exp := newExperiment("cooldown-wait-exp", "default", "Cooldown")
	now := metav1.Now()
	exp.Status.EndTime = &now // Just ended
	exp.Spec.Cooldown = "10m"
	r := newReconciler(exp)

	result, err := r.Reconcile(context.Background(), reconcileRequest("cooldown-wait-exp", "default"))
	if err != nil {
		t.Fatalf("reconcile error: %v", err)
	}

	if result.RequeueAfter == 0 {
		t.Error("expected RequeueAfter during cooldown wait")
	}

	got := getExperiment(t, r, "cooldown-wait-exp", "default")
	if got.Status.Phase != "Cooldown" {
		t.Errorf("expected Cooldown, got %q", got.Status.Phase)
	}
}

func TestReconcile_CompletedIsTerminal(t *testing.T) {
	exp := newExperiment("done-exp", "default", "Completed")
	r := newReconciler(exp)

	result, err := r.Reconcile(context.Background(), reconcileRequest("done-exp", "default"))
	if err != nil {
		t.Fatalf("reconcile error: %v", err)
	}

	// Completed is terminal — no requeue
	if result.Requeue || result.RequeueAfter != 0 {
		t.Error("Completed experiment should not requeue")
	}
}

func TestReconcile_AbortedIsTerminal(t *testing.T) {
	exp := newExperiment("aborted-exp", "default", "Aborted")
	exp.Status.AbortReason = "gate blocked"
	r := newReconciler(exp)

	result, err := r.Reconcile(context.Background(), reconcileRequest("aborted-exp", "default"))
	if err != nil {
		t.Fatalf("reconcile error: %v", err)
	}

	if result.Requeue || result.RequeueAfter != 0 {
		t.Error("Aborted experiment should not requeue")
	}
}

func TestReconcile_UnknownPhaseResetsToPending(t *testing.T) {
	exp := newExperiment("unknown-exp", "default", "BogusPhase")
	r := newReconciler(exp)

	_, err := r.Reconcile(context.Background(), reconcileRequest("unknown-exp", "default"))
	if err != nil {
		t.Fatalf("reconcile error: %v", err)
	}

	got := getExperiment(t, r, "unknown-exp", "default")
	if got.Status.Phase != "Pending" {
		t.Errorf("expected Pending after unknown phase reset, got %q", got.Status.Phase)
	}
}

// ─── parseDuration Tests ──────────────────────────────────────────────────────

func TestParseDuration(t *testing.T) {
	tests := []struct {
		input    string
		expected time.Duration
		wantErr  bool
	}{
		{"", 5 * time.Minute, false},
		{"10m", 10 * time.Minute, false},
		{"30s", 30 * time.Second, false},
		{"1h", 1 * time.Hour, false},
		{"invalid", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := parseDuration(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseDuration(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			}
			if !tt.wantErr && got != tt.expected {
				t.Errorf("parseDuration(%q) = %v, expected %v", tt.input, got, tt.expected)
			}
		})
	}
}

// ─── Target Pod Selection Tests ───────────────────────────────────────────────

func TestReconcile_OnlyRunningPodsTargeted(t *testing.T) {
	exp := newExperiment("select-exp", "default", "PreflightCheck")
	exp.Spec.Target.Percentage = 100
	exp.Spec.Target.Mode = "all"

	runningPod := newTargetPod("running-pod", "default", corev1.PodRunning)
	pendingPod := newTargetPod("pending-pod", "default", corev1.PodPending)
	failedPod := newTargetPod("failed-pod", "default", corev1.PodFailed)

	r := newReconciler(exp, runningPod, pendingPod, failedPod)

	_, err := r.Reconcile(context.Background(), reconcileRequest("select-exp", "default"))
	if err != nil {
		t.Fatalf("reconcile error: %v", err)
	}

	got := getExperiment(t, r, "select-exp", "default")
	// Only 1 running pod should be targeted
	if got.Status.Phase == "Running" && len(got.Status.TargetedPods) != 1 {
		t.Errorf("expected 1 targeted pod (only running), got %d", len(got.Status.TargetedPods))
	}
}
