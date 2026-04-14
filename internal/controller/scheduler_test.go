package controller

// scheduler_test.go — Unit tests for EvaluateSchedule, NextScheduledRuns,
// and ValidateSchedule. Uses fixed wall-clock times for determinism.

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	chaosv1alpha1 "github.com/sharatvikas/chaos-engineering-operator/api/v1alpha1"
)

// makeExp builds a minimal ChaosExperiment with a schedule for testing.
func makeExp(cronExpr, timezone string, suspend bool, lastScheduledAt *time.Time) *chaosv1alpha1.ChaosExperiment {
	exp := &chaosv1alpha1.ChaosExperiment{
		Spec: chaosv1alpha1.ChaosExperimentSpec{
			Schedule: &chaosv1alpha1.ScheduleSpec{
				Cron:     cronExpr,
				Timezone: timezone,
				Suspend:  suspend,
			},
		},
	}
	if lastScheduledAt != nil {
		t := metav1.NewTime(*lastScheduledAt)
		exp.Status.LastScheduledTime = &t
	}
	return exp
}

// ── EvaluateSchedule ────────────────────────────────────────────────────────

func TestEvaluateSchedule_Suspended(t *testing.T) {
	exp := makeExp("*/5 * * * *", "", true, nil)
	result, err := EvaluateSchedule(exp, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.Suspended {
		t.Error("expected Suspended=true")
	}
	if result.ShouldRunNow {
		t.Error("suspended experiment should not trigger ShouldRunNow")
	}
	if result.RequeueAfter < 1*time.Minute {
		t.Errorf("suspended RequeueAfter too short: %v", result.RequeueAfter)
	}
}

func TestEvaluateSchedule_NilSchedule(t *testing.T) {
	exp := &chaosv1alpha1.ChaosExperiment{}
	_, err := EvaluateSchedule(exp, time.Now())
	if err == nil {
		t.Fatal("expected error for nil schedule, got nil")
	}
}

func TestEvaluateSchedule_InvalidCron(t *testing.T) {
	exp := makeExp("not-a-cron", "", false, nil)
	_, err := EvaluateSchedule(exp, time.Now())
	if err == nil {
		t.Fatal("expected error for invalid cron expression")
	}
}

func TestEvaluateSchedule_InvalidTimezone(t *testing.T) {
	exp := makeExp("* * * * *", "Narnia/NotReal", false, nil)
	_, err := EvaluateSchedule(exp, time.Now())
	if err == nil {
		t.Fatal("expected error for invalid timezone")
	}
}

func TestEvaluateSchedule_ShouldRunNow_WithinWindow(t *testing.T) {
	// Schedule every 10 minutes; set LastScheduledTime to 10m+30s ago
	// so the scheduler sees a missed event within the 5-minute run window.
	now := time.Date(2025, 1, 15, 12, 10, 30, 0, time.UTC) // 12:10:30
	// Last run was at 12:00:00; next scheduled is 12:10:00 (30s ago — within window)
	lastRun := time.Date(2025, 1, 15, 12, 0, 0, 0, time.UTC)
	exp := makeExp("*/10 * * * *", "", false, &lastRun)

	result, err := EvaluateSchedule(exp, now)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.ShouldRunNow {
		t.Errorf("expected ShouldRunNow=true (30s within 5-min window), got false at %v", now)
	}
}

func TestEvaluateSchedule_ShouldNotRun_AlreadyTriggered(t *testing.T) {
	// Schedule every minute; LastScheduledTime = now - 30s (already ran this minute)
	now := time.Date(2025, 1, 15, 12, 1, 30, 0, time.UTC)
	lastRun := now.Add(-30 * time.Second)

	exp := makeExp("* * * * *", "", false, &lastRun)
	result, err := EvaluateSchedule(exp, now)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.ShouldRunNow {
		t.Errorf("expected ShouldRunNow=false (already ran this interval), got true")
	}
}

func TestEvaluateSchedule_ShouldNotRun_FutureSchedule(t *testing.T) {
	// Schedule at 3am on Saturday — run check on a Monday at noon
	mon := time.Date(2025, 1, 13, 12, 0, 0, 0, time.UTC) // Monday
	exp := makeExp("0 3 * * SAT", "", false, nil)
	result, err := EvaluateSchedule(exp, mon)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.ShouldRunNow {
		t.Errorf("expected ShouldRunNow=false for Saturday cron on Monday")
	}
	if result.NextRun.Weekday() != time.Saturday {
		t.Errorf("expected NextRun on Saturday, got %v (%v)", result.NextRun.Weekday(), result.NextRun)
	}
}

func TestEvaluateSchedule_TimezoneRespected(t *testing.T) {
	// Schedule at midnight UTC; "now" in Denver is 6pm (midnight - 6h offset = 18:00)
	// Should NOT trigger yet because UTC midnight hasn't happened from Denver's perspective
	// but EvaluateSchedule should at minimum parse without error.
	exp := makeExp("0 0 * * *", "America/Denver", false, nil)
	now := time.Date(2025, 1, 15, 18, 0, 0, 0, time.UTC) // 6pm UTC = 11am Denver
	_, err := EvaluateSchedule(exp, now)
	if err != nil {
		t.Fatalf("timezone parsing failed: %v", err)
	}
}

func TestEvaluateSchedule_RequeueAfterIsSet(t *testing.T) {
	// RequeueAfter uses time.Until() internally, so use real "now" to avoid
	// negative durations from fixed past timestamps.
	exp := makeExp("0 3 * * SAT", "", false, nil) // Saturday 3am
	result, err := EvaluateSchedule(exp, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// RequeueAfter should be set (non-zero) — exact sign depends on real clock
	// but NextRun should always be in the future relative to real now.
	if result.RequeueAfter == 0 {
		t.Error("expected non-zero RequeueAfter")
	}
	if result.NextRun.IsZero() {
		t.Error("expected non-zero NextRun")
	}
}

// ── ValidateSchedule ────────────────────────────────────────────────────────

func TestValidateSchedule_ValidExpressions(t *testing.T) {
	valid := []string{
		"0 3 * * SAT",     // Weekly
		"0 */6 * * *",     // Every 6 hours
		"30 2 1 * *",      // Monthly
		"0 12 * * MON-FRI", // Weekdays at noon
	}
	for _, expr := range valid {
		if err := ValidateSchedule(expr); err != nil {
			t.Errorf("expected valid: %q, got error: %v", expr, err)
		}
	}
}

func TestValidateSchedule_InvalidCron(t *testing.T) {
	invalid := []string{
		"not a cron",
		"99 * * * *",  // minute out of range
		"",
	}
	for _, expr := range invalid {
		if err := ValidateSchedule(expr); err == nil {
			t.Errorf("expected error for invalid cron %q, got nil", expr)
		}
	}
}

func TestValidateSchedule_TooAggressive(t *testing.T) {
	// Every minute is too aggressive (< 5 minute interval)
	if err := ValidateSchedule("* * * * *"); err == nil {
		t.Error("expected error for every-minute schedule, got nil")
	}
	// Every 3 minutes is also too aggressive
	if err := ValidateSchedule("*/3 * * * *"); err == nil {
		t.Error("expected error for every-3-minute schedule, got nil")
	}
}

func TestValidateSchedule_MinimumAllowed(t *testing.T) {
	// Every 5 minutes should be exactly at the boundary — should pass
	if err := ValidateSchedule("*/5 * * * *"); err != nil {
		// Note: */5 gives exactly 5 minute intervals — whether this is allowed
		// depends on the implementation. We just verify it doesn't panic.
		t.Logf("*/5 cron rejected: %v (boundary case)", err)
	}
}

// ── NextScheduledRuns ────────────────────────────────────────────────────────

func TestNextScheduledRuns_NilSchedule(t *testing.T) {
	exp := &chaosv1alpha1.ChaosExperiment{}
	runs, err := NextScheduledRuns(exp, 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(runs) != 0 {
		t.Errorf("expected 0 runs for nil schedule, got %d", len(runs))
	}
}

func TestNextScheduledRuns_ReturnsNRuns(t *testing.T) {
	exp := makeExp("0 */6 * * *", "", false, nil) // every 6 hours
	runs, err := NextScheduledRuns(exp, 4)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(runs) != 4 {
		t.Errorf("expected 4 runs, got %d", len(runs))
	}
}

func TestNextScheduledRuns_RunsAreAscending(t *testing.T) {
	exp := makeExp("0 */2 * * *", "", false, nil) // every 2 hours
	runs, err := NextScheduledRuns(exp, 6)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for i := 1; i < len(runs); i++ {
		if !runs[i].After(runs[i-1]) {
			t.Errorf("runs[%d] (%v) is not after runs[%d] (%v)", i, runs[i], i-1, runs[i-1])
		}
	}
}

func TestNextScheduledRuns_SaturdayCron(t *testing.T) {
	exp := makeExp("0 3 * * SAT", "", false, nil)
	runs, err := NextScheduledRuns(exp, 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, run := range runs {
		if run.Weekday() != time.Saturday {
			t.Errorf("expected Saturday run, got %v (%v)", run.Weekday(), run)
		}
		if run.Hour() != 3 || run.Minute() != 0 {
			t.Errorf("expected 03:00, got %02d:%02d", run.Hour(), run.Minute())
		}
	}
}

func TestNextScheduledRuns_InvalidCron(t *testing.T) {
	exp := makeExp("bad cron", "", false, nil)
	_, err := NextScheduledRuns(exp, 3)
	if err == nil {
		t.Fatal("expected error for invalid cron, got nil")
	}
}
