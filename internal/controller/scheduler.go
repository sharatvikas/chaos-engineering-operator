// scheduler.go — ChaosExperiment schedule controller.
//
// When a ChaosExperiment has spec.schedule set, the reconciler uses this
// scheduler to compute the next run time and create child ChaosExperiment
// CRs (or trigger the run) at the scheduled time.
//
// Schedule syntax: standard 5-field cron (minute hour dom month dow)
// Example:
//   schedule:
//     cron: "0 3 * * SAT"        # Every Saturday at 03:00 UTC
//     timezone: "America/Denver"  # Optional timezone
//     suspend: false             # Set true to pause without deleting
package controller

import (
	"context"
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"

	chaosv1alpha1 "github.com/sharatvikas/chaos-engineering-operator/api/v1alpha1"
)

// ScheduleResult tells the reconciler what to do next.
type ScheduleResult struct {
	// ShouldRunNow is true when the current time is within the run window
	ShouldRunNow bool
	// NextRun is when the next scheduled run should start
	NextRun time.Time
	// RequeuAfter is the duration the reconciler should wait before re-checking
	RequeueAfter time.Duration
	// Suspended indicates the schedule is paused — skip without error
	Suspended bool
}

// EvaluateSchedule checks the experiment's schedule and returns a ScheduleResult.
// It is called at the start of every reconcile loop when spec.schedule is set.
func EvaluateSchedule(exp *chaosv1alpha1.ChaosExperiment, now time.Time) (*ScheduleResult, error) {
	sched := exp.Spec.Schedule
	if sched == nil {
		return nil, fmt.Errorf("EvaluateSchedule called on experiment without schedule")
	}

	// Handle suspend
	if sched.Suspend {
		return &ScheduleResult{
			Suspended:    true,
			RequeueAfter: 5 * time.Minute,
		}, nil
	}

	// Parse timezone
	loc := time.UTC
	if sched.Timezone != "" {
		var err error
		loc, err = time.LoadLocation(sched.Timezone)
		if err != nil {
			return nil, fmt.Errorf("invalid timezone %q: %w", sched.Timezone, err)
		}
	}

	// Parse cron expression
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	schedule, err := parser.Parse(sched.Cron)
	if err != nil {
		return nil, fmt.Errorf("invalid cron expression %q: %w", sched.Cron, err)
	}

	nowInLoc := now.In(loc)

	// Determine the last scheduled time
	lastScheduled := exp.Status.LastScheduledTime
	var lastTime time.Time
	if lastScheduled != nil {
		lastTime = lastScheduled.Time
	} else {
		// First run: look back 1 hour to catch missed schedules
		lastTime = now.Add(-1 * time.Hour)
	}

	// Find the most recent scheduled time before now
	nextAfterLast := schedule.Next(lastTime)
	if nextAfterLast.IsZero() {
		return &ScheduleResult{
			ShouldRunNow: false,
			NextRun:      schedule.Next(now),
			RequeueAfter: time.Until(schedule.Next(now)) + time.Minute,
		}, nil
	}

	// Is this scheduled time within the past runWindow (default 5 minutes)?
	runWindow := 5 * time.Minute
	if nextAfterLast.Before(now) && now.Sub(nextAfterLast) <= runWindow {
		// We are within the run window — trigger now
		nextRun := schedule.Next(now)
		return &ScheduleResult{
			ShouldRunNow: true,
			NextRun:      nextRun,
			RequeueAfter: time.Until(nextRun) + time.Minute,
		}, nil
	}

	// Compute next run
	var nextRun time.Time
	if nextAfterLast.After(now) {
		nextRun = nextAfterLast
	} else {
		nextRun = schedule.Next(now)
	}

	_ = nowInLoc // used for timezone-aware display in logs
	return &ScheduleResult{
		ShouldRunNow: false,
		NextRun:      nextRun,
		RequeueAfter: time.Until(nextRun) + 30*time.Second,
	}, nil
}

// HandleScheduledRun processes a scheduled trigger: sets LastScheduledTime
// on the experiment status and returns the mutation needed.
func HandleScheduledRun(
	ctx context.Context,
	exp *chaosv1alpha1.ChaosExperiment,
	now time.Time,
) *chaosv1alpha1.ChaosExperiment {
	logger := log.FromContext(ctx)

	updated := exp.DeepCopy()
	nowMeta := metav1.NewTime(now)
	updated.Status.LastScheduledTime = &nowMeta
	updated.Status.Phase = chaosv1alpha1.PhasePending
	// Message is not a field on ChaosExperimentStatus; log it instead
	logger.Info("scheduled run triggered message",
		"message", fmt.Sprintf("Scheduled run triggered at %s", now.Format(time.RFC3339)),
	)

	logger.Info("scheduled run triggered",
		"experiment", exp.Name,
		"time", now.Format(time.RFC3339),
	)

	return updated
}

// NextScheduledRuns returns the next N scheduled run times for display purposes.
func NextScheduledRuns(exp *chaosv1alpha1.ChaosExperiment, n int) ([]time.Time, error) {
	if exp.Spec.Schedule == nil {
		return nil, nil
	}

	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	schedule, err := parser.Parse(exp.Spec.Schedule.Cron)
	if err != nil {
		return nil, fmt.Errorf("parse cron: %w", err)
	}

	loc := time.UTC
	if tz := exp.Spec.Schedule.Timezone; tz != "" {
		if l, err := time.LoadLocation(tz); err == nil {
			loc = l
		}
	}

	runs := make([]time.Time, 0, n)
	t := time.Now().In(loc)
	for i := 0; i < n; i++ {
		t = schedule.Next(t)
		if t.IsZero() {
			break
		}
		runs = append(runs, t)
	}
	return runs, nil
}

// ValidateSchedule checks that a cron expression is valid and not too aggressive.
// Returns an error if the interval is shorter than 5 minutes (safety guard).
func ValidateSchedule(cronExpr string) error {
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	schedule, err := parser.Parse(cronExpr)
	if err != nil {
		return fmt.Errorf("invalid cron expression: %w", err)
	}

	// Check minimum interval — chaos experiments need at least 5 minutes between runs
	now := time.Now()
	first := schedule.Next(now)
	second := schedule.Next(first)
	if !second.IsZero() && second.Sub(first) < 5*time.Minute {
		return fmt.Errorf("schedule interval %v is too short — minimum is 5 minutes for safety", second.Sub(first))
	}

	return nil
}
