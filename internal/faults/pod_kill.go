package faults

// pod_kill.go — PodKillInjector deletes target pods to simulate abrupt
// process/node failure. Deletion is irreversible, so Rollback is a no-op;
// the workload controller (Deployment/StatefulSet) is responsible for
// rescheduling replacements, which is exactly the recovery path under test.

import (
	"context"
	"fmt"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	chaosv1alpha1 "github.com/sharatvikas/chaos-engineering-operator/api/v1alpha1"
)

// PodKillInjector implements Injector for the PodKill fault.
type PodKillInjector struct {
	deps Deps
}

func (i *PodKillInjector) Type() chaosv1alpha1.FaultType { return chaosv1alpha1.FaultPodKill }

func (i *PodKillInjector) Inject(ctx context.Context, req InjectRequest) (InjectResult, error) {
	logger := log.FromContext(ctx)

	grace := int64(0)
	if p := req.Experiment.Spec.Fault.PodKill; p != nil && p.GracePeriodSeconds != nil {
		grace = *p.GracePeriodSeconds
	}

	result := InjectResult{}
	for idx := range req.Targets {
		pod := &req.Targets[idx]
		if err := i.deps.Client.Delete(ctx, pod, &client.DeleteOptions{
			GracePeriodSeconds: &grace,
		}); err != nil {
			return result, fmt.Errorf("kill pod %s: %w", pod.Name, err)
		}
		result.AffectedPods = append(result.AffectedPods, pod.Name)
		logger.Info("pod killed", "pod", pod.Name, "namespace", pod.Namespace, "gracePeriodSeconds", grace)
		// Stagger deletions slightly so a workload controller can react
		// between kills rather than seeing them all at once.
		time.Sleep(500 * time.Millisecond)
	}
	return result, nil
}

// Rollback is a no-op: killed pods cannot be un-killed.
func (i *PodKillInjector) Rollback(_ context.Context, _ RollbackRequest) error { return nil }

func (i *PodKillInjector) Describe(exp *chaosv1alpha1.ChaosExperiment) string {
	grace := int64(0)
	if p := exp.Spec.Fault.PodKill; p != nil && p.GracePeriodSeconds != nil {
		grace = *p.GracePeriodSeconds
	}
	return fmt.Sprintf(
		"Delete %d%% of pods matching %v (grace period %ds). Workload controller reschedules replacements.",
		blastRadius(exp), exp.Spec.Target.Selector.MatchLabels, grace,
	)
}

// blastRadius returns the configured percentage, defaulting to 25.
func blastRadius(exp *chaosv1alpha1.ChaosExperiment) int {
	if exp.Spec.Target.Percentage > 0 {
		return exp.Spec.Target.Percentage
	}
	return 25
}
