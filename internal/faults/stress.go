package faults

// stress.go — CPUStressInjector and MemoryStressInjector co-schedule a
// stress-ng pod onto the same node as each target so resource pressure is
// applied where it matters. The stress pods carry the standard chaos labels
// and are torn down by Rollback (delete-by-experiment-label).

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/controller-runtime/pkg/log"

	chaosv1alpha1 "github.com/sharatvikas/chaos-engineering-operator/api/v1alpha1"
)

const stressImage = "alexeiled/stress-ng:latest"

// CPUStressInjector implements Injector for the CPUStress fault.
type CPUStressInjector struct {
	deps Deps
}

func (i *CPUStressInjector) Type() chaosv1alpha1.FaultType { return chaosv1alpha1.FaultCPUStress }

func (i *CPUStressInjector) Inject(ctx context.Context, req InjectRequest) (InjectResult, error) {
	logger := log.FromContext(ctx)
	params := req.Experiment.Spec.Fault.CPUStress
	if params == nil {
		params = &chaosv1alpha1.CPUStressParams{Cores: 1, Load: 80}
	}

	result := InjectResult{}
	for idx := range req.Targets {
		target := &req.Targets[idx]
		stressPod := buildStressPod(
			req.Experiment,
			target,
			fmt.Sprintf("chaos-cpu-%s", shortName(target.Name)),
			"cpu-stress",
			[]string{
				"stress-ng",
				"--cpu", fmt.Sprintf("%d", params.Cores),
				"--cpu-load", fmt.Sprintf("%d", params.Load),
			},
		)
		if err := i.deps.Client.Create(ctx, stressPod); err != nil {
			return result, fmt.Errorf("create CPU stress pod for %s: %w", target.Name, err)
		}
		result.AffectedPods = append(result.AffectedPods, target.Name)
		result.Artifacts = append(result.Artifacts, stressPod.Name)
		logger.Info("cpu stress injected", "target", target.Name, "cores", params.Cores, "load", params.Load)
	}
	return result, nil
}

func (i *CPUStressInjector) Rollback(ctx context.Context, req RollbackRequest) error {
	return rollbackManagedPods(ctx, i.deps.Client, req.Experiment)
}

func (i *CPUStressInjector) Describe(exp *chaosv1alpha1.ChaosExperiment) string {
	p := exp.Spec.Fault.CPUStress
	if p == nil {
		return "Apply default CPU stress (1 core @ 80%) to targeted pods' nodes."
	}
	return fmt.Sprintf("Stress %d core(s) at %d%% load co-scheduled with each of %d%% of matching pods.",
		p.Cores, p.Load, blastRadius(exp))
}

// MemoryStressInjector implements Injector for the MemoryStress fault.
type MemoryStressInjector struct {
	deps Deps
}

func (i *MemoryStressInjector) Type() chaosv1alpha1.FaultType { return chaosv1alpha1.FaultMemoryStress }

func (i *MemoryStressInjector) Inject(ctx context.Context, req InjectRequest) (InjectResult, error) {
	logger := log.FromContext(ctx)
	params := req.Experiment.Spec.Fault.MemoryStress
	if params == nil {
		params = &chaosv1alpha1.MemoryStressParams{FillPercentage: 80}
	}

	vmBytes := fmt.Sprintf("%dM", params.MemoryMB)
	if params.FillPercentage > 0 {
		vmBytes = fmt.Sprintf("%d%%", params.FillPercentage)
	}

	result := InjectResult{}
	for idx := range req.Targets {
		target := &req.Targets[idx]
		stressPod := buildStressPod(
			req.Experiment,
			target,
			fmt.Sprintf("chaos-mem-%s", shortName(target.Name)),
			"memory-stress",
			[]string{"stress-ng", "--vm", "1", "--vm-bytes", vmBytes},
		)
		if err := i.deps.Client.Create(ctx, stressPod); err != nil {
			return result, fmt.Errorf("create memory stress pod for %s: %w", target.Name, err)
		}
		result.AffectedPods = append(result.AffectedPods, target.Name)
		result.Artifacts = append(result.Artifacts, stressPod.Name)
		logger.Info("memory stress injected", "target", target.Name, "vmBytes", vmBytes)
	}
	return result, nil
}

func (i *MemoryStressInjector) Rollback(ctx context.Context, req RollbackRequest) error {
	return rollbackManagedPods(ctx, i.deps.Client, req.Experiment)
}

func (i *MemoryStressInjector) Describe(exp *chaosv1alpha1.ChaosExperiment) string {
	p := exp.Spec.Fault.MemoryStress
	if p == nil {
		return "Apply default memory pressure to targeted pods' nodes."
	}
	if p.FillPercentage > 0 {
		return fmt.Sprintf("Consume %d%% of available memory co-scheduled with %d%% of matching pods (OOMKill risk).",
			p.FillPercentage, blastRadius(exp))
	}
	return fmt.Sprintf("Consume %dMB memory co-scheduled with %d%% of matching pods (OOMKill risk).",
		p.MemoryMB, blastRadius(exp))
}

// buildStressPod creates a stress-ng Pod pinned to the target's node.
func buildStressPod(exp *chaosv1alpha1.ChaosExperiment, target *corev1.Pod, name, faultType string, args []string) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: buildChaosPodMeta(name, target.Namespace, managedLabels(exp, faultType, target.Name)),
		Spec: corev1.PodSpec{
			NodeName:      target.Spec.NodeName,
			RestartPolicy: corev1.RestartPolicyNever,
			Tolerations:   []corev1.Toleration{{Operator: corev1.TolerationOpExists}},
			Containers: []corev1.Container{
				{
					Name:  "stress",
					Image: stressImage,
					Args:  args,
					Resources: corev1.ResourceRequirements{
						Limits: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("2"),
							corev1.ResourceMemory: resource.MustParse("2Gi"),
						},
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("100m"),
							corev1.ResourceMemory: resource.MustParse("64Mi"),
						},
					},
				},
			},
		},
	}
	return pod
}
