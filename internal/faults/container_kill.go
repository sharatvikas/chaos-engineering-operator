package faults

// container_kill.go — ContainerKillInjector kills a single container inside a
// running pod without deleting the pod itself. It exec's into the target
// container and signals PID 1 (the container entrypoint), so the kubelet
// restarts just that container. This exercises restart handling, readiness
// re-gating, and warm-state loss — a finer-grained failure than PodKill.
//
// Exec requires a *rest.Config + clientset (SPDY streaming to the kubelet),
// which the controller-runtime cache client cannot provide. Injectors that
// need it degrade with a clear error when those deps are absent (e.g. unit
// tests with a fake client), rather than silently no-op'ing.

import (
	"bytes"
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/remotecommand"
	"sigs.k8s.io/controller-runtime/pkg/log"

	chaosv1alpha1 "github.com/sharatvikas/chaos-engineering-operator/api/v1alpha1"
)

// ContainerKillInjector implements Injector for the ContainerKill fault.
type ContainerKillInjector struct {
	deps Deps
}

func (i *ContainerKillInjector) Type() chaosv1alpha1.FaultType {
	return chaosv1alpha1.FaultContainerKill
}

func (i *ContainerKillInjector) Inject(ctx context.Context, req InjectRequest) (InjectResult, error) {
	logger := log.FromContext(ctx)

	if i.deps.RESTConfig == nil || i.deps.ClientSet == nil {
		return InjectResult{}, fmt.Errorf(
			"ContainerKill requires a REST config and clientset (exec streaming); " +
				"the operator must be built with NewExecutorWithDeps")
	}

	signal := "SIGKILL"
	containerName := ""
	if p := req.Experiment.Spec.Fault.ContainerKill; p != nil {
		if p.Signal != "" {
			signal = p.Signal
		}
		containerName = p.ContainerName
	}

	result := InjectResult{}
	for idx := range req.Targets {
		target := &req.Targets[idx]
		container := resolveContainer(target, containerName)
		if container == "" {
			return result, fmt.Errorf("pod %s has no containers to kill", target.Name)
		}

		if err := i.killContainer(ctx, target, container, signal); err != nil {
			return result, fmt.Errorf("kill container %s/%s: %w", target.Name, container, err)
		}
		result.AffectedPods = append(result.AffectedPods, target.Name)
		logger.Info("container killed", "pod", target.Name, "container", container, "signal", signal)
	}
	return result, nil
}

// Rollback is a no-op: the kubelet restarts the killed container automatically.
func (i *ContainerKillInjector) Rollback(_ context.Context, _ RollbackRequest) error { return nil }

func (i *ContainerKillInjector) Describe(exp *chaosv1alpha1.ChaosExperiment) string {
	signal := "SIGKILL"
	container := "first container"
	if p := exp.Spec.Fault.ContainerKill; p != nil {
		if p.Signal != "" {
			signal = p.Signal
		}
		if p.ContainerName != "" {
			container = "container " + p.ContainerName
		}
	}
	return fmt.Sprintf("Send %s to PID 1 of %s in %d%% of matching pods; kubelet restarts the container.",
		signal, container, blastRadius(exp))
}

// killContainer exec's `kill -<signal> 1` inside the target container.
func (i *ContainerKillInjector) killContainer(ctx context.Context, pod *corev1.Pod, container, signal string) error {
	// Map signal name to number for the POSIX `kill` builtin, which is the
	// most portable across minimal container images (busybox, distroless-shell).
	sigNum := "9"
	if signal == "SIGTERM" {
		sigNum = "15"
	}
	command := []string{"/bin/sh", "-c", fmt.Sprintf("kill -%s 1", sigNum)}

	execReq := i.deps.ClientSet.CoreV1().RESTClient().
		Post().
		Resource("pods").
		Name(pod.Name).
		Namespace(pod.Namespace).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: container,
			Command:   command,
			Stdin:     false,
			Stdout:    true,
			Stderr:    true,
			TTY:       false,
		}, scheme.ParameterCodec)

	executor, err := remotecommand.NewSPDYExecutor(i.deps.RESTConfig, "POST", execReq.URL())
	if err != nil {
		return fmt.Errorf("build exec stream: %w", err)
	}

	var stdout, stderr bytes.Buffer
	err = executor.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdout: &stdout,
		Stderr: &stderr,
	})
	// A successful `kill -9 1` frequently tears down the exec stream itself,
	// surfacing as a stream error even though the signal was delivered. Treat
	// the disappearance of PID 1 as success; only report genuine failures.
	if err != nil && !isExpectedKillStreamError(stderr.String()) {
		return fmt.Errorf("exec kill: %w (stderr: %s)", err, stderr.String())
	}
	return nil
}

// resolveContainer returns the container to target: the named one if it
// exists, otherwise the pod's first container.
func resolveContainer(pod *corev1.Pod, name string) string {
	if len(pod.Spec.Containers) == 0 {
		return ""
	}
	if name == "" {
		return pod.Spec.Containers[0].Name
	}
	for _, c := range pod.Spec.Containers {
		if c.Name == name {
			return name
		}
	}
	// Named container not found — fall back to first so the fault still lands.
	return pod.Spec.Containers[0].Name
}

// isExpectedKillStreamError reports whether an exec stream error is the
// benign consequence of PID 1 exiting mid-stream.
func isExpectedKillStreamError(stderr string) bool {
	// stress-free heuristic: an empty stderr on a kill means the entrypoint
	// went away before writing anything — the expected outcome.
	return bytes.TrimSpace([]byte(stderr)) == nil || len(stderr) == 0
}
