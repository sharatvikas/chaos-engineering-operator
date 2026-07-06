package faults

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	chaosv1alpha1 "github.com/sharatvikas/chaos-engineering-operator/api/v1alpha1"
)

func testScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = chaosv1alpha1.AddToScheme(s)
	_ = corev1.AddToScheme(s)
	return s
}

func newFakeClient(objs ...client.Object) client.Client {
	return fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(objs...).Build()
}

func experiment(faultType chaosv1alpha1.FaultType) *chaosv1alpha1.ChaosExperiment {
	return &chaosv1alpha1.ChaosExperiment{
		ObjectMeta: metav1.ObjectMeta{Name: "exp", Namespace: "team-a"},
		Spec: chaosv1alpha1.ChaosExperimentSpec{
			Target: chaosv1alpha1.TargetSpec{
				Namespace:  "team-a",
				Selector:   metav1.LabelSelector{MatchLabels: map[string]string{"app": "svc"}},
				Percentage: 100,
				Mode:       "All",
			},
			Fault:    chaosv1alpha1.FaultSpec{Type: faultType},
			Duration: "5m",
		},
	}
}

func runningPod(name string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "team-a", Labels: map[string]string{"app": "svc"}},
		Spec:       corev1.PodSpec{NodeName: "node-1", Containers: []corev1.Container{{Name: "server"}, {Name: "sidecar"}}},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.1.2.3"},
	}
}

// ─── Registry ────────────────────────────────────────────────────────────────

func TestRegistryCoversAllInjectableFaults(t *testing.T) {
	reg := NewRegistry(Deps{Client: newFakeClient()})
	want := []chaosv1alpha1.FaultType{
		chaosv1alpha1.FaultPodKill,
		chaosv1alpha1.FaultContainerKill,
		chaosv1alpha1.FaultNetworkDelay,
		chaosv1alpha1.FaultNetworkPartition,
		chaosv1alpha1.FaultCPUStress,
		chaosv1alpha1.FaultMemoryStress,
		chaosv1alpha1.FaultDNSFailure,
	}
	for _, ft := range want {
		inj, err := reg.Get(ft)
		if err != nil {
			t.Errorf("registry missing injector for %s: %v", ft, err)
			continue
		}
		if inj.Type() != ft {
			t.Errorf("injector for %s reports Type()=%s", ft, inj.Type())
		}
		if inj.Describe(experiment(ft)) == "" {
			t.Errorf("injector for %s has empty Describe()", ft)
		}
	}
	if _, err := reg.Get("Bogus"); err == nil {
		t.Error("expected error for unregistered fault type")
	}
}

// ─── Target selection ──────────────────────────────────────────────────────

func TestSelectTargetsExcludesManagedAndNonRunning(t *testing.T) {
	running := runningPod("svc-1")
	pending := runningPod("svc-2")
	pending.Status.Phase = corev1.PodPending
	managed := runningPod("chaos-artifact")
	managed.Labels[LabelManaged] = "true"

	e := NewExecutor(newFakeClient(running, pending, managed))
	targets, total, err := e.SelectTargetsWithTotal(context.Background(), experiment(chaosv1alpha1.FaultPodKill))
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if total != 1 {
		t.Errorf("expected 1 running candidate, got %d", total)
	}
	if len(targets) != 1 || targets[0].Name != "svc-1" {
		t.Errorf("expected only svc-1 targeted, got %v", podNamesOf(targets))
	}
}

func TestSelectTargetsPercentage(t *testing.T) {
	var objs []client.Object
	for _, n := range []string{"p1", "p2", "p3", "p4"} {
		objs = append(objs, runningPod(n))
	}
	e := NewExecutor(newFakeClient(objs...))
	exp := experiment(chaosv1alpha1.FaultPodKill)
	exp.Spec.Target.Mode = "Random"
	exp.Spec.Target.Percentage = 50

	targets, total, err := e.SelectTargetsWithTotal(context.Background(), exp)
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if total != 4 {
		t.Errorf("expected total 4, got %d", total)
	}
	if len(targets) != 2 {
		t.Errorf("expected 50%% of 4 = 2 targets, got %d", len(targets))
	}
}

// ─── PodKill ─────────────────────────────────────────────────────────────────

func TestPodKillInjectDeletesPods(t *testing.T) {
	pod := runningPod("svc-1")
	c := newFakeClient(pod)
	e := NewExecutor(c)

	count, err := e.Start(context.Background(), experiment(chaosv1alpha1.FaultPodKill))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 pod affected, got %d", count)
	}

	var got corev1.Pod
	err = c.Get(context.Background(), client.ObjectKey{Name: "svc-1", Namespace: "team-a"}, &got)
	if err == nil {
		t.Error("expected pod to be deleted")
	}
}

func TestStartNoTargetsErrors(t *testing.T) {
	e := NewExecutor(newFakeClient())
	_, err := e.Start(context.Background(), experiment(chaosv1alpha1.FaultPodKill))
	if err == nil {
		t.Error("expected error when no pods match")
	}
}

// ─── Sidecar faults create + roll back scoped artifacts ─────────────────────

func TestCPUStressCreatesAndRollsBackScopedArtifacts(t *testing.T) {
	pod := runningPod("svc-1")
	c := newFakeClient(pod)
	e := NewExecutor(c)
	exp := experiment(chaosv1alpha1.FaultCPUStress)
	exp.Spec.Fault.CPUStress = &chaosv1alpha1.CPUStressParams{Cores: 1, Load: 50}

	if _, err := e.Start(context.Background(), exp); err != nil {
		t.Fatalf("start: %v", err)
	}

	// A stress artifact labelled to this experiment must exist.
	var pods corev1.PodList
	if err := c.List(context.Background(), &pods,
		client.MatchingLabels{LabelManaged: "true", LabelExperiment: "exp"}); err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(pods.Items) != 1 {
		t.Fatalf("expected 1 chaos artifact, got %d", len(pods.Items))
	}

	// Rollback removes it.
	if err := e.Stop(context.Background(), exp); err != nil {
		t.Fatalf("stop: %v", err)
	}
	var after corev1.PodList
	_ = c.List(context.Background(), &after, client.MatchingLabels{LabelManaged: "true", LabelExperiment: "exp"})
	if len(after.Items) != 0 {
		t.Errorf("expected artifacts removed after rollback, %d remain", len(after.Items))
	}
}

func TestRollbackDoesNotTouchOtherExperiments(t *testing.T) {
	// Two experiments target disjoint pods so their artifacts have distinct
	// names; rollback must still be scoped purely by the experiment label.
	podA := runningPod("svc-a")
	podA.Labels["app"] = "svc-a"
	podB := runningPod("svc-b")
	podB.Labels["app"] = "svc-b"
	c := newFakeClient(podA, podB)
	e := NewExecutor(c)

	expA := experiment(chaosv1alpha1.FaultCPUStress)
	expA.Name = "exp-a"
	expA.Spec.Target.Selector.MatchLabels = map[string]string{"app": "svc-a"}
	expA.Spec.Fault.CPUStress = &chaosv1alpha1.CPUStressParams{Cores: 1, Load: 50}
	expB := experiment(chaosv1alpha1.FaultCPUStress)
	expB.Name = "exp-b"
	expB.Spec.Target.Selector.MatchLabels = map[string]string{"app": "svc-b"}
	expB.Spec.Fault.CPUStress = &chaosv1alpha1.CPUStressParams{Cores: 1, Load: 50}

	if _, err := e.Start(context.Background(), expA); err != nil {
		t.Fatalf("start A: %v", err)
	}
	if _, err := e.Start(context.Background(), expB); err != nil {
		t.Fatalf("start B: %v", err)
	}

	// Rolling back A must leave B's artifact intact.
	if err := e.Stop(context.Background(), expA); err != nil {
		t.Fatalf("stop A: %v", err)
	}
	var pods corev1.PodList
	_ = c.List(context.Background(), &pods, client.MatchingLabels{LabelExperiment: "exp-b"})
	if len(pods.Items) != 1 {
		t.Errorf("expected exp-b artifact untouched, got %d", len(pods.Items))
	}
}

// ─── Same-pod / same-fault artifact-name uniqueness ─────────────────────────

// artifactNamesFor returns the names of the chaos artifacts labelled to exp.
func artifactNamesFor(t *testing.T, c client.Client, expName string) []string {
	t.Helper()
	var pods corev1.PodList
	if err := c.List(context.Background(), &pods,
		client.MatchingLabels{LabelManaged: "true", LabelExperiment: expName}); err != nil {
		t.Fatalf("list artifacts for %s: %v", expName, err)
	}
	names := make([]string, 0, len(pods.Items))
	for i := range pods.Items {
		names = append(names, pods.Items[i].Name)
	}
	return names
}

// TestSamePodSameFaultProducesDistinctArtifactNames is the regression test for
// the collision bug: two experiments injecting the SAME fault into the SAME
// target pod must create artifacts with DISTINCT, DNS-1123-valid names (so the
// second create no longer fails with "already exists"), and rollback must still
// resolve each experiment's own artifacts by label.
func TestSamePodSameFaultProducesDistinctArtifactNames(t *testing.T) {
	ctx := context.Background()
	pod := runningPod("svc-1")
	c := newFakeClient(pod)
	e := NewExecutor(c)

	expA := experiment(chaosv1alpha1.FaultCPUStress)
	expA.Name = "exp-alpha"
	expA.Spec.Fault.CPUStress = &chaosv1alpha1.CPUStressParams{Cores: 1, Load: 50}
	expB := experiment(chaosv1alpha1.FaultCPUStress)
	expB.Name = "exp-bravo"
	expB.Spec.Fault.CPUStress = &chaosv1alpha1.CPUStressParams{Cores: 1, Load: 50}

	if _, err := e.Start(ctx, expA); err != nil {
		t.Fatalf("start exp-alpha: %v", err)
	}
	// Before the fix this failed: the artifact name derived from the target
	// only, so the second create collided with the first.
	if _, err := e.Start(ctx, expB); err != nil {
		t.Fatalf("second experiment on same pod+fault must not collide: %v", err)
	}

	namesA := artifactNamesFor(t, c, "exp-alpha")
	namesB := artifactNamesFor(t, c, "exp-bravo")
	if len(namesA) != 1 || len(namesB) != 1 {
		t.Fatalf("expected 1 artifact per experiment, got A=%v B=%v", namesA, namesB)
	}
	if namesA[0] == namesB[0] {
		t.Fatalf("artifact names collide across experiments: %q", namesA[0])
	}
	for _, n := range append(namesA, namesB...) {
		if len(n) > 63 {
			t.Errorf("artifact name %q exceeds 63 chars (%d)", n, len(n))
		}
		if errs := validation.IsDNS1123Label(n); len(errs) > 0 {
			t.Errorf("artifact name %q is not a valid DNS-1123 label: %v", n, errs)
		}
	}

	// Rolling back exp-alpha must remove only its own artifact; exp-bravo's
	// artifact (same fault, same target) must survive — rollback is label-scoped.
	if err := e.Stop(ctx, expA); err != nil {
		t.Fatalf("stop exp-alpha: %v", err)
	}
	if remaining := artifactNamesFor(t, c, "exp-alpha"); len(remaining) != 0 {
		t.Errorf("expected exp-alpha artifacts removed, got %v", remaining)
	}
	if remaining := artifactNamesFor(t, c, "exp-bravo"); len(remaining) != 1 {
		t.Errorf("expected exp-bravo artifact untouched, got %v", remaining)
	}
	if err := e.Stop(ctx, expB); err != nil {
		t.Fatalf("stop exp-bravo: %v", err)
	}
	if remaining := artifactNamesFor(t, c, "exp-bravo"); len(remaining) != 0 {
		t.Errorf("expected exp-bravo artifacts removed after its own rollback, got %v", remaining)
	}
}

// TestChaosArtifactNameStaysWithinLimits verifies the name stays a valid
// DNS-1123 label <=63 chars even with a maximal-length target pod name, and
// that it remains deterministic and distinct per experiment.
func TestChaosArtifactNameStaysWithinLimits(t *testing.T) {
	longTarget := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		// 63-char pod name (the K8s maximum for a pod).
		Name:      "workload-with-a-very-long-generated-hash-suffix-abcdefghijklmn0",
		Namespace: "team-a",
	}}
	expA := experiment(chaosv1alpha1.FaultNetworkDelay)
	expA.Name = "experiment-with-a-fairly-long-descriptive-name-alpha"
	expB := experiment(chaosv1alpha1.FaultNetworkDelay)
	expB.Name = "experiment-with-a-fairly-long-descriptive-name-bravo"

	nameA := chaosArtifactName("chaos-net-delay", expA, longTarget)
	nameB := chaosArtifactName("chaos-net-delay", expB, longTarget)

	for _, n := range []string{nameA, nameB} {
		if len(n) > 63 {
			t.Errorf("name %q exceeds 63 chars (%d)", n, len(n))
		}
		if errs := validation.IsDNS1123Label(n); len(errs) > 0 {
			t.Errorf("name %q not a valid DNS-1123 label: %v", n, errs)
		}
	}
	if nameA == nameB {
		t.Errorf("distinct experiments produced identical names: %q", nameA)
	}
	// Deterministic: same inputs → same name (rollback lookups stay stable).
	if again := chaosArtifactName("chaos-net-delay", expA, longTarget); again != nameA {
		t.Errorf("name not deterministic: %q vs %q", nameA, again)
	}
}

// ─── Pure helpers ────────────────────────────────────────────────────────────

func TestBuildPartitionRules(t *testing.T) {
	// Full isolation, both directions → egress + ingress DROP, no peer clause.
	inject, cleanup := buildPartitionRules("10.1.2.3", &chaosv1alpha1.NetworkPartitionParams{Direction: "Both"})
	if !strings.Contains(inject, "-I FORWARD -s 10.1.2.3 -j DROP") {
		t.Errorf("expected egress DROP rule, got: %s", inject)
	}
	if !strings.Contains(inject, "-I FORWARD -d 10.1.2.3 -j DROP") {
		t.Errorf("expected ingress DROP rule, got: %s", inject)
	}
	if !strings.Contains(cleanup, "-D FORWARD") {
		t.Errorf("expected cleanup delete rules, got: %s", cleanup)
	}

	// Egress only, restricted to a peer CIDR.
	inject, _ = buildPartitionRules("10.1.2.3", &chaosv1alpha1.NetworkPartitionParams{
		Direction: "Egress", PeerCIDRs: []string{"10.20.0.0/16"},
	})
	if !strings.Contains(inject, "-d 10.20.0.0/16") {
		t.Errorf("expected peer CIDR clause, got: %s", inject)
	}
	if strings.Contains(inject, "FORWARD -d 10.1.2.3") {
		t.Errorf("egress-only must not add ingress rule, got: %s", inject)
	}
}

func TestDurationSeconds(t *testing.T) {
	cases := map[string]int{"5m": 300, "30s": 30, "1h": 3600, "": 300, "garbage": 300}
	for in, want := range cases {
		if got := durationSeconds(in); got != want {
			t.Errorf("durationSeconds(%q) = %d, want %d", in, got, want)
		}
	}
}

func TestResolveContainer(t *testing.T) {
	pod := runningPod("svc-1") // containers: server, sidecar
	if got := resolveContainer(pod, ""); got != "server" {
		t.Errorf("empty name should pick first container, got %q", got)
	}
	if got := resolveContainer(pod, "sidecar"); got != "sidecar" {
		t.Errorf("named container should be selected, got %q", got)
	}
	if got := resolveContainer(pod, "missing"); got != "server" {
		t.Errorf("missing container should fall back to first, got %q", got)
	}
}

func TestContainerKillRequiresExecDeps(t *testing.T) {
	// No REST config / clientset → must error clearly, not silently no-op.
	pod := runningPod("svc-1")
	e := NewExecutor(newFakeClient(pod))
	_, err := e.Start(context.Background(), experiment(chaosv1alpha1.FaultContainerKill))
	if err == nil || !strings.Contains(err.Error(), "REST config") {
		t.Errorf("expected REST-config error for ContainerKill without exec deps, got %v", err)
	}
}

func podNamesOf(pods []corev1.Pod) []string {
	out := make([]string, 0, len(pods))
	for _, p := range pods {
		out = append(out, p.Name)
	}
	return out
}
