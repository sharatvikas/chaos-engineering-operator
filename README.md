# chaos-engineering-operator

> A **Kubernetes operator** for running controlled chaos experiments on your cluster — pod failures, network partitions, CPU/memory pressure, and DNS failures — with built-in safety gates, SLO monitoring, and automatic rollback.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Go 1.22+](https://img.shields.io/badge/go-1.22+-00ADD8.svg)](https://go.dev)
[![Kubernetes](https://img.shields.io/badge/kubernetes-1.28+-326CE5.svg)](https://kubernetes.io)
[![CRD](https://img.shields.io/badge/CRD-v1-blue.svg)](config/crd)

---

## Philosophy

Chaos engineering is not about breaking things randomly. It's about **validating hypotheses** about your system's resilience under known failure conditions, in a controlled, observable, and stoppable way.

This operator makes chaos experiments:
- **Declarative** — define experiments as Kubernetes CRs
- **Safe** — automatic abort if SLO burn rate exceeds threshold
- **Auditable** — every experiment is logged with full timeline
- **Gradual** — progressive blast radius (1 pod → 10% → 50%)

---

## Custom Resources

### ChaosExperiment

```yaml
apiVersion: chaos.sharatvikas.io/v1alpha1
kind: ChaosExperiment
metadata:
  name: kill-payment-pods
  namespace: production
spec:
  target:
    selector:
      matchLabels:
        app: payments-api
    namespace: production
    percentage: 30            # Affect 30% of matching pods
    mode: Random              # Random | All | Fixed
    minHealthyReplicas: 2     # Preflight guard: never leave < 2 running

  fault:
    type: PodKill             # See the Fault Catalog below for all types
    podKill:
      gracePeriodSeconds: 0   # Hard kill

  duration: 10m
  cooldown: 5m

  safetyGates:
    - type: SLOBurnRate
      service: payments-api
      threshold: "5x"         # Abort if burn rate > 5x normal
    - type: ErrorRate
      service: payments-api
      threshold: "10"         # Abort if error rate > 10%

  schedule:
    cron: "0 14 * * 2"        # Every Tuesday at 2pm
    timezone: "America/Denver"

  notifications:
    slack: "#chaos-experiments"
    pagerduty: false          # Don't page on-call during planned chaos
```

Runnable, fully-annotated manifests for every fault type live in
[`config/samples/`](config/samples). A step-by-step walkthrough is in
[`docs/WALKTHROUGH.md`](docs/WALKTHROUGH.md).

---

## Fault Catalog

Every fault implements a single `Injector` interface (`Inject` / `Rollback` /
`Describe`) and is wired into the controller through a registry, so the fault
set is fully data-driven.

| Fault | Params key | Injection mechanism | Rollback |
|-------|------------|---------------------|----------|
| `PodKill` | `podKill` | Deletes pods with a configurable grace period | None (workload controller reschedules) |
| `ContainerKill` | `containerKill` | Exec's `kill -<signal> 1` into a named container (SPDY stream to kubelet) | None (kubelet restarts container) |
| `NetworkDelay` | `networkDelay` | Privileged node sidecar applies `tc`/`netem` latency + jitter to the pod's egress | Deletes sidecar; `tc qdisc del` on preStop |
| `NetworkPartition` | `networkPartition` | Privileged node sidecar `iptables -j DROP` on FORWARD (egress/ingress, optional peer CIDRs) | Deletes sidecar; `iptables -D` on preStop |
| `CPUStress` | `cpuStress` | `stress-ng` pod co-scheduled on the target's node | Deletes stress pod |
| `MemoryStress` | `memoryStress` | `stress-ng --vm` pod co-scheduled on the target's node | Deletes stress pod |
| `DNSFailure` | — | Privileged node sidecar drops port-53 (UDP+TCP) from the pod IP | Deletes sidecar; `iptables -D` on preStop |

`DiskFill` and `NodeDrain` are reserved in the API enum but not yet
implemented; the registry returns a clear error if they are requested.

Every artifact the operator creates is labelled with
`chaos.sharatvikas.io/experiment=<name>`, so rollback deletes **only** the pods
that experiment created — concurrent experiments never disturb each other.

---

## Safety Architecture

Safety is layered: cheap static guards run first (they never call out to
Prometheus, so they never "fail open"), then live SLO gates gate both the
start and the entire duration of the experiment.

```
Experiment Starts (Pending → PreflightCheck)
       │
       ▼
┌───────────────────────────┐
│ Static preflight guards    │
│  • namespace denylist      │  ← kube-system, chaos-system, … are protected
│  • min-healthy-replicas    │  ← block if fault would drop below N healthy
└────────────┬──────────────┘
             │ pass
             ▼
┌───────────────────────────┐
│ Prometheus SLO gates       │  ← SLOBurnRate / ErrorRate / Availability
│  (blocks start if breached)│     + arbitrary PromQL gates
└────────────┬──────────────┘
             │ pass
             ▼
┌───────────────────────────┐
│ Fault injection (Running)  │  ← target selection: label selector × %  × mode
└────────────┬──────────────┘
             │
             ├── SLO gates re-polled every requeue while Running
             │      └── breached? → HALT + AUTO-ROLLBACK → Aborted
             │
             ▼
┌───────────────────────────┐
│ Duration elapsed → Rollback│  ← Injector.Rollback removes all artifacts
└────────────┬──────────────┘
             │
             ▼
      Cooldown → Completed
```

- **Fail-safe on selection errors:** if preflight target selection errors while
  a `minHealthyReplicas` guard is set, the experiment is **blocked**, not run.
- **Fail-open on Prometheus errors:** if a Prometheus query itself errors
  (endpoint down), the SLO gate logs and continues rather than blocking all
  chaos on a monitoring outage — a deliberate, documented trade-off.

---

## Quick Start

### Install with Helm

```bash
helm repo add chaos-operator https://sharatvikas.github.io/chaos-engineering-operator/charts
helm install chaos-operator chaos-operator/chaos-engineering-operator \
  --namespace chaos-system --create-namespace \
  --set prometheus.url=http://prometheus.monitoring:9090 \
  --set slack.webhookUrl=$SLACK_WEBHOOK
```

### Run Your First Experiment

```bash
# Install the CRD
kubectl apply -f config/crd/chaosexperiment.yaml

# Validate targeting + safety gates without injecting (set spec.dryRun: true),
# then apply a fully-annotated sample experiment
kubectl apply -f config/samples/pod-kill-experiment.yaml

# Watch it move through its phases
kubectl get chaos recommendation-service-pod-kill -n staging -w

# Inspect the outcome (phase, targeted pods, abort reason, conditions)
kubectl describe chaos recommendation-service-pod-kill -n staging
```

Samples are provided for every fault type:

```bash
ls config/samples/
# container-kill-experiment.yaml   memory-stress-scheduled.yaml
# cpu-stress-experiment.yaml       network-partition-experiment.yaml
# delay-experiment.yaml            pod-kill-experiment.yaml
```

---

## Project Structure

```
chaos-engineering-operator/
├── api/
│   └── v1alpha1/
│       ├── chaosexperiment_types.go   # CRD types + fault params
│       └── zz_generated_deepcopy.go
├── cmd/
│   ├── operator/main.go               # controller-manager entrypoint
│   └── chaosctl/main.go               # CLI
├── internal/
│   ├── controller/
│   │   ├── chaosexperiment_controller.go  # phase state machine
│   │   ├── dryrun.go                       # dry-run simulation
│   │   └── scheduler.go                    # cron scheduling
│   ├── faults/
│   │   ├── injector.go        # Injector interface + Registry + rollback
│   │   ├── executor.go        # target selection + dispatch
│   │   ├── pod_kill.go        # PodKillInjector
│   │   ├── container_kill.go  # ContainerKillInjector (exec)
│   │   ├── network.go         # NetworkDelay + NetworkPartition injectors
│   │   ├── stress.go          # CPU + Memory stress injectors
│   │   └── dns.go             # DNSInjector
│   ├── safety/
│   │   ├── gates.go           # Prometheus SLO gates
│   │   └── preflight.go       # namespace denylist + min-healthy guards
│   └── reporting/report.go    # structured reports (ConfigMap + Slack)
├── config/
│   ├── crd/                   # CRD manifest
│   └── samples/               # one runnable ChaosExperiment per fault type
├── helm/                      # Helm chart
└── docs/WALKTHROUGH.md        # end-to-end example walkthrough
```

---

## Comparison with Chaos Mesh / Litmus

| Feature | chaos-engineering-operator | Chaos Mesh | Litmus |
|---------|---------------------------|------------|--------|
| SLO-aware safety gates | ✅ Native | ❌ | Partial |
| Progressive blast radius | ✅ | ❌ | ❌ |
| No daemon required | ✅ | ❌ (daemonset) | ❌ |
| Terraform-native schedule | ✅ | ❌ | ❌ |
| GameDay suite orchestration | ✅ | ❌ | ✅ |
| Lightweight (<50MB image) | ✅ | ❌ | ❌ |

---

## Roadmap

- [x] PodKill, ContainerKill, CPUStress, MemoryStress
- [x] NetworkDelay (tc/netem) + NetworkPartition (iptables) + DNSFailure
- [x] SLO burn rate / error rate / availability safety gates
- [x] Static preflight guards (namespace denylist, min-healthy-replicas)
- [x] Halt + auto-rollback on mid-experiment SLO breach
- [x] Dry-run simulation + cron scheduling
- [ ] DiskFill and NodeDrain faults
- [ ] ChaosSuite controller (GameDay orchestration)
- [ ] Web UI dashboard
- [ ] GameDay report PDF export

---

## License

MIT — see [LICENSE](LICENSE).
