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
    percentage: 30           # Kill 30% of pods

  fault:
    type: PodKill            # PodKill | NetworkDelay | CPUStress | MemoryStress | DNSFailure

  duration: 10m
  cooldown: 5m

  safetyGates:
    - type: SLOBurnRate
      service: payments-api
      threshold: 5x           # Abort if burn rate > 5x normal
    - type: ErrorRate
      endpoint: /health
      threshold: 10           # Abort if error rate > 10%

  schedule:
    cron: "0 14 * * 2"        # Every Tuesday at 2pm
    timezone: "America/Denver"

  notifications:
    slack: "#chaos-experiments"
    pagerduty: false          # Don't page on-call during planned chaos
```

### ChaosSuite

```yaml
apiVersion: chaos.sharatvikas.io/v1alpha1
kind: ChaosSuite
metadata:
  name: gameday-q2-2026
spec:
  experiments:
    - name: kill-payment-pods
    - name: slow-database-network
    - name: stress-search-cpu
  mode: Sequential            # Sequential | Parallel
  report:
    enabled: true
    destination: s3://my-chaos-reports/
```

---

## Supported Fault Types

| Fault | What it does | Platform |
|-------|-------------|----------|
| `PodKill` | Terminates pods matching selector | Any |
| `NetworkDelay` | Injects latency via tc/netem | Linux |
| `NetworkPartition` | Drops traffic between services via iptables | Linux |
| `CPUStress` | Burns CPU cycles in container | Any |
| `MemoryStress` | Allocates memory to trigger OOM conditions | Any |
| `DNSFailure` | Makes DNS resolution fail for target pods | Any |
| `DiskFill` | Fills ephemeral storage | Linux |
| `NodeDrain` | Cordons and drains a node (with undo) | Any |

---

## Safety Architecture

```
Experiment Starts
       │
       ▼
┌──────────────────┐
│  Pre-flight Check│  ← Verify SLOs are healthy before starting
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ Blast Radius Calc│  ← Never exceed configured % of replicas
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│  Fault Injection │
│  (progressive)   │  ← 1 pod → N% → target%
└────────┬─────────┘
         │
         ├── SLO Gate polling every 10s
         │      └── Burn rate > threshold? → ABORT + ROLLBACK
         │
         ▼
┌──────────────────┐
│  Auto Rollback   │  ← Restores all modified resources
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│  Experiment Report│ ← Markdown report + metrics timeline
└──────────────────┘
```

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
# Apply a simple pod kill experiment (dry-run first)
kubectl apply -f examples/pod-kill-basic.yaml --dry-run=server

# Run it for real
kubectl apply -f examples/pod-kill-basic.yaml

# Watch it in real time
kubectl get chaosexperiment kill-payment-pods -n production -w

# Get the report
kubectl describe chaosexperiment kill-payment-pods -n production
```

---

## Project Structure

```
chaos-engineering-operator/
├── api/
│   └── v1alpha1/
│       ├── chaosexperiment_types.go
│       └── chaossuite_types.go
├── internal/
│   ├── controller/
│   │   ├── chaosexperiment_controller.go
│   │   └── chaossuite_controller.go
│   ├── faults/
│   │   ├── pod_kill.go
│   │   ├── network_delay.go
│   │   ├── cpu_stress.go
│   │   └── memory_stress.go
│   ├── safety/
│   │   ├── gates.go           # SLO burn rate, error rate gates
│   │   └── rollback.go
│   └── reporting/
│       ├── markdown.go
│       └── s3.go
├── config/
│   ├── crd/                   # Generated CRD manifests
│   └── rbac/
├── charts/                    # Helm chart
├── examples/                  # Sample ChaosExperiment CRs
└── docs/
    ├── SAFETY.md
    └── FAULT_TYPES.md
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

- [x] PodKill, CPUStress, MemoryStress
- [x] SLO burn rate safety gate
- [x] Progressive blast radius
- [ ] NetworkDelay (tc/netem via init container)
- [ ] NodeDrain fault
- [ ] ChaosSuite controller
- [ ] Web UI dashboard
- [ ] Argo Workflows integration
- [ ] GameDay report PDF export

---

## License

MIT — see [LICENSE](LICENSE).
