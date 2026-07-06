# ChaosExperiment Walkthrough

This walkthrough runs a full experiment end to end — from installing the CRD to
watching the operator halt and roll back a fault when an SLO gate trips — and
explains what the operator does at each phase.

## Prerequisites

- A Kubernetes cluster (1.28+) and `kubectl` context pointing at it.
- The operator running in-cluster (see the Helm chart under `helm/`) or locally
  against the cluster:
  ```bash
  go run ./cmd/operator --prometheus-url http://prometheus.monitoring:9090
  ```
- A Prometheus reachable at `--prometheus-url` if you use SLO safety gates.
  Static preflight guards (namespace denylist, min-healthy-replicas) work
  without Prometheus.

## 1. Install the CRD

```bash
kubectl apply -f config/crd/chaosexperiment.yaml
kubectl get crd chaosexperiments.chaos.sharatvikas.io
```

## 2. Dry-run first (no blast radius)

Every experiment supports `spec.dryRun: true`. In dry-run the operator runs the
real preflight guards and Prometheus gates, resolves the exact target pods, and
writes a simulation report to `status.dryRunReport` — **without** injecting
anything.

```bash
# Copy a sample and flip dryRun to true
kubectl apply -f config/samples/pod-kill-experiment.yaml
kubectl patch chaos recommendation-service-pod-kill -n staging \
  --type=merge -p '{"spec":{"dryRun":true}}'

kubectl get chaos recommendation-service-pod-kill -n staging \
  -o jsonpath='{.status.dryRunReport}'
```

The report tells you which pods *would* be hit, the blast-radius percentage, the
result of each safety gate, and a human-readable impact estimate.

## 3. Run a real experiment

The pod-kill sample kills 50% of `recommendation-service` pods but is guarded by
`minHealthyReplicas: 2`, so the operator refuses to run if fewer than two pods
would be left healthy.

```bash
kubectl apply -f config/samples/pod-kill-experiment.yaml
kubectl get chaos recommendation-service-pod-kill -n staging -w
```

You will see it move through the phases:

| Phase | What happens |
|-------|--------------|
| `Pending` | Experiment accepted, queued for preflight. |
| `PreflightCheck` | Namespace denylist → min-healthy guard → Prometheus SLO gates. Any failure ⇒ `Aborted`. |
| `Running` | Fault injected. SLO gates are re-evaluated on every requeue. |
| `Cooldown` | Duration elapsed; fault rolled back; waiting out `spec.cooldown`. |
| `Completed` | Terminal success. |
| `Aborted` | A gate blocked the start, or a gate breached mid-run and triggered auto-rollback. `status.abortReason` explains why. |

Inspect the outcome:

```bash
kubectl describe chaos recommendation-service-pod-kill -n staging
```

## 4. Watch a safety gate halt the experiment

Apply an experiment with a tight SLO gate against a service you can degrade
(e.g. the network-delay sample against a latency-sensitive service):

```bash
kubectl apply -f config/samples/delay-experiment.yaml
```

While it is `Running`, the controller re-queries the `SLOBurnRate` and
`ErrorRate` gates. If the burn rate crosses the threshold, the operator:

1. Logs `mid-experiment gate tripped — auto-rolling back`.
2. Calls `Injector.Rollback`, deleting every artifact labelled
   `chaos.sharatvikas.io/experiment=<name>` (the tc/netem or iptables sidecars).
3. Sets phase `Aborted` with `status.abortReason`.

```bash
kubectl get chaos payment-service-network-delay -n production \
  -o jsonpath='{.status.abortReason}'
```

## 5. Verify rollback left nothing behind

Sidecar-based faults (network, DNS, stress) create labelled chaos pods. After
rollback none should remain for the experiment:

```bash
kubectl get pods -n production \
  -l chaos.sharatvikas.io/experiment=payment-service-network-delay
# (no resources found)
```

## Fault-specific notes

- **ContainerKill** exec's into the target container and signals PID 1. It needs
  the operator's in-cluster REST config (provided automatically by
  `cmd/operator`); it is not available when the controller is built with only a
  cache client.
- **NetworkDelay / NetworkPartition / DNSFailure** schedule a privileged
  `hostNetwork` sidecar onto the target's node. The node images
  (`ghcr.io/sharatvikas/chaos-tc`, `chaos-iptables`) must be pullable and the
  cluster must permit privileged pods with `NET_ADMIN`.
- **NetworkPartition** drops all traffic by default; set `peerCIDRs` to
  partition only from a specific subnet (e.g. the database) rather than fully
  isolating the pod.
- **CPUStress / MemoryStress** co-schedule a `stress-ng` pod on the target node.
  Set `minHealthyReplicas` to bound how many replicas you are willing to stress
  simultaneously.

## Cleaning up

```bash
kubectl delete chaos --all -n staging
kubectl delete chaos --all -n production
```

Because rollback is idempotent and scoped by experiment label, deleting a
`ChaosExperiment` mid-run is safe: the controller's next reconcile (or the
duration/cooldown transition) removes any remaining artifacts.
