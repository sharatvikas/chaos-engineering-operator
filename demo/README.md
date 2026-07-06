# Live Pod-Kill Chaos Demo (kind)

This directory contains a reproducible, end-to-end demo of the
chaos-engineering-operator running on a real `kind` cluster: it deploys the
controller, kills real pods of a target Deployment, watches the workload
self-heal, and shows the operator's two-layer safety system **refusing**
unsafe experiments.

`OUTPUT.md` in this directory has the actual captured commands + output from
the run described here.

## What it proves

| Capability | Evidence in `OUTPUT.md` |
|---|---|
| Controller deploys & elects leader | operator startup logs |
| CR phase state machine | `PreflightCheck -> Running -> Cooldown -> Completed` |
| Real fault injection | `pod killed` logs; 2 of 4 pods replaced |
| Self-healing | Deployment returns to 4/4 ready |
| Preflight guard: min-healthy-replicas | `guard-minhealthy-violation` -> `Aborted` |
| Preflight guard: namespace denylist | `guard-denylist-violation` -> `Aborted`, kube-dns untouched |
| Live Prometheus SLO gate | in-cluster Prometheus reachable (HTTP 200); `SLOBurnRate` gate evaluated |

## Files

- `target-deployment.yaml` — 4-replica nginx Deployment (the blast target)
- `podkill-experiment.yaml` — happy-path PodKill: 50%, minHealthyReplicas=2, SLOBurnRate gate
- `guard-minhealthy.yaml` — 100% kill with minHealthyReplicas=3 (blocked)
- `guard-denylist.yaml` — targets `kube-system` (blocked)

## Reproduce

```bash
# 1. Build + load the manager image
docker build -t chaos-operator:demo .
kind load docker-image chaos-operator:demo --name sre-platform

# 2. Install CRD + create namespace + dummy credentials secret
kubectl create namespace chaos-demo
kubectl apply -f config/crd/chaosexperiment.yaml
kubectl create secret generic chaos-operator-credentials -n chaos-demo \
  --from-literal=anthropic-api-key=dummy-not-used-in-demo

# 3. Deploy the operator (Prometheus gate -> in-cluster kube-prometheus)
helm install chaos-operator ./helm/chaos-operator -n chaos-demo \
  --set image.repository=chaos-operator --set image.tag=demo \
  --set prometheusURL=http://kube-prometheus-kube-prome-prometheus.monitoring.svc:9090 \
  --set crds.install=false

# 4. Deploy the target and run the experiments
kubectl apply -f demo/target-deployment.yaml
kubectl apply -f demo/podkill-experiment.yaml     # happy path -> Completed
kubectl apply -f demo/guard-minhealthy.yaml       # blocked -> Aborted
kubectl apply -f demo/guard-denylist.yaml         # blocked -> Aborted

# 5. Watch
kubectl get chaosexperiment -n chaos-demo -w
kubectl logs -n chaos-demo deploy/chaos-operator -f
```

## Notes / honesty

- **Two bugs were fixed to make the live demo work** (both committed):
  1. Helm RBAC `ClusterRole` used the wrong API group (`chaos.k8s-ai-ops.io`)
     instead of the CRD's real group `chaos.sharatvikas.io`, so the operator's
     ServiceAccount could not watch its own CRs.
  2. `setPhase` never stamped `status.endTime` on the `Running -> Cooldown`
     transition, so `handleCooldown` reset the cooldown clock every reconcile
     and the experiment was stranded in `Cooldown`, never reaching `Completed`.
- There is no `ANTHROPIC_API_KEY` in this environment. The PodKill happy path
  does not depend on the LLM, so it runs fully. LLM-backed reporting/analysis
  (if wired) would fail without a real key — a dummy secret value is supplied
  only so the pod can start.
- The `SLOBurnRate` gate issues a live query to in-cluster Prometheus. The
  demo target exports no `job:request_error_rate` metric, so the query returns
  no data and the gate evaluates as "within threshold" (safe) — the experiment
  proceeds. Prometheus reachability at the operator's configured URL is proven
  independently (HTTP 200).
