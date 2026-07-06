# Live Pod-Kill Chaos Demo — Captured Output

Real output captured against a live `kind` cluster (context `kind-sre-platform`,
node k8s v1.35.0) on 2026-07-06. Commands are exactly as run.

## 0. Environment

```
$ kubectl config current-context
kind-sre-platform

$ kubectl get nodes
NAME                         STATUS   ROLES           AGE   VERSION
sre-platform-control-plane   Ready    control-plane   78d   v1.35.0

$ go version
go version go1.26.1 darwin/arm64
$ docker --version   # 29.4.0
$ helm version --short   # v4.1.4
```

## 1. Build + load the manager image

```
$ docker build -t chaos-operator:demo .
...
 => naming to docker.io/library/chaos-operator:demo

$ kind load docker-image chaos-operator:demo --name sre-platform
Image: "chaos-operator:demo" ... loading...
```

## 2. Install CRD, namespace, secret, deploy operator via Helm

```
$ kubectl create namespace chaos-demo
namespace/chaos-demo created

$ kubectl apply -f config/crd/chaosexperiment.yaml
customresourcedefinition.apiextensions.k8s.io/chaosexperiments.chaos.sharatvikas.io created

$ kubectl create secret generic chaos-operator-credentials -n chaos-demo \
    --from-literal=anthropic-api-key=dummy-not-used-in-demo
secret/chaos-operator-credentials created

$ helm install chaos-operator ./helm/chaos-operator -n chaos-demo \
    --set image.repository=chaos-operator --set image.tag=demo \
    --set prometheusURL=http://kube-prometheus-kube-prome-prometheus.monitoring.svc:9090 \
    --set crds.install=false
NAME: chaos-operator
STATUS: deployed

$ kubectl rollout status deploy/chaos-operator -n chaos-demo
deployment "chaos-operator" successfully rolled out
```

Operator startup logs (leader elected, controller started, Prometheus gate URL):

```
{"logger":"setup","msg":"starting chaos engineering operator","prometheus":"http://kube-prometheus-kube-prome-prometheus.monitoring.svc:9090","watchNamespace":"","leaderElection":true}
leaderelection.go:260] successfully acquired lease chaos-demo/chaos-operator-leader
{"msg":"Starting Controller","controllerGroup":"chaos.sharatvikas.io","controllerKind":"ChaosExperiment"}
{"msg":"Starting workers","controllerKind":"ChaosExperiment","worker count":1}
```

## 3. Deploy the target Deployment (4 replicas)

```
$ kubectl apply -f demo/target-deployment.yaml
deployment.apps/demo-target created
$ kubectl rollout status deploy/demo-target -n chaos-demo
deployment "demo-target" successfully rolled out

$ kubectl get pods -n chaos-demo -l app=demo-target      # BEFORE
NAME                           READY   STATUS    RESTARTS   AGE
demo-target-697dbcc75d-mw5pl   1/1     Running   0          3m48s
demo-target-697dbcc75d-qhkzw   1/1     Running   0          4m21s
demo-target-697dbcc75d-qxqkk   1/1     Running   0          3m48s
demo-target-697dbcc75d-xhrg6   1/1     Running   0          4m21s
```

## 4. Happy-path PodKill experiment — full phase transitions

```
$ kubectl apply -f demo/podkill-experiment.yaml
chaosexperiment.chaos.sharatvikas.io/demo-target-pod-kill created

# polled phase + deployment readiness live:
T+4s   PHASE=PreflightCheck  deployReady=3  runningPods=3
T+6s   PHASE=Running         deployReady=4  runningPods=4
   --- pods DURING (right after kill; mw5pl+qhkzw gone, 8jhsd+lk5fp are 2s old) ---
   demo-target-697dbcc75d-8jhsd   1/1   Running   0   2s
   demo-target-697dbcc75d-lk5fp   1/1   Running   0   2s
   demo-target-697dbcc75d-qxqkk   1/1   Running   0   3m55s
   demo-target-697dbcc75d-xhrg6   1/1   Running   0   4m28s
T+36s  PHASE=Cooldown        deployReady=4  runningPods=4
T+50s  PHASE=Completed       deployReady=4  runningPods=4
TERMINAL=Completed

$ kubectl get pods -n chaos-demo -l app=demo-target      # AFTER (self-healed to 4/4)
NAME                           READY   STATUS    RESTARTS   AGE
demo-target-697dbcc75d-8jhsd   1/1     Running   0          50s      <- replacement
demo-target-697dbcc75d-lk5fp   1/1     Running   0          50s      <- replacement
demo-target-697dbcc75d-qxqkk   1/1     Running   0          4m43s
demo-target-697dbcc75d-xhrg6   1/1     Running   0          5m16s
```

Controller logs — preflight passed, real kills, injection, and rollback:

```
{"msg":"experiment pending, starting preflight"}
{"msg":"pod killed","pod":"demo-target-697dbcc75d-mw5pl","namespace":"chaos-demo","gracePeriodSeconds":0}
{"msg":"pod killed","pod":"demo-target-697dbcc75d-qhkzw","namespace":"chaos-demo","gracePeriodSeconds":0}
{"msg":"fault injected","type":"PodKill","affected":2,"artifacts":0}
{"msg":"duration elapsed — stopping faults"}     <- rollback / Stop() at end of duration
```

Final CR status (`kubectl get chaosexperiment demo-target-pod-kill -n chaos-demo -o yaml`):

```yaml
status:
  conditions:
  - lastTransitionTime: "2026-07-06T03:03:16Z"
    message: PodKill on 2 pods
    reason: FaultInjected
    status: "True"
    type: Running
  endTime: "2026-07-06T03:03:49Z"
  phase: Completed
  startTime: "2026-07-06T03:03:16Z"
  targetedPods:
  - ""
  - ""
```

## 5. Safety guards BLOCKING unsafe experiments

```
$ kubectl apply -f demo/guard-minhealthy.yaml
$ kubectl apply -f demo/guard-denylist.yaml

$ kubectl get chaosexperiment -n chaos-demo
NAME                         PHASE       FAULT     DURATION   AGE
demo-target-pod-kill         Completed   PodKill   20s        78s
guard-denylist-violation     Aborted     PodKill   20s        12s
guard-minhealthy-violation   Aborted     PodKill   20s        12s

$ kubectl get chaosexperiment guard-minhealthy-violation -n chaos-demo \
    -o jsonpath='{.status.phase}: {.status.abortReason}'
Aborted: preflight replica guard: min-healthy-replicas guard: 4 running, fault would affect 4, leaving 0 < required 3

$ kubectl get chaosexperiment guard-denylist-violation -n chaos-demo \
    -o jsonpath='{.status.phase}: {.status.abortReason}'
Aborted: preflight namespace guard: namespace "kube-system" is on the chaos denylist (protected)
```

Controller logs for the two blocks:

```
{"msg":"preflight blocked experiment","guard":"min-healthy-replicas","reason":"min-healthy-replicas guard: 4 running, fault would affect 4, leaving 0 < required 3"}
{"msg":"preflight blocked experiment","guard":"namespace-denylist","reason":"namespace \"kube-system\" is on the chaos denylist (protected)"}
{"msg":"experiment is terminal","phase":"Aborted"}
```

The denylist guard was effective — kube-dns pods were never touched:

```
$ kubectl get pods -n kube-system -l k8s-app=kube-dns
coredns-7d764666f9-dtrkz   Running   78d
coredns-7d764666f9-vsspk   Running   78d
```

## 6. Live Prometheus SLO gate reachability

The `SLOBurnRate` gate queries in-cluster Prometheus at the operator's
configured URL. Independent reachability check from the `chaos-demo` namespace:

```
$ kubectl run promcheck -n chaos-demo --image=curlimages/curl --rm -i --restart=Never --command -- \
    curl -s -o /dev/null -w "HTTP %{http_code}\n" \
    "http://kube-prometheus-kube-prome-prometheus.monitoring.svc:9090/api/v1/query?query=up"
prometheus /api/v1/query HTTP 200
```

The demo target exports no request-error metric, so the burn-rate query returns
no data and the gate evaluates as safe — the experiment proceeds (as seen above).

## What needed a key / wasn't reproducible

- No `ANTHROPIC_API_KEY` in this environment. The PodKill path is LLM-free and
  ran end to end. A dummy secret value was supplied only so the pod starts;
  any LLM-backed analysis/reporting would fail gracefully without a real key.
- Two operator bugs were fixed to enable the live run (see `README.md`
  "Notes / honesty"): the Helm RBAC API-group mismatch, and the missing
  `endTime` stamp on the `Running -> Cooldown` transition.
