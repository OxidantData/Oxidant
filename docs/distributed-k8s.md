# Running distributed Oxidant on Kubernetes (Kind + EKS)

This is the **minimal runnable Kubernetes** path: one Spark Connect driver pod + N Arrow Flight
worker pods, discovered via a headless Service. It matches the OSS runtime contract in
[`runtime-contract.md`](runtime-contract.md).

For an **EKS-free** AWS data plane (Packer AMI + CloudFormation Auto Scaling Groups), see
[`distributed-ec2.md`](distributed-ec2.md).

For the future full platform (SSO, gateway operator, Terraform), see
[`deployment.md`](deployment.md) — that outline is **not** required here.

## Architecture

```
PySpark / oxidant-bench  -->  oxidant-connect:50051 (driver)
                              |
                              |  OXIDANT_WORKER_SERVICE DNS (A records)
                              v
                         oxidant-worker pods :50561 (Flight)
```

- **Driver image:** `oxidant/connect-server` — `oxidant spark server --port 50051`
- **Worker image:** `oxidant/worker` — same binary, `oxidant worker --port 50561`
- Both images **bundle AWS CLI v2** at `OXIDANT_AWS_BIN=/usr/local/aws-cli/v2/current/bin/aws`
  (used by Glue catalog resolution). Credentials are **not** baked in — use IRSA / env / instance role.

## File-list sharding (required for correct multi-worker scans)

`ShardAssignment::from_env` in `crates/oxidant-loom/src/shard.rs` only activates when:

1. `OXIDANT_WORKER_COUNT` > 1, **and**
2. `OXIDANT_SHARD_INDEX` **or** `OXIDANT_POD_NAME` (StatefulSet ordinal from the trailing
   `-N` in `oxidant-worker-0`)

If either is missing, `from_env()` returns `None` and **every worker reads the entire
file list** — no error, silently duplicated aggregates. That is the worst failure mode
for a published benchmark.

The Helm chart therefore:

- Deploys workers as a **StatefulSet** governed by the existing headless `oxidant-worker`
  Service (DNS discovery for `OXIDANT_WORKER_SERVICE` is unchanged).
- Sets on every worker pod:
  - `OXIDANT_WORKER_COUNT=<worker.replicas>`
  - `OXIDANT_POD_NAME` via `fieldRef: metadata.name`
- Defaults `worker.autoscaling.enabled` to **false** and **`fail`s the template** if
  someone turns it on: a fixed `OXIDANT_WORKER_COUNT` plus an HPA that changes replica
  count is incoherent (scale out → unread shards / data loss; or pods keep reading
  everything). Pin `worker.replicas` instead.
- TCP **readiness** (and liveness) probes on the Flight port so the headless Service
  DNS only returns workers that have bound the port.
- Required **podAntiAffinity** on `app=oxidant-worker` / `kubernetes.io/hostname` so one
  worker lands per node.
- **Threshold shuffle spill** (not force-spill):
  - Spill PVC (or emptyDir) mounted at `/var/lib/oxidant/spill`
  - `TMPDIR=/var/lib/oxidant/spill` so `default_spill_root()` → `$TMPDIR/oxidant-shuffle-spill/…`
    lands on that volume (with only `OXIDANT_SHUFFLE_SPILL_BYTES` /
    `OXIDANT_MEMORY_LIMIT_BYTES`, the engine does **not** use `OXIDANT_SHUFFLE_SPILL_DIR`)
  - `OXIDANT_SHUFFLE_SPILL_BYTES` for an explicit threshold (takes precedence over
    `OXIDANT_MEMORY_LIMIT_BYTES`)
  - **`OXIDANT_SHUFFLE_SPILL_DIR` is debug-only** (`worker.forceShuffleSpill: true`): when
    set, `SpillStore::from_env` enables `force_spill` and writes **every** shuffle
    bucket to disk — do not enable for publishable benchmarks
  - Legacy `OXIDANT_SPILL_DIR` is **unused** by Rust — do not set it

### Auto-broadcast dims (no operator dim list)

SF100 / multi-worker Connect no longer needs `OXIDANT_REPLICATED_TABLES` for TPC-H/DS
dimensions. The driver classifies dims from Glue/Parquet (or lakehouse) file sizes —
tables smaller than the query's largest scan and under
`OXIDANT_AUTO_BROADCAST_THRESHOLD_BYTES` (default 32 GiB) are fully replicated on every
worker — and ships that list on each `StageTicket`. Keep `OXIDANT_REPLICATED_TABLES` only
as an optional force-include override.

### Silent shard loss (config mitigations + engine follow-up)

Each worker shards from its **own** env (`OXIDANT_POD_NAME` ordinal / `OXIDANT_WORKER_COUNT`).
The driver discovers live endpoints via DNS and partitions by `workers.len()`. Nothing
cross-checks the two today: if `oxidant-worker-1` is not Ready, the driver may dispatch
only to `oxidant-worker-0`, which still reads only shard 0 of 2 → ~half the rows, no error.

Config-side mitigations on this chart / harness:

1. Readiness probes so DNS omits unbound pods.
2. `bench/sf100/run-spark-connect.py` preflight: refuse unless Ready `oxidant-worker` pods
   equal `--worker-count` / `OXIDANT_WORKER_COUNT` (default 2 for SF≥100).
3. `OXIDANT_SHUFFLE_PARTITIONS` is pinned on the driver (see below).

### Shuffle modulus drift (a second, distinct hazard)

The preflight above only checks membership at the **start** of a run. There is a
separate failure that needs no pod to be missing at launch:

`shuffle_partitions()` falls back to the live worker count when neither
`OXIDANT_SHUFFLE_PARTITIONS` nor `OXIDANT_DEFAULT_PARALLELISM` is set, and
`refresh_cluster_workers()` recomputes `num_partitions` *between stages of a single
query*. Producers hash rows into `fnv1a(row) % np` buckets; a consumer pulls bucket
`partition_id` from every worker. So if `np` is 2 while producers run and 1 by the
time the consumer runs, every row in bucket 1 is never pulled by anyone — the query
returns fewer rows, faster, and reports success. Rendezvous ownership
(`owner_of(partition, num_partitions)`) is a function of the member set too, so a
blip can also route a bucket to a worker that never received it.

A mid-run OOM kill or a failed liveness probe is enough to trigger this, and the
memory budgets in `values-sf100.yaml` are deliberately tight.

Chart mitigation: `connect.shufflePartitions` (default `worker.replicas`) always emits
`OXIDANT_SHUFFLE_PARTITIONS` on the driver, so the modulus is constant regardless of what
DNS reports. Keep it pinned for any publishable run.

**Engine fix (landed on `vamzi/distributed-membership-stability`):** the driver now
freezes the planned worker set and `num_partitions` for the lifetime of a query,
hard-fails when live membership differs at any stage barrier, and cross-checks its
visible fan-out against `OXIDANT_WORKER_COUNT`. That last check only fires if the driver
process actually has the variable, which is why `OXIDANT_WORKER_COUNT` is set on the
**connect** pod too — deliberately without `OXIDANT_POD_NAME`/`OXIDANT_SHARD_INDEX`, since
`ShardAssignment::from_env` needs an index as well as a count and returns `None`
without one, so the driver never shards its own reads.

The reproduction is worth knowing: a 2-worker `GROUP BY` over 200 keys returned `Ok`
with 50 rows when membership shrank mid-query.

## Build images

From the repository root (BuildKit required):

```sh
TAG=$(git rev-parse --short HEAD)

docker build -f deploy/docker/connect-server.Dockerfile \
  -t oxidant/connect-server:$TAG .

docker build -f deploy/docker/worker.Dockerfile \
  --build-arg CONNECT_IMAGE=oxidant/connect-server:$TAG \
  -t oxidant/worker:$TAG .

# Verify AWS CLI is present (should print aws-cli/2.x …)
docker run --rm --entrypoint /usr/local/aws-cli/v2/current/bin/aws \
  oxidant/connect-server:$TAG --version
```

Published CI image: `docker.io/vamzi/oxidant` (built from `connect-server.Dockerfile`).
You can point both Helm image refs at that and override the worker command, or build the
thin `worker` rebase as above.

## Helm chart

Chart: [`deploy/helm/oxidant/`](../deploy/helm/oxidant/)

| Resource | Purpose |
|----------|---------|
| `oxidant` ServiceAccount | Optional IRSA annotations for S3 + Glue |
| `oxidant-connect` Deployment + Service | Spark Connect driver; sets `OXIDANT_WORKER_SERVICE` |
| `oxidant-worker` StatefulSet + headless Service | Flight workers with sharding env + spill PVC |
| `oxidant-gateway` | **Off by default** (`gateway.enabled=false`) |

Render locally:

```sh
helm template oxidant deploy/helm/oxidant --namespace oxidant \
  --set connect.image=oxidant/connect-server:$TAG \
  --set worker.image=oxidant/worker:$TAG

# SF100 topology overlay (2× m8g.4xlarge workers, 500Gi gp3, arm64, strict)
helm template oxidant deploy/helm/oxidant --namespace oxidant \
  -f deploy/helm/oxidant/values-sf100.yaml \
  --set connect.image=oxidant/connect-server:$TAG \
  --set worker.image=oxidant/worker:$TAG

# Must fail — autoscaling incompatible with sharding
helm template oxidant deploy/helm/oxidant --namespace oxidant \
  --set worker.autoscaling.enabled=true
```

## Kind (local)

Prerequisites: Docker, [`kind`](https://kind.sigs.k8s.io/), `kubectl`, `helm`.

```sh
kind create cluster --name oxidant
kind load docker-image oxidant/connect-server:$TAG --name oxidant
kind load docker-image oxidant/worker:$TAG --name oxidant

kubectl create namespace oxidant
helm upgrade --install oxidant deploy/helm/oxidant \
  --namespace oxidant \
  --set connect.image=oxidant/connect-server:$TAG \
  --set connect.imagePullPolicy=IfNotPresent \
  --set worker.image=oxidant/worker:$TAG \
  --set worker.imagePullPolicy=IfNotPresent \
  --set worker.replicas=2 \
  --set worker.persistence.enabled=false

kubectl -n oxidant rollout status deploy/oxidant-connect
kubectl -n oxidant rollout status statefulset/oxidant-worker
kubectl -n oxidant port-forward svc/oxidant-connect 50051:50051
```

Smoke with PySpark Connect (separate terminal):

```sh
pip install "pyspark-client>=4.0"
python - <<'PY'
from pyspark.sql import SparkSession
spark = SparkSession.builder.remote("sc://localhost:50051").getOrCreate()
spark.sql("SELECT 1 AS hello").show()
# INTERVAL / TPC-H-style date arithmetic
spark.sql("SELECT date '1998-12-01' - interval '90' day (3) AS d").show()
PY
```

### Notes for Kind smoke

- Multi-pod distributed SQL needs **table data registered on workers** (shared object store,
  or catalog over S3). A bare `SELECT 1` only proves Connect reaches the driver.
- For TPC-H distributed **without** K8s data plumbing, use the in-process harness:
  `cargo run -p oxidant-bench -- tpch-distributed --sf 0.01 --workers 2`
  or `oxidant spark server --mode local-cluster --workers 2 --port 50051` (see
  [`runtime-contract.md`](runtime-contract.md)).

## BYO EKS

Prerequisites: an existing EKS cluster, `kubectl` context pointed at it, ECR (or other)
registry access, and (for Glue/S3) an IRSA role bound to the chart ServiceAccount.

### SF100 topology

Overlay [`deploy/helm/oxidant/values-sf100.yaml`](../deploy/helm/oxidant/values-sf100.yaml).
**Instance shapes match the EC2 CloudFormation SF100 topology** (track both places —
see [`distributed-ec2.md` § SF100 topology](distributed-ec2.md#sf100-topology-canonical)):
1× **c6g.xlarge** driver + 2× **m8g.4xlarge** workers (min=max=2), 500 GiB gp3 spill
per worker, arm64.

- Connect sized for **c6g.xlarge** (4 vCPU / 8 GiB), workers for **m8g.4xlarge**
  (16 vCPU / 64 GiB), `kubernetes.io/arch=arm64`
- `worker.replicas: 2`, autoscaling off, **500Gi gp3** spill PVC per worker
- `OXIDANT_MEMORY_LIMIT_BYTES` + `OXIDANT_SHUFFLE_SPILL_BYTES` aligned with container memory
  (threshold spill); `TMPDIR` on the spill PVC; `forceShuffleSpill: false`
- `OXIDANT_PREFER_HASH_JOIN=true` on workers (KAN-45): the sort-merge path can deadlock under
  the bounded FairSpillPool at scale (upstream: delta-io/delta-rs#4614); hash-join build
  inputs fail fast rather than spill under pool pressure, which beats a 10-minute wedge
- `connect.distributedStrict: true` → `OXIDANT_DISTRIBUTED_STRICT=1` on the driver
- Connect CPU request **3000m** (not 3500m) so a c6g.xlarge can still schedule CNI /
  kube-proxy / node agents beside the pod
- Connect memory request **5Gi** (not 7Gi). Requests are matched against node
  **allocatable**, not capacity: EKS kube-reserved on an 8 GiB node is ~2.05 GiB, so a
  c6g.xlarge offers only ~5.85 GiB and a 7Gi request leaves the driver Pending with
  "Insufficient memory". Check `kubectl describe node` before raising it.
- Connect carries a `podAntiAffinity` against `app=oxidant-worker`. The instance-type
  nodeSelector is a placeholder, so arch is otherwise the only constraint and the
  scheduler could park the driver on a worker node, silently taking CPU and memory
  away from the worker being benchmarked.
- IRSA / instance-type labels left as **obvious placeholders** — fill before install

```sh
# Push images
aws ecr get-login-password --region "$AWS_REGION" \
  | docker login --username AWS --password-stdin "$ACCOUNT.dkr.ecr.$AWS_REGION.amazonaws.com"
CONNECT_REF=$ACCOUNT.dkr.ecr.$AWS_REGION.amazonaws.com/oxidant/connect-server:$TAG
WORKER_REF=$ACCOUNT.dkr.ecr.$AWS_REGION.amazonaws.com/oxidant/worker:$TAG
docker tag oxidant/connect-server:$TAG "$CONNECT_REF"
docker tag oxidant/worker:$TAG "$WORKER_REF"
docker push "$CONNECT_REF"
docker push "$WORKER_REF"

kubectl create namespace oxidant
helm upgrade --install oxidant deploy/helm/oxidant \
  --namespace oxidant \
  -f deploy/helm/oxidant/values-sf100.yaml \
  --set connect.image=$CONNECT_REF \
  --set worker.image=$WORKER_REF

# Expose for clients (choose one):
kubectl -n oxidant port-forward svc/oxidant-connect 50051:50051
# or: --set connect.serviceType=LoadBalancer  (then use the LB hostname)
```

Then run the direct Spark Connect SF100 harness (see [`bench/sf100/README.md`](../bench/sf100/README.md)):

```sh
OXIDANT_DISTRIBUTED_STRICT=1 python3 bench/sf100/run-spark-connect.py \
  --endpoint sc://localhost:50051 \
  --suite tpcds --sf 100 --glue-db tpcds_sf100 \
  --namespace oxidant --worker-count 2 \
  --json results/tpcds-sf100.jsonl --resume
```

### IRSA / Glue / S3

1. Create an IAM role trusted by the EKS OIDC provider for
   `system:serviceaccount:oxidant:oxidant` (chart default SA name).
2. Grant least-privilege S3 + Glue permissions the workload needs.
3. Set `serviceAccount.annotations.eks.amazonaws.com/role-arn` in values
   (SF100 overlay has an `AWS_ACCOUNT_ID` / `OXIDANT_IRSA_ROLE_NAME` placeholder).
4. Pods already set `OXIDANT_AWS_BIN` to the image-bundled CLI; Glue catalog code shells out
   to that binary. No static keys in the image.

See also [`runtime-contract.md`](runtime-contract.md) for the full env surface
(`OXIDANT_WORKER_SERVICE`, `OXIDANT_SHUFFLE_SPILL_BYTES`, `OXIDANT_MEMORY_LIMIT_BYTES`,
`TMPDIR`; `OXIDANT_SHUFFLE_SPILL_DIR` is force-spill / debug-only).

## TPC-H on distributed

```sh
# Local process harness (CI gate)
OXIDANT_TPCH_DIST_REQUIRE_ALL=1 \
  cargo run -p oxidant-bench -- tpch-distributed --sf 0.01 --workers 2
```

Bench SQL uses official-style `date '…'` + `INTERVAL` arithmetic (including ANSI
`interval '90' day (3)` on Q1). The engine strips unsupported leading precision and
sanitizes Unparser Postgres interval forms (`INTERVAL '12 MONS'` → `INTERVAL '12' MONTH`)
before workers re-parse stage SQL.

## Troubleshooting

| Symptom | Likely cause |
|---------|----------------|
| Inflated row counts / duplicated aggregates with 2 workers | Missing `OXIDANT_WORKER_COUNT` / `OXIDANT_POD_NAME` — confirm StatefulSet env |
| ~Half the rows, no error | A worker not Ready while others still shard as if N is full — check readiness + runner preflight |
| Driver plans but workers never receive tasks | `OXIDANT_WORKER_SERVICE` DNS empty — check headless Service + ready pods |
| Benchmarks unexpectedly disk-bound | `forceShuffleSpill` / `OXIDANT_SHUFFLE_SPILL_DIR` enabled — turn it off |
| `INTERVAL … leading_precision` plan error | Client bypassed `normalize_spark_sql` — use `Engine::sql` / Connect server |
| `INTERVAL requires a unit after the literal` on workers | Stage SQL not sanitized — ensure current `oxidant-execution` sanitize path |
| Glue / S3 auth failures | IRSA / role missing; confirm `aws sts get-caller-identity` inside the pod |
| Helm fails on `autoscaling.enabled=true` | Intentional — pin `worker.replicas` instead |
| `oxidant binary not found` in tests | Build `oxidant-cli` before `cargo test --workspace` (see `AGENTS.md`) |
