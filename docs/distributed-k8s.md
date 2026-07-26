# Running distributed Weft on Kubernetes (Kind + EKS)

This is the **minimal runnable** path: one Spark Connect driver pod + N Arrow Flight
worker pods, discovered via a headless Service. It matches the OSS runtime contract in
[`runtime-contract.md`](runtime-contract.md).

For the future full platform (SSO, gateway operator, Terraform), see
[`deployment.md`](deployment.md) — that outline is **not** required here.

## Architecture

```
PySpark / weft-bench  -->  weft-connect:50051 (driver)
                              |
                              |  WEFT_WORKER_SERVICE DNS (A records)
                              v
                         weft-worker pods :50561 (Flight)
```

- **Driver image:** `weft/connect-server` — `weft spark server --port 50051`
- **Worker image:** `weft/worker` — same binary, `weft worker --port 50561`
- Both images **bundle AWS CLI v2** at `WEFT_AWS_BIN=/usr/local/aws-cli/v2/current/bin/aws`
  (used by Glue catalog resolution). Credentials are **not** baked in — use IRSA / env / instance role.

## Build images

From the repository root (BuildKit required):

```sh
TAG=$(git rev-parse --short HEAD)

docker build -f deploy/docker/connect-server.Dockerfile \
  -t weft/connect-server:$TAG .

docker build -f deploy/docker/worker.Dockerfile \
  --build-arg CONNECT_IMAGE=weft/connect-server:$TAG \
  -t weft/worker:$TAG .

# Verify AWS CLI is present (should print aws-cli/2.x …)
docker run --rm --entrypoint /usr/local/aws-cli/v2/current/bin/aws \
  weft/connect-server:$TAG --version
```

Published CI image: `docker.io/vamzi/weft` (built from `connect-server.Dockerfile`).
You can point both Helm image refs at that and override the worker command, or build the
thin `worker` rebase as above.

## Helm chart

Chart: [`deploy/helm/weft/`](../deploy/helm/weft/)

| Resource | Purpose |
|----------|---------|
| `weft-connect` Deployment + Service | Spark Connect driver; sets `WEFT_WORKER_SERVICE` |
| `weft-worker` Deployment + headless Service | Flight workers |
| `weft-worker` HPA | Optional CPU autoscaling (on by default) |
| `weft-gateway` | **Off by default** (`gateway.enabled=false`) |

Render locally:

```sh
helm template weft deploy/helm/weft --namespace weft \
  --set connect.image=weft/connect-server:$TAG \
  --set worker.image=weft/worker:$TAG
```

## Kind (local)

Prerequisites: Docker, [`kind`](https://kind.sigs.k8s.io/), `kubectl`, `helm`.

```sh
kind create cluster --name weft
kind load docker-image weft/connect-server:$TAG --name weft
kind load docker-image weft/worker:$TAG --name weft

kubectl create namespace weft
helm upgrade --install weft deploy/helm/weft \
  --namespace weft \
  --set connect.image=weft/connect-server:$TAG \
  --set connect.imagePullPolicy=IfNotPresent \
  --set worker.image=weft/worker:$TAG \
  --set worker.imagePullPolicy=IfNotPresent \
  --set worker.replicas=2 \
  --set worker.autoscaling.enabled=false

kubectl -n weft rollout status deploy/weft-connect
kubectl -n weft rollout status deploy/weft-worker
kubectl -n weft port-forward svc/weft-connect 50051:50051
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
  `cargo run -p weft-bench -- tpch-distributed --sf 0.01 --workers 2`
  or `weft spark server --mode local-cluster --workers 2 --port 50051` (see
  [`runtime-contract.md`](runtime-contract.md)).

## BYO EKS

Prerequisites: an existing EKS cluster, `kubectl` context pointed at it, ECR (or other)
registry access, and (for Glue/S3) an IRSA role bound to the connect/worker ServiceAccounts.

```sh
# Push images
aws ecr get-login-password --region "$AWS_REGION" \
  | docker login --username AWS --password-stdin "$ACCOUNT.dkr.ecr.$AWS_REGION.amazonaws.com"
CONNECT_REF=$ACCOUNT.dkr.ecr.$AWS_REGION.amazonaws.com/weft/connect-server:$TAG
WORKER_REF=$ACCOUNT.dkr.ecr.$AWS_REGION.amazonaws.com/weft/worker:$TAG
docker tag weft/connect-server:$TAG "$CONNECT_REF"
docker tag weft/worker:$TAG "$WORKER_REF"
docker push "$CONNECT_REF"
docker push "$WORKER_REF"

kubectl create namespace weft
helm upgrade --install weft deploy/helm/weft \
  --namespace weft \
  --set connect.image=$CONNECT_REF \
  --set worker.image=$WORKER_REF \
  --set worker.replicas=3

# Expose for clients (choose one):
kubectl -n weft port-forward svc/weft-connect 50051:50051
# or: --set connect.serviceType=LoadBalancer  (then use the LB hostname)
```

### IRSA / Glue / S3

1. Create an IAM role trusted by the EKS OIDC provider for
   `system:serviceaccount:weft:default` (or a dedicated SA you attach in values).
2. Grant least-privilege S3 + Glue permissions the workload needs.
3. Annotate the ServiceAccount:
   `eks.amazonaws.com/role-arn=arn:aws:iam::<acct>:role/<role>`.
4. Pods already set `WEFT_AWS_BIN` to the image-bundled CLI; Glue catalog code shells out
   to that binary. No static keys in the image.

See also [`runtime-contract.md`](runtime-contract.md) for the full env surface
(`WEFT_WORKER_SERVICE`, spill dirs, memory limits).

## TPC-H on distributed

```sh
# Local process harness (CI gate)
WEFT_TPCH_DIST_REQUIRE_ALL=1 \
  cargo run -p weft-bench -- tpch-distributed --sf 0.01 --workers 2
```

Bench SQL uses official-style `date '…'` + `INTERVAL` arithmetic (including ANSI
`interval '90' day (3)` on Q1). The engine strips unsupported leading precision and
sanitizes Unparser Postgres interval forms (`INTERVAL '12 MONS'` → `INTERVAL '12' MONTH`)
before workers re-parse stage SQL.

## Troubleshooting

| Symptom | Likely cause |
|---------|----------------|
| Driver plans but workers never receive tasks | `WEFT_WORKER_SERVICE` DNS empty — check headless Service + ready pods |
| `INTERVAL … leading_precision` plan error | Client bypassed `normalize_spark_sql` — use `Engine::sql` / Connect server |
| `INTERVAL requires a unit after the literal` on workers | Stage SQL not sanitized — ensure current `weft-execution` sanitize path |
| Glue / S3 auth failures | IRSA / role missing; confirm `aws sts get-caller-identity` inside the pod |
| `weft binary not found` in tests | Build `weft-cli` before `cargo test --workspace` (see `AGENTS.md`) |
