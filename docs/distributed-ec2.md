# Running distributed Weft on EC2 (CloudFormation + ASG)

This is the **EKS-free** data-plane path: one Spark Connect driver EC2 instance + N
Arrow Flight worker instances in fixed-size Auto Scaling Groups, discovered via a
private Route53 multi-A name. It matches the OSS runtime contract in
[`runtime-contract.md`](runtime-contract.md).

For Kubernetes (Kind / EKS + Helm), see [`distributed-k8s.md`](distributed-k8s.md).
For catalog SPI details (Hive / Glue / REST), see [`catalogs.md`](catalogs.md).
For the future full platform (SSO, gateway operator, Terraform), see
[`deployment.md`](deployment.md).

## Architecture

```
PySpark / weft-bench  -->  weft driver :50051  (direct IP — no LB)
                              |
                              |  WEFT_WORKER_SERVICE DNS (multi-A)
                              v
                         worker EC2s :50561 (Flight)
                              |
                              |  aws glue + s3 (instance role)
                              v
                         Glue Data Catalog + S3 data
```

| Piece | Implementation |
|-------|----------------|
| AMI | Packer AL2023 image (`deploy/packer/`) with `weft`, AWS CLI v2, hardened `weft` user (uid 65532), systemd units |
| Driver | ASG `Min=Max=Desired=1`, `weft spark server --port 50051` |
| Workers | ASG `Min=Max=Desired=WorkerCount` (pinned — no scale policies) |
| Discovery | Private hosted zone; workers UPSERT `workers.<stack>.<zone>` with all InService private IPs |
| Sharding | Boot assigns `WEFT_SHARD_INDEX` = position in sorted InService instance IDs |
| Spill | Optional second EBS volume mounted at `/var/lib/weft/spill` (`TMPDIR`) |
| Auth | Instance profiles only — no credentials in the AMI |
| Catalog | Optional `WEFT_CATALOG_CONF` (Glue) on **driver and workers** via the `CatalogConf` stack parameter |

Workers are **not** behind a single L4 VIP for Flight (shuffle state is per-worker).
Connect clients talk to the **single driver IP** (private from VPC/VPN/bastion, or
public IP + `ClientCidr` for laptop honesty runs).

### Do not put Spark Connect behind an NLB

**Bad approach — do not use for Weft data-plane / SF100 / TPC honesty runs.**

An internet-facing Network Load Balancer on `:50051` (`ExposeConnect=true`) is the
wrong model for this workload:

| Reality | Why an NLB does not help |
|---------|--------------------------|
| One Connect driver | Nothing to load-balance; Databricks / LakeSail / OSS Spark Connect also terminate on the driver (or a control-plane proxy), not an L4 VIP in front of query compute |
| Workers are stateful Flight peers | Shuffle and shard index are per-instance; never put workers behind a shared VIP |
| Honesty / bench runs | NLB health checks + target registration add minutes of false “unhealthy” while `weft-driver` is already listening — wasted wall clock, not signal |
| Failure mode | ASG + TG health can mark the only driver unhealthy and black-hole clients even when `:50051` accepts connections |

Keep `ExposeConnect=false` (the default). Resolve the driver instance IP and use
`sc://<driver-ip>:50051`. The `ExposeConnect` parameter remains only as a
deprecated escape hatch and must not be used for published SF100 numbers.

## End-to-end checklist

Follow these sections in order:

1. [Prerequisites](#prerequisites)
2. [Prepare S3 + Glue](#prepare-s3--glue-data-catalog)
3. [Bake the AMI](#1-bake-the-ami)
4. [Deploy the CloudFormation stack](#2-deploy-the-cloudformation-stack)
5. [Verify the cluster](#3-verify-the-cluster)
6. [Point Weft at Glue](#4-point-weft-at-the-glue-catalog)
7. [Run a query](#5-run-a-query)
8. [Operate / tear down](#operate--tear-down)

---

## Prerequisites

### Tools (laptop / CI)

- AWS CLI v2 configured for the target account/region
- [Packer](https://developer.hashicorp.com/packer) ≥ 1.9 (Amazon plugin installed via `packer init`)
- Rust toolchain matching [`rust-toolchain.toml`](../rust-toolchain.toml) (to build `weft`), **or** a pre-built linux/amd64 `weft` binary
- Python 3 + `pip install "pyspark-client>=4.0"` for Connect clients
- Rights in the account for: EC2, Auto Scaling, IAM, Route53 (private zones), CloudFormation, S3, Glue, and (optional) ELBv2

### Network (existing VPC)

This template does **not** create a VPC. You need:

| Item | Notes |
|------|-------|
| VPC | Existing |
| Subnets | Prefer **private** subnets with NAT (or VPC endpoints) so instances can reach S3, Glue, and package updates |
| Path to S3/Glue | Gateway/Interface VPC endpoints **or** NAT egress |
| Client path to `:50051` | Host inside `ClientCidr` (VPC / VPN / bastion), **or** driver **public** IP with `ClientCidr` set to your `/32` — **not** an NLB (see [Do not put Spark Connect behind an NLB](#do-not-put-spark-connect-behind-an-nlb)) |

### Why worker count is fixed

Same contract as the Helm chart: file-list sharding uses a fixed `WEFT_WORKER_COUNT`
plus a stable `WEFT_SHARD_INDEX` per worker. Changing N under an ASG scaling policy
without coordinated re-assignment silently duplicates or drops shards. The template
sets `MinSize = MaxSize = DesiredCapacity = WorkerCount` and adds **no** scaling
policies. Bootstrap also pins `WEFT_SHUFFLE_PARTITIONS` to `WorkerCount` on the driver.
See [`distributed-k8s.md`](distributed-k8s.md) for the membership / silent-row-loss
failure modes — they apply identically here.

---

## Prepare S3 + Glue Data Catalog

Weft’s Glue provider (`weft-catalog-glue`) shells out to the AWS CLI
(`WEFT_AWS_BIN`, default `/usr/local/bin/aws` on the AMI). Credentials come from the
**instance profile** (IMDSv2). Tables must already exist in Glue and point at readable
S3 locations (Parquet is the well-supported path for remote object stores today; see
[`catalogs.md`](catalogs.md)).

### 1. Create (or choose) an S3 bucket

```sh
export AWS_REGION=us-west-2
export BUCKET=my-weft-data          # globally unique
export PREFIX=warehouse            # CTAS default root (optional)
export GLUE_DB=weft_demo

aws s3 mb "s3://${BUCKET}" --region "${AWS_REGION}"
```

Upload sample Parquet (or use an existing lakehouse layout):

```sh
# example: put a small Parquet dataset under a table prefix
aws s3 sync ./my-parquet/ "s3://${BUCKET}/${GLUE_DB}/orders/" --region "${AWS_REGION}"
```

### 2. Create a Glue database

```sh
aws glue create-database --region "${AWS_REGION}" \
  --database-input "{\"Name\":\"${GLUE_DB}\",\"Description\":\"Weft EC2 demo\"}"
```

### 3. Register a Glue table

Minimal EXTERNAL Parquet table (adjust columns to match your files):

```sh
aws glue create-table --region "${AWS_REGION}" --cli-input-json "{
  \"DatabaseName\": \"${GLUE_DB}\",
  \"TableInput\": {
    \"Name\": \"orders\",
    \"TableType\": \"EXTERNAL_TABLE\",
    \"Parameters\": { \"classification\": \"parquet\" },
    \"StorageDescriptor\": {
      \"Columns\": [
        {\"Name\": \"id\", \"Type\": \"bigint\"},
        {\"Name\": \"amount\", \"Type\": \"double\"}
      ],
      \"Location\": \"s3://${BUCKET}/${GLUE_DB}/orders/\",
      \"InputFormat\": \"org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat\",
      \"OutputFormat\": \"org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat\",
      \"SerdeInfo\": {
        \"SerializationLibrary\": \"org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe\"
      }
    }
  }
}"
```

Verify:

```sh
aws glue get-table --region "${AWS_REGION}" \
  --database-name "${GLUE_DB}" --name orders \
  --query 'Table.{Name:Name,Location:StorageDescriptor.Location}'
```

> Lakehouse / SF100: the repo’s [`bench/sf100/`](../bench/sf100/) scripts can materialize
> TPC-H/DS tables into S3 + Glue. Point the stack’s `DataBucketArns` at that bucket and
> use the printed database name as `glue.<db>.<table>` in queries.

### 4. Note the ARNs and catalog conf you will pass to CloudFormation

```text
DataBucketArns:
  arn:aws:s3:::my-weft-data,arn:aws:s3:::my-weft-data/*

EnableGlueAccess: true

CatalogConf (≤256 chars, applied to driver AND workers):
  spark.sql.catalog.glue.type=glue;spark.sql.catalog.glue.region=us-west-2;spark.sql.catalog.glue.warehouse=s3://my-weft-data/warehouse
```

| Key | Required | Meaning |
|-----|----------|---------|
| `spark.sql.catalog.<name>.type` | yes | Must be `glue` |
| `spark.sql.catalog.<name>.region` | recommended | Glue/S3 region (else `AWS_REGION` / `us-west-2`) |
| `spark.sql.catalog.<name>.warehouse` | optional | Default S3 root for CTAS without `LOCATION` |

`<name>` is the Spark catalog name (commonly `glue`). Queries then use
`glue.<database>.<table>` (three-part names). Unqualified names are **not** resolved
against external catalogs yet — always qualify (see [`catalogs.md`](catalogs.md)).

---

## 1. Bake the AMI

From the repository root:

```sh
cargo build -p weft-cli --release

# cross-check: binary must be linux/amd64 if you bake from macOS — build in CI,
# in a linux container, or pass --binary-url to a linux artifact instead.
./deploy/packer/build-ami.sh --binary ./target/release/weft --region "${AWS_REGION}"
```

Or:

```sh
./deploy/packer/build-ami.sh --binary-url https://example.com/releases/weft-linux-amd64
```

Packer prints a new `ami-…` id. Save it:

```sh
export AMI_ID=ami-0123456789abcdef0
```

The image:

- Installs `/usr/local/bin/weft` and AWS CLI v2
- Creates non-root user `weft` (uid/gid **65532**)
- Requires **IMDSv2** on the builder (launch templates also set `HttpTokens=required`)
- Disables SSH password auth; prefer SSM Session Manager
- Enables `dnf-automatic` for security updates
- Ships `weft-bootstrap.service` + role units (`weft-driver` / `weft-worker`)

Details: [`deploy/packer/README.md`](../deploy/packer/README.md).

---

## 2. Deploy the CloudFormation stack

```sh
export VPC_ID=vpc-0abc
export SUBNETS=subnet-aaa,subnet-bbb   # private preferred; public only if laptop hits driver public IP

./deploy/cloudformation/deploy-stack.sh \
  --ami "${AMI_ID}" \
  --vpc "${VPC_ID}" \
  --subnets "${SUBNETS}" \
  --stack weft-demo \
  --region "${AWS_REGION}" \
  --driver-type m6i.xlarge \
  --worker-type m6i.2xlarge \
  --workers 2 \
  --driver-spill-size 100 \
  --worker-spill-size 200 \
  --memory-limit-bytes 26000000000 \
  --shuffle-spill-bytes 8000000000 \
  --data-buckets "arn:aws:s3:::${BUCKET},arn:aws:s3:::${BUCKET}/*" \
  --glue true \
  --catalog-conf "spark.sql.catalog.glue.type=glue;spark.sql.catalog.glue.region=${AWS_REGION};spark.sql.catalog.glue.warehouse=s3://${BUCKET}/${PREFIX}" \
  --expose-connect false \
  --client-cidr 10.0.0.0/8
```

Template: [`deploy/cloudformation/weft-cluster.yaml`](../deploy/cloudformation/weft-cluster.yaml).

> **SF100 honesty runs** use a fixed Graviton topology — see
> [SF100 topology (canonical)](#sf100-topology-canonical) below. Do not publish
> numbers from the smaller `m6i.*` demo sizes in the example above.

What the stack creates:

- IAM instance profiles (SSM + optional S3/Glue; workers also get Route53 change on the private zone)
- Security groups (Connect `50051` from `ClientCidr`; Flight `50561` driver→workers and worker↔worker)
- Private hosted zone (`HostedZoneName`, default `weft.internal`)
- Driver LT + ASG (size 1) and worker LT + ASG (size `WorkerCount`)
- **No NLB** in the supported path (`ExposeConnect=false`)

### Parameters

| Parameter | Default | Purpose |
|-----------|---------|---------|
| `AmiId` | — | Runtime AMI from Packer |
| `VpcId` / `SubnetIds` | — | Existing network |
| `DriverInstanceType` | `m6i.xlarge` | Driver EC2 type |
| `WorkerInstanceType` | `m6i.2xlarge` | Worker EC2 type |
| `WorkerCount` | `2` | Fixed worker ASG size |
| `DriverRootVolumeSize` / `Type` | `40` / `gp3` | Driver root EBS |
| `WorkerRootVolumeSize` / `Type` | `40` / `gp3` | Worker root EBS |
| `DriverSpillVolumeSize` / `Type` | `100` / `gp3` | Extra volume → `/var/lib/weft/spill` (`0` = skip) |
| `WorkerSpillVolumeSize` / `Type` | `200` / `gp3` | Extra volume → `/var/lib/weft/spill` (`0` = skip) |
| `MemoryLimitBytes` | empty | `WEFT_MEMORY_LIMIT_BYTES` |
| `ShuffleSpillBytes` | empty | `WEFT_SHUFFLE_SPILL_BYTES` |
| `CatalogConf` | empty | `WEFT_CATALOG_CONF` on driver **and** workers (≤256 chars) |
| `DataBucketArns` | empty | S3 ARNs on the instance profiles |
| `EnableGlueAccess` | `false` | Glue `Get*` API permissions on instance profiles |
| `ExposeConnect` | `false` | **Deprecated / do not use** — creates an internet-facing Connect NLB (bad for this data plane; see above) |
| `ClientCidr` | `10.0.0.0/8` | Who may hit driver `:50051` (use your laptop `/32` for public-IP honesty runs) |
| `HostedZoneName` | `weft.internal` | Private zone created in the VPC |
| `KeyName` | empty | Optional SSH key (SSM preferred) |

When `MemoryLimitBytes` and `ShuffleSpillBytes` are both empty, `SpillStore::from_env`
stays off and the in-memory shuffle cache is **unbounded** — fine for smoke tests,
not safe for large queries. Set both for real workloads (same invariant as the Helm
SF100 overlay: memory + shuffle spill + headroom ≤ instance RAM).

Stack outputs include `ConnectEndpoint`, `WorkerDnsName`, ASG names, and `HostedZoneId`.

---

## 3. Verify the cluster

Wait until both ASGs show healthy capacity:

```sh
STACK=weft-demo
aws autoscaling describe-auto-scaling-groups --region "${AWS_REGION}" \
  --auto-scaling-group-names "${STACK}-driver" "${STACK}-workers" \
  --query 'AutoScalingGroups[].{Name:AutoScalingGroupName,Desired:DesiredCapacity,InService:length(Instances[?LifecycleState==`InService`])}'
```

SSM into the driver (no SSH required):

```sh
DRIVER_ID=$(aws ec2 describe-instances --region "${AWS_REGION}" \
  --filters "Name=tag:Name,Values=${STACK}-driver" "Name=instance-state-name,Values=running" \
  --query 'Reservations[0].Instances[0].InstanceId' --output text)

aws ssm start-session --region "${AWS_REGION}" --target "${DRIVER_ID}"
```

On the instance:

```sh
# identity + Glue reachability (uses instance role)
aws sts get-caller-identity
aws glue get-databases --region "$AWS_REGION" --query 'DatabaseList[].Name'

# Weft env + services
cat /etc/weft/weft.env
systemctl status weft-bootstrap weft-driver --no-pager
journalctl -u weft-bootstrap -u weft-driver -e --no-pager | tail -n 80

# workers should resolve via private DNS
getent hosts "$(grep WEFT_WORKER_SERVICE /etc/weft/weft.env | cut -d= -f2)"
```

Expect `/etc/weft/weft.env` on the **driver** to include roughly:

```bash
WEFT_AWS_BIN=/usr/local/bin/aws
AWS_REGION=us-west-2
WEFT_WORKER_SERVICE=workers.weft-demo.weft.internal
WEFT_WORKER_COUNT=2
WEFT_SHUFFLE_PARTITIONS=2
WEFT_CATALOG_CONF="spark.sql.catalog.glue.type=glue;..."
TMPDIR=/var/lib/weft/spill
```

On a **worker**, expect `WEFT_SHARD_INDEX=0` or `1` (not both the same), plus the same
`WEFT_CATALOG_CONF` / `WEFT_WORKER_COUNT`.

---

## 4. Point Weft at the Glue catalog

There are two complementary ways to register Glue. For **distributed** queries both
driver and workers must know the catalog — prefer stack `CatalogConf` (section 2).

### Option A — stack parameter (recommended)

Pass `--catalog-conf` / `CatalogConf` at deploy time (shown above). Bootstrap writes
`WEFT_CATALOG_CONF` into `/etc/weft/weft.env` for every instance. The CLI reads that env
at process start (`weft spark server` and `weft worker`).

After changing `CatalogConf`, update the stack and **instance-refresh / replace**
instances so bootstrap re-runs (launch template tag change alone does not restart
already-running nodes).

### Option B — Spark Connect client config (driver session)

Useful for quick experiments when the catalog is already on the server, or to override
warehouse/region for a session:

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.remote("sc://<driver-or-nlb>:50051")
    .config("spark.sql.catalog.glue.type", "glue")
    .config("spark.sql.catalog.glue.region", "us-west-2")
    .config("spark.sql.catalog.glue.warehouse", "s3://my-weft-data/warehouse")
    .getOrCreate()
)
```

Client-side config registers the catalog on the **Connect server (driver)**. Workers
still need `WEFT_CATALOG_CONF` (Option A) so distributed stages can resolve Glue/Parquet
locations when they execute. Do not rely on client-only config for multi-worker scans.

### How Glue auth works on EC2

1. Instance profile (from `EnableGlueAccess` + `DataBucketArns`) provides credentials via IMDSv2.
2. `WEFT_AWS_BIN=/usr/local/bin/aws` points at the AMI’s AWS CLI v2.
3. `weft-catalog-glue` runs `aws glue get-databases|get-tables|get-table …`.
4. Table `StorageDescriptor.Location` (`s3://…`) is read with the same role’s S3 permissions.

No static keys belong in the AMI, user-data, or `CatalogConf`.

---

## 5. Run a query

### Discover the Connect endpoint (driver IP — never NLB)

```sh
STACK=weft-demo
# Prefer public IP for laptop clients (SG must allow ClientCidr); else PrivateIpAddress.
DRIVER_IP=$(aws ec2 describe-instances --region "${AWS_REGION}" \
  --filters "Name=tag:Name,Values=${STACK}-driver" "Name=instance-state-name,Values=running" \
  --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
# fallback if the driver has no public IP:
if [[ -z "${DRIVER_IP}" || "${DRIVER_IP}" == "None" ]]; then
  DRIVER_IP=$(aws ec2 describe-instances --region "${AWS_REGION}" \
    --filters "Name=tag:Name,Values=${STACK}-driver" "Name=instance-state-name,Values=running" \
    --query 'Reservations[0].Instances[0].PrivateIpAddress' --output text)
fi
export CONNECT="sc://${DRIVER_IP}:50051"
echo "$CONNECT"
```

Do **not** use the stack’s legacy `ConnectEndpoint` NLB DNS when `ExposeConnect=true`.
Leave `ExposeConnect=false` and open `:50051` only to `ClientCidr`.

### Smoke test

```python
from pyspark.sql import SparkSession

ENDPOINT = "sc://10.0.1.20:50051"  # driver private or public IP

spark = SparkSession.builder.remote(ENDPOINT).getOrCreate()
spark.sql("SELECT 1 AS hello").show()

# Fully-qualified Glue table (catalog.database.table)
spark.sql("SELECT count(*) AS n FROM glue.weft_demo.orders").show()
spark.sql("SELECT * FROM glue.weft_demo.orders LIMIT 10").show()
```

### Distributed lakehouse harness (optional)

If you populated SF-scale Glue tables with [`bench/sf100/`](../bench/sf100/):

```sh
# Wrapper auto-resolves the driver IP when CONNECT is unset:
STACK=weft-sf100 SUITE=all ./bench/sf100/remeasure-distributed.sh

# Or explicit:
CONNECT=sc://<driver-ip>:50051 SUITE=all ./bench/sf100/remeasure-distributed.sh

# Or call the harness directly:
WEFT_DISTRIBUTED_STRICT=1 python3 bench/sf100/run-spark-connect.py \
  --endpoint sc://<driver-ip>:50051 \
  --suite tpch --sf 100 --glue-db tpch_sf100 \
  --region "${AWS_REGION}" \
  --json results/tpch-sf100-ec2.jsonl --resume --skip-worker-preflight
```

The harness sets `spark.sql.catalog.glue.type=glue` on the client; keep stack
`CatalogConf` aligned so workers resolve the same catalog.

---

## SF100 topology (canonical)

**Keep this table in sync with any published SF100 EC2 numbers** (KAN-14). Same
instance shapes as the EKS overlay
[`deploy/helm/weft/values-sf100.yaml`](../deploy/helm/weft/values-sf100.yaml) —
Graviton **arm64** AMI required (`c6g` / `m8g`).

| Role | Count | Instance type | vCPU / RAM | Root EBS | Spill EBS (`/var/lib/weft/spill`) | ASG |
|------|------:|---------------|------------|----------|-----------------------------------|-----|
| Driver (Spark Connect `:50051`) | 1 | **`c6g.xlarge`** | 4 / 8 GiB | **100 GiB gp3** | 0 (optional; root is enough for driver) | Min=Max=Desired=**1** |
| Workers (Flight `:50561`) | 2 | **`m8g.8xlarge`** | 32 / 128 GiB | 40 GiB gp3 (default) | **500 GiB gp3** each | Min=Max=Desired=**2** (pinned; no scale policies) |

| Engine env (SF100) | Value | Where |
|--------------------|-------|-------|
| `WEFT_DISTRIBUTED_STRICT` | `1` | driver (`--distributed-strict true`) |
| `WEFT_PREFER_HASH_JOIN` | `auto` | driver + workers (default; forced values are legacy — see `docs/runtime-contract.md`) |
| `WEFT_MEMORY_LIMIT_BYTES` | `42949672960` (40 Gi) | workers (DataFusion spill pool) |
| `WEFT_SHUFFLE_SPILL_BYTES` | `8589934592` (8 Gi) | workers (shuffle cache threshold) |
| `WEFT_SHUFFLE_PARTITIONS` | `32` | driver (≈ worker vCPU; > worker count spreads shuffle + reduces skew) |
| `WEFT_WORKER_COUNT` / shards | `2` | fixed; matches ASG size |
| Catalog | Glue Parquet `tpch_sf100` / `tpcds_sf100` | `CatalogConf` on driver **and** workers |
| Dataset | `s3://weft-artifacts-<account>/{tpch,tpcds}-sf100/` | Parquet only for publishable runs |

Memory invariant (same as Helm SF100): `memoryLimitBytes + shuffleSpillBytes + headroom
≤ instance RAM`. On `m8g.8xlarge` (128 GiB) that is 40 Gi + 8 Gi tracked ≈ 48 Gi, leaving
~80 Gi native headroom for Arrow / S3 / Glue CLI — do not raise both pools to the full
limit. Worker cgroup is `MemoryMax=112G` / `MemoryHigh=96G`.

> **Why `m8g.8xlarge`, not `m8g.4xlarge` (KAN-14 rerun, 2026-07-28):** DataFusion 54's
> `HashJoin` build side is **not spillable**, so at SF100 the big `lineitem ⋈ orders`
> build lands mostly *outside* the `FairSpillPool`. On `m8g.4xlarge` (64 GiB, 20 Gi pool)
> every multi-fact TPC-H join (Q2/Q3/Q4/Q5/Q7/Q8) blew past the 56 Gi worker cgroup and
> the stage aborted — the driver reported `register `result`: no batches` while the
> single-table scans (Q1/Q6) still passed. `m8g.8xlarge` (128 GiB, 40 Gi pool) plus
> `WEFT_SHUFFLE_PARTITIONS=32` (≈ worker vCPU; removes the 2-bucket skew that pinned all
> shuffle onto one worker) gives the join real headroom. Keep `values-sf100.yaml` in sync.

### Deploy recipe (copy/paste)

Bake an **arm64** Weft AMI first (`./deploy/packer/build-ami.sh` with a
`linux/aarch64` `weft` binary), then:

```sh
export AWS_REGION=us-west-2
export BUCKET=weft-artifacts-$(aws sts get-caller-identity --query Account --output text)
export AMI_ID=ami-…                 # arm64 Packer output
export VPC_ID=vpc-…
export SUBNETS=subnet-…,subnet-…    # public subnets only if the laptop hits the driver public IP
MY_IP=$(curl -fsS https://checkip.amazonaws.com)/32

./deploy/cloudformation/deploy-stack.sh \
  --ami "${AMI_ID}" \
  --vpc "${VPC_ID}" \
  --subnets "${SUBNETS}" \
  --stack weft-sf100 \
  --region "${AWS_REGION}" \
  --driver-type c6g.xlarge \
  --worker-type m8g.8xlarge \
  --workers 2 \
  --driver-root-size 100 \
  --worker-root-size 40 \
  --driver-spill-size 0 \
  --worker-spill-size 500 \
  --memory-limit-bytes 42949672960 \
  --shuffle-spill-bytes 8589934592 \
  --shuffle-partitions 32 \
  --distributed-strict true \
  --prefer-hash-join false \
  --data-buckets "arn:aws:s3:::${BUCKET},arn:aws:s3:::${BUCKET}/*" \
  --glue true \
  --catalog-conf "spark.sql.catalog.glue.type=glue;spark.sql.catalog.glue.region=${AWS_REGION};spark.sql.catalog.glue.warehouse=s3://${BUCKET}/warehouse" \
  --expose-connect false \
  --client-cidr "${MY_IP}"
```

**Never** pass `--expose-connect true` for SF100. Connect to the driver IP:

```sh
STACK=weft-sf100 SUITE=all ./bench/sf100/remeasure-distributed.sh
# or SUITE=tpch / SUITE=tpcds
```

Cross-links: EKS twin in [`distributed-k8s.md` § SF100 topology](distributed-k8s.md#sf100-topology);
harness notes in [`bench/sf100/README.md`](../bench/sf100/README.md);
definition of done D-4.* in [`DISTRIBUTED_DONE.md`](DISTRIBUTED_DONE.md).

---

## Bootstrap contract (AMI)

`/usr/local/lib/weft/bootstrap.sh` (oneshot `weft-bootstrap.service`):

1. Mounts the spill volume (if present) at `/var/lib/weft/spill` — detection is
   name-agnostic: the largest unmounted, unpartitioned, non-root whole disk
   wins (plain `lsblk` scan; Nitro NVMe enumeration order is not stable, so no
   `/dev/nvmeX` name hints). Persists via `/etc/fstab`; safe to re-run.
2. Reads instance tags (`weft:role`, `weft:worker-count`, `weft:worker-asg`,
   `weft:catalog-conf`, …)
3. **Workers:** waits for InService peers, assigns `WEFT_SHARD_INDEX`, UPSERTs the
   shared Route53 A RRSet from the InService peers **plus its own IP** (self may
   not be InService yet on a cold start; dead instances are pruned on every boot)
4. Writes `/etc/weft/weft.env` **atomically** (temp file + rename — a killed
   bootstrap can never leave a truncated env file) and **enables** (never starts)
   `weft-driver` or `weft-worker`

Ordering (KAN-58): the role units declare `Requires=`/`After=` on
`weft-bootstrap.service` and bootstrap declares `Before=` on them — so bootstrap
must never `systemctl start`/`--now` a role unit from inside its own oneshot
(that closed the cycle that deadlocked fresh instances until `TimeoutStartSec`).
First boot: UserData runs `systemctl start weft-bootstrap.service`, waits for it,
then `systemctl enable --now weft-<role>`. Reboots: the WantedBy/Requires/After
graph re-runs bootstrap (idempotent) before the role unit. Shutdown / scale-in:
`ExecStop` runs `bootstrap.sh --deregister`, which removes **only this
instance's IP** from the RRSet — never a full re-sync (at stop time the ASG may
still list the instance InService, which would re-add its dying IP and leave it
stale forever). Workers that die without `ExecStop` are pruned by the next
worker boot's full re-sync.

| Role | Env written |
|------|-------------|
| Driver | `WEFT_WORKER_SERVICE`, `WEFT_WORKER_PORT=50561`, `WEFT_WORKER_COUNT`, `WEFT_SHUFFLE_PARTITIONS`, `WEFT_AWS_BIN`, `AWS_REGION`, spill/`TMPDIR`, optional memory/shuffle thresholds, optional `WEFT_CATALOG_CONF` |
| Worker | `WEFT_WORKER_COUNT`, `WEFT_SHARD_INDEX`, same AWS/spill/catalog/memory |

---

## Security checklist

- [ ] IMDSv2 required on launch templates
- [ ] No static AWS keys in the AMI, user-data, or `CatalogConf`
- [ ] Instance profiles scoped to needed S3 ARNs (`DataBucketArns`) + Glue when enabled
- [ ] Worker SG: Flight **50561** only from driver SG + self (shuffle)
- [ ] Driver SG: Connect **50051** only from `ClientCidr` (tighten further in production)
- [ ] Prefer SSM over SSH; `KeyName` optional
- [ ] Spill + root EBS encrypted (`Encrypted: true` in the template)
- [ ] Do **not** set `WEFT_SHUFFLE_SPILL_DIR` (force-spill; invalidates benches)
- [ ] Private subnets + VPC endpoints/NAT for S3 and `glue.<region>.amazonaws.com`

---

## Operate / tear down

```sh
# Update AMI / instance types / CatalogConf (then replace instances)
./deploy/cloudformation/deploy-stack.sh --ami ami-new ... # same flags as create

# Tear down the compute stack (does not delete your S3 data or Glue DB)
aws cloudformation delete-stack --region "${AWS_REGION}" --stack-name weft-demo
aws cloudformation wait stack-delete-complete --region "${AWS_REGION}" --stack-name weft-demo

# Optional: drop Glue demo objects
aws glue delete-table --region "${AWS_REGION}" --database-name "${GLUE_DB}" --name orders
aws glue delete-database --region "${AWS_REGION}" --name "${GLUE_DB}"
```

---

## Troubleshooting

| Symptom | Likely cause |
|---------|----------------|
| Inflated / duplicated aggregates | Missing `WEFT_SHARD_INDEX` / `WEFT_WORKER_COUNT` on workers, or the **same** `WEFT_SHARD_INDEX` on every worker (each then reads shard 0's file subset, so single-file tables count 2× and size-balanced multi-file tables ~1× with skew) — check `/etc/weft/weft.env` on *every* worker |
| Fresh instance: bootstrap "starting" for 5 min, then no `/etc/weft/weft.env` and the role unit never starts | Pre-KAN-58 AMI: bootstrap started the role unit from inside its own oneshot while the role unit `Requires=`/`After=` bootstrap — a circular wait killed at `TimeoutStartSec`. Rebake from current `deploy/packer`; live fix: copy the repo `bootstrap.sh` over `/usr/local/lib/weft/bootstrap.sh`, then `systemctl reset-failed weft-bootstrap && systemctl start weft-bootstrap && systemctl start weft-<role>` |
| Queries fail on dead worker IPs ("no free task slots") | Stale A records in `workers.<zone>` — instances killed without `ExecStop` (or pre-KAN-58 deregistration, which re-synced the full InService set and could re-add the dying IP). `sudo systemctl restart weft-bootstrap` on any live worker forces a full re-sync that prunes dead IPs; graceful stops now remove only the instance's own IP |
| Driver fails membership vs count | Route53 A set size ≠ `WorkerCount` — wait for all workers InService; check worker Route53 IAM |
| Workers never receive tasks | Driver cannot resolve `WEFT_WORKER_SERVICE` — private zone VPC association / SG |
| `aws glue … EntityNotFound` | Wrong database/table name or region in `CatalogConf` |
| `AccessDenied` on Glue/S3 | Deployed with `--glue false` or incomplete `DataBucketArns` (need bucket **and** `/*`) |
| Catalog works locally on driver but distributed scan fails | Workers missing `WEFT_CATALOG_CONF` — set stack `CatalogConf`, replace instances |
| Spill fills root volume | Spill size `0`, or no eligible device: bootstrap picks the largest **unmounted, unpartitioned, non-root** whole disk (no fixed device names) — `lsblk -f` and the bootstrap log line `no spill block device found` tell you which disks were skipped and why (mounted / has partitions) |
| OOM on large queries | Empty memory/shuffle thresholds — set `MemoryLimitBytes` + `ShuffleSpillBytes` |
| `CatalogConf` deploy error / truncated tag | Value must be ≤256 characters |

```sh
# on an instance (via SSM)
journalctl -u weft-bootstrap -u weft-driver -u weft-worker -e
cat /etc/weft/weft.env
sudo -u weft /usr/local/bin/aws glue get-databases --region "$AWS_REGION"
```

---

## File map

| Path | Role |
|------|------|
| [`deploy/packer/weft-runtime.pkr.hcl`](../deploy/packer/weft-runtime.pkr.hcl) | Packer AMI |
| [`deploy/packer/files/bootstrap.sh`](../deploy/packer/files/bootstrap.sh) | Boot: spill mount + shard index + DNS upsert + atomic env (+ catalog); shutdown: self-IP DNS deregistration |
| [`deploy/packer/files/systemd/`](../deploy/packer/files/systemd/) | `weft-bootstrap` / `weft-driver` / `weft-worker` |
| [`deploy/packer/build-ami.sh`](../deploy/packer/build-ami.sh) | AMI build wrapper |
| [`deploy/cloudformation/weft-cluster.yaml`](../deploy/cloudformation/weft-cluster.yaml) | CFN stack |
| [`deploy/cloudformation/deploy-stack.sh`](../deploy/cloudformation/deploy-stack.sh) | Deploy wrapper |
| [`docs/catalogs.md`](catalogs.md) | Catalog SPI + Spark `spark.sql.catalog.*` keys |
| [`crates/weft-catalog-glue/`](../crates/weft-catalog-glue/) | Glue provider (AWS CLI shell-out) |
