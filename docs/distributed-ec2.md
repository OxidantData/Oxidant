# Running distributed Oxidant on EC2 (CloudFormation + ASG)

This is the **EKS-free** data-plane path: one Spark Connect driver EC2 instance + N
Arrow Flight worker instances in fixed-size Auto Scaling Groups. Worker membership is
**private IPs from the worker ASG** (IAM `Describe*`), not Route53 and not UDP broadcast.
It matches the OSS runtime contract in [`runtime-contract.md`](runtime-contract.md).

For catalog SPI details (Hive / Glue / REST), see [`catalogs.md`](catalogs.md).
For the future full platform (SSO, gateway operator, Terraform), see
[`deployment.md`](deployment.md).

## Architecture

```
PySpark / oxidant-bench  -->  oxidant driver :50051  (direct IP — no LB)
                              |
                              |  OXIDANT_WORKERS=private-ip:50561,…
                              |  (from ASG InService + EC2 DescribeInstances)
                              v
                         worker EC2s :50561 (Flight, VPC private)
                              |
                              |  aws glue + s3 (instance role)
                              v
                         Glue Data Catalog + S3 data
```

### Why not Route53 / broadcast?

| Approach | Verdict |
|----------|---------|
| **ASG → private IP list** (chosen) | Same idea as EMR/YARN / Databricks control plane: cluster manager hands the driver executor addresses. Auth is IAM instance role; traffic stays on private IPs; SGs restrict Flight to driver↔workers. |
| Route53 multi-A | Extra moving part; TTL/upsert races left the driver with an empty worker set and silent local scans. Kept only as unused legacy tags on older stacks. |
| UDP / LAN broadcast | **Not used.** Insecure, spoofable, and does not work across AWS subnets/AZs. |

### Driver vs workers (Spark EMR / Databricks parity)

Treat the Connect driver like a **Spark driver / EMR master**, not like an executor:

| Role | Does | Must not |
|------|------|----------|
| Driver (`c6g.xlarge`) | Spark Connect, planning, stage orchestration, tiny finalize | Scan SF100 facts, build large hash joins, hold worker memory limits |
| Workers (`m8g.*`) | Parquet/Iceberg scans, shuffle, joins/aggs under FairSpillPool | Sit idle while the driver OOMs |

**Never silently fall back to driver-local execution** when workers are configured.
`OXIDANT_DISTRIBUTED_STRICT=1` (CFN `--distributed-strict true`) makes that an error.
Bootstrap waits until the worker ASG has `WorkerCount` InService instances, resolves
their **private** IPs, and pins `OXIDANT_WORKERS=ip:50561,…` before Connect starts
(same gate as EMR waiting for core nodes).

**How to verify distribution (do this before trusting SF100 numbers):**

1. During a multi-fact query, CloudWatch **CPUUtilization** on workers must rise; driver stays comparatively light.
2. Worker `NetworkIn` should dominate (S3 + shuffle), not the driver’s.
3. `journalctl -u oxidant-worker` shows `Oxidant stage summary:` lines with `num_partitions` ≥ 200.
4. If workers are ~0% CPU while the driver climbs, **stop the run** — results are not distributed.

| Piece | Implementation |
|-------|----------------|
| AMI | Packer AL2023 image (`deploy/packer/`) with `oxidant`, AWS CLI v2, hardened `oxidant` user (uid 65532), systemd units |
| Driver | ASG `Min=Max=Desired=1`, `oxidant spark server --port 50051` |
| Workers | ASG `Min=Max=Desired=WorkerCount` (pinned — no scale policies) |
| Discovery | Driver bootstrap: ASG InService instance IDs → EC2 private IPs → `OXIDANT_WORKERS` |
| Sharding | Boot assigns `OXIDANT_SHARD_INDEX` = position in sorted InService instance IDs |
| Spill | Optional second EBS volume mounted at `/var/lib/oxidant/spill` (`TMPDIR`) |
| Auth | Instance profiles only — no credentials in the AMI |
| Catalog | Optional `OXIDANT_CATALOG_CONF` (Glue) on **driver and workers** via the `CatalogConf` stack parameter |

Workers are **not** behind a single L4 VIP for Flight (shuffle state is per-worker).
Connect clients talk to the **single driver IP** (private from VPC/VPN/bastion, or
public IP + `ClientCidr` for laptop honesty runs).

### Do not put Spark Connect behind an NLB

**Bad approach — do not use for Oxidant data-plane / SF100 / TPC honesty runs.**

An internet-facing Network Load Balancer on `:50051` (`ExposeConnect=true`) is the
wrong model for this workload:

| Reality | Why an NLB does not help |
|---------|--------------------------|
| One Connect driver | Nothing to load-balance; Databricks / LakeSail / OSS Spark Connect also terminate on the driver (or a control-plane proxy), not an L4 VIP in front of query compute |
| Workers are stateful Flight peers | Shuffle and shard index are per-instance; never put workers behind a shared VIP |
| Honesty / bench runs | NLB health checks + target registration add minutes of false “unhealthy” while `oxidant-driver` is already listening — wasted wall clock, not signal |
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
6. [Point Oxidant at Glue](#4-point-oxidant-at-the-glue-catalog)
7. [Run a query](#5-run-a-query)
8. [Operate / tear down](#operate--tear-down)

---

## Prerequisites

### Tools (laptop / CI)

- AWS CLI v2 configured for the target account/region
- [Packer](https://developer.hashicorp.com/packer) ≥ 1.9 (Amazon plugin installed via `packer init`)
- Rust toolchain matching [`rust-toolchain.toml`](../rust-toolchain.toml) (to build `oxidant`), **or** a pre-built linux/amd64 `oxidant` binary
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

File-list sharding uses a fixed `OXIDANT_WORKER_COUNT`
plus a stable `OXIDANT_SHARD_INDEX` per worker. Changing N under an ASG scaling policy
without coordinated re-assignment silently duplicates or drops shards. The template
sets `MinSize = MaxSize = DesiredCapacity = WorkerCount` and adds **no** scaling
policies. Leave `ShufflePartitions` empty so the engine applies its Spark-like default
(`max(200, worker_vcpus)`); do not pin shuffle to `WorkerCount`.

---

## Prepare S3 + Glue Data Catalog

Oxidant’s Glue provider (`oxidant-catalog-glue`) uses `aws-sdk-glue` in-process with the
standard AWS credential chain. Credentials come from the
**instance profile** (IMDSv2). Tables must already exist in Glue and point at readable
S3 locations (Parquet is the well-supported path for remote object stores today; see
[`catalogs.md`](catalogs.md)).

### 1. Create (or choose) an S3 bucket

```sh
export AWS_REGION=us-west-2
export BUCKET=my-oxidant-data          # globally unique
export PREFIX=warehouse            # CTAS default root (optional)
export GLUE_DB=oxidant_demo

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
  --database-input "{\"Name\":\"${GLUE_DB}\",\"Description\":\"Oxidant EC2 demo\"}"
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

### 4. Note the ARNs and catalog conf you will pass to CloudFormation

```text
DataBucketArns:
  arn:aws:s3:::my-oxidant-data,arn:aws:s3:::my-oxidant-data/*

EnableGlueAccess: true

CatalogConf (≤256 chars, applied to driver AND workers):
  spark.sql.catalog.glue.type=glue;spark.sql.catalog.glue.region=us-west-2;spark.sql.catalog.glue.warehouse=s3://my-oxidant-data/warehouse
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
cargo build -p oxidant-cli --release

# cross-check: binary must be linux/amd64 if you bake from macOS — build in CI,
# in a linux container, or pass --binary-url to a linux artifact instead.
./deploy/packer/build-ami.sh --binary ./target/release/oxidant --region "${AWS_REGION}"
```

Or:

```sh
./deploy/packer/build-ami.sh --binary-url https://example.com/releases/oxidant-linux-amd64
```

Packer prints a new `ami-…` id. Save it:

```sh
export AMI_ID=ami-0123456789abcdef0
```

The image:

- Installs `/usr/local/bin/oxidant` and AWS CLI v2
- Creates non-root user `oxidant` (uid/gid **65532**)
- Requires **IMDSv2** on the builder (launch templates also set `HttpTokens=required`)
- Disables SSH password auth; prefer SSM Session Manager
- Enables `dnf-automatic` for security updates
- Ships `oxidant-bootstrap.service` + role units (`oxidant-driver` / `oxidant-worker`)

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
  --stack oxidant-demo \
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

Template: [`deploy/cloudformation/oxidant-cluster.yaml`](../deploy/cloudformation/oxidant-cluster.yaml).

> **SF100 honesty runs** use a fixed Graviton topology — see
> [SF100 topology (canonical)](#sf100-topology-canonical) below. Do not publish
> numbers from the smaller `m6i.*` demo sizes in the example above.

What the stack creates:

- IAM instance profiles (SSM + optional S3/Glue; driver/workers: ASG + EC2 Describe for membership)
- Security groups (Connect `50051` from `ClientCidr`; Flight `50561` driver→workers and worker↔worker)
- Private hosted zone (`HostedZoneName`, default `oxidant.internal`)
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
| `DriverSpillVolumeSize` / `Type` | `100` / `gp3` | Extra volume → `/var/lib/oxidant/spill` (`0` = skip) |
| `WorkerSpillVolumeSize` / `Type` | `200` / `gp3` | Extra volume → `/var/lib/oxidant/spill` (`0` = skip) |
| `MemoryLimitBytes` | empty | `OXIDANT_MEMORY_LIMIT_BYTES` |
| `ShuffleSpillBytes` | empty | `OXIDANT_SHUFFLE_SPILL_BYTES` |
| `CatalogConf` | empty | `OXIDANT_CATALOG_CONF` on driver **and** workers (≤256 chars) |
| `DataBucketArns` | empty | S3 ARNs on the instance profiles |
| `EnableGlueAccess` | `false` | Glue `Get*` API permissions on instance profiles |
| `ExposeConnect` | `false` | **Deprecated / do not use** — creates an internet-facing Connect NLB (bad for this data plane; see above) |
| `ClientCidr` | `10.0.0.0/8` | Who may hit driver `:50051` (use your laptop `/32` for public-IP honesty runs) |
| `HostedZoneName` | `oxidant.internal` | Private zone created in the VPC |
| `KeyName` | empty | Optional SSH key (SSM preferred) |

When `MemoryLimitBytes` and `ShuffleSpillBytes` are both empty, `SpillStore::from_env`
stays off and the in-memory shuffle cache is **unbounded** — fine for smoke tests,
not safe for large queries. Set both for real workloads (the SF100 invariant below:
memory + shuffle spill + headroom ≤ instance RAM).

Stack outputs include `ConnectEndpoint`, `WorkerDnsName`, ASG names, and `HostedZoneId`.

---

## 3. Verify the cluster

Wait until both ASGs show healthy capacity:

```sh
STACK=oxidant-demo
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

# Oxidant env + services
cat /etc/oxidant/oxidant.env
systemctl status oxidant-bootstrap oxidant-driver --no-pager
journalctl -u oxidant-bootstrap -u oxidant-driver -e --no-pager | tail -n 80

# workers must be concrete private Flight endpoints (ASG membership)
grep '^OXIDANT_WORKERS=' /etc/oxidant/oxidant.env
```

Expect `/etc/oxidant/oxidant.env` on the **driver** to include roughly
(`OXIDANT_AWS_BIN` is consumed only by the AMI's own operator scripts —
`bootstrap.sh`/`shard-resolve.sh`; the engine reads Glue in-process via aws-sdk-glue):

```bash
OXIDANT_AWS_BIN=/usr/local/bin/aws
AWS_REGION=us-west-2
OXIDANT_WORKERS=172.31.10.11:50561,172.31.20.22:50561
OXIDANT_WORKER_COUNT=2
OXIDANT_DISTRIBUTED_STRICT=1
OXIDANT_CATALOG_CONF="spark.sql.catalog.glue.type=glue;..."
TMPDIR=/var/lib/oxidant/spill
```

On a **worker**, expect `OXIDANT_SHARD_INDEX=0` or `1` (not both the same), plus the same
`OXIDANT_CATALOG_CONF` / `OXIDANT_WORKER_COUNT`.

---

## 4. Point Oxidant at the Glue catalog

There are two complementary ways to register Glue. For **distributed** queries both
driver and workers must know the catalog — prefer stack `CatalogConf` (section 2).

### Option A — stack parameter (recommended)

Pass `--catalog-conf` / `CatalogConf` at deploy time (shown above). Bootstrap writes
`OXIDANT_CATALOG_CONF` into `/etc/oxidant/oxidant.env` for every instance. The CLI reads that env
at process start (`oxidant spark server` and `oxidant worker`).

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
    .config("spark.sql.catalog.glue.warehouse", "s3://my-oxidant-data/warehouse")
    .getOrCreate()
)
```

Client-side config registers the catalog on the **Connect server (driver)**. Workers
still need `OXIDANT_CATALOG_CONF` (Option A) so distributed stages can resolve Glue/Parquet
locations when they execute. Do not rely on client-only config for multi-worker scans.

### How Glue auth works on EC2

1. Instance profile (from `EnableGlueAccess` + `DataBucketArns`) provides credentials via IMDSv2.
2. `oxidant-catalog-glue` resolves them in-process via `aws-sdk-glue` / the standard AWS
   credential chain, then calls `GetDatabases`/`GetTables`/`GetTable` directly — no AWS CLI
   subprocess (the AMI still bundles the CLI for operator scripts).
3. Table `StorageDescriptor.Location` (`s3://…`) is read with the same role’s S3 permissions.

No static keys belong in the AMI, user-data, or `CatalogConf`.

---

## 5. Run a query

### Discover the Connect endpoint (driver IP — never NLB)

```sh
STACK=oxidant-demo
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
spark.sql("SELECT count(*) AS n FROM glue.oxidant_demo.orders").show()
spark.sql("SELECT * FROM glue.oxidant_demo.orders LIMIT 10").show()
```

---

## SF100 topology (canonical)

**Keep this table in sync with any published SF100 EC2 numbers** (KAN-14).
Graviton **arm64** AMI required (`c6g` / `m8g`). Target matches **Spark EMR** honesty
hardware: `c6g.xlarge` master + 2× `m8g.4xlarge` cores with 500 GiB gp3 spill.

| Role | Count | Instance type | vCPU / RAM | Root EBS | Spill EBS (`/var/lib/oxidant/spill`) | ASG |
|------|------:|---------------|------------|----------|-----------------------------------|-----|
| Driver (Spark Connect `:50051`) | 1 | **`c6g.xlarge`** | 4 / 8 GiB | **100 GiB gp3** | 0 (root is enough) | Min=Max=Desired=**1** |
| Workers (Flight `:50561`) | 2 | **`m8g.4xlarge`** | 16 / 64 GiB | 40 GiB gp3 (default) | **500 GiB gp3** each | Min=Max=Desired=**2** (pinned; no scale policies) |

| Engine env (SF100) | Value | Where |
|--------------------|-------|-------|
| `OXIDANT_DISTRIBUTED_STRICT` | `1` | driver (`--distributed-strict true`) — refuse local fallback when workers are configured but unreachable |
| `OXIDANT_PREFER_HASH_JOIN` | `auto` | driver + workers (Spark `preferSortMergeJoin`; large builds → SMJ) |
| `OXIDANT_MEMORY_LIMIT_BYTES` | e.g. `28895544320` (~27 Gi) | **workers only** (bootstrap skips this on the driver — a fat pool on an 8 GiB c6g OOMs Connect) |
| `OXIDANT_SHUFFLE_SPILL_BYTES` | `8589934592` (8 Gi) | **workers only** |
| `OXIDANT_SHUFFLE_PARTITIONS` | unset or `200` | driver — engine default is `max(200, worker_vcpus)`; empty CFN **must not** stamp `WorkerCount` |
| `OXIDANT_AQE` | `1` (default on) | driver + workers — coalesce toward 64 MiB advisory partitions |
| `OXIDANT_S3_CACHE_DIR` | empty on driver; `/var/lib/oxidant/s3cache` on workers | whole-object cache; workers also set `OXIDANT_S3_CACHE_MAX_OBJECT_BYTES=2Gi` so SF100 single-file facts stay on ranged GETs |
| `OXIDANT_STAGE_TIMEOUT_MS` | `3600000` (1 h) | driver + workers (default 10 min is too short for cold SF100) |
| Catalog | Glue Iceberg / Parquet `tpch_sf100` / `tpcds_sf100` | `CatalogConf` on driver **and** workers |
| Dataset | Prefer **many** Snappy Parquet files (or Iceberg) under `warehouse/` — a single ~21 GiB `lineitem.parquet` is a known foot-gun; see [`bench/tpch/SF100-EC2-NOTES.md`](../bench/tpch/SF100-EC2-NOTES.md) | Official TPC `dbgen`/`dsdgen` via `bench/tpc/prepare.sh` |

Memory invariant: `memoryLimitBytes + shuffleSpillBytes + headroom
≤ instance RAM`. On `m8g.4xlarge` (64 GiB) prefer ~27 Gi pool + 8 Gi shuffle cache, leaving
native headroom for Arrow / S3 — do not raise both pools to the full limit.

> **Spark EMR parity (why this topology works now):** DataFusion's `HashJoin` build is still
> not spillable, but `auto` join selection admits HashJoin only when the build fits Spark's
> `canBuildLocalHashMap` budget (`10 MiB × shuffle_partitions / 2`). Large SF100 equi-joins
> therefore plan SortMergeJoin (spillable external sort). Combined with a ≥200 shuffle
> partition floor (never `WorkerCount=2`), EMR-class `m8g.4xlarge` workers stay under the
> cgroup. Pre-parity (KAN-14, 2026-07-28) required `m8g.8xlarge` because HashJoin stayed on
> the critical path with a 2-bucket shuffle.

### Deploy recipe (copy/paste)

Bake an **arm64** Oxidant AMI first (`./deploy/packer/build-ami.sh` with a
`linux/aarch64` `oxidant` binary), then:

```sh
export AWS_REGION=us-west-2
export BUCKET=oxidant-artifacts-$(aws sts get-caller-identity --query Account --output text)
export AMI_ID=ami-…                 # arm64 Packer output
export VPC_ID=vpc-…
export SUBNETS=subnet-…,subnet-…    # public subnets only if the laptop hits the driver public IP
MY_IP=$(curl -fsS https://checkip.amazonaws.com)/32

./deploy/cloudformation/deploy-stack.sh \
  --ami "${AMI_ID}" \
  --vpc "${VPC_ID}" \
  --subnets "${SUBNETS}" \
  --stack oxidant-sf100 \
  --region "${AWS_REGION}" \
  --driver-type c6g.xlarge \
  --worker-type m8g.4xlarge \
  --workers 2 \
  --driver-root-size 100 \
  --worker-root-size 40 \
  --driver-spill-size 0 \
  --worker-spill-size 500 \
  --memory-limit-bytes 28895544320 \
  --shuffle-spill-bytes 8589934592 \
  --distributed-strict true \
  --prefer-hash-join auto \
  --data-buckets "arn:aws:s3:::${BUCKET},arn:aws:s3:::${BUCKET}/*" \
  --glue true \
  --catalog-conf "spark.sql.catalog.glue.type=glue;spark.sql.catalog.glue.region=${AWS_REGION};spark.sql.catalog.glue.warehouse=s3://${BUCKET}/warehouse" \
  --expose-connect false \
  --client-cidr "${MY_IP}"
```

**Never** pass `--expose-connect true` for SF100. Connect to the driver IP
(`sc://<driver-ip>:50051`) as shown in [Run a query](#5-run-a-query).

---

## Bootstrap contract (AMI)

`/usr/local/lib/oxidant/bootstrap.sh` (oneshot `oxidant-bootstrap.service`):

1. Mounts the spill volume (if present) at `/var/lib/oxidant/spill` — detection is
   name-agnostic: the largest unmounted, unpartitioned, non-root whole disk
   wins (plain `lsblk` scan; Nitro NVMe enumeration order is not stable, so no
   `/dev/nvmeX` name hints). Persists via `/etc/fstab`; safe to re-run.
2. Reads instance tags (`oxidant:role`, `oxidant:worker-count`, `oxidant:worker-asg`,
   `oxidant:catalog-conf`, …)
3. **Workers:** waits for InService peers, assigns `OXIDANT_SHARD_INDEX` (no DNS upsert)
4. **Driver:** waits for `WorkerCount` InService workers, resolves **private IPs** via
   EC2 `DescribeInstances`, pins `OXIDANT_WORKERS=ip:50561,…`
5. Writes `/etc/oxidant/oxidant.env` **atomically** (temp file + rename — a killed
   bootstrap can never leave a truncated env file) and **enables** (never starts)
   `oxidant-driver` or `oxidant-worker`

Ordering (KAN-58): the role units declare `Requires=`/`After=` on
`oxidant-bootstrap.service` and bootstrap declares `Before=` on them — so bootstrap
must never `systemctl start`/`--now` a role unit from inside its own oneshot
(that closed the cycle that deadlocked fresh instances until `TimeoutStartSec`).
First boot: UserData runs `systemctl start oxidant-bootstrap.service`, waits for it,
then `systemctl enable --now oxidant-<role>`. Reboots: the WantedBy/Requires/After
graph re-runs bootstrap (idempotent) before the role unit. After replace/scale,
re-run bootstrap on the driver (or replace the driver instance) so `OXIDANT_WORKERS`
picks up the new private IP set.

| Role | Env written |
|------|-------------|
| Driver | `OXIDANT_WORKERS` (private `ip:50561` list), `OXIDANT_WORKER_COUNT`, optional `OXIDANT_SHUFFLE_PARTITIONS` / `OXIDANT_DISTRIBUTED_STRICT`, `OXIDANT_AWS_BIN` (AMI operator scripts only), `AWS_REGION`, spill/`TMPDIR`, optional catalog/memory |
| Worker | `OXIDANT_WORKER_COUNT`, `OXIDANT_SHARD_INDEX`, same AWS/spill/catalog/memory |

---

## Security checklist

- [ ] IMDSv2 required on launch templates
- [ ] No static AWS keys in the AMI, user-data, or `CatalogConf`
- [ ] Instance profiles scoped to needed S3 ARNs (`DataBucketArns`) + Glue when enabled
- [ ] Worker SG: Flight **50561** only from driver SG + self (shuffle)
- [ ] Driver SG: Connect **50051** only from `ClientCidr` (tighten further in production)
- [ ] Prefer SSM over SSH; `KeyName` optional
- [ ] Spill + root EBS encrypted (`Encrypted: true` in the template)
- [ ] Do **not** set `OXIDANT_SHUFFLE_SPILL_DIR` (force-spill; invalidates benches)
- [ ] Private subnets + VPC endpoints/NAT for S3 and `glue.<region>.amazonaws.com`

---

## Operate / tear down

```sh
# Update AMI / instance types / CatalogConf (then replace instances)
./deploy/cloudformation/deploy-stack.sh --ami ami-new ... # same flags as create

# Tear down the compute stack (does not delete your S3 data or Glue DB)
aws cloudformation delete-stack --region "${AWS_REGION}" --stack-name oxidant-demo
aws cloudformation wait stack-delete-complete --region "${AWS_REGION}" --stack-name oxidant-demo

# Optional: drop Glue demo objects
aws glue delete-table --region "${AWS_REGION}" --database-name "${GLUE_DB}" --name orders
aws glue delete-database --region "${AWS_REGION}" --name "${GLUE_DB}"
```

---

## Troubleshooting

| Symptom | Likely cause |
|---------|----------------|
| Inflated / duplicated aggregates | Missing `OXIDANT_SHARD_INDEX` / `OXIDANT_WORKER_COUNT` on workers, or the **same** `OXIDANT_SHARD_INDEX` on every worker (each then reads shard 0's file subset, so single-file tables count 2× and size-balanced multi-file tables ~1× with skew) — check `/etc/oxidant/oxidant.env` on *every* worker |
| Fresh instance: bootstrap "starting" for 5 min, then no `/etc/oxidant/oxidant.env` and the role unit never starts | Pre-KAN-58 AMI: bootstrap started the role unit from inside its own oneshot while the role unit `Requires=`/`After=` bootstrap — a circular wait killed at `TimeoutStartSec`. Rebake from current `deploy/packer`; live fix: copy the repo `bootstrap.sh` over `/usr/local/lib/oxidant/bootstrap.sh`, then `systemctl reset-failed oxidant-bootstrap && systemctl start oxidant-bootstrap && systemctl start oxidant-<role>` |
| Queries fail on dead worker IPs ("no free task slots") | Stale `OXIDANT_WORKERS` on the driver after replace — re-run `oxidant-bootstrap` on the driver (or replace driver) so ASG private IPs are re-pinned |
| Driver fails membership vs count | ASG InService count &lt; `WorkerCount` or IAM missing `ec2:DescribeInstances` / `autoscaling:DescribeAutoScalingGroups` |
| Workers never receive tasks | Empty/missing `OXIDANT_WORKERS` on driver, or worker SG blocking Flight 50561 from driver SG |
| `aws glue … EntityNotFound` | Wrong database/table name or region in `CatalogConf` |
| `AccessDenied` on Glue/S3 | Deployed with `--glue false` or incomplete `DataBucketArns` (need bucket **and** `/*`) |
| Catalog works locally on driver but distributed scan fails | Workers missing `OXIDANT_CATALOG_CONF` — set stack `CatalogConf`, replace instances |
| Spill fills root volume | Spill size `0`, or no eligible device: bootstrap picks the largest **unmounted, unpartitioned, non-root** whole disk (no fixed device names) — `lsblk -f` and the bootstrap log line `no spill block device found` tell you which disks were skipped and why (mounted / has partitions) |
| OOM on large queries | Prefer setting `MemoryLimitBytes` + `ShuffleSpillBytes` for publishable SF100 numbers. Empty is no longer fatal: the engine auto-sizes the FairSpillPool and shuffle threshold from cgroup/host RAM × `OXIDANT_MEMORY_POOL_FRACTION` (default 0.7). Set `OXIDANT_MEMORY_LIMIT_BYTES=0` only when you intentionally want the unbounded pool. Opaque `do_get: transport error` after ~tens of seconds with many failed tasks usually means a worker died under an unbounded or undersized budget — check `journalctl -u oxidant-worker` / dmesg for OOM, and the enriched status source chain in the driver log. |
| `CatalogConf` deploy error / truncated tag | Value must be ≤256 characters |

```sh
# on an instance (via SSM)
journalctl -u oxidant-bootstrap -u oxidant-driver -u oxidant-worker -e
cat /etc/oxidant/oxidant.env
sudo -u oxidant /usr/local/bin/aws glue get-databases --region "$AWS_REGION"
```

---

## File map

| Path | Role |
|------|------|
| [`deploy/packer/oxidant-runtime.pkr.hcl`](../deploy/packer/oxidant-runtime.pkr.hcl) | Packer AMI |
| [`deploy/packer/files/bootstrap.sh`](../deploy/packer/files/bootstrap.sh) | Boot: spill mount + shard index + ASG private-IP `OXIDANT_WORKERS` pin + atomic env |
| [`deploy/packer/tests/test_asg_membership.sh`](../deploy/packer/tests/test_asg_membership.sh) | Hermetic regression: mocked ASG → private IP CSV; fail-closed on incomplete membership |
| [`deploy/packer/files/systemd/`](../deploy/packer/files/systemd/) | `oxidant-bootstrap` / `oxidant-driver` / `oxidant-worker` |
| [`deploy/packer/build-ami.sh`](../deploy/packer/build-ami.sh) | AMI build wrapper |
| [`deploy/cloudformation/oxidant-cluster.yaml`](../deploy/cloudformation/oxidant-cluster.yaml) | CFN stack |
| [`deploy/cloudformation/deploy-stack.sh`](../deploy/cloudformation/deploy-stack.sh) | Deploy wrapper |
| [`docs/catalogs.md`](catalogs.md) | Catalog SPI + Spark `spark.sql.catalog.*` keys |
| [`crates/oxidant-catalog-glue/`](../crates/oxidant-catalog-glue/) | Glue provider (aws-sdk-glue, in-process) |
