# SF10 Distributed Oxidant — Resume / Handoff

> **Purpose:** everything needed to resume this work in a fresh session (kimicode / new agent).
> Read top-to-bottom; the "Next steps" section is the immediate TODO.

---

## 1. TL;DR — where things stand

- Goal: prove Oxidant **distributed** execution runs **TPC-H + TPC-DS** end-to-end (Spark Connect → driver → 2 Flight workers), cheaply.
- Original target was **SF100** on EC2; it **OOM'd every multi-fact join** (see §7). We pivoted to **SF10** (1/10th data) on smaller instances.
- **SF10 data is generated in 3 formats** (Parquet + Iceberg + Delta) and registered in Glue (6 DBs). ✅
- **SF10 cluster is UP and healthy** (1 driver + 2 workers), multi-file data so **both workers now share load** (skew fixed). ✅
- **Harness improved** (per-query timeout) and **SF-agnostic**. ✅
- **Current blocker:** TPC-H **Q2** (correlated scalar subquery) makes the **driver** balloon past 14 GB and wedge (see §8). Everything else so far passes. Need to skip Q2 or fix the engine plan.

---

## 2. Live infrastructure (oxidant-sf10)

CloudFormation stack **`oxidant-sf10`** (region **us-west-2**), status UPDATE_COMPLETE.

| Role | Instance type | Public IP | Private IP | Notes |
|---|---|---|---|---|
| Driver / Spark Connect | **c6g.2xlarge** (8 vCPU / 16 GiB) | **18.236.223.115** | 172.31.0.87 | Connect on **:50051**, UI on :4040 |
| Worker (shard 0) | **m8g.2xlarge** (8 vCPU / 32 GiB) | 35.80.13.249 | 172.31.50.17 | Flight on **:50561**, 100 GiB gp3 spill |
| Worker (shard 1) | **m8g.2xlarge** (8 vCPU / 32 GiB) | 35.95.70.231 | 172.31.41.60 | Flight on :50561, 100 GiB gp3 spill |

**Engine env (effective):**
- Driver: `OXIDANT_DISTRIBUTED_STRICT=1`, `OXIDANT_PREFER_HASH_JOIN=false`, `OXIDANT_SHUFFLE_PARTITIONS=16`, `OXIDANT_MEMORY_LIMIT_BYTES=8589934592` (8 GiB pool), `OXIDANT_WORKER_COUNT=2`, `OXIDANT_WORKER_SERVICE=workers.oxidant-sf10.oxidant.internal`; systemd cgroup `MemoryMax=15G`/`MemoryHigh=13G`.
- Workers: `OXIDANT_MEMORY_LIMIT_BYTES=17179869184` (16 GiB pool), `OXIDANT_SHUFFLE_SPILL_BYTES=4294967296` (4 GiB), `OXIDANT_SHARD_INDEX` 0/1; cgroup `MemoryMax=28G`/`MemoryHigh=24G`.

**SSH access**
- Key: `~/.ssh/id_ed25519_kaicoder03` (AWS keypair `oxidant-debug`), user `ec2-user`.
- Driver: `ssh -i ~/.ssh/id_ed25519_kaicoder03 -o IdentitiesOnly=yes ec2-user@18.236.223.115`
- Workers (via driver bastion):
  `ssh -i ~/.ssh/id_ed25519_kaicoder03 -o IdentitiesOnly=yes -o "ProxyCommand ssh -i ~/.ssh/id_ed25519_kaicoder03 -o IdentitiesOnly=yes -W %h:%p ec2-user@18.236.223.115" ec2-user@172.31.50.17`
- Client IP `108.239.229.193` is allow-listed on the driver SG for **:50051** and **:22**; worker SG allows :22 from VPC (bastion) and :50561 from driver+worker SGs.

**Cost (this setup):** driver $0.272/hr + 2× $0.363/hr + EBS ≈ **$1.05/hr (~$25/day)** if left running. **Tear down when done** (see §10). The actual SF10 suite runs <1 hr → ~$1–2.


---

## 3. SF10 data (3 formats) — DONE

Source: DuckDB prebuilt `tpch-sf10.db` / `tpcds-sf10.db` downloaded to `/tmp/oxidant-sf10/`.

**Key decision:** export fact tables as **MULTIPLE Parquet files** (not one big file). Oxidant shards by *whole file* (size-weighted LPT in `crates/oxidant-loom/src/shard.rs`), so a single-file table lands entirely on ONE worker — that was the SF100 skew root cause. Multi-file is what lets both workers share scans.

- Export script: **`/tmp/oxidant-sf10/export_sf10.py`** (venv `/tmp/oxidant-datagen-venv`). Splits facts by `key % N`:
  - TPC-H: lineitem/orders 8 files, partsupp 6, part/customer 4, dims 1.
  - TPC-DS: store_sales/catalog_sales/web_sales 8, returns 4, inventory 4, dims 1.
- S3: `s3://oxidant-artifacts-810738286322/tpch-sf10/<table>/` and `.../tpcds-sf10/<table>/`.
- Iceberg + Delta metadata laid over the **same** Parquet bytes via `bench/sf100/build-lakehouse.py`.

**Glue DBs (all registered, us-west-2):**

| DB | Format | Tables |
|---|---|---|
| `tpch_sf10` | parquet | 8 |
| `tpch_sf10_delta` | delta | 8 |
| `tpch_sf10_iceberg` | iceberg | 8 |
| `tpcds_sf10` | parquet | 24 |
| `tpcds_sf10_delta` | delta | 24 |
| `tpcds_sf10_iceberg` | iceberg | 24 |

Query as `glue.<db>.<table>` (e.g. `glue.tpch_sf10.lineitem`).

---

## 4. Code changes already made (this branch)

Branch: `kaicoder03/kan-8-14-wave1-3`. All local, **not yet committed**.

1. **`bench/sf100/register-glue.sh`** — fixed a real bug: the "is prefix empty" check `aws s3 ls … | grep -q .` under `set -o pipefail` made `grep -q` close the pipe early → `aws` died with SIGPIPE on **multi-line** listings → falsely reported multi-file tables as empty and skipped them. Now captures the listing instead. (Single-file SF100 never hit this.)
2. **`deploy/cloudformation/oxidant-cluster.yaml`** — added **`ShufflePartitions`** parameter (default falls back to `WorkerCount`) and wired it to the `oxidant:shuffle-partitions` tag on driver+worker launch templates (was hardcoded to `${WorkerCount}`). Bootstrap reads the tag → `OXIDANT_SHUFFLE_PARTITIONS`.
3. **`deploy/cloudformation/deploy-stack.sh`** — added `--shuffle-partitions` flag. Validated (`aws cloudformation validate-template` OK, `bash -n` OK).
4. **`bench/sf100/remeasure-distributed.sh`** — made **SF-agnostic**: `SF` env (default 100), `GLUE_DB_*` now default from `SF` (`tpch_sf${SF_INT}`), and `--sf "$SF"` (was hardcoded `--sf 100`). Header comment still names the SF100 topology; the SF10 run uses the smaller instances in §2.
5. **`bench/sf100/run-spark-connect.py`** — added **`--query-timeout`** (default 300s, env `OXIDANT_QUERY_TIMEOUT`): wraps `df.collect()` in a `ThreadPoolExecutor` with a timeout; on timeout it records a fail and **recreates the Spark session** (so a wedged query fails fast instead of hanging the suite).
6. **Docs** (`docs/distributed-ec2.md`, `bench/sf100/README.md`, `deploy/helm/oxidant/values-sf100.yaml`) — updated SF100 topology notes: m8g.4xlarge under-provisioned for SF100 joins (HashJoin not spillable); documented memory invariant + `OXIDANT_SHUFFLE_PARTITIONS`.

**Driver AMI caveat:** `ami-062afcea2d6401e07` ships a **buggy** `/usr/local/lib/oxidant/bootstrap.sh` (deadlock in `enable_role_unit` + wrong spill device). The repo's `deploy/packer/files/bootstrap.sh` is the **fixed** version. Every fresh instance must be patched: copy the repo script over `/usr/local/lib/oxidant/bootstrap.sh`, `systemctl reset-failed oxidant-bootstrap`, re-run it, then start the role unit. **Proper follow-up: rebake the AMI** (`deploy/packer/build-ami.sh`) so this isn't manual.

---

## 5. How to run the SF10 suite

```bash
cd <repo>
export PATH=/tmp/oxidant-smoke-venv/bin:$PATH           # has pyspark-client
export PYTHONUNBUFFERED=1
export SPARK_CONNECT_GRPC_MAX_MESSAGE_LENGTH=$((512*1024*1024))
export OXIDANT_QUERY_TIMEOUT=300

# full run (tpch then tpcds), strict distributed, auto-resolves driver IP from stack tag
STACK=oxidant-sf10 SF=10 SUITE=all \
  OUT_DIR=$PWD/bench/sf100/results/remeasure-sf10-$(date -u +%Y%m%dT%H%M%SZ) \
  ./bench/sf100/remeasure-distributed.sh

# skip Q2 (pathological, see §8): --only takes comma query nums
/tmp/oxidant-smoke-venv/bin/python3 bench/sf100/run-spark-connect.py \
  --endpoint sc://18.236.223.115:50051 --suite tpch --sf 10 --glue-db tpch_sf10 \
  --only 1,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22 \
  --strict --worker-count 2 --skip-worker-preflight --query-timeout 300 \
  --json /tmp/tpch-sf10-noq2.jsonl
```

Smoke check: `SELECT 1`, `SELECT count(*) FROM glue.tpch_sf10.nation` (25), and a join (`customer⋈nation`, `lineitem⋈orders`) all passed.

---

## 6. What's proven working

- Cluster up, strict distributed mode, both workers engaged (balanced memory, no skew) thanks to multi-file data.
- Scans + aggregates (TPC-H Q1, Q6) pass.

---

## 7. Why SF100 failed (root causes — the "how do others avoid OOM" answer)

- **Whole-file sharding**: SF100 tables were each **one** Parquet file → one worker did all scanning, the other idled. Fixed for SF10 by multi-file export. Spark/Trino split by **row-group/byte-range**; Oxidant should too (see §9).
- **DataFusion 54 `HashJoin` is not spillable** → the join build lands **outside** the `FairSpillPool`, so the process OOMs even with a correct pool. Spark uses sort-merge-join (spills) / broadcast for small; DuckDB/Trino have spillable hash joins.
- **Unaccounted memory**: HashJoin/Arrow/S3/aws-cli RSS sits outside the pool.
- **Coarse shuffle** (`OXIDANT_SHUFFLE_PARTITIONS=2`) caused skew.

---

## 8. CURRENT BLOCKER — TPC-H Q2 wedges the driver

Q2 = 5-table join + **correlated scalar subquery** `ps_supplycost = (SELECT min(ps_supplycost) FROM partsupp, supplier, nation, region WHERE p_partkey=ps_partkey …)`.

Observed: the **driver** RSS climbs 7 GB → 14 GB and keeps rising, CPU ~0%, query never returns; killing the client does **not** free it (driver holds the gather). On the 8 GiB driver it OOM'd/wedged; on 16 GiB it still climbs past 13 GB. Diagnosis: the planner **over-centralizes** for this correlated-subquery shape (a driver-side gather/materialization that grows unbounded). This is an **engine plan bug**, not a sizing issue.

**Immediate workaround:** run the suite **without Q2** (see §5 `--only`). Other correlated-subquery TPC-H queries (Q4, Q17, Q21, Q22) and some TPC-DS queries may behave similarly — watch for driver RSS growth.

**Real fix (engine, follow-up ticket):** Q2 should NOT gather a whole sharded fact to the driver. Options: decorrelate the scalar subquery into a distributed shuffle-join/agg (like the multi-key/outer-join work in KAN-10/11), make the gather spill/bounded, or reject it cleanly in strict mode instead of wedging.

---

## 9. Next steps (TODO)

1. **Get a green suite:** run TPC-H + TPC-DS at SF10 **skipping Q2** (and note any other wedging queries). Use `--query-timeout 300` so a wedge fails fast.
2. **Validate all 3 formats:** run TPC-H/DS against `tpch_sf10_delta`, `tpch_sf10_iceberg` (and tpcds equivalents) by changing `--glue-db`.
3. **File the Q2 engine bug** (driver over-centralization for correlated subqueries) — highest-value correctness fix; relates to the Spark-like "don't gather whole facts to the coordinator" principle.
4. **Engine improvement (bigger):** row-group/byte-range sharding in `crates/oxidant-loom/src/shard.rs` (split large Parquet files across workers) — the "like Spark" fix from §7.
5. **Rebake the AMI** with the fixed `bootstrap.sh` so fresh deploys don't need manual patching.
6. **Document/commit** the register-glue fix, ShufflePartitions param, SF-agnostic remeasure, and query-timeout harness changes.
7. *(Optional)* separate `DriverMemoryLimitBytes` / `WorkerMemoryLimitBytes` CFN params (currently one shared tag; driver needs less than workers).

---

## 10. Teardown (stop all cost)

```bash
aws cloudformation delete-stack --stack-name oxidant-sf10 --region us-west-2
# confirm: aws ec2 describe-instances --region us-west-2 \
#   --filters 'Name=tag:aws:cloudformation:stack-name,Values=oxidant-sf10' \
#   --query 'Reservations[].Instances[].State.Name'
```
S3 data (`tpch-sf10/`, `tpcds-sf10/`) and Glue DBs persist (cheap) for re-runs; recreate the stack with `deploy/cloudformation/deploy-stack.sh` (see repo docs) when needed.

---

## 11. Handy references

- Local venvs: `/tmp/oxidant-smoke-venv` (pyspark client), `/tmp/oxidant-datagen-venv` (duckdb/pyiceberg/deltalake/s3fs/boto3).
- Data workdir: `/tmp/oxidant-sf10/` (DBs + export script).
- Run logs: `bench/sf100/results/remeasure-sf10*.log`; per-query JSONL under `bench/sf100/results/remeasure-sf10-*/`.
- Deploy: `deploy/cloudformation/deploy-stack.sh` (params documented in `docs/distributed-ec2.md`).
- Engine planner: `crates/oxidant-execution/src/plan/{stage_planner,shape_extensions,join_chain}.rs`; sharding `crates/oxidant-loom/src/shard.rs`; memory pool `crates/oxidant-loom/src/lib.rs` (`OXIDANT_MEMORY_LIMIT_BYTES` → `FairSpillPool`).

- Simple + fact-fact joins pass (smoke: customer⋈nation ~11s; lineitem⋈orders count = 59,986,052 in ~20s — correct).
