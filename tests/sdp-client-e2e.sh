#!/usr/bin/env bash
# Issue #89 gate: stock pyspark.pipelines client against oxidant spark server.
# Exercises DefineSqlGraphElements + StartRun over the committed Kafka spool (no broker).
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$here"

: "${SDP_VENV:=/tmp/sdp-venv}"
: "${SPARK_HOME_STUB:=/tmp/spark-home-stub}"

if [[ -z "${OXIDANT_PORT:-}" ]]; then
  OXIDANT_PORT="$(python3 -c 'import socket; s=socket.socket(); s.bind(("",0)); print(s.getsockname()[1]); s.close()')"
fi
: "${OXIDANT_WAREHOUSE:=$(mktemp -d /tmp/sdp-warehouse-XXXXXX)}"
: "${OXIDANT_CHECKPOINTS:=${OXIDANT_WAREHOUSE}/_checkpoints}"

log() { printf '==> %s\n' "$*"; }

ensure_venv() {
  if [[ ! -x "${SDP_VENV}/bin/python" ]]; then
    log "Creating Python venv at ${SDP_VENV}"
    python3 -m venv "${SDP_VENV}"
  fi
  if ! "${SDP_VENV}/bin/python" -c 'import pyspark' 2>/dev/null; then
    log "Installing pyspark-client (fallback: pyspark>=4.0)"
    if ! "${SDP_VENV}/bin/pip" install -q 'pyspark-client>=4.0'; then
      "${SDP_VENV}/bin/pip" install -q 'pyspark>=4.0'
    fi
  fi
  # pyspark-client is Connect-only and has no bin/spark-submit; session.py requires SPARK_HOME.
  if ! "${SDP_VENV}/bin/python" -c 'import os; from pyspark.find_spark_home import _find_spark_home; _find_spark_home()' 2>/dev/null; then
    if [[ ! -f "${SPARK_HOME_STUB}/bin/spark-submit" ]]; then
      log "Creating minimal SPARK_HOME stub at ${SPARK_HOME_STUB}"
      mkdir -p "${SPARK_HOME_STUB}/bin" "${SPARK_HOME_STUB}/jars"
      touch "${SPARK_HOME_STUB}/bin/spark-submit"
    fi
    export SPARK_HOME="${SPARK_HOME_STUB}"
  fi
}

ensure_venv

log "Building oxidant-cli"
cargo build -p oxidant-cli

mkdir -p "${OXIDANT_WAREHOUSE}" "${OXIDANT_CHECKPOINTS}"

log "Starting oxidant spark server on port ${OXIDANT_PORT}"
./target/debug/oxidant spark server --no-ui --port "${OXIDANT_PORT}" \
  --catalog-conf "spark.sql.catalog.local.type=local" \
  --catalog-conf "spark.sql.catalog.local.warehouse=${OXIDANT_WAREHOUSE}" \
  --catalog-conf "spark.sql.defaultCatalog=local" \
  >"/tmp/sdp-server-$$.log" 2>&1 &
server_pid=$!
cleanup() {
  kill "${server_pid}" 2>/dev/null || true
  wait "${server_pid}" 2>/dev/null || true
}
trap cleanup EXIT

for _ in $(seq 1 60); do
  if python3 -c "
import socket, sys
s = socket.socket()
s.settimeout(0.5)
sys.exit(0 if s.connect_ex(('127.0.0.1', ${OXIDANT_PORT})) == 0 else 1)
"; then
    break
  fi
  sleep 0.5
done

log "Running pyspark.pipelines DefineSqlGraphElements client"
export OXIDANT_REPO_ROOT="${here}"
export OXIDANT_CONNECT_URL="sc://localhost:${OXIDANT_PORT}"
export OXIDANT_WAREHOUSE
export OXIDANT_CHECKPOINTS
"${SDP_VENV}/bin/python" "${here}/tests/sdp_client_e2e.py"

log "Running spark-pipelines CLI (SQL file path)"
pipeline_dir="$(mktemp -d /tmp/sdp-pipeline-XXXXXX)"
cli_checkpoints="${OXIDANT_WAREHOUSE}/_checkpoints-cli"
mkdir -p "${pipeline_dir}/transformations" "${cli_checkpoints}"
cat >"${pipeline_dir}/spark-pipeline.yml" <<EOF
name: sdp-spool-e2e
storage: file://${cli_checkpoints}
catalog: local
database: live
configuration:
  spark.sql.catalog.local.type: local
  spark.sql.catalog.local.warehouse: ${OXIDANT_WAREHOUSE}
  spark.sql.defaultCatalog: local
libraries:
  - glob:
      include: transformations/**
EOF
spool="$(cd "${here}/examples/spool/orders" && pwd)"
cat >"${pipeline_dir}/transformations/pipeline.sql" <<EOF
CREATE STREAMING TABLE orders_bronze_cli
TBLPROPERTIES (
  'subscribe' = 'orders',
  'oxidant.spool.dir' = '${spool}',
  'startingOffsets' = 'earliest'
)
USING DELTA
AS SELECT
  CAST(get_json_object(CAST(value AS STRING), '\$.order_id') AS BIGINT) AS order_id,
  get_json_object(CAST(value AS STRING), '\$.customer') AS customer,
  CAST(get_json_object(CAST(value AS STRING), '\$.amount') AS BIGINT) AS amount
FROM stream;

CREATE MATERIALIZED VIEW revenue_gold_cli AS
SELECT customer, sum(amount) AS revenue, count(*) AS orders
FROM orders_bronze_cli WHERE amount > 0 GROUP BY customer;
EOF

export SPARK_REMOTE="sc://localhost:${OXIDANT_PORT}"
(
  cd "${pipeline_dir}"
  "${SDP_VENV}/bin/python" -m pyspark.pipelines.cli run --spec spark-pipeline.yml
)

total="$(
  OXIDANT_REPO_ROOT="${here}" \
  OXIDANT_CONNECT_URL="sc://localhost:${OXIDANT_PORT}" \
  OXIDANT_WAREHOUSE="${OXIDANT_WAREHOUSE}" \
  OXIDANT_CHECKPOINTS="${OXIDANT_CHECKPOINTS}" \
  "${SDP_VENV}/bin/python" - <<'PY'
from pyspark.sql import SparkSession
import os
spark = (
    SparkSession.builder.remote(os.environ["OXIDANT_CONNECT_URL"])
    .config("spark.sql.catalog.local.type", "local")
    .config("spark.sql.catalog.local.warehouse", os.environ["OXIDANT_WAREHOUSE"])
    .config("spark.sql.defaultCatalog", "local")
    .getOrCreate()
)
print(spark.sql("SELECT sum(revenue) FROM local.live.revenue_gold_cli").collect()[0][0])
PY
)"
if [[ "${total}" != "725" ]]; then
  echo "spark-pipelines CLI path: expected sum(revenue)=725, got ${total}" >&2
  exit 1
fi

log "PASS — pyspark.pipelines + spark-pipelines CLI e2e"
