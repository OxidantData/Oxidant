#!/usr/bin/env bash
# Validate the Kafka → Oxidant → Delta-on-S3 → Glue pipeline against REAL infrastructure.
#
# CI covers everything above the broker socket (crates/oxidant-connect/tests/
# streaming_kafka_lakehouse.rs). This script covers what CI cannot: a real Kafka broker, a real S3
# bucket, and a real Glue Data Catalog. Run it before claiming the streaming path works on AWS.
#
# It will CREATE a Glue database and table if they do not exist. Point it at a scratch database.
#
#   export KAFKA_BOOTSTRAP=b-1.msk.example:9092
#   export KAFKA_TOPIC=oxidant_validation
#   export GLUE_DATABASE=streaming_live
#   export S3_WAREHOUSE=s3://my-bucket/streaming
#   export AWS_REGION=us-east-1
#   ./scripts/validate-streaming-glue.sh
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

: "${KAFKA_BOOTSTRAP:?set KAFKA_BOOTSTRAP to a comma-separated broker list}"
: "${KAFKA_TOPIC:=oxidant_validation}"
: "${GLUE_DATABASE:=streaming_live}"
: "${S3_WAREHOUSE:?set S3_WAREHOUSE to an s3:// prefix Oxidant may write under}"
: "${AWS_REGION:=us-east-1}"
: "${OXIDANT_PORT:=50251}"
: "${RECORDS:=200}"

TABLE="${TABLE:-orders_live}"
RUN_ID="$(date +%Y%m%d%H%M%S)"
CHECKPOINT="${S3_WAREHOUSE%/}/_checkpoints/${TABLE}-${RUN_ID}"

log() { printf '\n=== %s\n' "$*"; }

log "Configuration"
cat <<EOF
  brokers      : ${KAFKA_BOOTSTRAP}
  topic        : ${KAFKA_TOPIC}
  glue database: ${GLUE_DATABASE}   (created if missing)
  glue table   : ${TABLE}
  warehouse    : ${S3_WAREHOUSE}
  checkpoint   : ${CHECKPOINT}
  region       : ${AWS_REGION}
EOF

for tool in aws python3; do
  command -v "$tool" >/dev/null || { echo "missing required tool: $tool" >&2; exit 1; }
done
python3 -c 'import pyspark' 2>/dev/null || {
  echo "install the Connect client first:  pip install 'pyspark-client>=4.0'" >&2
  exit 1
}

# The engine resolves S3 credentials from the *environment* (`AmazonS3Builder::from_env`), which is
# what IRSA and EC2 instance roles provide. A laptop authenticated with a shared-config profile has
# none of those set, and the S3 client falls through to the instance-metadata endpoint and fails
# there after a long retry. Export the resolved credentials so a profile works the same as a role.
if [ -z "${AWS_ACCESS_KEY_ID:-}" ] && aws configure export-credentials --format env >/dev/null 2>&1; then
  log "Exporting credentials from the active AWS profile"
  eval "$(aws configure export-credentials --format env)"
fi

log "Building the engine"
cargo build -p oxidant-cli --release

log "Producing ${RECORDS} records to ${KAFKA_TOPIC}"
if command -v kafka-console-producer >/dev/null; then
  python3 - "$RECORDS" <<'PY' | kafka-console-producer --bootstrap-server "$KAFKA_BOOTSTRAP" --topic "$KAFKA_TOPIC"
import json, sys, time
n = int(sys.argv[1])
now = int(time.time())
for i in range(n):
    print(json.dumps({
        "order_id": i,
        "customer": f"cust-{i % 7}",
        "amount": (i % 13) * 10,
        "event_ts": now + i,
    }))
PY
else
  echo "kafka-console-producer not found — assuming ${KAFKA_TOPIC} already has data" >&2
fi

log "Starting the Oxidant Connect server with the Glue catalog"
"${here}/target/release/oxidant" spark server --port "$OXIDANT_PORT" \
  --catalog-conf "spark.sql.catalog.glue.type=glue" \
  --catalog-conf "spark.sql.catalog.glue.region=${AWS_REGION}" \
  --catalog-conf "spark.sql.catalog.glue.warehouse=${S3_WAREHOUSE}" \
  >"/tmp/oxidant-streaming-validate-${RUN_ID}.log" 2>&1 &
server_pid=$!
cleanup() { kill "$server_pid" 2>/dev/null || true; }
trap cleanup EXIT

for _ in $(seq 1 60); do
  if python3 -c "
import socket,sys
s=socket.socket()
s.settimeout(0.5)
sys.exit(0 if s.connect_ex(('127.0.0.1', ${OXIDANT_PORT}))==0 else 1)
"; then break; fi
  sleep 1
done

log "Running the streaming query (Kafka → Delta → Glue)"
KAFKA_BOOTSTRAP="$KAFKA_BOOTSTRAP" \
KAFKA_TOPIC="$KAFKA_TOPIC" \
GLUE_DATABASE="$GLUE_DATABASE" \
GLUE_TABLE="$TABLE" \
CHECKPOINT="$CHECKPOINT" \
OXIDANT_PORT="$OXIDANT_PORT" \
python3 "${here}/python/examples/kafka_to_glue_delta.py"

log "Confirming the table is registered in Glue"
aws glue get-table --region "$AWS_REGION" \
  --database-name "$GLUE_DATABASE" --name "$TABLE" \
  --query 'Table.{Location:StorageDescriptor.Location,Provider:Parameters."spark.sql.sources.provider",Classification:Parameters.classification,Columns:StorageDescriptor.Columns[].Name}' \
  --output json

location="$(aws glue get-table --region "$AWS_REGION" \
  --database-name "$GLUE_DATABASE" --name "$TABLE" \
  --query 'Table.StorageDescriptor.Location' --output text)"

log "Confirming the Delta transaction log exists at ${location}"
aws s3 ls "${location%/}/_delta_log/" --region "$AWS_REGION"

log "Confirming the Iceberg metadata published over the same files"
aws s3 ls "${location%/}/metadata/" --region "$AWS_REGION"

log "Confirming the sibling Iceberg table is registered in Glue"
aws glue get-table --region "$AWS_REGION" \
  --database-name "$GLUE_DATABASE" --name "${TABLE}_iceberg" \
  --query 'Table.{TableType:Parameters.table_type,MetadataLocation:Parameters.metadata_location}' \
  --output json

log "PASS — streamed rows are queryable as glue.${GLUE_DATABASE}.${TABLE} (Delta)"
log "       and as glue.${GLUE_DATABASE}.${TABLE}_iceberg (Iceberg, same data files)"
