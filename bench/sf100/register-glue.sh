#!/usr/bin/env bash
# Register Glue EXTERNAL_TABLE entries for SF100 lakehouse tables already in S3.
# Safe to re-run (deletes + recreates tables).
#
#   # Parquet (default — original behaviour)
#   SUITE=tpch SF=100 ./bench/sf100/register-glue.sh
#
#   # Delta (same S3 prefix as Parquet; requires build-lakehouse.py --formats delta first)
#   SUITE=tpch SF=100 FORMAT=delta ./bench/sf100/register-glue.sh
#
#   # Iceberg (needs METADATA_LOCATION_MAP file: table=s3://.../xxx.metadata.json per line)
#   SUITE=tpch SF=100 FORMAT=iceberg METADATA_LOCATION_MAP=/tmp/iceberg-md.map \
#     ICEBERG_WAREHOUSE=s3://bucket/tpch-sf100-iceberg ./bench/sf100/register-glue.sh
#
# Prefer `build-lakehouse.py` (creates metadata + registers). This script remains the
# thin Glue-only path matching dump-to-s3.sh's SKIP_GLUE flow.
set -euo pipefail

REGION="${AWS_REGION:-${REGION:-us-west-2}}"
ACCOUNT="${AWS_ACCOUNT_ID:-$(aws sts get-caller-identity --query Account --output text)}"
BUCKET="${BUCKET:-weft-artifacts-${ACCOUNT}}"
SF="${SF:-100}"
SUITE="${SUITE:?Set SUITE=tpch or SUITE=tpcds}"
FORMAT="${FORMAT:-parquet}" # parquet | delta | iceberg

SF_TOKEN="${SF//./_}"
prefix="${SUITE}-sf${SF}"
case "$FORMAT" in
  parquet) glue_db="${SUITE}_sf${SF_TOKEN}" ;;
  delta)   glue_db="${SUITE}_sf${SF_TOKEN}_delta" ;;
  iceberg)
    glue_db="${SUITE}_sf${SF_TOKEN}_iceberg"
    ICEBERG_WAREHOUSE="${ICEBERG_WAREHOUSE:-s3://${BUCKET}/${prefix}-iceberg}"
    ;;
  *)
    echo "FORMAT must be parquet|delta|iceberg"; exit 2
    ;;
esac

case "$SUITE" in
  tpch)
    TABLES=(nation region supplier customer part partsupp orders lineitem)
    DESC="TPC-H SF${SF} (${FORMAT}, Weft bench)"
    ;;
  tpcds)
    TABLES=(
      call_center catalog_page catalog_returns catalog_sales customer customer_address
      customer_demographics date_dim household_demographics income_band inventory item
      promotion reason ship_mode store store_returns store_sales time_dim warehouse
      web_page web_returns web_sales web_site
    )
    DESC="TPC-DS SF${SF} (${FORMAT}, Weft bench)"
    ;;
  *)
    echo "SUITE must be tpch or tpcds"; exit 2
    ;;
esac

lookup_metadata_location() {
  local table="$1"
  if [[ -z "${METADATA_LOCATION_MAP:-}" ]]; then
    echo ""
    return
  fi
  awk -F= -v t="$table" '$1==t {print $2; exit}' "$METADATA_LOCATION_MAP"
}

echo "[glue] database ${glue_db} format=${FORMAT} → s3://${BUCKET}/${prefix}/"
aws glue create-database --region "$REGION" \
  --database-input "{\"Name\":\"${glue_db}\",\"Description\":\"${DESC}\"}" \
  2>/dev/null || true

for t in "${TABLES[@]}"; do
  s3_uri="s3://${BUCKET}/${prefix}/${t}/"
  if [[ "$FORMAT" == "iceberg" ]]; then
    s3_uri="${ICEBERG_WAREHOUSE%/}/${glue_db}/${t}/"
  fi
  if [[ "$FORMAT" != "iceberg" ]]; then
    if ! aws s3 ls "$s3_uri" --region "$REGION" | grep -q .; then
      echo "[glue] WARN: empty ${s3_uri} — skipping ${t}"
      continue
    fi
  fi

  case "$FORMAT" in
    parquet)
      PARAMS='{"classification":"parquet","EXTERNAL":"TRUE"}'
      IN="org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
      OUT="org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
      SERDE="org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      ;;
    delta)
      PARAMS='{"classification":"delta","provider":"delta","spark.sql.sources.provider":"delta","EXTERNAL":"TRUE"}'
      IN="org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
      OUT="org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
      SERDE="org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      ;;
    iceberg)
      MD="$(lookup_metadata_location "$t")"
      if [[ -z "$MD" ]]; then
        echo "[glue] WARN: no metadata_location for ${t} in METADATA_LOCATION_MAP — skipping"
        continue
      fi
      PARAMS=$(printf '{"table_type":"ICEBERG","metadata_location":"%s","classification":"parquet","EXTERNAL":"TRUE"}' "$MD")
      IN="org.apache.iceberg.mr.hive.HiveIcebergInputFormat"
      OUT="org.apache.iceberg.mr.hive.HiveIcebergOutputFormat"
      SERDE="org.apache.iceberg.mr.hive.HiveIcebergSerDe"
      ;;
  esac

  aws glue delete-table --region "$REGION" --database-name "$glue_db" --name "$t" 2>/dev/null || true
  aws glue create-table --region "$REGION" --database-name "$glue_db" --table-input "{
    \"Name\": \"${t}\",
    \"TableType\": \"EXTERNAL_TABLE\",
    \"Parameters\": ${PARAMS},
    \"StorageDescriptor\": {
      \"Location\": \"${s3_uri}\",
      \"InputFormat\": \"${IN}\",
      \"OutputFormat\": \"${OUT}\",
      \"SerdeInfo\": {
        \"SerializationLibrary\": \"${SERDE}\"
      },
      \"Columns\": []
    }
  }"
  echo "[glue] ${glue_db}.${t}"
done

echo "[glue] done — query as glue.${glue_db}.<table>"
aws glue get-tables --region "$REGION" --database-name "$glue_db" \
  --query 'TableList[].[Name,StorageDescriptor.Location,Parameters.table_type,Parameters.classification]' --output table
