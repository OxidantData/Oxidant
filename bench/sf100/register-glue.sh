#!/usr/bin/env bash
# Register Glue EXTERNAL_TABLE entries for SF100 Parquet already in S3.
# Safe to re-run (deletes + recreates tables).
#
#   SUITE=tpch SF=100 ./bench/sf100/register-glue.sh
#   SUITE=tpcds SF=100 ./bench/sf100/register-glue.sh
set -euo pipefail

REGION="${AWS_REGION:-${REGION:-us-west-2}}"
ACCOUNT="${AWS_ACCOUNT_ID:-$(aws sts get-caller-identity --query Account --output text)}"
BUCKET="${BUCKET:-weft-artifacts-${ACCOUNT}}"
SF="${SF:-100}"
SUITE="${SUITE:?Set SUITE=tpch or SUITE=tpcds}"

prefix="${SUITE}-sf${SF}"
glue_db="${SUITE}_sf${SF}"

case "$SUITE" in
  tpch)
    TABLES=(nation region supplier customer part partsupp orders lineitem)
    DESC="TPC-H SF${SF} (Weft bench)"
    ;;
  tpcds)
    TABLES=(
      call_center catalog_page catalog_returns catalog_sales customer customer_address
      customer_demographics date_dim household_demographics income_band inventory item
      promotion reason ship_mode store store_returns store_sales time_dim warehouse
      web_page web_returns web_sales web_site
    )
    DESC="TPC-DS SF${SF} (Weft bench)"
    ;;
  *)
    echo "SUITE must be tpch or tpcds"; exit 2
    ;;
esac

echo "[glue] database ${glue_db} → s3://${BUCKET}/${prefix}/"
aws glue create-database --region "$REGION" \
  --database-input "{\"Name\":\"${glue_db}\",\"Description\":\"${DESC}\"}" \
  2>/dev/null || true

for t in "${TABLES[@]}"; do
  s3_uri="s3://${BUCKET}/${prefix}/${t}/"
  # Confirm objects exist before registering.
  if ! aws s3 ls "$s3_uri" --region "$REGION" | grep -q .; then
    echo "[glue] WARN: empty ${s3_uri} — skipping ${t}"
    continue
  fi
  aws glue delete-table --region "$REGION" --database-name "$glue_db" --name "$t" 2>/dev/null || true
  aws glue create-table --region "$REGION" --database-name "$glue_db" --table-input "{
    \"Name\": \"${t}\",
    \"TableType\": \"EXTERNAL_TABLE\",
    \"Parameters\": {\"classification\": \"parquet\", \"EXTERNAL\": \"TRUE\"},
    \"StorageDescriptor\": {
      \"Location\": \"${s3_uri}\",
      \"InputFormat\": \"org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat\",
      \"OutputFormat\": \"org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat\",
      \"SerdeInfo\": {
        \"SerializationLibrary\": \"org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe\"
      },
      \"Columns\": []
    }
  }"
  echo "[glue] ${glue_db}.${t}"
done

echo "[glue] done — query as glue.${glue_db}.<table>"
aws glue get-tables --region "$REGION" --database-name "$glue_db" \
  --query 'TableList[].[Name,StorageDescriptor.Location]' --output table
