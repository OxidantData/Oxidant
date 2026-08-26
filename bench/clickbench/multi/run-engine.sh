#!/usr/bin/env bash
# Boot ONE engine's Spark Connect server, register `hits` (engine-specific DDL), run the 43
# queries × 3 via the stock PySpark client, write results/<engine>.json, then stop the server.
#
#   bash run-engine.sh <oxidant|sail|spark|gluten>
#
# Env: BENCH_DATA = path to hits.parquet (default: bench/clickbench/hits.parquet)
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$HERE/../../.." && pwd)"
ENGINE="${1:?usage: run-engine.sh <oxidant|sail|spark|gluten>}"
DATA="${BENCH_DATA:-$REPO/bench/clickbench/hits.parquet}"
RESULTS="$HERE/results"; mkdir -p "$RESULTS"
LOGS="$HERE/logs"; mkdir -p "$LOGS"
SPARK_VERSION="${SPARK_VERSION:-3.5.3}"
SPARK_HOME="${SPARK_HOME:-$HOME/spark-${SPARK_VERSION}-bin-hadoop3}"
NPROC="$(nproc)"

[ -f "$DATA" ] || { echo "[run] dataset missing: $DATA (run download in run-all.sh)"; exit 1; }

SERVER_PID=""
cleanup() {
  [ -n "$SERVER_PID" ] && kill "$SERVER_PID" 2>/dev/null || true
  # Spark's start-connect-server forks a daemon — stop it too.
  [ -n "${SPARK_HOME:-}" ] && "$SPARK_HOME/sbin/stop-connect-server.sh" >/dev/null 2>&1 || true
}
trap cleanup EXIT

wait_for_port() {  # host port timeout_s
  local h="$1" p="$2" t="${3:-120}" i=0
  while ! (exec 3<>"/dev/tcp/$h/$p") 2>/dev/null; do
    i=$((i+1)); [ "$i" -ge "$t" ] && { echo "[run] port $h:$p never opened"; return 1; }
    sleep 1
  done
  exec 3>&- 3<&- 2>/dev/null || true
}

# Registration: Oxidant uses SQL DDL (its connect server wants CREATE EXTERNAL TABLE, and it
# case-folds bare identifiers so it runs the quoted queries.oxidant.sql); the Spark-family engines
# register via the DataFrame API in runner.py (--register-mode dataframe). Both expose EventTime
# as TIMESTAMP and EventDate as DATE — the raw columns are int epoch-seconds / int days.
REG="$HERE/.register-$ENGINE.sql"
REG_MODE="dataframe"                       # default for spark-family
QUERIES="$HERE/queries.spark.sql"
case "$ENGINE" in
  oxidant)
    VENV="$HERE/.venv-oxidant"; REMOTE="sc://localhost:50051"; HOSTPORT="localhost 50051"
    REG_MODE="sql"; QUERIES="$HERE/queries.oxidant.sql"
    # to_timestamp_seconds(EventTime) → extract/date_trunc work (Q18/Q42); EventDate int→DATE.
    cat > "$REG" <<SQL
CREATE EXTERNAL TABLE hits_raw STORED AS PARQUET LOCATION '$DATA' OPTIONS ('binary_as_string' 'true')
CREATE VIEW hits AS SELECT * EXCEPT ("EventTime", "EventDate"), to_timestamp_seconds("EventTime") AS "EventTime", CAST(CAST("EventDate" AS INTEGER) AS DATE) AS "EventDate" FROM hits_raw
SQL
    echo "[run] starting oxidant server …"
    # --foreground: this harness backgrounds every engine the same way and reaps them all
    # from one trap, so it is the supervisor for oxidant exactly as it is for sail/spark.
    "$REPO/target/release/oxidant" spark server --port 50051 --foreground >"$LOGS/oxidant.log" 2>&1 &
    SERVER_PID=$!
    ;;
  sail)
    VENV="$HERE/.venv-sail"; REMOTE="sc://localhost:50052"; HOSTPORT="localhost 50052"
    echo "[run] starting sail server …"
    # pysail exposes a CLI; fall back to the module entrypoint if the name differs.
    ( "$VENV/bin/sail" spark server --ip 0.0.0.0 --port 50052 \
      || "$VENV/bin/python" -m pysail spark server --ip 0.0.0.0 --port 50052 ) \
      >"$LOGS/sail.log" 2>&1 &
    SERVER_PID=$!
    ;;
  spark)
    VENV="$HERE/.venv-spark"; REMOTE="sc://localhost:15002"; HOSTPORT="localhost 15002"
    echo "[run] starting spark connect server …"
    # start-connect-server.sh daemonizes then hits its own shutdown hook and exits; launch the
    # SparkConnectServer class directly under setsid so it stays up for the whole run.
    setsid "$SPARK_HOME/bin/spark-submit" \
      --class org.apache.spark.sql.connect.service.SparkConnectServer \
      --packages "org.apache.spark:spark-connect_2.12:$SPARK_VERSION" \
      --conf spark.connect.grpc.binding.port=15002 \
      --conf spark.driver.memory=24g \
      --conf spark.sql.shuffle.partitions="$NPROC" \
      --conf spark.driver.bindAddress=0.0.0.0 </dev/null >"$LOGS/spark.log" 2>&1 &
    SERVER_PID=$!
    ;;
  gluten)
    VENV="$HERE/.venv-spark"; REMOTE="sc://localhost:15002"; HOSTPORT="localhost 15002"
    GLUTEN_JAR="$(ls "$HERE"/jars/gluten-velox-bundle-*.jar 2>/dev/null | head -1 || true)"
    [ -n "$GLUTEN_JAR" ] || { echo "[run] gluten jar missing (install-gluten.sh)"; exit 1; }
    echo "[run] starting spark+gluten/velox connect server …"
    # Velox needs JDK17 module opens for off-heap/DirectByteBuffer access; the bundle jar must be
    # on the driver/executor classpath (not just --jars) so its gluten-components register.
    ADD_OPENS="--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.misc=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED"
    setsid "$SPARK_HOME/bin/spark-submit" \
      --class org.apache.spark.sql.connect.service.SparkConnectServer \
      --packages "org.apache.spark:spark-connect_2.12:$SPARK_VERSION" \
      --jars "$GLUTEN_JAR" \
      --conf spark.connect.grpc.binding.port=15002 \
      --conf spark.plugins=org.apache.gluten.GlutenPlugin \
      --conf spark.memory.offHeap.enabled=true \
      --conf spark.memory.offHeap.size=20g \
      --conf spark.gluten.sql.columnar.backend.lib=velox \
      --conf spark.shuffle.manager=org.apache.spark.shuffle.sort.ColumnarShuffleManager \
      --conf "spark.driver.extraClassPath=$GLUTEN_JAR" \
      --conf "spark.executor.extraClassPath=$GLUTEN_JAR" \
      --conf "spark.driver.extraJavaOptions=$ADD_OPENS" \
      --conf "spark.executor.extraJavaOptions=$ADD_OPENS" \
      --conf spark.driver.memory=20g \
      --conf spark.sql.shuffle.partitions="$NPROC" \
      --conf spark.driver.bindAddress=0.0.0.0 </dev/null >"$LOGS/gluten.log" 2>&1 &
    SERVER_PID=$!
    ;;
  *) echo "unknown engine: $ENGINE"; exit 2 ;;
esac

echo "[run] waiting for $REMOTE …"
# shellcheck disable=SC2086
wait_for_port $HOSTPORT 180

"$VENV/bin/python" "$HERE/runner.py" \
  --remote "$REMOTE" \
  --queries "$QUERIES" \
  --register-mode "$REG_MODE" \
  --register-file "$REG" \
  --data "$DATA" \
  --engine "$ENGINE" \
  --out "$RESULTS/$ENGINE.json" \
  --data-size "$(stat -c%s "$DATA" 2>/dev/null || echo 0)"

echo "[run] $ENGINE done → $RESULTS/$ENGINE.json"
