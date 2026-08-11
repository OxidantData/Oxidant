#!/usr/bin/env bash
# Compile dbgen (TPC-H) and dsdgen (TPC-DS) under $KITS_DIR.
set -euo pipefail

DATA_ROOT="${DATA_ROOT:-/data}"
KITS_DIR="${KITS_DIR:-${DATA_ROOT}/kits}"

uname_s="$(uname -s)"
case "$uname_s" in
  Linux*) MACHINE=LINUX; TPCDS_OS=LINUX ;;
  # TPC-H makefile.suite only knows LINUX/WIN32/... -- gcc on macOS uses MACHINE=LINUX.
  Darwin*) MACHINE=LINUX; TPCDS_OS=MACOS ;;
  *) echo "unsupported OS: $uname_s" >&2; exit 1 ;;
esac

JOBS="$(sysctl -n hw.ncpu 2>/dev/null || nproc 2>/dev/null || echo 4)"

build_tpch() {
  local dbgen=""
  if [[ -d "$KITS_DIR/tpch-kit/dbgen" ]]; then
    dbgen="$KITS_DIR/tpch-kit/dbgen"
  else
    local match
    match="$(find "$KITS_DIR/tpch-kit" -type d -name dbgen 2>/dev/null | head -1 || true)"
    if [[ -n "$match" ]]; then
      dbgen="$match"
    else
      echo "[build-kits] TPC-H dbgen dir not found under $KITS_DIR/tpch-kit" >&2
      return 1
    fi
  fi
  echo "[build-kits] building TPC-H in $dbgen ..."
  (
    cd "$dbgen"
    # macOS has no <malloc.h>; TPC sources include it unconditionally.
    if [[ "$(uname -s)" == Darwin ]]; then
      perl -pi -e 's/#include\s*<malloc\.h>/#include <stdlib.h>/' ./*.c ./*.h 2>/dev/null || true
    fi
    if [[ -f makefile.suite && ! -f makefile ]]; then
      cp makefile.suite makefile
      # DATABASE choice only affects qgen SQL dialect macros; dbgen data is identical.
      sed -i.bak \
        -e "s/^CC[ ]*=.*/CC = gcc/" \
        -e "s/^DATABASE[ ]*=.*/DATABASE = POSTGRESQL/" \
        -e "s/^MACHINE[ ]*=.*/MACHINE = ${MACHINE}/" \
        -e "s/^WORKLOAD[ ]*=.*/WORKLOAD = TPCH/" \
        makefile
      if ! grep -q 'POSTGRESQL' tpcd.h 2>/dev/null; then
        cat >> tpcd.h <<'EOF'

#ifdef POSTGRESQL
#define GEN_QUERY_PLAN  "EXPLAIN"
#define START_TRAN      "BEGIN TRANSACTION"
#define END_TRAN        "COMMIT;"
#define SET_OUTPUT      ""
#define SET_ROWCOUNT    "LIMIT %d\n"
#define SET_DBASE       ""
#endif
EOF
      fi
    fi
    make clean >/dev/null 2>&1 || true
    make -j"$JOBS"
  )
  [[ -x "$dbgen/dbgen" ]] || { echo "[build-kits] dbgen missing after make" >&2; return 1; }
  echo "[build-kits] ok: $dbgen/dbgen"
}

build_tpcds() {
  local tools=""
  if [[ -d "$KITS_DIR/tpcds-kit/tools" ]]; then
    tools="$KITS_DIR/tpcds-kit/tools"
  else
    echo "[build-kits] TPC-DS tools dir not found under $KITS_DIR/tpcds-kit" >&2
    return 1
  fi
  echo "[build-kits] building TPC-DS in $tools ..."
  (
    cd "$tools"
    # Modern clang rejects K&R implicit-int in official sources.
    if ! grep -q 'std=gnu89' Makefile 2>/dev/null; then
      perl -pi -e 's/(MACOS_CFLAGS\s*=\s*.*)/$1 -std=gnu89/; s/(LINUX_CFLAGS\s*=\s*.*)/$1 -std=gnu89/' Makefile
    fi
    make clean >/dev/null 2>&1 || true
    make OS="$TPCDS_OS" -j"$JOBS"
  )
  [[ -x "$tools/dsdgen" ]] || { echo "[build-kits] dsdgen missing after make" >&2; return 1; }
  echo "[build-kits] ok: $tools/dsdgen"
}

build_tpch
build_tpcds
echo "[build-kits] done"
