#!/usr/bin/env bash
# Build the Weft runtime AMI with Packer.
#
# Usage:
#   ./deploy/packer/build-ami.sh --binary ./target/release/weft
#   ./deploy/packer/build-ami.sh --binary-url https://example.com/weft
#
# Optional env:
#   AWS_REGION / REGION   (default us-west-2)
#   PACKER_INSTANCE_TYPE  (default t3.large)
#   PACKER_SUBNET_ID      (optional)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PKR_DIR="${ROOT}/deploy/packer"
FILES_DIR="${PKR_DIR}/files"
REGION="${AWS_REGION:-${REGION:-us-west-2}}"
INSTANCE_TYPE="${PACKER_INSTANCE_TYPE:-t3.large}"
SUBNET_ID="${PACKER_SUBNET_ID:-}"
BINARY_PATH=""
BINARY_URL=""

usage() {
  sed -n '2,12p' "$0" | sed 's/^# \?//'
  exit 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --binary) BINARY_PATH="${2:?}"; shift 2 ;;
    --binary-url) BINARY_URL="${2:?}"; shift 2 ;;
    --region) REGION="${2:?}"; shift 2 ;;
    -h|--help) usage ;;
    *) echo "unknown arg: $1" >&2; usage ;;
  esac
done

if [[ -z "${BINARY_PATH}" && -z "${BINARY_URL}" ]]; then
  echo "error: pass --binary /path/to/weft or --binary-url https://..." >&2
  exit 1
fi

if [[ -n "${BINARY_PATH}" ]]; then
  if [[ ! -f "${BINARY_PATH}" ]]; then
    echo "error: binary not found: ${BINARY_PATH}" >&2
    exit 1
  fi
  cp -f "${BINARY_PATH}" "${FILES_DIR}/weft"
  chmod 0755 "${FILES_DIR}/weft"
  trap 'rm -f "${FILES_DIR}/weft"' EXIT
fi

if ! command -v packer >/dev/null 2>&1; then
  echo "error: packer not found on PATH" >&2
  exit 1
fi

export GIT_SHA
GIT_SHA="$(git -C "${ROOT}" rev-parse --short HEAD 2>/dev/null || echo unknown)"

cd "${PKR_DIR}"
packer init weft-runtime.pkr.hcl

ARGS=(
  -var "region=${REGION}"
  -var "instance_type=${INSTANCE_TYPE}"
  -var "weft_binary_url=${BINARY_URL}"
)
if [[ -n "${SUBNET_ID}" ]]; then
  ARGS+=(-var "subnet_id=${SUBNET_ID}")
fi

packer build "${ARGS[@]}" weft-runtime.pkr.hcl
