#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${ROOT_DIR}/../aurekai-continuity-core"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --out-dir)
      OUT_DIR="$2"
      shift 2
      ;;
    *)
      echo "Unknown option: $1" >&2
      exit 2
      ;;
  esac
done

if [[ ! -d "$OUT_DIR" ]]; then
  echo "Missing split repo directory: $OUT_DIR" >&2
  exit 1
fi

required=(
  "package.json"
  "src/index.mjs"
  "src/integration-engine.mjs"
  "src/state-runtime.mjs"
  "src/transition-runtime.mjs"
  "src/trajectory-runtime.mjs"
  "src/claim-runtime.mjs"
  "src/trajectory-calculus.mjs"
  "src/policy-registry.mjs"
  "src/chart-registry.mjs"
  "src/schema-registry.mjs"
  "registry/protocol-mutation-boundaries.json"
  "schemas/aurekai.integration_execution.v1.json"
)

for item in "${required[@]}"; do
  if [[ ! -f "$OUT_DIR/$item" ]]; then
    echo "Missing required artifact: $OUT_DIR/$item" >&2
    exit 1
  fi
done

(
  cd "$OUT_DIR"
  node ./scripts/verify-split.mjs
)

echo "Split repo verification passed for: $OUT_DIR"
