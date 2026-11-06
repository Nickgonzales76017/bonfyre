#!/bin/sh
# The real native-Qwen receipt enters through ModelCatalog, then the generic
# AddressPlane compiler. It may be discoverable for its declared workload, but
# its DEFER boundary survives lowering and cannot become an admitted provider.
set -eu

estate_bin=${1:?estate binary required}
model_bin=${2:?model binary required}
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
tmp=$(mktemp -d)
cleanup() { rm -rf "$tmp"; }
trap cleanup EXIT INT TERM

export BONFYRE_MODEL_DB="$tmp/models.db"
export BONFYRE_MODEL_CACHE="$tmp/cache"

"$model_bin" add "$root/cmd/BonfyreQwenFPQ/results/2026-09-11-qwen-native-fp16-model-manifest.json" >/dev/null
"$model_bin" representation-record "$root/cmd/BonfyreQwenFPQ/results/2026-09-11-qwen-native-fp16-representation-passport.json" >/dev/null
"$estate_bin" emit-compute --model-db "$tmp/models.db" > "$tmp/advertisements.jsonl"
grep -q 'compute-representation:qwen2.5-coder-0.5b-native-fp16-fpq0/runtime-native-f16/2026-09-11' "$tmp/advertisements.jsonl"
grep -q '"operation:generation"' "$tmp/advertisements.jsonl"
grep -q '"admission:defer"' "$tmp/advertisements.jsonl"
grep -q 'ModelRepresentationReceipt:sha256:c34d616ac62b4aa72086185799610ba75ac55a17b51cfd117dcdc7cc3828465a' "$tmp/advertisements.jsonl"

"$estate_bin" import-advertisements --db "$tmp/address.db" --advertisements "$tmp/advertisements.jsonl" >/dev/null
"$estate_bin" forward --db "$tmp/address.db" --kind compute --need operation:generation --need admission:defer > "$tmp/forward.json"
grep -q 'compute-representation:qwen2.5-coder-0.5b-native-fp16-fpq0/runtime-native-f16/2026-09-11' "$tmp/forward.json"
grep -q '"truth":"eligible_for_further_consideration"' "$tmp/forward.json"

echo 'ModelCatalog → AddressPlane representation boundary: PASS'
