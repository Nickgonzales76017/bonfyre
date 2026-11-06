#!/bin/sh
# A ModelCatalog representation is not model identity and does not become a
# provider merely because an execution receipt exists. This exercises the
# actual measured native-Qwen receipt's normalized passport, then confirms
# unknown semantic fields and absent model identities fail closed.
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
bin=${1:-"$root/cmd/BonfyreModel/bonfyre-model"}
tmp=$(mktemp -d)
cleanup() { rm -rf "$tmp"; }
trap cleanup EXIT INT TERM

export BONFYRE_MODEL_DB="$tmp/models.db"
export BONFYRE_MODEL_CACHE="$tmp/cache"
export BONFYRE_CATALOG_DB="$tmp/catalog.db"

"$bin" add "$root/cmd/BonfyreQwenFPQ/results/2026-09-11-qwen-native-fp16-model-manifest.json" >/dev/null
"$bin" representation-record "$root/cmd/BonfyreQwenFPQ/results/2026-09-11-qwen-native-fp16-representation-passport.json" >"$tmp/record.json"
grep -q '"representation_passport":"recorded"' "$tmp/record.json"
"$bin" representations qwen2.5-coder-0.5b-native-fp16-fpq0 >"$tmp/list.json"
grep -q '"measurement":"measured"' "$tmp/list.json"
grep -q '"admission":"DEFER"' "$tmp/list.json"
grep -q '"scope":"generation"' "$tmp/list.json"
"$bin" representation-gate qwen2.5-coder-0.5b-native-fp16-fpq0/runtime-native-f16/2026-09-11 --scope generation >"$tmp/gate.json"
grep -q '"decision":"DEFER"' "$tmp/gate.json"
! "$bin" representation-gate qwen2.5-coder-0.5b-native-fp16-fpq0/runtime-native-f16/2026-09-11 --scope rerank >/dev/null 2>&1

cp "$root/cmd/BonfyreQwenFPQ/results/2026-09-11-qwen-native-fp16-representation-passport.json" "$tmp/unknown.json"
python3 - "$tmp/unknown.json" <<'PY'
import json, sys
p = sys.argv[1]
v = json.load(open(p))
v['untracked_semantic_field'] = 'must fail'
open(p, 'w').write(json.dumps(v))
PY
! "$bin" representation-record "$tmp/unknown.json" >/dev/null 2>&1

# The served catalogue carries the passport the registry holds, with its exact
# receipt digest and admission state, joined to its model.
rep='qwen2.5-coder-0.5b-native-fp16-fpq0/runtime-native-f16/2026-09-11'
test "$(sqlite3 "$tmp/catalog.db" "SELECT kind||'|'||category||'|'||source_hash FROM catalog_nodes WHERE node_id='representation:$rep'")" = \
  "representation|DEFER|$(sed -n 's/.*"receipt_digest": *"\([^"]*\)".*/\1/p' "$root/cmd/BonfyreQwenFPQ/results/2026-09-11-qwen-native-fp16-representation-passport.json")"
test "$(sqlite3 "$tmp/catalog.db" "SELECT meta FROM catalog_edges WHERE src_node_id='model:qwen2.5-coder-0.5b-native-fp16-fpq0' AND rel='has_representation' AND dst_node_id='representation:$rep'")" = 'DEFER:generation'
test "$(sqlite3 "$tmp/catalog.db" "SELECT json_extract(json_data,'\$.physical_bytes')>0 AND json_type(json_data,'\$.supports')='array' FROM catalog_nodes WHERE node_id='representation:$rep'")" = 1

# Every statically linked owner calls this shared sync, not just bonfyre-model.
# Emulate an older owner's scope deletion and prove a current owner restores
# both directions. A refused edge write must roll the entire refresh back.
cat >"$tmp/catalog-sync.c" <<'C'
#include <bonfyre.h>
int main(int argc, char **argv) {
    return argc == 3 ? bf_catalog_sync_repo(argv[1], argv[2]) : 2;
}
C
${CC:-cc} -I"$root/lib/libbonfyre/include" "$tmp/catalog-sync.c" \
  "$root/lib/libbonfyre/libbonfyre.a" -lsqlite3 -o "$tmp/catalog-sync"
sqlite3 "$tmp/catalog.db" "DELETE FROM catalog_edges WHERE rel IN ('has_representation','represents_model')"
"$tmp/catalog-sync" "$tmp/catalog.db" "$root"
test "$(sqlite3 "$tmp/catalog.db" "SELECT count(*) FROM catalog_edges WHERE rel IN ('has_representation','represents_model') AND meta='DEFER:generation'")" = 2
sqlite3 "$tmp/catalog.db" "CREATE TRIGGER reject_representation_edge BEFORE INSERT ON catalog_edges WHEN NEW.rel='represents_model' BEGIN SELECT RAISE(ABORT,'fixture refusal'); END"
! "$tmp/catalog-sync" "$tmp/catalog.db" "$root"
test "$(sqlite3 "$tmp/catalog.db" "SELECT count(*) FROM catalog_edges WHERE rel IN ('has_representation','represents_model') AND meta='DEFER:generation'")" = 2
sqlite3 "$tmp/catalog.db" "DROP TRIGGER reject_representation_edge"

# A passport naming a digest its receipt does not have is refused, and so is
# one whose receipt cannot be read.
sed 's/"receipt_digest": *"[^"]*"/"receipt_digest": "sha256:0000000000000000000000000000000000000000000000000000000000000000"/' \
  "$root/cmd/BonfyreQwenFPQ/results/2026-09-11-qwen-native-fp16-representation-passport.json" >"$tmp/wrong-digest.json"
! "$bin" representation-record "$tmp/wrong-digest.json" >/dev/null 2>"$tmp/wrong.err"
grep -q 'has digest' "$tmp/wrong.err"
sed 's#"receipt_ref": *"[^"]*"#"receipt_ref": "cmd/BonfyreModel/tests/no-such-receipt.json"#' \
  "$root/cmd/BonfyreQwenFPQ/results/2026-09-11-qwen-native-fp16-representation-passport.json" >"$tmp/missing-receipt.json"
! "$bin" representation-record "$tmp/missing-receipt.json" >/dev/null 2>&1

# status counts the bytes on disk, and verify corrects a registry size from
# the body it has just hashed.
mkdir -p "$tmp/cache"
head -c 3145728 /dev/zero >"$tmp/cache/qwen2.5-coder-0.5b-native-fp16-fpq0.bin"
sqlite3 "$BONFYRE_MODEL_DB" "UPDATE models SET sha256=NULL, size_mb=999 WHERE id='qwen2.5-coder-0.5b-native-fp16-fpq0'"
"$bin" status | grep -q 'Cached:.*(3 MB on disk)'
"$bin" verify qwen2.5-coder-0.5b-native-fp16-fpq0 >"$tmp/verify.txt"
grep -q '999.000 MB recorded -> 3.000 MB measured' "$tmp/verify.txt"
test "$(sqlite3 "$tmp/catalog.db" "SELECT json_extract(json_data,'\$.size_mb') FROM catalog_nodes WHERE node_id='model:qwen2.5-coder-0.5b-native-fp16-fpq0'")" = 3.0

echo 'BonfyreModel representation passport: PASS'
