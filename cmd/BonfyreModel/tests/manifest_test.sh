#!/bin/sh
# Exercise manifest parsing and atomic replacement through the native CLI.
#   manifest_test.sh BONFYRE_MODEL_BINARY
set -eu
binary=$(CDPATH='' cd -- "$(dirname -- "$1")" && pwd)/$(basename -- "$1")
root=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-model-manifest.XXXXXX")
trap 'rm -rf "$root"' 0 1 2 15
export BONFYRE_MODEL_DB="$root/models.db" BONFYRE_MODEL_CACHE="$root/cache" BONFYRE_CATALOG_DB="$root/catalog.db"
manifest=$root/manifest.json
fail() { echo "BonfyreModel manifest: FAIL $*" >&2; exit 1; }
has() { printf '%s' "$1" | grep -q -F -- "$2"; }
q() { sqlite3 "$root/models.db" "$1"; }

front=$("$binary")
has "$front" 'START HERE' && has "$front" 'setup <manifest.json>' || fail "front page"
for internal in 'representation-record' 'representation-gate' 'source add' 'rm --purge' 'push <id>'; do
  has "$front" "$internal" && fail "front page shows internal verb: $internal"
done
advanced=$("$binary" help --all)
has "$advanced" 'representation-record <passport.json>' && has "$advanced" 'source add <id> <url>' || fail "help --all"
[ ! -e "$root/models.db" ] || fail "help must not open the model store"

run() { # good|bad json
  printf '%s' "$2" > "$manifest"; rc=0; "$binary" add "$manifest" > "$root/out" 2>&1 || rc=$?
  if [ "$1" = good ] && [ "$rc" -ne 0 ]; then fail "add should succeed for $2: $(cat "$root/out")"; fi
  if [ "$1" = bad ] && [ "$rc" -eq 0 ]; then fail "add should refuse $2"; fi
}
base_head='"id":"real-model","name":"A \"quoted\" model","format":"safetensors"'
run good "{$base_head,\"description\":\"Nested id: \\\"wrong\\\"\",\"sources\":[{\"url\":\"https://example.org/weights?version=1\",\"scheme\":\"https\"},\"hf://owner/model/weights\"]}"
[ "$(q "select group_concat(url,'|') from (select url from sources where model_id='real-model' order by priority)")" = 'https://example.org/weights?version=1|hf://owner/model/weights' ] || fail "sources in priority order"
[ "$(q "select name from models where id='real-model'")" = 'A "quoted" model' ] || fail "quoted name"
q "insert into recipe_models values('test','real-model','embed')"
run good "{$base_head,\"description\":\"following fields are not sources\",\"sources\":[]}"
[ -z "$(q "select url from sources where model_id='real-model'")" ] || fail "empty sources replace"
[ "$(q "select count(*) from recipe_models where model_id='real-model'")" = 1 ] || fail "recipe link kept"
long=$(printf '%256s' '' | tr ' ' x)
for bad in "{$base_head,\"sources\":[{\"scheme\":\"https\"}]}" "{$base_head,\"sources\":\"url\"}" \
           "{$base_head,\"sources\":[7]}" "{$base_head,\"sources\":[\"a\\u0000b\"]}" \
           "{\"id\":\"real-model\",\"name\":\"$long\",\"format\":\"safetensors\",\"sources\":[]}" '{"id":' '[]'; do
  run bad "$bad"
  [ -z "$(q "select url from sources where model_id='real-model'")" ] || fail "refused manifest changed sources: $bad"
  [ "$(q "select name from models where id='real-model'")" = 'A "quoted" model' ] || fail "refused manifest changed name: $bad"
done
run good "{$base_head,\"sources\":[\"https://example.org/new\"]}"
[ "$(q "select url from sources where model_id='real-model'")" = 'https://example.org/new' ] || fail "new source"
# A later constraint failure must not remove the previous source set.
sha_a=$(printf '%64s' '' | tr ' ' a)
run good "{\"id\":\"second\",\"name\":\"A \\\"quoted\\\" model\",\"format\":\"safetensors\",\"sha256\":\"$sha_a\",\"sources\":[]}"
run bad "{$base_head,\"sha256\":\"$sha_a\",\"sources\":[\"https://example.org/weights?version=1\",\"hf://owner/model/weights\"]}"
[ "$(q "select url from sources where model_id='real-model'")" = 'https://example.org/new' ] || fail "constraint failure kept sources"

body=$root/setup-model.gguf
printf 'bonfyre-model-setup-fixture\n' > "$body"
digest=$(shasum -a 256 "$body" | cut -d' ' -f1)
size_mb=$(awk -v b="$(wc -c < "$body" | tr -d ' ')" 'BEGIN { printf "%.12f", b / 1048576 }')
printf '{"id":"setup-model","name":"Setup model","format":"gguf","sha256":"%s","size_mb":%s,"sources":["file://%s"]}' "$digest" "$size_mb" "$body" > "$manifest"
setup=$("$binary" setup "$manifest" 2>&1) || fail "setup: $setup"
has "$setup" 'body: ready and verified' && has "$setup" 'representation: unmeasured (0 measured, 0 admitted)' &&
  has "$setup" 'model prove setup-model <run.json>' || fail "setup output: $setup"
path=$("$binary" path setup-model)
[ -f "$path" ] || fail "path $path"

run_manifest=$root/run.json
write_run() { printf '{"kind":"qwen_generation","recipe_id":"bonfyre-qwen-fpq","status":"ok","created_at":"2026-10-23T16:00:00Z","model_id":"setup-model","model_hash":"%s","representation":"native-fp16","finite_model_state":true,"generated_tokens":2}' "$1" > "$run_manifest"; }
write_run "$digest"
evidence=$("$binary" prove setup-model "$run_manifest" 2>&1) || fail "prove: $evidence"
has "$evidence" '"measurement":"measured"' || fail "prove measured"
listed=$("$binary" representations setup-model)
has "$listed" '"measurement":"measured"' && has "$listed" '"admission":"DEFER"' && has "$listed" '"scope":"generation"' || fail "representations: $listed"
rep=$(q "select representation_id from model_representation_passport where model_id='setup-model' and measurement_state='measured' limit 1")
gate=$("$binary" representation-gate "$rep" --scope generation 2>&1) || fail "gate: $gate"
has "$gate" '"decision":"DEFER"' || fail "gate decision: $gate"
"$binary" representation-gate "$rep" --scope rerank >/dev/null 2>&1 && fail "wrong scope must refuse"
again=$("$binary" setup "$manifest")
has "$again" 'representation: evidence recorded (1 measured, 0 admitted)' || fail "setup again: $again"
write_run "$(printf '%64s' '' | tr ' ' 0)"
"$binary" prove setup-model "$run_manifest" >/dev/null 2>&1 && fail "wrong model hash must refuse"
[ "$(q "select count(*) from model_representation_passport where model_id='setup-model'")" = 1 ] || fail "refused proof added a passport"
echo 'BonfyreModel manifest: PASS'
