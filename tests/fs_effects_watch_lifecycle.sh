#!/bin/sh
# Proves the writable-.effect-file mechanism end to end against the real
# workgraph effect machinery: a partial write triggers no external mutation,
# a fully-written file gets committed for real (publish-local: a real file
# materializes on disk only after commit), and a real receipt is produced.
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
watch="$root/scripts/bonfyre-fs-effects-watch"
write_effect="$root/scripts/bonfyre-fs-write-effect"
run_root=${BONFYRE_STATE_DIR:-/tmp/bonfyre-fs-effects-watch-lifecycle}
projection="$run_root/projection"
state="$run_root/state"

rm -rf "$run_root"
mkdir -p "$projection/effects/pending" "$state"
export BONFYRE_STATE_DIR="$state"

fail() { echo "FAIL: $*" >&2; exit 1; }

fabric="$root/programs/bonfyre/bonfyre"
make -C "$root/engine/core" >/dev/null
make -C "$root/programs/bonfyre" >/dev/null
"$fabric" fabric compile "$root/bonfyre.workspace.yaff" "$root/bonfyre.lock.yaff" \
    "$root/estate/catalog.yaff" "$root/estate/compositions.yaff" \
    "$root/estate/profiles.yaff" "$root/estate/legacy-operators.tsv" >/dev/null

echo "== partial write must not trigger anything =="
target_path="$run_root/target-partial.txt"
input_path="$run_root/input-partial.txt"
printf 'partial-write fixture\n' >"$input_path"
partial_file="$projection/effects/pending/partial.effect"
tmp_partial="$projection/effects/pending/.partial.effect.tmp"
printf '{"mission_id":"m-partial","adapter":"publish-local","target_uri":"file://%s","input_artifact_uri":"file://%s"' \
    "$target_path" "$input_path" >"$tmp_partial"
# Deliberately left un-renamed and truncated -- simulates an in-progress
# write. The watcher must never see this as a pending *.effect file.
"$watch" tick "$projection"
[ -e "$target_path" ] && fail "partial write triggered a real mutation"
echo "partial write correctly produced no mutation"
rm -f "$tmp_partial"

echo "== fully-written effect file is committed for real =="
target_path="$run_root/target-real.txt"
input_path="$run_root/input-real.txt"
printf 'real effect fixture content\n' >"$input_path"
[ ! -e "$target_path" ] || fail "target pre-existed before commit (test setup bug)"
"$write_effect" "$projection/effects/pending/real.effect" \
    "$(printf '{"mission_id":"m-real","adapter":"publish-local","target_uri":"file://%s","input_artifact_uri":"file://%s","authority":"test"}' "$target_path" "$input_path")"
"$watch" tick "$projection"

[ -e "$target_path" ] || fail "target file was not created by commit"
diff "$input_path" "$target_path" >/dev/null || fail "committed target content does not match input"
[ -e "$projection/effects/committed/real.effect" ] || fail "effect file was not moved to committed/"
[ -e "$projection/effects/committed/real.effect.receipt.json" ] || fail "no receipt file was written"
grep -q '"receipt_id":"rcpt-' "$projection/effects/committed/real.effect.receipt.json" || fail "receipt file has no real receipt_id"
echo "real effect committed: target materialized, content matches, receipt present"

echo "== malformed effect file is rejected, not silently dropped =="
"$write_effect" "$projection/effects/pending/bad.effect" '{"adapter":"publish-local"}'
"$watch" tick "$projection"
[ -e "$projection/effects/failed/bad.effect" ] || fail "malformed effect was not moved to failed/"
[ -e "$projection/effects/failed/bad.effect.error.json" ] || fail "no error file for malformed effect"
grep -q "mission_id" "$projection/effects/failed/bad.effect.error.json" || fail "error file doesn't name the missing field"
echo "malformed effect correctly rejected with a real error record"

echo "fs effects watch lifecycle: all checks passed"
