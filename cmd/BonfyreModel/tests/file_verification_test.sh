#!/bin/sh
# Verify synthetic cached bytes without downloading or executing a model.
#   file_verification_test.sh BONFYRE_MODEL_BINARY
set -eu
binary=$(CDPATH='' cd -- "$(dirname -- "$1")" && pwd)/$(basename -- "$1")
root=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-model-hash.XXXXXX")
trap 'rm -rf "$root"' 0 1 2 15
cache=$root/cache; mkdir "$cache"
export BONFYRE_MODEL_DB="$root/models.db" BONFYRE_MODEL_CACHE="$cache" BONFYRE_CATALOG_DB="$root/catalog.db"
manifest=$root/manifest.json body=$cache/synthetic-bytes.bin checks=0
fail() { echo "BonfyreModel file verification: FAIL $*" >&2; exit 1; }
register() { # digest [model_id]
  printf '{"id":"%s","name":"Synthetic hash fixture","format":"bin","sha256":"%s","sources":[]}' "${2:-synthetic-bytes}" "$1" > "$manifest"
  "$binary" add "$manifest" >/dev/null 2>"$root/err" || fail "add: $(cat "$root/err")"
}
verify() { # expect_success text [model_id]
  rc=0; "$binary" verify "${3:-synthetic-bytes}" > "$root/out" 2>&1 || rc=$?
  if [ "$1" = yes ] && [ "$rc" -ne 0 ]; then fail "verify ${3:-synthetic-bytes} expected success: $(cat "$root/out")"; fi
  if [ "$1" = no ] && [ "$rc" -eq 0 ]; then fail "verify ${3:-synthetic-bytes} expected refusal: $(cat "$root/out")"; fi
  [ -z "$2" ] || grep -q -F -- "$2" "$root/out" || fail "verify ${3:-synthetic-bytes} missing '$2': $(cat "$root/out")"
  checks=$((checks + 1))
}
stored() { sqlite3 "$root/models.db" "select coalesce(sha256,'NULL') from models where id='$1'"; }

# 256 byte values, 65 times, then a partial final block: the same body as before.
i=0; : > "$root/block"; while [ $i -lt 256 ]; do printf "\\$(printf %o $i)" >> "$root/block"; i=$((i + 1)); done
: > "$root/data"; i=0; while [ $i -lt 65 ]; do cat "$root/block" >> "$root/data"; i=$((i + 1)); done
printf 'partial-final-block' >> "$root/data"
digest=$(shasum -a 256 "$root/data" | cut -d' ' -f1)
cp "$root/data" "$body"; register "$digest"
verify yes "$digest"
cmp -s "$body" "$root/data" || fail "verification changed the cached body"
{ head -c $(( $(wc -c < "$root/data") - 1 )) "$root/data"; printf X; } > "$body"
verify no FAIL
rm "$body"; verify no 'not in cache'
: > "$body"; empty=$(shasum -a 256 "$body" | cut -d' ' -f1); register "$empty"; verify yes "$empty"
rm "$body"; mkdir "$body"; verify no 'cannot hash'
register pending; verify no 'cannot hash'
[ "$(stored synthetic-bytes)" = NULL ] || fail "pending hash must not be stored"
rmdir "$body"; cp "$root/data" "$body"; verify yes "$digest"
register pending duplicate-body; cp "$root/data" "$cache/duplicate-body.bin"
verify no 'cannot store hash' duplicate-body
[ "$(stored duplicate-body)" = NULL ] || fail "duplicate body hash must not be stored"
[ "$(stored synthetic-bytes)" = "$digest" ] || fail "verified hash must remain stored"
verify yes "$digest"
echo "BonfyreModel file verification: PASS ($checks CLI checks)"
