#!/bin/sh
# An update is as large as the field allows and says exactly what was sent.
# The statement used to be printed into a fixed 8 KiB buffer with the values
# inlined, so updating a record past that size was cut off and refused.
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
cms=${1:-"$root/cmd/BonfyreCMS/bonfyre-cms"}
[ -x "$cms" ] || { echo "large update: SKIP (bonfyre-cms not built)"; exit 0; }
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT INT TERM
db="$tmp/cms.db"
mkdir "$tmp/types"
cat >"$tmp/types/big_note.json" <<'J'
{"name":"big_note","displayName":"Big note","attributes":{
  "slug":{"type":"string","required":true,"uniqueWithinNamespace":true},
  "body":{"type":"text","required":true},
  "label":{"type":"string","required":false},
  "extra":{"type":"json","required":false}}}
J
run() { "$cms" "$@" --db "$db" --schemas "$tmp/types" >"$tmp/out" 2>"$tmp/err"; }
run schema apply "$tmp/types/big_note.json"
run entry create big_note --ns fixture '{"slug":"one","body":"short"}'

# 24,000 characters with the awkward ones in it: quotes, a backslash, a newline.
python3 - "$tmp" <<'P'
import json, sys
body = ("It's a \"quoted\" line with a back\\slash; and -- a comment marker.\n" * 400)[:24000]
json.dump({"body": body, "extra": {"note": "it's json", "n": [1, 2, 3]}}, open(sys.argv[1] + "/update.json", "w"))
open(sys.argv[1] + "/want.txt", "w").write(body)
P
run entry update big_note 1 --ns fixture "$(cat "$tmp/update.json")" || { echo "large update: FAIL (refused)"; tail -1 "$tmp/err"; exit 1; }
sqlite3 "$db" "select body from big_note where id=1" >"$tmp/got.txt"
printf '\n' >>"$tmp/want.txt"
cmp -s "$tmp/got.txt" "$tmp/want.txt" || { echo "large update: FAIL (stored text differs from what was sent)"; exit 1; }
[ "$(sqlite3 "$db" "select json_extract(extra,'\$.note') from big_note where id=1")" = "it's json" ]

# A status that tries to end the statement is stored as words, not run.
run entry update big_note 1 --ns fixture "{\"status\":\"x', body='gone\"}" || true
cmp -s "$tmp/want.txt" - <<EOF2 || { echo "large update: FAIL (a status value rewrote another column)"; exit 1; }
$(sqlite3 "$db" "select body from big_note where id=1")
EOF2

# Another namespace cannot update it, and an update that empties a required field is undone.
if run entry update big_note 1 --ns elsewhere '{"body":"taken"}'; then echo "large update: FAIL (updated across namespaces)"; exit 1; fi
if run entry update big_note 1 --ns fixture '{"body":""}'; then echo "large update: FAIL (required field emptied)"; exit 1; fi
[ "$(sqlite3 "$db" "select length(body) from big_note where id=1")" = 24000 ]
# Bulk updates use the same typed owner, preserve conditional stale detection,
# preserve each record's namespace, and roll a generation back when any
# non-stale row is invalid.
run entry create big_note --ns fixture '{"slug":"two","body":"short"}'
run entry create big_note --ns account-two '{"slug":"three","body":"account short"}'
cat >"$tmp/update-batch.jsonl" <<'J'
{"id":1,"attributes":{"extra":{"batch":true}}}
{"id":2,"attributes":{"body":"batched"},"if_field":"body","if_value":"short"}
{"id":2,"attributes":{"extra":{"stale":"must-not-land"}},"if_field":"body","if_value":"short"}
{"id":3,"namespace":"account-two","attributes":{"body":"account batched","label":"filled"},"if_field":"label","if_value":""}
J
run entry import-jsonl big_note "$tmp/update-batch.jsonl" --ns fixture --mode update
grep -q '"updated":3' "$tmp/out"
grep -q '"skipped":1' "$tmp/out"
[ "$(sqlite3 "$db" "select body from big_note where id=2")" = batched ]
[ "$(sqlite3 "$db" "select body from big_note where id=3")" = "account batched" ]
[ "$(sqlite3 "$db" "select label from big_note where id=3")" = "filled" ]
[ "$(sqlite3 "$db" "select json_extract(extra,'$.stale') from big_note where id=2")" = "" ]

cat >"$tmp/update-bad.jsonl" <<'J'
{"id":2,"attributes":{"body":"must-roll-back"}}
{"id":1,"attributes":{"body":""}}
J
if run entry import-jsonl big_note "$tmp/update-bad.jsonl" --ns fixture --mode update; then
  echo "large update: FAIL (invalid batch committed)"
  exit 1
fi
[ "$(sqlite3 "$db" "select body from big_note where id=2")" = batched ]

echo "large update: ok"
