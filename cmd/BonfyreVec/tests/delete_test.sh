#!/bin/sh
set -eu
root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
bin=$root/bonfyre-vec
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-vec-delete.XXXXXX")
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
id1='document#p0'; id2='document#p1'; id3="other'; DELETE FROM artifacts; --"
jq -n --arg a "$id1" --arg b "$id2" --arg c "$id3" '{embeddings: ([$a,$b,$c] | to_entries | map(. as $e | {id:$e.value,source:"synthetic-document",type:"passage",text:$e.value,embedding: [range(0;384) | if . == $e.key then 1 else 0 end]}))}' > "$tmp/input.json"
jq -n '{vector: [range(0;384) | if . == 0 then 1 else 0 end]}' > "$tmp/query.json"
setup() { db=$tmp/$1.db; "$bin" init "$db" > /dev/null; "$bin" insert "$db" "$tmp/input.json" > /dev/null; }
setup first
"$bin" delete "$db" "$id1" "$id2" | jq -e '. == {requested_ids:2,deleted_vectors:2,deleted_artifacts:2}' >/dev/null
for exact in no yes; do
  if [ "$exact" = yes ]; then "$bin" search "$db" "$tmp/query.json" --top 3 --exact > "$tmp/out"; else "$bin" search "$db" "$tmp/query.json" --top 3 > "$tmp/out"; fi
  jq -e --arg id "$id3" '[.results[].id] == [$id]' "$tmp/out" >/dev/null
done
[ "$(sqlite3 "$db" 'SELECT count(*) FROM artifacts')" = 1 ]
"$bin" delete "$db" "$id1" "$id2" | jq -e '. == {requested_ids:2,deleted_vectors:0,deleted_artifacts:0}' >/dev/null
"$bin" delete "$db" "$id3" | jq -e '.deleted_artifacts == 1' >/dev/null
[ "$(sqlite3 "$db" 'SELECT count(*) FROM artifacts')" = 0 ]
setup rollback
sqlite3 "$db" "CREATE TRIGGER refuse_delete BEFORE DELETE ON artifacts WHEN OLD.id = 'document#p1' BEGIN SELECT RAISE(ABORT, 'retained for rollback test'); END;"
if "$bin" delete "$db" "$id1" "$id2" >"$tmp/out" 2>"$tmp/err"; then exit 1; fi
[ ! -s "$tmp/out" ]; grep -q 'retained for rollback test' "$tmp/err"
[ "$(sqlite3 "$db" 'SELECT count(*) FROM artifacts')" = 3 ]
"$bin" search "$db" "$tmp/query.json" --exact --top 3 > "$tmp/out"
jq -e --arg a "$id1" --arg b "$id2" --arg c "$id3" '([.results[].id] | sort) == ([$a,$b,$c] | sort)' "$tmp/out" >/dev/null
setup missing
if "$bin" delete "$tmp/absent.db" "$id1" >"$tmp/out" 2>"$tmp/err"; then exit 1; fi
[ ! -e "$tmp/absent.db" ]
if "$bin" delete "$db" '' >"$tmp/out" 2>"$tmp/err"; then exit 1; fi
[ "$(sqlite3 "$db" 'SELECT count(*) FROM artifacts')" = 3 ]
echo 'vec delete: 3 cases PASS'
