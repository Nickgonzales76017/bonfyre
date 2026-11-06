#!/bin/sh
# Source-value equality and destination schema equality are distinct facts.
# No production connections or external services are used.
set -eu
bin="${1:-./bonfyre-bridge}"
case "$bin" in /*) ;; *) bin="$(pwd -P)/${bin#./}" ;; esac
command -v duckdb >/dev/null || { echo 'bridge column readback: SKIP (duckdb unavailable)'; exit 0; }
command -v sqlite3 >/dev/null || { echo 'bridge column readback: SKIP (sqlite3 unavailable)'; exit 0; }
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-bridge-types.XXXXXX")
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
src="$tmp/source.db"; dst="$tmp/target.db"
sqlite3 "$src" "CREATE TABLE item(id INTEGER, amount INTEGER, label TEXT);
INSERT INTO item VALUES(1,630,'Dallas'),(2,250,'Stafford');"
sqlite3 "$dst" "CREATE TABLE item(id INTEGER, amount TEXT, label TEXT, tracking TEXT);
INSERT INTO item VALUES(1,'630','Dallas','legacy'),(2,'250','Stafford','legacy');"
"$bin" db-verify --from "sqlite:$src" --to "sqlite:$dst" --tables item > "$tmp/schema.json"
python3 - "$tmp/schema.json" <<'PY'
import json,sys
r=json.load(open(sys.argv[1]))
assert r['verified'] is True and r['arrived']==1, r
row=r['tables'][0]
assert row['state']=='arrived' and row['values_match'] is True, row
assert row['column_types_equal'] is False and row['column_type_changes']>=2, row
assert 'tracking: (absent) ->' in row['type_changes'], row
assert 'amount:' in row['type_changes'], row
PY
# The source columns still compare equal; the extra destination field must
# neither be hashed as if it existed on the source nor be silently ignored.
rm -f "$dst"; cp "$src" "$dst"
"$bin" db-verify --from "sqlite:$src" --to "sqlite:$dst" --tables item > "$tmp/equal.json"
python3 - "$tmp/equal.json" <<'PY'
import json,sys
r=json.load(open(sys.argv[1]))
assert r['verified'] is True, r
row=r['tables'][0]
assert row['column_types_equal'] is True and row['column_type_changes']==0, row
PY
sqlite3 "$dst" "UPDATE item SET amount=631 WHERE id=1;"
if "$bin" db-verify --from "sqlite:$src" --to "sqlite:$dst" --tables item > "$tmp/drift.json"; then
  echo 'bridge column readback: mutated destination falsely verified' >&2; exit 1
fi
python3 - "$tmp/drift.json" <<'PY'
import json,sys
r=json.load(open(sys.argv[1]))
assert r['verified'] is False and r['differ']==1, r
row=r['tables'][0]
assert row['state']=='values_differ' and row['column_types_equal'] is True, row
PY
[ "$(sqlite3 "$src" 'SELECT amount FROM item WHERE id=1')" = 630 ] || {
  echo 'bridge column readback: source changed' >&2; exit 1;
}
echo 'bridge column readback: PASS (value parity, type drift, extra columns, mutation)'
