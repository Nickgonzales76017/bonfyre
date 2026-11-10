#!/bin/sh
# `sql` answers in JSON whatever the answer is: no rows, one row, or far more
# than a pipe holds. The DuckDB shell prints "[{]" for no rows; that used to
# reach every caller as an unparseable answer.
set -eu
here=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
q=${1:-"$here/bonfyre-query"}
[ -x "$q" ] || { echo "sql rows: SKIP (bonfyre-query not built)"; exit 0; }
command -v duckdb >/dev/null 2>&1 || { echo "sql rows: SKIP (no duckdb)"; exit 0; }
ask() { "$q" sql :memory: "$1" 2>/dev/null; }
[ "$(ask "select 1 as a where 1=0" | jq -c .)" = "[]" ] || { echo "sql rows: FAIL (no rows is not [])"; exit 1; }
[ "$(ask "select 1 as a, 'x' as b" | jq -c .)" = '[{"a":1,"b":"x"}]' ] || { echo "sql rows: FAIL (one row changed)"; exit 1; }
# A short literal that looks like the quirk is still data.
[ "$(ask "select '[{]' as a" | jq -r '.[0].a')" = "[{]" ] || { echo "sql rows: FAIL (a value was rewritten)"; exit 1; }
# 200,000 rows, several megabytes: every row arrives, in order, still JSON.
[ "$(ask "select i, repeat('x', 20) as pad from range(200000) t(i)" | jq -c '[length, .[0].i, .[-1].i]')" = "[200000,0,199999]" ] || { echo "sql rows: FAIL (large answer damaged)"; exit 1; }
# A failing query still fails.
if "$q" sql :memory: "select from nowhere" >/dev/null 2>&1; then echo "sql rows: FAIL (bad query reported success)"; exit 1; fi
echo "sql rows: ok"
