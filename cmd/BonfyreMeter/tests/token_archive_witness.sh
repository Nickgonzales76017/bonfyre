#!/bin/sh
# Replays the owner's real reconciled agent-usage export into a throwaway Meter
# store and checks the recorded counters against the export's own summary.
# The export is private and stays where it is; nothing from it enters the repo.
# SKIP when the export is not on this machine.
set -eu
BIN=${1:?bonfyre-meter binary required}
case "$BIN" in /*) ;; *) BIN="$(CDPATH= cd -- "$(dirname "$BIN")" && pwd)/$(basename "$BIN")";; esac
DIR=${BONFYRE_AGENT_USAGE_DIR:-$HOME/Desktop/agent_usage_reconciled}
CSV="$DIR/reconciled_events.csv"; SUMMARY="$DIR/summary.json"
if [ ! -r "$CSV" ] || [ ! -r "$SUMMARY" ]; then echo "meter token archive witness: SKIP (no reconciled export)"; exit 0; fi
TMP=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-token-archive.XXXXXX")
trap 'rm -rf "$TMP"' EXIT HUP INT TERM
fail() { echo "meter token archive witness: $1" >&2; exit 1; }
get() { awk -F '\t' -v key="$1" '$1==key{print $2}' "$2"; }
want() { sqlite3 :memory: "select json_extract(readfile('$SUMMARY'),'$1')"; }

"$BIN" token-import --db "$TMP/m.db" --subject user:19 --source "$CSV" >"$TMP/import.tsv"
rows=$(get rows "$TMP/import.tsv")
[ "$rows" = "$(want '$.rows_out')" ] || fail "row count $rows differs from the export summary"
[ "$(get imported "$TMP/import.tsv")" = "$rows" ] || fail "not every row recorded"
"$BIN" token-import --db "$TMP/m.db" --subject user:19 --source "$CSV" >"$TMP/replay.tsv"
[ "$(get imported "$TMP/replay.tsv"):$(get replayed "$TMP/replay.tsv")" = "0:$rows" ] || fail "replay of the real export was not idempotent"
"$BIN" token-usage --db "$TMP/m.db" --subject user:19 >"$TMP/usage.tsv"
for p in claude codex; do
  got=$(awk -F '\t' -v p="$p" '$1=="model"&&$2==p{a+=$12+$13;r+=$4} END{printf "%d:%d", r, a}' "$TMP/usage.tsv")
  exp="$(want "\$.reconciled.$p.records"):$(want "\$.reconciled.$p.total_tokens")"
  [ "$got" = "$exp" ] || fail "$p records:tokens $got, export summary says $exp"
done
[ "$(get provisional_valuation_usd "$TMP/usage.tsv")" = unknown ] || fail "a price appeared without an admitted rate"
[ "$(get charged_usd "$TMP/usage.tsv")" = unknown_no_authoritative_ledger ] || fail "charge inferred"
echo "meter token archive witness: PASS ($rows rows, $(get source_reported_tokens "$TMP/usage.tsv") tokens, $(get nonadditive_snapshot_tokens "$TMP/usage.tsv") in inherited snapshots)"
