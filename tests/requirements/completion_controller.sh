#!/bin/sh
set -eu

# The controller self-test deliberately uses a synthetic graph.  It must never
# recurse into tests/unified_fabric_acceptance.sh, because that would test a
# running production graph from inside itself.
state=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-controller.XXXXXX")
trap 'rm -rf "$state"' EXIT
base="$state/completion"
mkdir -p "$base/runs/older" "$base/runs/current/logs" "$base/runs/current/states/A" "$base/runs/current/states/B" "$base/runs/current/states/C" "$base/runs/current/states/D"
printf 'older\n' >"$base/latest"
printf 'current\n' >"$base/latest"

status="$base/runs/current/requirement-status.tsv"
printf 'requirement\tstatus\tdependencies\tcommand\texit_code\tstdout_log\tstderr_log\tstate_directory\tevidence\tfailure_excerpt\n' >"$status"
printf 'A\tpassed\tnone\ttrue\t0\t%s\t%s\t%s\tevidence-a\t\n' "$base/runs/current/logs/A.out" "$base/runs/current/logs/A.err" "$base/runs/current/states/A" >>"$status"
printf 'B\tpassed\tnone\ttrue\t0\t%s\t%s\t%s\tevidence-b\t\n' "$base/runs/current/logs/B.out" "$base/runs/current/logs/B.err" "$base/runs/current/states/B" >>"$status"
printf 'C\tfailed\tA,B\tfalse\t1\t%s\t%s\t%s\tevidence-c\treal assertion failed\n' "$base/runs/current/logs/C.out" "$base/runs/current/logs/C.err" "$base/runs/current/states/C" >>"$status"
printf 'D\tblocked_dependency\tC\ttrue\tnull\t%s\t%s\t%s\tevidence-d\tdependency C did not pass\n' "$base/runs/current/logs/D.out" "$base/runs/current/logs/D.err" "$base/runs/current/states/D" >>"$status"

printf '{"requirement":"C","status":"failed","command":"false","exit_code":1,"error":"real assertion failed"}\n' >"$base/runs/current/evidence.jsonl"
python3 - "$base/runs/current/evidence.jsonl" <<'PY'
import json, sys
for line in open(sys.argv[1]):
    json.loads(line)
PY

[ "$(cat "$base/latest")" = current ]
awk -F '\t' 'NR==1 { exit !($1=="requirement" && $2=="status") }' "$status"
awk -F '\t' 'NR>1 && $1=="D" { exit !($2=="blocked_dependency" && $5=="null") }' "$status"

# A and B unlock C; C's failure blocks D.  The unique dependency-ready
# failure is C, proving multiple comma-separated dependencies are evaluated.
next=$(awk -F '\t' '
  NR==1 { next }
  $2=="passed" { passed[$1]=1; next }
  $2!="failed" { next }
  {
    split($3,deps,","); ready=1
    if ($3!="none") for (i in deps) if (!passed[deps[i]]) ready=0
    if (ready) { print $1; exit }
  }
' "$status")
[ "$next" = C ]

# A blocked-only status never constitutes a successful acceptance run.
blocked=1; failed=0; passed=0; total=1
if [ "$failed" -eq 0 ] && [ "$blocked" -eq 0 ] && [ "$passed" -eq "$total" ]; then
  exit 1
fi

# A fully passed synthetic run is accepted.
blocked=0; failed=0; passed=2; total=2
[ "$failed" -eq 0 ] && [ "$blocked" -eq 0 ] && [ "$passed" -eq "$total" ]
