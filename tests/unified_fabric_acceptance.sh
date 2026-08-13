#!/bin/sh
set -u

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
base=${BONFYRE_STATE_DIR:-"${TMPDIR:-/tmp}/bonfyre-completion"}/completion
run_id="$(date -u +%Y%m%dT%H%M%SZ)-$$"
run="$base/runs/$run_id"
mkdir -p "$run/logs" "$run/states"
printf '%s\n' "$run_id" >"$base/latest"
map="$run/acceptance-map.tsv"; evidence="$run/evidence.jsonl"; failures="$run/failures.tsv"; status="$run/status.json"; requirement_status="$run/requirement-status.tsv"
: >"$map"; : >"$evidence"; : >"$failures"
printf 'requirement\tstatus\tdependencies\tcommand\texit_code\tstdout_log\tstderr_log\tstate_directory\tevidence\tfailure_excerpt\n' >"$requirement_status"

awk '
 /^requirement / {if(id) print id "\t" deps "\t" test "\t" proof; id=$2; deps="none"; test=""; proof=""; next}
 /^[[:space:]]+depends / {sub(/^[[:space:]]+depends /,"");deps=$0;next}
 /^[[:space:]]+test / {sub(/^[[:space:]]+test /,"");test=$0;next}
 /^[[:space:]]+evidence / {sub(/^[[:space:]]+evidence /,"");proof=$0;next}
 END {if(id) print id "\t" deps "\t" test "\t" proof}
' "$root/completion/requirements.yaff" >"$map"

python3 - "$map" <<'PY'
import sys

rows = []
ids = set()
for number, line in enumerate(open(sys.argv[1]), 1):
    parts = line.rstrip("\n").split("\t")
    if len(parts) != 4 or not all(parts[:3]):
        raise SystemExit(f"invalid requirement declaration at map line {number}")
    ident, dependencies, command, evidence = parts
    if ident in ids:
        raise SystemExit(f"duplicate requirement: {ident}")
    ids.add(ident)
    rows.append((ident, [] if dependencies == "none" else dependencies.split(",")))

graph = dict(rows)
for ident, dependencies in rows:
    for dependency in dependencies:
        if dependency == ident:
            raise SystemExit(f"self dependency: {ident}")
        if dependency not in graph:
            raise SystemExit(f"unknown dependency: {ident} -> {dependency}")

visiting, visited = set(), set()
def visit(ident):
    if ident in visiting:
        raise SystemExit(f"dependency cycle: {ident}")
    if ident in visited:
        return
    visiting.add(ident)
    for dependency in graph[ident]:
        visit(dependency)
    visiting.remove(ident)
    visited.add(ident)

for ident in graph:
    visit(ident)
PY

passed_file="$run/passed.ids"; processed_file="$run/processed.ids"
: >"$passed_file"; : >"$processed_file"
total=$(awk 'END {print NR}' "$map"); passed=0; failed=0; blocked=0

# Always leave an atomic terminal record.  This makes an interrupted run
# diagnosable instead of indistinguishable from a scheduler defect.
finalize_run() {
  runner_rc=${1:-1}
  terminal=failed
  [ "$runner_rc" -eq 0 ] && [ "$failed" -eq 0 ] && [ "$blocked" -eq 0 ] &&
    [ "$passed" -eq "$total" ] && terminal=passed
  [ "$runner_rc" -ne 0 ] && [ "$(wc -l <"$processed_file")" -lt "$total" ] && terminal=aborted
  temporary_status="$status.tmp.$$"
  printf '{"run_id":"%s","state":"%s","runner_exit_code":%s,"total":%s,"passed":%s,"failed":%s,"blocked_dependency":%s,"processed":%s,"ended_at":"%s","evidence":"%s","failures":"%s"}\n' \
    "$run_id" "$terminal" "$runner_rc" "$total" "$passed" "$failed" "$blocked" \
    "$(wc -l <"$processed_file")" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$evidence" "$failures" >"$temporary_status"
  mv "$temporary_status" "$status"
}

on_exit() { rc=$?; finalize_run "$rc"; }
trap on_exit EXIT

dependencies_passed() {
  [ "$1" = none ] && return 0
  oldifs=$IFS; IFS=,; set -- $1; IFS=$oldifs
  for dependency in "$@"; do grep -qx "$dependency" "$passed_file" || return 1; done
  return 0
}

record_requirement() {
  id=$1; deps=$2; command=$3; proof=$4
  state="$run/states/$id"; mkdir -p "$state"
  out="$run/logs/$id.stdout"; err="$run/logs/$id.stderr"; start=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  # A requirement must never inherit the scheduler's requirement-map stdin:
  # tools such as make or a shell test may read it and silently truncate this
  # topological pass.  Each requirement receives an explicit empty stdin.
  (cd "$root" && BONFYRE_STATE_DIR="$state" BONFYRE_ACCEPTANCE_RUN="$run" \
    BONFYRE_REQUIREMENT_ID="$id" sh -c "$command") </dev/null >"$out" 2>"$err"; code=$?
  if [ "$code" -eq 0 ]; then
    result=passed; passed=$((passed+1)); printf '%s\n' "$id" >>"$passed_file"; excerpt=""
  else
    result=failed; failed=$((failed+1))
    excerpt=$(awk 'NF && $0 !~ /Nothing to be done/ { first = first ? first : $0; last=$0 } END { print last ? last : first }' "$err" "$out" | tr '"' "'")
    printf '%s\t%s\t%s\t%s\n' "$id" "$code" "$command" "$excerpt" >>"$failures"
  fi
  end=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  clean_excerpt=$(printf '%s' "$excerpt" | tr '\t\r\n' '   ')
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$id" "$result" "$deps" "$command" "$code" "$out" "$err" "$state" "$proof" "$clean_excerpt" >>"$requirement_status"
  REQUIREMENT="$id" RESULT="$result" COMMAND="$command" CODE="$code" START="$start" END="$end" OUT="$out" ERR="$err" STATE="$state" PROOF="$proof" EXCERPT="$excerpt" python3 - <<'PY' >>"$evidence"
import json, os
print(json.dumps({"requirement":os.environ["REQUIREMENT"],"status":os.environ["RESULT"],"command":os.environ["COMMAND"],"exit_code":None if os.environ["CODE"] == "null" else int(os.environ["CODE"]),"start_time":os.environ["START"],"end_time":os.environ["END"],"stdout_log":os.environ["OUT"],"stderr_log":os.environ["ERR"],"state_directory":os.environ["STATE"],"evidence":os.environ["PROOF"],"error":os.environ["EXCERPT"]}, sort_keys=True))
PY
  printf '%s\n' "$id" >>"$processed_file"
}

# Schedule only dependency-ready nodes.  Requirements can be declared in any
# order; unresolved cycles and failed prerequisites are classified after no
# further runnable requirement exists.
while :; do
  progressed=0
  while IFS='	' read -r id deps command proof; do
    grep -qx "$id" "$processed_file" && continue
    if dependencies_passed "$deps"; then record_requirement "$id" "$deps" "$command" "$proof"; progressed=1; fi
  done <"$map"
  [ "$progressed" -eq 1 ] || break
done

while IFS='	' read -r id deps command proof; do
  grep -qx "$id" "$processed_file" && continue
  state="$run/states/$id"; mkdir -p "$state"; out="$run/logs/$id.stdout"; err="$run/logs/$id.stderr"
  : >"$out"; printf 'dependency not passed: %s\n' "$deps" >"$err"
  blocked=$((blocked+1)); code=null; excerpt="dependency not passed: $deps"
  printf '%s\tblocked_dependency\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$id" "$deps" "$command" "$code" "$out" "$err" "$state" "$proof" "$excerpt" >>"$requirement_status"
  REQUIREMENT="$id" RESULT=blocked_dependency COMMAND="$command" CODE=null START="" END="" OUT="$out" ERR="$err" STATE="$state" PROOF="$proof" EXCERPT="$excerpt" python3 - <<'PY' >>"$evidence"
import json, os
print(json.dumps({"requirement":os.environ["REQUIREMENT"],"status":os.environ["RESULT"],"command":os.environ["COMMAND"],"exit_code":None,"start_time":None,"end_time":None,"stdout_log":os.environ["OUT"],"stderr_log":os.environ["ERR"],"state_directory":os.environ["STATE"],"evidence":os.environ["PROOF"],"error":os.environ["EXCERPT"]}, sort_keys=True))
PY
done <"$map"
printf '{"run_id":"%s","total":%s,"passed":%s,"failed":%s,"blocked_dependency":%s,"evidence":"%s","failures":"%s"}\n' "$run_id" "$total" "$passed" "$failed" "$blocked" "$evidence" "$failures" >"$status"
[ "$failed" -eq 0 ] && [ "$blocked" -eq 0 ] && [ "$passed" -eq "$total" ]
