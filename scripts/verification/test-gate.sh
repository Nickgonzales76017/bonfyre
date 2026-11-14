#!/usr/bin/env bash
# One declared, bounded, agent-quiet test scheduler. Not a public release witness.
# macOS Bash 3.2 treats an initialized, empty array as unset with -u.
# Array cardinalities are validated explicitly; retain -e and pipefail.
set -eo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cue_bin="${BONFYRE_CUE_BINARY:-cue}"; jq_bin="${BONFYRE_JQ_BINARY:-jq}"; bun_bin="${BONFYRE_BUN_BINARY:-bun}"
contract="$root/scripts/verification/suites.cue"
command -v "$cue_bin" >/dev/null || { echo 'REFUSE: CUE unavailable' >&2; exit 2; }
command -v "$jq_bin" >/dev/null || { echo 'REFUSE: jq unavailable' >&2; exit 2; }
"$cue_bin" vet "$contract" >&2
matrix=$("$cue_bin" export -e suites "$contract" --out json)
globals=$("$cue_bin" export -e globalWatches "$contract" --out json)
printf '%s' "$matrix" | "$jq_bin" -e 'type=="object" and (length>0) and all(.[]; (.files|length>0) and (.owner|length>0) and (.kind=="unit" or .kind=="browser" or .kind=="vector") and (.files|all(.[]; type=="string")))' >/dev/null || { echo 'REFUSE: invalid suite matrix' >&2; exit 2; }
suite_names=()
while IFS= read -r key; do
 [[ "$key" =~ ^[a-z][a-z0-9_]*$ ]] || { echo "REFUSE: suite name $key" >&2; exit 2; }
 suite_names+=("$key")
done < <(printf '%s' "$matrix" | "$jq_bin" -r 'keys[]')
mode="${1:-plan}"; [[ $# == 0 ]] || shift
case "$mode" in plan|run|list|validate) ;; *) echo 'usage: test-gate.sh plan|run|list|validate [--all|--suite NAME|--change FILE|--git|--staged|--since REF] [--fast|--r5-db PATH]' >&2; exit 2;; esac
selected=(); changes=(); all=0; automatic=0; staged=0; since=""; fast=0
# The native R5 store is optional; CUE selection remains authoritative when absent.
r5_db="${BONFYRE_TEST_R5_DB:-${BONFYRE_SPINES_HOME:-$HOME/.bonfyre/agent-hypervisor/native-spines-r7}/spines.sqlite3}"
if [[ "$mode" == validate ]]; then
 for suite in "${suite_names[@]}"; do
  while IFS= read -r path; do
   [[ "$path" != /* && "$path" != *".."* && -f "$root/$path" ]] || { echo "REFUSE: unsafe/missing $suite: $path" >&2; exit 2; }
  done < <(printf '%s' "$matrix" | "$jq_bin" -r --arg s "$suite" '.[$s].files[]')
  if [[ "$(printf '%s' "$matrix" | "$jq_bin" -r --arg s "$suite" '.[$s].kind')" == vector ]]; then
   build=$(printf '%s' "$matrix" | "$jq_bin" -r --arg s "$suite" '.[$s].build // empty')
   target=$(printf '%s' "$matrix" | "$jq_bin" -r --arg s "$suite" '.[$s].target // empty')
   [[ "$target" =~ ^bonfyre-[a-z0-9-]+$ && -n "$build" && "$build" != /* && "$build" != *".."* && -f "$root/$build/Makefile" ]] || { echo "REFUSE: missing native build for $suite" >&2; exit 2; }
  fi
 done
 echo "VERIFIED ${#suite_names[@]} declared suites"; exit 0
fi
if [[ "$mode" == list ]]; then
 for suite in "${suite_names[@]}"; do
  printf '%s\t%s\t%s\n' "$suite" "$(printf '%s' "$matrix" | "$jq_bin" -r --arg s "$suite" '.[$s].kind')" "$(printf '%s' "$matrix" | "$jq_bin" -r --arg s "$suite" '.[$s].owner')"
 done
 exit 0
fi
while [[ $# -gt 0 ]]; do
 case "$1" in
  --all) all=1; shift;;
  --suite) [[ $# -gt 1 ]] || exit 2; selected+=("$2"); shift 2;;
  --change) [[ $# -gt 1 ]] || exit 2; changes+=("${2#./}"); shift 2;;
  --git) automatic=1; shift;;
  --staged) staged=1; shift;;
  --fast) fast=1; shift;;
  --r5-db) [[ $# -gt 1 ]] || exit 2; r5_db="$2"; shift 2;;
  --since) [[ $# -gt 1 ]] || exit 2; since="$2"; shift 2;;
  *) echo "REFUSE: unknown option $1" >&2; exit 2;;
 esac
done
if [[ "$all" == 1 && ( "${#selected[@]}" -gt 0 || "${#changes[@]}" -gt 0 || "$automatic" == 1 || "$staged" == 1 || -n "$since" ) ]]; then echo 'REFUSE: incompatible selectors' >&2; exit 2; fi
if (( automatic + staged > 1 )) || [[ -n "$since" && ( "$automatic" == 1 || "$staged" == 1 ) ]]; then echo 'REFUSE: incompatible change sources' >&2; exit 2; fi
# Gather changed paths in the parent shell. Process substitutions hide Git
# failures, allowing a failed dependency scan to masquerade as zero changes.
change_output=""
if [[ -n "$since" ]]; then
 [[ "$since" =~ ^[a-zA-Z0-9][a-zA-Z0-9_./~^-]{0,127}$ ]] || { echo 'REFUSE: unsafe commit selector' >&2; exit 2; }
 git -C "$root" rev-parse --verify "$since^{commit}" >/dev/null 2>&1 || { echo 'REFUSE: unknown base commit' >&2; exit 2; }
 change_output=$(cd "$root" && git diff --name-only --diff-filter=ACDMRT "$since" --) || { echo 'REFUSE: Git base diff failed' >&2; exit 2; }
elif [[ "$staged" == 1 ]]; then
 change_output=$(cd "$root" && git diff --cached --name-only --diff-filter=ACDMRT --) || { echo 'REFUSE: staged diff failed' >&2; exit 2; }
elif [[ "$automatic" == 1 || ( "$all" == 0 && "${#selected[@]}" == 0 && "${#changes[@]}" == 0 ) ]]; then
 change_output=$(cd "$root" && { git diff --name-only HEAD || exit 2; git ls-files --others --exclude-standard || exit 2; } | LC_ALL=C sort -u) || { echo 'REFUSE: dirty-tree scan failed' >&2; exit 2; }
fi
if [[ -n "$change_output" ]]; then
 while IFS= read -r path; do [[ -z "$path" ]] || changes+=("$path"); done <<< "$change_output"
fi
for suite in "${selected[@]}"; do
 printf '%s' "$matrix" | "$jq_bin" -e --arg s "$suite" 'has($s)' >/dev/null || { echo "REFUSE: unknown suite $suite" >&2; exit 2; }
done
# The R5 read is a bounded, read-only dependency-closure projection.
# It can add tests, but never suppress a CUE-selected test or make an
# unknown change appear covered. An absent store is not an error for CI clones.
if [[ -n "$r5_db" && -e "$r5_db" ]]; then
 [[ -f "$r5_db" && ! -L "$r5_db" ]] || { echo 'REFUSE: unsafe R5 store' >&2; exit 2; }
 command -v sqlite3 >/dev/null || { echo 'REFUSE: R5 store present but sqlite3 missing' >&2; exit 2; }
elif [[ -n "$r5_db" && "${BONFYRE_TEST_R5_DB:-}" != "" ]]; then
 echo 'REFUSE: explicit R5 store missing' >&2; exit 2
else
 r5_db=""
fi
# A watched file is exact; directory and explicit glob prefixes expand.
match_input() {
 local changed="$1" watch="$2"
 if [[ "$watch" == */ ]]; then [[ "$changed" == "$watch"* ]]
 elif [[ "$watch" == *'*' ]]; then [[ "$changed" == "${watch%\*}"* ]]
 else [[ "$changed" == "$watch" ]]
 fi
}
for changed in "${changes[@]}"; do
 [[ -n "$changed" && "$changed" != /* && "$changed" != *".."* ]] || { echo 'REFUSE: unsafe changed path' >&2; exit 2; }
 affected=0
 while IFS= read -r global; do if match_input "$changed" "$global"; then all=1; affected=1; break; fi; done < <(printf '%s' "$globals" | "$jq_bin" -r '.[]')
 [[ "$all" == 1 ]] && break
 for suite in "${suite_names[@]}"; do
  found=0
  while IFS= read -r dependency; do if match_input "$changed" "$dependency"; then found=1; break; fi; done < <(printf '%s' "$matrix" | "$jq_bin" -r --arg s "$suite" '.[$s].watches[], .[$s].files[]')
  if [[ "$found" == 1 ]]; then selected+=("$suite"); affected=1; fi
 done
 # Native R5 typed dependency edges may discover additional affected suites.
 # File IDs are canonical relative paths; refuse SQL interpolation for others.
 if [[ -n "$r5_db" && "$changed" =~ ^[a-zA-Z0-9_./-]+$ ]]; then
  query="WITH RECURSIVE reach(kind,id,depth,seen) AS (
    SELECT 'file','$changed',0,'|file:$changed|'
    UNION ALL
    SELECT edge.target_kind,edge.target_id,reach.depth+1,
     reach.seen||edge.target_kind||':'||edge.target_id||'|'
    FROM dependency_edges edge JOIN reach
      ON edge.source_kind=reach.kind AND edge.source_id=reach.id
    WHERE reach.depth < 4 AND
      instr(reach.seen,'|'||edge.target_kind||':'||edge.target_id||'|')=0
  )
  SELECT DISTINCT id FROM reach WHERE kind='verification_suite' ORDER BY id LIMIT 33;"
  if ! graph=$(sqlite3 -readonly -batch -noheader "$r5_db" "$query" 2>&1); then
   echo "REFUSE: native R5 graph query failed: ${graph:0:500}" >&2; exit 2
  fi
  count=0
  while IFS= read -r suite; do
   [[ -n "$suite" ]] || continue
   count=$((count+1))
   (( count <= 32 )) || { echo 'REFUSE: R5 verification closure unbounded' >&2; exit 2; }
   printf '%s' "$matrix" | "$jq_bin" -e --arg s "$suite" 'has($s)' >/dev/null ||
     { echo "REFUSE: R5 suite not declared in CUE: $suite" >&2; exit 2; }
   selected+=("$suite")
  done <<< "$graph"
 fi
 if [[ "$affected" == 0 ]]; then echo "UNMAPPED $changed => full verification" >&2; all=1; break; fi
done
[[ "$all" == 0 ]] || selected=("${suite_names[@]}")
ordered=(); omitted_browser=0
for name in "${suite_names[@]}"; do
 for want in "${selected[@]}"; do
  if [[ "$name" == "$want" ]]; then
   kind=$(printf '%s' "$matrix" | "$jq_bin" -r --arg s "$name" '.[$s].kind')
   if [[ "$fast" == 1 && "$kind" == browser ]]; then omitted_browser=$((omitted_browser+1)); else ordered+=("$name"); fi
   break
  fi
 done
done
if [[ "${#ordered[@]}" == 0 ]]; then
 if [[ "$omitted_browser" -gt 0 ]]; then
  echo "REFUSE_NO_FAST_SUITES: $omitted_browser browser suite(s) require full verification" >&2
  exit 3
 fi
 echo 'NO_AFFECTED_SUITES (not a release witness)'
 exit 0
fi
if [[ "$mode" == plan ]]; then
 for suite in "${ordered[@]}"; do printf 'SELECT %s %s\n' "$suite" "$(printf '%s' "$matrix" | "$jq_bin" -r --arg s "$suite" '.[$s].kind')"; done
 printf 'PLAN %d/%d suites (%d browser deferred, not a release witness)\n' "${#ordered[@]}" "${#suite_names[@]}" "$omitted_browser"; exit 0
fi
umask 077
log_root="${BONFYRE_TEST_LOG_ROOT:-${TMPDIR:-/tmp}}"; mkdir -p "$log_root"
logs=$(mktemp -d "$log_root/bonfyre-verification.XXXXXXXX")
# A generated HTML snapshot can occupy an entire single log line. Bound both
# lines and bytes so agent transcripts cannot absorb a full document.
failure_excerpt() {
 local log="$1"
 tail -n 30 "$log" | LC_ALL=C cut -b 1-420 | tail -c 6000 >&2
}
fails=0
for suite in "${ordered[@]}"; do
 files=()
 while IFS= read -r path; do
  [[ "$path" != /* && "$path" != *".."* && -f "$root/$path" ]] || { echo "REFUSE: missing/unsafe $suite input $path" >&2; exit 2; }
  files+=("$path")
 done < <(printf '%s' "$matrix" | "$jq_bin" -r --arg s "$suite" '.[$s].files[]')
 file="$logs/$suite.log"
 kind=$(printf '%s' "$matrix" | "$jq_bin" -r --arg s "$suite" '.[$s].kind')
 if [[ "$kind" == vector ]]; then
  build=$(printf '%s' "$matrix" | "$jq_bin" -r --arg s "$suite" '.[$s].build // empty')
  target=$(printf '%s' "$matrix" | "$jq_bin" -r --arg s "$suite" '.[$s].target // empty')
  [[ "$target" =~ ^bonfyre-[a-z0-9-]+$ && -n "$build" && "$build" != /* && "$build" != *".."* && -f "$root/$build/Makefile" ]] || { echo "REFUSE: missing native target for $suite" >&2; exit 2; }
  if (cd "$root" && make -s -C tests/vectors bonfyre-vectors && make -s -C "$build" "$target" && tests/vectors/bonfyre-vectors --root . "${files[@]}") >"$file" 2>&1; then
   if grep -Eq '^vectors: [1-9][0-9]* file\(s\), [1-9][0-9]* checks passed, 0 failed$' "$file"; then
    echo "PASS $suite native vectors"; rm -f "$file"
   else
    echo "REFUSE $suite: no positive vector evidence; $file" >&2; fails=$((fails+1))
   fi
  else
   echo "FAIL $suite: $file" >&2; failure_excerpt "$file"; fails=$((fails+1))
  fi
 else
  command -v "$bun_bin" >/dev/null || { echo 'REFUSE: Bun unavailable' >&2; exit 2; }
  build=$(printf '%s' "$matrix" | "$jq_bin" -r --arg s "$suite" '.[$s].build // empty')
  if [[ -n "$build" ]]; then
   target=$(printf '%s' "$matrix" | "$jq_bin" -r --arg s "$suite" '.[$s].target // empty')
   [[ "$target" =~ ^bonfyre-[a-z0-9-]+$ && "$build" != /* && "$build" != *".."* && -f "$root/$build/Makefile" ]] || { echo "REFUSE: invalid native build for $suite" >&2; exit 2; }
   make -s -C "$root/$build" "$target" >"$file" 2>&1 || { echo "FAIL $suite native prerequisite: $file" >&2; failure_excerpt "$file"; fails=$((fails+1)); continue; }
  fi
  timeout_ms=30000
  [[ "$kind" != browser ]] || timeout_ms=90000
  if (cd "$root" && AGENT=1 "$bun_bin" test --timeout "$timeout_ms" --reporter=dots --bail 1 --retry 0 "${files[@]}") >"$file" 2>&1; then
  if grep -Eq '(^|[[:space:]])[1-9][0-9]* pass' "$file"; then echo "PASS $suite"; rm -f "$file"; else echo "REFUSE $suite: zero passing tests; $file" >&2; fails=$((fails+1)); fi
  else
   echo "FAIL $suite: $file" >&2; failure_excerpt "$file"; fails=$((fails+1))
  fi
 fi
done
if [[ "$fails" == 0 ]]; then rmdir "$logs"; echo "VERIFIED ${#ordered[@]} suites ($omitted_browser browser deferred; release acceptance independent)"; else echo "FAILED $fails suite(s); full logs $logs" >&2; fi
[[ "$fails" == 0 ]]
