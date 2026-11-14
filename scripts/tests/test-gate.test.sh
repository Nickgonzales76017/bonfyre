#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
gate="$root/scripts/verification/test-gate.sh"
list=$(bash "$gate" list)
[[ "$list" == *activity_unit* && "$list" == *inbox_browser* ]] || exit 1
suite_count=$(printf '%s\n' "$list" | grep -c .)
one=$(bash "$gate" plan --change integrations/browser/tests/aurekai-model.browser.test.mjs)
[[ "$one" == *"SELECT model_browser browser"* && "$one" != *"SELECT activity_browser browser"* ]] || { echo 'exact test selection' >&2; exit 1; }
fan=$(bash "$gate" plan --change integrations/browser/aurekai-activity.mjs)
[[ "$fan" == *activity_browser* && "$fan" == *push_rail_browser* && "$fan" == *inbox_browser* ]] || { echo 'owner fanout' >&2; exit 1; }
all=$(bash "$gate" plan --change lib/libbonfyre/include/bonfyre.h)
[[ "$all" == *"PLAN $suite_count/$suite_count suites"* ]] || { echo 'crosscut selection' >&2; exit 1; }
if bash "$gate" plan --suite nonexistent_suite >/dev/null 2>&1; then echo 'unknown suite admitted' >&2; exit 1; fi
unknown=$(bash "$gate" plan --change external/unknown.c 2>/dev/null)
[[ "$unknown" == *"PLAN $suite_count/$suite_count suites"* ]] || { echo 'unmapped input skipped' >&2; exit 1; }
lookalike=$(bash "$gate" plan --change integrations/browser/tests/aurekai-model.browser.test.mjs.old 2>/dev/null)
[[ "$lookalike" == *"PLAN $suite_count/$suite_count suites"* ]] || { echo 'suffix lookalike incorrectly matched test' >&2; exit 1; }
vec=$(bash "$gate" plan --change cmd/BonfyreFinance/src/main.c)
[[ "$vec" == *"SELECT finance_vectors vector"* && "$vec" != *"SELECT experience_vectors vector"* ]] || { echo 'native vector owner selection' >&2; exit 1; }
queue=$(bash "$gate" plan --change cmd/BonfyreQueue/src/main.c --fast)
[[ "$queue" == *"SELECT queue_vectors vector"* ]] || { echo 'Queue vector selection absent' >&2; exit 1; }
# Account research and source-credit changes reach the installed-subtools journey.
for changed in cmd/BonfyreWebAddress/src/credit_profile_projection.h integrations/browser/source-desk.mjs integrations/browser/workspace-destinations.mjs; do
 plan=$(bash "$gate" plan --change "$changed")
 [[ "$plan" == *"SELECT finance_subtools browser"* ]] || { echo "WebAddress integration impact missing: $changed" >&2; exit 1; }
done
transcribe=$(bash "$gate" plan --change cmd/BonfyreTranscribe/src/main.c --fast)
[[ "$transcribe" == *"SELECT transcribe_caption_vectors vector"* && "$transcribe" != *"SELECT finance_browser browser"* ]] || { echo 'transcription owner selection invalid' >&2; exit 1; }
quick=$(bash "$gate" plan --change cmd/BonfyreFinance/src/main.c --fast)
[[ "$quick" == *"SELECT finance_vectors vector"* && "$quick" == *"SELECT finance_unit unit"* && "$quick" != *"SELECT finance_browser browser"* && "$quick" == *"browser deferred"* ]] || { echo 'fast tier failed' >&2; exit 1; }
if bash "$gate" plan --change integrations/browser/tests/aurekai-activity.browser.test.mjs --fast >/dev/null 2>&1; then
 echo 'browser-only change wrongly admitted as fast verification' >&2
 exit 1
fi
bystage=$(bash "$gate" plan --staged)
[[ "$bystage" == *"PLAN "* || "$bystage" == *"NO_AFFECTED_SUITES"* ]] || { echo 'staged selection invalid' >&2; exit 1; }
if bash "$gate" plan --since "--help" >/dev/null 2>&1; then echo 'invalid ref admitted' >&2; exit 1; fi
# A failed Git observation must refuse, never silently select zero suites.
git_fail=$(mktemp -d)
cat > "$git_fail/git" <<'MOCK_GIT'
#!/bin/sh
exit 78
MOCK_GIT
chmod 700 "$git_fail/git"
if PATH="$git_fail:$PATH" bash "$gate" plan --git >/dev/null 2>&1; then
 echo 'failed Git observation was treated as no changes' >&2; rm -rf "$git_fail"; exit 1
fi
rm -rf "$git_fail"
# A real temporary R5 graph augments, but cannot weaken, the declared selection.
if command -v sqlite3 >/dev/null 2>&1; then
 scratch=$(mktemp -d)
 trap 'rm -rf "$scratch"' EXIT
 db="$scratch/spines.sqlite3"
 sqlite3 "$db" "CREATE TABLE dependency_edges(source_kind TEXT,source_id TEXT,target_kind TEXT,target_id TEXT,edge_kind TEXT);
 INSERT INTO dependency_edges VALUES('file','cmd/BonfyreFinance/src/main.c','ir','finance-closure','declares');
 INSERT INTO dependency_edges VALUES('ir','finance-closure','verification_suite','experience_vectors','tests');"
 widened=$(bash "$gate" plan --change cmd/BonfyreFinance/src/main.c --r5-db "$db")
 [[ "$widened" == *"SELECT finance_vectors vector"* && "$widened" == *"SELECT experience_vectors vector"* ]] || { echo 'R5 transitive closure failed' >&2; exit 1; }
 sqlite3 "$db" "INSERT INTO dependency_edges VALUES('file','cmd/BonfyreFinance/src/main.c','verification_suite','unregistered_suite','tests');"
 if bash "$gate" plan --change cmd/BonfyreFinance/src/main.c --r5-db "$db" >/dev/null 2>&1; then echo 'R5 undeclared target admitted' >&2; exit 1; fi
fi
# Simulated giant single-line HTML failure proves diagnostics are byte-bounded
# while the full original log remains inspectable, without running a real suite.
scratch2=$(mktemp -d)
trap 'rm -rf "$scratch2"' EXIT
cat > "$scratch2/failing-bun" <<'MOCK_BUN'
#!/bin/sh
awk 'BEGIN {for(i=0;i<90000;i++)printf "X"; print ""}'
printf '\\nTRAILER_ROOT_CAUSE: fixture is invalid\\n' >&2
exit 1
MOCK_BUN
chmod 700 "$scratch2/failing-bun"
failure=$(BONFYRE_BUN_BINARY="$scratch2/failing-bun" BONFYRE_TEST_LOG_ROOT="$scratch2" bash "$gate" run --suite activity_unit 2>&1) && { echo 'mock failed suite falsely passed' >&2; exit 1; }
[[ "${#failure}" -lt 8000 && "$failure" == *"FAIL activity_unit:"* && "$failure" == *"TRAILER_ROOT_CAUSE"* ]] || { echo 'failure excerpt is unbounded or missing root cause' >&2; exit 1; }
bash "$gate" validate
echo 'verification selection: PASS'
