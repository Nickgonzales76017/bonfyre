#!/bin/sh
set -eu

bin=${1:?operator store binary required}
tmp=$(mktemp -d)
db="$tmp/operator.sqlite3"
trap 'rm -rf "$tmp"' EXIT

set +e
"$bin" unknown --db "$tmp/unknown.sqlite3" >/dev/null 2>&1
unknown_status=$?
set -e
[ "$unknown_status" -eq 2 ]
[ ! -e "$tmp/unknown.sqlite3" ]

"$bin" init --db "$db" > "$tmp/init.json"
grep -q '"schema_version":"bonfyre.operator_store.v1"' "$tmp/init.json"
sqlite3 "$db" "INSERT INTO goals(id,dedupe_key,title,details,priority,state,source,recurring,interval_seconds,retry_at,attempts,last_error,created_at,updated_at) VALUES('00000000000000000000000000000001','fixture:goal-1','Open fixture goal','',50,'queued','fixture',0,0,0,0,'',1,1);"
sqlite3 "$db" "INSERT INTO goals(id,dedupe_key,title,details,priority,state,source,recurring,interval_seconds,retry_at,attempts,last_error,created_at,updated_at) VALUES('00000000000000000000000000000002','fixture:goal-2','Inactive fixture goal','',50,'blocked','fixture',0,0,0,0,'fixture block',1,1);"
sqlite3 "$db" "INSERT INTO runs(id,goal_id,started_at,completed_at,state,response_id,summary,usage_json) VALUES('run-1','00000000000000000000000000000001',1,NULL,'running',NULL,'','{}');"
"$bin" inspect --db "$db" > "$tmp/inspect.json"
grep -q '"goals":2' "$tmp/inspect.json"
grep -q '"queued_goals":1' "$tmp/inspect.json"
grep -q '"blocked_goals":1' "$tmp/inspect.json"
grep -q '"open_runs":1' "$tmp/inspect.json"

set +e
"$bin" archive --db "$db" --stop "$tmp/STOP" --reason 'fixture archival' --goal 00000000000000000000000000000002 >/dev/null 2>&1
missing_stop_status=$?
set -e
[ "$missing_stop_status" -eq 2 ]
[ "$(sqlite3 "$db" "SELECT state FROM goals WHERE id='00000000000000000000000000000002';")" = blocked ]

touch "$tmp/STOP"
"$bin" archive --db "$db" --stop "$tmp/STOP" --reason 'fixture archival' --goal 00000000000000000000000000000002 > "$tmp/archive.json"
grep -q '"archived":1' "$tmp/archive.json"
grep -q '"retained_history":true' "$tmp/archive.json"
[ "$(sqlite3 "$db" "SELECT state FROM goals WHERE id='00000000000000000000000000000002';")" = superseded ]
[ "$(sqlite3 "$db" "SELECT last_error FROM goals WHERE id='00000000000000000000000000000002';")" = "fixture block
superseded through STOP-gated native archival: fixture archival" ]
[ "$(sqlite3 "$db" "SELECT COUNT(*) FROM runs WHERE goal_id='00000000000000000000000000000001' AND completed_at IS NULL;")" = 1 ]

set +e
"$bin" archive --db "$db" --stop "$tmp/STOP" --reason 'fixture archival' --goal 00000000000000000000000000000001 >/dev/null 2>&1
open_run_status=$?
set -e
[ "$open_run_status" -eq 2 ]
[ "$(sqlite3 "$db" "SELECT state FROM goals WHERE id='00000000000000000000000000000001';")" = queued ]

sqlite3 "$db" "INSERT INTO actions(run_id,tool,args_json,state,result_json,started_at,duration_ms) VALUES('run-1','fixture','{}','reverted','{}',1,0);"
"$bin" reconcile-reverted --db "$db" --stop "$tmp/STOP" --run run-1 --summary 'fixture rollback' > "$tmp/reconcile.json"
grep -q '"reconciled":true' "$tmp/reconcile.json"
grep -q '"state":"blocked"' "$tmp/reconcile.json"
[ "$(sqlite3 "$db" "SELECT state FROM goals WHERE id='00000000000000000000000000000001';")" = blocked ]
[ "$(sqlite3 "$db" "SELECT state FROM runs WHERE id='run-1';")" = blocked ]
[ "$(sqlite3 "$db" "SELECT COUNT(*) FROM actions WHERE run_id='run-1' AND state='reverted';")" = 1 ]

set +e
"$bin" reconcile-reverted --db "$db" --stop "$tmp/STOP" --run run-1 --summary 'second pass' >/dev/null 2>&1
repeat_reconcile_status=$?
set -e
[ "$repeat_reconcile_status" -eq 2 ]

ln -s "$db" "$tmp/operator-link.sqlite3"
set +e
"$bin" inspect --db "$tmp/operator-link.sqlite3" >/dev/null 2>&1
link_status=$?
set -e
[ "$link_status" -eq 2 ]

printf '%s\n' 'operator store native substrate: PASS'
