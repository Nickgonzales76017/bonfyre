#!/bin/sh
set -eu
bin=${1:?project mcp binary required}
case "$bin" in /*) ;; *) bin="$(pwd)/$bin" ;; esac
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-work-session.XXXXXX")
trap 'rm -rf "$tmp"' EXIT INT TERM
mkdir -p "$tmp/operator"
db="$tmp/operator/operator.sqlite3"
sqlite3 "$db" "CREATE TABLE goals(id TEXT PRIMARY KEY,title TEXT,state TEXT,created_at REAL,updated_at REAL);
CREATE TABLE runs(id TEXT PRIMARY KEY,goal_id TEXT,started_at REAL,completed_at REAL,state TEXT,response_id TEXT,summary TEXT NOT NULL DEFAULT '',usage_json TEXT NOT NULL DEFAULT '{}');
CREATE TABLE actions(seq INTEGER PRIMARY KEY AUTOINCREMENT,run_id TEXT,tool TEXT,args_json TEXT,state TEXT,result_json TEXT,started_at REAL,duration_ms INTEGER);
INSERT INTO goals VALUES('g1','Interactive composition','queued',1,1);"
run(){ BONFYRE_OPERATOR_HOME="$tmp/operator" "$bin" "$@"; }
run work-session-open g1 ChatGPT implementation chat-123 --apply | grep -q 'interactive-chat-123 opened'
run work-session-open g1 ChatGPT implementation chat-123 --apply | grep -q 'interactive-chat-123 reused'
[ "$(sqlite3 "$db" "select count(*) from runs where id='interactive-chat-123'")" = 1 ]
[ "$(sqlite3 "$db" "select agent||':'||role||':'||session_id from operator_run_context where run_id='interactive-chat-123'")" = 'ChatGPT:implementation:chat-123' ]
run work-session-step interactive-chat-123 project_search passed receipt-search --apply >/dev/null
run work-session-step interactive-chat-123 project_apply_low_risk_patch passed patch-123 --apply >/dev/null
[ "$(sqlite3 "$db" "select group_concat(tool||':'||state,',') from actions order by seq")" = 'project_search:passed,project_apply_low_risk_patch:passed' ]
run work-session-close interactive-chat-123 resolved 'Interactive slice complete' --apply >/dev/null
[ "$(sqlite3 "$db" "select state||':'||(completed_at is not null)||':'||summary from runs where id='interactive-chat-123'")" = 'resolved:1:Interactive slice complete' ]
if run work-session-step interactive-chat-123 project_search passed late-receipt --apply 2>/dev/null; then echo 'work session: closed run accepted a step' >&2; exit 1; fi
if run work-session-open g1 Claude review chat-123 --apply 2>/dev/null; then echo 'work session: conflicting session identity reopened' >&2; exit 1; fi
echo 'work session: PASS'
