#!/bin/sh
# The account work index survives a rebuild that can read none of its sources:
# a mis-split source list once replaced every row with nothing and exited 0.
set -eu
queue=${1:?bonfyre-queue binary required}
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-account-work.XXXXXX")
trap 'rm -rf "$tmp"' EXIT INT TERM
sqlite3 "$tmp/operator.sqlite3" "CREATE TABLE goals(id TEXT,title TEXT,state TEXT,created_at REAL,updated_at REAL);
CREATE TABLE runs(id TEXT,goal_id TEXT,state TEXT,started_at REAL,completed_at REAL,usage_json TEXT);
CREATE TABLE actions(seq INTEGER PRIMARY KEY,run_id TEXT,tool TEXT,state TEXT,started_at REAL,duration_ms INTEGER);
CREATE TABLE operator_run_context(run_id TEXT PRIMARY KEY,agent TEXT,role TEXT,session_id TEXT,interactive INTEGER);
INSERT INTO goals VALUES('g1','Bylines run together on aurekai.live','blocked',1792800000,1792800001);
INSERT INTO runs VALUES('r1','g1','resolved',1792800000,1792800001,json_object('input_tokens',300,'output_tokens',30));
INSERT INTO actions VALUES(1,'r1','project_search','passed',1792800000,150);
INSERT INTO actions VALUES(2,'r1','public_surface_rollout','reverted',1792800000,150);
INSERT INTO operator_run_context VALUES('r1','ChatGPT','implementation','chat-123',1);"
index() { "$queue" account-work index --db "$tmp/work.db" --subject user:19 --scope self --owner user:19 "$@"; }
cat > "$tmp/codex.jsonl" <<'EOF'
{"type":"turn.completed","usage":{"input_tokens":100,"cached_input_tokens":80,"output_tokens":12,"reasoning_output_tokens":5}}
EOF
cat > "$tmp/claude.jsonl" <<'EOF'
{"type":"result","total_cost_usd":1.25,"usage":{"input_tokens":10,"cache_read_input_tokens":30,"cache_creation_input_tokens":20,"output_tokens":7}}
EOF
cat > "$tmp/incomplete.jsonl" <<'EOF'
{"type":"result","usage":{"input_tokens":10,"output_tokens":7}}
EOF
sqlite3 "$tmp/fabric.sqlite3" "CREATE TABLE tasks(id TEXT,title TEXT,state TEXT,created_at REAL,started_at REAL,completed_at REAL);
CREATE TABLE runs(id TEXT,task_id TEXT,agent TEXT,role TEXT,state TEXT,started_at REAL,completed_at REAL,stdout_path TEXT);
INSERT INTO tasks VALUES('f1','Fabric task','completed',1792800000,1792800000,1792800001);
INSERT INTO runs VALUES('c1','f1','codex','worker','passed',1792800000,1792800001,'$tmp/codex.jsonl');
INSERT INTO runs VALUES('c2','f1','claude','worker','passed',1792800000,1792800001,'$tmp/claude.jsonl');
INSERT INTO runs VALUES('c3','f1','claude','worker','passed',1792800000,1792800001,'$tmp/incomplete.jsonl');"
index --source "fabric=$tmp/fabric.sqlite3"
[ "$(sqlite3 "$tmp/work.db" "select count(*),sum(input_tokens),sum(output_tokens) from account_work_usage")" = '2|160|19' ] || { echo 'account work: Fabric terminal usage not reconciled' >&2; exit 1; }
[ "$(sqlite3 "$tmp/work.db" "select estimated_micro_usd from account_work_usage where run_id='c2'")" = 1250000 ] || { echo 'account work: Claude estimate missing' >&2; exit 1; }
sqlite3 "$tmp/work.db" "INSERT INTO account_work_estimate VALUES('account-fabric-f1','c1',100,12,2300000,'fixture');"
index --source "fabric=$tmp/fabric.sqlite3"
[ "$(sqlite3 "$tmp/work.db" "select estimated_micro_usd from account_work_usage where run_id='c1'")" = 2300000 ] || { echo 'account work: matching archive estimate missing' >&2; exit 1; }
sqlite3 "$tmp/work.db" "UPDATE account_work_estimate SET input_tokens=101 WHERE run_id='c1';"
index --source "fabric=$tmp/fabric.sqlite3"
[ "$(sqlite3 "$tmp/work.db" "select estimated_micro_usd is null from account_work_usage where run_id='c1'")" = 1 ] || { echo 'account work: stale archive estimate retained' >&2; exit 1; }
"$queue" account-work usage --db "$tmp/work.db" --subject user:19 --scope self --mission account-fabric-f1 > "$tmp/fabric-usage.tsv"
[ "$(awk -F '\t' '$1=="coverage" {print $2}' "$tmp/fabric-usage.tsv")" = partial ] || { echo 'account work: incomplete Fabric usage was not left unknown' >&2; exit 1; }
cat "$tmp/codex.jsonl" >> "$tmp/codex.jsonl.duplicate"
cat "$tmp/codex.jsonl" >> "$tmp/codex.jsonl.duplicate"
sqlite3 "$tmp/fabric.sqlite3" "UPDATE runs SET stdout_path='$tmp/codex.jsonl.duplicate' WHERE id='c1';"
index --source "fabric=$tmp/fabric.sqlite3"
[ "$(sqlite3 "$tmp/work.db" "select count(*) from account_work_usage")" = 1 ] || { echo 'account work: ambiguous terminal usage counted' >&2; exit 1; }
# Completed Fabric receipts become outputs only when the retained bytes still
# match the owner's digest and live in the owner's receipts directory.
mkdir -p "$tmp/receipts" "$tmp/elsewhere"
printf '{"review":"kept"}\n' > "$tmp/receipts/good.json"
printf '{"review":"edited"}\n' > "$tmp/receipts/tampered.json"
printf '{"review":"outside"}\n' > "$tmp/elsewhere/outside.json"
good=$(shasum -a 256 "$tmp/receipts/good.json" | cut -d' ' -f1)
outside=$(shasum -a 256 "$tmp/elsewhere/outside.json" | cut -d' ' -f1)
sqlite3 "$tmp/fabric.sqlite3" "CREATE TABLE receipts(id TEXT,task_id TEXT,kind TEXT,path TEXT,digest TEXT,payload_json TEXT,created_at REAL);
INSERT INTO tasks VALUES('f2','Failed Fabric task','failed',1792800000,1792800000,1792800001);
INSERT INTO receipts VALUES('rg','f1','cross-review','$tmp/receipts/good.json','$good','{}',1792800002);
INSERT INTO receipts VALUES('rt','f1','duel','$tmp/receipts/tampered.json','$good','{}',1792800002);
INSERT INTO receipts VALUES('ro','f1','relay','$tmp/elsewhere/outside.json','$outside','{}',1792800002);
INSERT INTO receipts VALUES('rf','f2','source-request','$tmp/receipts/good.json','$good','{}',1792800002);"
index --source "fabric=$tmp/fabric.sqlite3"
[ "$(sqlite3 "$tmp/work.db" "select group_concat(node_id||'='||output_ref) from account_work_step where work_id GLOB 'account-fabric-*'")" = "receipt-rg=sha256:$good" ] || { echo 'account work: Fabric receipt outputs not digest-verified' >&2; exit 1; }
"$queue" account-work outputs --db "$tmp/work.db" --subject user:19 --scope self > "$tmp/fabric-outputs.mp"
LC_ALL=C grep -a -q -F 'account-fabric-f1' "$tmp/fabric-outputs.mp" || { echo 'account work: verified Fabric receipt missing from outputs' >&2; exit 1; }
if LC_ALL=C grep -a -q -F "$tmp/receipts" "$tmp/fabric-outputs.mp"; then echo 'account work: Fabric receipt path leaked' >&2; exit 1; fi
sqlite3 "$tmp/fabric.sqlite3" "DROP TABLE receipts; DELETE FROM tasks WHERE id='f2';"
sqlite3 "$tmp/operator.sqlite3" "INSERT INTO goals VALUES('superseded','Superseded import marker','superseded',strftime('%s','now'),strftime('%s','now'));"
sqlite3 "$tmp/operator.sqlite3" "INSERT INTO goals VALUES('superseded-history','Completed historical assistant work','superseded',1792800000,1792800001);
INSERT INTO runs VALUES('history-run','superseded-history','resolved',1792800000,1792800001,'{}');"
sqlite3 "$tmp/fabric.sqlite3" "INSERT INTO tasks VALUES('cancelled','Cancelled Fabric marker','cancelled',strftime('%s','now'),strftime('%s','now'),strftime('%s','now'));"
index --source "operator=$tmp/operator.sqlite3" --source "fabric=$tmp/fabric.sqlite3"
"$queue" account-inbox list --db "$tmp/work.db" --subject user:19 --scope self --actor user:19 > "$tmp/filtered-inbox.mp"
if LC_ALL=C grep -a -q -F 'Superseded import marker' "$tmp/filtered-inbox.mp"; then echo 'account inbox: superseded import flooded notifications' >&2; exit 1; fi
LC_ALL=C grep -a -q -F 'Cancelled Fabric marker' "$tmp/filtered-inbox.mp" || { echo 'account inbox: a real Fabric cancellation was hidden' >&2; exit 1; }
[ "$(sqlite3 "$tmp/work.db" "SELECT count(*) FROM account_work WHERE id='account-operator-superseded'")" = 0 ] || { echo 'account work: never-started superseded goal still consumes board density' >&2; exit 1; }
[ "$(sqlite3 "$tmp/work.db" "SELECT count(*) FROM account_work WHERE id='account-operator-superseded-history'")" = 1 ] || { echo 'account work: superseded run history was discarded' >&2; exit 1; }
[ "$(sqlite3 "$tmp/work.db" "SELECT count(*) FROM account_work_run WHERE work_id='account-operator-superseded-history'")" = 1 ] || { echo 'account work: superseded run receipt was discarded' >&2; exit 1; }
sqlite3 "$tmp/operator.sqlite3" "DELETE FROM goals WHERE id='superseded';"
sqlite3 "$tmp/operator.sqlite3" "DELETE FROM goals WHERE id='superseded-history';DELETE FROM runs WHERE id='history-run';"
index --source "operator=$tmp/operator.sqlite3"
# A blocked assistant goal waits on the person, so the rail reads it as attention, not queued.
"$queue" account-work read --db "$tmp/work.db" --subject user:19 --scope self > "$tmp/read.mp"; LC_ALL=C grep -a -q -F "$(printf 'phase\251attention')" "$tmp/read.mp" && ! LC_ALL=C grep -a -q -F "$(printf 'phase\247pending')" "$tmp/read.mp" || { echo 'account work index: a goal waiting on the person did not read as attention' >&2; exit 1; }
[ "$(sqlite3 "$tmp/work.db" "select source from account_work")" = "Aurekai assistant" ] || { echo "account work index: synthetic root is not assistant-owned" >&2; exit 1; }
[ "$(sqlite3 "$tmp/work.db" "select worker_id from account_work_run")" = "ChatGPT · implementation" ] || { echo "account work index: interactive run lost agent role" >&2; exit 1; }
[ "$(sqlite3 "$tmp/work.db" "select group_concat(operator_id||':'||state,'|') from (select operator_id,state from account_work_step order by node_id)")" = "project_search:complete|public_surface_rollout:cancelled" ] || { echo "account work index: operator action state was not projected truthfully" >&2; exit 1; }
# The account view gets one fixed recovery sentence, never a raw provider error.
sqlite3 "$tmp/operator.sqlite3" "ALTER TABLE goals ADD COLUMN last_error TEXT NOT NULL DEFAULT ''; UPDATE goals SET state='retry',updated_at=CAST(strftime('%s','now') AS REAL),last_error='RateLimitError: insufficient_quota; internal provider diagnostic' WHERE id='g1';"
index --source "operator=$tmp/operator.sqlite3"
[ "$(sqlite3 "$tmp/work.db" "select attention from account_work where id='account-operator-g1'")" = 'OpenAI API credits are exhausted. Add credits before retrying this work.' ] || { echo 'account work index: quota recovery was not classified' >&2; exit 1; }
"$queue" account-inbox list --db "$tmp/work.db" --subject user:19 --scope self --actor user:19 --mission account-operator-g1 > "$tmp/credit.mp"
LC_ALL=C grep -a -q -F 'OpenAI API credits are exhausted. Add credits before retrying this work.' "$tmp/credit.mp" || { echo 'account work index: classified recovery did not reach Inbox' >&2; exit 1; }
if LC_ALL=C grep -a -q -F 'insufficient_quota' "$tmp/credit.mp"; then echo 'account work index: provider error leaked into Inbox' >&2; exit 1; fi
# Meter one run without inventing token counts for an unmetered second attempt.
sqlite3 "$tmp/operator.sqlite3" "INSERT INTO runs VALUES('r2','g1','failed',1792800001,1792800002,'{}');"
index --source "operator=$tmp/operator.sqlite3"
"$queue" account-work usage --db "$tmp/work.db" --subject user:19 --scope self --mission account-operator-g1 > "$tmp/usage.tsv"
[ "$(sqlite3 "$tmp/work.db" "select count(*),sum(input_tokens),sum(output_tokens) from account_work_usage")" = '1|300|30' ] || { echo 'account work: reported token rows not projected' >&2; exit 1; }
[ "$(awk -F '\t' '$1=="coverage" {print $2}' "$tmp/usage.tsv")" = partial ] || { echo 'account work: partial run coverage hidden' >&2; exit 1; }
if "$queue" account-work usage --db "$tmp/work.db" --subject user:20 --scope self --mission account-operator-g1 >/dev/null 2>&1; then echo 'account work: leaked other account usage' >&2; exit 1; fi
sqlite3 "$tmp/operator.sqlite3" "UPDATE runs SET usage_json='{}' WHERE id='r1';"
index --source "operator=$tmp/operator.sqlite3"
"$queue" account-work usage --db "$tmp/work.db" --subject user:19 --scope self --mission account-operator-g1 > "$tmp/usage.tsv"
[ "$(awk -F '\t' '$1=="coverage" {print $2}' "$tmp/usage.tsv")" = unmeasured ] || { echo 'account work: absent usage was fabricated' >&2; exit 1; }
[ "$(awk -F '\t' '$1=="reported_input_tokens" {print $2}' "$tmp/usage.tsv")" = unknown ] || { echo 'account work: absent usage treated as zero' >&2; exit 1; }
sqlite3 "$tmp/workgraph.db" "CREATE TABLE missions(id TEXT,status TEXT,created_at TEXT,updated_at TEXT);
CREATE TABLE workgraph_nodes(mission_id TEXT,node_id TEXT,operator_id TEXT,status TEXT,attempt INTEGER,last_heartbeat_at_ms INTEGER,claim_acquired_at_ms INTEGER,output_uri TEXT);
INSERT INTO missions VALUES('m1','complete','2026-10-26T10:00:00Z','2026-10-26T10:01:00Z');
INSERT INTO workgraph_nodes VALUES('m1','render','RenderAgent','complete',1,1793032860000,1793032800000,'artifact://output/render-1');
INSERT INTO workgraph_nodes VALUES('m1','job','RootAgent','complete',1,1793032860000,1793032800000,'file:///private/root-result');"
index --source "operator=$tmp/operator.sqlite3" --source "workgraph=$tmp/workgraph.db"
[ "$(sqlite3 "$tmp/work.db" "select output_ref from account_work_step where work_id='account-workgraph-m1' and node_id='render'")" = "artifact://output/render-1" ] || { echo "account work index: WorkGraph output reference was dropped" >&2; exit 1; }
"$queue" account-work outputs --db "$tmp/work.db" --subject user:19 --scope self > "$tmp/outputs.mp"
LC_ALL=C grep -a -q -F 'account-workgraph-m1' "$tmp/outputs.mp" || { echo 'account work: output page missed retained references' >&2; exit 1; }
if LC_ALL=C grep -a -q -F 'account-operator-g1' "$tmp/outputs.mp"; then echo 'account work: output page included work with no outputs' >&2; exit 1; fi
"$queue" account-work read --db "$tmp/work.db" --subject user:19 --scope self --mission account-workgraph-m1 > "$tmp/root.mp"
LC_ALL=C grep -a -q -F 'RootAgent output reference' "$tmp/root.mp" || { echo 'account work index: root output reference missing' >&2; exit 1; }
if LC_ALL=C grep -a -q -F '/private/root-result' "$tmp/root.mp"; then echo 'account work index: private output path leaked' >&2; exit 1; fi
"$queue" account-work audit --db "$tmp/work.db" --subject user:19 --scope self > "$tmp/audit.tsv"
[ "$(awk -F '\t' '$1=="work" {print $2}' "$tmp/audit.tsv")" = 2 ] || { echo 'account work audit: work coverage differs' >&2; exit 1; }
[ "$(awk -F '\t' '$1=="operator_work" {print $2}' "$tmp/audit.tsv")" = 1 ] || { echo 'account work audit: operator missing' >&2; exit 1; }
[ "$(awk -F '\t' '$1=="workgraph_work" {print $2}' "$tmp/audit.tsv")" = 1 ] || { echo 'account work audit: WorkGraph missing' >&2; exit 1; }
[ "$(awk -F '\t' '$1=="completed_output_references" {print $2}' "$tmp/audit.tsv")" = 2 ] || { echo 'account work audit: output reference lost' >&2; exit 1; }
[ "$(awk -F '\t' '$1=="freshness" {print $2}' "$tmp/audit.tsv")" = fresh ] || { echo 'account work audit: index recency not reported' >&2; exit 1; }
[ "$(awk -F '\t' '$1=="unreported_token_runs" {print $2}' "$tmp/audit.tsv")" = 2 ] || { echo 'account work audit: missing usage not counted' >&2; exit 1; }
[ "$(awk -F '\t' '$1=="token_coverage" {print $2}' "$tmp/audit.tsv")" = unmeasured ] || { echo 'account work audit: absent usage treated as complete' >&2; exit 1; }
"$queue" account-work audit --db "$tmp/work.db" --subject user:20 --scope self > "$tmp/other-audit.tsv"
[ "$(awk -F '\t' '$1=="work" {print $2}' "$tmp/other-audit.tsv")" = 0 ] || { echo 'account work audit: other account leaked' >&2; exit 1; }
if grep -aF 'Bylines run together' "$tmp/audit.tsv"; then echo 'account work audit: private titles exposed' >&2; exit 1; fi
sqlite3 "$tmp/operator.sqlite3" "DELETE FROM runs WHERE id='r2';"
sqlite3 "$tmp/operator.sqlite3" "DROP TABLE operator_run_context;"
index --source "operator=$tmp/operator.sqlite3"
[ "$(sqlite3 "$tmp/work.db" "select worker_id from account_work_run")" = "Aurekai assistant" ] || { echo "account work index: legacy operator fallback changed" >&2; exit 1; }
[ "$(sqlite3 "$tmp/work.db" "select count(*) from account_work")" = 1 ] || { echo 'account work index: first build empty' >&2; exit 1; }
sqlite3 "$tmp/malformed.sqlite3" "CREATE TABLE unrelated(id TEXT);"
if index --source "operator=$tmp/operator.sqlite3" --source "fabric=$tmp/malformed.sqlite3" 2>"$tmp/malformed.err"; then
    echo 'account work: existing malformed source committed a partial index' >&2
    exit 1
fi
grep -F 'is not a fabric store' "$tmp/malformed.err" >/dev/null
[ "$(sqlite3 "$tmp/work.db" "select count(*),min(title) from account_work")" = '1|Bylines run together on aurekai.live' ] || { echo 'account work: malformed source wiped retained work' >&2; exit 1; }
# Missing second owner is not a partial success. Preserve the last full
# multi-owner projection including WorkGraph output references and run rows.
index --source "operator=$tmp/operator.sqlite3" --source "workgraph=$tmp/workgraph.db"
before=$(sqlite3 "$tmp/work.db" "SELECT group_concat(id||':'||source,'|') FROM (SELECT id,source FROM account_work ORDER BY id)")
before_runs=$(sqlite3 "$tmp/work.db" "SELECT count(*) FROM account_work_run")
before_outputs=$(sqlite3 "$tmp/work.db" "SELECT group_concat(output_ref,'|') FROM account_work_step WHERE output_ref<>''")
if index --source "operator=$tmp/operator.sqlite3" --source "fabric=$tmp/missing-secondary.sqlite3" 2>"$tmp/missing-secondary.err"; then
    echo 'account work: missing second source published partial owner snapshot' >&2; exit 1
fi
grep -F 'cannot read' "$tmp/missing-secondary.err" >/dev/null
[ "$(sqlite3 "$tmp/work.db" "SELECT group_concat(id||':'||source,'|') FROM (SELECT id,source FROM account_work ORDER BY id)")" = "$before" ] || { echo 'account work: missing second owner deleted prior work' >&2; exit 1; }
[ "$(sqlite3 "$tmp/work.db" "SELECT count(*) FROM account_work_run")" = "$before_runs" ] || { echo 'account work: missing second owner lost run history' >&2; exit 1; }
[ "$(sqlite3 "$tmp/work.db" "SELECT group_concat(output_ref,'|') FROM account_work_step WHERE output_ref<>''")" = "$before_outputs" ] || { echo 'account work: missing second owner lost retained outputs' >&2; exit 1; }

if index --source "operator=$tmp/missing.sqlite3" 2>/dev/null; then echo 'account work index: unreadable rebuild reported success' >&2; exit 1; fi
[ "$(sqlite3 "$tmp/work.db" "SELECT group_concat(id||':'||source,'|') FROM (SELECT id,source FROM account_work ORDER BY id)")" = "$before" ] || { echo 'account work index: unreadable rebuild wiped multi-owner index' >&2; exit 1; }
if index 2>"$tmp/empty.err"; then echo 'account work index: empty source set erased owner snapshot' >&2; exit 1; fi
[ "$(sqlite3 "$tmp/work.db" "SELECT group_concat(id||':'||source,'|') FROM (SELECT id,source FROM account_work ORDER BY id)")" = "$before" ] || { echo 'account work index: empty owner list wiped retained state' >&2; exit 1; }
# Agents lane: Fabric session kind, the runner's wall time, a fixed limit flag
# from a short failed final, cache breakdowns and nil where nothing was reported.
sqlite3 "$tmp/agents.sqlite3" "CREATE TABLE tasks(id TEXT PRIMARY KEY,kind TEXT,title TEXT,state TEXT,created_at REAL,started_at REAL,completed_at REAL);
CREATE TABLE runs(id TEXT,task_id TEXT,agent TEXT,role TEXT,state TEXT,started_at REAL,completed_at REAL,stdout_path TEXT,duration_ms INTEGER,result_json TEXT);
INSERT INTO tasks VALUES('d1','duel','Duel task','completed',1792800000,1792800000,1792800010);
INSERT INTO runs VALUES('a1','d1','codex','peer-critique','passed',1792800000,1792800004,'$tmp/codex.jsonl',4200,'{}');
INSERT INTO runs VALUES('a2','d1','claude','convergence-arbiter','failed',1792800005,1792800006,'',777,json_object('final','You''ve hit your session limit · resets 2:50am'));
INSERT INTO runs VALUES('a3','d1','claude','cross-review','failed',1792800007,1792800009,'',NULL,json_object('final','The review discusses a rate limit at length but this one failed for another reason. '||printf('%.400c','x')));
INSERT INTO runs VALUES('a4','d1','claude','verification','running',1792800009,NULL,'',NULL,'{}');"
rm -f "$tmp/agents.db"
"$queue" account-work index --db "$tmp/agents.db" --subject user:19 --scope self --owner user:19 --source "fabric=$tmp/agents.sqlite3"
[ "$(sqlite3 "$tmp/agents.db" "select group_concat(run_id||':'||session_kind||':'||ifnull(duration_ms,'-')||':'||limit_signal,'|') from (select * from account_work_run order by run_id)")" = 'a1:duel:4200:0|a2:duel:777:1|a3:duel:2000:0|a4:duel:-:0' ] || { echo 'account work agents: Fabric run detail not projected' >&2; exit 1; }
[ "$(sqlite3 "$tmp/agents.db" "select cache_read_tokens||':'||ifnull(cache_write_tokens,'nil') from account_work_usage where run_id='a1'")" = '80:nil' ] || { echo 'account work agents: Codex cache breakdown invented or lost' >&2; exit 1; }
# Provider session identity: only the runner's own well-formed session_id is
# projected, as Meter's exact join key. A malformed one stays empty.
sqlite3 "$tmp/sessions.sqlite3" "CREATE TABLE tasks(id TEXT PRIMARY KEY,kind TEXT,title TEXT,state TEXT,created_at REAL,started_at REAL,completed_at REAL);
CREATE TABLE runs(id TEXT,task_id TEXT,agent TEXT,role TEXT,state TEXT,session_id TEXT,started_at REAL,completed_at REAL,stdout_path TEXT);
INSERT INTO tasks VALUES('s1','duel','Session task','completed',1792800000,1792800000,1792800010);
INSERT INTO runs VALUES('b1','s1','claude','arbiter','failed','14eafccc-76cc-42de-b864-6a6384a90d85',1792800000,1792800004,'');
INSERT INTO runs VALUES('b2','s1','codex','critic','passed','../etc/passwd',1792800005,1792800006,'');
INSERT INTO runs VALUES('b3','s1','codex','critic','passed',NULL,1792800007,1792800008,'');"
rm -f "$tmp/sessions.db"
"$queue" account-work index --db "$tmp/sessions.db" --subject user:19 --scope self --owner user:19 --source "fabric=$tmp/sessions.sqlite3" >/dev/null
[ "$(sqlite3 "$tmp/sessions.db" "select group_concat(run_id||'='||provider_session_ref,'|') from (select * from account_work_run order by run_id)")" = 'b1=14eafccc-76cc-42de-b864-6a6384a90d85|b2=|b3=' ] || { echo 'account work: provider session join key invented or lost' >&2; exit 1; }
"$queue" account-work agents --db "$tmp/agents.db" --subject user:19 --scope self > "$tmp/agents.mp"
for want in "$(printf 'session\244duel')" "$(printf 'phase\251attention')" "$(printf 'unconfirmed\303')" "$(printf 'limit\303')" "$(printf 'agents\224')"; do
  LC_ALL=C grep -a -q -F "$want" "$tmp/agents.mp" || { echo "account work agents: packet missing $want" >&2; exit 1; }
done
if LC_ALL=C grep -a -q -F 'session limit' "$tmp/agents.mp"; then echo 'account work agents: provider final leaked' >&2; exit 1; fi
"$queue" account-work agents --db "$tmp/agents.db" --subject user:20 --scope self > "$tmp/other-agents.mp"
LC_ALL=C grep -a -q -F "$(printf 'agents\220')" "$tmp/other-agents.mp" || { echo 'account work agents: another account saw runs' >&2; exit 1; }
if "$queue" account-work agents --db "$tmp/agents.db" --subject user:19 --scope self --mission account-fabric-d1 >/dev/null 2>&1; then echo 'account work agents: mission coordinate accepted' >&2; exit 1; fi
# An assistant run the provider refused for credit carries the limit flag; an ordinary failure does not.
sqlite3 "$tmp/opq.sqlite3" "CREATE TABLE goals(id TEXT,title TEXT,state TEXT,created_at REAL,updated_at REAL);
CREATE TABLE runs(id TEXT,goal_id TEXT,state TEXT,started_at REAL,completed_at REAL,summary TEXT);
CREATE TABLE actions(seq INTEGER PRIMARY KEY,run_id TEXT,tool TEXT,state TEXT,started_at REAL,duration_ms INTEGER);
INSERT INTO goals VALUES('gq','Parked by credit','retry',1792800000,1792800001);
INSERT INTO runs VALUES('rq','gq','failed',1792800000,1792800001,'RateLimitError: Error code: 429 - insufficient_quota credit_balance_exhausted');
INSERT INTO runs VALUES('rf','gq','failed',1792800002,1792800003,'AssertionError: expected 2 got 3');"
"$queue" account-work index --db "$tmp/opq.db" --subject user:19 --scope self --owner user:19 --source "operator=$tmp/opq.sqlite3" >/dev/null
[ "$(sqlite3 "$tmp/opq.db" "select group_concat(run_id||':'||limit_signal,' ') from (select run_id,limit_signal from account_work_run order by run_id)")" = "rf:0 rq:1" ] || { echo 'account work index: a provider credit refusal was not flagged, or an ordinary failure was' >&2; exit 1; }
# Governed WorkGraph runs: only an attempt that attested its harness session under
# the live claim becomes a run; an unattested attempt stays a step, and a receipt
# whose session or handoff is malformed is never projected or repaired.
printf 'g1\tdeveloper.task\tfile:///task\tqueue\t100\t0\t0\ng2\tdeveloper.task\tfile:///task2\tqueue\t100\t0\t0\n' >"$tmp/batch.tsv"
"$queue" enqueue-batch "$tmp/batch.tsv" --db "$tmp/governed.db" >/dev/null
tok() { sed -E 's/.*"claim_token":"([0-9a-f]{64})".*/\1/'; }
t1=$("$queue" claim g1 job dev --db "$tmp/governed.db" | tok); t2=$("$queue" claim g2 job dev --db "$tmp/governed.db" | tok)
if "$queue" run-attest g1 job dev "$t1" --agent gemini --session 019a0000-1111-2222 --db "$tmp/governed.db" >/dev/null; then echo 'account work: unknown harness attested' >&2; exit 1; fi
"$queue" run-attest g1 job dev "$t1" --agent codex --session 019a0000-1111-2222 --db "$tmp/governed.db" >/dev/null
"$queue" ack g1 job dev "$t1" --output-uri file:///g1 --db "$tmp/governed.db" >/dev/null
"$queue" ack g2 job dev "$t2" --output-uri file:///g2 --db "$tmp/governed.db" >/dev/null
cp "$tmp/governed.db" "$tmp/forged.db"
sqlite3 "$tmp/forged.db" "UPDATE receipts SET payload=json_set(payload,'$.provider_session_ref','../etc/passwd') WHERE json_extract(payload,'$.transition')='run_attested'"
"$queue" account-work index --db "$tmp/governed-work.db" --subject user:19 --scope self --owner user:19 --source "workgraph=$tmp/governed.db" >/dev/null
[ "$(sqlite3 "$tmp/governed-work.db" "select group_concat(work_id||'/'||run_id||'='||agent||':'||provider_session_ref||':'||state,'|') from account_work_run")" = 'account-workgraph-g1/job#1=codex:019a0000-1111-2222:completed' ] || { echo 'account work: governed attempts projected wrongly' >&2; exit 1; }
[ "$(sqlite3 "$tmp/governed-work.db" "select count(*) from account_work where id in('account-workgraph-g1','account-workgraph-g2')")" = 2 ] || { echo 'account work: governed Work lost' >&2; exit 1; }
"$queue" account-work index --db "$tmp/forged-work.db" --subject user:19 --scope self --owner user:19 --source "workgraph=$tmp/forged.db" >/dev/null
[ "$(sqlite3 "$tmp/forged-work.db" "select count(*) from account_work_run")" = 0 ] || { echo 'account work: malformed attested session projected' >&2; exit 1; }
# Meter evidence is projected only for a single, exact provider session/run.
sqlite3 "$tmp/meter-fabric.db" "CREATE TABLE tasks(id TEXT,title TEXT,state TEXT,created_at REAL,started_at REAL,completed_at REAL);
CREATE TABLE runs(id TEXT,task_id TEXT,agent TEXT,role TEXT,state TEXT,session_id TEXT,started_at REAL,completed_at REAL);
INSERT INTO tasks VALUES('meter','Meter task','completed',1,1,2);
INSERT INTO runs VALUES('exact','meter','claude','worker','failed','session-exact',1,2);
INSERT INTO runs VALUES('shared1','meter','claude','worker','failed','session-shared',1,2);
INSERT INTO runs VALUES('shared2','meter','claude','worker','failed','session-shared',1,2);
INSERT INTO runs VALUES('snapshot','meter','codex','worker','failed','session-snapshot',1,2);
INSERT INTO runs VALUES('wrong','meter','codex','worker','failed','session-wrong',1,2);"
sqlite3 "$tmp/meter.db" "CREATE TABLE events(key_id TEXT,op TEXT,provider TEXT,session_ref TEXT,counter_basis TEXT,uncached_input_tokens INTEGER,cached_input_tokens INTEGER,cache_write_tokens INTEGER,output_tokens INTEGER);
INSERT INTO events VALUES('user:19','model.tokens','claude','session-exact','message_usage',10,20,30,4),('user:19','model.tokens','claude','session-exact','message_usage',1,2,3,5),('user:20','model.tokens','claude','session-exact','message_usage',1000,0,0,0),('user:19','model.tokens','claude','session-shared','message_usage',9,0,0,1),('user:19','model.tokens','codex','session-snapshot','counter_delta',8,0,0,1),('user:19','model.tokens','codex','session-snapshot','inherited_cumulative_snapshot',500,0,0,5),('user:19','model.tokens','claude','session-wrong','message_usage',9,0,0,1);"
"$queue" account-work index --db "$tmp/meter-work.db" --subject user:19 --scope self --owner user:19 --source "meter=$tmp/meter.db" --source "fabric=$tmp/meter-fabric.db" >/dev/null
[ "$(sqlite3 "$tmp/meter-work.db" "SELECT group_concat(run_id||':'||input_tokens||':'||output_tokens||':'||cache_read_tokens||':'||cache_write_tokens||':'||(estimated_micro_usd IS NULL)) FROM account_work_usage")" = 'exact:66:9:22:33:1' ] || { echo 'account work: Meter exact attribution failed' >&2; exit 1; }
"$queue" account-work index --db "$tmp/meter-work.db" --subject user:19 --scope self --owner user:19 --source "fabric=$tmp/meter-fabric.db" --source "meter=$tmp/meter.db" >/dev/null
[ "$(sqlite3 "$tmp/meter-work.db" 'SELECT count(*) FROM account_work_usage')" = 1 ] || { echo 'account work: Meter replay changed evidence' >&2; exit 1; }
# Native Compute reads Meter evidence without invented rates or cross-account attribution.
cat >"$tmp/compute-check.mjs" <<'JS'
import {readFileSync} from 'node:fs';
const {decode}=await import(process.env.BONFYRE_MP_SOURCE);
const reply=decode(new Uint8Array(readFileSync(process.env.BONFYRE_PACKET)));
const v=reply[2];
if(reply[0]!==1||reply[1]!==true||v.records!==Number(process.env.EXPECT_ROWS)||
   v.additiveTokens!==Number(process.env.EXPECT_TOKENS)||
   (process.env.EXPECT_ROWS==='6' && (v.providerSessions!==4||
    v.inheritedSnapshotTokens!==505||v.cachedInputTokens!==22||
    v.providers.length!==2)))throw Error('Account Compute evidence mismatch: '+JSON.stringify(v));
JS
"$queue" account-work compute --db "$tmp/meter-work.db" --subject user:19 --scope self --owner user:19 --source "meter=$tmp/meter.db" >"$tmp/compute.mp"
BONFYRE_MP_SOURCE="file://$root/integrations/browser/workspace-msgpack.mjs" BONFYRE_PACKET="$tmp/compute.mp" EXPECT_ROWS=6 EXPECT_TOKENS=104 bun "$tmp/compute-check.mjs"
"$queue" account-work compute --db "$tmp/meter-work.db" --subject user:20 --scope self --owner user:20 --source "meter=$tmp/meter.db" >"$tmp/compute-foreign.mp"
BONFYRE_MP_SOURCE="file://$root/integrations/browser/workspace-msgpack.mjs" BONFYRE_PACKET="$tmp/compute-foreign.mp" EXPECT_ROWS=1 EXPECT_TOKENS=1000 bun "$tmp/compute-check.mjs"
if "$queue" account-work compute --db "$tmp/meter-work.db" --subject user:19 --scope institution --owner user:19 --source "meter=$tmp/meter.db" >"$tmp/compute-refused.mp"; then echo 'account compute: cross-scope read admitted' >&2; exit 1; fi
[ ! -s "$tmp/compute-refused.mp" ] || { echo 'account compute: refusal leaked token evidence' >&2; exit 1; }
# Meter is optional: missing, malformed and corrupt stores warn, remove stale
# Meter usage and preserve direct owner usage and the normal Work projection.
sqlite3 "$tmp/operator.sqlite3" "UPDATE runs SET usage_json=json_object('input_tokens',300,'output_tokens',30) WHERE id='r1';"
printf 'not a sqlite database' > "$tmp/corrupt-meter.db"
sqlite3 "$tmp/malformed-meter.db" 'CREATE TABLE unrelated(id INTEGER);'
for bad_meter in "$tmp/missing-meter.db" "$tmp/corrupt-meter.db" "$tmp/malformed-meter.db"; do
    "$queue" account-work index --db "$tmp/meter-work.db" --subject user:19 --scope self --owner user:19 --source "operator=$tmp/operator.sqlite3" --source "fabric=$tmp/meter-fabric.db" --source "meter=$bad_meter" >/dev/null 2>"$tmp/meter-warning"
    grep -F 'warning: Meter unavailable; Meter token evidence omitted' "$tmp/meter-warning" >/dev/null || { echo 'account work: missing Meter warning' >&2; exit 1; }
    [ "$(sqlite3 "$tmp/meter-work.db" "SELECT group_concat(work_id||':'||run_id||':'||input_tokens) FROM account_work_usage")" = 'account-operator-g1:r1:300' ] || { echo 'account work: unavailable Meter lost owner usage or retained stale Meter usage' >&2; exit 1; }
    [ "$(sqlite3 "$tmp/meter-work.db" "SELECT count(*) FROM account_work WHERE id IN ('account-operator-g1','account-fabric-meter')")" = 2 ] || { echo 'account work: unavailable Meter lost Work' >&2; exit 1; }
done
# An error after a Meter query starts must discard all of its projected rows.
cp "$tmp/meter.db" "$tmp/overflow-meter.db"
sqlite3 "$tmp/meter-fabric.db" "INSERT INTO runs VALUES('overflow','meter','claude','worker','failed','session-overflow',1,2);"
sqlite3 "$tmp/overflow-meter.db" "INSERT INTO events VALUES('user:19','model.tokens','claude','session-overflow','message_usage',1,0,0,9223372036854775807),('user:19','model.tokens','claude','session-overflow','message_usage',1,0,0,1);"
"$queue" account-work index --db "$tmp/meter-work.db" --subject user:19 --scope self --owner user:19 --source "operator=$tmp/operator.sqlite3" --source "fabric=$tmp/meter-fabric.db" --source "meter=$tmp/overflow-meter.db" >/dev/null 2>"$tmp/meter-warning"
grep -F 'warning: Meter unavailable; Meter token evidence omitted' "$tmp/meter-warning" >/dev/null || { echo 'account work: Meter query failure warning missing' >&2; exit 1; }
[ "$(sqlite3 "$tmp/meter-work.db" "SELECT group_concat(work_id||':'||run_id||':'||input_tokens) FROM account_work_usage")" = 'account-operator-g1:r1:300' ] || { echo 'account work: Meter failure kept partial evidence' >&2; exit 1; }
# A session also named by another indexed account is ambiguous globally.
sqlite3 "$tmp/meter-work.db" "INSERT INTO account_work(id,subject,scope,source,title,state,created_ms,updated_ms) VALUES('foreign','user:20','self','fabric','foreign','complete',1,2); INSERT INTO account_work_run(work_id,run_id,worker_id,state,created_ms,updated_ms,agent,provider_session_ref) VALUES('foreign','foreign-run','worker','completed',1,2,'claude','session-exact');"
"$queue" account-work index --db "$tmp/meter-work.db" --subject user:19 --scope self --owner user:19 --source "fabric=$tmp/meter-fabric.db" --source "meter=$tmp/meter.db" >/dev/null
[ "$(sqlite3 "$tmp/meter-work.db" 'SELECT count(*) FROM account_work_usage')" = 0 ] || { echo 'account work: cross-account session attributed' >&2; exit 1; }
echo 'account work index: PASS'
