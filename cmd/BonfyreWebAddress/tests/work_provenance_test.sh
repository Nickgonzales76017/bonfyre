#!/bin/sh
# Account work provenance over real owners: a seeded operator store and Fabric
# store, the real BonfyreQueue account work index built from them, real Meter
# token evidence joined by exact provider session, and real Auth sessions.
# Checks the task view under /work-board?task= and /work-board/outputs for the
# owner, a stranger and a signed-out reader. Nothing here reads ~/.bonfyre.
#
#   sh tests/work_provenance_test.sh ./bonfyre-web-address
#
# BONFYRE_TEST_BROWSER_HOOK, when set, is run while the server is up with
# BASE COOKIE OWNER_TASK FAILED_TASK OPERATOR_TASK as arguments (screenshots).
set -eu
bin=${1:?bonfyre-web-address binary required}
case "$bin" in /*) ;; *) bin="$(pwd)/$bin" ;; esac
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
auth="$root/cmd/BonfyreAuth/bonfyre-auth"
cms="$root/cmd/BonfyreCMS/bonfyre-cms"
lex="$root/cmd/BonfyreLex/bonfyre-lex"
queue=${BONFYRE_TEST_QUEUE:-"$root/cmd/BonfyreQueue/bonfyre-queue"}
meter=${BONFYRE_TEST_METER:-"$root/cmd/BonfyreMeter/bonfyre-meter"}
for b in "$auth" "$cms" "$lex" "$queue" "$meter"; do
  [ -x "$b" ] || { echo "work provenance: SKIP ($(basename "$b") not built)"; exit 0; }
done
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-work-provenance.XXXXXX")
pid=''
cleanup() { [ -n "$pid" ] && kill "$pid" 2>/dev/null || true; rm -rf "$tmp"; }
trap cleanup EXIT INT TERM
fail() { echo "work provenance: $1" >&2; [ -f "$tmp/server.log" ] && tail -5 "$tmp/server.log" >&2; exit 1; }
H='Host: aurekai.life'
mkdir -p "$tmp/state" "$tmp/fabric/receipts" "$tmp/elsewhere"

# ---- owner stores -------------------------------------------------------
now=$(date +%s)
sqlite3 "$tmp/operator.sqlite3" "CREATE TABLE goals(id TEXT PRIMARY KEY,title TEXT,state TEXT,created_at REAL,updated_at REAL,last_error TEXT NOT NULL DEFAULT '');
CREATE TABLE runs(id TEXT,goal_id TEXT,state TEXT,summary TEXT NOT NULL DEFAULT '',started_at REAL,completed_at REAL,usage_json TEXT);
CREATE TABLE actions(seq INTEGER PRIMARY KEY,run_id TEXT,tool TEXT,state TEXT,started_at REAL,duration_ms INTEGER);
INSERT INTO goals VALUES('g-index','Rebuild the permit search index','blocked',$now-7200,$now-3600,'Index writer refused the old schema');
INSERT INTO runs VALUES('r-index','g-index','failed','RateLimitError: insufficient_quota',$now-3700,$now-3640,json_object('input_tokens',300,'output_tokens',30));
INSERT INTO actions VALUES(1,'r-index','project_search','passed',$now-3700,150);
INSERT INTO actions VALUES(2,'r-index','index_write','failed',$now-3690,900);
INSERT INTO actions VALUES(3,'r-index','public_surface_rollout','reverted',$now-3680,20);"

cat >"$tmp/codex.jsonl" <<'EOF'
{"type":"turn.completed","usage":{"input_tokens":1200,"cached_input_tokens":800,"output_tokens":140,"reasoning_output_tokens":40}}
EOF
cat >"$tmp/claude.jsonl" <<'EOF'
{"type":"result","total_cost_usd":0.42,"usage":{"input_tokens":90,"cache_read_input_tokens":3000,"cache_creation_input_tokens":200,"output_tokens":75}}
EOF
printf '{"review":"two findings, both fixed"}\n' >"$tmp/fabric/receipts/review.json"
printf '{"review":"outside the owner"}\n' >"$tmp/elsewhere/outside.json"
good=$(shasum -a 256 "$tmp/fabric/receipts/review.json" | cut -d' ' -f1)
outside=$(shasum -a 256 "$tmp/elsewhere/outside.json" | cut -d' ' -f1)
sqlite3 "$tmp/fabric/fabric.sqlite3" "CREATE TABLE tasks(id TEXT,title TEXT,state TEXT,kind TEXT,created_at REAL,started_at REAL,completed_at REAL);
CREATE TABLE runs(id TEXT,task_id TEXT,agent TEXT,role TEXT,state TEXT,started_at REAL,completed_at REAL,stdout_path TEXT,session_id TEXT,duration_ms INTEGER,result_json TEXT);
CREATE TABLE receipts(id TEXT,task_id TEXT,kind TEXT,path TEXT,digest TEXT,payload_json TEXT,created_at REAL);
INSERT INTO tasks VALUES('f-review','Cross-review the Dallas zoning summary','completed','review',$now-5400,$now-5400,$now-5000);
INSERT INTO tasks VALUES('f-duel','Draft the weekly permit digest','failed','duel',$now-1800,$now-1800,$now-1500);
INSERT INTO runs VALUES('c1','f-review','codex','worker','passed',$now-5400,$now-5148,'$tmp/codex.jsonl','thread-codex-0001',252000,NULL);
INSERT INTO runs VALUES('c2','f-review','claude','reviewer','passed',$now-5140,$now-5079,'$tmp/claude.jsonl','sess-claude-0001',61000,NULL);
INSERT INTO runs VALUES('d1','f-duel','claude','worker','failed',$now-1800,$now-1770,NULL,'sess-claude-0002',30000,json_object('final','You have hit your usage limit.'));
INSERT INTO runs VALUES('d2','f-duel','codex','worker','failed',$now-1800,$now-1500,NULL,'',NULL,json_object('final','Tests failed.'));
INSERT INTO receipts VALUES('rv','f-review','cross-review','$tmp/fabric/receipts/review.json','$good','{}',$now-5000);
INSERT INTO receipts VALUES('ro','f-review','relay','$tmp/elsewhere/outside.json','$outside','{}',$now-5000);"

# ---- auth + server ------------------------------------------------------
"$bin" bootstrap-owned --db "$tmp/matrix.db" >/dev/null
"$cms" schema migrate --db "$tmp/cms.db" --schemas "$root/cmd/BonfyreCMS/content-types" >/dev/null 2>&1
port=$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1])')
base="http://127.0.0.1:$port"
serve() {
  env BONFYRE_AUTH_BINARY="$auth" BONFYRE_CMS_DB="$tmp/cms.db" BONFYRE_BIN_DIR="$(dirname "$queue")" \
    BONFYRE_ACCOUNT_WORK_DB="$tmp/state/account-work.db" BONFYRE_ACCOUNT_WORK_SOURCES="operator=$tmp/operator.sqlite3;fabric=$tmp/fabric/fabric.sqlite3" \
    BONFYRE_WORKSPACE_CLIENT="$root/integrations/browser" "$@" \
    "$bin" serve --db "$tmp/matrix.db" --auth-db "$tmp/auth.db" --port "$port" --site-root "$root/site/matrix" >"$tmp/server.log" 2>&1 &
  pid=$!
  for _ in $(seq 1 100); do curl -fsS -o /dev/null -H "$H" "$base/healthz" 2>/dev/null && return 0; sleep 0.05; done
  fail "server did not start"
}
stop() { kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; pid=''; }
serve BONFYRE_METER_DB="$tmp/meter.db" BONFYRE_METER_BINARY="$meter"
pw='work-provenance-passphrase'
session() {
  curl -fsS -X POST -H "$H" -d "{\"email\":\"$1\",\"name\":\"$2\",\"password\":\"$pw\"}" "$base/signup" >/dev/null
  curl -fsS -X POST -H "$H" -d "{\"email\":\"$1\",\"password\":\"$pw\"}" "$base/login" | "$lex" field --kind json_string --key session.token
}
owner=$(session owner@provenance.test Owner)
stranger=$(session stranger@provenance.test Stranger)
[ -n "$owner" ] && [ -n "$stranger" ] || fail "no sessions"
uid=$(sqlite3 "$tmp/auth.db" "select id from users where email='owner@provenance.test'")
sid=$(sqlite3 "$tmp/auth.db" "select id from users where email='stranger@provenance.test'")

# The real index, built only from the owner stores, under the owner's account.
"$queue" account-work index --db "$tmp/state/account-work.db" --subject "user:$uid" --scope self --owner "user:$uid" \
  --source "operator=$tmp/operator.sqlite3" --source "fabric=$tmp/fabric/fabric.sqlite3" >/dev/null || fail "index build refused"

# Meter evidence: Claude and Codex rows keyed by the sessions the runs name.
Hd='provider,session_id,execution_id,run_id,message_id,request_id,event_uuid,parent_uuid,agent_id,timestamp,model,effort,speed,uncached_input_tokens,cached_input_tokens,cache_write_input_tokens,input_tokens_including_cache,output_tokens,reasoning_output_tokens,total_tokens,source_file,accounting_note'
D='deduplicated_message_id_and_request_id;no_separate_reasoning_count'
cat >"$tmp/usage.csv" <<EOF
$Hd
claude,sess-claude-0001,ex1,,m1,q1,u1,,,2026-10-29T10:00:00.000Z,claude-opus-5,xhigh,standard,90,3000,200,3290,75,,3365,private/transcript.jsonl,$D
codex,thread-codex-0001,e1,e1,,,,,,2026-10-29T09:00:00.000Z,gpt-5.4,high,,400,800,0,1200,140,40,1340,private/rollout.jsonl,cumulative_counter_delta
claude,sess-claude-0002,ex2,,m2,q2,u2,,,2026-10-29T11:00:00.000Z,claude-opus-5,xhigh,standard,40,1000,0,1040,12,,1052,private/transcript-2.jsonl,$D
EOF
"$meter" token-import --db "$tmp/meter.db" --subject "user:$uid" --source "$tmp/usage.csv" >/dev/null || fail "meter import refused"
for pair in uncached_input:15 cached_input:1.5 cache_write:18.75 output:75; do
  "$meter" admit-rate --db "$tmp/meter.db" --provider claude --op "claude-opus-5:${pair%%:*}" --unit megatoken --amount "${pair#*:}" \
    --effective-at 2026-10-01T00:00:00Z --evidence receipt:fixture-rate-card >/dev/null 2>&1
done

page() { curl -sS -o "$tmp/$1.html" -w '%{http_code}' -H "$H" ${3:+-H "Cookie: bonfyre_session=$3"} "$base$2"; }
has() { grep -q -F -- "$2" "$tmp/$1.html" || fail "$1 lacks: $2"; }
lacks() { if grep -q -F -- "$2" "$tmp/$1.html"; then fail "$1 shows: $2"; fi; }
review=account-fabric-f-review; duel=account-fabric-f-duel; goal=account-operator-g-index

# ---- signed out ---------------------------------------------------------
[ "$(page out "/work-board?context=self&task=$review")" = 303 ] || fail "signed-out task view did not ask for sign-in"
[ "$(page outs "/work-board/outputs?context=self")" = 303 ] || fail "signed-out outputs did not ask for sign-in"

# ---- a finished task: runs, usage with cache, kept output, meter --------
[ "$(page review "/work-board?context=self&task=$review" "$owner")" = 200 ] || fail "owner task view"
for s in 'data-work-provenance' 'Cross-review the Dallas zoning summary' 'data-task-state="complete"' 'data-sum="runs">2<' 'data-sum="failed-runs">0<' \
         'data-sum="outputs">1<' '<strong>Codex</strong>' '<strong>Claude</strong>' 'Reviewer' 'Review session' 'Session recorded for token matching' \
         '1,200 in' '800 read from cache' '3,000 read from cache' '200 written to cache' 'about $0.42 estimated' '4 min 12 s' '1 min 1 s' \
         "data-digest=\"$good\"" 'Cross review' 'data-meter="read"' '2 of 2 sessions on this work have token records.' 'data-meter-models' 'claude-opus-5' 'gpt-5.4' \
         '3,000' 'data-valuation="partial"' 'not a charge' 'Nothing here is a bill.' 'data-next="finished"' 'href="/work-board/outputs?context=self"' \
         'href="/s/models/all?context=self"'; do has review "$s"; done
lacks review "$outside"; lacks review "$tmp/"; lacks review 'private/transcript'; lacks review 'thread-codex-0001'; lacks review 'sess-claude-0001'
[ "$(grep -o 'data-session>' "$tmp/review.html" | wc -l | tr -d ' ')" = 2 ] || fail "session keys not both noted"
# ---- a failed task: limit, failed run that used tokens, no output -------
[ "$(page duel "/work-board?context=self&task=$duel" "$owner")" = 200 ] || fail "owner failed task view"
for s in 'data-task-state="dead_letter"' 'data-sum="failed-runs" data-tone="failed">2<' 'data-limit' "Stopped at the provider's usage limit" \
         'data-usage="unreported"' 'Tokens not reported' 'data-outputs="none"' '1 of 1 session on this work has token records.' \
         '1 run started without a session key' '1 failed run still used tokens.' 'data-next="stopped"' 'href="/inbox?context=self"'; do has duel "$s"; done
[ "$(grep -c 'data-limit' "$tmp/duel.html")" = 1 ] || fail "limit flag raised on the wrong run"
lacks duel 'sess-claude-0002'
# ---- an assistant goal: failed step listed first ------------------------
[ "$(page goal "/work-board?context=self&task=$goal" "$owner")" = 200 ] || fail "owner goal view"
for s in 'How this work was done' 'data-sum="verified">1<' 'data-sum="failed-steps" data-tone="failed">1<' 'Index write' 'data-step="failed"' \
         'data-step="verified"' 'Project search' 'undone' 'data-limit' '300 in' 'data-meter="read"' 'No run on this work names a session the meter can match.'; do has goal "$s"; done
first=$(grep -o 'data-step="[a-z]*"' "$tmp/goal.html" | head -1)
[ "$first" = 'data-step="failed"' ] || fail "failed step is not listed first ($first)"
# ---- account outputs ----------------------------------------------------
[ "$(page outputs "/work-board/outputs?context=self" "$owner")" = 200 ] || fail "owner outputs"
for s in 'data-work-outputs' "href=\"/work-board?context=self&amp;task=$review#provenance\"" 'Cross-review the Dallas zoning summary' "data-digest=\"$good\""; do has outputs "$s"; done
[ "$(grep -o 'class="wp-out"' "$tmp/outputs.html" | wc -l | tr -d ' ')" = 1 ] || fail "outputs listed an unverified receipt"
lacks outputs "$outside"; lacks outputs 'Draft the weekly permit digest'
[ "$(page board "/work-board?context=self" "$owner")" = 200 ] || fail "board"
has board 'href="/work-board/outputs?context=self"'; lacks board 'data-work-provenance'
# ---- another account sees none of it ------------------------------------
[ "$(page other "/work-board?context=self&task=$review" "$stranger")" = 200 ] || fail "stranger board"
lacks other 'data-work-provenance'; lacks other 'Cross-review the Dallas zoning summary'; lacks other "$good"
[ "$(page otherouts "/work-board/outputs?context=self" "$stranger")" = 200 ] || fail "stranger outputs"
has otherouts 'data-outputs="none"'; lacks otherouts "$good"
[ "$(page foreign "/work-board/outputs?context=institution%3Anot-mine" "$owner")" = 403 ] || fail "foreign destination admitted"
[ "$(page junk "/work-board/outputs?context=self&task=$review" "$owner")" = 400 ] || fail "outputs accepted a task"
[ "$(curl -sS -o /dev/null -w '%{http_code}' -X POST -H "$H" -H "Cookie: bonfyre_session=$owner" "$base/work-board/outputs?context=self")" = 405 ] || fail "outputs accepted POST"
# Meter refuses a task outside the reader's account even when asked directly.
if "$meter" work-tokens --db "$tmp/meter.db" --work-db "$tmp/state/account-work.db" --subject "user:$sid" --scope self --mission "$review" >/dev/null 2>&1; then fail "meter read across accounts"; fi

if [ -n "${BONFYRE_TEST_BROWSER_HOOK:-}" ]; then
  $BONFYRE_TEST_BROWSER_HOOK "$base" "bonfyre_session=$owner" "$review" "$duel" "$goal" || fail "browser hook failed"
fi

# ---- without Meter or without the index: unavailable, never zero --------
stop; serve
[ "$(page nometer "/work-board?context=self&task=$review" "$owner")" = 200 ] || fail "no-meter view"
has nometer 'data-meter="unavailable"'; lacks nometer 'data-meter-models'; has nometer '1,200 in'
stop; mv "$tmp/state/account-work.db" "$tmp/state/moved.db"; serve
[ "$(page noindex "/work-board/outputs?context=self" "$owner")" = 200 ] || fail "no-index outputs"
has noindex 'data-outputs="unavailable"'; lacks noindex 'data-outputs="none"'
# A connected but invalid Meter is unavailable, not an empty evidence store.
stop; mv "$tmp/state/moved.db" "$tmp/state/account-work.db"; sqlite3 "$tmp/broken-meter.db" 'CREATE TABLE unrelated(id INTEGER);'; serve BONFYRE_METER_DB="$tmp/broken-meter.db" BONFYRE_METER_BINARY="$meter"
[ "$(page badmeter "/work-board?context=self&task=$review" "$owner")" = 200 ] || fail "bad-meter view"
has badmeter 'data-meter="unavailable"'; has badmeter 'Token records could not be read'; lacks badmeter 'data-meter="empty"'
echo "work provenance: PASS"
