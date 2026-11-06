#!/bin/sh
# Real API contract witness: real server, real sqlite auth, real HTTP statuses.
set -eu

root_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
run_dir=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-api-contract.XXXXXX")
db="$run_dir/api.db"
log="$run_dir/server.log"

port=$(python3 -c "import socket; s=socket.socket(); s.bind(('127.0.0.1',0)); print(s.getsockname()[1]); s.close()")

# The HTTP catalog must come from the exact CLI command registry, not
# a second list of verbs maintained inside BonfyreAPI.
cli="$root_dir/../BonfyreCLI/bonfyre"
if [ ! -x "$cli" ]; then
    make -C "$root_dir/../BonfyreCLI" bonfyre >/dev/null
fi
export BONFYRE_CLI_BINARY="$cli"

# Exercise migration of a legacy API-key table without an Auth-user owner.
sqlite3 "$db" "CREATE TABLE api_keys(id INTEGER PRIMARY KEY AUTOINCREMENT,key TEXT NOT NULL UNIQUE,label TEXT,scopes TEXT DEFAULT '*',active INTEGER DEFAULT 1,created_at TEXT NOT NULL);"
# Exercise upgrade of old webhook storage without inventing an owner for legacy rows.
sqlite3 "$db" "CREATE TABLE webhooks (id INTEGER PRIMARY KEY AUTOINCREMENT, url TEXT NOT NULL, events TEXT DEFAULT 'completed,failed', active INTEGER DEFAULT 1, created_at TEXT NOT NULL);"
sqlite3 "$db" "INSERT INTO webhooks(url,events,active,created_at) VALUES('https://legacy.example.org/hook','completed',1,datetime('now'));"

# Exercise the actual native Auth and CMS owners, not caller-supplied actor
# coordinates or invented PackGraph digests.
auth="$root_dir/../BonfyreAuth/bonfyre-auth"
cms="$root_dir/../BonfyreCMS/bonfyre-cms"
[ -x "$auth" ] || make -C "$root_dir/../BonfyreAuth" bonfyre-auth >/dev/null
[ -x "$cms" ] || make -C "$root_dir/../BonfyreCMS" bonfyre-cms >/dev/null
export BONFYRE_AUTH_BINARY="$auth" BONFYRE_AUTH_DB="$run_dir/auth.db"
export BONFYRE_CMS_BINARY="$cms" BONFYRE_CMS_DB="$run_dir/cms.db"
export BONFYRE_CMS_SCHEMAS="$run_dir/schemas"
"$auth" --db "$BONFYRE_AUTH_DB" signup --email api-owner@example.test --name 'API Owner' --password contract-secret >/dev/null
"$auth" --db "$BONFYRE_AUTH_DB" actor-bind --user-id 1 --actor actor:api-owner >/dev/null
"$auth" --db "$BONFYRE_AUTH_DB" institution-grant --user-id 1 --institution cooperative --role write >/dev/null
session=$("$auth" --db "$BONFYRE_AUTH_DB" login --email api-owner@example.test --password contract-secret | python3 -c 'import json,sys;print(json.load(sys.stdin)["session"]["token"])')
RUN_DIR="$run_dir" bun -e '
import {packFixture} from "../BonfyreCMS/tests/pack_lifecycle_fixture.mjs";
import {copyFileSync,cpSync} from "node:fs";
import {join} from "node:path";
const f=packFixture();
try {
  f.pack("api-scope");
  f.add("pack_demand",{pack:"api-scope",demand_kind:"source",needs:"Original fixture",body_regime:"REMOTE",status:"published"});
  const plan=f.call("plan","api-scope",{actor:"user:1"});
  if(plan.code!==0)throw Error(plan.raw+" "+plan.stderr);
  const inst=f.call("install","api-scope",{actor:"user:1",revision:0,digest:plan.digest});
  if(inst.code!==0)throw Error(inst.raw+" "+inst.stderr);
  const active=f.call("activate","api-scope",{actor:"user:1",revision:Number(inst.revision),digest:inst.pinned_digest});
  if(active.code!==0||active.state!=="active")throw Error(active.raw+" "+active.stderr);
  const orgCtx={actor:"user:1",subject:"institution:cooperative",scope:"institution"};
  const orgPlan=f.call("plan","api-scope",orgCtx);
  if(orgPlan.code!==0)throw Error(orgPlan.raw+" "+orgPlan.stderr);
  const orgInst=f.call("install","api-scope",{...orgCtx,revision:0,digest:orgPlan.digest});
  if(orgInst.code!==0)throw Error(orgInst.raw+" "+orgInst.stderr);
  const orgActive=f.call("activate","api-scope",{...orgCtx,revision:Number(orgInst.revision),digest:orgInst.pinned_digest});
  if(orgActive.code!==0||orgActive.state!=="active")throw Error(orgActive.raw+" "+orgActive.stderr);
  f.sql.exec("PRAGMA wal_checkpoint(TRUNCATE)");
  copyFileSync(f.db,join(process.env.RUN_DIR,"cms.db"));
  cpSync(f.schemas,join(process.env.RUN_DIR,"schemas"),{recursive:true});
}finally{f.close();}
' >/dev/null

"$root_dir/bonfyre-api" serve --port "$port" --db "$db" --static "$root_dir/static" </dev/null >"$log" 2>&1 &
server_pid=$!

cleanup() {
    kill "$server_pid" 2>/dev/null || true
    wait_attempt=0
    while kill -0 "$server_pid" 2>/dev/null; do
        wait_attempt=$((wait_attempt + 1))
        if [ "$wait_attempt" -ge 40 ]; then
            kill -9 "$server_pid" 2>/dev/null || true
            break
        fi
        sleep 0.05
    done
    wait "$server_pid" 2>/dev/null || true
    rm -rf "$run_dir"
}
trap cleanup EXIT

attempt=0
until curl -s -o /dev/null "http://127.0.0.1:$port/api/health"; do
    attempt=$((attempt + 1))
    if [ "$attempt" -ge 100 ] || ! kill -0 "$server_pid" 2>/dev/null; then
        echo "API contract: server did not become ready" >&2
        cat "$log" >&2
        exit 1
    fi
    sleep 0.05
done

key="contract-$$-$(date +%s)"
sqlite3 "$db" "INSERT INTO api_keys (key, label, active, created_at) VALUES ('$key', 'contract', 1, datetime('now'));"

# True native actor and pinned pack checks: Auth derives the principal,
# CMS verifies the installed/activated PackGraph closure. Neither grants work.
without_session=$(curl -s -o /dev/null -w '%{http_code}' \
    -H "Authorization: Bearer $key" \
    "http://127.0.0.1:$port/api/authority?context=self&pack=api-scope")
[ "$without_session" = 403 ] || {
    echo "API contract: API key impersonated a native ActorClosure" >&2; exit 1
}
# Legacy keys do not inherit an Auth actor merely because a session exists.
unbound_key=$(curl -s -o /dev/null -w '%{http_code}' \
    -H "Authorization: Bearer $key" -H "X-Bonfyre-Session: $session" \
    "http://127.0.0.1:$port/api/authority?context=self&pack=api-scope")
[ "$unbound_key" = 403 ] || { echo "API contract: unbound key adopted a session" >&2; exit 1; }
key_owner_column=$(sqlite3 "$db" "SELECT COUNT(*) FROM pragma_table_info('api_keys') WHERE name='auth_user_id';")
[ "$key_owner_column" = 1 ] || { echo "API contract: legacy API-key migration failed" >&2; exit 1; }
sqlite3 "$db" "UPDATE api_keys SET auth_user_id=1 WHERE key='$key';"

curl -fsS -H "Authorization: Bearer $key" -H "X-Bonfyre-Session: $session" \
    "http://127.0.0.1:$port/api/authority?context=self&pack=api-scope" > "$run_dir/authority-self.json"
curl -fsS -H "Authorization: Bearer $key" -H "X-Bonfyre-Session: $session" \
    "http://127.0.0.1:$port/api/authority?context=institution:cooperative&pack=api-scope" > "$run_dir/authority-org.json"
python3 -c 'import json,sys; rows=[json.load(open(p)) for p in sys.argv[1:]]; assert [r["destination"] for r in rows]==["user:1","institution:cooperative"]; assert all(r["actor"]=="actor:api-owner" and r["actor_and_pack_current"] is True and r["admitted"] is False and len(r["pack_digest"])==64 for r in rows)' "$run_dir/authority-self.json" "$run_dir/authority-org.json"

anonymous=$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$port/api/status")
anonymous_binary=$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$port/api/binaries/hash")
anonymous_commands=$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$port/api/commands")
health=$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$port/api/health")
[ "$anonymous" = 401 ] && [ "$anonymous_binary" = 401 ] &&
[ "$anonymous_commands" = 401 ] && [ "$health" = 200 ] || {
    echo "API contract: anonymous route escaped the authorization boundary" >&2
    exit 1
}
curl -fsS -H "Authorization: Bearer $key" "http://127.0.0.1:$port/api/commands" > "$run_dir/api-commands.json"
"$cli" list --json > "$run_dir/cli-commands.json"
cmp -s "$run_dir/api-commands.json" "$run_dir/cli-commands.json" || {
    echo "API contract: HTTP catalog drifted from the CLI's single native registry" >&2
    exit 1
}

# Filtered discovery and lab opt-in are native CLI projections, not copies.
curl -fsS -H "Authorization: Bearer $key" "http://127.0.0.1:$port/api/commands?q=hash" > "$run_dir/api-filtered.json"
"$cli" list --json hash > "$run_dir/cli-filtered.json"
cmp -s "$run_dir/api-filtered.json" "$run_dir/cli-filtered.json" || {
    echo "API contract: query catalog drifted from native CLI" >&2
    exit 1
}
lab_denied=$(curl -s -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $key" "http://127.0.0.1:$port/api/commands?all=1")
[ "$lab_denied" = 403 ] || {
    echo "API contract: lab catalog leaked without catalog:all scope" >&2
    exit 1
}
sqlite3 "$db" "UPDATE api_keys SET scopes='catalog:all' WHERE key='$key';"
curl -fsS -H "Authorization: Bearer $key" "http://127.0.0.1:$port/api/commands?all=1" > "$run_dir/api-all.json"
"$cli" list --json --all > "$run_dir/cli-all.json"
cmp -s "$run_dir/api-all.json" "$run_dir/cli-all.json" || {
    echo "API contract: lab catalog drifted from the native CLI" >&2
    exit 1
}
sqlite3 "$db" "UPDATE api_keys SET scopes='*' WHERE key='$key';"

authenticated=$(curl -s -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $key" "http://127.0.0.1:$port/api/status")
rejected=$(curl -s -o /dev/null -w '%{http_code}' -H "Authorization: Bearer invalid-key-does-not-exist" "http://127.0.0.1:$port/api/status")
# An active key with the legacy '*' scope is discovery-only: no arbitrary
# native dispatch. Every command needs its own exact scoped route.
unscoped=$(curl -s -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $key" \
    "http://127.0.0.1:$port/api/binaries/hash/--help")
[ "$unscoped" = 403 ] || {
    echo "API contract: wildcard key invoked a native binary ($unscoped)" >&2
    exit 1
}
sqlite3 "$db" "UPDATE api_keys SET scopes='command:hash:--help' WHERE key='$key';"
granted=$(curl -s -o "$run_dir/help.json" -w '%{http_code}' \
    -H "Authorization: Bearer $key" "http://127.0.0.1:$port/api/binaries/hash/--help")
[ "$granted" = 200 ] || {
    echo "API contract: explicitly scoped native read returned $granted" >&2
    exit 1
}
python3 -c 'import json,sys; value=json.load(open(sys.argv[1])); assert isinstance(value.get("data"),str)' "$run_dir/help.json"
post=$(curl -s -o /dev/null -w '%{http_code}' \
    -X POST -H "Authorization: Bearer $key" -H "Content-Type: application/json" \
    -d '{"password":"do-not-leak-to-argv"}' \
    "http://127.0.0.1:$port/api/binaries/hash/--help")
[ "$post" = 403 ] || {
    echo "API contract: proxy accepted HTTP body or effect method ($post)" >&2
    exit 1
}
sqlite3 "$db" "UPDATE api_keys SET scopes='command:outreach:send' WHERE key='$key';"
effect=$(curl -s -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $key" \
    "http://127.0.0.1:$port/api/binaries/outreach/send")
[ "$effect" = 403 ] || {
    echo "API contract: route scope incorrectly promoted an effect ($effect)" >&2
    exit 1
}

sqlite3 "$db" "UPDATE api_keys SET scopes='*' WHERE key='$key';"

# Shared WAL state must not leak other accounts' job paths or results.
other="other-contract-$$"
sqlite3 "$db" "INSERT INTO api_keys(key,label,active,created_at) VALUES('$other','other',1,datetime('now'));"
sqlite3 "$db" "INSERT INTO jobs(type,status,priority,input_path,source,api_key,created_at) VALUES('transcribe','queued',100,'mine-sentinel','api','$key',datetime('now'));"
mine=$(sqlite3 "$db" "SELECT max(id) FROM jobs;")
sqlite3 "$db" "INSERT INTO jobs(type,status,priority,input_path,source,api_key,created_at) VALUES('transcribe','queued',100,'foreign-sentinel','api','$other',datetime('now'));"
foreign=$(sqlite3 "$db" "SELECT max(id) FROM jobs;")
curl -fsS -H "Authorization: Bearer $key" "http://127.0.0.1:$port/api/jobs" > "$run_dir/jobs.json"
python3 -c 'import json,sys; x=json.load(open(sys.argv[1])); assert [r["id"] for r in x["data"]]==[int(sys.argv[2])]' "$run_dir/jobs.json" "$mine"
my_job=$(curl -s -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $key" "http://127.0.0.1:$port/api/jobs/$mine")
foreign_job=$(curl -s -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $key" "http://127.0.0.1:$port/api/jobs/$foreign")
[ "$my_job" = 200 ] && [ "$foreign_job" = 404 ] || {
    echo "API contract: shared queue leaked a foreign job ($my_job/$foreign_job)" >&2
    exit 1
}

# Quote-bearing database fields must be serialized by the one native JSON
# writer rather than interpolated into HTTP responses.
sqlite3 "$db" "UPDATE jobs SET input_path='input \"quoted\"', worker='worker \"quoted\"', error='failure \"quoted\"' WHERE id=$mine;"
curl -fsS -H "Authorization: Bearer $key" "http://127.0.0.1:$port/api/jobs/$mine" > "$run_dir/quoted-job.json"
curl -fsS -H "Authorization: Bearer $key" "http://127.0.0.1:$port/api/jobs" > "$run_dir/quoted-list.json"
python3 -c 'import json,sys; detail=json.load(open(sys.argv[1]))["data"]; rows=json.load(open(sys.argv[2]))["data"]; assert detail["input"]==chr(105)+chr(110)+chr(112)+chr(117)+chr(116)+" \"quoted\""; assert detail["error"]=="failure \"quoted\""; assert rows[0]["worker"]=="worker \"quoted\""' "$run_dir/quoted-job.json" "$run_dir/quoted-list.json"

# FTS and event streams must agree with direct job reads on ownership.
sqlite3 "$db" "UPDATE jobs SET result_json='privateprobe \"quoted\"' WHERE id=$mine;"
sqlite3 "$db" "INSERT INTO jobs_fts(rowid,type,source,result_json,error) VALUES($mine,'transcribe','api','privateprobe \"quoted\"','failure \"quoted\"');"
sqlite3 "$db" "INSERT INTO jobs_fts(rowid,type,source,result_json,error) VALUES($foreign,'transcribe','api','privateprobe','');"
curl -fsS -H "Authorization: Bearer $key" "http://127.0.0.1:$port/api/search?q=privateprobe" > "$run_dir/search.json"
python3 -c 'import json,sys; x=json.load(open(sys.argv[1])); assert [r["id"] for r in x["results"]]==[int(sys.argv[2])]' "$run_dir/search.json" "$mine"
curl -fsS -H "Authorization: Bearer $key" "http://127.0.0.1:$port/api/search?q=privateprobe" > "$run_dir/quoted-search.json"
python3 -c 'import json,sys; row=json.load(open(sys.argv[1]))["results"][0]; assert chr(34)+"quoted"+chr(34) in row["snippet"]' "$run_dir/quoted-search.json"

sqlite3 "$db" "INSERT INTO events(job_id,event,data,created_at) VALUES($mine,'queued','{}',datetime('now'));"
sqlite3 "$db" "INSERT INTO events(job_id,event,data,created_at) VALUES($foreign,'queued','{}',datetime('now'));"
curl -sN --max-time 1 -H "Authorization: Bearer $key" "http://127.0.0.1:$port/api/events?since=0" > "$run_dir/events.txt" || true
grep -Fq "\"job_id\":$mine" "$run_dir/events.txt" || {
    echo "API contract: own SSE event was not delivered" >&2
    exit 1
}
if grep -Fq "\"job_id\":$foreign" "$run_dir/events.txt"; then
    echo "API contract: foreign SSE event leaked" >&2
    exit 1
fi

# A valid API key is not blanket authority to mutate upload/queue/webhooks.
denied_upload=$(curl -s -o /dev/null -w '%{http_code}' -X POST -H "Authorization: Bearer $key" -d "opaque" "http://127.0.0.1:$port/api/upload")
denied_enqueue=$(curl -s -o /dev/null -w '%{http_code}' -X POST -H "Authorization: Bearer $key" -H "Content-Type: application/json" -d '{"type":"transcribe"}' "http://127.0.0.1:$port/api/jobs")
denied_hook=$(curl -s -o /dev/null -w '%{http_code}' -X POST -H "Authorization: Bearer $key" -d '{"url":"https://example.org"}' "http://127.0.0.1:$port/api/webhooks")
denied_hook_read=$(curl -s -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $key" "http://127.0.0.1:$port/api/webhooks")
[ "$denied_upload" = 403 ] && [ "$denied_enqueue" = 403 ] &&
[ "$denied_hook" = 403 ] && [ "$denied_hook_read" = 403 ] || {
    echo "API contract: wildcard scope escalated a mutation or webhook list" >&2
    exit 1
}
sqlite3 "$db" "UPDATE api_keys SET scopes='api:jobs:create,job:transcribe:enqueue' WHERE key='$key';"
before_enqueue=$(sqlite3 "$db" "SELECT count(*) FROM jobs;")
key_only=$(curl -s -o /dev/null -w '%{http_code}' -X POST \
    -H "Authorization: Bearer $key" -H "Content-Type: application/json" \
    -d '{"type":"transcribe","input":"fixture-only","priority":100}' \
    "http://127.0.0.1:$port/api/jobs?context=self&pack=api-scope")
[ "$key_only" = 403 ] || {
    echo "API contract: type-scoped API key bypassed native actor" >&2; exit 1
}
review_needed=$(curl -s -o "$run_dir/job-review.json" -w '%{http_code}' -X POST \
    -H "Authorization: Bearer $key" -H "X-Bonfyre-Session: $session" \
    -H "Content-Type: application/json" \
    -d '{"type":"transcribe","input":"fixture-only","priority":100}' \
    "http://127.0.0.1:$port/api/jobs?context=self&pack=api-scope")
[ "$review_needed" = 409 ] || {
    echo "API contract: unreviewed generic queue admission returned $review_needed" >&2; exit 1
}
python3 -c 'import json,sys; x=json.load(open(sys.argv[1])); assert x["state"]=="review_required" and x["admitted"] is False and x["next_owner"]=="BonfyreReview/BonfyreQueue"' "$run_dir/job-review.json"
after_enqueue=$(sqlite3 "$db" "SELECT count(*) FROM jobs;")
[ "$before_enqueue" = "$after_enqueue" ] || {
    echo "API contract: review refusal still wrote a queue job" >&2; exit 1
}
bad_type=$(curl -s -o /dev/null -w '%{http_code}' -X POST \
    -H "Authorization: Bearer $key" -H "Content-Type: application/json" \
    -d '{"type":"pay","input":"effect-not-authorized"}' "http://127.0.0.1:$port/api/jobs")
[ "$bad_type" = 403 ] || {
    echo "API contract: one job grant admitted a different type ($bad_type)" >&2
    exit 1
}
sqlite3 "$db" "UPDATE api_keys SET scopes='*' WHERE key='$key';"

# Legacy callback rows are retained but not assigned to an arbitrary account.
has_owner=$(sqlite3 "$db" "SELECT COUNT(*) FROM pragma_table_info('webhooks') WHERE name='api_key';")
legacy_owner=$(sqlite3 "$db" "SELECT COUNT(*) FROM webhooks WHERE api_key IS NULL AND url='https://legacy.example.org/hook';")
[ "$has_owner" = 1 ] && [ "$legacy_owner" = 1 ] || {
    echo "API contract: webhook owner migration did not preserve legacy state" >&2
    exit 1
}
sqlite3 "$db" "UPDATE api_keys SET scopes='api:webhooks:create,api:webhooks:read' WHERE key IN ('$key','$other');"
hook_a=$(curl -s -o /dev/null -w '%{http_code}' -X POST -H "Authorization: Bearer $key" \
    -H "Content-Type: application/json" -d '{"url":"https://example.org/hooks/owner-a"}' "http://127.0.0.1:$port/api/webhooks")
hook_b=$(curl -s -o /dev/null -w '%{http_code}' -X POST -H "Authorization: Bearer $other" \
    -H "Content-Type: application/json" -d '{"url":"https://example.org/hooks/owner-b"}' "http://127.0.0.1:$port/api/webhooks")
[ "$hook_a" = 201 ] && [ "$hook_b" = 201 ] || {
    echo "API contract: account-owned webhook registration failed" >&2
    exit 1
}
invalid_webhook=$(curl -s -o /dev/null -w '%{http_code}' -X POST -H "Authorization: Bearer $key" \
    -H "Content-Type: application/json" -d '{"url":42}' "http://127.0.0.1:$port/api/webhooks")
unsafe_webhook=$(curl -s -o /dev/null -w '%{http_code}' -X POST -H "Authorization: Bearer $key" \
    -H "Content-Type: application/json" -d '{"url":"javascript:alert(1)"}' "http://127.0.0.1:$port/api/webhooks")
[ "$invalid_webhook" = 400 ] && [ "$unsafe_webhook" = 400 ] || {
    echo "API contract: invalid webhook grammar reached storage" >&2
    exit 1
}

sqlite3 "$db" "UPDATE webhooks SET url='https://example.org/hooks/owner-a?flag=\"quoted\"' WHERE api_key='$key';"
curl -fsS -H "Authorization: Bearer $key" "http://127.0.0.1:$port/api/webhooks" > "$run_dir/hooks-a.json"
curl -fsS -H "Authorization: Bearer $other" "http://127.0.0.1:$port/api/webhooks" > "$run_dir/hooks-b.json"
python3 -c 'import json,sys; a=json.load(open(sys.argv[1])); b=json.load(open(sys.argv[2])); assert [x["url"] for x in a["data"]]==["https://example.org/hooks/owner-a?flag=\"quoted\""]; assert [x["url"] for x in b["data"]]==["https://example.org/hooks/owner-b"]' "$run_dir/hooks-a.json" "$run_dir/hooks-b.json"
sqlite3 "$db" "UPDATE api_keys SET scopes='*' WHERE key='$key';"

# An API key cannot adopt another account session, even if that session is
# live and the pack is installed. Its Auth-user binding is independent.
cross_key=$(curl -s -o /dev/null -w '%{http_code}' \
    -H "Authorization: Bearer $other" -H "X-Bonfyre-Session: $session" \
    "http://127.0.0.1:$port/api/authority?context=self&pack=api-scope")
[ "$cross_key" = 403 ] || { echo "API contract: cross-key session composition escaped" >&2; exit 1; }

# Auth changes invalidate the next request: scope or stale readback is no lease.
"$auth" --db "$BONFYRE_AUTH_DB" institution-revoke --user-id 1 --institution cooperative >/dev/null
revoked_org=$(curl -s -o /dev/null -w '%{http_code}' \
    -H "Authorization: Bearer $key" -H "X-Bonfyre-Session: $session" \
    "http://127.0.0.1:$port/api/authority?context=institution:cooperative&pack=api-scope")
[ "$revoked_org" = 403 ] || { echo "API contract: revoked institutional grant remained active" >&2; exit 1; }
curl -fsS -H "Authorization: Bearer $key" -H "X-Bonfyre-Session: $session" \
    "http://127.0.0.1:$port/api/authority?context=self&pack=api-scope" >/dev/null

# Altering a published pack input invalidates the pinned active installation;
# changing it back must recover the exact original native digest.
sqlite3 "$BONFYRE_CMS_DB" "UPDATE pack_demand SET needs='Changed fixture' WHERE pack='api-scope';"
stale_pack=$(curl -s -o /dev/null -w '%{http_code}' \
    -H "Authorization: Bearer $key" -H "X-Bonfyre-Session: $session" \
    "http://127.0.0.1:$port/api/authority?context=self&pack=api-scope")
[ "$stale_pack" = 403 ] || { echo "API contract: stale PackGraph digest was accepted" >&2; exit 1; }
sqlite3 "$BONFYRE_CMS_DB" "UPDATE pack_demand SET needs='Original fixture' WHERE pack='api-scope';"
curl -fsS -H "Authorization: Bearer $key" -H "X-Bonfyre-Session: $session" \
    "http://127.0.0.1:$port/api/authority?context=self&pack=api-scope" >/dev/null

"$auth" --db "$BONFYRE_AUTH_DB" actor-revoke --user-id 1 >/dev/null
revoked_actor=$(curl -s -o /dev/null -w '%{http_code}' \
    -H "Authorization: Bearer $key" -H "X-Bonfyre-Session: $session" \
    "http://127.0.0.1:$port/api/authority?context=self&pack=api-scope")
[ "$revoked_actor" = 403 ] || { echo "API contract: revoked ActorClosure remained active" >&2; exit 1; }
"$auth" --db "$BONFYRE_AUTH_DB" actor-bind --user-id 1 --actor actor:api-owner >/dev/null
curl -fsS -H "Authorization: Bearer $key" -H "X-Bonfyre-Session: $session" \
    "http://127.0.0.1:$port/api/authority?context=self&pack=api-scope" >/dev/null
"$auth" --db "$BONFYRE_AUTH_DB" sessions-revoke --user-id 1 >/dev/null
revoked_session=$(curl -s -o /dev/null -w '%{http_code}' \
    -H "Authorization: Bearer $key" -H "X-Bonfyre-Session: $session" \
    "http://127.0.0.1:$port/api/authority?context=self&pack=api-scope")
[ "$revoked_session" = 403 ] || { echo "API contract: revoked session remained active" >&2; exit 1; }

static_title=$(curl -s "http://127.0.0.1:$port/" | grep -c '<title>Bonfyre')
logged=$(sqlite3 "$db" "SELECT COUNT(*) FROM request_log WHERE api_key='$key';")

[ "$authenticated" = 200 ] || {
    echo "API contract: valid bearer returned $authenticated, expected 200" >&2
    exit 1
}
[ "$rejected" = 401 ] || {
    echo "API contract: invalid bearer returned $rejected, expected 401" >&2
    exit 1
}
[ "$static_title" -ge 1 ] || {
    echo "API contract: static surface did not render" >&2
    exit 1
}
[ "$logged" -ge 1 ] || {
    echo "API contract: authenticated request was not recorded" >&2
    exit 1
}

echo "api http contract: PASS"
exit 0
