#!/bin/sh
set -eu

web=${1:?web-address binary required}
case "$web" in /*) ;; *) web="$(pwd)/$web" ;; esac
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
cms="$root/cmd/BonfyreCMS/bonfyre-cms"
auth="$root/cmd/BonfyreAuth/bonfyre-auth"
foundry="$root/cmd/BonfyreCMS/tools/compile_foundry_behavior.mjs"
compiler="$root/cmd/BonfyreCMS/tools/compile_role_program.mjs"
[ -x "$cms" ] || { echo 'role program owner: SKIP (cms not built)'; exit 0; }
[ -x "$auth" ] || { echo 'role program owner: SKIP (auth not built)'; exit 0; }

tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-role-program.XXXXXX")
pid=''
cleanup() { [ -n "$pid" ] && kill "$pid" 2>/dev/null || true; rm -rf "$tmp"; }
trap cleanup EXIT INT TERM

schemas="$tmp/schemas"
mkdir -p "$schemas"
cp "$root/cmd/BonfyreCMS/content-types/role_program.json" "$schemas/"
cp "$root/cmd/BonfyreCMS/content-types/role_assignment.json" "$schemas/"
cmsdb="$tmp/cms.db"
catalog="$tmp/foundry.json"
roles="$tmp/roles.json"
bun "$foundry" --spec "$root/schemas/control/foundry-behavior-specs.json" --out "$catalog" >/dev/null
bun "$compiler" --spec "$root/schemas/control/role-program-specs.json" --foundry-catalog "$catalog" --out "$roles" >/dev/null
"$cms" schema migrate --db "$cmsdb" --schemas "$schemas" >/dev/null
python3 - "$cms" "$cmsdb" "$schemas" "$roles" <<'PY'
import json, subprocess, sys
cms, db, schemas, roles = sys.argv[1:]
for program in json.load(open(roles))["programs"]:
    subprocess.run([cms, "entry", "create", "role_program", json.dumps(program),
                    "--ns", "root", "--db", db, "--schemas", schemas], check=True,
                   stdout=subprocess.DEVNULL)
PY

matrix="$tmp/matrix.db"
"$web" bootstrap-owned --db "$matrix" >/dev/null
port=$(python3 -c "import socket; s=socket.socket(); s.bind(('127.0.0.1',0)); print(s.getsockname()[1]); s.close()")
BONFYRE_AUTH_BINARY="$auth" BONFYRE_CMS_DB="$cmsdb" BONFYRE_CMS_SCHEMAS="$schemas" \
  "$web" serve --db "$matrix" --auth-db "$tmp/auth.db" --port "$port" \
  --site-root "$root/site/matrix" >"$tmp/server.log" 2>&1 &
pid=$!
base="http://127.0.0.1:$port"
host='Host: aurekai.life'
attempt=0
until curl -sS -o /dev/null -H "$host" "$base/healthz"; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 80 ] || ! kill -0 "$pid" 2>/dev/null; then cat "$tmp/server.log" >&2; exit 1; fi
  sleep 0.05
done

[ "$(curl -sS -o "$tmp/anonymous" -w '%{http_code}' -X POST -H "$host" -d 'role=internet-operator&scope=community:fixture' "$base/roles/assume")" = 401 ]
curl -fsS -X POST -H "$host" -d '{"email":"roles@foundry.test","name":"Role Fixture","password":"a-long-enough-passphrase"}' "$base/signup" >/dev/null
session=$(curl -fsS -X POST -H "$host" -d '{"email":"roles@foundry.test","password":"a-long-enough-passphrase"}' "$base/login" | python3 -c 'import json,sys; print(json.load(sys.stdin)["session"]["token"])')
[ -n "$session" ]
[ "$(curl -sS -o /dev/null -w '%{http_code}' -H "$host" "$base/roles")" = 401 ]
[ "$(curl -sS -o /dev/null -w '%{http_code}' -H 'Host: other.example.test' -H "Cookie: bonfyre_session=$session" "$base/roles")" = 404 ]
[ "$(curl -sS -o /dev/null -w '%{http_code}' -X POST -H "$host" -H "Cookie: bonfyre_session=$session" "$base/roles")" = 405 ]
curl -fsS -H "$host" -H "Cookie: bonfyre_session=$session" "$base/roles" >"$tmp/roles.html"
grep -q 'data-role-directory' "$tmp/roles.html"
[ "$(grep -o 'data-role-program' "$tmp/roles.html" | wc -l | tr -d ' ')" = 3 ]
grep -q 'name="role" value="internet-operator"' "$tmp/roles.html"
grep -q 'Authority required, not granted' "$tmp/roles.html"
grep -q 'Operations not yet connected' "$tmp/roles.html"
grep -q 'No operating postures are recorded' "$tmp/roles.html"
curl -fsS -H "$host" -H "Cookie: bonfyre_session=$session" "$base/roles?q=internet" >"$tmp/roles.html"
[ "$(grep -o 'data-role-program' "$tmp/roles.html" | wc -l | tr -d ' ')" = 1 ]
grep -q 'Internet Operator' "$tmp/roles.html"
if grep -q 'Brand Operator' "$tmp/roles.html"; then echo 'role directory search leaked unrelated roles' >&2; exit 1; fi
[ "$(curl -sS -o /dev/null -w '%{http_code}' -H "$host" -H "Cookie: bonfyre_session=$session" "$base/roles?page=9999")" = 400 ]
curl -fsS -H "$host" -H "Cookie: bonfyre_session=$session" "$base/roles?scope=community%3Afixture" >"$tmp/scoped-roles.html"
grep -q 'name="scope" value="community:fixture"' "$tmp/scoped-roles.html"
grep -q 'value="community:fixture" aria-label="Role context scope"' "$tmp/scoped-roles.html"
curl -fsS -H "$host" -H "Cookie: bonfyre_session=$session" "$base/roles?q=internet&scope=community%3Afixture" >"$tmp/scoped-filtered.html"
[ "$(grep -o 'data-role-program' "$tmp/scoped-filtered.html" | wc -l | tr -d ' ')" = 1 ]
grep -q 'name="scope" value="community:fixture"' "$tmp/scoped-filtered.html"
[ "$(curl -sS -o /dev/null -w '%{http_code}' -H "$host" -H "Cookie: bonfyre_session=$session" "$base/roles?scope=community%2Fwrong")" = 400 ]
[ "$(sqlite3 "$cmsdb" "SELECT count(*) FROM role_assignment;")" = 0 ]
[ "$(curl -sS -o /dev/null -w '%{http_code}' -X POST -H "$host" -H "Cookie: bonfyre_session=$session" -d 'role=internet-operator&scope=global&return=/roles' "$base/roles/assume")" = 403 ]
[ "$(curl -sS -o /dev/null -w '%{http_code}' -X POST -H "$host" -H "Cookie: bonfyre_session=$session" -H 'Origin: null' -d 'role=internet-operator&scope=global&return=/roles' "$base/roles/assume")" = 403 ]
[ "$(curl -sS -o /dev/null -w '%{http_code}' -X POST -H "$host" -H "Cookie: bonfyre_session=$session" -H 'Origin: https://foreign.example.test' -d 'role=internet-operator&scope=global&return=/roles' "$base/roles/assume")" = 403 ]
[ "$(curl -sS -o /dev/null -w '%{http_code}' -X POST -H 'Host: aurekai.shop' -H "Cookie: bonfyre_session=$session" -H 'Origin: https://aurekai.shop' -d 'role=internet-operator&scope=global&return=/roles' "$base/roles/assume")" = 404 ]
[ "$(sqlite3 "$cmsdb" "SELECT count(*) FROM role_assignment;")" = 0 ]
code=$(curl -sS -o /dev/null -w '%{http_code}' -X POST -H "$host" -H "Cookie: bonfyre_session=$session" -H 'Origin: http://aurekai.life' \
  -d 'role=internet-operator&scope=community:fixture&return=/subjects' "$base/roles/assume")
[ "$code" = 303 ]
[ "$(sqlite3 "$cmsdb" "SELECT count(*) FROM role_assignment WHERE role_program='internet-operator' AND context_scope='community:fixture' AND state='assumed';")" = 1 ]
[ "$(sqlite3 "$cmsdb" "SELECT count(*) FROM role_assignment WHERE cannot_infer LIKE '%authority grant%';")" = 1 ]
assignment=$(sqlite3 "$cmsdb" "SELECT assignment_id FROM role_assignment WHERE role_program='internet-operator';")
[ -n "$assignment" ]
[ "$(curl -sS -o /dev/null -w '%{http_code}' -X POST -H "$host" -H "Cookie: bonfyre_session=$session" -H 'Origin: http://aurekai.life' \
  -d 'role=internet-operator&scope=community:second&return=/subjects' "$base/roles/assume")" = 303 ]
second=$(sqlite3 "$cmsdb" "SELECT assignment_id FROM role_assignment WHERE role_program='internet-operator' AND context_scope='community:second';")
[ -n "$second" ] && [ "$second" != "$assignment" ]
[ "$(sqlite3 "$cmsdb" "SELECT count(*) FROM role_assignment WHERE actor_subject='user:1' AND role_program='internet-operator';")" = 2 ]
[ "$(curl -sS -o /dev/null -w '%{http_code}' -X POST -H "$host" -H "Cookie: bonfyre_session=$session" -H 'Origin: http://aurekai.life' \
  -d 'role=internet-operator&scope=community:fixture&return=/subjects' "$base/roles/assume")" = 303 ]
[ "$(sqlite3 "$cmsdb" "SELECT count(*) FROM role_assignment WHERE actor_subject='user:1' AND role_program='internet-operator';")" = 2 ]
[ "$(curl -sS -o /dev/null -w '%{http_code}' -H "$host" "$base/roles/workspace?assignment=$assignment")" = 401 ]
workspace=$(curl -fsS -H "$host" -H "Cookie: bonfyre_session=$session" "$base/roles/workspace?assignment=$assignment")
printf '%s' "$workspace" | grep -q '"role_program":"internet-operator"'
printf '%s' "$workspace" | grep -q '"exact_operations"'
printf '%s' "$workspace" | grep -q '"required_authority"'
printf '%s' "$workspace" | grep -q 'not authority grants'
printf '%s' "$workspace" | grep -q '"context_scope":"community:fixture"'
second_workspace=$(curl -fsS -H "$host" -H "Cookie: bonfyre_session=$session" "$base/roles/workspace?assignment=$second")
printf '%s' "$second_workspace" | grep -q '"context_scope":"community:second"'
curl -fsS -H "$host" -H "Cookie: bonfyre_session=$session" "$base/roles" >"$tmp/roles.html"
grep -q 'data-role-assignment' "$tmp/roles.html"
grep -q "href=\"/roles?assignment=$assignment\"" "$tmp/roles.html"
grep -q "href=\"/roles?assignment=$second\"" "$tmp/roles.html"
curl -fsS -H "$host" -H "Cookie: bonfyre_session=$session" "$base/roles?assignment=$assignment" >"$tmp/role-detail.html"
grep -q 'data-role-posture' "$tmp/role-detail.html"
grep -q 'community:fixture' "$tmp/role-detail.html"
grep -q 'Work queues' "$tmp/role-detail.html"
grep -q 'Operations not yet bound' "$tmp/role-detail.html"
grep -q 'Authority still required' "$tmp/role-detail.html"
grep -q "href=\"/roles/workspace?assignment=$assignment\"" "$tmp/role-detail.html"
[ "$(sqlite3 "$cmsdb" "SELECT count(*) FROM role_assignment;")" = 2 ]
curl -fsS -X POST -H "$host" -d '{"email":"other-role@foundry.test","name":"Other","password":"a-long-enough-passphrase"}' "$base/signup" >/dev/null
other=$(curl -fsS -X POST -H "$host" -d '{"email":"other-role@foundry.test","password":"a-long-enough-passphrase"}' "$base/login" | python3 -c 'import json,sys; print(json.load(sys.stdin)["session"]["token"])')
[ "$(curl -sS -o /dev/null -w '%{http_code}' -H "$host" -H "Cookie: bonfyre_session=$other" "$base/roles/workspace?assignment=$assignment")" = 404 ]
[ "$(curl -sS -o /dev/null -w '%{http_code}' -H "$host" -H "Cookie: bonfyre_session=$other" "$base/roles?assignment=$assignment")" = 404 ]
curl -fsS -H "$host" -H "Cookie: bonfyre_session=$other" "$base/roles" >"$tmp/other-roles.html"
if grep -q "$assignment" "$tmp/other-roles.html"; then echo 'personal role assignments leaked to another actor' >&2; exit 1; fi
[ "$(curl -sS -o /dev/null -w '%{http_code}' -X POST -H "$host" -H "Cookie: bonfyre_session=$session" -H 'Origin: http://aurekai.life' -d 'role=not-a-role&scope=global' "$base/roles/assume")" = 404 ]

grep -q 'action="/roles/leave"' "$tmp/role-detail.html"
[ "$(curl -sS -o /dev/null -w '%{http_code}' -X POST -H "$host" -H "Cookie: bonfyre_session=$other" -H 'Origin: http://aurekai.life' -d "assignment=$assignment" "$base/roles/leave")" = 404 ]
[ "$(curl -sS -o /dev/null -w '%{http_code}' -X POST -H "$host" -H "Cookie: bonfyre_session=$session" -d "assignment=$assignment" "$base/roles/leave")" = 403 ]
[ "$(curl -sS -o /dev/null -w '%{http_code}' -X POST -H "$host" -H "Cookie: bonfyre_session=$session" -H 'Origin: https://untrusted.test' -d "assignment=$assignment" "$base/roles/leave")" = 403 ]
[ "$(curl -sS -o /dev/null -w '%{http_code}' -H "$host" -H "Cookie: bonfyre_session=$session" "$base/roles/leave")" = 405 ]
[ "$(sqlite3 "$cmsdb" "SELECT state FROM role_assignment WHERE assignment_id='$assignment'")" = assumed ]
[ "$(curl -sS -o /dev/null -w '%{http_code}' -X POST -H "$host" -H "Cookie: bonfyre_session=$session" -H 'Origin: http://aurekai.life' -d "assignment=$assignment" "$base/roles/leave")" = 303 ]
[ "$(sqlite3 "$cmsdb" "SELECT state FROM role_assignment WHERE assignment_id='$assignment'")" = ended ]
[ "$(sqlite3 "$cmsdb" "SELECT state FROM role_assignment WHERE assignment_id='$second'")" = assumed ]
[ "$(curl -sS -o /dev/null -w '%{http_code}' -H "$host" -H "Cookie: bonfyre_session=$session" "$base/roles?assignment=$assignment")" = 404 ]
[ "$(curl -sS -o /dev/null -w '%{http_code}' -H "$host" -H "Cookie: bonfyre_session=$session" "$base/roles/workspace?assignment=$assignment")" = 404 ]
[ "$(curl -sS -o /dev/null -w '%{http_code}' -X POST -H "$host" -H "Cookie: bonfyre_session=$session" -H 'Origin: http://aurekai.life' -d "assignment=$assignment" "$base/roles/leave")" = 303 ]
curl -fsS -H "$host" -H "Cookie: bonfyre_session=$session" "$base/roles" >"$tmp/roles.html"
if grep -q "href=\"/roles?assignment=$assignment\"" "$tmp/roles.html"; then echo 'ended role still appears current' >&2; exit 1; fi
grep -q "href=\"/roles?assignment=$second\"" "$tmp/roles.html"
[ "$(sqlite3 "$cmsdb" "SELECT count(*) FROM role_assignment WHERE actor_subject='user:1'")" = 2 ]
[ "$(curl -sS -o /dev/null -w '%{http_code}' -X POST -H "$host" -H "Cookie: bonfyre_session=$session" -H 'Origin: http://aurekai.life' -d 'role=internet-operator&scope=community:fixture&return=/roles' "$base/roles/assume")" = 303 ]
[ "$(sqlite3 "$cmsdb" "SELECT state FROM role_assignment WHERE assignment_id='$assignment'")" = assumed ]
BONFYRE_ROLE_BROWSER_BASE="$base" BONFYRE_ROLE_BROWSER_SESSION="$session" \
  bun "$root/cmd/BonfyreWebAddress/tests/role_directory_browser_test.mjs"
echo 'role program owner: PASS'
