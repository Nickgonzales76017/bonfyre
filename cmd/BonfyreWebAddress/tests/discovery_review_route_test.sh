#!/bin/sh
# Actual CMS discovery review through controlled WebAddress (no external calls).
set -eu
web=$1
case "$web" in /*) ;; *) web="$(pwd)/$web";; esac
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
cms="$root/cmd/BonfyreCMS/bonfyre-cms"
auth="$root/cmd/BonfyreAuth/bonfyre-auth"
[ -x "$cms" ] && [ -x "$auth" ] || { echo "native discovery review: SKIP (missing CMS/Auth)"; exit 0; }
command -v sqlite3 >/dev/null || { echo "native discovery review: SKIP (missing sqlite3)"; exit 0; }
tmp=$(mktemp -d)
pid=''
trap '[ -n "$pid" ] && kill "$pid" 2>/dev/null || :; rm -rf "$tmp"' EXIT HUP INT TERM
schemas="$root/cmd/BonfyreCMS/content-types"
cmsdb="$tmp/cms.db"; authdb="$tmp/auth.db"; matrix="$tmp/matrix.db"
BONFYRE_FACT_DELTAS='' "$cms" schema migrate --db "$cmsdb" --schemas "$schemas" >/dev/null 2>&1
create() {
  BONFYRE_FACT_DELTAS='' "$cms" entry create "$1" "$2" --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null 2>&1
}
create discovery_set '{"set_id":"review-sample","label":"Native calendar research","criteria":"host_suffix=.edu;publishes_plane=calendar","monitoring":"manual","opened_by":"operator","opened_at":"2026-10-14T20:00:00Z","cannot_infer":"A proposal does not read or approve publishers","status":"published"}'
frontier="$tmp/frontier.db"; archive="$tmp/archive.db"
sqlite3 "$frontier" "CREATE TABLE root(root_id TEXT PRIMARY KEY,host TEXT); CREATE TABLE channel(root_id TEXT,state TEXT,refusal TEXT);"
sqlite3 "$archive" "CREATE TABLE item(source TEXT,key TEXT);"
for host in calendar.a.edu calendar.b.edu; do
 case "$host" in calendar.a.edu) source=a-calendar;; *) source=b-calendar;; esac
 create discovery_candidate "{\"candidate_id\":\"cand-$host\",\"set_id\":\"review-sample\",\"root_host\":\"$host\",\"display_name\":\"Published calendar candidate\",\"verdict\":\"pending\",\"evaluated_at\":\"2026-10-14T20:00:00Z\",\"cannot_infer\":\"Only a named human may review\",\"status\":\"published\"}"
 create internet_source "{\"family\":\"calendar\",\"source\":\"$source\",\"label\":\"$source registered publisher\",\"says\":\"Observed event listings still require review\",\"url\":\"https://$host/calendar/\",\"state\":\"declared\",\"status\":\"published\"}"
 sqlite3 "$frontier" "INSERT INTO root VALUES('$host','$host'); INSERT INTO channel VALUES('$host','admitted','');"
 sqlite3 "$archive" "INSERT INTO item VALUES('$source','one-retained-occurrence');"
done
"$web" bootstrap-owned --db "$matrix" >/dev/null
port=$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()')
BONFYRE_FACT_DELTAS='' BONFYRE_AUTH_BINARY="$auth" BONFYRE_CMS_BINARY="$cms" BONFYRE_CMS_DB="$cmsdb" BONFYRE_CMS_SCHEMAS="$schemas" BONFYRE_SOURCE_FRONTIER_DB="$frontier" BONFYRE_ITEM_ARCHIVE_DB="$archive" BONFYRE_ECONOMY_BINARY="$root/cmd/BonfyreEconomy/bonfyre-economy" BONFYRE_ECONOMY_DB="$tmp/economy.db" BONFYRE_REVIEW_COMPILER="$root/cmd/BonfyreExperienceConvergence/tools/reviewed_source_frontier.mjs" "$web" serve --db "$matrix" --auth-db "$authdb" --port "$port" --site-root "$root/site/matrix" >"$tmp/web.log" 2>&1 &
pid=$!
base="http://127.0.0.1:$port"
H="Host: aurekai.work"
n=0
until curl -fsS --max-time 2 -H "$H" "$base/healthz" >/dev/null 2>&1; do
 n=$((n+1)); [ "$n" -lt 80 ] && kill -0 "$pid" 2>/dev/null || { tail -50 "$tmp/web.log"; exit 1; }
 sleep 0.05
done
"$auth" --db "$authdb" signup --email review@fixture.test --name Reviewer --password a-long-enough-passphrase --tier pro >/dev/null
token=$("$auth" --db "$authdb" login --email review@fixture.test --password a-long-enough-passphrase | python3 -c 'import json,sys;d=json.load(sys.stdin);print(d.get("session",{}).get("token") or d.get("token",""))')
[ -n "$token" ]
uid=$(sqlite3 "$authdb" "SELECT id FROM users WHERE email='review@fixture.test';")
[ -n "$uid" ]
page=$(curl -fsS -H "$H" -H "Authorization: Bearer $token" -H 'Accept: text/html' "$base/s/set/review-sample")
printf '%s' "$page" | grep -q 'action="/discovery/decide"' || { echo "no owner-backed review form"; exit 1; }
printf '%s' "$page" | grep -q 'Reject this publisher'
fields=$(sqlite3 "$cmsdb" "SELECT updated_at||'|'||evaluated_at FROM discovery_candidate WHERE root_host='calendar.a.edu';")
updated=$(printf '%s' "$fields" | cut -d '|' -f1)
evaluated=$(printf '%s' "$fields" | cut -d '|' -f2)
request() {
 curl -sS -o "$tmp/response" -w '%{http_code}' -X POST -H "$H" -H "Authorization: Bearer $token" -H 'Origin: https://aurekai.work' \
 --data-urlencode 'set=review-sample' --data-urlencode "publisher=$1" \
 --data-urlencode "updated=$2" --data-urlencode "evaluated=$3" \
 --data-urlencode "reason=$4" --data-urlencode "verdict=$5" "$base/discovery/decide"
}
# Refuse anonymous and originless requests rather than trusting a browser checkbox.
code=$(curl -sS -o /dev/null -w '%{http_code}' -X POST -H "$H" -H 'Origin: https://aurekai.work' -d 'set=review-sample' "$base/discovery/decide")
[ "$code" = 403 ]
code=$(curl -sS -o /dev/null -w '%{http_code}' -X POST -H "$H" -H "Authorization: Bearer $token" -d 'set=review-sample' "$base/discovery/decide")
[ "$code" = 403 ]
[ "$(request calendar.a.edu "$updated" "$evaluated" 'Reviewer selected this calendar for follow-up research' accepted)" = 303 ] || {
 cat "$tmp/response"; tail -60 "$tmp/web.log"; exit 1;
}
row=$(sqlite3 "$cmsdb" "SELECT verdict||'|'||decided_by||'|'||(reason<>'') FROM discovery_candidate WHERE root_host='calendar.a.edu';")
[ "$row" = "accepted|$uid|1" ] || { echo "native CMS reviewer readback mismatch: $row"; exit 1; }
[ "$(request calendar.a.edu "$updated" "$evaluated" 'Stale override attempts are forbidden' rejected)" = 409 ]
[ "$(sqlite3 "$cmsdb" "SELECT verdict FROM discovery_candidate WHERE root_host='calendar.a.edu';")" = accepted ]
[ "$(request calendar.b.edu invalid "$evaluated" 'Stale source candidate must not be accepted' rejected)" = 409 ]
[ "$(sqlite3 "$cmsdb" "SELECT verdict FROM discovery_candidate WHERE root_host='calendar.b.edu';")" = pending ]
after=$(curl -fsS -H "$H" -H "Authorization: Bearer $token" -H 'Accept: text/html' "$base/s/set/review-sample")
printf '%s' "$after" | grep -q 'accepted'
[ "$(sqlite3 "$cmsdb" "SELECT count(*) FROM discovery_candidate WHERE verdict='accepted';")" = 1 ]
# The next human page is compiled directly from the same accepted CMS source set.
# It cannot read a static browser-owned source list or accept a rejected publisher.
brief=$(curl -fsS -H "$H" -H "Authorization: Bearer $token" \
 "$base/discovery/brief?set=review-sample&domain=itad")
printf '%s' "$brief" | grep -q 'a-calendar'
if printf '%s' "$brief" | grep -q 'b-calendar'; then echo "pending source leaked into human brief" >&2; exit 1; fi
# Reviewer rejects B, then the recipient page and YAFF still exclude its identity.
other=$(sqlite3 "$cmsdb" "SELECT updated_at||'|'||evaluated_at FROM discovery_candidate WHERE root_host='calendar.b.edu';")
bupdated=$(printf '%s' "$other" | cut -d '|' -f1)
bevaluated=$(printf '%s' "$other" | cut -d '|' -f2)
[ "$(request calendar.b.edu "$bupdated" "$bevaluated" 'Reject this publisher for the institution research scope' rejected)" = 303 ]
brief=$(curl -fsS -H "$H" -H "Authorization: Bearer $token" \
 "$base/discovery/brief?set=review-sample&domain=itad")
printf '%s' "$brief" | grep -q '1 rejected/withheld'
if printf '%s' "$brief" | grep -q 'b.edu'; then echo "rejected publisher identity leaked" >&2; exit 1; fi
md=$(curl -fsS -H "$H" -H "Authorization: Bearer $token" \
 "$base/discovery/brief?set=review-sample&domain=itad&format=md")
printf '%s' "$md" | grep -q '^# Source-reviewed research handoff'
printf '%s' "$md" | grep -q 'a-calendar'
if printf '%s' "$md" | grep -q 'b.edu'; then echo "rejected publisher in recipient Markdown" >&2; exit 1; fi
yaff=$(curl -fsS -H "$H" -H "Authorization: Bearer $token" \
 "$base/discovery/brief?set=review-sample&domain=itad&format=yaff")
printf '%s' "$yaff" | grep -q 'admitted_material "a-calendar"'
printf '%s' "$yaff" | grep -q 'rejected_count 1'
if printf '%s' "$yaff" | grep -q 'b.edu'; then echo "rejected publisher in typed handoff" >&2; exit 1; fi
[ "$(curl -sS -o /dev/null -w '%{http_code}' -H "$H" "$base/discovery/brief?set=review-sample")" = 403 ]
# Source selections, review and admitted material are owner-reread on every POST.
civic=$(curl -fsS -H "$H" -H "Authorization: Bearer $token" \
 "$base/discovery/brief?set=review-sample&domain=civic")
printf '%s' "$civic" | grep -q 'action="/discovery/verification"'
civic_yaff=$(curl -fsS -H "$H" -H "Authorization: Bearer $token" \
 "$base/discovery/brief?set=review-sample&domain=civic&format=yaff")
generation=$(printf '%s\n' "$civic_yaff" | sed -n 's/^  generation sha256:\([a-f0-9]*\)$/\1/p')
[ "$(printf '%s' "$generation" | wc -c | tr -d ' ')" = 64 ]
intake_request() {
 curl -sS -o "$tmp/intake.response" -w '%{http_code}' -X POST -H "$H" \
  -H "Authorization: Bearer $token" -H 'Origin: https://aurekai.work' \
  --data-urlencode 'set=review-sample' --data-urlencode "source=$1" \
  --data-urlencode 'domain=civic' --data-urlencode 'recipient=Civic Reviewer' \
  --data-urlencode "budget-cents=$3" --data-urlencode "generation=$2" \
  "$base/discovery/verification"
}
[ "$(intake_request a-calendar "$generation" 30000)" = 200 ] || {
 cat "$tmp/intake.response" >&2;tail -70 "$tmp/web.log" >&2;exit 1;
}
grep -q 'Actual source' "$tmp/intake.response"
grep -q '3 proof obligations' "$tmp/intake.response"
grep -q 'CMS:internet_source:a-calendar' "$tmp/intake.response"
! grep -q 'b-calendar' "$tmp/intake.response"
[ "$(sqlite3 "$tmp/economy.db" 'SELECT count(*) FROM verification_cases')" = 1 ]
[ "$(sqlite3 "$tmp/economy.db" 'SELECT count(*) FROM verification_needs')" = 3 ]
[ "$(sqlite3 "$tmp/economy.db" 'SELECT count(*) FROM verification_offers')" = 0 ]
[ "$(intake_request a-calendar "$generation" 30000)" = 200 ]
grep -q 'Identical native replay' "$tmp/intake.response"
[ "$(intake_request a-calendar "$generation" 40000)" = 409 ]
[ "$(intake_request b-calendar "$generation" 30000)" = 409 ]
[ "$(intake_request c-calendar "$generation" 30000)" = 409 ]
[ "$(intake_request a-calendar 0000000000000000000000000000000000000000000000000000000000000000 30000)" = 409 ]
[ "$(sqlite3 "$tmp/economy.db" 'SELECT count(*) FROM verification_cases')" = 1 ]
sqlite3 "$cmsdb" "UPDATE internet_source SET family='supplier' WHERE source='a-calendar';"
[ "$(intake_request a-calendar "$generation" 30000)" = 409 ]
[ "$(sqlite3 "$tmp/economy.db" 'SELECT count(*) FROM verification_cases')" = 1 ]
code=$(curl -sS -o /dev/null -w '%{http_code}' -X POST -H "$H" \
 -H 'Origin: https://aurekai.work' --data-urlencode "set=review-sample" \
 "$base/discovery/verification")
[ "$code" = 403 ]
code=$(curl -sS -o /dev/null -w '%{http_code}' -X POST -H "$H" \
 -H "Authorization: Bearer $token" --data-urlencode "set=review-sample" \
 "$base/discovery/verification")
[ "$code" = 403 ]
echo 'native discovery review HTTP: PASS (auth/origin/CAS/readback, accepted source -> HTML/Markdown/YAFF, rejected withheld)'
