#!/bin/sh
# Auth surface over HTTP: signup, login, account, logout.
#
# The address organ holds no credentials. It forwards to BonfyreAuth, which
# stores users and sessions and derives password hashes with PBKDF2-HMAC-SHA256
# at 120,000 iterations. This witness covers the HTTP edge: that a session can
# be created and used, that every failure path refuses rather than leaks, and
# that a bearer token stops working the moment it is revoked.
set -eu

bin="$1"
case "$bin" in
  /*) ;;
  *) bin="$(pwd)/$bin" ;;
esac
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
auth="$root/cmd/BonfyreAuth/bonfyre-auth"
lex="$root/cmd/BonfyreLex/bonfyre-lex"
economy="$root/cmd/BonfyreEconomy/bonfyre-economy"
entity="$root/cmd/BonfyreEntity/bonfyre-entity"
query="$root/cmd/BonfyreQuery/bonfyre-query"
cms="$root/cmd/BonfyreCMS/bonfyre-cms"
frappe_compiler="$root/cmd/BonfyreFrappeCompiler/bonfyre-frappe-compiler"
project="$root/cmd/BonfyreProject/bonfyre-project"
[ -x "$cms" ] || { echo "auth surface: SKIP (bonfyre-cms not built)"; exit 0; }
[ -x "$frappe_compiler" ] || { echo "auth surface: SKIP (bonfyre-frappe-compiler not built)"; exit 0; }
[ -x "$auth" ] || { echo "auth surface: SKIP (bonfyre-auth not built)"; exit 0; }
[ -x "$economy" ] || { echo "auth surface: SKIP (bonfyre-economy not built)"; exit 0; }
[ -x "$entity" ] || { echo "auth surface: SKIP (bonfyre-entity not built)"; exit 0; }
[ -x "$query" ] || { echo "auth surface: SKIP (bonfyre-query not built)"; exit 0; }
[ -x "$project" ] || { echo "auth surface: SKIP (bonfyre-project not built)"; exit 0; }

tmp=$(mktemp -d)
pid=''
source_pid=''
cleanup() { [ -n "$pid" ] && kill "$pid" 2>/dev/null || true; [ -n "$source_pid" ] && kill "$source_pid" 2>/dev/null || true; rm -rf "$tmp"; }
trap cleanup EXIT INT TERM

db="$tmp/matrix.db"
adb="$tmp/auth.db"
cdb="$tmp/cms.db"
mdb="$tmp/membrane.db"
cms_schemas="$root/cmd/BonfyreCMS/content-types"
port=18893
source_port=18894
digest=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
H='Host: aurekai.life'

"$bin" subject --db "$db" --id object:auth:probe --kind PlantPassport \
  --digest "$digest" --evidence receipt:auth >/dev/null
"$bin" bootstrap-owned --db "$db" >/dev/null
"$cms" schema migrate --db "$cdb" --schemas "$cms_schemas" >/dev/null
"$frappe_compiler" --root "$root" --phase 1 --emit-cms-schema "$tmp/crm-deal.cms.json" \
  --cms-doctype "CRM Deal" "$root/integrations/frappe-bench/apps/crm" >/dev/null
"$cms" schema apply "$tmp/crm-deal.cms.json" --db "$cdb" >/dev/null
# The public membrane may say that coverage exists, but must never publish
# exact foreign roots or descriptor candidates.  Only the separate Bonfyre
# admin closure may inspect this fixture's source coordinate.
sqlite3 "$mdb" "create table source_passport(root text primary key,best_carrier text,rung integer,declined_reason text); create table machine_reader(agent text primary key,family text,requests integer,answered_200 integer,answered_404 integer); create table source_protocol_descriptor(root text,family text,descriptor text,truth_level text,active integer,first_seen text,last_probed text,primary key(root,family,descriptor)); insert into source_passport values('external-control.example','feed',3,null); insert into machine_reader values('fixturebot','test',2,2,0); insert into source_protocol_descriptor values('external-control.example','mcp-descriptor-candidate','https://external-control.example/.well-known/mcp.json','declared_descriptor',1,'now','now');"

mkdir "$tmp/receipts"
mkdir "$tmp/bin"
chmod +x "$root/cmd/BonfyreWebAddress/tests/foreign-discover-fixture.sh"
ln -s "$economy" "$tmp/bin/bonfyre-economy"
ln -s "$entity" "$tmp/bin/bonfyre-entity"
ln -s "$query" "$tmp/bin/bonfyre-query"
ln -s "$project" "$tmp/bin/bonfyre-project"
ln -s "$bin" "$tmp/bin/bonfyre-web-address"
printf 'private self source fixture\n' >"$tmp/self-source.html"
printf 'User-agent: *\nAllow: /\n' >"$tmp/robots.txt"
python3 -m http.server "$source_port" --bind 127.0.0.1 --directory "$tmp" >"$tmp/source.log" 2>&1 &
source_pid=$!
BONFYRE_AUTH_BINARY="$auth" BONFYRE_BIN_DIR="$tmp/bin" BONFYRE_MATRIX_DB="$db" BONFYRE_ECONOMY_DB="$tmp/economy.db" BONFYRE_ENTITY_DB="$tmp/entity.db" BONFYRE_MATRIX_RECEIPT_DIR="$tmp/receipts" BONFYRE_INTERNET_DB="$tmp/internet.db" BONFYRE_MEMBRANE_DB="$mdb" BONFYRE_FOREIGN_ROOT="$tmp/foreign" BONFYRE_FOREIGN_DISCOVER_BIN="$root/cmd/BonfyreWebAddress/tests/foreign-discover-fixture.sh" BONFYRE_CMS_BINARY="$cms" BONFYRE_CMS_DB="$cdb" BONFYRE_CMS_SCHEMAS="$cms_schemas" "$bin" serve --db "$db" --auth-db "$adb" \
  --port "$port" --site-root "$root/site/matrix" >"$tmp/server.log" 2>&1 &
pid=$!
sleep 1

base="http://127.0.0.1:$port"
pw='a-long-enough-passphrase'

# The descriptor advertises auth only because the binary is reachable.
curl -fsS -H "$H" "$base/.well-known/bonfyre" | grep -q '"auth":'

# Signup creates an account.
curl -fsS -X POST -H "$H" -d "{\"email\":\"w@t.test\",\"name\":\"W\",\"password\":\"$pw\"}" \
  "$base/signup" | grep -q '"created":true'

# A second signup on the same address must not silently create a duplicate.
code=$(curl -sS -o /dev/null -w '%{http_code}' -X POST -H "$H" \
  -d "{\"email\":\"w@t.test\",\"name\":\"W\",\"password\":\"$pw\"}" "$base/signup")
[ "$code" = "400" ] || { echo "duplicate signup returned $code"; exit 1; }

# Login issues a bearer token and an HttpOnly cookie scoped only to the
# controlled Life territory. It may follow a user to a Life subname, never to
# sibling Live/Shop/Work territories. The cookie is authentication only;
# downstream institution and product-role admission remain separate tests.
login_reply=$(curl -fsS -i -X POST -H "$H" -d "{\"email\":\"w@t.test\",\"password\":\"$pw\"}" \
  "$base/login")
printf '%s\n' "$login_reply" | grep -qi '^Set-Cookie: bonfyre_session=bfy_.*Domain=.aurekai.life; Path=/; Max-Age=86400; Secure; HttpOnly; SameSite=Lax'
token=$(printf '%s\n' "$login_reply" | "$lex" field --kind json_string --key session.token)
[ -n "$token" ] || { echo "login issued no token"; exit 1; }
case "$token" in bfy_*) ;; *) echo "token has wrong shape: $token"; exit 1;; esac

# The token reads the account.
curl -fsS -H "$H" -H "Authorization: Bearer $token" "$base/account" | grep -q '"valid":true'

# A normal browser can now carry the session to controlled subdomains without
# exposing the credential to JavaScript.  The server still validates the exact
# native session before serving the account.
curl -fsS -H "$H" -H "Cookie: bonfyre_session=$token" "$base/account" | grep -q '"valid":true'

# A normal browser gets a page after the redirect, not the machine account
# document just because its HttpOnly cookie has already been parsed.  Legacy
# surfaces that still send an empty localStorage bearer must also fall back to
# that valid cookie rather than masking it with `Bearer null`.
browser_account=$(curl -fsS -H "$H" -H 'Accept: text/html' \
  -H "Cookie: bonfyre_session=$token" "$base/account")
printf '%s' "$browser_account" | grep -q 'Reading your secure session'
printf '%s' "$browser_account" | grep -q 'Connect your household to an institution'
curl -fsS -H "$H" -H 'Authorization: Bearer null' \
  -H "Cookie: bonfyre_session=$token" "$base/account" | grep -q '"valid":true'

# Presentation class is resolved by the native session/grant closure. It is
# never a caller-selected role and carries no authority by itself.
curl -fsS -H "$H" "$base/_surface/presentation-class?grammar=discover" | grep -q '"presentation_class":"anonymous"'
curl -fsS -H "$H" -H "Authorization: Bearer $token" \
  "$base/_surface/presentation-class?grammar=discover" | grep -q '"presentation_class":"personal"'
presentation_code=$(curl -sS -o /dev/null -w '%{http_code}' -H "$H" -H "Authorization: Bearer $token" \
  "$base/_surface/presentation-class?grammar=discover&role=operator")
[ "$presentation_code" = "400" ] || { echo "presentation URL accepted a role: $presentation_code"; exit 1; }

# Existing navigation remains at its native address.  Candidate composition
# must be server-lowered from declared SurfaceIR, never appended as a browser
# application with a caller-selected presentation class or source reservoir.
if grep -q 'data-bonfyre-grammar\|external-observation-candidates' "$root/cmd/BonfyreWebAddress/src/main.c"; then
  echo "browser-owned external composition returned to the native address compiler"; exit 1
fi
presentation_code=$(curl -sS -o /dev/null -w '%{http_code}' -H "$H" -H "Authorization: Bearer $token" \
  "$base/_surface/presentation-class?grammar=discover&presentation_class=operator")
[ "$presentation_code" = "400" ] || { echo "presentation URL accepted a presentation class: $presentation_code"; exit 1; }

# A retired browser token can still be syntactically perfect. The HttpOnly
# cookie is the browser's current session transport, so a stale bfy_+64hex
# header must not mask it. Machine clients without the cookie still use their
# explicit Authorization bearer exactly as before.
stale_browser_bearer='bfy_0000000000000000000000000000000000000000000000000000000000000000'
curl -fsS -H "$H" -H "Authorization: Bearer $stale_browser_bearer" \
  -H "Cookie: bonfyre_session=$token" "$base/account" | grep -q '"valid":true'

# The same session is a narrow edge witness for protected native projections.
curl -fsS -H "$H" -H "Authorization: Bearer $token" "$base/_bonfyre_session" | grep -q '"valid":true'
code=$(curl -sS -o /dev/null -w '%{http_code}' -H "$H" "$base/_bonfyre_session")
[ "$code" = "401" ] || { echo "unsigned session witness returned $code"; exit 1; }
code=$(curl -sS -o /dev/null -w '%{http_code}' -H "$H" "$base/studio/v1/models/status")
[ "$code" = "401" ] || { echo "unsigned Studio projection returned $code"; exit 1; }

# Being signed in is not generic authority. A free-tier session has only the
# explicit self-authoring path below; it remains refused from market work,
# institutional/Frappe records, finance, ForeignTwin observation and generic
# Matrix publication.
code=$(curl -sS -o /dev/null -w '%{http_code}' -X POST -H 'Host: aurekai.shop' \
  -H "Authorization: Bearer $token" -H 'Content-Type: application/json' \
  -d '{"title":"Refused","slug":"refused","kind":"MarketOffer","scope":"market"}' \
  "$base/studio/v1/matrix/publish")
[ "$code" = "403" ] || { echo "free-tier session was allowed to mutate: $code"; exit 1; }

# A regular account can still complete a real, bounded lifecycle: create only
# its own PersonalProfile in the self territory, receive the ordinary Matrix
# constellation/receipt, and retain a private durable carrier recipe. That is
# not a general public/market publication or a QR/payment/device effect.
"$auth" --db "$adb" authorize --token "$token" --scope self | grep -q '"authorized":true'
profile_html=$(curl -fsS -H "$H" -H "Authorization: Bearer $token" -H 'Accept: text/html' "$base/studio/v1/profile")
printf '%s' "$profile_html" | grep -q 'Create my Life presence'
printf '%s' "$profile_html" | grep -q 'Generic write'
if printf '%s' "$profile_html" | grep -q '{"view"'; then
  echo "self account profile rendered raw JSON"; exit 1
fi
self_form=$(curl -fsS -X POST -H "$H" -H "Authorization: Bearer $token" -H 'Accept: text/html' \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -d 'title=Dallas+night+HTMX&slug=dallas-night-htmx' "$base/studio/v1/matrix/self-publish")
printf '%s' "$self_form" | grep -q 'Life presence created'
if printf '%s' "$self_form" | grep -q '{"created"'; then
  echo "self publication rendered raw JSON"; exit 1
fi
code=$(curl -sS -o /dev/null -w '%{http_code}' -H 'Host: aurekai.life' "$base/dallas-night-htmx")
[ "$code" = "404" ] || { echo "actor-owned self presence was publicly readable: $code"; exit 1; }
curl -fsS -H 'Host: aurekai.life' -H "Authorization: Bearer $token" "$base/dallas-night-htmx" | grep -q 'subject:actor-'
self_published=$(curl -fsS -X POST -H 'Host: aurekai.life' -H "Authorization: Bearer $token" \
  -H 'Content-Type: application/json' \
  -d '{"title":"Dallas night profile","slug":"dallas-night-profile","kind":"PersonalProfile","scope":"self"}' \
  "$base/studio/v1/matrix/self-publish")
printf '%s' "$self_published" | grep -q '"created":true'
printf '%s' "$self_published" | grep -q '"address_constellation"'
code=$(curl -sS -o /dev/null -w '%{http_code}' -H 'Host: aurekai.life' "$base/dallas-night-profile")
[ "$code" = "404" ] || { echo "actor-owned self profile was publicly readable: $code"; exit 1; }
code=$(curl -sS -o /dev/null -w '%{http_code}' -H 'Host: aurekai.life' \
  "$base/ro-crate/ro-crate-metadata.json?address=/dallas-night-profile")
[ "$code" = "404" ] || { echo "actor-owned self crate was publicly readable: $code"; exit 1; }
curl -fsS -H 'Host: aurekai.life' -H "Authorization: Bearer $token" "$base/dallas-night-profile" | grep -q 'subject:actor-'
self_carrier=$(curl -fsS -X POST -H "$H" -H "Authorization: Bearer $token" \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -d 'title=Dallas+night+durable+entry&subject_address=https%3A%2F%2Faurekai.life%2Fdallas-night-profile&carrier=qr&payload_profile=web-uri&purpose=resolve&temporal_policy=permanent&audience=self&authority_boundary=none&proximity_policy=scanned-present&security_policy=replay%3Dnot-applicable&status=draft' \
  "$base/cms/v1/api/physical_carrier")
printf '%s' "$self_carrier" | grep -q 'Created in the native CMS'
printf '%s' "$self_carrier" | grep -q 'resolved Matrix subject'
self_carrier_foreign=$(curl -fsS -X POST -H "$H" -H "Authorization: Bearer $token" \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -d 'title=Wrong+subject&subject_address=https%3A%2F%2Faurekai.life%2F&carrier=qr&payload_profile=web-uri&purpose=resolve&temporal_policy=permanent&audience=self&authority_boundary=none&proximity_policy=scanned-present&security_policy=replay%3Dnot-applicable&status=draft' \
  "$base/cms/v1/api/physical_carrier")
printf '%s' "$self_carrier_foreign" | grep -q "own Life subject"
code=$(curl -sS -o /dev/null -w '%{http_code}' -X POST -H 'Host: aurekai.shop' \
  -H "Authorization: Bearer $token" -H 'Content-Type: application/json' \
  -d '{"title":"Wrong territory","slug":"wrong-territory","kind":"PersonalProfile","scope":"self"}' \
  "$base/studio/v1/matrix/self-publish")
[ "$code" = "403" ] || { echo "self publication escaped life territory: $code"; exit 1; }
free_crm=$(curl -fsS -X POST -H "$H" -H "Authorization: Bearer $token" \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -d 'organization_name=Refused+Free+CRM&website=https%3A%2F%2Fexample.test' \
  "$base/cms/v1/api/frappe_crm_crm_deal")
printf '%s' "$free_crm" | grep -q 'not authorized'

# The free account's CMS is a compiled self closure, not a broad operational
# catalogue with a refusal deferred until Save. It exposes only the one
# admitted carrier grammar; a guessed Frappe type produces no form/row leak.
free_cms_nav=$(curl -fsS -H "$H" -H "Authorization: Bearer $token" "$base/cms/parts/nav")
printf '%s' "$free_cms_nav" | grep -q 'physical_carrier'
if printf '%s' "$free_cms_nav" | grep -q 'frappe_crm_crm_deal'; then
  echo "free self closure exposed an institutional CMS grammar"; exit 1
fi
free_carrier_form=$(curl -fsS -H "$H" -H "Authorization: Bearer $token" \
  "$base/cms/parts/edit?type=physical_carrier&id=new")
printf '%s' "$free_carrier_form" | grep -q 'private carrier recipe only'
free_crm_surface=$(curl -fsS -H "$H" -H "Authorization: Bearer $token" \
  "$base/cms/parts/edit?type=frappe_crm_crm_deal&id=new")
printf '%s' "$free_crm_surface" | grep -q 'Institutional and operational content'
if printf '%s' "$free_crm_surface" | grep -q 'organization_name'; then
  echo "free self closure rendered a Frappe form"; exit 1
fi

uid=$("$auth" --db "$adb" users | "$lex" field --kind text_column --key 1 --where 2=w@t.test --required)
[ -n "$uid" ] || uid=$(sqlite3 "$adb" "select id from users where email='w@t.test';" 2>/dev/null)
[ -n "$uid" ] || { echo "could not find the self ForeignTwin witness account"; exit 1; }

# A self account can retain a public source as a private ForeignTwin under its
# actor-derived root. The receipt/body are not public Matrix state, not a
# ForeignTwin claim, and not a way to select another actor's root.
self_foreign=$(curl -fsS -X POST -H "$H" -H "Authorization: Bearer $token" -H 'Accept: text/html' \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  --data "twin_id=self-source&url=http%3A%2F%2F127.0.0.1%3A$source_port%2Fself-source.html" \
  "$base/studio/v1/self/foreign/observe")
printf '%s' "$self_foreign" | grep -q 'Private ForeignTwin retained'
self_receipt=$(find "$tmp/foreign/private/self/actor-$uid/receipts" -type f -name 'self-source-*.json' | head -n 1)
[ -n "$self_receipt" ] || { echo "self ForeignTwin wrote no actor-private receipt"; exit 1; }
grep -q '"twin_id":"self-source"' "$self_receipt"
# A signed-in person gets an actual source desk rather than a one-shot
# success receipt: the private workspace lists only this actor's receipts and
# inspection verifies the retained body before showing an excerpt.
self_workspace=$(curl -fsS -H "$H" -H "Authorization: Bearer $token" -H 'Accept: text/html' \
  "$base/studio/v1/self/foreign")
printf '%s' "$self_workspace" | grep -q 'Your private source desk'
printf '%s' "$self_workspace" | grep -q 'self-source'
self_inspection=$(curl -fsS -X POST -H "$H" -H "Authorization: Bearer $token" -H 'Accept: text/html' \
  -H 'Content-Type: application/x-www-form-urlencoded' --data "receipt=$(basename "$self_receipt")" \
  "$base/studio/v1/self/foreign/inspect")
printf '%s' "$self_inspection" | grep -q 'Private source inspection'
printf '%s' "$self_inspection" | grep -q 'matches receipt'
printf '%s' "$self_inspection" | grep -q 'private self source fixture'
code=$(curl -sS -o /dev/null -w '%{http_code}' -X POST -H 'Host: aurekai.shop' -H "Authorization: Bearer $token" \
  -H 'Content-Type: application/json' -d '{"twin_id":"escaped","url":"https://example.org/"}' \
  "$base/studio/v1/self/foreign/observe")
[ "$code" = "403" ] || { echo "self ForeignTwin escaped Life territory: $code"; exit 1; }

# Raising the tier and signing in again grants write scope, because scope is
# decided at login from the account's tier.
uid=$("$auth" --db "$adb" users | "$lex" field --kind text_column --key 1 --where 2=w@t.test --required)
[ -n "$uid" ] || uid=$(sqlite3 "$adb" "select id from users where email='w@t.test';" 2>/dev/null)
[ -n "$uid" ] || { echo "could not find the witness account"; exit 1; }
"$auth" --db "$adb" update --id "$uid" --tier pro >/dev/null
token=$(curl -fsS -X POST -H "$H" -d "{\"email\":\"w@t.test\",\"password\":\"$pw\"}" \
  "$base/login" | "$lex" field --kind json_string --key session.token)
[ -n "$token" ] || { echo "re-login issued no token"; exit 1; }

# CandidateAddress is a separate truth level from ForeignTwin.  A signed-in
# operator can retain a real discovery receipt through the Observatory's HTMX
# carrier, but the public maintained view reports only a count and cannot leak
# the candidate address or imply a live service.
operator=$(curl -fsS -H 'Host: internet.aurekai.work' -H "Authorization: Bearer $token" "$base/operators/")
printf '%s' "$operator" | grep -q 'Discover the address first'
if printf '%s' "$operator" | grep -q 'candidate.example'; then
  echo "operator lab invented a candidate"; exit 1
fi
candidate=$(curl -fsS -X POST -H 'Host: internet.aurekai.work' -H "Authorization: Bearer $token" \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -d 'address=https%3A%2F%2Fcandidate.example%2F.well-known%2Fagent-card.json&kind=agent%3Aa2a&source=official-registry&query=protocol%3DA2A' \
  "$base/operators/candidates")
printf '%s' "$candidate" | grep -q 'CandidateAddress retained'
printf '%s' "$candidate" | grep -q 'has not been fetched'
sqlite3 "$tmp/internet.db" "select evidence_level from candidate_address where address='https://candidate.example/.well-known/agent-card.json';" | grep -q '^discovery_only$'
public_a2a=$(curl -fsS -H 'Host: internet.aurekai.work' "$base/agents/a2a")
printf '%s' "$public_a2a" | grep -q '>1<'
if printf '%s' "$public_a2a" | grep -q 'candidate.example'; then
  echo "public Observatory view leaked operator candidate address"; exit 1
fi
# A candidate can be selected for the real MachineMembrane passport boundary.
# The probe records how the publisher asks to be read and remains strictly
# below ForeignTwin/body admission. Raw passport/candidate data stays private.
passport=$(curl -fsS -X POST -H 'Host: internet.aurekai.work' -H "Authorization: Bearer $token" \
  -H 'Content-Type: application/x-www-form-urlencoded' -d 'candidate_id=1' "$base/operators/probe")
printf '%s' "$passport" | grep -q 'MachineMembrane passport retained'
printf '%s' "$passport" | grep -q 'feed carrier'
sqlite3 "$tmp/internet.db" "select best_carrier || ':' || robots_allows from source_passport where candidate_id=1;" | grep -q '^feed:1$'
operator=$(curl -fsS -H 'Host: internet.aurekai.work' -H "Authorization: Bearer $token" "$base/operators/")
printf '%s' "$operator" | grep -q 'passport pending\|feed'

# A paid Bonfyre account is still not a managed-client account.  The client
# edge asks the native auth owner for a revocable institution closure; generic
# write scope alone cannot disclose JRSS or AI Loss through a guessed host.
client_read_code=$(curl -sS -o /dev/null -w '%{http_code}' -H "$H" \
  -H "Authorization: Bearer $token" -H 'X-Bonfyre-Institution: jrss' \
  "$base/_bonfyre_institution")
[ "$client_read_code" = "403" ] || { echo "ungranted account reached JRSS closure: $client_read_code"; exit 1; }

# A read grant admits only reads.  A request's original verb is supplied by
# nginx's auth_request bridge, so a POST cannot borrow its GET subrequest.
"$auth" --db "$adb" institution-grant --user-id "$uid" --institution jrss --role read >/dev/null
curl -fsS -H "$H" -H "Authorization: Bearer $token" \
  "$base/_surface/presentation-class?grammar=inbox" | grep -q '"presentation_class":"member_worker"'
curl -fsS -H "$H" -H "Authorization: Bearer $token" -H 'X-Bonfyre-Institution: jrss' \
  "$base/_bonfyre_institution" | grep -q '"institution_authorized":true'
client_write_code=$(curl -sS -o /dev/null -w '%{http_code}' -H "$H" \
  -H "Authorization: Bearer $token" -H 'X-Bonfyre-Institution: jrss' \
  -H 'X-Original-Method: POST' "$base/_bonfyre_institution")
[ "$client_write_code" = "403" ] || { echo "JRSS read grant allowed a write: $client_write_code"; exit 1; }
"$auth" --db "$adb" institution-grant --user-id "$uid" --institution jrss --role write >/dev/null
curl -fsS -H "$H" -H "Authorization: Bearer $token" -H 'X-Bonfyre-Institution: jrss' \
  -H 'X-Original-Method: POST' "$base/_bonfyre_institution" | grep -q '"institution_authorized":true'
# `admin` is not a presentation rung. Administering JRSS remains the exact
# member/worker class; only Bonfyre/admin is the owner/operator closure.
"$auth" --db "$adb" institution-grant --user-id "$uid" --institution jrss --role admin >/dev/null
curl -fsS -H "$H" -H "Authorization: Bearer $token" \
  "$base/_surface/presentation-class?grammar=inbox" | grep -q '"presentation_class":"member_worker"'
"$auth" --db "$adb" institution-revoke --user-id "$uid" --institution jrss >/dev/null
curl -fsS -H "$H" -H "Authorization: Bearer $token" \
  "$base/_surface/presentation-class?grammar=inbox" | grep -q '"presentation_class":"personal"'
client_revoked_code=$(curl -sS -o /dev/null -w '%{http_code}' -H "$H" \
  -H "Authorization: Bearer $token" -H 'X-Bonfyre-Institution: jrss' \
  "$base/_bonfyre_institution")
[ "$client_revoked_code" = "403" ] || { echo "revoked JRSS grant remained usable: $client_revoked_code"; exit 1; }

# Human closure status is a real HTMX fragment over the same native Matrix
# compiler used by RO-Crate, Zarr and MCP. It must show owner-plane boundaries,
# not leak a machine JSON document into the operating surface.
closure_html=$(curl -fsS -H "$H" -H "Authorization: Bearer $token" -H 'Accept: text/html' \
  "$base/studio/v1/closure")
printf '%s' "$closure_html" | grep -q 'Recursive Matrix closure'
printf '%s' "$closure_html" | grep -q 'institution:aurekai:life'
printf '%s' "$closure_html" | grep -q 'Await WorkGraph and financial owners'
if printf '%s' "$closure_html" | grep -q '"@context"'; then
  echo "Studio closure rendered raw RO-Crate JSON"; exit 1
fi

# The Content Studio is an authenticated HTMX surface over the native CMS, not
# a JSON viewer.  A DocType compiled from the installed CRM lineage must become
# an editable native grammar on the same HTTP edge, and its data must remain
# scoped to the signed-in actor's CMS namespace.
cms_nav=$(curl -fsS -H "$H" -H "Authorization: Bearer $token" "$base/cms/parts/nav")
printf '%s' "$cms_nav" | grep -q 'frappe_crm_crm_deal'
printf '%s' "$cms_nav" | grep -q 'physical_carrier'
cms_form=$(curl -fsS -H "$H" -H "Authorization: Bearer $token" \
  "$base/cms/parts/edit?type=frappe_crm_crm_deal&id=new")
printf '%s' "$cms_form" | grep -q 'hx-post="/cms/v1/api/frappe_crm_crm_deal"'
printf '%s' "$cms_form" | grep -q 'organization_name'
if printf '%s' "$cms_form" | grep -q '{"'; then
  echo "CMS human form leaked raw JSON"; exit 1
fi
cms_created=$(curl -fsS -X POST -H "$H" -H "Authorization: Bearer $token" \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -d 'organization_name=Native+CRM+Deal&website=https%3A%2F%2Fexample.test' \
  "$base/cms/v1/api/frappe_crm_crm_deal")
printf '%s' "$cms_created" | grep -q 'Created in the native CMS'
cms_id=$(printf '%s' "$cms_created" | sed -n 's/.*&amp;id=\([0-9][0-9]*\).*/\1/p')
[ -n "$cms_id" ] || { echo "CMS create returned no entry id"; exit 1; }
cms_saved=$(curl -fsS -X PUT -H "$H" -H "Authorization: Bearer $token" \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -d 'organization_name=Changed+Native+CRM+Deal&website=https%3A%2F%2Fexample.test' \
  "$base/cms/v1/api/frappe_crm_crm_deal/$cms_id")
printf '%s' "$cms_saved" | grep -q 'Saved in the native CMS'
cms_list=$(curl -fsS -H "$H" -H "Authorization: Bearer $token" \
  "$base/cms/parts/list?type=frappe_crm_crm_deal")
printf '%s' "$cms_list" | grep -q 'Changed Native CRM Deal'

# JSON-shaped CMS fields cross the authenticated edge as typed JSON, not as a
# quoted browser string. A source collection remains an unpublished reagent
# selection until a later owner binds and publishes material from it.
collection_created=$(curl -fsS -X POST -H "$H" -H "Authorization: Bearer $token" \
  -H 'Accept: application/json' -H 'Content-Type: application/x-www-form-urlencoded' \
  --data-urlencode 'collection_id=auth-foreign-source-proof' \
  --data-urlencode 'subject_address=foreign_twin:auth-proof' \
  --data-urlencode 'source_selector={"source_refs":["auth-proof-source"]}' \
  --data-urlencode 'material_kind=["PublicationOccurrence"]' \
  --data-urlencode 'refresh_policy=manual' --data-urlencode 'status=draft' \
  "$base/cms/v1/api/source_collection")
printf '%s' "$collection_created" | grep -q '"outcome":"saved"'
[ "$(sqlite3 "$cdb" "select json_extract(source_selector,'$.source_refs[0]') from source_collection where collection_id='auth-foreign-source-proof';")" = auth-proof-source ]
[ "$(sqlite3 "$cdb" "select json_type(material_kind) from source_collection where collection_id='auth-foreign-source-proof';")" = array ]
[ "$(sqlite3 "$cdb" "select count(*) from source_collection where collection_id='auth-foreign-source-proof' and status='published';")" = 0 ]

# Physical/mobile carrier recipes are authored in the same scoped native CMS
# grammar. A durable QR is only a lawful HTTPS coordinate and explicit policy,
# never a browser-side claim of identity, payment or effect authority.
carrier_form=$(curl -fsS -H "$H" -H "Authorization: Bearer $token" \
  "$base/cms/parts/edit?type=physical_carrier&id=new")
printf '%s' "$carrier_form" | grep -q 'subject_address'
printf '%s' "$carrier_form" | grep -q 'authority_boundary'
carrier_created=$(curl -fsS -X POST -H "$H" -H "Authorization: Bearer $token" \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -d 'title=Life+presence+durable+entry&subject_address=https%3A%2F%2Faurekai.life%2Fdallas-night-htmx&carrier=qr&payload_profile=web-uri&purpose=resolve&temporal_policy=permanent&audience=public&authority_boundary=none&proximity_policy=scanned-present&security_policy=replay%3Dnot-applicable&status=ready' \
  "$base/cms/v1/api/physical_carrier")
printf '%s' "$carrier_created" | grep -q 'Created in the native CMS'
printf '%s' "$carrier_created" | grep -q 'resolved Matrix subject'
carrier_refused=$(curl -fsS -X POST -H "$H" -H "Authorization: Bearer $token" \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -d 'title=Unsafe+permanent+effect&subject_address=https%3A%2F%2Funsafe.aurekai.live%2F&carrier=qr&payload_profile=web-uri&purpose=pay&temporal_policy=permanent&audience=public&authority_boundary=bounded-effect&proximity_policy=scanned-present&security_policy=replay%3Dallow&status=refused' \
  "$base/cms/v1/api/physical_carrier")
printf '%s' "$carrier_refused" | grep -q 'does not satisfy the physical-carrier policy'

# A second account is a distinct CMS namespace.  A valid session is therefore
# not permission to read a different actor's native content rows.
curl -fsS -X POST -H "$H" -d "{\"email\":\"other@t.test\",\"name\":\"Other\",\"password\":\"$pw\"}" \
  "$base/signup" | grep -q '"created":true'
other_uid=$("$auth" --db "$adb" users | "$lex" field --kind text_column --key 1 --where 2=other@t.test --required)
[ -n "$other_uid" ] || other_uid=$(sqlite3 "$adb" "select id from users where email='other@t.test';" 2>/dev/null)
[ -n "$other_uid" ] || { echo "could not find the second CMS witness account"; exit 1; }
"$auth" --db "$adb" update --id "$other_uid" --tier pro >/dev/null
other_token=$(curl -fsS -X POST -H "$H" -d "{\"email\":\"other@t.test\",\"password\":\"$pw\"}" \
  "$base/login" | "$lex" field --kind json_string --key session.token)
[ -n "$other_token" ] || { echo "second CMS witness login issued no token"; exit 1; }
other_list=$(curl -fsS -H "$H" -H "Authorization: Bearer $other_token" \
  "$base/cms/parts/list?type=frappe_crm_crm_deal")
if printf '%s' "$other_list" | grep -q 'Changed Native CRM Deal'; then
  echo "CMS namespace leaked one actor's row to another"; exit 1
fi

# A write-scoped actor can create a real local Matrix subject, receive its
# address constellation and receipt, then resolve its new web coordinate.
published=$(curl -fsS -X POST -H 'Host: aurekai.shop' -H "Authorization: Bearer $token" \
  -H 'Content-Type: application/json' \
  -d '{"title":"Auth Matrix publication","slug":"auth-matrix-publication","kind":"MarketOffer","scope":"market"}' \
  "$base/studio/v1/matrix/publish")
printf '%s' "$published" | grep -q '"created":true'
printf '%s' "$published" | grep -q '"address_constellation"'
subject=$(printf '%s' "$published" | "$lex" field --kind json_string --key subject_id)
[ -n "$subject" ] || { echo "publication returned no subject"; exit 1; }
curl -fsS -H 'Host: aurekai.shop' "$base/auth-matrix-publication" | grep -q 'subject:actor-'
test -n "$(find "$tmp/receipts" -name '*.json' -print -quit)" || { echo 'publication wrote no receipt'; exit 1; }

# A saved location, alternate Presence, and alias all remain bindings over the
# published subject. The generic endpoints reject a session trying to extend a
# host-owned subject, so signing in never becomes institution authority.
standing=$(curl -fsS -X POST -H 'Host: aurekai.shop' -H "Authorization: Bearer $token" \
  -H 'Content-Type: application/json' \
  -d '{"address":"https://aurekai.shop/auth-matrix-publication","subject_kind":"MarketOffer","territory":"shop","scope":"market","world":"live"}' \
  "$base/studio/v1/matrix/standing")
printf '%s' "$standing" | grep -q '"bound":"standing_address"'
presence=$(curl -fsS -X POST -H 'Host: aurekai.shop' -H "Authorization: Bearer $token" \
  -H 'Content-Type: application/json' \
  -d "{\"id\":\"presence:auth-surface-extra\",\"subject\":\"$subject\",\"territory\":\"shop\",\"scope\":\"market\",\"world\":\"live\",\"repr\":\"API\"}" \
  "$base/studio/v1/matrix/presence")
printf '%s' "$presence" | grep -q '"bound":"presence"'
code=$(curl -sS -o /dev/null -w '%{http_code}' -X POST -H "$H" -H "Authorization: Bearer $token" \
  -H 'Content-Type: application/json' \
  -d '{"id":"presence:forbidden","subject":"institution:aurekai:life","territory":"life","scope":"self","world":"live","repr":"HTML"}' \
  "$base/studio/v1/matrix/presence")
[ "$code" = "403" ] || { echo "foreign Matrix presence returned $code"; exit 1; }

# A host is a disclosure grammar, not a cosmetic alias: incompatible scope
# requests are refused before any subject or address can be bound.
code=$(curl -sS -o /dev/null -w '%{http_code}' -X POST -H "$H" -H "Authorization: Bearer $token" \
  -H 'Content-Type: application/json' \
  -d '{"title":"Bad scope","slug":"bad-scope","kind":"PersonalProject","scope":"public"}' \
  "$base/studio/v1/matrix/publish")
[ "$code" = "503" ] || { echo "incompatible host scope returned $code"; exit 1; }

# Economic control is a real native effect, but this witness deliberately
# proves only a budget cap—not a booked payment or settlement.
budget=$(curl -fsS -X POST -H "$H" -H "Authorization: Bearer $token" \
  -H 'Content-Type: application/json' -d '{"recipe":"auth-surface","usd_cap":"12.50"}' \
  "$base/studio/v1/economy/budget")
printf '%s' "$budget" | grep -q 'budget set: auth-surface'
curl -fsS -H "$H" -H "Authorization: Bearer $token" "$base/studio/v1/economy/status" | grep -q 'auth-surface'

# A client is a real native identity-graph organization rather than a
# surface-local card or a second list. The richer read projection requires the
# deployed DuckDB SQLite extension and is exercised at the Oracle edge.
client=$(curl -fsS -X POST -H "$H" -H "Authorization: Bearer $token" \
  -H 'Content-Type: application/json' -d '{"slug":"auth-surface-client"}' \
  "$base/studio/v1/clients")
printf '%s' "$client" | grep -q 'client:auth-surface-client'

# Bonfyre/admin is the one explicit operator closure that can repair native
# account admission and reach canonical CMS copy from a browser. It is not a
# paid-tier shortcut: the same write-scoped user is refused before the named
# Bonfyre admin grant, then can change a second fixture user's level only after
# that revocable grant exists. The room is private/noindex and never belongs on
# a delegated client host.
public_membrane=$(curl -fsS -H 'Host: aurekai.work' -H 'Accept: text/html' "$base/membrane")
printf '%s' "$public_membrane" | grep -q 'source passport'
if printf '%s' "$public_membrane" | grep -q 'external-control.example\|fixturebot\|mcp-descriptor-candidate'; then
  echo "public membrane leaked external source or reader detail"; exit 1
fi
code=$(curl -sS -o /dev/null -w '%{http_code}' -H 'Host: aurekai.work' -H "Authorization: Bearer $token" "$base/studio/admin")
[ "$code" = "403" ] || { echo "ungranted admin room returned $code"; exit 1; }
"$auth" --db "$adb" institution-grant --user-id "$uid" --institution bonfyre --role admin >/dev/null
curl -fsS -H 'Host: aurekai.work' -H "Authorization: Bearer $token" \
  "$base/_surface/presentation-class?grammar=console" | grep -q '"presentation_class":"operator"'
admin_room=$(curl -fsS -i -H 'Host: aurekai.work' -H "Authorization: Bearer $token" "$base/studio/admin")
printf '%s' "$admin_room" | grep -q 'Bonfÿre / operator closure'
printf '%s' "$admin_room" | grep -qi '^X-Robots-Tag: noindex'
admin_membrane=$(curl -fsS -H 'Host: aurekai.work' -H 'Accept: text/html' -H "Authorization: Bearer $token" "$base/membrane")
printf '%s' "$admin_membrane" | grep -q 'external-control.example'
printf '%s' "$admin_membrane" | grep -q '>test<'
printf '%s' "$admin_membrane" | grep -q 'mcp-descriptor-candidate'
admin_update=$(curl -fsS -H 'Host: aurekai.work' -H "Authorization: Bearer $token" \
  -H 'Content-Type: application/x-www-form-urlencoded' -d "user_id=$other_uid&tier=enterprise" "$base/studio/admin/user")
printf '%s' "$admin_update" | grep -q 'Change applied'
sqlite3 "$adb" "select tier from users where id=$other_uid;" | grep -q '^enterprise$'
# A tier update burns the old session because its scope was compiled at login.
code=$(curl -sS -o /dev/null -w '%{http_code}' -H "$H" -H "Authorization: Bearer $other_token" "$base/account")
[ "$code" = "401" ] || { echo "tier change retained stale session: $code"; exit 1; }
other_token=$(curl -fsS -X POST -H "$H" -d "{\"email\":\"other@t.test\",\"password\":\"$pw\"}" \
  "$base/login" | "$lex" field --kind json_string --key session.token)
[ -n "$other_token" ] || { echo "tier-updated user could not create fresh session"; exit 1; }
admin_grant=$(curl -fsS -H 'Host: aurekai.work' -H "Authorization: Bearer $token" \
  -H 'Content-Type: application/x-www-form-urlencoded' -d "user_id=$other_uid&institution=leads&role=read" "$base/studio/admin/grant")
printf '%s' "$admin_grant" | grep -q 'Change applied'
"$auth" --db "$adb" institution-authorize --token "$other_token" --institution leads --role read | grep -q '"authorized":true'
admin_revoke=$(curl -fsS -H 'Host: aurekai.work' -H "Authorization: Bearer $token" \
  -H 'Content-Type: application/x-www-form-urlencoded' -d "user_id=$other_uid&institution=leads" "$base/studio/admin/revoke")
printf '%s' "$admin_revoke" | grep -q 'Change applied'
"$auth" --db "$adb" institution-authorize --token "$other_token" --institution leads --role read | grep -q '"authorized":false'
admin_sessions=$(curl -fsS -H 'Host: aurekai.work' -H "Authorization: Bearer $token" \
  -H 'Content-Type: application/x-www-form-urlencoded' -d "user_id=$other_uid" "$base/studio/admin/sessions-revoke")
printf '%s' "$admin_sessions" | grep -q 'Change applied'
code=$(curl -sS -o /dev/null -w '%{http_code}' -H "$H" -H "Authorization: Bearer $other_token" "$base/account")
[ "$code" = "401" ] || { echo "session revoke retained active token: $code"; exit 1; }
code=$(curl -sS -o /dev/null -w '%{http_code}' -H 'Host: jrss.aurekai.work' -H "Authorization: Bearer $token" "$base/studio/admin")
[ "$code" = "404" ] || { echo "admin room leaked to delegated estate: $code"; exit 1; }

# Every refusal path refuses.
for probe in \
  "GET::/account::" \
  "GET::/account::Authorization: Bearer bfy_notavalidtokenatall" \
  "POST::/logout::"
do
  method=$(echo "$probe" | cut -d: -f1)
  path=$(echo "$probe" | sed 's/^[^:]*:://; s/::.*$//')
  hdr=$(echo "$probe" | sed 's/^.*:://')
  if [ -n "$hdr" ]; then
    code=$(curl -sS -o /dev/null -w '%{http_code}' -X "$method" -H "$H" -H "$hdr" "$base$path")
  else
    code=$(curl -sS -o /dev/null -w '%{http_code}' -X "$method" -H "$H" "$base$path")
  fi
  [ "$code" = "401" ] || { echo "$method $path with '$hdr' returned $code, expected 401"; exit 1; }
done

# A wrong password is refused and the response carries no hash or salt.
body=$(curl -sS -X POST -H "$H" -d '{"email":"w@t.test","password":"wrong"}' "$base/login")
echo "$body" | grep -qv '"token"' || { echo "failed login returned a token"; exit 1; }
echo "$body" | grep -qiv 'password_hash' || { echo "failed login leaked a hash"; exit 1; }
echo "$body" | grep -qiv '"salt"' || { echo "failed login leaked a salt"; exit 1; }

# GET on a credential endpoint is not allowed.
code=$(curl -sS -o /dev/null -w '%{http_code}' -H "$H" "$base/login")
[ "$code" = "400" ] || { echo "GET /login returned $code, expected 400"; exit 1; }

# Logout revokes: the same token must stop working immediately and the browser
# receives a deletion cookie for this territory only.
logout_reply=$(curl -fsS -i -X POST -H "$H" -H "Authorization: Bearer $token" "$base/logout")
printf '%s\n' "$logout_reply" | grep -qi '^Set-Cookie: bonfyre_session=; Domain=.aurekai.life; Path=/; Max-Age=0; Secure; HttpOnly; SameSite=Lax'
code=$(curl -sS -o /dev/null -w '%{http_code}' -H "$H" -H "Authorization: Bearer $token" "$base/account")
[ "$code" = "401" ] || { echo "revoked token still worked: $code"; exit 1; }

# No credential ever reaches the server log.
if grep -qi "$pw" "$tmp/server.log"; then echo "password appeared in the log"; exit 1; fi

echo "auth surface: PASS"
