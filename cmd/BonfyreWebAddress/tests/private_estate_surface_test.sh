#!/bin/sh
# A real browser-carrier witness for the delegated estates.  This proves the
# two distinct authority gates and the native Upper90 product owner together:
# an Aurekai session is not client access; client admission is not a role; a
# role only exposes its own product/scope; a request is WorkGraph review work,
# never a direct JRSS or AI Loss mutation.
set -eu

web=${1:?web-address binary required}
case "$web" in /*) ;; *) web="$(pwd)/$web" ;; esac
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
auth="$root/cmd/BonfyreAuth/bonfyre-auth"
lex="$root/cmd/BonfyreLex/bonfyre-lex"
upper90="$root/cmd/BonfyreUpper90/bonfyre-upper90"
cms="$root/cmd/BonfyreCMS/bonfyre-cms"
project="$root/cmd/BonfyreProject/bonfyre-project"
[ -x "$auth" ] || { echo 'private estate surface: SKIP (auth not built)'; exit 0; }
[ -x "$upper90" ] || { echo 'private estate surface: SKIP (upper90 not built)'; exit 0; }
[ -x "$cms" ] || { echo 'private estate surface: SKIP (cms not built)'; exit 0; }
[ -x "$project" ] || make -C "$root/cmd/BonfyreProject" bonfyre-project >/dev/null

tmp=$(mktemp -d)
pid=''
source_pid=''
cleanup() { [ -n "$pid" ] && kill "$pid" 2>/dev/null || true; [ -n "$source_pid" ] && kill "$source_pid" 2>/dev/null || true; rm -rf "$tmp"; }
trap cleanup EXIT INT TERM

matrix="$tmp/matrix.db"
authdb="$tmp/auth.db"
u90db="$tmp/upper90.db"
cmsdb="$tmp/cms.db"
cms_schemas="$root/cmd/BonfyreCMS/content-types"
foreign_root="$tmp/foreign"
port=18895
source_port=18896
host='Host: jrss.aurekai.work'
public_host='Host: aurekai.life'
pw='private-estate-fixture-passphrase'

"$web" subject --db "$matrix" --id object:private:fixture --kind PrivateFixture \
  --digest bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb \
  --evidence receipt:private-fixture >/dev/null
"$web" bootstrap-owned --db "$matrix" >/dev/null
"$cms" schema migrate --db "$cmsdb" --schemas "$cms_schemas" >/dev/null
mkdir "$tmp/bin"
ln -s "$upper90" "$tmp/bin/bonfyre-upper90"
ln -s "$project" "$tmp/bin/bonfyre-project"
printf 'private observed source fixture\n' >"$tmp/source.html"
printf 'User-agent: *\nAllow: /\n' >"$tmp/robots.txt"
python3 -m http.server "$source_port" --bind 127.0.0.1 --directory "$tmp" >"$tmp/source.log" 2>&1 &
source_pid=$!
sleep 1
BONFYRE_AUTH_BINARY="$auth" BONFYRE_BIN_DIR="$tmp/bin" BONFYRE_UPPER90_DB="$u90db" \
  BONFYRE_CMS_BINARY="$cms" BONFYRE_CMS_DB="$cmsdb" BONFYRE_CMS_SCHEMAS="$cms_schemas" BONFYRE_FOREIGN_ROOT="$foreign_root" \
  "$web" serve --db "$matrix" --auth-db "$authdb" --port "$port" \
  --site-root "$root/site/matrix" >"$tmp/server.log" 2>&1 &
pid=$!
sleep 1
base="http://127.0.0.1:$port"

curl -fsS -X POST -H "$public_host" -d "{\"email\":\"private@t.test\",\"name\":\"Private\",\"password\":\"$pw\"}" \
  "$base/signup" | grep -q '"created":true'
uid=$(sqlite3 "$authdb" "select id from users where email='private@t.test';")
[ -n "$uid" ] || { echo 'private estate surface: fixture user missing'; exit 1; }
"$auth" --db "$authdb" update --id "$uid" --tier pro >/dev/null
token=$(curl -fsS -X POST -H "$public_host" -d "{\"email\":\"private@t.test\",\"password\":\"$pw\"}" "$base/login" | "$lex" field --kind json_string --key session.token)
[ -n "$token" ] || { echo 'private estate surface: login issued no token'; exit 1; }

# Neither authentication nor a paid tier discloses a private hostname.
code=$(curl -sS -o /dev/null -w '%{http_code}' -H "$host" -H "Authorization: Bearer $token" "$base/")
[ "$code" = 403 ] || { echo "private estate: ungranted root returned $code"; exit 1; }

# The generic admission edge is exact to JRSS.  It admits this estate but no
# role/scope appears until the native AuthorityGraph product grant exists.
"$auth" --db "$authdb" institution-grant --user-id "$uid" --institution jrss --role write >/dev/null
unassigned=$(curl -fsS -H "$host" -H "Authorization: Bearer $token" "$base/")
printf '%s' "$unassigned" | grep -q 'No private client product is assigned'
if printf '%s' "$unassigned" | grep -q 'Payments and splits\|AI Loss financial department'; then
  echo 'private estate: unassigned JRSS surface leaked another product'; exit 1
fi

# This is the second, scope-bearing authority gate.  The actual HTML is the
# named Stable Owner product, not a JSON inspection of a role bitset.
"$upper90" --db "$u90db" grant --actor "auth-user:$uid" --estate jrss \
  --role jrss-stable-owner --scope stable:cedar --evidence receipt:jrss:cedar >/dev/null
jrss=$(curl -fsS -H "$host" -H 'Accept: text/html' -H "Authorization: Bearer $token" "$base/")
printf '%s' "$jrss" | grep -q 'Stable owner'
printf '%s' "$jrss" | grep -q 'stable:cedar'
printf '%s' "$jrss" | grep -q 'My horses'
if printf '%s' "$jrss" | grep -Eq 'AI Loss financial department|Payments and splits|\{"'; then
  echo 'private estate: JRSS human surface leaked AI Loss or JSON'; exit 1
fi

# Private Studio is a closure-derived workroom, not the global operator
# console. It embeds the real native Upper90 work surface and opens only the
# same scope's CMS and ForeignTwin carriers.
studio=$(curl -fsS -H "$host" -H 'Accept: text/html' -H "Authorization: Bearer $token" "$base/studio/")
printf '%s' "$studio" | grep -q 'Work from one admitted closure'
printf '%s' "$studio" | grep -q 'Native operating work'
printf '%s' "$studio" | grep -q 'Stable owner'
printf '%s' "$studio" | grep -q 'href="/cms/"'
printf '%s' "$studio" | grep -q 'Prepare a lawful handoff'
printf '%s' "$studio" | grep -q 'cannot publish client work'
printf '%s' "$studio" | grep -q 'href="/foreign/"'
printf '%s' "$studio" | grep -q 'private:jrss:stable:cedar'
if printf '%s' "$studio" | grep -Eq 'Institution Studio|FINANCIAL MEMBRANE|\{"'; then
  echo 'private estate: Studio exposed generic operator state or JSON'; exit 1
fi

# The CMS carrier is the same server-rendered HTMX owner used by the normal
# product, but its namespace comes from the just-evaluated Upper90 closure.
# A browser cannot name a client, role, or CMS namespace to read/write another
# estate.  Create a real native page, then prove the same row is invisible to
# an otherwise admitted AI Loss estate.
cms_form=$(curl -fsS -H "$host" -H 'Accept: text/html' -H "Authorization: Bearer $token" \
  "$base/cms/parts/edit?type=wiki_page&id=new")
printf '%s' "$cms_form" | grep -q 'hx-post="/cms/v1/api/wiki_page"'
printf '%s' "$cms_form" | grep -q 'name="title"'
if printf '%s' "$cms_form" | grep -q '{"'; then
  echo 'private estate: CMS form exposed JSON instead of HTMX grammar'; exit 1
fi
cms_created=$(curl -fsS -X POST -H "$host" -H 'Accept: text/html' -H "Authorization: Bearer $token" \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  --data 'title=Cedar+stable+brief&route=cedar-brief&content=Private+stable+content&published=0&allow_guest=0' \
  "$base/cms/v1/api/wiki_page")
printf '%s' "$cms_created" | grep -q 'Created in the native CMS'
cms_list=$(curl -fsS -H "$host" -H 'Accept: text/html' -H "Authorization: Bearer $token" \
  "$base/cms/parts/list?type=wiki_page")
printf '%s' "$cms_list" | grep -q 'Cedar stable brief'
sqlite3 "$cmsdb" "select namespace from wiki_page where title='Cedar stable brief';" | grep -q '^private:jrss:stable:cedar$'

# A separate client grant and role are not a route to the JRSS content
# namespace.  This is a cross-estate data test, not merely an access-page test.
"$auth" --db "$authdb" institution-grant --user-id "$uid" --institution ai-loss --role write >/dev/null
"$upper90" --db "$u90db" grant --actor "auth-user:$uid" --estate ai-loss \
  --role ai-loss-admin --scope finance:losses --evidence receipt:loss:fixture >/dev/null
loss_list=$(curl -fsS -H 'Host: ai-loss.aurekai.work' -H 'Accept: text/html' \
  -H "Authorization: Bearer $token" "$base/cms/parts/list?type=wiki_page")
if printf '%s' "$loss_list" | grep -q 'Cedar stable brief'; then
  echo 'private estate: AI Loss CMS exposed JRSS row'; exit 1
fi
"$upper90" --db "$u90db" revoke --actor "auth-user:$uid" --estate ai-loss \
  --role ai-loss-admin --scope finance:losses >/dev/null
"$auth" --db "$authdb" institution-revoke --user-id "$uid" --institution ai-loss >/dev/null

# ForeignTwin intake is also a closure carrier.  The private source form is
# server-rendered and points only to the native observer; it does not reuse a
# public Studio route, a public list, or a caller-selected store namespace.
foreign=$(curl -fsS -H "$host" -H 'Accept: text/html' -H "Authorization: Bearer $token" "$base/foreign/")
printf '%s' "$foreign" | grep -q 'Private evidence intake'
printf '%s' "$foreign" | grep -q 'hx-post="/foreign/observe"'
printf '%s' "$foreign" | grep -q 'private:jrss:stable:cedar'
if printf '%s' "$foreign" | grep -q 'Studio\|{"'; then
  echo 'private estate: ForeignTwin carrier leaked generic Studio or JSON'; exit 1
fi

# Follow the HTMX form all the way through the real native observer. The body
# and receipt must be created below the closure-derived root, never in a
# shared/public ForeignTwin directory or merely echoed as a browser receipt.
observed=$(curl -fsS -X POST -H "$host" -H 'Accept: text/html' -H "Authorization: Bearer $token" \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  --data "twin_id=cedar-source&url=http%3A%2F%2F127.0.0.1%3A$source_port%2Fsource.html" \
  "$base/foreign/observe")
printf '%s' "$observed" | grep -q 'studio-result'
printf '%s' "$observed" | grep -q 'source_observed'
receipt=$(find "$foreign_root/private/jrss/stable:cedar/receipts" -type f -name 'cedar-source-*.json' | head -n 1)
[ -n "$receipt" ] || { echo 'private estate: no closure-scoped ForeignTwin receipt'; exit 1; }
grep -q '"twin_id":"cedar-source"' "$receipt"
grep -q '"observation_kind":"source_observed"' "$receipt"
if find "$foreign_root" -path "$foreign_root/private" -prune -o -type f -print | grep -q .; then
  echo 'private estate: ForeignTwin wrote outside its private closure'; exit 1
fi

# A request reaches native WorkGraph as bounded review work.  The supplied
# reference remains opaque—the surface does not fetch, display, or mutate a
# client record.
request=$(curl -fsS -X POST -H "$host" -H 'Accept: text/html' -H "Authorization: Bearer $token" \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  --data 'estate=jrss&scope=stable%3Acedar&request_kind=jrss-correction&reference=statement-opaque-42' \
  "$base/upper90/requests")
printf '%s' "$request" | grep -q 'durable review work'
printf '%s' "$request" | grep -q 'statement-opaque-42'
sqlite3 "$u90db" "select count(*) from upper90_private_requests where estate='jrss' and reference_token='statement-opaque-42';" | grep -q '^1$'
sqlite3 "$u90db" "select count(*) from workgraph_nodes where operator_id='jrss-correction' and family='upper90-private';" | grep -q '^1$'

# A hidden form field cannot turn the JRSS host into an AI Loss route, even if
# that client is otherwise valid in the wider company estate.
code=$(curl -sS -o /dev/null -w '%{http_code}' -X POST -H "$host" -H 'Accept: text/html' \
  -H "Authorization: Bearer $token" -H 'Content-Type: application/x-www-form-urlencoded' \
  --data 'estate=ai-loss&scope=stable%3Acedar&request_kind=loss-split-review&reference=loss-opaque-9' \
  "$base/upper90/requests")
[ "$code" = 403 ] || { echo "private estate: cross-client form returned $code"; exit 1; }
sqlite3 "$u90db" "select count(*) from upper90_private_requests where reference_token='loss-opaque-9';" | grep -q '^0$'

# Private hostnames do not publish machine-carrier paths, even before a token
# is supplied.  A guessed hostname must not become a sitemap or MCP oracle.
code=$(curl -sS -o /dev/null -w '%{http_code}' -H "$host" "$base/.well-known/bonfyre")
[ "$code" = 404 ] || { echo "private estate: discovery path returned $code"; exit 1; }
code=$(curl -sS -o /dev/null -w '%{http_code}' -H "$host" "$base/ro-crate/ro-crate-metadata.json")
[ "$code" = 404 ] || { echo "private estate: RO-Crate path returned $code"; exit 1; }

# Client admission is exact: the same session cannot use the AI Loss host.
code=$(curl -sS -o /dev/null -w '%{http_code}' -H 'Host: ai-loss.aurekai.work' \
  -H "Authorization: Bearer $token" "$base/")
[ "$code" = 403 ] || { echo "private estate: JRSS grant reached AI Loss: $code"; exit 1; }

# Role revocation and client-grant revocation are independently immediate.
"$upper90" --db "$u90db" revoke --actor "auth-user:$uid" --estate jrss \
  --role jrss-stable-owner --scope stable:cedar >/dev/null
revoked_role=$(curl -fsS -H "$host" -H "Authorization: Bearer $token" "$base/")
printf '%s' "$revoked_role" | grep -q 'No private client product is assigned'
"$auth" --db "$authdb" institution-revoke --user-id "$uid" --institution jrss >/dev/null
code=$(curl -sS -o /dev/null -w '%{http_code}' -H "$host" -H "Authorization: Bearer $token" "$base/")
[ "$code" = 403 ] || { echo "private estate: revoked client admission returned $code"; exit 1; }

# The same address grammar is inherited through the declared company tree:
# Upper90 admission plus one leadership edge opens only its named child hosts.
"$auth" --db "$authdb" institution-grant --user-id "$uid" --institution upper90 --role write >/dev/null
"$upper90" --db "$u90db" grant --actor "auth-user:$uid" --estate upper90 \
  --role upper90-leadership --scope institution:upper90 --evidence receipt:upper90:parent >/dev/null
for estate in jrss ai-loss leads; do
  page=$(curl -fsS -H "Host: $estate.aurekai.work" -H 'Accept: text/html' \
    -H "Authorization: Bearer $token" "$base/")
  printf '%s' "$page" | grep -q 'inherited from Upper90 leadership'
done
# Parent authority inherits a child product aperture but it does not merge
# into a child's former direct scope. The CMS owner gets the inherited child
# coordinate, so leadership cannot silently read stable:cedar's private rows.
inherited_cms=$(curl -fsS -X POST -H 'Host: jrss.aurekai.work' -H 'Accept: text/html' \
  -H "Authorization: Bearer $token" -H 'Content-Type: application/x-www-form-urlencoded' \
  --data 'title=Leadership+JRSS+brief&route=leadership-jrss-brief&content=Parent+scoped+content&published=0&allow_guest=0' \
  "$base/cms/v1/api/wiki_page")
printf '%s' "$inherited_cms" | grep -q 'Created in the native CMS'
sqlite3 "$cmsdb" "select namespace from wiki_page where title='Leadership JRSS brief';" | grep -q '^private:jrss:institution:jrss$'
inherited_list=$(curl -fsS -H "$host" -H 'Accept: text/html' -H "Authorization: Bearer $token" \
  "$base/cms/parts/list?type=wiki_page")
if printf '%s' "$inherited_list" | grep -q 'Cedar stable brief'; then
  echo 'private estate: inherited parent CMS closure merged a child scope'; exit 1
fi
"$upper90" --db "$u90db" revoke --actor "auth-user:$uid" --estate upper90 \
  --role upper90-leadership --scope institution:upper90 >/dev/null
page=$(curl -fsS -H "$host" -H "Authorization: Bearer $token" "$base/")
printf '%s' "$page" | grep -q 'No private client product is assigned'
"$auth" --db "$authdb" institution-revoke --user-id "$uid" --institution upper90 >/dev/null
code=$(curl -sS -o /dev/null -w '%{http_code}' -H "$host" -H "Authorization: Bearer $token" "$base/")
[ "$code" = 403 ] || { echo "private estate: revoked parent admission returned $code"; exit 1; }

printf '%s\n' 'private estate HTTP surface: PASS'
