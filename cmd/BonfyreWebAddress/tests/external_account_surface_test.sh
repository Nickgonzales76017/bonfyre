#!/bin/sh
# The account page is a human-facing projection of the native Auth owner. This
# narrow witness keeps account-link ownership and browser transport separate
# from the much larger estate fixture used by auth_surface_test.sh.
set -eu

bin="$1"
case "$bin" in /*) ;; *) bin="$(pwd)/$bin" ;; esac
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
auth="$root/cmd/BonfyreAuth/bonfyre-auth"
tmp=$(mktemp -d)
pid=''
cleanup() { [ -n "$pid" ] && kill "$pid" 2>/dev/null || true; rm -rf "$tmp"; }
trap cleanup EXIT INT TERM
db="$tmp/matrix.db"
adb="$tmp/auth.db"
port=18937
host='Host: aurekai.life'

"$bin" bootstrap-owned --db "$db" >/dev/null
BONFYRE_AUTH_BINARY="$auth" "$bin" serve --db "$db" --auth-db "$adb" --port "$port" \
  --site-root "$root/site/matrix" >"$tmp/server.log" 2>&1 &
pid=$!
sleep 1
base="http://127.0.0.1:$port"
password='correct-horse-battery-staple'
# The visible forms are genuine progressively enhanced forms: with no script,
# signup deliberately leads to login (no session is minted by registration),
# and login sets the HttpOnly territory cookie before returning to the account.
signup_form=$(curl -fsS -i -X POST -H "$host" -H 'Accept: text/html' \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -d 'name=Form+Person&email=form-person%40example.test&password=correct-horse-battery-staple' \
  "$base/signup")
printf '%s' "$signup_form" | grep -q '^Location: /login?created=1'
login_form=$(curl -fsS -i -X POST -H "$host" -H 'Accept: text/html' \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -d 'email=form-person%40example.test&password=correct-horse-battery-staple' "$base/login")
printf '%s' "$login_form" | grep -q '^Location: /account'
printf '%s' "$login_form" | grep -qi '^Set-Cookie: bonfyre_session=bfy_.*HttpOnly; SameSite=Lax'
curl -fsS -X POST -H "$host" -H 'Content-Type: application/json' \
  -d "{\"email\":\"account-link@example.test\",\"name\":\"Account Link\",\"password\":\"$password\"}" \
  "$base/signup" >/dev/null
token=$(curl -fsS -X POST -H "$host" -H 'Content-Type: application/json' \
  -d "{\"email\":\"account-link@example.test\",\"password\":\"$password\"}" "$base/login" |
  sed -n 's/.*"token":"\([^"]*\)".*/\1/p')
[ -n "$token" ]

page=$(curl -fsS -H "$host" -H 'Accept: text/html' -H "Cookie: bonfyre_session=$token" "$base/account")
printf '%s' "$page" | grep -q 'Profiles elsewhere'
printf '%s' "$page" | grep -q 'Review profile link'
printf '%s' "$page" | grep -q 'Choose the site you want to shape'
printf '%s' "$page" | grep -q '/apps/account-choices.mjs'
printf '%s' "$page" | grep -q '/assets/matrix-htmx.js'
# The account is deliberately served from a dedicated native sibling in the
# public deployment.  That host has no AddressPlane domain record of its own,
# but it must receive the exact checked-in HTMX runtime or the rendered
# self-authoring controls would be inert.  This is a transport witness, not a
# permission to serve assets on arbitrary wildcard hosts.
asset=$(curl -fsS -H 'Host: auth.aurekai.life' "$base/assets/matrix-htmx.js")
printf '%s' "$asset" | grep -q 'globalThis.htmx'
profile=$(curl -fsS -H 'Host: auth.aurekai.life' -H 'Accept: text/html' -H "Cookie: bonfyre_session=$token" "$base/studio/v1/profile")
printf '%s' "$profile" | grep -q 'Create my Life presence'
printf '%s' "$profile" | grep -q 'hx-post="/studio/v1/matrix/self-publish"'
curl -fsS -X POST -H "$host" -H "Authorization: Bearer $token" \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -d 'provider=github&remote_account=account-link&profile_url=https%3A%2F%2Fgithub.com%2Faccount-link&display_name=Account+Link' \
  "$base/account/connections" | grep -q '"proof_state":"declared"'
curl -fsS -H "$host" -H "Authorization: Bearer $token" "$base/account/connections" |
  grep -q 'https://github.com/account-link'
curl -fsS -X POST -H "$host" -H "Authorization: Bearer $token" \
  -H 'Content-Type: application/x-www-form-urlencoded' -d 'link_id=1' \
  "$base/account/connections/revoke" | grep -q '"revoked":true'

echo "external account surface: ok"
