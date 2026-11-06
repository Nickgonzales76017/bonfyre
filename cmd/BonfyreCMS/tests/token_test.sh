#!/bin/sh
# Tokens guard the content store's HTTP surface.  Issued tokens are long and
# unlike each other, only a SHA-256 of one is stored, a wrong or absent token
# is refused, a token reaches only its own namespace, and a token stored the
# old way still works but is marked for reissue.  Run against the store's own
# server, which this test starts.
set -eu
here=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
cms=${1:-$here/../bonfyre-cms}
schemas=$here/../content-types
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bfcms-token.XXXXXX")
pid=''
cleanup() { [ -n "$pid" ] && kill "$pid" 2>/dev/null || true; rm -rf "$tmp"; }
trap cleanup EXIT INT TERM
fail() { echo "token test: $1" >&2; exit 1; }
db="$tmp/cms.db"
mkdir "$tmp/schemas"; cp "$schemas/site_page.json" "$tmp/schemas/"
"$cms" schema migrate --db "$db" --schemas "$tmp/schemas" >/dev/null 2>&1
"$cms" entry create site_page '{"route":"/a","title":"A","heading":"A","status":"published"}' --ns acme --db "$db" --schemas "$tmp/schemas" >/dev/null 2>&1

one=$("$cms" token issue --scope acme --actions find,findOne --label one --db "$db" --schemas "$tmp/schemas" 2>/dev/null | grep -o 'bfcms_[0-9a-f]*')
two=$("$cms" token issue --scope other --actions find,findOne --label two --db "$db" --schemas "$tmp/schemas" 2>/dev/null | grep -o 'bfcms_[0-9a-f]*')
[ "${#one}" = 70 ] || fail "a token is not 32 random bytes: ${#one} characters"
[ "$one" != "$two" ] || fail "two tokens are the same"
[ "$(sqlite3 "$db" "SELECT min(length(token_hash)) FROM _tokens")" = 64 ] || fail "a token is not stored as SHA-256"
if sqlite3 "$db" "SELECT token_hash FROM _tokens" | grep -q "${one#bfcms_}"; then fail "the token itself is in the store"; fi

port=$(python3 -c "import socket; s=socket.socket(); s.bind(('127.0.0.1',0)); print(s.getsockname()[1]); s.close()")
"$cms" serve --port "$port" --db "$db" --schemas "$tmp/schemas" >"$tmp/serve.log" 2>&1 &
pid=$!
n=0; until curl -s -o /dev/null "http://127.0.0.1:$port/v1/health"; do n=$((n+1)); [ "$n" -lt 60 ] || fail "server did not start"; sleep 0.1; done
code() { curl -s -o "$tmp/out" -w '%{http_code}' ${2:+-H "Authorization: Bearer $2"} "http://127.0.0.1:$port/v1/api/$1/site_page"; }
[ "$(code acme "$one")" = 200 ] || fail "its own token was refused: $(cat "$tmp/out")"
grep -q '"route":"/a"' "$tmp/out" || fail "the token did not read its namespace"
[ "$(code acme)" != 200 ] || fail "no token was accepted"
[ "$(code acme bfcms_0000000000000000000000000000000000000000000000000000000000000000)" != 200 ] || fail "a wrong token was accepted"
[ "$(code acme "$two")" != 200 ] || fail "another namespace's token was accepted"
# A token stored under the old 64-bit hash still verifies, and is marked.
sqlite3 "$db" "INSERT INTO _tokens(token_hash,label,scope,created_at,active) VALUES ('af63dc4c8601ec8c','old','acme','2026-01-01T00:00:00Z',1);
INSERT INTO _permissions(token_hash,content_type,action,namespace_scope) VALUES ('af63dc4c8601ec8c','site_page','find','acme');"
[ "$(code acme a)" = 200 ] || fail "a token stored the old way stopped working"
"$cms" token list --db "$db" --schemas "$tmp/schemas" 2>/dev/null | grep 'old' | grep -q 'WEAK' || fail "an old token is not marked for reissue"
# A withdrawn token is refused from the next request on, and stays on record.
"$cms" token revoke --scope acme --label one --db "$db" --schemas "$tmp/schemas" 2>/dev/null | grep -q '"revoked":1' || fail "revoke did not withdraw the token"
[ "$(code acme "$one")" != 200 ] || fail "a withdrawn token still works"
[ "$(sqlite3 "$db" "SELECT count(*) FROM _tokens WHERE label='one' AND active=0")" = 1 ] || fail "the withdrawn token's record is gone"
if "$cms" token revoke --scope acme --label nobody --db "$db" --schemas "$tmp/schemas" >/dev/null 2>&1; then fail "revoking a token that does not exist succeeded"; fi
echo 'token test: PASS'
