#!/bin/sh
# Native WebSub operator inspection with no foreign reads, effects, or secret leaks.
set -eu
web=$1
case "$web" in /*) ;; *) web="$(pwd)/$web";; esac
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
auth="$root/cmd/BonfyreAuth/bonfyre-auth"
[ -x "$auth" ] || exit 1
tmp=$(mktemp -d /tmp/bonfyre-source-push.XXXXXXXX)
pid=""
cleanup(){ [ -z "$pid" ] || kill "$pid" 2>/dev/null || true; rm -rf "$tmp"; }
trap cleanup EXIT HUP INT TERM
fail(){ echo "SOURCE_PUSH_FAIL $*" >&2; tail -15 "$tmp/server.log" >&2 || true; exit 1; }
umask 077
export BONFYRE_WEBSUB_DB="$tmp/websub.db" BONFYRE_CMS_DB="$tmp/cms.db" BONFYRE_MATRIX_DB="$tmp/matrix.db"
export BONFYRE_AUTH_BINARY="$auth"
"$web" bootstrap-owned --db "$BONFYRE_MATRIX_DB" >/dev/null
"$auth" --db "$tmp/auth.db" signup --email owner@example.test --name Operator --password 'fixture-operator-password' >/dev/null
"$auth" --db "$tmp/auth.db" signup --email reader@example.test --name Reader --password 'fixture-operator-password' >/dev/null
owner=$("$auth" --db "$tmp/auth.db" login --email owner@example.test --password 'fixture-operator-password' | jq -r .session.token)
reader=$("$auth" --db "$tmp/auth.db" login --email reader@example.test --password 'fixture-operator-password' | jq -r .session.token)
"$auth" --db "$tmp/auth.db" institution-grant --user-id 1 --institution bonfyre --role admin >/dev/null
"$auth" --db "$tmp/auth.db" institution-grant --user-id 2 --institution bonfyre --role read >/dev/null
sqlite3 "$BONFYRE_WEBSUB_DB" "
CREATE TABLE subscriptions(token TEXT PRIMARY KEY,topic TEXT,hub TEXT,secret TEXT,authority_receipt TEXT,state TEXT,lease_expires INTEGER,verified_at TEXT,last_delivery_at TEXT,deliveries INTEGER,rejected INTEGER);
CREATE TABLE source_dirty(token TEXT PRIMARY KEY,topic TEXT,frontier TEXT,state TEXT,dirty_at INTEGER,attempts INTEGER);
INSERT INTO subscriptions VALUES('secret-token-1','https://news.example.test/private?credential=never-show','https://hub.example.test','secret-hmac-1','secret-authority','active',strftime('%s','now')+86400,'2026-11-04','2026-11-05',8,2);
INSERT INTO subscriptions VALUES('secret-token-2','https://expired.example.test/rss','https://hub.example.test','secret-hmac-2','','active',strftime('%s','now')-7200,'2026-10-04','',0,0);
INSERT INTO subscriptions VALUES('secret-token-3','https://pending.example.test/feed','https://hub.example.test','secret-hmac-3','','pending',0,'','',0,0);
INSERT INTO source_dirty VALUES('secret-token-1','https://news.example.test/private?credential=never-show','websub-delivery','pending',strftime('%s','now'),1);
"
port=$((26000 + ($$ % 16000)))
"$web" serve --db "$BONFYRE_MATRIX_DB" --auth-db "$tmp/auth.db" --site-root "$root/site/matrix" --port "$port" >"$tmp/server.log" 2>&1 & pid=$!
base="http://127.0.0.1:$port"
i=0;until curl -fsS "$base/healthz" >/dev/null 2>&1;do i=$((i+1)); [ "$i" -lt 100 ] || fail 'server boot';sleep .1;done
fetch(){
 if [ -n "$1" ];then
  curl -sS -o "$tmp/response" -w '%{http_code}' -X "$4" -H "Host: $3" -H 'Accept: text/html' -H "Authorization: Bearer $1" "$base$2"
 else
  curl -sS -o "$tmp/response" -w '%{http_code}' -X "$4" -H "Host: $3" -H 'Accept: text/html' "$base$2"
 fi
}
[ "$(fetch '' /sources/push aurekai.work GET)" = 403 ] || fail 'anonymous'
[ "$(fetch "$reader" /sources/push aurekai.work GET)" = 403 ] || fail 'read grant cannot inspect'
[ "$(fetch "$owner" /sources/push aurekai.life GET)" = 404 ] || fail 'apex isolation'
[ "$(fetch "$owner" /sources/push aurekai.work POST)" = 405 ] || fail 'no mutating operation'
[ "$(fetch "$owner" /sources/push aurekai.work GET)" = 200 ] || fail 'admin cannot inspect'
grep -q 'data-source-push-operations' "$tmp/response" || fail 'no inspector'
for domain in news.example.test expired.example.test pending.example.test;do grep -q "$domain" "$tmp/response" || fail "missing $domain";done
for forbidden in secret-token-1 secret-hmac-1 secret-authority never-show;do
 if grep -q "$forbidden" "$tmp/response";then fail "secret exposure $forbidden";fi
done
grep -q 'Stored receipt present (not revalidated here)' "$tmp/response" || fail 'authority receipt indicator'
grep -q 'No retained authority receipt' "$tmp/response" || fail 'unbound source'
for state in expired pending dirty rejected;do
 [ "$(fetch "$owner" "/sources/push?state=$state" aurekai.work GET)" = 200 ] || fail "$state filter"
 case "$state" in expired) yes=expired.example.test;no=news.example.test;;
 pending) yes=pending.example.test;no=expired.example.test;;
 dirty|rejected) yes=news.example.test;no=pending.example.test;;esac
 grep -q "$yes" "$tmp/response" || fail "$state missed $yes"
 if grep -q "$no" "$tmp/response";then fail "$state leaked $no";fi
done
[ "$(fetch "$owner" '/sources/push?state=invalid' aurekai.work GET)" = 400 ] || fail 'invalid filter'
[ "$(fetch "$owner" '/sources/push?page=9999' aurekai.work GET)" = 400 ] || fail 'page bound'
[ "$(fetch "$owner" /sources aurekai.work GET)" = 200 ] || fail 'operator sources'
grep -q 'href="/sources/push"' "$tmp/response" || fail 'admin continuation'
[ "$(fetch "$reader" /sources aurekai.work GET)" = 200 ] || fail 'reader sources'
if grep -q 'href="/sources/push"' "$tmp/response";then fail 'admin continuation leak';fi
before=$(sqlite3 "$BONFYRE_WEBSUB_DB" "SELECT sum(deliveries),sum(rejected),sum(lease_expires) FROM subscriptions")
[ "$(fetch "$owner" '/sources/push?q=news' aurekai.work GET)" = 200 ] || fail 'search'
after=$(sqlite3 "$BONFYRE_WEBSUB_DB" "SELECT sum(deliveries),sum(rejected),sum(lease_expires) FROM subscriptions")
[ "$before" = "$after" ] || fail 'read mutated subscriptions'
"$auth" --db "$tmp/auth.db" institution-revoke --user-id 1 --institution bonfyre >/dev/null
[ "$(fetch "$owner" /sources/push aurekai.work GET)" = 403 ] || fail 'revocation ignored'
echo 'SOURCE_PUSH_PASS: active, pending, expired, dirty and rejected WebSub; scoped, revocable, secret-safe, no writes'
