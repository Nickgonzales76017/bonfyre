#!/bin/sh
# Native authority and concurrency checks in a disposable Auth database.
set -eu
here=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
binary=$here/../bonfyre-auth
home=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-members.XXXXXX")
lockpid=''
cleanup() { [ -z "$lockpid" ] || kill "$lockpid" 2>/dev/null || :; rm -rf "$home"; }
trap cleanup 0 1 2 15
db=$home/auth.db
fail() { echo "institution members: FAIL $*" >&2; exit 1; }
# j JSON PATH -> value (sqlite JSON functions; no jq dependency)
j() { printf '%s' "$1" > "$home/j.json"; sqlite3 :memory: "select json_extract(cast(readfile('$home/j.json') as text),'$2')"; }
sql() { sqlite3 "$db" "$1"; }
# call COMMAND [ARGS...] with token on stdin; expect success unless EXPECT=fail
call() {
  rc=0; printf '%s' "${TOKEN:-}" | "$binary" --db "$db" "$@" > "$home/out" 2> "$home/err" || rc=$?
  if [ "${EXPECT:-ok}" = ok ] && [ "$rc" -ne 0 ]; then fail "$* expected success: $(cat "$home/err") $(cat "$home/out")"; fi
  if [ "${EXPECT:-ok}" = fail ] && [ "$rc" -eq 0 ]; then fail "$* expected refusal: $(cat "$home/out")"; fi
  cat "$home/out"
  TOKEN=''; EXPECT=ok
}
member_field() { # members_json user_id field
  printf '%s' "$1" > "$home/m.json"
  sqlite3 :memory: "select json_extract(value,'\$.$3') from json_each(cast(readfile('$home/m.json') as text),'\$.members') where json_extract(value,'\$.user_id')=$2"
}

for name in admin member reader; do
  TOKEN="{\"email\":\"$name@example.test\",\"password\":\"team-test-password\",\"name\":\"$name\"}" call http-signup >/dev/null
done
admin=$(j "$(TOKEN='{"email":"admin@example.test","password":"team-test-password"}' call http-login)" '$.session.token')
member=$(j "$(TOKEN='{"email":"member@example.test","password":"team-test-password"}' call http-login)" '$.session.token')
call institution-grant --user-id 1 --institution upper90 --role admin --expires-at 2090-01-01T00:00:00Z >/dev/null
call institution-grant --user-id 3 --institution jrss --role read >/dev/null
TOKEN=$member EXPECT=fail call institution-member-set-stdin --institution jrss --email member@example.test --role write >/dev/null
TOKEN=$admin call institution-member-set-stdin --institution jrss --email member@example.test --role write >/dev/null
TOKEN=$member call institution-authorize-stdin --institution jrss --role write >/dev/null
TOKEN=$member EXPECT=fail call institution-authorize-stdin --institution ai-loss --role read >/dev/null
rows=$(TOKEN=$admin call institution-members-stdin --institution jrss)
[ "$(member_field "$rows" 2 expires_at)" = 2090-01-01T00:00:00Z ] || fail "member expiry follows admin bound"
printf '%s' "$rows" > "$home/r.json"; [ "$(sqlite3 :memory: "select json_array_length(cast(readfile('$home/r.json') as text),'\$.members')")" = 2 ] || fail "parent grants are not editable direct grants"
# Reviewed mutations compare the direct grant inside the write transaction.
ms() { TOKEN=$admin call institution-member-set-stdin --institution jrss --email member@example.test "$@"; }
ms --role read --expected-role write --expected-expiry 2090-01-01T00:00:00Z >/dev/null
[ "$(j "$(EXPECT=fail ms --role admin --expected-role write --expected-expiry 2090-01-01T00:00:00Z)" '$.status')" = 409 ] || fail "stale review"
[ "$(j "$(EXPECT=fail ms --role write --expected-role none)" '$.status')" = 409 ] || fail "absent review"
ms --role write --expected-role read --expected-expiry 2090-01-01T00:00:00Z >/dev/null
TOKEN=$admin EXPECT=fail call institution-member-set-stdin --institution upper90 --email admin@example.test --role none >/dev/null
TOKEN=$admin EXPECT=fail call institution-member-set-stdin --institution bonfyre --email member@example.test --role admin >/dev/null
TOKEN=$admin EXPECT=fail call institution-member-set-stdin --institution jrss-lookalike --email member@example.test --role admin >/dev/null
EXPECT=fail ms --role root >/dev/null
TOKEN=$admin EXPECT=fail call institution-member-set-stdin --institution jrss --email missing@example.test --role read >/dev/null
# Role editing preserves existing expiry, including an already expired grant.
sql "UPDATE institution_grants SET expires_at='2020-01-01T00:00:00Z' WHERE user_id=2 AND institution='jrss'"
[ "$(j "$(EXPECT=fail ms --role read --expected-role write --expected-expiry 2090-01-01T00:00:00Z)" '$.status')" = 409 ] || fail "stale expiry review"
ms --role read --expected-role write --expected-expiry 2020-01-01T00:00:00Z >/dev/null
rows=$(TOKEN=$admin call institution-members-stdin --institution jrss)
[ "$(member_field "$rows" 2 expires_at)" = 2020-01-01T00:00:00Z ] && [ "$(member_field "$rows" 2 effective)" = 0 ] || fail "expired grant kept and ineffective"
# A role change on an unlimited grant remains bounded by temporary admin authority.
sql "UPDATE institution_grants SET expires_at=NULL WHERE user_id=2 AND institution='jrss'"
ms --role write --expected-role read --expected-expiry '' >/dev/null
[ "$(sql "SELECT expires_at FROM institution_grants WHERE user_id=2 AND institution='jrss'")" = 2090-01-01T00:00:00Z ] || fail "unlimited grant bounded by admin expiry"

# hold_lock: a sqlite3 session holds BEGIN IMMEDIATE until release_lock SQL arrives.
hold_lock() {
  mkfifo "$home/lock.fifo"; sqlite3 "$db" < "$home/lock.fifo" > "$home/lock.out" 2>&1 & lockpid=$!
  exec 3> "$home/lock.fifo"; printf 'PRAGMA busy_timeout=5000;\nBEGIN IMMEDIATE;\nSELECT 1;\n' >&3
  i=0; until grep -q 1 "$home/lock.out" 2>/dev/null; do i=$((i+1)); [ $i -lt 100 ] || fail "lock not acquired"; sleep 0.05; done
}
release_lock() { printf '%s\nCOMMIT;\n.quit\n' "$1" >&3; exec 3>&-; wait "$lockpid" 2>/dev/null || :; lockpid=''; rm -f "$home/lock.fifo" "$home/lock.out"; }
# A queued review must see a competing committed change after acquiring the lock.
hold_lock
printf '%s' "$admin" | "$binary" --db "$db" institution-member-set-stdin --institution jrss --email member@example.test --role admin --expected-role write --expected-expiry 2090-01-01T00:00:00Z > "$home/pending.out" 2>&1 & pending=$!
sleep 0.15; kill -0 "$pending" 2>/dev/null || fail "review did not wait for the writer lock"
release_lock "UPDATE institution_grants SET role='read' WHERE user_id=2 AND institution='jrss';"
rc=0; wait "$pending" || rc=$?
[ "$rc" -ne 0 ] && [ "$(j "$(cat "$home/pending.out")" '$.status')" = 409 ] || fail "queued review saw stale state"
[ "$(sql "SELECT role FROM institution_grants WHERE user_id=2 AND institution='jrss'")" = read ] || fail "competing change kept"
sqlite3 "$db" "UPDATE users SET name='A \"quoted\" name' || char(10) || 'with a line' WHERE id=2"
rows=$(TOKEN=$admin call institution-members-stdin --institution jrss)
expected_name=$(printf 'A "quoted" name\nwith a line')
[ "$(member_field "$rows" 2 name)" = "$expected_name" ] || fail "escaped name"
profile=$(TOKEN=$member call account-profile-stdin)
[ "$(j "$profile" '$.id')|$(j "$profile" '$.email')" = "2|member@example.test" ] && [ "$(j "$profile" '$.name')" = "$expected_name" ] || fail "profile"
ms --role none >/dev/null
TOKEN=$member EXPECT=fail call institution-authorize-stdin --institution jrss --role read >/dev/null
# Revoked inactive rows count as absent and can be deliberately granted again.
ms --role write --expected-role none >/dev/null
ms --role none --expected-role write --expected-expiry 2090-01-01T00:00:00Z >/dev/null
# Hold the writer lock, queue an operation, then revoke the administrator.
hold_lock
printf '%s' "$admin" | "$binary" --db "$db" institution-member-set-stdin --institution jrss --email member@example.test --role admin > "$home/pending.out" 2>&1 & pending=$!
sleep 0.15; kill -0 "$pending" 2>/dev/null || fail "mutation did not wait for the writer lock"
release_lock "UPDATE institution_grants SET active=0 WHERE user_id=1 AND institution='upper90';"
rc=0; wait "$pending" || rc=$?
[ "$rc" -ne 0 ] || fail "revoked administrator's queued mutation succeeded"
[ "$(sql "SELECT active FROM institution_grants WHERE user_id=2 AND institution='jrss'")" = 0 ] || fail "queued mutation applied"
TOKEN=$admin EXPECT=fail call institution-members-stdin --institution jrss >/dev/null
call institution-grant --user-id 1 --institution upper90 --role admin >/dev/null
# Pagination stays inside the exact requested institution and escapes data.
i=0; while [ $i -lt 55 ]; do
  sql "INSERT INTO users(email,name,password_hash,salt,created_at) VALUES('page-$i@example.test','Member $i','fixture','fixture','2026-01-01T00:00:00Z'); INSERT INTO institution_grants(user_id,institution,role,granted_at,active) VALUES(last_insert_rowid(),'jrss','read','2026-01-01T00:00:00Z',1);"
  i=$((i+1)); done
first=$(TOKEN=$admin call institution-members-stdin --institution jrss)
second=$(TOKEN=$admin call institution-members-stdin --institution jrss --after-id "$(j "$first" '$.next_user_id')")
printf '%s' "$first" > "$home/f.json"; printf '%s' "$second" > "$home/s.json"
[ "$(sqlite3 :memory: "select json_array_length(cast(readfile('$home/f.json') as text),'\$.members')||'|'||json_array_length(cast(readfile('$home/s.json') as text),'\$.members')")" = '50|6' ] || fail "page sizes"
[ -z "$(j "$second" '$.next_user_id')" ] || fail "last page has no cursor"
[ "$(sqlite3 :memory: "select count(*) from json_each(cast(readfile('$home/f.json') as text),'\$.members') a join json_each(cast(readfile('$home/s.json') as text),'\$.members') b on json_extract(a.value,'\$.user_id')=json_extract(b.value,'\$.user_id')")" = 0 ] || fail "pages overlap"
TOKEN=$admin EXPECT=fail call institution-members-stdin --institution jrss --after-id oops >/dev/null
TOKEN=$admin call logout-stdin >/dev/null
TOKEN=$admin EXPECT=fail call account-profile-stdin >/dev/null
TOKEN=$admin EXPECT=fail call institution-members-stdin --institution jrss >/dev/null
echo 'institution members: PASS (scope, expiry preservation, stale reviews, revocation, pagination, locked mutation)'
