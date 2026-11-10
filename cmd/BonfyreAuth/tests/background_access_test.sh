#!/bin/sh
# Fresh background projections use the same Auth and CMS policy as sessions.
set -eu
here=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
binary=${BONFYRE_AUTH_TEST_BINARY:-$here/../bonfyre-auth}
root=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-background-test.XXXXXX")
trap 'rm -rf "$root"' 0 1 2 15
auth=$root/auth.db cms=$root/cms.db
fail() { echo "background access: FAIL $*" >&2; exit 1; }

# Decode the native reply [version, ok, body] into key=value lines:
#   version=, ok=, and body.<key>= for a map body (nested values flattened).
decode() {
  od -An -v -tx1 | tr -s ' \n' '\n\n' | grep . | awk '
    function hex(h){ return index("0123456789abcdef", substr(h,1,1))*16 + index("0123456789abcdef", substr(h,2,1)) - 17 }
    function take(n,   s,i){ s=""; for(i=0;i<n;i++){ s=s sprintf("%c", b[p++]) } return s }
    function uint(n,   v,i){ v=0; for(i=0;i<n;i++){ v=v*256+b[p++] } return v }
    function val(   t,n,i,k,v,out){
      t=b[p++]
      if (t<128) return t
      if (t==194) return "false"; if (t==195) return "true"
      if (t>=160 && t<=191) return take(t-160)
      if (t==217) return take(uint(1)); if (t==218) return take(uint(2)); if (t==219) return take(uint(4))
      if (t>=204 && t<=207) return uint(2^(t-204))
      if (t>=208 && t<=211) { n=2^(t-208); v=uint(n); if (v>=2^(8*n-1)) v-=2^(8*n); return v }
      if (t>=144 && t<=159) { n=t-144; out=""; for(i=0;i<n;i++){ v=val(); out=out (i?",":"") v; arr[i]=v }; arrn=n; return out }
      if (t>=128 && t<=143) { n=t-128; for(i=0;i<n;i++){ k=val(); v=val(); map[k]=v; keys[++mk]=k }; return "{map}" }
      printf "decode error: tag %d\n", t > "/dev/stderr"; exit 2
    }
    { b[n++]=hex($1) }
    END { p=0; t=b[p++]; if (t<144||t>159) { print "decode error: not an array" > "/dev/stderr"; exit 2 }
          cnt=t-144; for(j=0;j<cnt;j++){ mk=0; split("",map); split("",keys); v=val()
            if (j==0) print "version=" v; else if (j==1) print "ok=" v
            else { for(m=1;m<=mk;m++) print "body." keys[m] "=" map[keys[m]] } }
          if (p!=n) { print "decode error: trailing data" > "/dev/stderr"; exit 2 } }'
}
field() { sed -n "s/^$1=//p" "$root/reply" | head -1; }
# call actor subject scope permission [auth] [cms]; writes $root/reply
call() {
  rc=0; "$binary" --db "${5:-$auth}" background-access --actor "$1" --subject "$2" --scope "$3" --permission "$4" --cms-db "${6:-$cms}" > "$root/raw" 2>/dev/null || rc=$?
  decode < "$root/raw" > "$root/reply" || fail "undecodable reply for $*"
  [ "$(field version)" = 1 ] || fail "version for $*"
  if [ "$rc" -eq 0 ]; then [ "$(field ok)" = true ] || fail "exit 0 but ok=false for $*"; else [ "$(field ok)" = false ] || fail "exit $rc but ok=true for $*"; fi
  if [ "$(field ok)" = true ]; then
    [ "$(field body.actor)|$(field body.subject)|$(field body.scope)|$(field body.permission)" = "$1|$2|$3|$4" ] || fail "binding echo for $*"
    now=$(date +%s); at=$(field body.checked_at); d=$((now - at)); [ ${d#-} -lt 3 ] || fail "checked_at not fresh for $*"
  fi
  echo x >> "$root/count"
}
allowed() { call "$@"; [ "$(field ok)" = true ] || fail "check did not complete for $*"; field body.allowed; }
expect() { got=$(allowed "$2" "$3" "$4" "$5"); [ "$got" = "$1" ] || fail "expected allowed=$1 for $2 $3 $4 $5, got $got"; }

reset() {
  rm -f "$auth" "$cms"
  sqlite3 "$auth" "CREATE TABLE users(id INTEGER PRIMARY KEY,active INTEGER NOT NULL);
    INSERT INTO users VALUES(1,1),(2,1),(3,0);
    CREATE TABLE sessions(token TEXT PRIMARY KEY,user_id INTEGER,expires_at TEXT,active INTEGER);
    CREATE TABLE institution_grants(user_id INTEGER,institution TEXT,role TEXT,expires_at TEXT,active INTEGER,UNIQUE(user_id,institution));
    INSERT INTO institution_grants VALUES(1,'upper90','write',NULL,1),(2,'ai-loss','read',NULL,1),(3,'bonfyre','admin',NULL,1);"
  sqlite3 "$cms" "CREATE TABLE household(id INTEGER PRIMARY KEY,namespace TEXT,household_id TEXT,status TEXT,steward TEXT);
    INSERT INTO household VALUES(1,'root','home','published','user:1');
    CREATE TABLE _household_authority(household_row INTEGER PRIMARY KEY,epoch INTEGER);
    INSERT INTO _household_authority VALUES(1,7);
    CREATE TABLE _household_membership(household_row INTEGER,member TEXT,authority_epoch INTEGER,role TEXT,expires_at INTEGER);
    INSERT INTO _household_membership VALUES(1,'user:2',7,'read',0);"
}

# active self has read and write without a retained session
reset; expect true user:1 user:1 self read; expect true user:1 user:1 self write
[ "$(sqlite3 "$auth" 'SELECT count(*) FROM sessions')" = 0 ] || fail "a session was retained"
# inactive or unknown users are never authorized
reset; expect false user:3 user:3 self read; expect false user:42 user:42 self read
expect false user:3 institution:ai-loss institution read
# institution ancestry and role use the generated policy
reset
expect true user:1 institution:ai-loss institution write; expect true user:1 institution:jrss institution read
expect false user:1 institution:bonfyre institution read; expect false user:1 institution:nightgrid institution read
expect true user:2 institution:ai-loss institution read; expect false user:2 institution:ai-loss institution write
expect false user:2 institution:upper90 institution read; expect false user:2 institution:jrss institution read
# revocation and expiry are fresh
reset; expect true user:1 institution:ai-loss institution read
sqlite3 "$auth" "UPDATE institution_grants SET active=0 WHERE user_id=1"; expect false user:1 institution:ai-loss institution read
sqlite3 "$auth" "UPDATE institution_grants SET active=1,expires_at='2000-01-01T00:00:00Z' WHERE user_id=1"; expect false user:1 institution:ai-loss institution read
sqlite3 "$auth" 'UPDATE users SET active=0 WHERE id=1'; expect false user:1 user:1 self read
# household steward and current member role
reset; expect true user:1 household:home household write; expect true user:2 household:home household read
expect false user:2 household:home household write
sqlite3 "$cms" "UPDATE _household_membership SET role='write'"; expect true user:2 household:home household write
# household epoch and member expiry
reset; sqlite3 "$cms" 'UPDATE _household_authority SET epoch=8'; expect false user:2 household:home household read
sqlite3 "$cms" 'UPDATE _household_membership SET authority_epoch=8,expires_at=1'; expect false user:2 household:home household read
# a household cannot borrow institution membership
reset; sqlite3 "$auth" "INSERT INTO institution_grants VALUES(2,'home','admin',NULL,1)"; sqlite3 "$cms" 'DELETE FROM _household_membership'
expect false user:2 household:home household read; expect false user:1 institution:home institution read
# invalid bindings are refused with the native invalid reply
reset
for binding in 'user:01 user:01 self read' 'user:2147483648 user:2147483648 self read' 'user:1 user:2 self read' \
               'user:1 user:1 self admin' 'user:1 user:1 public read' 'user:1 household:home institution read'; do
  set -- $binding; call "$1" "$2" "$3" "$4"
  [ "$(field ok)|$(field body.code)|$(field body.message)" = 'false|invalid|The native background access check could not complete.' ] || fail "invalid binding $binding"
done
# an unavailable database is neither a deny nor a created database
reset; missing=$root/missing.db
call user:1 user:1 self read "$missing"; [ "$(field ok)" = false ] && [ ! -e "$missing" ] || fail "missing auth database"
call user:1 household:home household read "$auth" "$missing"; [ "$(field ok)" = false ] && [ ! -e "$missing" ] || fail "missing cms database"
# a read projection leaves schema and rows unchanged
reset; a0=$(shasum -a 256 "$auth" | cut -c1-64); c0=$(shasum -a 256 "$cms" | cut -c1-64)
expect true user:1 institution:ai-loss institution read; expect true user:1 household:home household read
[ "$(shasum -a 256 "$auth" | cut -c1-64)|$(shasum -a 256 "$cms" | cut -c1-64)" = "$a0|$c0" ] || fail "read projection changed a database"
echo "background access: PASS ($(wc -l < "$root/count" | tr -d ' ') native checks)"
