#!/bin/sh
# Guild, care-circle, pantry, charter and field-registry records are written by
# a person's own form submit and read back from the CMS store; a stranger can
# neither write them nor (for private circles and pantries) read them.
#
# Owners: src/guild_records_web.h, src/household_circles_web.h,
# src/charters_web.h. A guild is created through the existing community owner
# (/community/action create-guild); one member row is added to the temp store
# as fixture so a second account can be confirmed by the steward.
set -eu

bin="$1"
case "$bin" in /*) ;; *) bin="$(pwd)/$bin" ;; esac
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
auth="$root/cmd/BonfyreAuth/bonfyre-auth"
cms="$root/cmd/BonfyreCMS/bonfyre-cms"
lex="$root/cmd/BonfyreLex/bonfyre-lex"
schemas="$root/cmd/BonfyreCMS/content-types"
for x in "$auth" "$cms" "$lex"; do [ -x "$x" ] || { echo "guild records: SKIP ($x not built)"; exit 0; }; done

tmp=$(mktemp -d)
pid=''
cleanup() { [ -n "$pid" ] && kill "$pid" 2>/dev/null || true; rm -rf "$tmp"; }
trap cleanup EXIT INT TERM

db="$tmp/matrix.db"; adb="$tmp/auth.db"; cdb="$tmp/cms.db"
port=18931
H='Host: aurekai.life'; O='Origin: https://aurekai.life'
base="http://127.0.0.1:$port"
"$bin" bootstrap-owned --db "$db" >/dev/null
"$cms" schema migrate --db "$cdb" --schemas "$schemas" >/dev/null 2>&1

BONFYRE_AUTH_BINARY="$auth" BONFYRE_CMS_DB="$cdb" BONFYRE_CMS_BINARY="$cms" BONFYRE_CMS_SCHEMAS="$schemas" \
  "$bin" serve --db "$db" --auth-db "$adb" --port "$port" --site-root "$root/site/matrix" >"$tmp/server.log" 2>&1 &
pid=$!
i=0; until curl -s -o /dev/null -m 1 "$base/"; do i=$((i+1)); [ $i -lt 50 ] || { cat "$tmp/server.log"; exit 1; }; sleep 0.2; done

pw='a-long-enough-passphrase'
session() {
  curl -fsS -X POST -H "$H" -d "{\"email\":\"$1\",\"name\":\"$2\",\"password\":\"$pw\"}" "$base/signup" >/dev/null
  curl -fsS -X POST -H "$H" -d "{\"email\":\"$1\",\"password\":\"$pw\"}" "$base/login" | "$lex" field --kind json_string --key session.token
}
one=$(session 'one@t.test' 'One'); two=$(session 'two@t.test' 'Two'); three=$(session 'three@t.test' 'Three')
[ -n "$one" ] && [ -n "$two" ] && [ -n "$three" ] || { echo "no sessions"; exit 1; }
uid() { sqlite3 "$adb" "SELECT id FROM users WHERE email='$1'"; }
u1="user:$(uid one@t.test)"; u2="user:$(uid two@t.test)"

n=0
nonce() { n=$((n+1)); printf '%032x' "$n"; }
# post COOKIE PATH BODY -> prints status
post() { curl -sS -o "$tmp/r" -w '%{http_code}' -X POST -H "$H" -H "$O" -H "Cookie: bonfyre_session=$1" \
  -H 'Content-Type: application/x-www-form-urlencoded' --data "$3" "$base$2"; }
get() { curl -sS -o "$tmp/g" -w '%{http_code}' -H "$H" -H 'Accept: text/html' ${1:+-H "Cookie: bonfyre_session=$1"} "$base$2"; }
want() { [ "$1" = "$2" ] || { echo "FAIL $3: expected $2, got $1: $(head -c 300 "$tmp/r" 2>/dev/null)"; exit 1; }; }
has() { grep -q "$1" "$tmp/g" || { echo "FAIL page lacks: $1"; exit 1; }; }

# A guild through the existing community owner.
want "$(post "$one" /community/action "operation=create-guild&community=&target=&expected=&request=$(nonce)&title=Bench+Guild&text=Joinery+and+lathe+work")" 303 "create guild"
g=$(sqlite3 "$cdb" "SELECT community FROM community_body WHERE governance_scope='local-guild' LIMIT 1")
[ -n "$g" ] || { echo "no guild"; exit 1; }
"$cms" entry create community_membership "{\"membership\":\"fixture-m2\",\"community\":\"$g\",\"member_subject\":\"$u2\",\"role\":\"member\",\"state\":\"active\",\"cannot_infer\":\"fixture\",\"status\":\"published\"}" --ns root --db "$cdb" --schemas "$schemas" >/dev/null 2>&1

# Curriculum: steward writes a module; a non-steward cannot.
want "$(post "$one" "/guild/$g/act" "op=module-add&request=$(nonce)&level=1&title=Shop+habits&text=Sharpening+and+safety&hours=40")" 303 "module add"
want "$(post "$three" "/guild/$g/act" "op=module-add&request=$(nonce)&level=2&title=Stranger&text=x&hours=1")" 403 "stranger module"
want "$(post "" "/guild/$g/act" "op=module-add&request=$(nonce)&level=2&title=Anon&text=x")" 401 "anonymous module"
m=$(sqlite3 "$cdb" "SELECT module FROM guild_module WHERE community='$g'")
# Hours: the apprentice logs, a stranger cannot confirm, the steward confirms.
want "$(post "$two" "/guild/$g/act" "op=hours-log&request=$(nonce)&module=$m&date=2026-11-10&hours=6.5&title=Honing+chisels")" 303 "hours log"
want "$(post "$three" "/guild/$g/act" "op=hours-log&request=$(nonce)&module=$m&date=2026-11-10&hours=2&title=x")" 403 "stranger hours"
h=$(sqlite3 "$cdb" "SELECT entry FROM guild_hours WHERE community='$g'")
want "$(post "$three" "/guild/$g/act" "op=hours-confirm&request=$(nonce)&target=$h")" 403 "stranger confirm"
want "$(post "$two" "/guild/$g/act" "op=hours-confirm&request=$(nonce)&target=$h")" 403 "self confirm"
want "$(post "$one" "/guild/$g/act" "op=hours-confirm&request=$(nonce)&target=$h")" 303 "steward confirm"
[ "$(sqlite3 "$cdb" "SELECT state||'|'||confirmed_by FROM guild_hours WHERE entry='$h'")" = "confirmed|$u1" ] || { echo "hours not confirmed by steward"; exit 1; }
want "$(post "$one" "/guild/$g/act" "op=hours-confirm&request=$(nonce)&target=$h")" 409 "double confirm"
# Bench proof and written sign-off.
want "$(post "$two" "/guild/$g/act" "op=proof-submit&request=$(nonce)&module=$m&title=Dovetail+box&url=https%3A%2F%2Fexample.org%2Fbox&text=Hand+cut")" 303 "proof"
p=$(sqlite3 "$cdb" "SELECT proof FROM guild_bench_proof WHERE community='$g'")
want "$(get "$three" "/guild/$g/proof/$p")" 403 "stranger reads proof"
want "$(post "$one" "/guild/$g/act" "op=proof-decide&request=$(nonce)&target=$p&kind=sign&text=")" 400 "sign-off needs words"
want "$(post "$one" "/guild/$g/act" "op=proof-decide&request=$(nonce)&target=$p&kind=sign&text=Tight+joints%2C+square+box")" 303 "sign off"
want "$(get "$two" "/guild/$g/proof/$p")" 200 "apprentice reads proof"; has 'Tight joints, square box'
want "$(get "$two" "/guild/$g/bench")" 200 "bench"; has 'Honing chisels'; has 'Signed off'
want "$(get "" "/guild/$g/")" 200 "curriculum"; has 'Shop habits'; has 'Living syllabus'
# Attestation is explicit and revocable by the steward only.
want "$(post "$one" "/guild/$g/act" "op=attest&request=$(nonce)&member=$u2&title=Hand+joinery&text=Cut+three+dovetails")" 303 "attest"
a=$(sqlite3 "$cdb" "SELECT attestation FROM guild_attestation WHERE community='$g'")
want "$(post "$two" "/guild/$g/act" "op=attest-revoke&request=$(nonce)&target=$a&text=no")" 403 "member revoke"
want "$(post "$one" "/guild/$g/act" "op=attest-revoke&request=$(nonce)&target=$a&text=Re-test+due")" 303 "revoke"
[ "$(sqlite3 "$cdb" "SELECT state FROM guild_attestation WHERE attestation='$a'")" = revoked ] || { echo "not revoked"; exit 1; }
# Offers: steward posts, anyone signed in applies, steward decides; no money table is touched.
want "$(post "$one" "/guild/$g/act" "op=offer-post&request=$(nonce)&kind=apprenticeship&title=Spring+bench&text=Two+days+a+week&value=&place=&closes=")" 303 "offer"
o=$(sqlite3 "$cdb" "SELECT offer FROM guild_offer WHERE community='$g'")
want "$(post "$three" "/guild/$g/act" "op=apply&request=$(nonce)&target=$o&text=I+turn+bowls")" 303 "apply"
want "$(post "$three" "/guild/$g/act" "op=apply&request=$(nonce)&target=$o&text=again")" 409 "duplicate apply"
ap=$(sqlite3 "$cdb" "SELECT application FROM guild_offer_application WHERE offer='$o'")
want "$(post "$three" "/guild/$g/act" "op=apply-decide&request=$(nonce)&target=$ap&kind=accept&text=me")" 403 "applicant decides"
want "$(post "$one" "/guild/$g/act" "op=apply-decide&request=$(nonce)&target=$ap&kind=accept&text=Welcome")" 303 "decide"
want "$(get "" /guild/openings)" 200 "openings"; has 'Spring bench'
# Field registry: members write, strangers cannot.
want "$(post "$two" "/guild/$g/act" "op=observe&request=$(nonce)&title=Ash+dieback&text=Three+trees&url=https%3A%2F%2Fexample.org%2Fash&date=2026-11-09&place=")" 303 "observe"
want "$(post "$three" "/guild/$g/act" "op=observe&request=$(nonce)&title=x&text=y&url=https%3A%2F%2Fexample.org&date=2026-11-09&place=")" 403 "stranger observe"
want "$(get "" "/guild/$g/registry")" 200 "registry"; has 'Ash dieback'

# Charters: members write and revise; revisions come from the store's log.
want "$(post "$one" /charters/act "op=write&request=$(nonce)&community=$g&title=Shop+rules&text=%23+Safety%0AGuards+stay+on.&note=First")" 303 "charter"
r=$(sqlite3 "$cdb" "SELECT route FROM wiki_page WHERE community='$g'")
want "$(post "$three" /charters/act "op=revise&request=$(nonce)&community=$g&target=$r&title=Mine&text=x&note=x")" 403 "stranger revise"
want "$(post "$two" /charters/act "op=revise&request=$(nonce)&community=$g&target=$r&title=Shop+rules&text=%23+Safety%0AGuards+stay+on.%0ASweep+daily.&note=Added+sweeping")" 303 "revise"
want "$(get "" "/$r")" 200 "charter page"; has 'Sweep daily.'; has 'Added sweeping'; has '2 revisions'
want "$(get "" /charters)" 200 "charter index"; has 'Shop rules'

# Care circle: private to its members, joined only through the steward's link.
want "$(post "$one" /care/act "op=create&request=$(nonce)&title=Gran+care&text=Meals+and+visits")" 303 "care create"
c=$(sqlite3 "$cdb" "SELECT community FROM community_body WHERE namespace='care'")
want "$(get "$two" "/care/$c")" 404 "outsider reads circle"
want "$(post "$two" "/care/$c/act" "op=add&request=$(nonce)&kind=note&title=x&text=y&date=")" 403 "outsider writes circle"
want "$(post "$one" "/care/$c/act" "op=add&request=$(nonce)&kind=task&title=Soup+on+Tuesday&text=&date=2026-11-17")" 303 "task"
want "$(post "$one" "/care/$c/act" "op=invite&request=$(nonce)")" 303 "invite"
t=$(sqlite3 "$cdb" "SELECT substr(member_subject,8) FROM community_membership WHERE namespace='care' AND state='invited'")
want "$(get "" /communities)" 200 "directory"; ! grep -q 'Gran care' "$tmp/g" || { echo "private circle listed publicly"; exit 1; }
want "$(post "$two" "/care/$c/act" "op=accept&request=$(nonce)&target=$t")" 303 "accept invite"
want "$(get "$two" "/care/$c")" 200 "member reads circle"; has 'Soup on Tuesday'
want "$(post "$three" "/care/$c/act" "op=accept&request=$(nonce)&target=$t")" 404 "reused invite"

# Pantry: a household's own calendar.
want "$(post "$one" /pantry/act "op=household-open&request=$(nonce)&title=Hill+house")" 303 "household"
hh=$(sqlite3 "$cdb" "SELECT household_id FROM household LIMIT 1")
want "$(post "$one" /pantry/act "op=entry&request=$(nonce)&household=$hh&kind=batch&title=Crab+apple+jelly&date=2026-11-08&value=6+jars&place=Cellar&text=")" 303 "pantry entry"
want "$(get "$one" "/pantry?household=$hh")" 200 "pantry"; has 'Crab apple jelly'
want "$(post "$two" /pantry/act "op=entry&request=$(nonce)&household=$hh&kind=batch&title=x&date=2026-11-08&value=&place=&text=")" 403 "stranger pantry"
want "$(get "$two" "/pantry?household=$hh")" 200 "stranger pantry page"; ! grep -q 'Crab apple jelly' "$tmp/g" || { echo "pantry leaked"; exit 1; }

# Cross-site and session checks.
code=$(curl -sS -o /dev/null -w '%{http_code}' -X POST -H "$H" -H 'Origin: https://evil.example' -H "Cookie: bonfyre_session=$one" \
  -H 'Content-Type: application/x-www-form-urlencoded' --data "op=module-add&request=$(nonce)&level=3&title=x&text=y" "$base/guild/$g/act")
want "$code" 403 "foreign origin"
echo "guild household records: ok"
