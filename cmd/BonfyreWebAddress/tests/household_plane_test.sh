#!/bin/sh
# A granted plane belongs to one household, and a public plane is counted by
# what its address is.
#
# Two things this covers, both of which were once only shapes on a page. First:
# planes were counted from the frontier's protocol column, so a meeting packet
# published as a PDF was counted as a page and every institution appeared to
# publish no documents. Second: nothing anywhere connected a granted plane, so
# the room could name a roster, say it answers only to a relationship, and mean
# nothing by it -- no household had established a relationship with any
# institution, and there was no way for one to.
#
# The scoping is the part worth protecting: a connection makes a plane readable
# for the household that established it and for nobody else, including a
# signed-out reader of the same page and another signed-in household.
set -eu

bin="$1"
case "$bin" in /*) ;; *) bin="$(pwd)/$bin" ;; esac
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
auth="$root/cmd/BonfyreAuth/bonfyre-auth"
cms="$root/cmd/BonfyreCMS/bonfyre-cms"
lex="$root/cmd/BonfyreLex/bonfyre-lex"
[ -x "$auth" ] || { echo "household plane: SKIP (bonfyre-auth not built)"; exit 0; }
[ -x "$cms" ] || { echo "household plane: SKIP (bonfyre-cms not built)"; exit 0; }
[ -x "$lex" ] || { echo "household plane: SKIP (bonfyre-lex not built)"; exit 0; }

tmp=$(mktemp -d)
pid=''
cleanup() { [ -n "$pid" ] && kill "$pid" 2>/dev/null || true; rm -rf "$tmp"; }
trap cleanup EXIT INT TERM

db="$tmp/matrix.db"; adb="$tmp/auth.db"; cdb="$tmp/cms.db"; fdb="$tmp/frontier.db"
port=18897
H='Host: aurekai.life'
base="http://127.0.0.1:$port"

"$bin" subject --db "$db" --id object:plane:probe --kind PlantPassport \
  --digest aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --evidence receipt:plane >/dev/null
"$bin" bootstrap-owned --db "$db" >/dev/null
"$cms" schema migrate --db "$cdb" --schemas "$root/cmd/BonfyreCMS/content-types" >/dev/null

# One institution whose addresses cover every public plane, including the two
# shapes that used to be misread: a packet published as a PDF, and a feed whose
# path says "agenda".
sqlite3 "$fdb" <<'SQL'
CREATE TABLE root(root_id TEXT PRIMARY KEY, host TEXT NOT NULL, name TEXT DEFAULT '', identity TEXT DEFAULT '', kind TEXT DEFAULT '', robots TEXT DEFAULT 'unknown', economic TEXT DEFAULT '', first_seen TEXT, last_seen TEXT);
CREATE TABLE channel(channel_id TEXT PRIMARY KEY, family_id TEXT NOT NULL, root_id TEXT NOT NULL, address TEXT NOT NULL, exact_address TEXT NOT NULL, protocol TEXT DEFAULT 'unknown', state TEXT NOT NULL DEFAULT 'candidate');
INSERT INTO root VALUES('r1','fixture-school.test','Fixture Unified School District','','','unknown','',NULL,NULL);
INSERT INTO root VALUES('r2','candidate-school.test','Candidate School','','','unknown','',NULL,NULL);
INSERT INTO channel VALUES
 ('c1','f','r1','a','https://fixture-school.test/','html','admitted'),
 ('c2','f','r1','a','https://fixture-school.test/news/feed','html','admitted'),
 ('c3','f','r1','a','https://fixture-school.test/agendas.rss','rss','admitted'),
 ('c4','f','r1','a','https://fixture-school.test/board/agenda-2026-09.pdf','html','admitted'),
 ('c5','f','r1','a','https://fixture-school.test/calendar/term.ics','html','admitted'),
 ('c6','f','r1','a','https://fixture-school.test/data/enrolment.csv','html','admitted'),
 ('c7','f','r1','a','https://docs.fixture-school.test/handbook','html','admitted'),
 ('c8','f','r2','a','https://candidate-school.test/','html','candidate');
SQL

BONFYRE_AUTH_BINARY="$auth" BONFYRE_CMS_DB="$cdb" BONFYRE_SOURCE_FRONTIER_DB="$fdb" \
  "$bin" serve --db "$db" --auth-db "$adb" --port "$port" \
  --site-root "$root/site/matrix" >"$tmp/server.log" 2>&1 &
pid=$!
sleep 1

pw='a-long-enough-passphrase'
session() {
  curl -fsS -X POST -H "$H" -d "{\"email\":\"$1\",\"name\":\"$2\",\"password\":\"$pw\"}" \
    "$base/signup" >/dev/null
  curl -fsS -X POST -H "$H" -d "{\"email\":\"$1\",\"password\":\"$pw\"}" "$base/login" \
    | "$lex" field --kind json_string --key session.token
}
one=$(session 'one@t.test' 'One')
two=$(session 'two@t.test' 'Two')
[ -n "$one" ] && [ -n "$two" ] || { echo "no sessions"; exit 1; }

room() { curl -fsS -H "$H" ${2:+-H "Cookie: bonfyre_session=$2"} "$base/s/institution/$1"; }

# A plane is counted by what the address is. docs.fixture-school.test is a
# hostname containing ".docs." and must not make the district publish a
# document; agendas.rss is a feed about agendas and not a document either.
room fixture-school.test >"$tmp/public.html"
grep -q 'Published document' "$tmp/public.html" || { echo "no document plane row"; exit 1; }
grep -q 'Published material already in this world' "$tmp/public.html" || { echo "no public material reader"; exit 1; }
grep -q 'https://fixture-school.test/agendas.rss' "$tmp/public.html" || { echo "admitted public material was not linked"; exit 1; }
python3 - "$tmp/public.html" <<'PY'
import re, sys
t = open(sys.argv[1]).read()
rows = dict((re.sub('<[^>]+>', '', l), a) for l, a in
            re.findall(r'desk-l">(.*?)</span><span class="desk-a">([^<]*)</span>', t))
def want(label, answer):
    got = rows.get(label)
    assert got == answer, "%s: expected %r, got %r" % (label, answer, got)
# One .pdf under /board/ is the one document. The .rss is a feed, and the
# handbook under a ".docs." hostname is a page.
want('Published document', 'reachable')
want('Published calendar', 'reachable')
want('News or notices feed', 'reachable')
want('Public page', 'reachable')
want('Open data catalogue', 'reachable')
# Nothing grants a plane yet, to anyone.
for granted in ('Roster exchange', 'Student information system',
                'Family messaging', 'Fees and payments'):
    want(granted, 'needs a relationship')
print("planes counted by address")
PY

# Every refusal, before anything is established.
refuse() {
  code=$(curl -sS -o "$tmp/r" -w '%{http_code}' "$@")
  [ "$code" = "$expect" ] || { echo "expected $expect, got $code: $(cat "$tmp/r")"; exit 1; }
}
expect=405 refuse -H "$H" "$base/govern/connect"
expect=401 refuse -X POST -H "$H" -d 'institution=fixture-school.test&plane=roster' "$base/govern/connect"
expect=400 refuse -X POST -H "$H" -H "Cookie: bonfyre_session=$one" -d 'institution=fixture-school.test' "$base/govern/connect"
expect=400 refuse -X POST -H "$H" -H "Cookie: bonfyre_session=$one" -d 'institution=fixture-school.test&plane=web-page' "$base/govern/connect"
expect=400 refuse -X POST -H "$H" -H "Cookie: bonfyre_session=$one" -d 'institution=fixture-school.test&plane=telepathy' "$base/govern/connect"
expect=403 refuse -X POST -H "$H" -H "Cookie: bonfyre_session=$one" -H 'Origin: https://elsewhere.example' -d 'institution=fixture-school.test&plane=roster' "$base/govern/connect"
# A hostname-shaped string cannot create an institution/household relationship.
# The public room and the mutation both start from the same admitted frontier.
expect=404 refuse -X POST -H "$H" -H "Cookie: bonfyre_session=$one" -d 'institution=invented-school.test&plane=roster' "$base/govern/connect"
expect=404 refuse -H "$H" "$base/s/institution/candidate-school.test"
expect=404 refuse -X POST -H "$H" -H "Cookie: bonfyre_session=$one" -d 'institution=candidate-school.test&plane=roster' "$base/govern/connect"
[ "$(sqlite3 "$cdb" "SELECT COUNT(*) FROM household")" = "0" ] || { echo "unadmitted institution opened a household"; exit 1; }

# A signed-in reader is offered the claim and holds nothing yet.
printf '%s' "$(room fixture-school.test "$one")" | grep -q 'This household holds it'

# One household establishes one plane.
expect=303 refuse -X POST -H "$H" -H "Cookie: bonfyre_session=$one" \
  -H "Origin: http://aurekai.life" \
  -d 'institution=fixture-school.test&plane=roster' "$base/govern/connect"
[ "$(sqlite3 "$cdb" "SELECT COUNT(*) FROM household")" = "1" ] || { echo "no household opened"; exit 1; }
[ "$(sqlite3 "$cdb" "SELECT state FROM institution_connection")" = "established" ] || { echo "not established"; exit 1; }

# It is theirs, and the release control is there because a relationship a
# household cannot end is not one it holds.
mine=$(room fixture-school.test "$one")
printf '%s' "$mine" | grep -q 'desk-a">yours' || { echo "holder does not see it"; exit 1; }
printf '%s' "$mine" | grep -q 'Release it' || { echo "no release control"; exit 1; }
# Holding the roster is not holding the gradebook.
[ "$(printf '%s' "$mine" | grep -c 'desk-a">yours')" = "1" ] || { echo "one connection granted more than one plane"; exit 1; }

# Nobody else sees it: not the other household, not a signed-out reader.
printf '%s' "$(room fixture-school.test "$two")" | grep -q 'desk-a">yours' \
  && { echo "another household sees this one's plane"; exit 1; }
printf '%s' "$(room fixture-school.test)" | grep -q 'desk-a">yours' \
  && { echo "a signed-out reader sees a granted plane"; exit 1; }

# Released by the same household, in one submission.
expect=303 refuse -X POST -H "$H" -H "Cookie: bonfyre_session=$one" \
  -d 'institution=fixture-school.test&plane=roster&release=1' "$base/govern/connect"
[ "$(sqlite3 "$cdb" "SELECT state FROM institution_connection")" = "released" ] || { echo "not released"; exit 1; }
printf '%s' "$(room fixture-school.test "$one")" | grep -q 'desk-a">yours' \
  && { echo "a released plane still reads as held"; exit 1; }

echo "household plane contract: PASS"
