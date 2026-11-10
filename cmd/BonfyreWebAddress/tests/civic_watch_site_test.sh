#!/bin/sh
# Who Watches the Road, end to end: fixture receipts -> civic-ledger -> the
# documented `site` verbs -> a served site, read back over HTTP.
#
#   BONFYRE_TEST_WEB_BINARY=/path/to/bonfyre-web-address sh tests/civic_watch_site_test.sh
#
# The receipts are made-up fixtures in the shapes Overpass and Legistar answer
# with. Everything lands in a temporary site home and is removed afterwards.
set -eu
HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../../.." && pwd)
WEB=${BONFYRE_TEST_WEB_BINARY:-$ROOT/cmd/BonfyreWebAddress/bonfyre-web-address}
T=$(mktemp -d "${TMPDIR:-/tmp}/civic-watch.XXXXXX")
PORT=${CIVIC_TEST_PORT:-$((29100 + $$ % 700))}
HOST=watch.example.test
cleanup() { "$WEB" site stop --home "$T/home" >/dev/null 2>&1 || true; rm -rf "$T"; }
trap cleanup 0 1 2 15
pass=0 fail=0
ok() { pass=$((pass+1)); }
no() { echo "FAIL: $1" >&2; fail=$((fail+1)); }

cc -O2 -Wall -Wextra -Werror -std=c11 -D_DARWIN_C_SOURCE -I"$ROOT/lib/libbonfyre/include" -o "$T/civic-ledger" \
  "$ROOT/cmd/BonfyreSourceFrontier/tools/civic_ledger.c" "$ROOT/lib/libbonfyre/bf_json.c" "$ROOT/lib/libbonfyre/src/bf_sha256.c"

mkdir -p "$T/obs"
receipt() { # name status
  sha=$(shasum -a 256 "$T/obs/$1.body" | cut -d' ' -f1)
  printf '{"schema":"bonfyre-foreign-observation.v1","twin_id":"fixture","source_url":"https://fixture.example.test/%s","observed_at":"2026-10-30T01:02:03Z","policy_decision":"allow","http_status":%s,"content_sha256":"%s","body_path":"%s.body"}' "$1" "$2" "$sha" "$1" > "$T/obs/$1.receipt.json"
}
cat > "$T/obs/towns.body" <<'EOF'
{"osm3s":{"copyright":"The data included in this document is from www.openstreetmap.org. The data is made available under ODbL."},
"elements":[{"type":"city","id":1,"tags":{"county":"Dallas County","name":"Fixtureville","cameras":"41"}},
{"type":"city","id":2,"tags":{"county":"Dallas County","name":"Sampleton","cameras":"7"}},
{"type":"county","id":3,"tags":{"name":"Dallas County","cameras":"48"}}]}
EOF
receipt towns 200
cat > "$T/obs/council-cityofdallas.body" <<'EOF'
[{"MatterId":101,"MatterGuid":"ABCD-0001","MatterFile":"26-100","MatterStatusName":"Approved","MatterIntroDate":"2026-04-01T00:00:00","MatterTitle":"Fixture contract for automatic license plate reading cameras\r\n[Presenter]"},
 {"MatterId":102,"MatterGuid":"ABCD-0002","MatterFile":"24-200","MatterStatusName":"Approved","MatterIntroDate":"2024-04-01T00:00:00","MatterTitle":"Fixture ordinance on expired license plates"}]
EOF
receipt council-cityofdallas 200
printf 'error' > "$T/obs/council-irving.body"; receipt council-irving 500

BONFYRE_TEST_WEB_BINARY="$WEB" sh "$ROOT/docs/experiences/civic-transparency/build.sh" \
  --home "$T/home" --obs "$T/obs" --ledger "$T/civic-ledger" --host "$HOST" --port "$PORT" > "$T/build.out" 2>&1 \
  && ok || { no "build"; cat "$T/build.out" >&2; }
# Building again keeps one copy of everything.
BONFYRE_TEST_WEB_BINARY="$WEB" sh "$ROOT/docs/experiences/civic-transparency/build.sh" \
  --home "$T/home" --obs "$T/obs" --ledger "$T/civic-ledger" --host "$HOST" --port "$PORT" >/dev/null 2>&1 && ok || no "rebuild"
[ "$(sqlite3 -readonly "$T/home/cms.db" "select count(*) from site_page where namespace='civic-watch'")" = 6 ] && ok || no "six pages after two builds"
[ "$(sqlite3 -readonly "$T/home/cms.db" "select count(*) from community_post where title='What this site holds' and status!='withdrawn'")" = 1 ] && ok || no "one welcome post"

"$WEB" site stop --home "$T/home" >/dev/null 2>&1 || true
"$WEB" site serve --home "$T/home" --port "$PORT" >/dev/null 2>&1 || true
i=0; until curl -s -o /dev/null "http://127.0.0.1:$PORT/" || [ $i -gt 40 ]; do sleep 0.25; i=$((i+1)); done
get() { curl -s -H "Accept: text/html" -H "Host: $HOST" "http://127.0.0.1:$PORT$1"; }
code() { curl -s -o /dev/null -w '%{http_code}' -H "Accept: text/html" -H "Host: $HOST" "http://127.0.0.1:$PORT$1"; }
for r in / /cameras /council-votes /ask /search-logs /sources /fix /contact; do
  [ "$(code "$r")" = 200 ] && ok || no "$r answers 200"
done
home=$(get /)
echo "$home" | grep -q 'What this site holds' && ok || no "welcome post on the front"
echo "$home" | grep -q 'This site is live' && no "starter post still shown" || ok
echo "$home" | grep -q 'data-template="ghost:civic-watch"' && ok || no "site program serves the home"
echo "$home" | grep -q -- '--bf-accent:#f2c230' && ok || no "civic presentation applied"
for r in /cameras /council-votes /ask /search-logs /sources /fix; do
  echo "$home" | grep -q "href=\"$r\"" && ok || no "front links $r"
done

cams=$(get /cameras)
echo "$cams" | grep -q 'Fixtureville' && echo "$cams" | grep -q '>41<' && ok || no "town count served"
echo "$cams" | grep -q 'Open Database License' && ok || no "ODbL attribution served"
echo "$cams" | grep -q 'A city has not confirmed them' && ok || no "confidence stated"

council=$(get /council-votes)
echo "$council" | grep -q 'Fixture contract for automatic license plate reading cameras' && ok || no "council item served"
echo "$council" | grep -q 'https://cityofdallas.legistar.com/LegislationDetail.aspx?ID=101&amp;GUID=ABCD-0001' && ok || no "record link served"
echo "$council" | grep -q 'expired license plates' && no "registration ordinance served" || ok
echo "$council" | grep -q 'Presenter' && no "presenter line served" || ok
echo "$council" | grep -q 'Irving' && no "failed read listed as a place" || ok

src=$(get /sources)
echo "$src" | grep -q 'Irving council agendas' && echo "$src" | grep -q 'answered with an error' && ok || no "failed read disclosed on sources"
echo "$src" | grep -q 'Read in full' && ok || no "good read disclosed"
get /ask | grep -q 'Texas Government Code, Chapter 552' && ok || no "request letter served"
get /ask | grep -q 'class="code-copy"' && ok || no "letter can be copied"

# A correction arrives through the contact page and reaches the owner's inbox, unpublished.
curl -s -o /dev/null -H "Host: $HOST" --data-urlencode "name=Fixture Resident" --data-urlencode "contact=resident@example.test" \
  --data-urlencode "message=Correction: council item 26-101 is missing" --data-urlencode "website=" --data-urlencode "about=" "http://127.0.0.1:$PORT/contact"
"$WEB" site inbox civic-watch --home "$T/home" | grep -q 'council item 26-101 is missing' && ok || no "correction reaches the inbox"
get / | grep -q 'Fixture Resident' && no "sender name published" || ok

# Safety and wording across every page a visitor can reach.
all=""
for r in / /cameras /council-votes /ask /search-logs /sources /fix; do all="$all
$(get "$r" | sed -e 's/<script[^>]*>[^<]*<\/script>//g' -e 's/<[^>]*>/ /g')"; done
echo "$all" | grep -Eq -- '-?[0-9]{1,3}\.[0-9]{4,}' && no "a coordinate-like number is visible" || ok
echo "$all" | grep -Eiq '\b(receipt|observed|closure|candidate|namespace|sha256|twin)\b' && no "machine vocabulary visible: $(echo "$all" | grep -Eio '\b(receipt|observed|closure|candidate|namespace|sha256|twin)\b' | sort -u | tr '\n' ' ')" || ok
echo "$all" | grep -qi 'have i been' && no "borrowed product name" || ok

echo "civic watch site: $pass passed, $fail failed"
[ "$fail" = 0 ]
