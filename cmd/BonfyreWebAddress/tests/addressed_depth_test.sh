#!/bin/sh
# An item page opens onto what the estate holds about it, from an empty
# directory: the piece's own lead from its retained entry, its parts, the
# organizations it links resolved to the addresses this estate gives them, a
# platform link left out, an unknown site kept as the publisher's own link,
# every channel its publisher has, and the publisher named and linked in the
# byline. Nothing is filled from unrelated material, and an address of this
# estate opens in place.
set -eu
web=${1:?web-address binary required}
case "$web" in /*) ;; *) web="$(pwd)/$web" ;; esac
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
cms="$root/cmd/BonfyreCMS/bonfyre-cms"
[ -x "$cms" ] || { echo 'addressed depth: SKIP (cms not built)'; exit 0; }
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-depth.XXXXXX")
pid=''
cleanup() { [ -n "$pid" ] && kill "$pid" 2>/dev/null || true; rm -rf "$tmp"; }
trap cleanup EXIT INT TERM
fail() { echo "addressed depth: $1" >&2; exit 1; }

schemas="$tmp/schemas"; mkdir -p "$schemas" "$tmp/foreign/receipts" "$tmp/foreign/bodies"
for type in world world_fragment internet_source; do cp "$root/cmd/BonfyreCMS/content-types/$type.json" "$schemas/"; done
"$cms" schema migrate --db "$tmp/cms.db" --schemas "$schemas" >/dev/null 2>&1
"$cms" entry create internet_source '{"family":"news","source":"feed-lab-test","label":"Lab Institute","says":"what Lab Institute publishes","url":"https://lab.test/rss.xml","twin":"feed-lab-test-twin","state":"observed","extract":"rss:item","title_field":"title","meta_field":"","link_field":"feed:auto","image_field":"","image_base":"","body_regime":"retained-public","status":"published","tier":"warm","kind":"text","cadence_seconds":1800}' \
  --ns root --db "$tmp/cms.db" --schemas "$schemas" >/dev/null 2>&1
printf '{"body_path":"%s"}\n' "$tmp/foreign/bodies/feed-lab-test.body" >"$tmp/foreign/receipts/feed-lab-test-twin.json"
# A structured source whose records share one landing page: its key field
# keeps them apart, and each opens as its own record.
"$cms" entry create internet_source '{"family":"news","source":"stations-test","label":"Stations","says":"weather stations","url":"https://json.test/api","twin":"stations-test-twin","state":"observed","extract":"data","title_field":"name","meta_field":"","link_field":"url","key_field":"id","image_field":"","image_base":"","body_regime":"retained-public","status":"published","tier":"warm","kind":"text"}' \
  --ns root --db "$tmp/cms.db" --schemas "$schemas" >/dev/null 2>&1
printf '{"body_path":"%s"}\n' "$tmp/foreign/bodies/stations-test.body" >"$tmp/foreign/receipts/stations-test-twin.json"
cat >"$tmp/foreign/bodies/stations-test.body" <<'JSON'
{"data":[{"id":"a1","name":"Station North","url":"https://json.test/stations","temp_c":12.5,"status":"open","owner":{"name":"River City"}},
 {"id":"b2","name":"Station South","url":"https://json.test/stations","temp_c":9,"status":"closed","owner":{"name":"Lake County"}}]}
JSON
when=$(python3 -c 'import email.utils,time; print(email.utils.formatdate(time.time()-3600, usegmt=True))')
cat >"$tmp/foreign/bodies/feed-lab-test.body" <<XML
<?xml version="1.0"?><rss><channel><title>Lab Institute</title>
<item><title>New atlas of river proteins</title><link>https://lab.test/news/river-proteins</link><pubDate>$when</pubDate>
<description>&lt;p&gt;New atlas of river proteins&lt;/p&gt;&lt;p&gt;Researchers can now study river proteins at a scale that was not possible before, across nine basins and four seasons.&lt;/p&gt;
&lt;h3&gt;How the atlas was made&lt;/h3&gt;&lt;p&gt;Samples were taken by volunteers and sequenced with &lt;a href="https://partner.test/sequencing"&gt;Partner sequencing&lt;/a&gt; over two years of field work.&lt;/p&gt;
&lt;h3&gt;What comes next&lt;/h3&gt;&lt;p&gt;The atlas is open, and a second release with &lt;a href="https://unknown.example/thing"&gt;Unknown Consortium&lt;/a&gt; is planned for spring next year.&lt;/p&gt;
&lt;p&gt;Watch the &lt;a href="https://www.youtube.com/watch?v=abc"&gt;launch video&lt;/a&gt; from the opening event.&lt;/p&gt;</description></item>
</channel></rss>
XML
python3 - "$tmp/frontier.db" <<'PY'
import sqlite3, sys
db = sqlite3.connect(sys.argv[1])
db.executescript("""
CREATE TABLE root(root_id TEXT PRIMARY KEY, host TEXT NOT NULL, name TEXT DEFAULT '', identity TEXT DEFAULT '', kind TEXT DEFAULT '', robots TEXT DEFAULT 'unknown', economic TEXT DEFAULT '', first_seen TEXT, last_seen TEXT);
CREATE TABLE family(family_id TEXT PRIMARY KEY, root_id TEXT, protocol TEXT);
CREATE TABLE channel(channel_id TEXT PRIMARY KEY, family_id TEXT NOT NULL, root_id TEXT NOT NULL, address TEXT NOT NULL, exact_address TEXT NOT NULL, title TEXT DEFAULT '', material TEXT DEFAULT '', protocol TEXT DEFAULT 'unknown', state TEXT NOT NULL DEFAULT 'candidate', truth_level TEXT DEFAULT 'listed', refusal TEXT DEFAULT '', virtual INTEGER DEFAULT 0, census_source TEXT DEFAULT '', credential TEXT DEFAULT '', rights TEXT DEFAULT '', cadence TEXT DEFAULT '', discovered_from TEXT DEFAULT '', body_sha256 TEXT DEFAULT '', identity TEXT DEFAULT '', first_seen TEXT, last_seen TEXT, last_contributed TEXT DEFAULT '', contributions INTEGER DEFAULT 0, carries TEXT DEFAULT '');
INSERT INTO root(root_id,host,name,identity) VALUES ('r-lab','lab.test','','Q901'),('r-partner','partner.test','Partner Org','Q902');
INSERT INTO channel(channel_id,family_id,root_id,address,exact_address,title,material,protocol,state,census_source,discovered_from,identity,last_seen)
  VALUES ('c1','f1','r-lab','https://lab.test/rss.xml','https://lab.test/rss.xml','Lab Institute','news','rss','admitted','feed-lab-test','lab.test publishes an RSS feed','Q901','2026-10-01T00:00:00Z'),
         ('c2','f2','r-lab','https://lab.test/','https://lab.test/','Lab Institute','','unknown','candidate','','Wikidata records https://lab.test/ as the official website','Q901','2026-10-01T00:00:00Z');
""")
db.commit()
PY
matrix="$tmp/matrix.db"
"$web" institution --db "$matrix" --id inst:lab-test --kind publisher --world live --evidence fixture >/dev/null
"$web" domain --db "$matrix" --host lab-test.aurekai.life --institution inst:lab-test --territory life --scope public --world live --repr HTML --evidence fixture >/dev/null
"$web" institution --db "$matrix" --id inst:partner-test --kind publisher --world live --evidence fixture >/dev/null
"$web" domain --db "$matrix" --host partner-test.aurekai.life --institution inst:partner-test --territory life --scope public --world live --repr HTML --evidence fixture >/dev/null

port=$(python3 -c "import socket; s=socket.socket(); s.bind(('127.0.0.1',0)); print(s.getsockname()[1]); s.close()")
BONFYRE_CMS_BINARY="$cms" BONFYRE_CMS_DB="$tmp/cms.db" BONFYRE_CMS_SCHEMAS="$schemas" BONFYRE_FOREIGN_ROOT="$tmp/foreign" \
BONFYRE_SOURCE_FRONTIER_DB="$tmp/frontier.db" \
  "$web" serve --db "$matrix" --auth-db "$tmp/auth.db" --port "$port" --site-root "$root/site/matrix" >"$tmp/server.log" 2>&1 &
pid=$!
attempt=0
until curl -sS -o /dev/null "http://127.0.0.1:$port/healthz" 2>/dev/null; do
  attempt=$((attempt + 1)); [ "$attempt" -lt 80 ] && kill -0 "$pid" 2>/dev/null || { cat "$tmp/server.log" >&2; fail 'server did not start'; }
  sleep 0.1
done
key=$(python3 -c "import hashlib; print(hashlib.sha256(b'https://lab.test/news/river-proteins').hexdigest()[:24])")
page=$(curl -sS -H 'Host: aurekai.live' -H 'Accept: text/html' "http://127.0.0.1:$port/discover" | grep -o '/o/feed-lab-test/[0-9a-f]\{24\}' | head -1)
[ -n "$page" ] || page="/o/feed-lab-test/$key"
curl -sS -H 'Host: aurekai.live' -H 'Accept: text/html' -o "$tmp/item.html" -w '%{http_code}' "http://127.0.0.1:$port$page" | grep -q 200 || fail "item page $page did not answer"
python3 - "$tmp/item.html" <<'PY' || fail 'item page did not carry its depth'
import re, sys
s = open(sys.argv[1]).read()
def region(role):
    i = s.find('region-' + role + '"')
    return s[i:s.find('</section>', i)] if i >= 0 else ''
checks = {
    'the lead is the publisher\'s own, without the repeated title': 'Researchers can now study river proteins' in s and '<p class="thing-body">New atlas' not in s,
    'the byline names the publisher and opens its address here': re.search(r'class="by"><span><a href="https://lab-test\.aurekai\.life/">Lab Institute</a>', s) is not None,
    'the parts of the piece are a region': 'How the atlas was made' in region('parts') and 'What comes next' in region('parts'),
    'a named organization opens its estate address': 'href="https://partner-test.aurekai.life/"' in region('named') and 'Partner Org' in region('named'),
    'an organization the estate does not address keeps the publisher\'s link': 'https://unknown.example/thing' in region('named'),
    'a platform link is not an organization': 'youtube' not in region('named'),
    'every channel of the publisher is listed': 'News RSS feed' in region('channels') and 'Website' in region('channels'),
    'nothing unrelated fills the page': 'Also in' not in s,
    'an estate address opens in place': not re.search(r'target="_blank"[^>]*href="https://[a-z-]+\.aurekai\.', s) and not re.search(r'href="https://[a-z-]+\.aurekai\.[a-z]+/"[^>]*target="_blank"', s),
}
bad = [name for name, ok in checks.items() if not ok]
for name in bad: print('missing:', name, file=sys.stderr)
sys.exit(1 if bad else 0)
PY
for id in a1 b2; do
  k=$(python3 -c "import hashlib,sys; print(hashlib.sha256(('stations-test\n#'+sys.argv[1]).encode()).hexdigest()[:24])" $id)
  curl -sS -H 'Host: aurekai.live' -H 'Accept: text/html' -o "$tmp/$id.html" -w '%{http_code}' "http://127.0.0.1:$port/o/stations-test/$k" | grep -q 200 || fail "record $id did not open at its own key"
done
grep -q 'Station North' "$tmp/a1.html" && grep -q 'River City' "$tmp/a1.html" && grep -q '12.5' "$tmp/a1.html" || fail 'the first record did not read as its own facts'
grep -q 'Station South' "$tmp/b2.html" && grep -q 'Lake County' "$tmp/b2.html" && ! grep -q 'River City' "$tmp/b2.html" || fail 'a record sharing a landing page read as another record'
grep -q '<dt>Owner name</dt>' "$tmp/a1.html" || fail 'a nested field is not named in words'
echo 'addressed depth: PASS'
