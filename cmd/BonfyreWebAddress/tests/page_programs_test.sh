#!/bin/sh
# Main public routes are composed pages over the source families their program
# names, and the remaining native documents are readable.  This fixture proves
# /channels groups public threads by where they happen, /parcels plots the
# places sources give on a map before listing them, a family a program does not
# name never leaks in, the shared nav marks the current page, and a native
# document such as /inbox loads the native stylesheet with a human title.
set -eu

web=${1:?web-address binary required}
case "$web" in /*) ;; *) web="$(pwd)/$web" ;; esac
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
cms="$root/cmd/BonfyreCMS/bonfyre-cms"
[ -x "$cms" ] || { echo 'page programs: SKIP (cms not built)'; exit 0; }

tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-page-programs.XXXXXX")
pid=''
cleanup() {
  [ -n "$pid" ] && kill "$pid" 2>/dev/null || true
  [ -n "${PAGE_COMPOSITION_KEEP:-}" ] && cp "$tmp"/*.html "$PAGE_COMPOSITION_KEEP"/ 2>/dev/null || true
  rm -rf "$tmp"
}
trap cleanup EXIT INT TERM

schemas="$tmp/schemas"
mkdir -p "$schemas" "$tmp/foreign/receipts" "$tmp/foreign/bodies"
for type in world world_fragment internet_source publication_occurrence content_flag capability_program site_presentation site_program; do
  cp "$root/cmd/BonfyreCMS/content-types/$type.json" "$schemas/"
done
cmsdb="$tmp/cms.db"
"$cms" schema migrate --db "$cmsdb" --schemas "$schemas" >/dev/null
entry() { "$cms" entry create "$1" "$2" --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null; }
source_row() {
  entry internet_source "{\"family\":\"$2\",\"source\":\"$1\",\"label\":\"x\",\"says\":\"x\",\"url\":\"$3\",\"twin\":\"$1-twin\",\"state\":\"observed\",\"extract\":\"$4\",\"title_field\":\"$5\",\"meta_field\":\"$6\",\"link_field\":\"$7\",\"image_field\":\"\",\"image_base\":\"\",\"sub_field\":\"$8\",\"body_regime\":\"retained-public\",\"status\":\"published\",\"tier\":\"warm\",\"kind\":\"text\"}"
}
body() { printf '%s\n' "{\"body_path\":\"$tmp/foreign/bodies/$1.body\"}" >"$tmp/foreign/receipts/$1-twin.json"; cat >"$tmp/foreign/bodies/$1.body"; }

source_row lemmy forums https://lemmy.world/api/v3/post/list posts post.name post.published post.ap_id community.name
body lemmy <<'JSON'
{"posts":[
 {"post":{"name":"Rust 2.0 thoughts","published":"2026-09-16T10:00:00Z","ap_id":"https://lemmy.world/post/1"},"community":{"name":"rust"}},
 {"post":{"name":"Bike lanes in my city","published":"2026-09-16T09:00:00Z","ap_id":"https://lemmy.world/post/2"},"community":{"name":"urbanism"}}
]}
JSON
source_row hn hacker-attention https://hacker-news.firebaseio.com/v0/topstories.json hits title created_at url author
body hn <<'JSON'
{"hits":[{"title":"Show HN: A tiny database","created_at":"2026-09-16T11:00:00Z","url":"https://news.example.test/1","author":"pg"}]}
JSON
source_row quakes hazards https://earthquake.usgs.gov/feed.geojson features properties.place properties.updated properties.url ''
body quakes <<'JSON'
{"features":[
 {"properties":{"place":"10 km N of Anchorage, Alaska","updated":"2026-09-16T08:00:00Z","url":"https://quake.example.test/1"},"geometry":{"type":"Point","coordinates":[-149.9,61.3,10]}},
 {"properties":{"place":"5 km E of Tokyo, Japan","updated":"2026-09-16T07:00:00Z","url":"https://quake.example.test/2"},"geometry":{"type":"Point","coordinates":[139.8,35.7,40]}},
 {"properties":{"place":"Unknown spot","updated":"2026-09-16T06:00:00Z","url":"https://quake.example.test/3"},"geometry":{"type":"Point","coordinates":[0,0,0]}},
 {"properties":{"place":"Near Santiago, Chile","updated":"2026-09-16T05:00:00Z","url":"https://quake.example.test/4"},"geometry":{"type":"Point","coordinates":[-70.6,-33.4,30]}}
]}
JSON
source_row gbif nature https://api.gbif.org/v1/occurrence/search results scientificName eventDate '' basisOfRecord
body gbif <<'JSON'
{"results":[{"scientificName":"Quercus robur","eventDate":"2026-09-15","decimalLatitude":51.5,"decimalLongitude":-0.12,"basisOfRecord":"HUMAN_OBSERVATION"}]}
JSON
source_row pkgs packages https://pypi.org/rss/updates.xml items name '' '' ''
body pkgs <<'JSON'
{"items":[{"name":"leaky-package-must-not-appear"}]}
JSON
source_row hidden forums https://hidden.example.test/feed items title published url author
body hidden <<'JSON'
{"items":[{"title":"MUST NOT LEAK FROM GLOBAL FAMILY","published":"2026-09-16T12:00:00Z","url":"https://hidden.example.test/1","author":"hidden"}]}
JSON

# The life front reads its own city: local news is its family, so a forum
# post about a programming language is not what it opens on.
source_row dallas local-news https://www.dallasnews.com/arc/outboundfeeds/rss/ items title published url author
body dallas <<'JSON'
{"items":[{"title":"Fair Park reopens its gates for the State Fair","published":"2026-09-16T12:04:00Z","url":"https://www.dallasnews.com/fair-park","author":"Metro desk"}]}
JSON

# A source family only chooses a reservoir.  The public Releases and
# Advisories routes must additionally require their compiled publisher/form
# contract, so an ordinary forum item cannot borrow a high-claim route label.
source_row gamespress gaming https://gamespress.com/en-US/News/RSS items title published url author
body gamespress <<'JSON'
{"items":[{"title":"Canon Press Release: A game is available now","published":"2026-09-16T12:00:00Z","url":"https://gamespress.com/release/1","author":"GamesPress"}]}
JSON
source_row resetera gaming https://resetera.com/forums/-/index.rss items title published url author
body resetera <<'JSON'
{"items":[{"title":"Forum thread merely discussing a game release","published":"2026-09-16T12:01:00Z","url":"https://resetera.com/thread/1","author":"member"}]}
JSON
source_row stonemaier gaming https://stonemaiergames.com/feed/ items title published url author
body stonemaier <<'JSON'
{"items":[
 {"title":"Spending on Prelaunch Ads: Worth the Squeeze?","published":"2026-09-16T12:02:00Z","url":"https://stonemaiergames.com/prelaunch","author":"Stonemaier"},
 {"title":"Our next game is available now","published":"2026-09-16T12:01:30Z","url":"https://stonemaiergames.com/available","author":"Stonemaier"}
]}
JSON
source_row threat vulnerabilities https://threatfilter.dev/feed.xml items title published url author
body threat <<'JSON'
{"items":[{"title":"[CRITICAL] Canon provider advisory","published":"2026-09-16T12:03:00Z","url":"https://threatfilter.dev/advisory/1","author":"Threat Filter"}]}
JSON
source_row spice vulnerabilities https://community.spiceworks.com/posts.rss items title published url author
body spice <<'JSON'
{"items":[{"title":"Forum question that is not an advisory","published":"2026-09-16T12:03:00Z","url":"https://community.spiceworks.com/post/1","author":"member"}]}
JSON

# A SitePresentation bound to a territory apex must not replace that
# territory's front page, the way an agriculture program once took .life.
entry site_presentation '{"site_id":"agriculture","host":"aurekai.life","subject_address":"topic:agriculture","profile":"ghost:ghost-broadsheet","territory":"life","navigation":[],"status":"published"}'
entry site_program '{"program_id":"agriculture:v1","site_id":"agriculture","experience_programs":["Publication"],"application_bindings":[],"renderer_preferences":{},"source_collections":[],"capability_programs":[],"actor_presentations":{},"status":"published"}'

matrix="$tmp/matrix.db"
"$web" bootstrap-owned --db "$matrix" >/dev/null
fabric="$tmp/subject-fabric.db"
python3 - "$fabric" <<'PY'
import sqlite3,sys
db=sqlite3.connect(sys.argv[1])
db.execute("CREATE TABLE subject_bundle(subject TEXT,scope TEXT)")
db.execute("INSERT INTO subject_bundle VALUES(?,?)",
           ("institution:aurekai:live","public"))
db.execute("""CREATE TABLE subject_reader(
 subject TEXT,scope TEXT,census_source TEXT,root_id TEXT,channel_id TEXT,material TEXT,
 protocol TEXT,state TEXT,last_contributed TEXT,contributions INTEGER,first_seen TEXT)""")
rows=[
 ("institution:aurekai:live","public","lemmy","r-lemmy","c-lemmy","forums","RSS","admitted","",0,""),
 ("institution:aurekai:live","public","hn","r-hn","c-hn","hacker-attention","JSON_API","admitted","",0,""),
 ("institution:aurekai:live","public","quakes","r-usgs","c-quakes","hazards","JSON_API","admitted","",0,""),
 ("institution:aurekai:live","public","gbif","r-gbif","c-gbif","nature","JSON_API","admitted","",0,""),
 ("institution:aurekai:live","public","gamespress","r-gamespress","c-gamespress","gaming","RSS","admitted","",0,""),
 ("institution:aurekai:live","public","resetera","r-resetera","c-resetera","gaming","RSS","admitted","",0,""),
 ("institution:aurekai:live","public","stonemaier","r-stonemaier","c-stonemaier","gaming","RSS","admitted","",0,""),
 ("institution:aurekai:live","public","threat","r-threat","c-threat","vulnerabilities","RSS","admitted","",0,""),
 ("institution:aurekai:live","public","spice","r-spice","c-spice","vulnerabilities","RSS","admitted","",0,""),
]
db.executemany("INSERT INTO subject_reader VALUES(?,?,?,?,?,?,?,?,?,?,?)",rows)
db.execute("""CREATE TABLE subject_route(
 subject TEXT,scope TEXT,route TEXT,territory TEXT,title TEXT,lede TEXT,
 character TEXT,cannot_infer TEXT)""")
db.execute("INSERT INTO subject_route VALUES(?,?,?,?,?,?,?,?)",
           ("institution:aurekai:live","public","/channels","live",
            "Compiled Channels", "Exact route contract from Subject Fabric.",
            "signal", "fixture route boundary"))
db.execute("""CREATE TABLE subject_route_region(
 subject TEXT,scope TEXT,route TEXT,ordinal INTEGER,role TEXT)""")
db.execute("INSERT INTO subject_route_region VALUES(?,?,?,?,?)",
           ("institution:aurekai:live","public","/channels",0,"topics"))
db.execute("""CREATE TABLE subject_route_item(
 subject TEXT,scope TEXT,route TEXT,ordinal INTEGER,source TEXT,item_key TEXT,material TEXT,
 title TEXT,byline TEXT,publisher TEXT,href TEXT,group_name TEXT,image TEXT,asset TEXT,poster TEXT,
 body TEXT,ident TEXT,about TEXT,rights TEXT,rights_href TEXT,origin TEXT,origin_href TEXT,
 people TEXT,where_text TEXT,transcript_href TEXT,chapters_href TEXT,talk_href TEXT,
 at INTEGER,duration_ms INTEGER,placed INTEGER,lon REAL,lat REAL)""")
material_rows=[
 ("institution:aurekai:live","public","/channels",0,"lemmy","pc1","publication",
  "PRECOMPUTED WITHOUT BODY PARSE","alice","lemmy.world","https://precomputed.test/1","","","","",
  "This text exists only in Subject Fabric.","","","","","","","","","","","","",1789593000,0,0,0,0),
 ("institution:aurekai:live","public","/channels",1,"lemmy","pc2","publication",
  "Bike lanes in my city","bob","lemmy.world","https://precomputed.test/2","","","","",
  "Compiled Lemmy material.","","","","","","","","","","","","",1789592000,0,0,0,0),
 ("institution:aurekai:live","public","/channels",2,"hn","pc3","publication",
  "Show HN: A tiny database","pg","Hacker News","https://precomputed.test/3","","","","",
  "Compiled Hacker News material.","","","","","","","","","","","","",1789591000,0,0,0,0),
]
material_rows=[row[:27]+row[28:] for row in material_rows]
assert all(len(row)==32 for row in material_rows),material_rows
db.executemany("INSERT INTO subject_route_item VALUES("+",".join("?" for _ in range(32))+")",material_rows)
db.commit();db.close()
PY
port=$(python3 -c "import socket; s=socket.socket(); s.bind(('127.0.0.1',0)); print(s.getsockname()[1]); s.close()")
BONFYRE_CMS_BINARY="$cms" BONFYRE_CMS_DB="$cmsdb" BONFYRE_CMS_SCHEMAS="$schemas" \
BONFYRE_FOREIGN_ROOT="$tmp/foreign" \
BONFYRE_SUBJECT_FABRIC_DB="$fabric" \
BONFYRE_PUBLIC_PROJECTION_CACHE_DB="" \
  "$web" serve --db "$matrix" --auth-db "$tmp/auth.db" --port "$port" \
  --site-root "$root/site/matrix" >"$tmp/server.log" 2>&1 &
pid=$!
attempt=0
until curl -sS -o /dev/null -H 'Host: aurekai.live' "http://127.0.0.1:$port/healthz" 2>/dev/null; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 80 ] || ! kill -0 "$pid" 2>/dev/null; then cat "$tmp/server.log" >&2; echo 'page programs: server did not start' >&2; exit 1; fi
  sleep 0.05
done
get() { curl -sS -H 'Host: aurekai.live' -H 'Accept: text/html' "http://127.0.0.1:$port$1"; }
get /channels >"$tmp/channels.html"
get /parcels >"$tmp/map.html"
get /inbox >"$tmp/inbox.html"
get /releases >"$tmp/releases.html"
get /advisories >"$tmp/advisories.html"
curl -sS -H 'Host: aurekai.life' -H 'Accept: text/html' "http://127.0.0.1:$port/" >"$tmp/front-life.html"
curl -sS -H 'Host: www.aurekai.work:443' -H 'Accept: text/html' "http://127.0.0.1:$port/" >"$tmp/front-work.html"
curl -sS -H 'Host: aurekai.life' -H 'Accept: application/json' "http://127.0.0.1:$port/" >"$tmp/front-life.json"
code=$(curl -sS -o /dev/null -w '%{http_code}' -H 'Host: aurekai.live' "http://127.0.0.1:$port/assets/aurekai-native.css")
land=$(curl -sS -o /dev/null -w '%{http_code} %{content_type}' -H 'Host: aurekai.live' "http://127.0.0.1:$port/assets/world-land.svg")
case "$land" in "200 image/svg+xml"*) ;; *) echo "page programs: FAIL: map land plate not served ($land)" >&2; exit 1 ;; esac

python3 - "$tmp/channels.html" "$tmp/map.html" "$tmp/inbox.html" "$tmp/releases.html" "$tmp/advisories.html" "$code" "$tmp/front-life.html" "$tmp/front-work.html" "$tmp/front-life.json" <<'PY' || exit 1
import re, sys
from html.parser import HTMLParser
class Navigation(HTMLParser):
    def __init__(self, page, label):
        super().__init__(); self.label = label; self.active = False; self.anchor = None; self.links = {}; self.svg = False; self.feed(page)
    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == 'nav': self.active = attrs.get('aria-label') == self.label
        if self.active and tag == 'a': self.anchor = [attrs, '']
        if tag == 'svg': self.svg = True
    def handle_data(self, text):
        if self.active and self.anchor is not None and not self.svg: self.anchor[1] += text
    def handle_endtag(self, tag):
        if tag == 'svg': self.svg = False
        if tag == 'a' and self.anchor is not None:
            attrs, text = self.anchor; self.links[attrs.get('href')] = (text.strip(), attrs); self.anchor = None
        if tag == 'nav': self.active = False
channels, mapped, inbox, releases, advisories = (open(p, encoding="utf-8").read() for p in sys.argv[1:6])
css_code = sys.argv[6]
front_life, front_work, front_json = (open(p, encoding="utf-8").read() for p in sys.argv[7:10])
problems = []
def check(ok, why):
    if not ok: problems.append(why)

check("<title>Compiled Channels</title>" in channels, "channels renders the exact compiled route title")
check("Exact route contract from Subject Fabric." in channels, "channels renders the compiled route lede")
check('class="panes"' in channels, "channels is two panes: places, then the selected place's topics")
streams = re.findall(r'href="/channels\?s=([^"]+)"[^>]*>([^<]*)<small>', channels)
names = [n for _, n in streams]
check("lemmy.world" in names and "Hacker News" in names, "places are where the conversation happens, relays named for their publisher: %s" % names)
topics = channels[channels.find('class="pane-topics"'):]
check("<h3>" in topics, "a place is selected by default")
check('?s=lemmy.world' in channels, "choosing a place is a shareable link")
check("PRECOMPUTED WITHOUT BODY PARSE" in channels, "compiled route material reaches the page without reopening retained bodies")
check("MUST NOT LEAK FROM GLOBAL FAMILY" not in channels, "same-family CMS source outside the subject closure is excluded")
check("leaky-package-must-not-appear" not in channels, "a family the program does not name never leaks in")
sections = Navigation(channels, 'Sections').links
check(sections.get('/whats-on', ('', {}))[0] == "What's on" and '/channels' not in sections, "shared nav lists the sections that hold material")
check(sections.get("/board", ("", {}))[0] == "Collections" and "/s/work/all" not in sections and "/s/prices/observed" not in sections, "navigation keeps reading sections and excludes dead or internal counters")
check(sections.get('https://docs.aurekai.work/', ('', {}))[0] == 'Guides' and '/examples' not in sections, "shared nav carries guides without a global demo archive")
check("Quercus robur" not in channels, "nature is not a channel")

check("<title>Map</title>" in mapped, "map is a composed page")
svg_at, list_at = mapped.find('<figure class="map with-list">'), mapped.find('class="places-list"')
check(svg_at >= 0 and list_at > svg_at, "the map sits beside its numbered list")
points = re.findall(r'<circle class="point[^"]*" cx="([\d.]+)" cy="([\d.]+)"', mapped)
check(len(points) == 4, "four placed reports become four points, 0,0 is not a place: %s" % points)
check(("30.10", "28.70") in points, "GeoJSON lon/lat project onto the plate")
check(("179.88", "38.50") in points, "Darwin Core decimalLatitude/decimalLongitude project too")
check("Unknown spot" not in mapped[svg_at:mapped.find("</figure>", svg_at)], "an unplaced report is not a point or a list entry on the map")
check("leaky-package-must-not-appear" not in mapped, "packages are not on the map")

check("Canon Press Release: A game is available now" in releases, "Releases keeps material from its declared press source contract")
check("Forum thread merely discussing a game release" not in releases, "a gaming-family forum discussion cannot pose as a release")
check("Our next game is available now" in releases, "a declared publisher plus an exact release-form term is admitted")
check("Spending on Prelaunch Ads" not in releases, "a term embedded in a different word cannot manufacture a release")
check("[CRITICAL] Canon provider advisory" in advisories, "Advisories keeps material from its declared security source contract")
check("Forum question that is not an advisory" not in advisories, "a vulnerabilities-family forum question cannot pose as an advisory")

check('href="/assets/aurekai-native.css' in inbox, "a native document loads the native stylesheet")
check("<h1>Inbox</h1>" in inbox, "a native document's title is a word, not a program name")
check(css_code == "200", "the native stylesheet is served (%s)" % css_code)

life_titles = ("This morning", "Today", "Tonight", "Still open")
check(any("<title>%s</title>" % t in front_life for t in life_titles), "aurekai.life opens on its hour-derived composed front page, not the SitePresentation bound to the apex")
check("agriculture" not in front_life and "ghost:" not in front_life, "no site program vocabulary on the front page")
territories = Navigation(front_life, 'Territories').links
check(territories.get('https://aurekai.life/', ('', {}))[0] == 'Life' and territories.get('https://aurekai.life/', ('', {}))[1].get('aria-current') == 'page', "the front page names its territory and offers the others")
check(all(territories.get('https://aurekai.' + name.lower() + '/', ('', {}))[0] == name for name in ['Life', 'Live', 'Work', 'Shop']), "every territory is one tap away")
check("Fair Park reopens its gates" in front_life, "the front page composes real material from its families")
check("Rust 2.0 thoughts" not in front_life, "the life front does not open on a forum it does not name")
check('class="ak-brand"' in front_life and "&larr; Aurekai" not in front_life, "a front page names the place instead of pointing back to it")
check("<title>Work</title>" in front_work, "www. apex with a port in its Host answers with that territory's front page")
check("Forum question that is not an advisory" not in front_work, "the Work preview cannot bypass Advisories' source/form contract")
check("Spending on Prelaunch Ads" not in front_work, "the Work preview cannot bypass Releases' whole-term contract")
check("<title>Today</title>" not in front_json and "<html" not in front_json, "a machine request to the apex still gets its machine answer")

if problems:
    print("page programs: FAIL")
    for p in problems: print("  -", p)
    sys.exit(1)
PY
echo 'page programs: PASS'
