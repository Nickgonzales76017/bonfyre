#!/bin/sh
# The page reads retained sources by the census contract the retired browser
# reader used, natively: nested and indexed fields, templated extracts, the
# maker, the picture, Atom and RSS media, the source's own sensitivity flags,
# and a screen's boxed twin.  Fixture bodies are shaped like the production
# sources that feed the public pages (PeerTube, the Art Institute's IIIF API,
# USGS GeoJSON, an Atom blog, a flagged trending list).
set -eu

web=${1:?web-address binary required}
case "$web" in /*) ;; *) web="$(pwd)/$web" ;; esac
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
cms="$root/cmd/BonfyreCMS/bonfyre-cms"
[ -x "$cms" ] || { echo 'page census reader: SKIP (cms not built)'; exit 0; }

tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-page-census.XXXXXX")
pid=''
cleanup() {
  [ -n "$pid" ] && kill "$pid" 2>/dev/null || true
  [ -n "${PAGE_COMPOSITION_KEEP:-}" ] && cp "$tmp"/*.html "$PAGE_COMPOSITION_KEEP"/ 2>/dev/null || true
  rm -rf "$tmp"
}
trap cleanup EXIT INT TERM

schemas="$tmp/schemas"
mkdir -p "$schemas" "$tmp/foreign/receipts" "$tmp/foreign/bodies"
for type in world world_fragment internet_source publication_occurrence content_flag; do
  cp "$root/cmd/BonfyreCMS/content-types/$type.json" "$schemas/"
done
cmsdb="$tmp/cms.db"
"$cms" schema migrate --db "$cmsdb" --schemas "$schemas" >/dev/null
entry() { "$cms" entry create "$1" "$2" --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null; }
source_row() {
  entry internet_source "{\"family\":\"$2\",\"source\":\"$1\",\"label\":\"cataloguing phrase\",\"says\":\"x\",\"url\":\"https://api.$1.test/feed\",\"twin\":\"$1-twin\",\"state\":\"observed\",\"extract\":\"$3\",\"title_field\":\"$4\",\"meta_field\":\"$5\",\"link_field\":\"$6\",\"image_field\":\"$7\",\"image_base\":\"$8\",\"sub_field\":\"$9\",\"body_regime\":\"retained-public\",\"status\":\"published\",\"tier\":\"warm\",\"kind\":\"${KIND:-text}\"}"
}
body() { printf '%s\n' "{\"body_path\":\"$tmp/foreign/bodies/$1.body\"}" >"$tmp/foreign/receipts/$1-twin.json"; cat >"$tmp/foreign/bodies/$1.body"; }

entry world '{"world":"made-this-week","title":"Made this week","character":"scrapbook","lede":"Pictures, video and writing people published.","subject":"topic:made","sources":"tube,museum,quakes,blog,trending,neo","grammars":"board","cannot_infer":"a picture is not a licence","status":"published"}'

KIND=video source_row tube video data name views url thumbnailPath '@channel.host' channel.displayName
body tube <<'JSON'
{"data":[
 {"name":"Learning Blender \u2014 part one \ud83c\udfa8 \"quoted\"","views":120,"url":"https://tube.example.test/w/abc","thumbnailPath":"/static/thumbnails/abc.jpg","channel":{"displayName":"Kiri","host":"tube.example.test"}},
 {"name":"Adult clip","views":3,"url":"https://tube.example.test/w/nsfw","thumbnailPath":"/static/thumbnails/nsfw.jpg","nsfw":true,"channel":{"displayName":"Hidden","host":"tube.example.test"}},
 {"name":"No picture talk","views":9,"url":"https://tube.example.test/w/talk","channel":{"displayName":"Ana","host":"tube.example.test"}},
 {"name":"Second clip","views":7,"url":"https://tube.example.test/w/two","thumbnailPath":"/static/thumbnails/two.jpg","channel":{"displayName":"Lu","host":"tube.example.test"}}
]}
JSON

KIND=image source_row museum museums data title '' id image_id 'https://iiif.example.test/iiif/2/|/full/400,/0/default.jpg' artist_title
body museum <<'JSON'
{"data":[
 {"title":"Water Lilies","id":16568,"image_id":"3c27b499","artist_title":"Claude Monet"},
 {"title":"The Bedroom","id":28560,"image_id":"25c31d8d","artist_title":"Vincent van Gogh"},
 {"title":"Unpictured study","id":1,"artist_title":"Unknown"},
 {"title":"Bad picture","id":2,"image_id":"x\" onerror=\"alert(1)","artist_title":"Nobody"}
]}
JSON

source_row quakes hazards features properties.place properties.time properties.url '' '' ''
body quakes <<'JSON'
{"type":"FeatureCollection","features":[
 {"type":"Feature","properties":{"place":"12 km SW of Ridgecrest, CA","time":"2026-09-15T08:00:00Z","url":"https://quake.example.test/ci1"}},
 {"type":"Feature","properties":{"place":"4 km N of Anza, CA","time":"2026-09-14T08:00:00Z","url":"https://quake.example.test/ci2"}}
]}
JSON

source_row blog security rss:entry title '' feed:auto feed:auto '' ''
body blog <<'XML'
<?xml version="1.0"?><feed xmlns="http://www.w3.org/2005/Atom" xmlns:media="http://search.yahoo.com/mrss/">
<entry><title type="html">Keys &amp;amp; locks, again</title><link rel="alternate" href="https://blog.example.test/keys"/><updated>2026-09-13T10:00:00Z</updated><author><name>Bruce</name></author><media:thumbnail url="https://blog.example.test/keys.jpg"/></entry>
<entry><title>Plain entry</title><link href="https://blog.example.test/plain"/><published>2026-09-12T10:00:00Z</published></entry>
</feed>
XML

source_row trending github items full_name stargazers_count html_url owner.avatar_url '' language
body trending <<'JSON'
{"items":[{"full_name":"boxed/repo","stargazers_count":5,"html_url":"https://example.test/boxed","owner":{"avatar_url":"https://example.test/a.png"},"language":"C"}]}
JSON
entry content_flag '{"flag":"cf-proof","twin":"trending-twin","source":"trending","screen":"proof-screen","signal":"term matched","review_state":"boxed","cannot_infer":"a flag is not a verdict","status":"published"}'

today=$(python3 -c 'import datetime as d; print(d.datetime.now(d.timezone.utc).strftime("%Y-%m-%d"))')
source_row neo space 'near_earth_objects.{today-iso}' name '' nasa_jpl_url '' '' 'descriptions.0.value'
body neo <<JSON
{"near_earth_objects":{"$today":[{"name":"(2026 RX) passes Earth","nasa_jpl_url":"https://neo.example.test/rx","descriptions":[{"value":"A small asteroid that will pass well outside the orbit of the Moon, described here at length so that it reads as what the object is rather than who made it."}]}],"1999-01-01":[{"name":"Stale day must not appear"}]}}
JSON

matrix="$tmp/matrix.db"
"$web" bootstrap-owned --db "$matrix" >/dev/null
port=$(python3 -c "import socket; s=socket.socket(); s.bind(('127.0.0.1',0)); print(s.getsockname()[1]); s.close()")
BONFYRE_CMS_BINARY="$cms" BONFYRE_CMS_DB="$cmsdb" BONFYRE_CMS_SCHEMAS="$schemas" \
BONFYRE_FOREIGN_ROOT="$tmp/foreign" \
  "$web" serve --db "$matrix" --auth-db "$tmp/auth.db" --port "$port" \
  --site-root "$root/site/matrix" >"$tmp/server.log" 2>&1 &
pid=$!
attempt=0
until curl -sS -o /dev/null -H 'Host: aurekai.live' "http://127.0.0.1:$port/healthz" 2>/dev/null; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 80 ] || ! kill -0 "$pid" 2>/dev/null; then cat "$tmp/server.log" >&2; echo 'page census reader: server did not start' >&2; exit 1; fi
  sleep 0.05
done
curl -sS -H 'Host: aurekai.live' -H 'Accept: text/html' "http://127.0.0.1:$port/w/made-this-week" >"$tmp/census.html"

python3 - "$tmp/census.html" "$port" <<'PY' || exit 1
import html, re, subprocess, sys
page = open(sys.argv[1], encoding="utf-8").read()
port = sys.argv[2]
def fetch(path):
    return subprocess.run(["curl", "-sS", "-H", "Host: aurekai.live", "-H", "Accept: text/html", "http://127.0.0.1:%s%s" % (port, path)],
                          capture_output=True, text=True).stdout
# Every item opens its own Aurekai page; the original link lives there.
item_links = sorted(set(re.findall(r'href="(/o/[^"]+)"', page)))
item_pages = {link: fetch(link) for link in item_links}
opened = "\n".join(item_pages.values())
problems = []
def check(ok, why):
    if not ok: problems.append(why)
body = page.split("</head>", 1)[-1]

shelf = body[body.find('class="shelf"'):]
shelf = shelf[:shelf.find("</section>")]
check('class="shelf"' in body, "video source with pictures becomes a Watch shelf")
check('src="https://tube.example.test/static/thumbnails/abc.jpg"' in shelf, "@host image base completes against the maker's host")
check(re.search(r'href="/o/tube/[0-9a-f]{24}"', shelf), "a remote video opens its own Aurekai page")
check('href="https://tube.example.test/w/abc"' in opened, "the video's page links to where it was published")
check("Learning Blender — part one 🎨 &quot;quoted&quot;" in shelf, "JSON escapes decode, including surrogate pairs and quotes")
check("\\u2014" not in body, "no raw JSON escape reaches the page")
check(">Kiri<" in shelf and "tube.test" in shelf, "channel is the maker; the publisher's domain stays alongside")
check("Adult clip" not in body and "nsfw.jpg" not in body, "a source's own nsfw flag withholds the item")
check("No picture talk" in body and "No picture talk" not in shelf, "a video item without a picture is only a headline")
check('referrerpolicy="no-referrer"' in shelf and 'loading="lazy"' in shelf, "remote pictures load lazily without a referrer")

field = body[body.find('class="field"'):]
field = field[:field.find("</section>")]
check('class="field"' in body, "image source with pictures becomes a Photos field")
check('src="https://iiif.example.test/iiif/2/3c27b499/full/400,/0/default.jpg"' in field, "pre|post image base wraps the identifier")
check("Claude Monet" in field, "artist is the maker")
check("onerror" not in body and "Bad picture" not in field, "an unsafe picture URL is never written into an attribute")
check("Unpictured study" in body and "Unpictured study" not in field, "an image item without a picture is only a headline")

check("12 km SW of Ridgecrest, CA" in body and 'href="https://quake.example.test/ci1"' in opened, "nested GeoJSON fields resolve, link included")
check("Keys &amp; locks, again" in body and "&amp;amp;" not in body, "double-escaped feed titles decode once")
check('href="https://blog.example.test/keys"' in opened, "Atom link href is the story's original link")
check(">Bruce<" in body, "Atom author names the maker")
check('src="https://blog.example.test/keys.jpg"' in body, "media:thumbnail becomes the story's picture")
check("Plain entry" in body and 'href="https://blog.example.test/plain"' in opened, "Atom entry without a picture still reads")
check("boxed/repo" not in body, "a screen's boxed twin is withheld until cleared")
check("(2026 RX) passes Earth" in body, "{today-iso} extract resolves to today's list")
check("Stale day must not appear" not in body, "other days of a date-keyed list are not read")
check('class="dek"' in body and "A small asteroid" in body, "a long secondary field is what the item says, not its maker")
check("cataloguing phrase" not in body, "a cataloguing label is never a byline")
check(">quakes.test<" in body, "a byline is the publisher's registrable domain, not the API host")
# Shared identity refresh is not a content renderer. The only other scripts
# are explicitly guarded home enhancements, inactive on this World address.
scripts = re.findall(r"<script\b[^>]*>(.*?)</script>", page, re.S)
account = [s for s in scripts if "document.currentScript.closest('.ak-bar')" in s]
home = [s for s in scripts if "if(location.pathname!=='/')return;" in s or "if(location.pathname!=='/'||" in s]
check(len(account) == 1 and len(home) == 2 and len(scripts) == 3,
      "only scoped navigation and inactive home enhancement scripts accompany retained material")
check(not re.search(r'<script\b[^>]*(?:src=|type=)', page), "no content renderer is fetched or mounted")

if problems:
    print("page census reader: FAIL")
    for p in problems: print("  -", p)
    sys.exit(1)
PY
echo 'page census reader: PASS'
