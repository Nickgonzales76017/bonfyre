#!/bin/sh
# Every clickable on a composed page works, every route has its own layout and
# its own sources, and every item opens a page with the verbs people already
# know for that kind of thing.  This walks the pages the way a person would:
# it follows every internal link it can see and expects each one to answer.
set -eu

web=${1:?web-address binary required}
case "$web" in /*) ;; *) web="$(pwd)/$web" ;; esac
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
cms="$root/cmd/BonfyreCMS/bonfyre-cms"
[ -x "$cms" ] || { echo 'page items: SKIP (cms not built)'; exit 0; }

tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-page-items.XXXXXX")
pid=''
cleanup() {
  [ -n "$pid" ] && kill "$pid" 2>/dev/null || true
  [ -n "${PAGE_COMPOSITION_KEEP:-}" ] && cp "$tmp"/*.html "$PAGE_COMPOSITION_KEEP"/ 2>/dev/null || true
  rm -rf "$tmp"
}
trap cleanup EXIT INT TERM

schemas="$tmp/schemas"
mkdir -p "$schemas" "$tmp/foreign/receipts" "$tmp/foreign/bodies"
for type in world world_fragment internet_source publication_occurrence content_flag project team_need bounty project_support governance_proposal; do
  cp "$root/cmd/BonfyreCMS/content-types/$type.json" "$schemas/"
done
cmsdb="$tmp/cms.db"
"$cms" schema migrate --db "$cmsdb" --schemas "$schemas" >/dev/null
entry() { "$cms" entry create "$1" "$2" --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null; }
entry project '{"project_id":"page-walk-project","subject_ref":"program:page-walk-project","title":"Page Walk Project","pitch":"A published fixture project.","stage":"open","opened_at":"2026-09-29T00:00:00Z","cannot_infer":"fixture project is not a commitment","status":"published"}'
python3 - "$tmp/prices.db" <<'PY'
import sqlite3,sys
db=sqlite3.connect(sys.argv[1])
db.execute("CREATE TABLE observed_price(id INTEGER PRIMARY KEY,source TEXT,publisher TEXT,item TEXT,amount REAL,currency TEXT,unit TEXT,region TEXT,kind TEXT,evidence_ref TEXT,observed_at TEXT)")
db.execute("INSERT INTO observed_price(source,publisher,item,amount,currency,unit,region,kind,evidence_ref,observed_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
           ("fixture-list","Fixture Publisher","Fixture service",1.0,"USD","each","","service","fixture:price-observation","2026-09-29T00:00:00Z"))
for i in range(4):
 db.execute("INSERT INTO observed_price(source,publisher,item,amount,currency,unit,region,kind,evidence_ref,observed_at) VALUES(?,?,?,?,?,?,?,?,?,?)",("fixture-list","Medicine publisher",f"CLOTRIMAZOLE 1% TOPICAL CREAM ({i})",i+1,"USD","unit","US","medicine","fixture:medicine","2026-09-29T00:00:00Z"))
for i in range(70):
 db.execute("INSERT INTO observed_price(source,publisher,item,amount,currency,unit,region,kind,evidence_ref,observed_at) VALUES(?,?,?,?,?,?,?,?,?,?)",("fixture-list","Fixture Publisher",f"Fixture Paged {i:03}",1,"USD","each","","service","fixture:paged","2026-09-29T00:00:00Z"))
for i in range(3):
 db.execute("INSERT INTO observed_price(source,publisher,item,amount,currency,unit,region,kind,evidence_ref,observed_at) VALUES(?,?,?,?,?,?,?,?,?,?)",("fixture-list",f"Shop {i}","Not a barcode",i+1,"USD","each","","consumer","fixture:product","2026-09-29T00:00:00Z"))
for i in range(3):
 db.execute("INSERT INTO observed_price(source,publisher,item,amount,currency,unit,region,kind,evidence_ref,observed_at) VALUES(?,?,?,?,?,?,?,?,?,?)",("fixture-list",f"Shop {i}","8076809523509",i+1,"USD","each","","consumer","fixture:product","2026-09-29T00:00:00Z"))
for publisher, rates in [("Router A",(0.000001,0.000002)),("Router B",(0.000004,0.000005))]:
 for unit, amount in zip(("token in","token out"),rates):
  db.execute("INSERT INTO observed_price(source,publisher,item,amount,currency,unit,region,kind,evidence_ref,observed_at) VALUES(?,?,?,?,?,?,?,?,?,?)",("fixture-router",publisher,"Fixture model",amount,"USD",unit,"","inference","https://prices.example.test/list","2026-09-29T00:00:00Z"))
db.execute("UPDATE observed_price SET evidence_ref='https://prices.example.test/list' WHERE item='Fixture Paged 000'")
db.execute("INSERT INTO observed_price(source,publisher,item,amount,currency,unit,region,kind,evidence_ref,observed_at) VALUES(?,?,?,?,?,?,?,?,?,?)",("fake-service","Not a model price","Fixture model",99,"USD","each","","service","fixture:service","2026-09-30T00:00:00Z"))
db.execute("INSERT INTO observed_price(source,publisher,item,amount,currency,unit,region,kind,evidence_ref,observed_at) VALUES(?,?,?,?,?,?,?,?,?,?)",("fixture-list","Fresh public publisher","Fixture Recent Service",12.5,"USD","each","US","service","https://prices.example.test/latest","2026-10-02T00:00:00Z"))
for publisher,amount in [("Comparative Low",10),("Comparative Middle",12),("Comparative High",15)]:
 db.execute("INSERT INTO observed_price(source,publisher,item,amount,currency,unit,region,kind,evidence_ref,observed_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
            ("fixture-comparison",publisher,"Fixture Comparative Transport",amount,"USD","each","Dallas","service",f"https://prices.example.test/{publisher.lower().replace(chr(32),chr(45))}","2026-09-29T12:00:00Z"))
for publisher,amount,unit,region,date in [("Different date",800,"each","Dallas","2026-10-02T12:00:00Z"),("Different unit",700,"hour","Dallas","2026-09-29T12:00:00Z"),("Different region",600,"each","Austin","2026-09-29T12:00:00Z"),("Different currency",500,"each","Dallas","2026-09-29T12:00:00Z")]:
 db.execute("INSERT INTO observed_price(source,publisher,item,amount,currency,unit,region,kind,evidence_ref,observed_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
            ("fixture-comparison",publisher,"Fixture Comparative Transport",amount,"EUR" if publisher=="Different currency" else "USD",unit,region,"service","fixture:private",date))
db.commit();db.close()
PY
python3 - "$tmp/models.db" <<'PY'
import sqlite3,sys
db=sqlite3.connect(sys.argv[1]);db.executescript("CREATE TABLE catalog_nodes(node_id TEXT,kind TEXT,external_id TEXT,name TEXT,summary TEXT,json_data TEXT);CREATE TABLE catalog_edges(src_node_id TEXT,dst_node_id TEXT,rel TEXT);INSERT INTO catalog_nodes VALUES('model:fixture','model','fixture-model','Fixture model','Fixture model','{}');");db.commit();db.close()
PY
row() {
  entry internet_source "{\"family\":\"$2\",\"source\":\"$1\",\"label\":\"x\",\"says\":\"x\",\"url\":\"$3\",\"twin\":\"$1-twin\",\"state\":\"observed\",\"extract\":\"$4\",\"title_field\":\"$5\",\"meta_field\":\"$6\",\"link_field\":\"$7\",\"image_field\":\"$8\",\"image_base\":\"$9\",\"sub_field\":\"${SUB:-}\",\"body_regime\":\"retained-public\",\"status\":\"published\",\"tier\":\"warm\",\"kind\":\"${KIND:-text}\"}"
}
body() { printf '%s\n' "{\"body_path\":\"$tmp/foreign/bodies/$1.body\"}" >"$tmp/foreign/receipts/$1-twin.json"; cat >"$tmp/foreign/bodies/$1.body"; }
now=$(python3 -c 'import datetime as d; print(d.datetime.now(d.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))')

KIND=post row newsdesk news https://www.newsdesk.test/rss rss:item title '' feed:auto '' ''
body newsdesk <<XML
<?xml version="1.0"?><rss xmlns:media="http://search.yahoo.com/mrss/"><channel>
<item><title>City opens a new library</title><link>https://www.newsdesk.test/library</link><pubDate>$(python3 -c 'import email.utils,time; print(email.utils.formatdate(time.time()-3600, usegmt=True))')</pubDate><media:thumbnail url="https://www.newsdesk.test/library.jpg"/></item>
<item><title>Council votes on transit plan</title><link>https://www.newsdesk.test/transit</link><pubDate>$(python3 -c 'import email.utils,time; print(email.utils.formatdate(time.time()-7200, usegmt=True))')</pubDate></item>
</channel></rss>
XML
KIND=video SUB=channel.displayName row tube video https://tube.example.test/api/v1/videos data name '' url thumbnailPath '@channel.host'
body tube <<JSON
{"data":[
 {"name":"Blender basics","url":"https://tube.example.test/w/abc123","thumbnailPath":"/static/abc.jpg","publishedAt":"$now","channel":{"displayName":"Kiri","host":"tube.example.test"}},
 {"name":"Sculpting a fox","url":"https://tube.example.test/videos/watch/def-456","thumbnailPath":"/static/def.jpg","channel":{"displayName":"Kiri","host":"tube.example.test"}},
 {"name":"Knitting at home","url":"https://tube.example.test/w/ghi789","thumbnailPath":"/static/ghi.jpg","channel":{"displayName":"Ana","host":"tube.example.test"}},
 {"name":"Schedule only without a player","url":"https://tube.example.test/schedule","thumbnailPath":"/static/schedule.jpg","channel":{"displayName":"Ana","host":"tube.example.test"}},
 {"name":"Schedule publication without video","url":"https://tube.example.test/listings","channel":{"displayName":"Ana","host":"tube.example.test"}}
]}
JSON
KIND=image SUB=artist_title row museum museums https://api.museum.test/artworks data title '' url image_id 'https://img.museum.test/iiif/|/full/400,/0/default.jpg'
body museum <<'JSON'
{"data":[{"title":"Water Lilies","url":"https://api.museum.test/art/other","artist_title":"Other","provenance":[{"date":"1900","description":"Unrelated history must stay hidden"}]},{"title":"Water Lilies","url":"https://api.museum.test/art/lilies","image_id":"lilies","artist_title":"Claude Monet","provenance":[{"date":"1900","description":"A named collector"},{"date":"1920","description":"Museum collection"}]},{"title":"The Bedroom","url":"https://api.museum.test/art/bedroom","image_id":"bedroom","artist_title":"Vincent van Gogh","provenance":[{"date":"1900","description":"A named collector"},{"date":"1920","description":"Museum collection"}]},{"title":"Museum schedule without image","artist_title":"Gallery"}]}
JSON
SUB=community.name row forum forums https://lemmy.world/api/v3/post/list posts post.name post.published post.ap_id '' ''
body forum <<JSON
{"posts":[{"post":{"name":"Best trail near town","published":"$now","ap_id":"https://lemmy.world/post/9"},"community":{"name":"hiking"}}]}
JSON
SUB=account.acct row statuses fediverse https://mastodon.social/api/v1/trends/statuses '' content created_at url '' ''
body statuses <<JSON
[{"content":"Sunset over the bay tonight","created_at":"$now","url":"https://mastodon.social/@maya/1","account":{"acct":"maya"}},
 {"content":"Garden update: tomatoes!","created_at":"$now","url":"https://mastodon.social/@maya/2","account":{"acct":"maya"}},
 {"content":"New zine out","created_at":"$now","url":"https://mastodon.social/@sam/3","account":{"acct":"sam"}}]
JSON
row quakes hazards https://earthquake.usgs.gov/feed features properties.place properties.updated properties.url '' ''
body quakes <<JSON
{"features":[{"properties":{"place":"10 km N of Anchorage, Alaska","updated":"$now","url":"https://quake.example.test/1"},"geometry":{"coordinates":[-149.9,61.3,10]}},
 {"properties":{"place":"5 km E of Tokyo, Japan","updated":"$now","url":"https://quake.example.test/2"},"geometry":{"coordinates":[139.8,35.7,40]}},
 {"properties":{"place":"Near Santiago, Chile","updated":"$now","url":"https://quake.example.test/3"},"geometry":{"coordinates":[-70.6,-33.4,30]}}]}
JSON
row pypi packages https://pypi.org/rss/updates.xml rss:item title '' link '' ''
body pypi <<XML
<?xml version="1.0"?><rss><channel><item><title>requests 3.0.0</title><link>https://pypi.org/project/requests/3.0.0/</link><pubDate>$(python3 -c 'import email.utils,time; print(email.utils.formatdate(time.time()-600, usegmt=True))')</pubDate></item></channel></rss>
XML
row nvd vulnerabilities https://services.nvd.nist.gov/rest vulnerabilities cve.id cve.published '' '' ''
body nvd <<JSON
{"vulnerabilities":[{"cve":{"id":"CVE-2026-0001","published":"$now"}},{"cve":{"id":"CVE-2026-0002","published":"$now"}},{"cve":{"id":"CVE-2026-0003","published":"$now"}}]}
JSON

# /releases and /advisories are high-claim reading routes: a source family is an
# acquisition fact and does not make an item a release or an advisory.  Those
# two routes will only carry a publisher that holds a route contract, so the
# fixture gives them one each -- and keeps pypi and nvd above, which hold none,
# to prove the contract is what admits material rather than the family.
row gamespress gaming https://www.gamespress.com/rss rss:item title '' link '' ''
body gamespress <<XML
<?xml version="1.0"?><rss><channel><item><title>Foxhollow ships its second chapter</title><link>https://www.gamespress.com/foxhollow-two</link><pubDate>$(python3 -c 'import email.utils,time; print(email.utils.formatdate(time.time()-900, usegmt=True))')</pubDate></item></channel></rss>
XML
row threatfilter vulnerabilities https://threatfilter.dev/feed rss:item title '' link '' ''
body threatfilter <<XML
<?xml version="1.0"?><rss><channel>
<item><title>Signing keys rotated after a build host was reached</title><link>https://threatfilter.dev/a/keys</link><pubDate>$(python3 -c 'import email.utils,time; print(email.utils.formatdate(time.time()-1200, usegmt=True))')</pubDate></item>
<item><title>Padding oracle filed against a widely packaged TLS library</title><link>https://threatfilter.dev/a/padding</link><pubDate>$(python3 -c 'import email.utils,time; print(email.utils.formatdate(time.time()-2400, usegmt=True))')</pubDate></item>
</channel></rss>
XML

matrix="$tmp/matrix.db"
"$web" bootstrap-owned --db "$matrix" >/dev/null
port=$(python3 -c "import socket; s=socket.socket(); s.bind(('127.0.0.1',0)); print(s.getsockname()[1]); s.close()")
BONFYRE_CMS_BINARY="$cms" BONFYRE_CMS_DB="$cmsdb" BONFYRE_CMS_SCHEMAS="$schemas" \
BONFYRE_FOREIGN_ROOT="$tmp/foreign" \
BONFYRE_PRICES_DB="$tmp/prices.db" BONFYRE_MODEL_CATALOG="$tmp/models.db" \
  "$web" serve --db "$matrix" --auth-db "$tmp/auth.db" --port "$port" \
  --site-root "$root/site/matrix" >"$tmp/server.log" 2>&1 &
pid=$!
attempt=0
until curl -sS -o /dev/null -H 'Host: aurekai.live' "http://127.0.0.1:$port/healthz" 2>/dev/null; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 80 ] || ! kill -0 "$pid" 2>/dev/null; then cat "$tmp/server.log" >&2; echo 'page items: server did not start' >&2; exit 1; fi
  sleep 0.05
done

python3 - "$port" "$tmp" <<'PY' || exit 1
import html, os, re, subprocess, sys, urllib.parse
port, tmp = sys.argv[1], sys.argv[2]
problems = []
def check(ok, why):
    if not ok: problems.append(why)
def fetch(path, host="aurekai.life"):
    r = subprocess.run(["curl", "-sS", "-o", "-", "-w", "\n%{http_code}", "-H", "Host: " + host, "-H", "Accept: text/html",
                        "http://127.0.0.1:%s%s" % (port, path)], capture_output=True, text=True)
    body, _, code = r.stdout.rpartition("\n")
    return code, body

code, prices = fetch("/s/prices/observed")
check("Find out what things cost" in prices and "Browse prices by category" in prices, "price room starts with a useful question and category browsing")
check('<details class="price-method">' in prices and prices.index("Find out what things cost") < prices.index("About these observations"), "internal coverage counts are secondary, not the price-room lead")
check('Recent publisher price observations' in prices and 'Fixture Recent Service' in prices, "unfiltered prices surface actual retained publisher entries")
check('href="https://prices.example.test/latest"' in prices, "recent price evidence opens original publisher source")
check('Historical statements, not live quotes or available offers' in prices, "recent prices do not claim current availability")
check(code == "200", "price observatory responds")
check("CLOTRIMAZOLE%201%25%20TOPICAL%20CREAM" in prices, "medicine link preserves punctuation")
check("world.openfoodfacts.org/product/Not" not in prices, "non-barcode names are not advertised as barcode links")
check("not a current offer or a guaranteed saving" in prices, "historical ranges do not promise available savings")
check("Records may differ in strength" in prices, "medicine range describes its limitations")
code, match = fetch("/s/prices/observed?kind=medicine&q=CLOTRIMAZOLE%201%25%20TOPICAL%20CREAM")
check(code == "200" and "CLOTRIMAZOLE 1% TOPICAL CREAM" in match, "encoded medicine search reaches its retained listing")
code, first = fetch("/s/prices/observed?kind=service&q=Fixture%20Paged")
check("Fixture Paged 000" in first and "Fixture Paged 059" in first and "Fixture Paged 060" not in first and "offset=60" in first, "first price page has a next link")
check('href="https://prices.example.test/list"' in first, "price row links to its public retained source")
code, second = fetch("/s/prices/observed?kind=service&q=Fixture%20Paged&offset=60")
check("Fixture Paged 060" in second and "Fixture Paged 069" in second and "Fixture Paged 000" not in second and "Previous prices" in second, "next price page preserves search and exposes previous")

code, compared = fetch("/s/prices/observed?kind=service&q=Fixture%20Comparative%20Transport")
check(code == "200" and '<section data-price-comparison>' in compared, "searched item produces a same-scope publisher comparison")
comparison = compared.split('<section data-price-comparison>',1)[1].split("</section>",1)[0]
check("Comparative Low" in comparison and "Comparative High" in comparison, "lowest and highest prices retain source publisher names")
check("10.00 USD" in comparison and "15.00 USD" in comparison and "3 · <time>2026-09-29</time>" in comparison, "same-day comparison reports grounded prices, publisher count and day")
check("$800.00" not in comparison and "$700.00" not in comparison and "$600.00" not in comparison and "$500.00" not in comparison, "different day, units, region and currency cannot contaminate the comparable price range")
check("not live checkout offers or guaranteed savings" in comparison, "dated comparisons are not mistaken for live quotes")
check("href=\"https://prices.example.test/comparative-low\"" in compared, "individual comparison remains tied to original publisher evidence")

code, models = fetch("/s/prices/observed?kind=inference&q=Fixture%20model")
check(code == "200" and "Router A" in models and "Router B" in models, "model rows retain publisher identity")
check(re.search(r"Fixture model</th><td>\$1\.00</td><td>\$2\.00</td><td>Router A", models) is not None, "router A input and output stay paired")
check(re.search(r"Fixture model</th><td>\$4\.00</td><td>\$5\.00</td><td>Router B", models) is not None, "router B input and output stay paired")

code, commons = fetch("/s/models/all")
check(code == "200" and re.search(r"Fixture model</th><td>\$1\.00</td><td>\$2\.00</td><td>Router A", commons) is not None, "Model Commons keeps router A prices paired")
check(re.search(r"Fixture model</th><td>\$4\.00</td><td>\$5\.00</td><td>Router B", commons) is not None, "Model Commons keeps router B prices paired")
code, detail = fetch("/s/model/fixture-model")
check(code == "200" and "Router A" in detail and "Not a model price" not in detail, "model detail excludes same-name non-inference observations")

if os.environ.get("BONFYRE_PRICE_ROOM_ONLY"):
    if problems:
        print("price room: FAIL\n  - " + "\n  - ".join(problems))
        sys.exit(1)
    print("price room: PASS (search, identity, observations and pagination)")
    sys.exit(0)

if os.environ.get("BONFYRE_WATCH_ONLY"):
    code, watch = fetch("/watch")
    check(code == "200" and "Blender basics" in watch, "playable video remains on Watch")
    check("Schedule only without a player" not in watch, "a thumbnail alone does not become playable video")
    check("Schedule publication without video" not in watch, "a schedule publication does not become a Watch lead")
    if problems:
        print("watch room: FAIL\n  - " + "\n  - ".join(problems))
        sys.exit(1)
    print("watch room: PASS (playable and nonplayable selection)")
    sys.exit(0)

if os.environ.get("BONFYRE_BOARD_ONLY"):
    code, board = fetch("/board")
    check("Museum schedule without image" not in board, "a picture source without an image stays off Boards")
    import hashlib
    picture = "/o/museum/" + hashlib.sha256(b"museum\nhttps://api.museum.test/art/lilies").hexdigest()[:24]
    check('href="' + picture + '"' in board, "a museum picture opens its own record")
    _, detail = fetch(picture)
    check("A named collector" in detail and "Museum collection" in detail, "the exact source record supplies its provenance")
    check("Unrelated history must stay hidden" not in detail, "a same-title object cannot supply another object's history")
    check("As recorded by the source" in detail, "the source is named for the history")
    check(code == "200" and "Water Lilies" in board, "a real museum picture remains on Boards")
    check("City opens a new library" not in board, "a news article does not become a board picture")
    check("Blender basics" not in board, "a playable video does not become a board picture")
    if problems:
        print("board room: FAIL\n  - " + "\n  - ".join(problems))
        sys.exit(1)
    print("board room: PASS (picture and nonpicture selection)")
    sys.exit(0)

routes = {"/": "front", "/discover": "feed", "/watch": "video-grid", "/channels": "panes", "/community": "communities",
          "/board": "wall-pins", "/parcels": "map with-list", "/releases": "ledger", "/advisories": "ledger"}
pages = {}
for route, marker in routes.items():
    code, page = fetch(route)
    pages[route] = page
    open("%s/route%s.html" % (tmp, route.replace("/", "_") or "_front"), "w").write(page)
    check(code == "200", "%s answers (%s)" % (route, code))
    # LIFE now has its own authored opening; it no longer repeats the generic feed.
    check('class="%s"' % marker in page or (marker == "front" and 'class="lf"' in page), "%s has its own layout (%s)" % (route, marker))
    anchors = re.findall(r"<a\b([^>]*)>", page)
    dead = [a for a in anchors if "href=" not in a]
    check(not dead, "%s has no anchor that goes nowhere: %s" % (route, dead[:3]))

# The Work editorial beat must close its heading before opening a story.
# Otherwise nested links deform the rest of the feed and footer.
work_status, work_front = fetch("/", "aurekai.work")
check(work_status == "200", "Work editorial front responds")
beat_open = '<div class="wf-beat"><h2 class="wf-beat-name"><a href="'
if beat_open in work_front:
    endings = re.findall(r'<div class="wf-beat"><h2 class="wf-beat-name"><a href="[^"]+">(?:(?!<a\b).)*?</a></h2>', work_front, flags=re.S)
    check(len(endings) == work_front.count(beat_open), "Work beat headings close before story links")

# A family is an acquisition fact, not a claim that what it carries is a release
# or an advisory.  The contracted publisher reaches these routes; the
# same-family publisher that holds no contract stays out of them, and both are
# still readable wherever a family is enough.
check("Foxhollow ships its second chapter" in pages["/releases"], "a contracted publisher reaches /releases")
check("requests 3.0.0" not in pages["/releases"], "a packages-family publisher with no route contract stays off /releases")
check("Signing keys rotated" in pages["/advisories"], "a contracted publisher reaches /advisories")
check("CVE-2026-0001" not in pages["/advisories"], "a vulnerabilities-family publisher with no route contract stays off /advisories")

# Structure, not class names: each route's body below its header is reduced to
# the shape of its elements (tag nesting, repeats collapsed) and compared.
def signature(page):
    body = page.split("</header>", 1)[-1]
    body = re.sub(r"<(script|style|template)[^>]*>.*?</\1>", "", body, flags=re.S)
    shape, depth = [], 0
    for m in re.finditer(r"<(/?)([a-z0-9]+)\b[^>]*?(/?)>", body):
        closing, tag, selfclose = m.group(1), m.group(2), m.group(3)
        if tag in ("br", "img", "input", "path", "line", "circle", "meta", "link"): shape.append((depth, tag)); continue
        if closing: depth = max(0, depth - 1); continue
        shape.append((depth, tag)); depth += 0 if selfclose else 1
    grams = set()
    for i in range(len(shape) - 2):
        grams.add(tuple(shape[i:i+3]))
    return grams
sigs = {r: signature(pages[r]) for r in ("/discover", "/watch", "/channels", "/community", "/board", "/parcels", "/releases")}
names = list(sigs)
for i in range(len(names)):
    for j in range(i + 1, len(names)):
        a, b = sigs[names[i]], sigs[names[j]]
        sim = len(a & b) / max(1, len(a | b))
        check(sim < 0.6, "%s and %s are structurally different (similarity %.2f)" % (names[i], names[j], sim))

# Public words: machinery stays in code, metadata and operator carriers.
banned = re.compile(r"\b(World|Worlds|Surface|Scene|Closure|Runtime|Semantic|Occurrence|Projection|Extent|Aperture|Frontier|Candidate|MaterialLease|ForeignTwin|AddressPlane)\b")
for route, page in pages.items():
    text = html.unescape(re.sub(r"<[^>]+>", " ", re.sub(r"<(script|style|template)[^>]*>.*?</\1>", " ", page.split("</head>", 1)[-1], flags=re.S)))
    leak = sorted(set(banned.findall(text)))
    check(not leak, "%s shows no machine vocabulary: %s" % (route, leak))

# Each route draws on its own sources.
check("Blender basics" not in pages["/discover"] and "Water Lilies" not in pages["/discover"], "Discover does not repeat Watch or Boards")
check("City opens a new library" not in pages["/watch"], "Watch does not repeat Discover")
check("Blender basics" in pages["/watch"] and "Water Lilies" in pages["/board"], "each route shows its own material")
layouts = [re.search(r'<section class="[^"]*region-([a-z]+)"', pages[r]) for r in ("/discover", "/watch", "/channels", "/community", "/board", "/parcels", "/releases")]
roles = [m.group(1) if m else None for m in layouts]
check(len(set(roles)) == len(roles), "no two routes share a main rhythm: %s" % roles)

# Follow every internal link a person can see.
seen, failures = set(), []
for route, page in pages.items():
    for href in re.findall(r'href="(/[^"#]*)"', page):
        href = html.unescape(href)
        if href in seen or href.startswith("/assets/") or len(seen) > 220: continue
        seen.add(href)
        code, _ = fetch(href)
        if code == "303" and urllib.parse.urlsplit(href).path == "/session/continue":
            # LIFE delegates authentication to WORK. Resolve that exact native
            # chain on the same fixture, never by following a live external URL.
            walk, walk_host = href, "aurekai.life"
            for _ in range(4):
                response = subprocess.run(["curl", "-sS", "--max-time", "5", "-i", "-H", "Host: " + walk_host,
                    "-H", "Accept: text/html", "http://127.0.0.1:%s%s" % (port, walk)], capture_output=True, text=True).stdout
                status = re.match(r"HTTP/[^ ]+ (\d+)", response)
                code = status.group(1) if status else "missing-response"
                if code == "200" and urllib.parse.urlsplit(walk).path == "/login": break
                if code != "303": code = "invalid-session-response"; break
                location = re.search(r"(?im)^Location: ([^\r\n]+)", response)
                target = urllib.parse.urlsplit(location.group(1) if location else "")
                if (target.scheme not in ("", "https") or target.username or target.password or target.fragment or
                    (target.netloc and target.netloc not in ("aurekai.life", "aurekai.work")) or
                    target.path not in ("/session/authorize", "/login")):
                    code = "unsafe-session-redirect"; break
                if target.netloc: walk_host = target.netloc
                walk = target.path + ("?" + target.query if target.query else "")
        if code != "200": failures.append((href, code))
check(len(seen) > 20, "the walk followed many links (%d)" % len(seen))
check(not failures, "every internal link answers: %s" % failures[:6])

# A video opens with its player and its verbs.
video = re.search(r'href="(/o/tube/[0-9a-f]{24})"', pages["/watch"]).group(1)
code, vpage = fetch(video)
check(code == "200", "a video's page answers")
check('<iframe src="https://tube.example.test/videos/embed/abc123"' in vpage or 'videos/embed/def-456' in vpage or 'videos/embed/ghi789' in vpage, "the video plays in place")
check('class="verb primary"' in vpage and "Watch on" in vpage, "the primary verb is to watch it where it lives")
check('href="mailto:?subject=' in vpage, "share works without a script")
check(re.search(r'href="/login\?next=/o/tube/', vpage), "saving asks a person to sign in and comes back")
more = re.search(r'href="(/watch\?from=[^"]+)"><svg class="glyph(?: [^"]*)?"[^>]*>.*?</svg><span>More from ([^<]+)</span>', vpage, re.S)
check(more is not None, "more from the maker is a verb")
if more:
    code, mpage = fetch(html.unescape(more.group(1)))
    open("%s/narrowed.html" % tmp, "w").write(mpage)
    check(code == "200" and "Showing" in mpage, "more from the maker narrows the page")
    makers = re.findall(r'<span class="by"><span>([^<]*)</span>', mpage)
    check(makers and set(makers) == {more.group(2)}, "a narrowed page shows only that maker: %s" % set(makers))

# A picture opens large; a place opens on a map.
pic = re.search(r'href="(/o/museum/[0-9a-f]{24})"', pages["/board"]).group(1)
code, ppage = fetch(pic)
check('class="hero-picture"' in ppage and "View full size" in ppage, "a picture opens large with a way to see it full size")
place = re.search(r'href="(/o/quakes/[0-9a-f]{24})"', pages["/parcels"]).group(1)
code, qpage = fetch(place)
check("openstreetmap.org/?mlat=" in qpage and '<figure class="map">' in qpage, "a place opens on a map with a way to open it in a map app")

code, gone = fetch("/o/tube/" + "0" * 24)
check(code == "404" and "Not here any more" in gone, "a stale item says so plainly")

code, found = fetch("/search?q=blender")
check(code == "200" and "Blender basics" in found and "Water Lilies" not in found, "search finds what it says and nothing else")
code, empty = fetch("/search")
check("Type what you are looking for" in empty, "an empty search invites a search")
check('<form class="ak-find" action="/search"' in pages["/discover"], "every page has a search box")

front = pages["/"]
sections = re.search(r'<nav class="ak-sections"[^>]*>(.*?)</nav>', front, re.S)
check(sections is not None and all(('href="' + path + '"') in sections.group(1)
      for path in ("/discover", "/watch", "/listen", "/board")),
      "the authored LIFE opening retains working entrances to its public sections")

if problems:
    print("page items: FAIL")
    for p in problems: print("  -", p)
    sys.exit(1)
PY
if [ -n "$(printenv BONFYRE_PRICE_ROOM_ONLY 2>/dev/null || true)" ]; then
  before_price=$(sqlite3 "$tmp/prices.db" "SELECT count(*),sum(amount),count(DISTINCT publisher) FROM observed_price")
  PRICE_BROWSER_PORT="$port" PRICE_BROWSER_ROOT="$root" node <<'JS'
const assert=require('node:assert/strict');
const path=require('node:path');
const {createRequire}=require('node:module');
const http=require('node:http');
const root=process.env.PRICE_BROWSER_ROOT,port=Number(process.env.PRICE_BROWSER_PORT);
const puppeteer=createRequire(path.join(root,'tools/creator-ingest/package.json'))('puppeteer');
(async()=>{
 const browser=await puppeteer.launch({headless:true,executablePath:'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',args:['--disable-background-networking','--disable-extensions']});
 const context=await browser.createBrowserContext(),page=await context.newPage(),errors=[],verbs=[];
 try{
  page.on('pageerror',e=>errors.push(e.message));page.setDefaultTimeout(12000);
  await page.setRequestInterception(true);
  page.on('request',async req=>{
   try{
    const url=new URL(req.url());
    if(url.origin!=='https://aurekai.life'||url.pathname!=='/s/prices/observed'){
      await req.respond({status:204,body:''});return;
    }
    verbs.push(req.method());
    const result=await new Promise((resolve,reject)=>{
      const wire=http.request({hostname:'127.0.0.1',port,path:url.pathname+url.search,
        method:req.method(),headers:{Host:'aurekai.life',Accept:'text/html'}},res=>{
          const chunks=[];res.on('data',x=>chunks.push(x));
          res.on('end',()=>resolve({status:res.statusCode,body:Buffer.concat(chunks)}));
        });
      wire.on('error',reject);wire.end();
    });
    await req.respond({status:result.status,contentType:'text/html; charset=utf-8',body:result.body});
   }catch(e){errors.push('Price transport '+e.message);await req.abort().catch(()=>{});}
  });
  const navigate=path=>page.goto('https://aurekai.life'+path,{waitUntil:'domcontentloaded'});
  const advance=f=>Promise.all([page.waitForNavigation({waitUntil:'domcontentloaded'}),f()]);
  await page.setViewport({width:390,height:844});
  const arrival=await navigate('/s/prices/observed');assert.equal(arrival.status(),200);
  assert.match(await page.$eval('body',n=>n.textContent),/Find out what things cost/);
  assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true,'390px opening overflow');
  await advance(()=>page.click('a[href="/s/prices/observed?kind=service"]'));
  assert.equal(await page.$eval('input[name=kind]',n=>n.value),'service');
  assert.equal(await page.$eval('a[href="/s/prices/observed?kind=service"]',n=>n.getAttribute('aria-current')),'page');
  await page.type('.price-find [name=q]','Fixture Comparative Transport');
  await advance(()=>page.click('.price-find button'));
  const comparison=await page.$eval('[data-price-comparison]',n=>n.textContent);
  for(const good of ['Same-day publisher comparisons','Comparative Low','Comparative High','10.00 USD','15.00 USD'])
    assert(comparison.includes(good),'Missing comparison '+good);
  for(const bad of ['800.00','700.00','600.00','500.00'])
    assert(!comparison.includes(bad),'Incompatible observation in comparison');
  assert.match(comparison,/not live checkout offers or guaranteed savings/);
  assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true,'390px comparison overflow');
  assert.equal(await page.$eval('a[href="https://prices.example.test/comparative-low"]',n=>n.getAttribute('href')),'https://prices.example.test/comparative-low');
  await navigate('/s/prices/observed?kind=service&q=Fixture%20Paged');
  let body=await page.$eval('body',n=>n.textContent);
  assert(body.includes('Fixture Paged 000')&&!body.includes('Fixture Paged 060'));
  await advance(()=>page.click('.price-pages a:last-child'));
  body=await page.$eval('body',n=>n.textContent);
  assert(body.includes('Fixture Paged 060')&&!body.includes('Fixture Paged 000'));
  assert.match(page.url(),/offset=60/);
  await page.setViewport({width:1365,height:900});
  assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true,'desktop overflow');
  assert(verbs.length>=5&&verbs.every(x=>x==='GET'),'unexpected write method');
  assert.deepEqual(errors,[]);
  console.log('PRICE_BROWSER_PASS: categories, same-day comparisons, provenance, paging, mobile/desktop, no writes');
 }finally{await page.close();await context.close();await browser.close();}
})().catch(e=>{console.error(e);process.exitCode=1;});
JS
  after_price=$(sqlite3 "$tmp/prices.db" "SELECT count(*),sum(amount),count(DISTINCT publisher) FROM observed_price")
  [ "$before_price" = "$after_price" ] || { echo 'PRICE_BROWSER_FAIL: price ledger mutated' >&2; exit 1; }
fi
echo 'page items: PASS'
