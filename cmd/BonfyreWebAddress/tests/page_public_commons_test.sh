#!/bin/sh
# The public commons beyond Wikimedia reach pages through the same contract:
#   - an Internet Archive film or concert plays in the Archive's own player,
#     and a concert with no file of its own still offers Play on its page;
#   - a licence address reads as words and as what it lets a person do;
#   - a podcast episode plays its enclosure (never mistaken for a picture) and
#     states its length, guests, transcript, chapters and conversation;
#   - an iNaturalist original is asked for at the size each slot needs, and
#     credited with its terms;
#   - a many-item rhythm takes turns across sources;
#   - one identity has a page: its report, the pictures that depict it and
#     where it is; a picture says what it shows;
#   - recordings that sound alike are linked from the atlas, and one gone from
#     its source is not;
#   - a page's history is shown from a retained Wayback answer.
set -eu
web=${1:?web-address binary required}
case "$web" in /*) ;; *) web="$(pwd)/$web" ;; esac
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
cms="$root/cmd/BonfyreCMS/bonfyre-cms"
atlas="$root/cmd/BonfyreLatentAtlas/bonfyre-latent-atlas"
[ -x "$cms" ] || { echo 'page public commons: SKIP (cms not built)'; exit 0; }
[ -x "$atlas" ] || { echo 'page public commons: SKIP (latent atlas not built)'; exit 0; }
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-page-public-commons.XXXXXX")
pid=''
cleanup() { [ -n "$pid" ] && kill "$pid" 2>/dev/null || true; rm -rf "$tmp"; }
trap cleanup EXIT INT TERM

schemas="$tmp/schemas"
mkdir -p "$schemas" "$tmp/foreign/receipts" "$tmp/foreign/bodies"
for type in world world_fragment internet_source publication_occurrence content_flag; do cp "$root/cmd/BonfyreCMS/content-types/$type.json" "$schemas/"; done
cmsdb="$tmp/cms.db"
"$cms" schema migrate --db "$cmsdb" --schemas "$schemas" >/dev/null
bun "$root/cmd/BonfyreWebAddress/tools/apply_census_sources.mjs" --cms "$cms" --db "$cmsdb" --schemas "$schemas" >/dev/null
body() { printf '%s\n' "{\"body_path\":\"$tmp/foreign/bodies/$1.body\"}" >"$tmp/foreign/receipts/$1.json"; cat >"$tmp/foreign/bodies/$1.body"; }

body archive-feature-films <<'JSON'
{"response":{"docs":[
 {"identifier":"his_girl_friday","title":"His Girl Friday","year":"1940","creator":"Howard Hawks","description":"A newspaper editor uses every trick to keep his ex-wife.","licenseurl":"http://creativecommons.org/publicdomain/mark/1.0/"},
 {"identifier":"house_on_haunted_hill_ipod","title":"House on Haunted Hill","year":"1959","creator":"William Castle","licenseurl":"http://creativecommons.org/licenses/publicdomain/"}
]}}
JSON
body archive-dallas-live-music <<'JSON'
{"response":{"docs":[
 {"identifier":"jj2026-08-30.dpa4560","title":"Jack Johnson Live at Dos Equis Pavilion on 2026-08-30","creator":"Jack Johnson","date":"2026-08-30T00:00:00Z","venue":"Dos Equis Pavilion"},
 {"identifier":"sci2026-05-31.seedy.MK41","title":"String Cheese Incident Live at Longhorn Ballroom on 2026-05-31","creator":["String Cheese Incident"],"date":"2026-05-31T00:00:00Z","venue":"Longhorn Ballroom"}
]}}
JSON
body archive-great-78 <<'JSON'
{"response":{"docs":[
 {"identifier":"78_house-of-the-rising-sun_josh-white","title":"House Of The Rising Sun","creator":"Josh White and his Guitar","year":"1942"},
 {"identifier":"78_mr-sandman_the-chordettes","title":"Mr. Sandman","creator":["The Chordettes","Archie Bleyer"],"year":"1954"},
 {"identifier":"78_st-louis-blues_bessie-smith","title":"St. Louis Blues","creator":"Bessie Smith","year":"1925"}
]}}
JSON
body podcasting-2-0 <<'XML'
<?xml version="1.0"?><rss xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd" xmlns:podcast="https://podcastindex.org/namespace/1.0"><channel><title>Podcasting 2.0</title>
<item><title>Episode 270: YOLO Guy</title><link>https://podcastindex.org/podcast/920666?episode=270</link><pubDate>Fri, 11 Sep 2026 20:00:00 GMT</pubDate>
<description><![CDATA[<p>Dave &amp; Adam talk about the <b>podcast rig</b>&nbsp;&mdash; and more.</p>]]></description>
<enclosure url="https://mp3s.example.test/PC20-270.mp3" length="90000000" type="audio/mpeg"/>
<itunes:image href="https://images.example.test/pc20.jpg"/><itunes:author>Podcast Index LLC</itunes:author><itunes:duration>01:33:58</itunes:duration>
<podcast:transcript url="https://mp3s.example.test/PC20-270-transcript.srt" type="application/srt"/>
<podcast:chapters url="https://mp3s.example.test/PC20-270-chapters.json" type="application/json+chapters"/>
<podcast:person role="host" href="https://podcastindex.org">Adam Curry</podcast:person><podcast:person role="host">Dave Jones</podcast:person>
<podcast:location geo="geo:30.2672,-97.7431">Austin, Texas</podcast:location>
<podcast:socialInteract uri="https://podcastindex.social/@dave/111" protocol="activitypub"/>
</item></channel></rss>
XML
body buzzcast </dev/null
body gbif-inaturalist-dfw <<'JSON'
{"results":[
 {"species":"Bubo virginianus","scientificName":"Bubo virginianus (Gmelin, 1788)","eventDate":"2026-09-10T09:05","references":"https://www.inaturalist.org/observations/1001","recordedBy":"Brent Franklin","decimalLatitude":32.8,"decimalLongitude":-96.9,
  "media":[{"identifier":"https://inaturalist-open-data.s3.amazonaws.com/photos/713951976/original.jpg","license":"http://creativecommons.org/licenses/by-nc/4.0/"}]},
 {"species":"Danaus gilippus","scientificName":"Danaus gilippus","eventDate":"2026-09-09T12:03","references":"https://www.inaturalist.org/observations/1002","recordedBy":"ashleyrsteel","decimalLatitude":32.7,"decimalLongitude":-97.1,
  "media":[{"identifier":"https://inaturalist-open-data.s3.amazonaws.com/photos/721083982/original.jpeg","license":"http://creativecommons.org/licenses/by/4.0/"}]},
 {"species":"Ardea herodias","scientificName":"Ardea herodias","eventDate":"2026-09-08T08:00","references":"https://www.inaturalist.org/observations/1003","recordedBy":"B","decimalLatitude":32.9,"decimalLongitude":-96.8,
  "media":[{"identifier":"https://inaturalist-open-data.s3.amazonaws.com/photos/700000001/original.jpg","license":"http://creativecommons.org/licenses/by/4.0/"}]}
]}
JSON
body commons-dfw-rated <<'JSON'
{"head":{"vars":[]},"results":{"bindings":[
 {"file":{"value":"https://commons.wikimedia.org/entity/M5"},"caption":{"value":"Dallas skyline at dusk"},"name":{"value":"Dallas_dusk.jpg"},"picture":{"value":"https://commons.wikimedia.org/wiki/Special:FilePath/Dallas_dusk.jpg?width=500"},"page":{"value":"https://commons.wikimedia.org/wiki/File:Dallas_dusk.jpg"},"author":{"value":"Fkbowen"},"license":{"value":"CC BY-SA 4.0"},"deed":{"value":"https://creativecommons.org/licenses/by-sa/4.0/"},"geometry":{"value":"POINT(-96.80 32.78)"},"taken":{"value":"2020-03-09T00:00:00Z"},"depicts":{"value":"http://www.wikidata.org/entity/Q16557"}},
 {"file":{"value":"https://commons.wikimedia.org/entity/M6"},"caption":{"value":"Reunion Tower"},"name":{"value":"Reunion.jpg"},"picture":{"value":"https://commons.wikimedia.org/wiki/Special:FilePath/Reunion.jpg?width=500"},"page":{"value":"https://commons.wikimedia.org/wiki/File:Reunion.jpg"},"author":{"value":"Radomianin"},"license":{"value":"CC BY-SA 4.0"},"deed":{"value":"https://creativecommons.org/licenses/by-sa/4.0/"},"geometry":{"value":"POINT(-96.81 32.77)"},"taken":{"value":"2019-08-28T00:00:00Z"}}
]}}
JSON
body wikidata-places-dfw <<'JSON'
{"head":{"vars":[]},"results":{"bindings":[
 {"place":{"value":"http://www.wikidata.org/entity/Q16557"},"name":{"value":"Dallas"},"about":{"value":"city in the state of Texas"},"picture":{"value":"https://commons.wikimedia.org/wiki/Special:FilePath/Dallas%20Skyline.jpg?width=500"},"geometry":{"value":"POINT(-96.808889 32.779167)"},"page":{"value":"https://en.wikipedia.org/wiki/Dallas"}},
 {"place":{"value":"http://www.wikidata.org/entity/Q16558"},"name":{"value":"Fort Worth"},"about":{"value":"city in Tarrant County"},"picture":{"value":"https://commons.wikimedia.org/wiki/Special:FilePath/Fort%20Worth.jpg?width=500"},"geometry":{"value":"POINT(-97.3325 32.756389)"},"page":{"value":"https://en.wikipedia.org/wiki/Fort_Worth,_Texas"}},
 {"place":{"value":"http://www.wikidata.org/entity/Q17943"},"name":{"value":"Arlington"},"about":{"value":"city in Tarrant County"},"picture":{"value":"https://commons.wikimedia.org/wiki/Special:FilePath/Arlington.jpg?width=500"},"geometry":{"value":"POINT(-97.122839 32.705033)"},"page":{"value":"https://en.wikipedia.org/wiki/Arlington,_Texas"}}
]}}
JSON

# The atlas: three 78s projected in the spectrum chart, two alike.
python3 - "$atlas" "$tmp" <<'PY'
import hashlib, json, subprocess, sys
atlas, tmp = sys.argv[1], sys.argv[2]
def key(source, href): return hashlib.sha256(("%s\n%s" % (source, href)).encode()).hexdigest()[:24]
ids = ["78_house-of-the-rising-sun_josh-white", "78_mr-sandman_the-chordettes", "78_st-louis-blues_bessie-smith"]
spectra = [[1.0] * 8 + [0.1] * 16, [0.95] * 8 + [0.12] * 16, [0.1] * 16 + [1.0] * 8]
lines = [{"chart": {"id": "spectrum-24", "family": "distribution", "dims": 24, "version": "log24-v1", "encoder": "fixture", "universe": "u"}}]
for i, s in zip(ids, spectra):
    a = "o/archive-great-78/" + key("archive-great-78", "https://archive.org/details/" + i)
    lines.append({"project": {"address": a, "chart": "spectrum-24", "values": s, "source_digest": "d-" + i}})
    lines.append({"relate": {"src": a, "relation": "member_of", "dst": "group:78-rpm-records@spectrum-24", "evidence": "fixture"}})
open(tmp + "/atlas.jsonl", "w").write("\n".join(json.dumps(l) for l in lines) + "\n")
subprocess.run([atlas, "import", "--db", tmp + "/atlas.db", "--jsonl", tmp + "/atlas.jsonl"], check=True, capture_output=True)
PY

# A retained Wayback answer for Dallas's article.
twin=$(python3 -c "import hashlib; print('history-' + hashlib.sha256(b'https://en.wikipedia.org/wiki/Dallas').hexdigest()[:24])")
body "$twin" <<'JSON'
[["timestamp","statuscode","digest","length"],["20120101000000","200","AAA","1"],["20180704120000","200","BBB","1"],["20260901083000","301","CCC","1"]]
JSON

# A city's own voice, compiled rather than authored: Dallas publishes it.
"$cms" entry create internet_source '{"family":"local-voices","source":"auto-dallas-q16557","label":"Dallas","says":"what Dallas publishes","url":"https://dallascityhall.example/RSSFeed.aspx","twin":"auto-dallas-q16557","state":"observed","extract":"rss:item","title_field":"title","status":"published","tier":"COLD","kind":"text","publisher_ident":"Q16557","publisher_name":"Dallas","compiled_by":"live-source-compiler"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
body auto-dallas-q16557 <<'XML'
<?xml version="1.0"?><rss><channel><title>Dallas News Flash</title><language>en-us</language>
<item><title>Trinity River trail reopens between Dallas and Fort Worth</title><link>https://dallascityhall.example/news/trail</link><pubDate>Wed, 16 Sep 2026 15:00:00 GMT</pubDate><description>The regional trail connecting the two cities reopened Tuesday.</description></item>
<item><title>Library hours extended for the fall semester</title><link>https://dallascityhall.example/news/library</link><pubDate>Tue, 15 Sep 2026 15:00:00 GMT</pubDate><description>Branches stay open later on weekdays.</description></item>
<item><title>Library hours extended at branch libraries this fall</title><link>https://dallascityhall.example/news/branches</link><pubDate>Mon, 14 Sep 2026 15:00:00 GMT</pubDate><description>More branch library hours on weekdays.</description></item>
</channel></rss>
XML

# The standing automation's daily work, run once: items, words chart, identity links.
BONFYRE_CMS_DB="$cmsdb" BONFYRE_FOREIGN_ROOT="$tmp/foreign" "$web" census-items >"$tmp/items.jsonl" 2>/dev/null
"$atlas" project-items --db "$tmp/atlas.db" --items "$tmp/items.jsonl" >/dev/null
bun "$root/cmd/BonfyreLatentAtlas/tools/link_identities.mjs" --items "$tmp/items.jsonl" --cms "$cmsdb" --atlas "$atlas" --db "$tmp/atlas.db" >/dev/null

# Evidence kept smaller is still evidence: a body compressed where it lies is
# read exactly as the raw one was.
gzip -9 "$tmp/foreign/bodies/archive-feature-films.body"

"$web" bootstrap-owned --db "$tmp/matrix.db" >/dev/null
port=$(python3 -c "import socket; s=socket.socket(); s.bind(('127.0.0.1',0)); print(s.getsockname()[1]); s.close()")
BONFYRE_CMS_DB="$cmsdb" BONFYRE_CMS_SCHEMAS="$schemas" BONFYRE_FOREIGN_ROOT="$tmp/foreign" BONFYRE_LATENT_ATLAS_DB="$tmp/atlas.db" \
BONFYRE_PROJECT_BINARY=/nonexistent/bonfyre-project \
  "$web" serve --db "$tmp/matrix.db" --auth-db "$tmp/auth.db" --port "$port" --site-root "$root/site/matrix" >"$tmp/server.log" 2>&1 &
pid=$!
attempt=0
until curl -sS -o /dev/null -H 'Host: aurekai.life' "http://127.0.0.1:$port/healthz" 2>/dev/null; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 80 ] || ! kill -0 "$pid" 2>/dev/null; then cat "$tmp/server.log" >&2; echo 'page public commons: server did not start' >&2; exit 1; fi
  sleep 0.05
done

python3 - "$port" "$tmp" <<'PY' || exit 1
import re, subprocess, sys, html as H
port, tmp = sys.argv[1], sys.argv[2]
base = "http://127.0.0.1:%s" % port
problems = []
def check(ok, why):
    if not ok: problems.append(why)
def get(path, method="GET", data=None, origin=None):
    cmd = ["curl", "-sS", "-i", "-X", method, "-H", "Host: aurekai.life", "-H", "Accept: text/html"]
    if origin: cmd += ["-H", "Origin: " + origin]
    if data is not None: cmd += ["--data", data]
    out = subprocess.run(cmd + [base + path], capture_output=True).stdout.decode("utf-8", "replace")
    head, _, body = out.partition("\r\n\r\n")
    return (head.split(" ")[1] if " " in head else ""), head, body
def item(page, source):
    m = re.search(r'href="(/o/%s/[0-9a-f]{24})"' % re.escape(source), page)
    return m.group(1) if m else None

_, _, watch = get("/watch")
film = item(watch, "archive-feature-films")
check(film is not None, "an Archive film is on Watch")
if film:
    _, _, page = get(film)
    check('src="https://archive.org/embed/' in page and "<iframe" in page, "an Archive film plays in the Archive's own player")
    check("See it at Internet Archive" in page, "its link out is named by where it goes")
    check("Public domain" in page and "You may use it in something you sell." in page, "a licence address reads as words and as what it lets a person do")

_, _, listen = get("/listen")
check('class="play-on-page"' in listen, "a recording without a file of its own offers Play on its page")
check("String Cheese Incident" in listen, "a maker given as a list reads as a name")
concert = item(listen, "archive-dallas-live-music")
if concert:
    _, _, page = get(concert)
    check("https://archive.org/embed/" in page, "a concert plays in the Archive's player")

_, _, pods = get("/podcasts")
ep = item(pods, "podcasting-2-0")
check(ep is not None and "<audio" in pods, "an episode is on Podcasts with its audio")
if ep:
    _, _, page = get(ep)
    check('src="https://mp3s.example.test/PC20-270.mp3"' in page, "the enclosure plays")
    check('src="https://mp3s.example.test/PC20-270.mp3"' not in page.split("<audio")[0], "the enclosure is never used as a picture")
    check("1:33:58" in page and "Adam Curry, Dave Jones" in page and "Austin, Texas" in page, "the episode states its length, guests and place")
    check("Read the transcript" in page and "Chapters" in page and "Join the conversation" in page, "transcript, chapters and conversation are verbs")
    check("&amp;nbsp;" not in page and "&nbsp;" not in page and "—" in H.unescape(page), "typographic entities read as characters")

_, _, near = get("/nearby")
check("photos/713951976/medium.jpg" in near, "an iNaturalist original is asked for at a page's size")
check("original.jpg" not in near, "no full-size original is put on a page")
photos = re.search(r'<section class="[^"]*region-photos".*?</section>', near, re.S)
check(photos and "/o/gbif-inaturalist-dfw/" in photos.group(0) and "/o/commons-dfw-rated/" in photos.group(0), "the photo field takes turns across sources")
sighting = item(near, "gbif-inaturalist-dfw")
if sighting:
    _, _, page = get(sighting)
    check("photos/713951976/large.jpg" in page or "photos/721083982/large.jpeg" in page or "photos/700000001/large.jpg" in page, "the sighting's page asks for the large size, keeping the file's own extension")
    check("Picture from" in page and "iNaturalist" in page, "a sighting credits iNaturalist")

code, _, about = get("/about/Q16557")
check(code == "200" and "<h1>Dallas</h1>" in about, "an identity has a page named by its report")
check("Dallas skyline at dusk" in about and "region-wall" in about, "the page shows the pictures that depict it")
check("plate streets" in about, "and where it is")
code, _, _ = get("/about/Q424242")
check(code == "404", "an identity nothing names has no page")
dusk = re.search(r'href="(/o/commons-dfw-rated/[0-9a-f]{24})"', about)
if dusk:
    _, _, page = get(dusk.group(1))
    check("<span>Shows Dallas</span>" in page, "a picture says what it shows")
place = item(near, "wikidata-places-dfw")
_, _, dallas = get("/o/wikidata-places-dfw/" + __import__("hashlib").sha256(b"wikidata-places-dfw\nhttps://en.wikipedia.org/wiki/Dallas").hexdigest()[:24])
check("Pictures of it" in dallas and "Everything about this" in dallas, "a place's page shows pictures of it and everything about it")

key = lambda i: __import__("hashlib").sha256(("archive-great-78\nhttps://archive.org/details/" + i).encode()).hexdigest()[:24]
_, _, rising = get("/o/archive-great-78/" + key("78_house-of-the-rising-sun_josh-white"))
similar = re.search(r'Sounds similar.*?</section>', rising, re.S)
check(similar and "Mr. Sandman" in similar.group(0), "a recording links the one that sounds alike")
check(similar and similar.group(0).find("Mr. Sandman") < similar.group(0).find("St. Louis Blues") if similar and "St. Louis Blues" in similar.group(0) else bool(similar), "the nearest sound comes first")
body = open(tmp + "/foreign/bodies/archive-great-78.body").read()
open(tmp + "/foreign/bodies/archive-great-78.body", "w").write(body.replace('{"identifier":"78_mr-sandman_the-chordettes","title":"Mr. Sandman","creator":["The Chordettes","Archie Bleyer"],"year":"1954"},', ""))
_, _, rising = get("/o/archive-great-78/" + key("78_house-of-the-rising-sun_josh-white"))
check("Mr. Sandman" not in rising, "a neighbour gone from its source is not linked")

code, _, changes = get("/o/wikidata-places-dfw/" + __import__("hashlib").sha256(b"wikidata-places-dfw\nhttps://en.wikipedia.org/wiki/Dallas").hexdigest()[:24] + "/changes")
check(code == "200" and "change 3 times between 2012 and 2026" in changes, "a page's history is told from its retained captures (%s)" % code)
check('href="https://web.archive.org/web/20180704120000/https://en.wikipedia.org/wiki/Dallas"' in changes, "each change links to the Archive's copy")
check("answered 301" in changes, "a capture that answered differently says so")
check("How this page changed" in dallas, "an item that links out offers its history")
code, head, _ = get("/o/changes", method="POST", data="item=wikidata-places-dfw/" + __import__("hashlib").sha256(b"wikidata-places-dfw\nhttps://en.wikipedia.org/wiki/Dallas").hexdigest()[:24], origin="https://attacker.example")
check(code == "403", "a history lookup from another site is refused")

_, _, dallas_about = get("/about/Q16557")
voices = re.search(r'aria-label="In its own words".*?</section>', dallas_about, re.S)
check(voices and "Trinity River trail reopens" in voices.group(0), "an identity page carries what that organization publishes itself")
_, _, fort_worth = get("/about/Q16558")
mentioned = re.search(r'Mentioned in.*?</section>', fort_worth, re.S)
check(mentioned and "Trinity River trail reopens" in mentioned.group(0), "a place's page lists the posts that mention it")
trail = re.search(r'href="(/o/auto-dallas-q16557/[0-9a-f]{24})"[^>]*>(?:<b>)?Trinity', voices.group(0) if voices else "")
trail = trail or re.search(r'href="(/o/auto-dallas-q16557/[0-9a-f]{24})"', dallas_about)
if trail:
    _, _, post = get(trail.group(1))
    check("More from Dallas" in post, "a post links the organization that published it")
    check("Mentions Fort Worth" in post, "a post links the places it mentions")
    check("Mentions Dallas" not in post, "an organization's own post does not mention itself")
library = re.search(r'href="(/o/auto-dallas-q16557/[0-9a-f]{24})"[^>]*>(?:<b>)?Library hours extended for the fall', dallas_about)
if library:
    _, _, post = get(library.group(1))
    related = re.search(r'<h2 class="region-label">Related</h2>.*?</section>', post, re.S)
    check(related and "branch libraries" in related.group(0), "related posts come from the atlas's words chart")
_, _, near2 = get("/nearby")
check("Trinity River trail reopens" in near2, "Nearby carries what the area's own organizations are saying")

if problems:
    print("page public commons: FAIL")
    for p in problems: print("  -", p)
    sys.exit(1)
PY
echo 'page public commons: PASS'
