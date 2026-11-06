#!/bin/sh
# A subject's page is composed from the material it has, in reading rhythms
# people already know, and it says nothing about the machinery.  This fixture
# gives one subject stories from two feeds, video, recordings and a
# conversation, and proves each kind lands in its own topology, in order, with
# human words -- and that a subject with only stories grows no empty shelves.
set -eu

web=${1:?web-address binary required}
case "$web" in /*) ;; *) web="$(pwd)/$web" ;; esac
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
cms="$root/cmd/BonfyreCMS/bonfyre-cms"
[ -x "$cms" ] || { echo 'page composition: SKIP (cms not built)'; exit 0; }

tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-page-composition.XXXXXX")
pid=''
# PAGE_COMPOSITION_KEEP=<dir> keeps the rendered pages for a person to look at.
cleanup() {
  [ -n "$pid" ] && kill "$pid" 2>/dev/null || true
  [ -n "${PAGE_COMPOSITION_KEEP:-}" ] && cp "$tmp"/*.html "$PAGE_COMPOSITION_KEEP"/ 2>/dev/null || true
  rm -rf "$tmp"
}
trap cleanup EXIT INT TERM
fail() { echo "page composition: FAIL: $1" >&2; exit 1; }

schemas="$tmp/schemas"
mkdir -p "$schemas" "$tmp/foreign/receipts" "$tmp/foreign/bodies"
for type in world world_fragment internet_source publication_occurrence community_body community_post music_recording media_program; do
  cp "$root/cmd/BonfyreCMS/content-types/$type.json" "$schemas/"
done
cmsdb="$tmp/cms.db"
"$cms" schema migrate --db "$cmsdb" --schemas "$schemas" >/dev/null
entry() { "$cms" entry create "$1" "$2" --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null; }

entry world '{"world":"tonight-proof","title":"Tonight near Deep Ellum","character":"zine","template":"ghost-wire","lede":"Shows, sets and what people are saying after dark.","subject":"place:deep-ellum","sources":"ellum-listings,city-desk","grammars":"stage,board,community,unlabelled-internal","cannot_infer":"a listing is not a ticket","status":"published"}'
entry world '{"world":"stories-only","title":"City desk","character":"journal","lede":"Local reporting.","subject":"topic:city","sources":"city-desk","grammars":"","cannot_infer":"a story is not an Aurekai claim","status":"published"}'
entry world '{"world":"quiet-proof","title":"Quiet street","character":"almanac","lede":"Nothing published yet.","subject":"place:quiet","sources":"","grammars":"","cannot_infer":"an empty page is not an empty street","status":"published"}'

frag() { entry world_fragment "{\"fragment_id\":\"tonight-proof:$1\",\"world\":\"tonight-proof\",\"ordinal\":$2,\"subject_address\":\"$3\",\"surface_grammar\":\"$4\",\"renderer_family\":\"$4\",\"renderer_variant\":\"page\",\"extent\":\"page\",\"source_projection\":\"/_surface/world-material\",\"visible_sections\":\"page\",\"cannot_infer\":\"a fragment is not the subject\",\"status\":\"published\"}"; }
frag stage 1 place:deep-ellum stage
frag board 2 place:deep-ellum board
frag community 3 community:ellum-regulars community
frag walkthrough 4 program:trees-walkthrough watch
frag soundcheck 5 program:soundcheck watch
frag late-set 6 recording:late-set listen
frag opener 7 recording:opener listen
frag community-again 8 community:ellum-regulars community

entry community_body '{"community":"ellum-regulars","subject_address":"community:ellum-regulars","title":"Deep Ellum regulars","description":"People who go out here.","visibility":"public","governance_scope":"public","moderation_policy":"accepted posts only","cannot_infer":"a body is not a venue","status":"published"}'
entry community_post '{"post":"p1","community":"ellum-regulars","author_subject":"person:maya-lee","title":"Doors moved to 9","body":"Line is already down the block.","published_at":"2026-09-08T21:30:00Z","visibility":"public","moderation_state":"accepted","cannot_infer":"a post is not a schedule","status":"published"}'
entry community_post '{"post":"p2","community":"ellum-regulars","author_subject":"person:sam","title":"","body":"Anyone headed to It Will Do after?","published_at":"2026-09-08T22:10:00Z","visibility":"public","moderation_state":"accepted","cannot_infer":"a post is not a plan","status":"published"}'
entry community_post '{"post":"p3","community":"ellum-regulars","author_subject":"person:hidden","title":"Pending","body":"Not yet accepted.","published_at":"2026-09-08T22:20:00Z","visibility":"public","moderation_state":"pending","cannot_infer":"pending is not speech","status":"published"}'

entry media_program '{"program_id":"trees-walkthrough","subject_ref":"venue:trees-walkthrough","media_kind":"video","asset_ref":"/assets/trees-walkthrough.mp4","poster_asset_ref":"/assets/trees-walkthrough.jpg","duration_ms":94000,"publication_state":"published","cannot_infer":"a clip is not attendance","status":"published"}'
entry media_program '{"program_id":"soundcheck","subject_ref":"set:soundcheck","media_kind":"video","asset_ref":"/assets/soundcheck.mp4","duration_ms":41000,"publication_state":"published","cannot_infer":"a clip is not attendance","status":"published"}'
entry music_recording '{"recording_id":"late-set","work_ref":"work:late-set","title":"Late Set","primary_artist_ref":"artist:nine-volt-collective","duration_ms":192000,"cannot_infer":"a recording is not a right","status":"published"}'
entry media_program '{"program_id":"late-set:audio","subject_ref":"recording:late-set","media_kind":"audio","asset_ref":"/assets/late-set.mp3","publication_state":"published","cannot_infer":"a program is not playback","status":"published"}'
entry music_recording '{"recording_id":"opener","work_ref":"work:opener","title":"Opener","primary_artist_ref":"artist:rail-and-ember","duration_ms":61000,"cannot_infer":"a recording is not a right","status":"published"}'
entry media_program '{"program_id":"opener:audio","subject_ref":"recording:opener","media_kind":"audio","asset_ref":"/assets/opener.mp3","publication_state":"published","cannot_infer":"a program is not playback","status":"published"}'

entry internet_source '{"family":"publication","source":"ellum-listings","label":"realtime listings","says":"What venues announce","url":"https://www.ellumlistings.test/feed.xml","twin":"ellum-twin","state":"observed","extract":"rss:item","title_field":"title","meta_field":"pubDate","link_field":"link","body_regime":"retained-public","status":"published","tier":"warm","kind":"publication"}'
entry internet_source '{"family":"publication","source":"city-desk","label":"City Desk","says":"Local reporting","url":"https://api.citydesk.test/v1/stories.json","twin":"city-twin","state":"observed","extract":"stories","title_field":"headline","meta_field":"published","link_field":"url","body_regime":"retained-public","status":"published","tier":"warm","kind":"publication"}'
entry internet_source '{"family":"video","source":"grid-video","label":"Video desk","says":"Published clips","url":"https://grid.test/videos.json","twin":"grid-twin","state":"observed","extract":"items","title_field":"title","meta_field":"published","link_field":"url","image_field":"image","sub_field":"author","body_regime":"retained-public","status":"published","tier":"warm","kind":"video"}'

# The lead must be the newest story across both feeds, and its time must read
# the way feeds say it.  Two hours ago is generated, not hard-coded.
recent=$(python3 -c 'import datetime as d; print((d.datetime.now(d.timezone.utc)-d.timedelta(hours=2,minutes=5)).strftime("%a, %d %b %Y %H:%M:%S +0000"))')
{
  printf '<?xml version="1.0"?><rss><channel>'
  printf '<item><title>Nine Volt Collective adds a late set</title><link>https://example.test/late-set</link><pubDate>%s</pubDate></item>' "$recent"
  i=1
  while [ "$i" -le 9 ]; do
    printf '<item><title>Listing %s at Trees</title><link>https://example.test/listing-%s</link><pubDate>Mon, 0%s Sep 2026 20:00:00 GMT</pubDate></item>' "$i" "$i" "$i"
    i=$((i + 1))
  done
  printf '<item><title>Shared story from both feeds</title><link>https://example.test/shared</link><pubDate>Tue, 08 Sep 2026 12:00:00 GMT</pubDate></item>'
  printf '</channel></rss>'
} >"$tmp/foreign/bodies/ellum.body"
cat >"$tmp/foreign/bodies/city.body" <<'JSON'
{"stories":[
 {"headline":"Elm Street closes to cars on weekends","published":"2026-09-07T18:00:00-05:00","url":"https://example.test/elm"},
 {"headline":"Shared story from both feeds","published":"2026-09-08T12:00:00Z","url":"https://example.test/shared"},
 {"headline":"Transit adds late trains","published":"2026-09-06T09:00:00Z","url":"https://example.test/trains"},
 {"headline":"Parks board meets Thursday","published":"2026-09-05T09:00:00Z","url":"https://example.test/parks"},
 {"headline":"Rain expected after 11","published":"2026-09-04T09:00:00Z","url":"https://example.test/rain"},
 {"headline":"Undated item stays last","url":"https://example.test/undated"}
]}
JSON
printf '%s\n' "{\"body_path\":\"$tmp/foreign/bodies/ellum.body\"}" >"$tmp/foreign/receipts/ellum-twin.json"
printf '%s\n' "{\"body_path\":\"$tmp/foreign/bodies/city.body\"}" >"$tmp/foreign/receipts/city-twin.json"
# /watch uses the video grid; the World above uses the separate video shelf.
cat >"$tmp/foreign/bodies/grid.body" <<'JSON'
{"items":[{"title":"A walk through the studio","published":"2024-11-27T12:00:00Z","url":"https://grid.test/studio","image":"https://grid.test/studio.jpg","author":"Maya Chen"}]}
JSON
printf '%s\n' "{\"body_path\":\"$tmp/foreign/bodies/grid.body\"}" >"$tmp/foreign/receipts/grid-twin.json"

matrix="$tmp/matrix.db"
"$web" bootstrap-owned --db "$matrix" >/dev/null
port=$(python3 -c "import socket; s=socket.socket(); s.bind(('127.0.0.1',0)); print(s.getsockname()[1]); s.close()")
BONFYRE_CMS_BINARY="$cms" BONFYRE_CMS_DB="$cmsdb" BONFYRE_CMS_SCHEMAS="$schemas" \
BONFYRE_FOREIGN_ROOT="$tmp/foreign" \
  "$web" serve --db "$matrix" --auth-db "$tmp/auth.db" --port "$port" \
  --site-root "$root/site/matrix" >"$tmp/server.log" 2>&1 &
pid=$!
base="http://127.0.0.1:$port"
attempt=0
until curl -sS -o /dev/null -H 'Host: aurekai.live' "$base/healthz" 2>/dev/null; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 80 ] || ! kill -0 "$pid" 2>/dev/null; then cat "$tmp/server.log" >&2; fail 'server did not become ready'; fi
  sleep 0.05
done

get() { curl -sS -H 'Host: aurekai.live' -H 'Accept: text/html' "$base$1"; }
get /w/tonight-proof >"$tmp/page.html"
get /w/stories-only >"$tmp/stories.html"
get /w/quiet-proof >"$tmp/quiet.html"
get /watch >"$tmp/grid.html"

python3 - "$tmp/page.html" "$tmp/stories.html" "$tmp/quiet.html" "$tmp/grid.html" <<'PY' || exit 1
import html, re, sys

page, stories, quiet, grid = (open(p, encoding="utf-8").read() for p in sys.argv[1:5])
problems = []
def check(ok, why):
    if not ok: problems.append(why)

def visible(doc):
    body = doc.split("</head>", 1)[-1]
    # Scene compatibility data is an inert template, like the page's metadata:
    # it is not rendered or exposed as page copy.
    body = re.sub(r"<(script|style|template)[^>]*>.*?</\1>", " ", body, flags=re.S)
    return html.unescape(re.sub(r"<[^>]+>", " ", body))

# The page names the subject, not the kind of software object it is.
check("<title>Tonight near Deep Ellum</title>" in page, "title is the subject")
check("A world" not in page.split("</head>")[0], "template title replaced")
check('name="description" content="Shows, sets and what people are saying after dark."' in page, "description is the lede")
check('data-character="zine"' in page, "character stays declared in markup")
check('data-template="ghost-wire"' in page, "template stays declared in markup")
check('name="bonfyre:cannot-infer" content="a listing is not a ticket"' in page, "cannot-infer travels as metadata")
# Material is server-rendered. The shared account updater has its own scope;
# the two home-only enhancements must return before touching this World page.
# Isolate the player when enforcing its no-fetch/no-content-renderer contract.
script_tags = re.findall(r"<script\b([^>]*)>(.*?)</script>", page, re.S)
external = [(attrs, body) for attrs, body in script_tags if re.search(r'\bsrc\s*=', attrs)]
inline = [(attrs, body) for attrs, body in script_tags if not re.search(r'\bsrc\s*=', attrs)]
scripts = [body for attrs, body in inline]
check(len(external) == 1 and re.fullmatch(r'\s*type="module"\s+src="/apps/aurekai-activity\.mjs"\s*', external[0][0])
      and not external[0][1].strip(), "only the required shared activity module is fetched")
check(not any(re.search(r'\btype\s*=', attrs) for attrs, body in inline), "no inline page renderer is mounted")
check('id="bonfyre-surface-scene" data-schema="bonfyre-surface-scene.v1"' in page,
      "inert compatibility Scene accompanies the server document")
check('data-bonfyre-region="page-main" data-bonfyre-continuity="page-main"' in page,
      "page root has stable runtime continuity")
players = [body for body in scripts if "deck=script.previousElementSibling" in body]
account = [body for body in scripts if "document.currentScript.closest('.ak-bar')" in body]
home = [body for body in scripts if "if(location.pathname!=='/')return;" in body or
        "if(location.pathname!=='/'||" in body]
check(len(players) == 1, "exactly one player controls the named queue")
check(len(account) == 1 and "data-account-slot" in account[0], "one scoped native account updater")
check(len(home) == 2, "only the two path-guarded home enhancements accompany the page")
check(len(scripts) == len(players) + len(account) + len(home), "no unclassified page scripts")
for body in players:
    check(not re.search(r"fetch\(|XMLHttpRequest|innerHTML|outerHTML|document\.write|insertAdjacent|import\s|WebSocket|localStorage", body),
          "the player composes nothing, stores nothing and contacts no host")
    check("querySelector" in body and "audio" in body, "and drives only the media the page already names")
decks = re.findall(r'<div\b(?=[^>]*\bdata-deck(?:\s|>))[^>]*>', page)
check(len(decks) == 1 and re.search(r'\bhidden(?:\s|>)', decks[0]),
      "the player is hidden before enhancement, with no dead no-script controls")
check('<audio preload="none">' in page and not re.search(r'<audio[^>]*\bautoplay', page),
      "opening the composition does not preload or autoplay its audio")
check("realtime listings" not in page, "a cataloguing label is not a byline")

# Each kind of material lands in its own rhythm, in the declared order.
marks = ['class="lead"', 'class="shelf"', 'class="index"', 'class="tracks"', 'class="topic"', 'class="more"']
positions = [page.find(m) for m in marks]
for m, p in zip(marks, positions):
    check(p >= 0, "region present: " + m)
check(all(a < b for a, b in zip(positions, positions[1:])), "regions in reading order")
lead = page[positions[0]:positions[1]]
check("Nine Volt Collective adds a late set" in lead, "lead is the newest story across feeds")
check(re.search(r">2h ago</time>", lead), "recent time reads as 2h ago")
# The listening rhythm offers the first recording at the top, and one deck under
# the track list plays every recording in turn: a person who starts listening
# keeps listening instead of pressing play once per row.
check(page.count("<video") == 2, "video plays in place (%d)" % page.count("<video"))
check(page.count("<audio") == 1 and len(decks) == 1,
      "one deck plays the whole queue (%d audio, %d deck)" % (page.count("<audio"), len(decks)))
check(page.count("data-play=") >= 2, "and every recording that plays says what it plays (%d)" % page.count("data-play="))
check(bool(decks) and page.find("<ol class=\"tracks\"") < page.find(decks[0]), "the deck stands under the list it plays")
check(page.count('class="now-playing"') <= 1, "and the surface offers at most one place to start (%d)" % page.count('class="now-playing"'))
check('poster="/assets/trees-walkthrough.jpg"' in page, "poster kept")
check(">1:34<" in page and ">3:12<" in page and ">1:01<" in page, "durations read as clock time")
check(page.count("Shared story from both feeds") == 1, "one story through two carriers is one story")
check(page.rfind("Undated item stays last") > page.find("Rain expected after 11"), "undated story sorts last")
check(page.count('class="topic"') == 1, "a community reached twice is one conversation")
check("Doors moved to 9" in page and "Anyone headed to It Will Do after?" in page, "accepted posts shown")
check(page.find("Doors moved to 9") < page.find("Anyone headed to It Will Do after?"), "conversation reads oldest first")
check("Rail and Ember" in page, "small words stay lower case in names")
check("Not yet accepted." not in page, "pending post withheld")
index = page[positions[2]:positions[3]]
check(index.count("<b>") == 12, "index takes the next twelve stories")
more = page[positions[5]:]
check(more.count("<li>") == 3, "compact list takes what remains")

# Tabs use the words people use; an unlabelled internal grammar is not a tab.
check('href="/w/tonight-proof/stage">Schedule</a>' in page, "stage tab reads Schedule")
check('href="/w/tonight-proof/board">Boards</a>' in page, "board tab reads Boards")
check('href="/w/tonight-proof/community">Community</a>' in page, "community tab")
check("unlabelled-internal" not in page, "internal grammar never becomes a tab")

text = visible(page)
for label in ("Watch", "Latest", "Listen", "What people are saying", "More", "Maya Lee", "Nine Volt Collective", "Deep Ellum regulars", "ellumlistings.test", "citydesk.test"):
    check(label in text, "visible: " + label)
check('</span><span class="via">' not in page and not re.search(r'</span><time\b', page), "byline tokens keep literal separation")
check('class="video-grid"' in grid and 'A walk through the studio' in grid,
      "dated video reaches the grid branch, not only the World shelf")
check(bool(re.search(r'<span class="by"><span>Maya Chen</span>\s+<span class="via">grid\.test</span>\s+<time>[^<]+</time>', grid)),
      "video grid separates the distinct author, publisher and date with literal whitespace")
check('class="page-media-card"' in grid, "media records own a card outside their primary item link")
check(bool(re.search(r'data-item-detail href="/o/grid-video/[0-9a-f]{24}"',grid)), "same retained video has an exact item page")
check('data-item-credit href="/people?who=Maya%20Chen&amp;source=grid-video"' in grid, "same video opens observed creator credit, not an inferred member")
check(bool(re.search(r'data-item-continue href="/feed-use\?context=self&amp;item=grid-video%2F[0-9a-f]{24}"',grid)), "same video reaches the installed app chooser without a duplicate source")
check('<nav class="page-item-continuations"' in grid, "item choices remain a separate accessible navigation from playback")
banned = r"\b(worlds?|observed|observation|closure|candidates?|grammars?|projection|retained|admitted|occurrences?|zine|boundary|subject|fragment|renderer|carrier)\b|\b(person|artist|recording|program|community|venue|place):"
leak = re.findall(banned, text, flags=re.I)
check(not leak, "no machine vocabulary on the page: %s" % sorted(set(l[0] or l[1] for l in leak)))

# A subject with only stories grows no empty shelves.
check('class="lead"' in stories and 'class="index"' in stories, "stories-only page has lead and index")
for m in ('class="shelf"', 'class="tracks"', 'class="topic"'):
    check(m not in stories, "stories-only page has no " + m)
stext = visible(stories)
check("Watch" not in stext and "Listen" not in stext, "no empty media headings")

# A subject with nothing says so plainly.
check("Nothing here right now." in quiet, "empty page is plain")
check(not re.search(banned, visible(quiet), flags=re.I), "empty page has no machine vocabulary")

if problems:
    print("page composition: FAIL")
    for p in problems: print("  -", p)
    sys.exit(1)
PY

echo 'page composition: PASS'
