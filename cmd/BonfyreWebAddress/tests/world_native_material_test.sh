#!/bin/sh
# A published World must arrive as a complete human document.  This fixture
# proves that an RSS receipt becomes publication material in the World grammar
# on the server, rather than a browser-side census reader or a source card.
set -eu
# Negative checks are written as `if ...; then exit 1; fi`: under set -e a bare
# `! cmd` never fails the script, which silently disabled every one of them.

web=${1:?web-address binary required}
case "$web" in /*) ;; *) web="$(pwd)/$web" ;; esac
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
cms="$root/cmd/BonfyreCMS/bonfyre-cms"
[ -x "$cms" ] || { echo 'native World material: SKIP (cms not built)'; exit 0; }

tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-world-material.XXXXXX")
pid=''
cleanup() { [ -n "$pid" ] && kill "$pid" 2>/dev/null || true; rm -rf "$tmp"; }
trap cleanup EXIT INT TERM

schemas="$tmp/schemas"
mkdir -p "$schemas" "$tmp/foreign/receipts" "$tmp/foreign/bodies"
cp "$root/cmd/BonfyreCMS/content-types/world.json" "$schemas/"
cp "$root/cmd/BonfyreCMS/content-types/internet_source.json" "$schemas/"
cp "$root/cmd/BonfyreCMS/content-types/publication_occurrence.json" "$schemas/"
cp "$root/cmd/BonfyreCMS/content-types/source_collection.json" "$schemas/"
cp "$root/cmd/BonfyreCMS/content-types/world_fragment.json" "$schemas/"
cp "$root/cmd/BonfyreCMS/content-types/community_body.json" "$schemas/"
cp "$root/cmd/BonfyreCMS/content-types/community_post.json" "$schemas/"
cp "$root/cmd/BonfyreCMS/content-types/site_presentation.json" "$schemas/"
cp "$root/cmd/BonfyreCMS/content-types/site_program.json" "$schemas/"
cp "$root/cmd/BonfyreCMS/content-types/capability_program.json" "$schemas/"
cmsdb="$tmp/cms.db"
matrix="$tmp/matrix.db"
authdb="$tmp/auth.db"
projection_cache="$tmp/public-projections.db"
"$cms" schema migrate --db "$cmsdb" --schemas "$schemas" >/dev/null

"$cms" entry create world '{"world":"rss-proof","title":"Tonight, already happening","character":"zine","lede":"Observed local reporting arranged as a World, not a feed directory.","subject":"subject:test:rss-proof","sources":"proof-rss","grammars":"discover,stage,community","cannot_infer":"an observed publication is not an Aurekai claim or a reservation","status":"published"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
"$cms" entry create world_fragment '{"fragment_id":"rss-proof:stage","world":"rss-proof","ordinal":1,"subject_address":"subject:test:rss-proof","surface_grammar":"stage","renderer_family":"stage","renderer_variant":"timeline","extent":"page","source_projection":"/_surface/world-material","visible_sections":"published condition","cannot_infer":"a World fragment is not an event or a reservation","status":"published"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
"$cms" entry create world_fragment '{"fragment_id":"rss-proof:community","world":"rss-proof","ordinal":2,"subject_address":"community:aurekai-commons","surface_grammar":"community","renderer_family":"community","renderer_variant":"human_body","extent":"page","source_projection":"/_surface/community-body","visible_sections":"accepted human posts","cannot_infer":"a publication is not a community post","status":"published"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
"$cms" entry create world_fragment '{"fragment_id":"rss-proof:board","world":"rss-proof","ordinal":3,"subject_address":"subject:test:rss-proof","surface_grammar":"board","renderer_family":"board","renderer_variant":"masonry","extent":"page","source_projection":"/_surface/world-material","visible_sections":"published World views","cannot_infer":"a fragment is not a saved private collection relation","status":"published"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
"$cms" entry create community_body '{"community":"aurekai-commons","subject_address":"community:aurekai-commons","title":"Aurekai Commons","description":"A canonical human community body.","visibility":"public","governance_scope":"public","moderation_policy":"accepted posts only","cannot_infer":"federation transport is not a human community post","status":"published"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
"$cms" entry create community_post '{"post":"rss-proof-human-post","community":"aurekai-commons","author_subject":"person:proof","title":"After the game","body":"Meet at the community kitchen.","published_at":"2026-09-08T21:30:00Z","visibility":"public","moderation_state":"accepted","cannot_infer":"an accepted post is not an external publication","status":"published"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
"$cms" entry create community_body '{"community":"other-commons","subject_address":"community:other-commons","title":"Other Commons","description":"A separate human community body.","visibility":"public","governance_scope":"public","moderation_policy":"accepted posts only","cannot_infer":"federation transport is not a human community post","status":"published"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
"$cms" entry create community_post '{"post":"other-human-post","community":"other-commons","author_subject":"person:other","title":"Elsewhere","body":"This must not leak into rss-proof.","published_at":"2026-09-08T22:30:00Z","visibility":"public","moderation_state":"accepted","cannot_infer":"an accepted post is not an external publication","status":"published"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
"$cms" entry create internet_source '{"family":"publication","source":"proof-rss","label":"Proof local reporting","says":"What people are reporting tonight","url":"https://example.test/feed.xml","twin":"rss-proof-twin","state":"observed","extract":"rss:item","title_field":"title","meta_field":"pubDate","link_field":"link","body_regime":"retained-public","status":"published","tier":"warm","kind":"publication"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null

cat >"$tmp/foreign/bodies/rss-proof.body" <<'RSS'
<?xml version="1.0"?><rss><channel>
<item><title>Canonical set: observed without a source card</title><link>https://example.test/canonical-set</link><pubDate>Tue, 08 Sep 2026 21:00:00 GMT</pubDate><description><![CDATA[<p>Not rendered as an application card.</p>]]></description></item>
<item><title>Community kitchen opens after the game</title><link>https://example.test/kitchen</link><pubDate>Tue, 08 Sep 2026 20:30:00 GMT</pubDate></item>
</channel></rss>
RSS
printf '%s\n' "{\"body_path\":\"$tmp/foreign/bodies/rss-proof.body\"}" >"$tmp/foreign/receipts/rss-proof-twin.json"

# A native promotion crosses a retained RSS body into PublicationOccurrence.
# The raw body remains a lawful ingestion input but may not create a duplicate,
# source-owned presentation after promotion.
BONFYRE_FOREIGN_ROOT="$tmp/foreign" "$cms" publication promote-rss --source proof-rss --limit 18 --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
"$cms" entry create source_collection '{"collection_id":"proof-weather-publication","subject_address":"topic:weather","source_selector":{"domains":["weather"],"source_refs":["proof-rss"]},"material_kind":["PublicationOccurrence"],"refresh_policy":"demand:proof-weather-publication","status":"published"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
"$cms" entry create source_collection '{"collection_id":"proof-discover-publication","subject_address":"world:rss-proof","source_selector":{"domains":["discover"],"source_refs":["proof-rss"]},"material_kind":["PublicationOccurrence"],"refresh_policy":"demand:proof-discover-publication","status":"published"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
"$cms" entry create world '{"world":"collection-proof","title":"Collection-backed publication","character":"zine","lede":"One published World reads its named published source collection.","subject":"subject:test:rss-proof","sources":"source_collection:proof-discover-publication","grammars":"discover","cannot_infer":"a source selection is not a source claim or a right to act","status":"published"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
"$cms" entry create source_collection '{"collection_id":"proof-storage-kind","subject_address":"world:rss-proof","source_selector":{"source_refs":["proof-rss"]},"material_kind":["publication_occurrence"],"refresh_policy":"manual","status":"published"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
"$cms" entry create world '{"world":"storage-kind-proof","title":"Collection using the CMS type name","character":"zine","lede":"Backfilled collections use the stored content type name.","subject":"subject:test:rss-proof","sources":"source_collection:proof-storage-kind","grammars":"discover","cannot_infer":"a material type alias does not publish private records","status":"published"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
# A public World may name a collection, but cannot turn an unpublished
# selection into a publication path merely by being published itself.
"$cms" entry create source_collection '{"collection_id":"proof-draft-publication","subject_address":"world:rss-proof","source_selector":{"source_refs":["proof-rss"]},"material_kind":["PublicationOccurrence"],"refresh_policy":"manual","status":"draft"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
"$cms" entry create world '{"world":"draft-collection-proof","title":"Draft collection cannot feed a public World","character":"zine","lede":"A published World must not activate an unpublished selection.","subject":"subject:test:rss-proof","sources":"source_collection:proof-draft-publication","grammars":"discover","cannot_infer":"an unpublished source selection is not public material or a route input","status":"published"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
# Publishing a collection with an unrelated material kind cannot relabel its
# sources as publication material merely because a World names the collection.
"$cms" entry create source_collection '{"collection_id":"proof-wrong-kind","subject_address":"world:rss-proof","source_selector":{"source_refs":["proof-rss"]},"material_kind":["CommunityPost"],"refresh_policy":"manual","status":"published"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
"$cms" entry create world '{"world":"wrong-kind-collection-proof","title":"Wrong material kind cannot feed a publication","character":"zine","lede":"A collection must declare PublicationOccurrence before it reaches this surface.","subject":"subject:test:rss-proof","sources":"source_collection:proof-wrong-kind","grammars":"discover","cannot_infer":"a collection material kind is not interchangeable with a public publication","status":"published"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
"$cms" entry create capability_program '{"program_id":"rss-proof-service","application_kind":"ServiceRequest","action":"Request follow-up","consumes":["subject:test:rss-proof"],"produces":["work:service-request"],"carrier_candidates":["mcp","x402","beckn"],"capability_requirements":["world:rss-proof"],"authority_requirement":"service.request","transition_conditions":["authority:service.request"],"failure_projection":"show refusal","status":"published"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
"$cms" entry create site_presentation '{"site_id":"rss-proof-site","host":"rss-proof.aurekai.live","subject_address":"subject:test:rss-proof","profile":"ghost:edition","territory":"live","navigation":[],"status":"published"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
"$cms" entry create site_program '{"program_id":"rss-proof:v1","site_id":"rss-proof-site","experience_programs":["Publication"],"application_bindings":[],"renderer_preferences":{},"source_collections":["proof-discover-publication"],"capability_programs":["rss-proof-service"],"actor_presentations":{},"status":"published"}' --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null
# The CMS list order is storage order, not a claim about editorial recency.  The
# fixture deliberately retains two observations and proves the canonical item
# survived promotion without coupling this assertion to incidental row order.
canonical=$($cms entry list publication_occurrence --ns root --status published --limit 2 --db "$cmsdb" --schemas "$schemas")
printf '%s' "$canonical" | grep -q '"canonical_href":"https://example.test/canonical-set"'
printf '%s' "$canonical" | grep -q '"published_at":"Tue, 08 Sep 2026 21:00:00 GMT"'

"$web" bootstrap-owned --db "$matrix" >/dev/null
port=$(python3 -c "import socket; s=socket.socket(); s.bind(('127.0.0.1',0)); print(s.getsockname()[1]); s.close()")
BONFYRE_CMS_BINARY="$cms" BONFYRE_CMS_DB="$cmsdb" BONFYRE_CMS_SCHEMAS="$schemas" \
BONFYRE_FOREIGN_ROOT="$tmp/foreign" BONFYRE_PUBLIC_PROJECTION_DB="$projection_cache" \
  "$web" serve --db "$matrix" --auth-db "$authdb" --port "$port" \
  --site-root "$root/site/matrix" >"$tmp/server.log" 2>&1 &
pid=$!
base="http://127.0.0.1:$port"

# Account navigation and home continuation are shared shell scripts.
# Remove their exact owner bytes before checking for a content compositor.
strip_account_refresh() {
  python3 -c '
import ast, pathlib, re, sys
text = sys.stdin.read()
for filename, name in [("account_navigation_markup.h", "BF_ACCOUNT_REFRESH"), ("main.c", "BF_ESTATE_BAR")]:
    header = (pathlib.Path(sys.argv[1]) / filename).read_text()
    macro = header.split("#define " + name + " ", 1)[1].split("\n\n", 1)[0].split("\n#endif", 1)[0]
    if name == "BF_ACCOUNT_REFRESH":
        # Navigation has one browser-module owner, not a C inline script.
        assert macro.strip() == "BF_AUREKAI_ACTIVITY_ASSETS", macro
        repo = pathlib.Path(sys.argv[1]).resolve().parents[2]
        owner = (repo / "integrations/browser/aurekai-activity.mjs").read_text()
        assert "function mountNativeAccountNavigation" in owner
        assert "mountNativeAccountNavigation();" in owner
        continue
    parts = []
    for line in macro.splitlines():
        match = re.match(r"""\s*("(?:\\.|[^"\\])*")""", line)
        if match:
            parts.append(ast.literal_eval(match.group(1)))
    scripts = re.findall(r"<script>.*?</script>", "".join(parts), re.S)
    assert scripts, name
    for script in scripts:
        text = text.replace(script, "")
sys.stdout.write(text)
' "$root/cmd/BonfyreWebAddress/src"
}
strip_account_refresh </dev/null >/dev/null
attempt=0
until curl -sS -o /dev/null -H 'Host: aurekai.live' "$base/healthz"; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 80 ] || ! kill -0 "$pid" 2>/dev/null; then
    echo 'native World material: server did not become ready' >&2
    cat "$tmp/server.log" >&2
    exit 1
  fi
  sleep 0.05
done

page=$(curl -sS -H 'Host: aurekai.live' -H 'Accept: text/html' "$base/w/rss-proof")
printf '%s' "$page" | grep -q 'Tonight, already happening'
printf '%s' "$page" | grep -q 'Canonical set: observed without a source card'
if printf '%s' "$page" | grep -q 'Late set added at the neighborhood hall'; then echo "native World material: FAIL: must not match: printf '%s' $page | grep -q 'Late set added at the neighbo" >&2; exit 1; fi
# Both promoted stories belong on the page.  This line used to be a bare
# `! grep` that set -e never enforced, and it contradicted the fixture.
[ "$(printf '%s' "$page" | grep -o 'Community kitchen opens after the game' | wc -l | tr -d ' ')" = 1 ]
# The page is composed (tests/page_composition_test.sh owns the full contract):
# the newest story leads, the source is named in words, and neither the
# retired flyer card nor the source's catalogue heading shapes the page.
printf '%s' "$page" | grep -q 'class="lead"'
# A byline names the publisher by its own domain; a census label is a
# cataloguing phrase and never reaches the page as a name.
printf '%s' "$page" | grep -q '<span>example.test</span>'
if printf '%s' "$page" | grep -q 'Proof local reporting'; then echo "native World material: FAIL: a census label reached the page as a byline" >&2; exit 1; fi
if printf '%s' "$page" | grep -q 'class="fl noimg"'; then echo "native World material: FAIL: must not match: printf '%s' $page | grep -q 'class=fl noimg'" >&2; exit 1; fi
if printf '%s' "$page" | grep -q 'What people are reporting tonight'; then echo "native World material: FAIL: must not match: printf '%s' $page | grep -q 'What people are reporting ton" >&2; exit 1; fi
if printf '%s' "$page" | grep -q 'Reading boundary'; then echo "native World material: FAIL: must not match: printf '%s' $page | grep -q 'Reading boundary'" >&2; exit 1; fi
printf '%s' "$page" | grep -q 'href="/w/rss-proof/stage"'
printf '%s' "$page" | grep -q 'href="/w/rss-proof/community"'
printf '%s' "$page" | grep -q 'href="/w/rss-proof/board"'
printf '%s' "$page" | grep -q 'href="/w/rss-proof/feed.atom"'
printf '%s' "$page" | grep -q 'href="/w/rss-proof/ap/outbox"'
printf '%s' "$page" | grep -q 'href="/w/rss-proof/events"'
printf '%s' "$page" | grep -q 'href="/w/rss-proof/manifest.json"'
if printf '%s' "$page" | grep -q 'assembling this world'; then echo "native World material: FAIL: must not match: printf '%s' $page | grep -q 'assembling this world'" >&2; exit 1; fi
if printf '%s' "$page" | grep -q 'bonfyre-kit.js'; then echo "native World material: FAIL: must not match: printf '%s' $page | grep -q 'bonfyre-kit.js'" >&2; exit 1; fi
if printf '%s' "$page" | strip_account_refresh | grep -q '<script'; then echo "native World material: FAIL: must not match: printf '%s' $page | grep -q '<script'" >&2; exit 1; fi
if printf '%s' "$page" | grep -q 'internet-census'; then echo "native World material: FAIL: must not match: printf '%s' $page | grep -q 'internet-census'" >&2; exit 1; fi
if printf '%s' "$page" | grep -q 'Deep chain\|human program'; then echo "native World material: FAIL: must not match: printf '%s' $page | grep -q 'Deep chain\|human program'" >&2; exit 1; fi
# Reading coordinates compose across the same World's published views.
# The first request above has already populated the unfiltered cache.
read_page() { curl -fsS -H 'Host: aurekai.live' -H 'Accept: text/html' "$base$1"; }
has() { printf '%s' "$1" | grep -Fq -- "$2"; }
lacks() {
  if has "$1" "$2"; then
    echo "native World reading cuts: unexpected $2" >&2
    exit 1
  fi
}
filtered=$(read_page '/w/rss-proof?q=Canonical')
has "$filtered" 'Canonical set: observed without a source card'
lacks "$filtered" 'Community kitchen opens after the game'
lacks "$filtered" 'Meet at the community kitchen.'
has "$filtered" 'action="/w/rss-proof"'
has "$filtered" 'href="/w/rss-proof/board?q=Canonical"'
has "$filtered" 'matches for <b>Canonical</b>'
has "$filtered" 'href="/w/rss-proof">Show all'
combined=$(read_page '/w/rss-proof?q=Canonical&from=example.test')
has "$combined" 'Canonical set: observed without a source card'
has "$combined" 'name="from" value="example.test"'
has "$combined" 'href="/w/rss-proof/board?q=Canonical&amp;from=example.test"'
has "$combined" 'matches for <b>Canonical</b> from <b>example.test</b>'
for path in '/w/rss-proof?q=absent-material-987' '/w/rss-proof?q=Canonical&from=absent.example' '/w/rss-proof/community?q=absent-material-987' '/discover?q=absent-material-987'; do
  empty=$(read_page "$path")
  has "$empty" 'Nothing here'
  lacks "$empty" 'Canonical set: observed without a source card'
  lacks "$empty" 'Community kitchen opens after the game'
  lacks "$empty" 'Meet at the community kitchen.'
done
community_filtered=$(read_page '/w/rss-proof/community?q=kitchen')
has "$community_filtered" 'Meet at the community kitchen.'
has "$community_filtered" 'action="/w/rss-proof/community"'
lacks "$community_filtered" 'This must not leak into rss-proof.'
collection_filtered=$(read_page '/w/collection-proof?q=Canonical')
has "$collection_filtered" 'Canonical set: observed without a source card'
lacks "$collection_filtered" 'Community kitchen opens after the game'
untrusted=$(read_page '/w/rss-proof?q=%22%3E%3Cimg%20src%3Dx%3E&class=operator&source=other&actor=other')
has "$untrusted" '&lt;img'
lacks "$untrusted" '<img src=x>'
lacks "$untrusted" 'class=operator'
lacks "$untrusted" 'This must not leak into rss-proof.'
unfiltered_again=$(read_page '/w/rss-proof')
has "$unfiltered_again" 'Canonical set: observed without a source card'
has "$unfiltered_again" 'Community kitchen opens after the game'
has "$unfiltered_again" 'Meet at the community kitchen.'
lacks "$unfiltered_again" 'matches for <b>'
echo 'native World reading cuts: PASS'

collection_page=$(curl -sS -H 'Host: aurekai.live' -H 'Accept: text/html' "$base/w/collection-proof")
printf '%s' "$collection_page" | grep -q 'Collection-backed publication'
printf '%s' "$collection_page" | grep -q 'Canonical set: observed without a source card'
storage_kind_page=$(read_page '/w/storage-kind-proof?q=Canonical')
has "$storage_kind_page" 'Collection using the CMS type name'
has "$storage_kind_page" 'Canonical set: observed without a source card'
lacks "$storage_kind_page" 'Community kitchen opens after the game'
draft_collection_page=$(curl -sS -H 'Host: aurekai.live' -H 'Accept: text/html' "$base/w/draft-collection-proof")
printf '%s' "$draft_collection_page" | grep -q 'Draft collection cannot feed a public World'
if printf '%s' "$draft_collection_page" | grep -q 'Canonical set: observed without a source card'; then
  echo 'native World material: unpublished source collection reached public composition' >&2
  exit 1
fi
wrong_kind_collection_page=$(curl -sS -H 'Host: aurekai.live' -H 'Accept: text/html' "$base/w/wrong-kind-collection-proof")
printf '%s' "$wrong_kind_collection_page" | grep -q 'Wrong material kind cannot feed a publication'
if printf '%s' "$wrong_kind_collection_page" | grep -q 'Canonical set: observed without a source card'; then
  echo 'native World material: non-publication collection reached public composition' >&2
  exit 1
fi
manifest=$(curl -sS -H 'Host: aurekai.live' "$base/w/rss-proof/manifest.json")
printf '%s' "$manifest" | grep -q '"schema":"bonfyre.world-federation.v1"'
printf '%s' "$manifest" | grep -q '"source_refs":\["proof-rss"\]'
printf '%s' "$manifest" | grep -q '"source_collections":\["proof-discover-publication"\]'
printf '%s' "$manifest" | grep -q '"program_id":"rss-proof-service"'
printf '%s' "$manifest" | grep -q '"carrier_candidates":\["mcp","x402","beckn"\]'
printf '%s' "$manifest" | grep -q '"authority_requirement":"service.request"'
printf '%s' "$manifest" | grep -q 'ForeignTwin:https://example.test/feed.xml'
printf '%s' "$manifest" | grep -q 'cms:site_presentation:rss-proof-site'
printf '%s' "$manifest" | grep -q 'cms:site_program:rss-proof:v1'
printf '%s' "$manifest" | grep -q 'cms:source_collection:proof-discover-publication'
printf '%s' "$manifest" | grep -q 'cms:capability_program:rss-proof-service'
atom=$(curl -sS -H 'Host: aurekai.live' "$base/w/rss-proof/feed.atom")
printf '%s' "$atom" | grep -q '<feed xmlns="http://www.w3.org/2005/Atom">'
printf '%s' "$atom" | grep -q 'Canonical set: observed without a source card'
printf '%s' "$atom" | grep -q '<category term="proof-rss"/>'
activity=$(curl -sS -H 'Host: aurekai.live' "$base/w/rss-proof/ap/outbox")
printf '%s' "$activity" | grep -q '"type":"OrderedCollection"'
printf '%s' "$activity" | grep -q '"type":"Announce"'
printf '%s' "$activity" | grep -q 'https://example.test/canonical-set'
events=$(curl -sS -H 'Host: aurekai.live' "$base/w/rss-proof/events")
printf '%s' "$events" | grep -q '"specversion":"1.0"'
printf '%s' "$events" | grep -q 'org.aurekai.world.publication.observed'
printf '%s' "$events" | grep -q '"truth":"observed_foreign_publication_not_aurekai_claim"'
[ "$(sqlite3 "$projection_cache" "SELECT count(*) FROM public_projection WHERE closure_key='public-world-reading-v2|/w/rss-proof';")" = 1 ]
[ "$(sqlite3 "$projection_cache" "SELECT count(*) FROM public_projection_dependency WHERE closure_key='public-world-reading-v2|/w/rss-proof' AND dependency='cms:world:rss-proof';")" = 1 ]
[ "$(sqlite3 "$projection_cache" "SELECT count(*) FROM public_projection_dependency WHERE closure_key='public-world-reading-v2|/w/rss-proof' AND dependency='cms:world_fragment:rss-proof:stage';")" = 1 ]
[ "$(sqlite3 "$projection_cache" "SELECT count(*) FROM public_projection_dependency WHERE closure_key='public-world-reading-v2|/w/rss-proof' AND dependency='ForeignTwin:https://example.test/feed.xml';")" = 1 ]
[ "$(sqlite3 "$projection_cache" "SELECT count(*) FROM public_projection_dependency WHERE closure_key='public-world-reading-v2|/w/rss-proof' AND dependency='cms:publication_occurrence';")" = 1 ]
[ "$(sqlite3 "$projection_cache" "SELECT count(*) FROM public_projection_dependency WHERE closure_key='public-world-reading-v2|/w/rss-proof' AND dependency LIKE 'cms:publication_occurrence:pub-%';")" = 2 ]
mkdir -p "$tmp/lowerer-inbox/SurfaceIR-world"
cat > "$tmp/lowerer-inbox/SurfaceIR-world/world-refresh.json" <<'JSON'
{"schema":"bonfyre-lowering-handoff.v1","delta_id":"delta:world:refresh","identity":"cms:world:rss-proof","candidate_kind":"world_publication","owner_ref":"CMS:world:rss-proof","lowerer_ref":"SurfaceIR:world","truth":"dependency_cut_only"}
JSON
invalidate=$($web public-projection-drain --cache-db "$projection_cache" --lowerer-dir "$tmp/lowerer-inbox")
printf '%s' "$invalidate" | grep -q '"invalidated":1'
[ -f "$tmp/lowerer-inbox/SurfaceIR-world/world-refresh.json.handled" ]
[ "$(sqlite3 "$projection_cache" "SELECT count(*) FROM public_projection WHERE closure_key='public-world-reading-v2|/w/rss-proof';")" = 0 ]
# A new read rebuilds only the retired World closure. The retained observation
# itself remains untouched: invalidation is a derived projection operation.
page_after_cut=$(curl -sS -H 'Host: aurekai.live' -H 'Accept: text/html' "$base/w/rss-proof")
printf '%s' "$page_after_cut" | grep -q 'Canonical set: observed without a source card'
[ "$(sqlite3 "$projection_cache" "SELECT count(*) FROM public_projection WHERE closure_key='public-world-reading-v2|/w/rss-proof';")" = 1 ]
# A promoted PublicationOccurrence is its own material-owner coordinate.  Its
# mutation retires this World projection without treating the whole retained
# source, the World, or any unrelated World as changed.
occurrence=$(sqlite3 "$cmsdb" "SELECT occurrence_id FROM publication_occurrence ORDER BY occurrence_id LIMIT 1")
cat > "$tmp/lowerer-inbox/SurfaceIR-world/publication-refresh.json" <<JSON
{"schema":"bonfyre-lowering-handoff.v1","delta_id":"delta:publication:refresh","identity":"cms:publication_occurrence:$occurrence","candidate_kind":"publication_occurrence","owner_ref":"CMS:publication_occurrence:$occurrence","lowerer_ref":"SurfaceIR:world","truth":"dependency_cut_only"}
JSON
publication_invalidate=$($web public-projection-drain --cache-db "$projection_cache" --lowerer-dir "$tmp/lowerer-inbox")
printf '%s' "$publication_invalidate" | grep -q '"invalidated":1'
[ -f "$tmp/lowerer-inbox/SurfaceIR-world/publication-refresh.json.handled" ]
[ "$(sqlite3 "$projection_cache" "SELECT count(*) FROM public_projection WHERE closure_key='public-world-reading-v2|/w/rss-proof';")" = 0 ]
page_after_publication_cut=$(curl -sS -H 'Host: aurekai.live' -H 'Accept: text/html' "$base/w/rss-proof")
printf '%s' "$page_after_publication_cut" | grep -q 'Canonical set: observed without a source card'
[ "$(sqlite3 "$projection_cache" "SELECT count(*) FROM public_projection WHERE closure_key='public-world-reading-v2|/w/rss-proof';")" = 1 ]

shelf=$(curl -sS -H 'Host: aurekai.live' -H 'Accept: text/html' "$base/w")
# /w is now a native surface program.  It links each page by its title and
# does not print the character dressing it.
printf '%s' "$shelf" | grep -q 'href="/w/rss-proof"'
printf '%s' "$shelf" | grep -q 'Tonight, already happening'
printf '%s' "$shelf" | grep -q 'data-character="zine"'
if printf '%s' "$shelf" | strip_account_refresh | grep -q '<span>zine</span>\|assembling this world\|<script\|bonfyre-kit.js'; then echo 'native World material: FAIL: shelf leaks character or browser renderer' >&2; exit 1; fi

# Discover and Stage retain the donor documents, but their responses are
# server-lowered.  Publication material is a Pixelfed-style stream item, never
# browser JSON that a surface interprets into a catalogue.
discover=$(curl -sS -H 'Host: aurekai.live' -H 'Accept: text/html' "$base/discover")
printf '%s' "$discover" | grep -q 'Canonical set: observed without a source card'
printf '%s' "$discover" | grep -q 'timeline-status-component'
if printf '%s' "$discover" | strip_account_refresh | grep -q '<script\|bonfyre-kit.js\|external-estate'; then echo "native World material: FAIL: must not match: printf '%s' $discover | grep -q '<script\|bonfyre-kit.js\|" >&2; exit 1; fi

# A wildcard request is still a human document request, not permission to
# fall through to the legacy donor file whose browser script rebuilt content.
discover_wildcard=$(curl -sS -H 'Host: aurekai.live' "$base/discover")
printf '%s' "$discover_wildcard" | grep -q 'Canonical set: observed without a source card'
if printf '%s' "$discover_wildcard" | strip_account_refresh | grep -q '<script\|BF_DISCOVER\|bonfyre-kit.js'; then echo "native World material: FAIL: must not match: printf '%s' $discover_wildcard | grep -q '<script\|BF_DISC" >&2; exit 1; fi

# A URL domain narrows only through the CMS-authored SourceCollection. It
# cannot turn into a lexical search that leaks the global newest publication.
weather_discover=$(curl -sS -H 'Host: aurekai.live' -H 'Accept: text/html' "$base/discover?domain=weather")
printf '%s' "$weather_discover" | grep -q 'What is actually published for weather'
printf '%s' "$weather_discover" | grep -q 'Canonical set: observed without a source card'
if printf '%s' "$weather_discover" | grep -q 'Tonight in this city'; then echo "native World material: FAIL: must not match: printf '%s' $weather_discover | grep -q 'Tonight in this c" >&2; exit 1; fi

empty_discover=$(curl -sS -H 'Host: aurekai.live' -H 'Accept: text/html' "$base/discover?domain=healthcare")
printf '%s' "$empty_discover" | grep -q 'Nothing public has been compiled for healthcare'
if printf '%s' "$empty_discover" | grep -q 'Canonical set: observed without a source card'; then echo "native World material: FAIL: must not match: printf '%s' $empty_discover | grep -q 'Canonical set: obse" >&2; exit 1; fi

stage=$(curl -sS -H 'Host: aurekai.live' -H 'Accept: text/html' "$base/stage")
printf '%s' "$stage" | grep -q 'published public condition'
printf '%s' "$stage" | grep -q 'No event is on the schedule yet'
if printf '%s' "$stage" | strip_account_refresh | grep -q '<script\|bonfyre-kit.js\|external-estate'; then echo "native World material: FAIL: must not match: printf '%s' $stage | grep -q '<script\|bonfyre-kit.js\|ext" >&2; exit 1; fi
stage_wildcard=$(curl -sS -H 'Host: aurekai.live' "$base/stage")
printf '%s' "$stage_wildcard" | grep -q 'No event is on the schedule yet'
if printf '%s' "$stage_wildcard" | strip_account_refresh | grep -q '<script\|BF_STAGE\|bonfyre-kit.js'; then echo "native World material: FAIL: must not match: printf '%s' $stage_wildcard | grep -q '<script\|BF_STAGE\|" >&2; exit 1; fi

# The World grammar is a real nested address.  Board, Stage and Community
# remain distinct donor interaction languages, but all preserve rss-proof as
# their canonical context and arrive without the retired browser renderer.
board=$(curl -sS -H 'Host: aurekai.live' -H 'Accept: text/html' "$base/w/rss-proof/board")
printf '%s' "$board" | grep -q 'href="/w/rss-proof/stage"'
printf '%s' "$board" | grep -q 'published World views'
if printf '%s' "$board" | strip_account_refresh | grep -q '<script\|BF_BOARD\|bonfyre_board_saved'; then echo "native World material: FAIL: must not match: printf '%s' $board | grep -q '<script\|BF_BOARD\|bonfyre_b" >&2; exit 1; fi

stage_nested=$(curl -sS -H 'Host: aurekai.live' -H 'Accept: text/html' "$base/w/rss-proof/stage")
printf '%s' "$stage_nested" | grep -q 'href="/w/rss-proof/community"'
if printf '%s' "$stage_nested" | strip_account_refresh | grep -q '<script\|BF_STAGE'; then echo "native World material: FAIL: must not match: printf '%s' $stage_nested | grep -q '<script\|BF_STAGE'" >&2; exit 1; fi

community=$(curl -sS -H 'Host: aurekai.live' -H 'Accept: text/html' "$base/w/rss-proof/community")
printf '%s' "$community" | grep -q 'After the game'
if printf '%s' "$community" | grep -q 'This must not leak into rss-proof'; then echo "native World material: FAIL: must not match: printf '%s' $community | grep -q 'This must not leak into " >&2; exit 1; fi
printf '%s' "$community" | grep -q 'href="/w/rss-proof/board"'
if printf '%s' "$community" | strip_account_refresh | grep -q '<script\|BF_COMMUNITY\|Federation</h1>'; then echo "native World material: FAIL: must not match: printf '%s' $community | grep -q '<script\|BF_COMMUNITY\|F" >&2; exit 1; fi

# A World without a published Listings fragment cannot claim that its source
# conditions became availability merely because the visitor typed a path.
status=$(curl -sS -o "$tmp/listings-status" -w '%{http_code}' -H 'Host: aurekai.live' -H 'Accept: text/html' "$base/w/rss-proof/listings")
[ "$status" = 404 ]
grep -q 'has not published that interaction grammar' "$tmp/listings-status"

echo 'native World material: PASS'
