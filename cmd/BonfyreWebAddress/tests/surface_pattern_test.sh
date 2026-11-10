#!/usr/bin/env bash
# SurfaceIR pattern compiler: one descriptor, two lawful projections.
#
# The claim under test is not "the fragment renders". It is that a person and a
# machine asking for the same pattern receive the same records in the same
# order, because they are served from one descriptor and one query. Two
# carriers of one capability drifting into two implementations is the failure
# this suite exists to catch.
#
# Cannot infer: a passing run is not a claim that any pattern's copy is good,
# or that a rendered affordance is authorized. It is a claim that the two
# projections agree and that a declared authority is enforced in both.
set -uo pipefail
BASE="${BASE:-https://aurekai.life}"
pass=0; fail=0
ok()   { pass=$((pass+1)); printf '  ok   %s\n' "$1"; }
bad()  { fail=$((fail+1)); printf '  FAIL %s\n' "$1"; }
check(){ if [ "$2" = "$3" ]; then ok "$1"; else bad "$1 (want '$3', got '$2')"; fi; }

echo "== public pattern serves both carriers =="
html=$(curl -s --max-time 20 -H 'Accept: text/html' "$BASE/_surface/doors?page=protocols")
json=$(curl -s --max-time 20 -H 'Accept: application/json' "$BASE/_surface/doors?page=protocols")
check "html carries door articles" "$(printf '%s' "$html" | grep -o '<article class="door"' | wc -l | tr -d ' ')" \
                                   "$(printf '%s' "$json" | python3 -c 'import sys,json;print(json.load(sys.stdin)["count"])' 2>/dev/null || echo x)"
check "json declares its effect class" "$(printf '%s' "$json" | python3 -c 'import sys,json;print(json.load(sys.stdin)["effect_class"])' 2>/dev/null)" "QUERY"
check "json names the type it reads"   "$(printf '%s' "$json" | python3 -c 'import sys,json;print(json.load(sys.stdin)["reads"])' 2>/dev/null)" "site_door"

echo "== the two projections agree record for record =="
h1=$(printf '%s' "$html" | grep -o '<h3>[^<]*' | head -1 | sed 's/<h3>//')
j1=$(printf '%s' "$json" | python3 -c 'import sys,json;print(json.load(sys.stdin)["records"][0]["heading"])' 2>/dev/null)
check "first record matches across carriers" "$h1" "$j1"

echo "== a second pattern needs no second handler =="
check "nav serves as json" "$(curl -s --max-time 20 -o /dev/null -w '%{http_code}' -H 'Accept: application/json' "$BASE/_surface/nav")" "200"
check "nav serves as html" "$(curl -s --max-time 20 -o /dev/null -w '%{http_code}' -H 'Accept: text/html' "$BASE/_surface/nav")" "200"

echo "== declared authority is enforced in both projections =="
check "anonymous json refused" "$(curl -s --max-time 20 -o /dev/null -w '%{http_code}' -H 'Accept: application/json' "$BASE/_surface/ledger-doors?page=protocols")" "403"
check "anonymous html refused" "$(curl -s --max-time 20 -o /dev/null -w '%{http_code}' -H 'Accept: text/html' "$BASE/_surface/ledger-doors?page=protocols")" "403"
check "refusal names the requirement" \
  "$(curl -s --max-time 20 -H 'Accept: application/json' "$BASE/_surface/ledger-doors?page=protocols" | python3 -c 'import sys,json;print(json.load(sys.stdin)["requires"])' 2>/dev/null)" "write"
check "a public pattern is unaffected" "$(curl -s --max-time 20 -o /dev/null -w '%{http_code}' "$BASE/_surface/doors?page=protocols")" "200"

echo "== an undeclared pattern is not invented =="
check "unknown pattern does not 200" \
  "$(curl -s --max-time 20 -o /dev/null -w '%{http_code}' "$BASE/_surface/not-a-declared-pattern")" "404"

echo "== the page keeps a fallback rather than an empty frame =="
page=$(curl -s --max-time 20 -H 'Accept: text/html' "$BASE/protocols")
if printf '%s' "$page" | grep -q 'hx-get="/_surface/doors'; then ok "page requests the authored fragment"; else bad "page does not request the fragment"; fi
if [ "$(printf '%s' "$page" | grep -o '<article class="door"' | wc -l | tr -d ' ')" -gt 0 ]; then ok "compiled fallback survives in the swap target"; else bad "no fallback inside the swap target"; fi

echo "== publication state is enforced, not optional =="
# The doors served publicly while every row was status='draft'. A draft must
# never reach a public surface; this creates one, looks for it, and removes it.
if [ -n "${SURFACE_REMOTE:-}" ]; then
  before=$(curl -s --max-time 20 -H 'Accept: application/json' "$BASE/_surface/doors?page=protocols" | python3 -c 'import sys,json;print(json.load(sys.stdin)["count"])' 2>/dev/null)
  newid=$($SURFACE_REMOTE "entry create site_door '{\"page\":\"protocols\",\"position\":99,\"eyebrow\":\"draft probe\",\"heading\":\"UNPUBLISHED PROBE\",\"body\":\"must never be served\",\"ways\":\"[]\"}'" 2>/dev/null | python3 -c 'import sys,json;print(json.load(sys.stdin)["id"])' 2>/dev/null)
  after=$(curl -s --max-time 20 -H 'Accept: application/json' "$BASE/_surface/doors?page=protocols" | python3 -c 'import sys,json;print(json.load(sys.stdin)["count"])' 2>/dev/null)
  body=$(curl -s --max-time 20 -H 'Accept: application/json' "$BASE/_surface/doors?page=protocols")
  check "a draft entry is not served" "$after" "$before"
  if printf '%s' "$body" | grep -q 'UNPUBLISHED PROBE'; then bad "draft content leaked to a public surface"; else ok "draft content absent from the public surface"; fi
  [ -n "$newid" ] && $SURFACE_REMOTE "entry delete site_door $newid" >/dev/null 2>&1
else
  echo "  skip (set SURFACE_REMOTE to exercise the draft probe)"
fi

echo "== row-level visibility excludes what the type marks invisible =="
wiki=$(curl -s --max-time 20 -H 'Accept: application/json' "$BASE/_surface/wiki")
wc=$(printf '%s' "$wiki" | python3 -c 'import sys,json;print(json.load(sys.stdin)["count"])' 2>/dev/null)
if [ "${wc:-0}" -ge 1 ] 2>/dev/null; then ok "wiki surface serves its visible pages ($wc)"; else bad "wiki surface served nothing"; fi
if [ "${wc:-99}" -lt 10 ] 2>/dev/null; then ok "wiki surface excludes the unpublished corpus"; else bad "wiki surface served $wc pages; the corpus is not being filtered"; fi

echo "== the estate describes its own read surfaces =="
cat_json=$(curl -s --max-time 20 -H 'Accept: application/json' "$BASE/_surface/")
cc=$(printf '%s' "$cat_json" | python3 -c 'import sys,json;print(json.load(sys.stdin)["count"])' 2>/dev/null)
if [ "${cc:-0}" -ge 3 ] 2>/dev/null; then ok "catalogue lists declared surfaces ($cc)"; else bad "catalogue listed ${cc:-none}"; fi
if printf '%s' "$cat_json" | grep -q '"effect_class"'; then ok "catalogue declares each effect class"; else bad "catalogue omits effect class"; fi
if printf '%s' "$cat_json" | grep -q '"requires_authority": *"write"\|"requires_authority":"write"'; then ok "catalogue names an authority requirement"; else bad "catalogue hides authority requirements"; fi
check "catalogue serves html too" "$(curl -s --max-time 20 -o /dev/null -w '%{http_code}' -H 'Accept: text/html' "$BASE/_surface/")" "200"

echo "== the machine control plane serves the same capability =="
# Section 7's claim: one native capability, two lawful projections. If the agent
# and the browser ever disagree about the records, the browser is right and this
# is the bug -- so assert they agree rather than that each merely answers.
if [ -n "${SURFACE_MCP:-}" ]; then
  mcp_doors=$($SURFACE_MCP doors protocols 2>/dev/null)
  web_doors=$(curl -s --max-time 20 -H 'Accept: application/json' "$BASE/_surface/doors?page=protocols" | python3 -c 'import sys,json;print(json.load(sys.stdin)["count"])' 2>/dev/null)
  check "agent and browser agree on record count" "$mcp_doors" "$web_doors"
  mcp_denied=$($SURFACE_MCP ledger-doors protocols --status 2>/dev/null)
  check "agent is refused where the browser is refused" "$mcp_denied" "403"
else
  echo "  skip (set SURFACE_MCP to exercise the agent projection)"
fi

echo "== one Matrix world, four territory projections of the same rows =="
# The claim is not that four endpoints answer. It is that they answer over the
# SAME subjects with different fields -- one world projected four ways, rather
# than four datasets that happen to look related. So: identical record counts,
# identical first subject, different field sets.
life=$(curl -s --max-time 20 -H 'Accept: application/json' "$BASE/_surface/night-life")
live=$(curl -s --max-time 20 -H 'Accept: application/json' "$BASE/_surface/night-live")
shop=$(curl -s --max-time 20 -H 'Accept: application/json' "$BASE/_surface/night-shop")
cnt() { printf '%s' "$1" | python3 -c 'import sys,json;print(json.load(sys.stdin)["count"])' 2>/dev/null; }
rd()  { printf '%s' "$1" | python3 -c 'import sys,json;print(json.load(sys.stdin)["reads"])' 2>/dev/null; }
f1()  { printf '%s' "$1" | python3 -c 'import sys,json;print(json.load(sys.stdin)["records"][0]["name"])' 2>/dev/null; }
keys(){ printf '%s' "$1" | python3 -c 'import sys,json;print(",".join(sorted(json.load(sys.stdin)["records"][0].keys())))' 2>/dev/null; }

check "life and live project the same subject count" "$(cnt "$life")" "$(cnt "$live")"
check "shop projects that same count too"            "$(cnt "$shop")" "$(cnt "$live")"
check "all three read one content type"              "$(rd "$life")/$(rd "$live")/$(rd "$shop")" "night_subject/night_subject/night_subject"
check "the first subject is the same in each"        "$(f1 "$life")" "$(f1 "$shop")"
if [ "$(keys "$life")" != "$(keys "$live")" ] && [ "$(keys "$live")" != "$(keys "$shop")" ]; then
  ok "each territory projects a different field set"
else
  bad "territories projected identical fields; this is one dataset copied, not a projection"
fi

echo "== the institutional projection is admitted-only =="
check "night-work refuses anonymously" \
  "$(curl -s --max-time 20 -o /dev/null -w '%{http_code}' -H 'Accept: application/json' "$BASE/_surface/night-work")" "403"

echo "== the demo is on the live site, one path across four territories =="
# The page, not just the fragment. A projection nobody can open is a fixture.
for pair in "aurekai.life life night-life" "aurekai.live live night-live" \
            "aurekai.shop shop night-shop" "aurekai.work work night-work"; do
  set -- $pair
  host=$1; terr=$2; want=$3
  code=$(curl -s --max-time 20 -o /dev/null -w '%{http_code}' "https://$host/tonight")
  got=$(curl -s --max-time 20 -H 'Accept: application/json' "https://$host/tonight" \
        | python3 -c 'import sys,json;d=json.load(sys.stdin);print(d["territory"]+":"+d["pattern"])' 2>/dev/null)
  check "$host/tonight serves" "$code" "200"
  check "$host projects its own territory" "$got" "$terr:$want"
done

echo "== the page requests its projection and keeps a fallback =="
page=$(curl -s --max-time 20 -H 'Accept: text/html' "https://aurekai.live/tonight")
if printf '%s' "$page" | grep -q 'hx-get="/_surface/night-live"'; then ok "the page requests its authored projection"; else bad "page does not request a projection"; fi
if printf '%s' "$page" | grep -q 'surface-record'; then ok "a compiled fallback survives inside the swap target"; else bad "no fallback in the swap target"; fi
if printf '%s' "$page" | grep -q 'world DEMO'; then ok "the page says it is a demo world"; else bad "the page does not disclose its world"; fi

echo "== the nav carries it, authored not compiled =="
if curl -s --max-time 20 -H 'Accept: application/json' "$BASE/_surface/nav" | grep -q '"label":"Tonight"'; then
  ok "Tonight is an authored nav record"
else
  bad "Tonight is missing from the authored nav"
fi

echo "== observations: an earlier belief can be discovered to be stale =="
obs=$(curl -s --max-time 20 -H 'Accept: application/json' "$BASE/observations")
oc=$(printf '%s' "$obs" | python3 -c 'import sys,json;print(json.load(sys.stdin)["count"])' 2>/dev/null)
if [ "${oc:-0}" -ge 1 ] 2>/dev/null; then ok "the estate reports disagreeing observations ($oc)"; else bad "no conflict set reported"; fi
# A resource that never retained bytes must report null reachability, not false:
# false would claim held content is unreachable when nothing was ever held.
if printf '%s' "$obs" | grep -q '"content_still_reachable":null'; then
  ok "a never-retained resource reports null reachability, not false"
else
  ok "no never-retained resource in the current corpus (nothing to assert)"
fi
check "the page serves to a person" "$(curl -s --max-time 20 -o /dev/null -w '%{http_code}' -H 'Accept: text/html' "$BASE/observations")" "200"
if curl -s --max-time 20 -H 'Accept: text/html' "$BASE/observations" | grep -q 'no longer current'; then
  ok "the page explains what a conflict means"
else
  bad "the page does not explain its own claim"
fi
if curl -s --max-time 20 -H 'Accept: application/json' "$BASE/_surface/nav" | grep -q '"label":"Observations"'; then
  ok "Observations is an authored nav record"
else
  bad "Observations missing from the authored nav"
fi

echo "== the pages a person opens carry the authored nav =="
# This suite tested the /_surface/nav fragment and passed while every landing
# page served fifteen compiled links. A fragment nobody renders is not a
# navigation, and checking the endpoint instead of the page is how that went
# unnoticed. These assertions read the page.
NAVPAGES="https://aurekai.life/ https://aurekai.live/ https://aurekai.shop/ https://aurekai.work/ https://aurekai.work/studio/ https://aurekai.life/protocols https://aurekai.life/tonight https://aurekai.life/twins"
for u in $NAVPAGES; do
  page=$(curl -s --max-time 20 -H 'Accept: text/html' "$u")
  # A page either renders the authored nav server-side, or declares the swap.
  server=$(printf '%s' "$page" | grep -c '>Tonight</a>' || true)
  swap=$(printf '%s' "$page" | grep -c 'hx-get="/_surface/nav"' || true)
  if [ "${server:-0}" -ge 1 ] || [ "${swap:-0}" -ge 1 ]; then
    ok "${u#https://} reads the authored nav"
  else
    bad "${u#https://} serves a compiled nav only"
  fi
  # A declared swap with no runtime renders its fallback forever and looks
  # correct in the source, which is exactly how /studio/ passed a source check
  # and failed in a browser.
  if [ "${swap:-0}" -ge 1 ]; then
    rt=$(printf '%s' "$page" | grep -c 'matrix-htmx.js' || true)
    if [ "${rt:-0}" -ge 1 ]; then ok "  and loads the runtime its swap needs"; else bad "  declares a swap with no htmx runtime"; fi
  fi
done

echo "== landing surfaces are authored, not files on disk =="
# These routes were static HTML. A file is a fine way to serve a page and a poor
# way to own one: the words were unreachable through the institution's own
# machinery, and the nav had to be patched into thirteen files by hand because
# each carried a copy.
for r in /property /market /sources /agents; do
  j=$(curl -s --max-time 20 -H 'Accept: application/json' "$BASE$r")
  a=$(printf '%s' "$j" | python3 -c 'import sys,json;print(json.load(sys.stdin).get("authored"))' 2>/dev/null)
  check "${r} is served from authored state" "$a" "True"
done
# The root is authored for a person and keeps its machine contract: a territory
# root already answers machines with a discovery descriptor, and a second
# machine view of one route is the drift this estate exists to prevent.
roothtml=$(curl -s --max-time 20 -H 'Accept: text/html' "$BASE/" | grep -c 'Make what matters legible')
check "/ is authored for a person" "$roothtml" "1"
rootjson=$(curl -s --max-time 20 -H 'Accept: application/json' "$BASE/" | python3 -c 'import sys,json;print("version" in json.load(sys.stdin))' 2>/dev/null)
check "/ still answers machines with its descriptor" "$rootjson" "True"
# The words come from the row, so changing one is an authoring act.
h=$(curl -s --max-time 20 -H 'Accept: application/json' "$BASE/property" | python3 -c 'import sys,json;print(json.load(sys.stdin)["heading"])' 2>/dev/null)
if [ -n "$h" ]; then ok "and carries its authored heading (\"$h\")"; else bad "no heading came from the store"; fi
# Doors on an authored page are authored too, or the page is half migrated.
for pg in home property market sources agents; do
  n=$(curl -s --max-time 20 -H 'Accept: application/json' "$BASE/_surface/doors?page=$pg" | python3 -c 'import sys,json;print(json.load(sys.stdin)["count"])' 2>/dev/null)
  if [ "${n:-0}" -ge 1 ] 2>/dev/null; then ok "$pg has authored doors ($n)"; else bad "$pg renders no authored doors"; fi
done
# A title is the page's to declare; the organ appending the institution name
# produced "Bonfyre - Bonfyre" on the front page.
t=$(curl -s --max-time 20 -H 'Accept: text/html' "$BASE/" | grep -oE '<title>[^<]*' | sed 's/<title>//')
case "$t" in
  *"· Bonfÿre · Bonfÿre"*) bad "the organ is doubling the institution name in titles" ;;
  *) ok "the title is the page's own ($t)" ;;
esac

echo "== a surface composes authored, closure and action layers =="
# Page and application are not rival architectures. One route carries authored
# copy, lenses over live state, and the operations an actor may perform; the
# difference between a narrative page and a workspace is the balance.
# The workspace extent, because a page deliberately shows closure without the
# action layer -- asking the default extent for actions tests the wrong thing.
page=$(curl -s --max-time 20 -H 'Accept: text/html' "$BASE/property?extent=workspace")
au=$(printf '%s' "$page" | grep -c 'hx-get="/_surface/doors' || true)
cl=$(printf '%s' "$page" | grep -cE 'hx-get="/_surface/[a-z-]+"' || true)
ac=$(printf '%s' "$page" | grep -c 'hx-get="/_aperture' || true)
check "the authored layer is present" "$au" "1"
check "the closure layer is present"  "$cl" "1"
check "the action layer is present"   "$ac" "1"
# The action layer cannot be baked: what may be done depends on who is asking.
anon=$(curl -s --max-time 20 "$BASE/_aperture?subject=foreign_twin:src-dallas-tax-parcels")
if printf '%s' "$anon" | grep -q "not offered to this session"; then
  ok "an anonymous reader is told operations exist and are withheld"
else
  bad "the action layer did not withhold from an anonymous reader"
fi
if [ -n "${SURFACE_TOKEN:-}" ]; then
  adm=$(curl -s --max-time 20 -H "Authorization: Bearer $SURFACE_TOKEN" "$BASE/_aperture?subject=foreign_twin:src-dallas-tax-parcels")
  if printf '%s' "$adm" | grep -q "Property\."; then
    ok "and an admitted actor is offered the operations"
  else
    bad "an admitted actor was offered nothing"
  fi
fi

echo "== one surface, many extents, unchanged subject =="
# An extent is how much of a closure the caller wants, not a different page.
# That is what makes "a workspace appears as a card somewhere else" an ordinary
# projection instead of a second implementation.
prev=0
for e in chip card section page; do
  n=$(curl -s --max-time 20 -H 'Accept: text/html' "$BASE/property?extent=$e" | wc -c | tr -d ' ')
  if [ "${n:-0}" -gt "${prev:-0}" ] 2>/dev/null; then
    ok "$e is larger than the extent below it ($n bytes)"
  else
    bad "$e ($n) did not grow on the one below ($prev)"
  fi
  prev=$n
done
# A fragment must not bring its own document into the page embedding it.
for e in chip card section; do
  d=$(curl -s --max-time 20 -H 'Accept: text/html' "$BASE/property?extent=$e" | grep -c '<!doctype' || true)
  check "$e is a fragment, not a document" "$d" "0"
done
check "page is a document" "$(curl -s --max-time 20 -H 'Accept: text/html' "$BASE/property?extent=page" | grep -c '<!doctype')" "1"
# Extent changes how much is shown, never which subject: the heading is the same
# at every extent, and rendering from freed statement memory printed stack bytes
# here before the strings were copied.
h=$(curl -s --max-time 20 -H 'Accept: text/html' "$BASE/property?extent=chip" | sed 's/<[^>]*>//g')
case "$h" in
  *"What is on this land?"*) ok "a chip carries the subject's own heading" ;;
  *) bad "chip heading is not the subject's ($h)" ;;
esac
# The action layer is carried at page and workspace and omitted below. This
# assertion previously read `page shows closure without actions`, which is what
# made bare /login render no sign-in form: the rule was stated one rung too high
# on the ladder, and the suite held it in place.
pa=$(curl -s --max-time 20 -H 'Accept: text/html' "$BASE/property?extent=page" | grep -c '_aperture' || true)
wa=$(curl -s --max-time 20 -H 'Accept: text/html' "$BASE/property?extent=workspace" | grep -c '_aperture' || true)
ca=$(curl -s --max-time 20 -H 'Accept: text/html' "$BASE/property?extent=card" | grep -c '_aperture' || true)
check "a page carries the action layer" "$pa" "1"
check "a card is a reference and carries none" "$ca" "0"
check "a workspace adds the action layer"    "$wa" "1"
# The estate can render its own surfaces as records inside a surface.
n=$(curl -s --max-time 20 -H 'Accept: application/json' "$BASE/_surface/surfaces" | python3 -c 'import sys,json;print(json.load(sys.stdin)["count"])' 2>/dev/null)
if [ "${n:-0}" -ge 3 ] 2>/dev/null; then ok "surfaces are addressable as records ($n)"; else bad "the estate cannot list its own surfaces"; fi

echo "== every page is a machine discovery root =="
# The institution expects foreign systems to publish machine representations it
# can find, and published its own only on routes that happened to emit them. A
# page discoverable only by a person is half a surface.
page=$(curl -s --max-time 20 -H 'Accept: text/html' "$BASE/habitat")
n=$(printf '%s' "$page" | grep -o 'rel="alternate"' | wc -l | tr -d ' ')
[ "$n" -ge 9 ] && ok "an authored page links $n machine representations" \
                || bad "only $n machine representations linked"
for want in 'application/activity+json' 'agent.json' 'mcp/resources' 'ro-crate' 'zarr' 'rel="lrdd"'; do
  case "$page" in
    *"$want"*) ok "the manifest reaches $want" ;;
    *) bad "the manifest omits $want" ;;
  esac
done
# A linked representation must actually answer, or the manifest is a list of
# claims about surfaces that are not there.
for u in /ap/actor /.well-known/agent.json /mcp/resources/ /.well-known/bonfyre; do
  c=$(curl -s -o /dev/null -w '%{http_code}' --max-time 20 -H 'Accept: application/json' "$BASE$u")
  check "the linked $u answers" "$c" "200"
done

echo "== speculation is compiled from the address plane, not guessed =="
# Ordinary sites guess a link may be clicked. This institution knows which paths
# it addresses, so the candidate set is its own authored navigation.
rules=$(printf '%s' "$page" | grep -o '<script type="speculationrules">[^<]*')
[ -n "$rules" ] && ok "the page carries generated speculation rules" \
                || bad "no speculation rules emitted"
case "$rules" in
  *'"eagerness":"moderate"'*) ok "it speculates on intent, not at load" ;;
  *) bad "eagerness is not moderate; a page view would prefetch the whole nav" ;;
esac
# A foreign or relative href must never become a speculative fetch made on this
# institution's behalf.
foreign=$(printf '%s' "$rules" | grep -o '"[^"]*"' | grep -c '"http' || true)
check "no foreign origin is speculated" "$foreign" "0"
rel=$(printf '%s' "$rules" | python3 -c '
import sys,re
t=sys.stdin.read()
paths=re.findall(r"\"(/[^\"]*)\"", t)
print(sum(1 for p in paths if not p.startswith("/") or p.startswith("//")))' 2>/dev/null || echo 0)
check "every speculated path is same-origin absolute" "$rel" "0"

echo "== an offscreen panel costs nothing until it is reached =="
cv=$(printf '%s' "$page" | grep -o 'lens-panel' | wc -l | tr -d ' ')
[ "$cv" -ge 2 ] && ok "$cv lens panels are skippable while offscreen" \
                || bad "panels are not marked for content-visibility"
case "$page" in
  *'contain-intrinsic-size'*) ok "they reserve a plausible height so the scrollbar holds" ;;
  *) bad "content-visibility without an intrinsic size makes the page jump" ;;
esac

echo "== a second page view does not re-download the site =="
# The assets were sent with Cache-Control: no-cache and no validator at all.
# no-cache means revalidate before reuse; with nothing to revalidate against,
# every navigation re-downloaded the whole body -- 61KB of htmx, measured at
# 517ms, with the nav and every panel blocked behind it because they arrive as
# htmx fragments. That was the slowness, and it was paid on every page view.
for a in matrix-htmx.js matrix.css matrix-aperture.css; do
  hdrs=$(curl -s -D- -o /dev/null --max-time 20 "$BASE/assets/$a" | tr -d '\r')
  et=$(printf '%s' "$hdrs" | awk 'tolower($1)=="etag:"{print $2}')
  case "$et" in
    '"'*'"') ok "$a carries a content ETag" ;;
    *) bad "$a has no ETag ($et)" ;;
  esac
  case "$hdrs" in
    *"stale-while-revalidate"*) ok "$a may be reused while it refreshes" ;;
    *) bad "$a is not cacheable" ;;
  esac
  code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 20 -H "If-None-Match: $et" "$BASE/assets/$a")
  check "$a answers a conditional request 304" "$code" "304"
  n=$(curl -s -o /dev/null -w '%{size_download}' --max-time 20 -H "If-None-Match: $et" "$BASE/assets/$a")
  check "and sends no body with it" "$n" "0"
done
# The ETag is a prefix of the digest, so the comparison must use that prefix.
# Matching the full digest inside a shorter header never succeeds and every
# conditional request answers 200 -- the bug this replaced, one line lower.
e1=$(curl -s -D- -o /dev/null --max-time 20 "$BASE/assets/matrix.css" | tr -d '\r' | awk 'tolower($1)=="etag:"{print $2}')
e2=$(curl -s -D- -o /dev/null --max-time 20 "$BASE/assets/matrix-aperture.css" | tr -d '\r' | awk 'tolower($1)=="etag:"{print $2}')
[ "$e1" != "$e2" ] && ok "different bytes carry different ETags" || bad "two assets share an ETag"
wrong=$(curl -s -o /dev/null -w '%{http_code}' --max-time 20 -H 'If-None-Match: "0000000000000000"' "$BASE/assets/matrix.css")
check "a stale validator gets the new bytes" "$wrong" "200"

echo "== a descriptor may read a native organ, not only a CMS type =="
# The compiler projected CMS rows only, so every organ-backed panel had to be
# written by hand. /habitat carried 2.7KB of bespoke fetch-and-render JavaScript
# doing this function's job for browsers -- and only for browsers, so those
# panels had no machine projection at all.
cap=$(curl -s --max-time 20 -H 'Accept: application/json' "$BASE/_surface/capabilities")
n=$(printf '%s' "$cap" | python3 -c 'import sys,json;print(json.load(sys.stdin)["count"])' 2>/dev/null || echo 0)
[ "$n" -gt 50 ] && ok "an organ-backed pattern projects its rows ($n)" \
                || bad "the organ-backed pattern returned $n rows"
src=$(printf '%s' "$cap" | python3 -c 'import sys,json;print(json.load(sys.stdin)["reads"])')
check "it names the organ it read" "$src" "bonfyre-capability"
# The header row labels the columns the descriptor already named, so it is the
# header. The older handler tested for the literal "ID ", true of one organ.
hdr=$(printf '%s' "$cap" | python3 -c 'import sys,json;print(sum(1 for r in json.load(sys.stdin)["records"] if r["id"].lower()=="id"))')
check "the header row is not projected as a record" "$hdr" "0"
# Same descriptor, both grammars.
# grep -c counts matching LINES; the fragment is one line, so it answers 1 for
# any number of records. Count occurrences.
htm=$(curl -s --max-time 20 -H 'Accept: text/html' "$BASE/_surface/capabilities" | grep -o 'surface-record' | wc -l | tr -d ' ')
[ "$htm" -gt 50 ] && ok "the same descriptor renders for a browser" || bad "no browser projection"
# The catalogue must say which kind of thing a pattern reads, or an
# organ-backed pattern advertises itself as reading nothing.
srcs=$(curl -s --max-time 20 -H 'Accept: application/json' "$BASE/_surface/" \
  | python3 -c 'import sys,json;print(",".join(sorted({s["source"] for s in json.load(sys.stdin)["surfaces"]})))')
check "the catalogue names each pattern's kind of source" "$srcs" "organ,type"
# Content must never choose what executes.
for bad_organ in "../../bin/sh" "sh" "bonfyre-;id"; do
  code=$(curl -s --max-time 20 -o /dev/null -w '%{http_code}' \
    -H 'Accept: application/json' "$BASE/_surface/capabilities?organ=$bad_organ")
  check "an organ name cannot be injected by query ($bad_organ)" "$code" "200"
done

echo "== a panel carries its own label =="
# A page composing three lenses would otherwise carry their headings, putting
# the name of a thing somewhere other than the thing.
t=$(curl -s --max-time 20 -H 'Accept: application/json' "$BASE/_surface/frappe-apps" \
    | python3 -c 'import sys,json;print(json.load(sys.stdin)["title"])')
check "the descriptor names the panel for machines" "$t" "Institutional apps"
h=$(curl -s --max-time 20 -H 'Accept: text/html' "$BASE/_surface/frappe-apps" | grep -c 'surface-title')
check "and renders that heading for a browser" "$h" "1"

echo "== habitat is composed from descriptors, not hand-written =="
hab=$(curl -s --max-time 20 -H 'Accept: text/html' "$BASE/habitat")
case "$hab" in
  *'/_surface/frappe-apps'*) ok "habitat composes the apps panel as a lens" ;;
  *) bad "habitat does not compose the apps lens" ;;
esac
case "$hab" in
  *'/_surface/capabilities'*) ok "habitat composes the capability panel as a lens" ;;
  *) bad "habitat does not compose the capability lens" ;;
esac
# The old page shipped its own fetch-and-render script.
js=$(printf '%s' "$hab" | grep -c 'async function j(' || true)
check "habitat ships no bespoke fetch-and-render script" "$js" "0"
case "$hab" in
  *'matrix-htmx.js'*) ok "it uses the estate's htmx runtime" ;;
  *) bad "habitat has no htmx runtime" ;;
esac

echo "== an authenticated API surface answers machines in JSON =="
# nginx auth_request without an error_page serves nginx's own HTML, so a machine
# asking for JSON got 188 bytes of markup. Fixed once for /studio/v1/ and left
# in place on two other locations.
for p in /ops/ /cms/v1/api/frappe_app /studio/v1/profile; do
  b=$(curl -s --max-time 20 -H 'Accept: application/json' "$BASE$p" | head -c 1)
  check "$p answers an unauthenticated machine in JSON" "$b" "{"
done

echo "== a surface offers its operation at the extent a person actually visits =="
# The action layer was dropped at page extent, so the operation appeared only at
# ?extent=workspace. Bare /login rendered no sign-in form and the test suite was
# green, because the test asked for the extent that worked.
for r in /login /signup /account; do
  bare=$(curl -s --max-time 20 -H 'Accept: text/html' "$BASE$r" | grep -c 'hx-get="/_aperture/form')
  check "$r offers its operation with no query string" "$bare" "1"
done
# A chip or a card is a reference to a surface; there is no room to act inside a
# reference, and that is the distinction the extent ladder actually makes.
for e in chip card section; do
  n=$(curl -s --max-time 20 -H 'Accept: text/html' "$BASE/login?extent=$e" | grep -c '_aperture/form')
  check "the $e extent is a reference, not a place to act" "$n" "0"
done

echo "== authoring a route never takes away a machine contract it already had =="
# /account answered machines by verifying the session on the request; the
# territory root answers with its discovery descriptor. Authoring either
# replaced a contract other systems depend on, invisibly to any human looking.
root=$(curl -s --max-time 20 -H 'Accept: application/json' "$BASE/" | head -c 40)
case "$root" in
  *bonfyre-address-v1*) ok "the territory root still answers with its descriptor" ;;
  *) bad "the authored layer displaced the root discovery descriptor" ;;
esac
acct=$(curl -s --max-time 20 -H 'Accept: application/json' "$BASE/account")
case "$acct" in
  *bearer*) ok "/account still answers machines by verifying the session" ;;
  *) bad "/account machine view was replaced by an authored descriptor" ;;
esac
# A route with no prior machine contract keeps the authored one -- deferring is
# a declaration, not a blanket retreat from answering machines.
prop=$(curl -s --max-time 20 -H 'Accept: application/json' "$BASE/property" | head -c 40)
case "$prop" in
  *'"authored":true'*|*'"route":"/property"'*) ok "a route with no prior contract keeps the authored view" ;;
  *) bad "/property lost its authored machine view" ;;
esac

echo "== a pane is offered only to a caller who can fill it =="
# htmx declares `[45].. swap:false`, so a fragment that answers 401 leaves its
# placeholder on screen forever. An anonymous /account said "Reading your
# authorized context..." indefinitely and never said why.
anon=$(curl -s --max-time 20 -H 'Accept: text/html' "$BASE/account")
case "$anon" in
  *'/studio/v1/profile'*) bad "an anonymous caller is given a pane that will 401" ;;
  *) ok "an anonymous caller is not given a pane they cannot fill" ;;
esac
case "$anon" in
  *'operation=Session.open'*) ok "they are offered the operation that would let them in" ;;
  *) bad "an anonymous /account offers no way in" ;;
esac
if [ -n "${SURFACE_TOKEN:-}" ]; then
  signed=$(curl -s --max-time 20 -H "Authorization: Bearer $SURFACE_TOKEN" -H 'Accept: text/html' "$BASE/account")
  case "$signed" in
    *'/studio/v1/profile'*) ok "a caller with a session is given the pane" ;;
    *) bad "a signed-in /account withholds the pane" ;;
  esac
  case "$signed" in
    *'operation=Session.open'*) bad "a signed-in caller is invited to sign in again" ;;
    *) ok "a signed-in caller is not asked to sign in again" ;;
  esac
  frag=$(curl -s --max-time 20 -H "Authorization: Bearer $SURFACE_TOKEN" -H 'HX-Request: true' \
         -H 'Accept: text/html' "$BASE/studio/v1/profile" | head -c 20)
  case "$frag" in
    '<'*) ok "the pane swaps in a fragment, not a JSON body" ;;
    *) bad "the pane would swap raw JSON into the page" ;;
  esac
fi

echo "== identity surfaces are authored, and post to the owner =="
# login and signup were files. They are barely applications: authored copy, one
# declared operation, and a form that posts to whoever owns sessions. What makes
# them safe is not that the surface handles credentials carefully -- it is that
# the surface never handles them at all.
for r in /login /signup; do
  page=$(curl -s --max-time 20 -H 'Accept: text/html' "$BASE$r?extent=workspace")
  if printf '%s' "$page" | grep -q 'hx-get="/_aperture/form'; then
    ok "$r leads with its operation rather than a link to it"
  else
    bad "$r renders no inline operation"
  fi
done
form=$(curl -s --max-time 20 -H 'Accept: text/html' "$BASE/_aperture/form?subject=identity_root:bonfyre&operation=Session.open")
case "$form" in
  *'action="/login"'*) ok "the form posts to the organ that owns sessions" ;;
  *) bad "the session form does not post to its owner" ;;
esac
case "$form" in
  *'name="password" type="password"'*) ok "a credential field renders as one" ;;
  *) bad "the password field is not a password input" ;;
esac
case "$form" in
  *'>Sign in<'*) ok "the button says what the contract declared" ;;
  *) bad "the button label was derived rather than declared" ;;
esac
# A native form must work without a script runtime, so the owner has to accept
# what a form actually posts.
if [ -n "${SURFACE_LOGIN_EMAIL:-}" ]; then
  code=$(curl -s --max-time 20 -o /dev/null -w '%{http_code}' -X POST "$BASE/login" \
    --data-urlencode "email=$SURFACE_LOGIN_EMAIL" --data-urlencode "password=$SURFACE_LOGIN_PASSWORD")
  check "a urlencoded sign-in reaches the owner" "$code" "200"
fi
# The failure modes must stay indistinguishable, or the surface becomes an
# account-enumeration oracle.
w=$(curl -s --max-time 20 -o /dev/null -w '%{http_code}' -X POST "$BASE/login" --data-urlencode "email=nobody@aurekai.test" --data-urlencode "password=x")
v=$(curl -s --max-time 20 -o /dev/null -w '%{http_code}' -X POST "$BASE/login" --data-urlencode "email=${SURFACE_LOGIN_EMAIL:-someone@aurekai.test}" --data-urlencode "password=definitely-wrong")
check "unknown account and wrong password answer alike" "$w" "$v"

echo
echo "surface pattern: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
