#!/bin/sh
# The A2A carrier and per-territory page identity.
#
# Two defects this pins shut:
#
#   1. The agent card advertised "url":"https://<host>/a2a" while nothing was
#      routed there. Every agent that discovered the card followed it into a
#      404 -- an advertised interface with no door behind it.
#   2. index.html carried <title>, og:* and rel="canonical" hard-coded to
#      aurekai.life, so .live, .shop and .work each told every crawler they
#      were duplicates of .life. Three territories suppressed themselves, and
#      the only signal that distinguished them sat inside a <script> that no
#      social-preview fetcher executes.
#
# What this cannot prove: that any skill executes. Execution is paid and needs
# a configured facilitator; with none, the correct answer is the quote.
set -eu

# This suite runs the organ deliberately without a CMS, to exercise the compiled
# floor beneath the authored surfaces. A BONFYRE_CMS_DB inherited from the
# caller silently changes what is being tested -- the root then serves its
# authored page and the assertion about the compiled title fails for a reason
# that has nothing to do with this organ. The environment is cleared here so the
# suite cannot be perturbed by whoever invoked it.
unset BONFYRE_CMS_DB BONFYRE_CMS_SCHEMAS 2>/dev/null || true

root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
bin="$root/cmd/BonfyreWebAddress/bonfyre-web-address"
tmp=$(mktemp -d)
pid=''
cleanup() { [ -n "$pid" ] && kill "$pid" 2>/dev/null || true; rm -rf "$tmp"; }
trap cleanup EXIT INT TERM
db="$tmp/matrix.db"
# A fixed listener makes parallel WebAddress witnesses race each other and can
# turn an unrelated server's health response into a false startup success.
port=$((20000 + ($$ % 30000)))
base="http://127.0.0.1:$port"

"$bin" bootstrap-owned --db "$db" >/dev/null
"$bin" domain --db "$db" --host aurekai.life --territory life --scope self \
  --world live --repr HTML --evidence receipt:a2a:life >/dev/null
"$bin" domain --db "$db" --host aurekai.live --territory live --scope public \
  --world live --repr HTML --evidence receipt:a2a:live >/dev/null
"$bin" domain --db "$db" --host aurekai.shop --territory shop --scope market \
  --world live --repr HTML --evidence receipt:a2a:shop >/dev/null
"$bin" domain --db "$db" --host aurekai.work --territory work --scope institution \
  --world live --repr HTML --evidence receipt:a2a:work >/dev/null

"$bin" serve --db "$db" --port "$port" --site-root "$root/site/matrix" >"$tmp/server.log" 2>&1 &
pid=$!
i=0
while [ $i -lt 60 ]; do
    kill -0 "$pid" 2>/dev/null || {
        echo "a2a server exited during startup on port $port" >&2
        tail -n 20 "$tmp/server.log" >&2
        exit 1
    }
    curl -fsS "$base/healthz" >/dev/null 2>&1 && break
    i=$((i+1)); sleep 0.1
done
curl -fsS "$base/healthz" >/dev/null

get()  { curl -fsS -H "Host: $1" "$base$2"; }
getr() { curl -s   -H "Host: $1" "$base$2"; }   # body wanted even on a refusal
post() { curl -s -H "Host: aurekai.life" -X POST -d "$1" "$base/a2a"; }

# ---- the card's promise and the door behind it -------------------------------
card=$(get aurekai.life /.well-known/agent.json)
printf '%s' "$card" | grep -q '"url":"https://aurekai.life/a2a"'

# The endpoint the card names answers, and answers as A2A rather than as the
# generic JSON every other protocol projection returns.
code=$(curl -s -o "$tmp/a2a.json" -w '%{http_code}' -H 'Host: aurekai.life' "$base/a2a")
[ "$code" = "200" ] || { echo "a2a: card url answered $code, not 200" >&2; exit 1; }
curl -fsS -o /dev/null -w '%{content_type}' -H 'Host: aurekai.life' "$base/a2a" \
  | grep -q 'application/a2a+json'

# Every skill the card advertises is described by the endpoint, and vice versa.
# One table feeds both, so a skill cannot appear in only one of them.
for skill in parcel-underwrite owner-concentration work-mission; do
    printf '%s' "$card" | grep -q "\"id\":\"$skill\""
    grep -q "\"id\":\"$skill\"" "$tmp/a2a.json"
done

# Discovery is free and says so; execution is never described as free.
grep -q '"discovery":"free"' "$tmp/a2a.json"
grep -q '"forbidden_inference"' "$tmp/a2a.json"

# ---- JSON-RPC refusals are typed, not generic --------------------------------
# An unknown skill is invalid params and is handed back the card rather than a
# guess at what the caller meant.
post '{"jsonrpc":"2.0","id":"t1","method":"message/send","params":{"skill":"no-such-skill"}}' \
  | grep -q '"code":-32602'
# An unsupported method names what is supported.
post '{"jsonrpc":"2.0","id":"t2","method":"tasks/cancel"}' | grep -q '"code":-32601'
# A request with no method is an invalid request.
post '{"jsonrpc":"2.0","id":"t3"}' | grep -q '"code":-32600'
# The JSON-RPC id is echoed, so a client can match the answer to its call.
post '{"jsonrpc":"2.0","id":"t4","method":"tasks/cancel"}' | grep -q '"id":"t4"'
# A skill that is real but not a priced query says where it is admitted instead
# of pretending it is for sale here.
post '{"jsonrpc":"2.0","id":"t5","method":"message/send","params":{"skill":"work-mission"}}' \
  | grep -q '"code":-32004'
# GET-only and POST-only are distinguished rather than collapsed into 404.
curl -s -o /dev/null -w '%{http_code}' -X DELETE -H 'Host: aurekai.life' "$base/a2a" | grep -q 405

# The sibling routes are not swallowed by the new exact-match handler.
curl -fsS -o /dev/null -H 'Host: aurekai.life' "$base/a2a/agent.json"
getr aurekai.life /a2a/resources/ | grep -q 'surface_not_served'

# ---- per-territory identity, readable without executing JavaScript -----------
# Four hosts, four titles, each canonical to itself. A fetcher that runs no
# script must still be able to tell the territories apart.
prev_title=''
for pair in 'aurekai.life workshop' 'aurekai.live open' 'aurekai.shop market' 'aurekai.work institution'; do
    set -- $pair
    host=$1; want=$2
    page=$(get "$host" /)
    case "$page" in
      *'id="estate-try"'*'hx-get="/_surface/external-estate"'*) : ;;
      *) echo "identity: $host lost the native public search" >&2; exit 1 ;;
    esac

    title=$(printf '%s' "$page" | grep -o '<title>[^<]*</title>')
    printf '%s' "$title" | grep -q "$want" || {
        echo "identity: $host title did not mention $want: $title" >&2; exit 1; }
    [ "$title" != "$prev_title" ] || {
        echo "identity: $host repeated the previous host's title" >&2; exit 1; }
    prev_title=$title

    # Self-canonical. The whole defect was three hosts naming a fourth.
    printf '%s' "$page" | grep -q "<link rel=\"canonical\" href=\"https://$host/\">" || {
        echo "identity: $host is not canonical to itself" >&2; exit 1; }
    printf '%s' "$page" | grep -q "<meta property=\"og:url\" content=\"https://$host/\">"
    printf '%s' "$page" | grep -q '<meta property="og:title"'
    printf '%s' "$page" | grep -q '<meta name="description"'

    # Exactly one of each identity tag: a second <title> or canonical is worse
    # than none, because the crawler picks and the server no longer decides.
    # Full tag syntax, not the bare name: a comment mentioning a tag is not
    # an occurrence of it, and a count that cannot tell them apart is noise.
    for tag in '<title>' '<link rel="canonical"' '<meta property="og:url"' \
               '<meta property="og:title"' '<meta name="description"'; do
        n=$(printf '%s\n' "$page" | grep -c "$tag" || true)
        [ "$n" = "1" ] || { echo "identity: $host has $n of $tag, want 1" >&2; exit 1; }
    done
done

# The resolved context stays where it was; this adds tags, it does not replace
# the subject the root HTML already carried.
get aurekai.shop / | grep -q '"subject":"institution:aurekai:shop"'
get aurekai.shop / | grep -q '"audience":"market"'

# ---- one resource, two carriers ---------------------------------------------
# The rule this file pins: a person asking for HTML never receives raw JSON, and
# a machine asking for JSON never receives a page. /protocols was the only
# surface honouring it; the capability catalogue and the A2A carrier handed
# everyone the same JSON, which is how a priced catalogue becomes unreadable by
# the person deciding whether to buy.
HTML_ACCEPT='text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
geth() { curl -s -H "Host: aurekai.life" -H "Accept: $HTML_ACCEPT" "$base$1"; }
getj() { curl -s -H "Host: aurekai.life" -H 'Accept: application/json' "$base$1"; }
ctype() { curl -s -o /dev/null -w '%{content_type}' -H "Host: aurekai.life" -H "Accept: $2" "$base$1"; }

for path in /a2a /capability /protocols; do
    ctype "$path" "$HTML_ACCEPT" | grep -q 'text/html' || {
        echo "carrier: $path served a browser $(ctype "$path" "$HTML_ACCEPT"), not HTML" >&2; exit 1; }
    # A page, not a JSON body wearing an HTML content-type.
    geth "$path" | head -c 1 | grep -q '<' || {
        echo "carrier: $path sent a browser a JSON body" >&2; exit 1; }
    # Same page grammar every time: one stylesheet, one shared bar.
    geth "$path" | grep -q '/assets/matrix.css' || {
        echo "carrier: $path page does not use the shared stylesheet" >&2; exit 1; }
    geth "$path" | grep -q 'class="ak-bar"' || {
        echo "carrier: $path page has no shared bar" >&2; exit 1; }
done

# The machine carrier is unchanged by any of that.
ctype /a2a 'application/json' | grep -q 'application/a2a+json'
getj /a2a | head -c 1 | grep -q '{'
getj /capability | head -c 1 | grep -q '{'

# Price parity: whatever the JSON carrier says about a capability being priced,
# the page must say the same. Two carriers, one service book, one answer.
for capname in parcel-underwrite owner-concentration estate-coverage frappe-schema; do
    jpriced=$(getj /capability | python3 -c "
import sys,json
d=json.load(sys.stdin)
for c in d['capabilities']:
    if c['name']=='$capname': print('true' if c['priced'] else 'false'); break
else: print('missing')")
    page=$(geth /capability)
    # Locate this capability's door and read the price line inside it.
    hpriced=$(printf '%s' "$page" | python3 -c "
import sys,re
p=sys.stdin.read()
i=p.find('<h3>$capname</h3>')
print('missing' if i<0 else ('false' if 'unpriced' in p[i:i+400] else 'true'))")
    [ "$jpriced" = "$hpriced" ] || {
        echo "parity: $capname priced=$jpriced in JSON but $hpriced on the page" >&2; exit 1; }
done

# The A2A page names every skill the card advertises, so a reader and a crawler
# are looking at the same institution.
page=$(geth /a2a)
for skill in parcel-underwrite owner-concentration work-mission; do
    printf '%s' "$page" | grep -q "$skill" || {
        echo "carrier: /a2a page omits advertised skill $skill" >&2; exit 1; }
done
printf '%s' "$page" | grep -q 'message/send'

# ---- the machine direction -------------------------------------------------
# A surface holding both representations must hand a machine the JSON one. This
# used to always prefer .html, so Accept: application/json got a full page.
for surf in agents market property sources; do
    ct=$(curl -s -o /dev/null -w '%{content_type}' -H "Host: aurekai.life" \
         -H 'Accept: application/json' "$base/$surf")
    case "$ct" in
      *json*) : ;;
      *) echo "carrier: /$surf served a JSON client $ct" >&2; exit 1 ;;
    esac
    curl -s -H "Host: aurekai.life" -H 'Accept: application/json' "$base/$surf" \
      | head -c 1 | grep -qE '[{[]' || {
        echo "carrier: /$surf sent a JSON client a non-JSON body" >&2; exit 1; }
done

# The root answers an explicit JSON ask with its declared machine view.
curl -s -H "Host: aurekai.life" -H 'Accept: application/json' "$base/" \
  | grep -q '"version":"bonfyre-address-v1"' || {
    echo "carrier: / did not answer a JSON client with the address descriptor" >&2; exit 1; }

# A client that did not say what it wanted keeps exactly what it had. Absence of
# an HTML ask is not a machine ask; flipping those would rewrite what every
# plain shell fetch has been reading.
for surf in agents market; do
    curl -s -H "Host: aurekai.life" -H 'Accept: */*' "$base/$surf" \
      | head -c 1 | grep -q '<' || {
        echo "carrier: /$surf changed representation for a wildcard client" >&2; exit 1; }
done
curl -s -H "Host: aurekai.life" "$base/" | head -c 1 | grep -q '<' || {
    echo "carrier: / changed representation for a client sending no Accept" >&2; exit 1; }

# And a browser still gets pages everywhere.
for surf in agents market property sources; do
    curl -s -H "Host: aurekai.life" -H "Accept: $HTML_ACCEPT" "$base/$surf" \
      | head -c 1 | grep -q '<' || {
        echo "carrier: /$surf stopped serving a browser HTML" >&2; exit 1; }
done

# ---- one settlement declaration, five grammars ------------------------------
# Each commerce carrier renders the same rail rather than asserting its own.
# Beckn used to quote an amount and declare nothing, so an agent arriving there
# got a price with no way to pay it.
for surf in /mpp/quote /ucp/catalog /acp/products /beckn/search; do
    out=$(curl -s -H "Host: aurekai.life" -H 'Accept: application/json' "$base$surf")
    printf '%s' "$out" | grep -q 'x402' || {
        echo "settlement: $surf names no rail" >&2; exit 1; }
done
# Beckn declares it in Beckn's own vocabulary, not someone else's.
curl -s -H "Host: aurekai.life" -H 'Accept: application/json' "$base/beckn/search" \
  | grep -q '"bpp/payments"' || {
    echo "settlement: beckn quotes without a bpp/payments block" >&2; exit 1; }

echo "settlement declaration: PASS"

echo "machine direction: PASS"

echo "carrier parity: PASS"

echo "a2a + territory identity: PASS"
