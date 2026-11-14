#!/bin/sh
# The four owned Aurekai apex roots open on the native brand masthead, over
# real HTTP from the built WebAddress service. Ported from the retired
# front_arrival_test.sh: the arrival doors are gone (the edited front replaced
# them), the brand contract they carried is kept here.
set -eu
web=${1:?web-address binary required}
case "$web" in /*) ;; *) web="$(pwd)/$web" ;; esac
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
cms="$root/cmd/BonfyreCMS/bonfyre-cms"
[ -x "$cms" ] || { echo 'front brand page: SKIP (cms not built)'; exit 0; }
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-front-brand.XXXXXX"); pid=''
cleanup(){ [ -n "$pid" ] && kill "$pid" 2>/dev/null || :; rm -rf "$tmp"; }
trap cleanup EXIT HUP INT TERM
fail(){ echo "front brand page: FAIL: $1" >&2; exit 1; }
schemas="$tmp/schemas"; mkdir -p "$schemas"
for type in internet_source world; do cp "$root/cmd/BonfyreCMS/content-types/$type.json" "$schemas/"; done
cmsdb="$tmp/cms.db"; "$cms" schema migrate --db "$cmsdb" --schemas "$schemas" >/dev/null
entry(){ "$cms" entry create "$1" "$2" --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null; }
entry internet_source '{"family":"brand","source":"brand-feed","label":"Brand Feed","says":"Fixture public source","state":"observed","front_door":"yes","status":"published"}'
entry world '{"world":"brand-world","title":"Brand World","character":"scrapbook","lede":"Fixture world","sources":"brand-feed","cannot_infer":"fixture only","status":"published"}'
matrix="$tmp/matrix.db"; "$web" bootstrap-owned --db "$matrix" >/dev/null
port=$(python3 -c "import socket; s=socket.socket(); s.bind(('127.0.0.1',0)); print(s.getsockname()[1]); s.close()")
BONFYRE_CMS_BINARY="$cms" BONFYRE_CMS_DB="$cmsdb" BONFYRE_CMS_SCHEMAS="$schemas" "$web" serve --db "$matrix" --auth-db "$tmp/auth.db" --port "$port" --site-root "$root/site/matrix" >"$tmp/server.log" 2>&1 &
pid=$!; base="http://127.0.0.1:$port"; attempt=0
until curl -fsS -o /dev/null -H 'Host: aurekai.work' "$base/healthz" 2>/dev/null; do attempt=$((attempt+1)); [ "$attempt" -lt 100 ] && kill -0 "$pid" 2>/dev/null || { cat "$tmp/server.log" >&2; fail 'server did not become ready'; }; sleep 0.05; done
for territory in live life shop work; do curl -fsS -H "Host: aurekai.$territory" -H 'Accept: text/html' "$base/" >"$tmp/$territory.html"; done
# The public brand must not leak into a person's site, an unrelated Host, or a nested route.
curl -sS -H 'Host: independent.example.test' -H 'Accept: text/html' "$base/" >"$tmp/independent.html" || :
grep -q 'data-brand-masthead' "$tmp/independent.html" && fail 'unrelated host inherited the Aurekai brand'
curl -sS -H 'Host: aurekai.work' -H 'Accept: text/html' "$base/discover" >"$tmp/nested.html" || :
grep -q 'data-brand-masthead' "$tmp/nested.html" && fail 'nested route inherited the apex brand masthead'
answer=$(curl -sS -o "$tmp/native-brand.css" -w '%{http_code} %{content_type}' -H 'Host: aurekai.work' "$base/assets/front-brand.css")
case "$answer" in
    "200 text/css"*) cmp -s "$tmp/native-brand.css" "$root/site/matrix/front-brand.css" || fail 'brand CSS does not equal source-owned bytes' ;;
    *) fail "native shared brand asset returned $answer" ;;
esac
brand_css_version=$(grep -oE 'front-brand\.css\?v=[0-9a-f]{16}' "$tmp/work.html" | head -1 | sed 's/.*=//')
[ -n "$brand_css_version" ] || fail 'no digest-versioned brand stylesheet in Work HTML'
versioned_answer=$(curl -sS -o "$tmp/brand-versioned.css" -w '%{http_code} %{content_type}' -H 'Host: aurekai.work' "$base/assets/front-brand.css?v=$brand_css_version")
case "$versioned_answer" in
    "200 text/css"*) cmp -s "$tmp/brand-versioned.css" "$root/site/matrix/front-brand.css" || fail 'versioned brand CSS bytes differ from source' ;;
    *) fail "native versioned brand CSS returned $versioned_answer" ;;
esac
python3 - "$tmp" <<'PY'
import pathlib,re,sys
root=pathlib.Path(sys.argv[1]); problems=[]; brand_versions=set()
def check(ok,msg):
    if not ok: problems.append(msg)
brand={
    'work':('Build the business people remember.','/apps?context=self'),
    'life':('Make your corner of the internet your own.','/people'),
    'live':('Give your work more places to go.','/channels'),
    'shop':('Build the business behind every product.','/retail')
}
for territory in ('live','life','shop','work'):
    page=(root/f'{territory}.html').read_text()
    check('class="ak-app' in page,f'{territory}: renders inside the one estate shell')
    check(page.count('data-brand-masthead')==1,f'{territory}: native brand masthead is mounted exactly once')
    check(page.count('<h1')==1,f'{territory}: exactly one primary heading')
    check('<h1 class="af-brand-title" id="af-brand-title">' in page,f'{territory}: native brand is the document H1')
    check('<header class="hero">' not in page,f'{territory}: no redundant generic lead')
    main=page.find('<main'); masthead=page.find('data-brand-masthead'); front=page.find('class="fd-head"')
    check(0<=main<masthead,f'{territory}: brand opens the page main')
    check(front<0 or masthead<front,f'{territory}: brand precedes the edited front')
    stylesheet=re.search(r'<link[^>]*href="/assets/front-brand\.css\?v=([0-9a-f]{16})"[^>]*>',page[:page.find('</head>')])
    check(bool(stylesheet),f'{territory}: versioned native CSS loads in document head')
    if stylesheet: brand_versions.add(stylesheet.group(1))
    check(f'data-brand-territory="{territory}"' in page,f'{territory}: exact host brand')
    check('<title>'+brand[territory][0]+'</title>' in page,f'{territory}: source-owned title')
    check('property="og:title" content="'+brand[territory][0]+'"' in page,f'{territory}: social preview title')
    check('property="og:description"' in page,f'{territory}: social description')
    check(page.count('rel="canonical"')==1,f'{territory}: one canonical address')
    check(f'<link rel="canonical" href="https://aurekai.{territory}/">' in page,f'{territory}: exact canonical address')
    check('Built on software.<br>Driven by Aurekai.' in page,f'{territory}: enduring Aurekai signature')
    check(f'href="{brand[territory][1]}"' in page,f'{territory}: real destination-owned primary action')
    check('href="https://nickgonzales76017.github.io/bonfyre/about/"' in page,f'{territory}: working company story')
    check(page.count('<nav class="af-brand-nav"')==1,f'{territory}: one visible four-apex navigation')
    for apex in ('work','life','live','shop'):
        check(f'href="https://aurekai.{apex}/"' in page,f'{territory}: link to {apex}')
    brand_nav=re.search(r'<nav class="af-brand-nav"[^>]*>.*?</nav>',page,re.S)
    check(bool(brand_nav) and brand_nav.group(0).count('aria-current="page"')==1,f'{territory}: exactly one active apex within the brand navigation')
    check(f'href="https://aurekai.{territory}/" aria-current="page"' in page,f'{territory}: active apex is correct')
    check('af-brand-title' in page and 'af-brand-links' in page and 'af-brand-side' in page,f'{territory}: editorial hierarchy and real actions')
check(len(brand_versions)==1,'the four territories use one immutable versioned brand stylesheet')
if problems:
    print('front brand page: FAIL',file=sys.stderr)
    for p in problems: print('  -',p,file=sys.stderr)
    raise SystemExit(1)
print('front brand page: PASS')
PY
