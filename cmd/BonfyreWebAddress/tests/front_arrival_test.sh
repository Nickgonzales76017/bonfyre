#!/bin/sh
set -eu
web=${1:?web-address binary required}
case "$web" in /*) ;; *) web="$(pwd)/$web" ;; esac
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
cms="$root/cmd/BonfyreCMS/bonfyre-cms"
[ -x "$cms" ] || { echo 'front arrival: SKIP (cms not built)'; exit 0; }
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-front-arrival.XXXXXX"); pid=''
cleanup(){ [ -n "$pid" ] && kill "$pid" 2>/dev/null || :; rm -rf "$tmp"; }
trap cleanup EXIT HUP INT TERM
fail(){ echo "front arrival: FAIL: $1" >&2; exit 1; }
schemas="$tmp/schemas"; mkdir -p "$schemas"
for type in pack_artifact program_template work_example community_body presentation_template site_presentation internet_source world; do cp "$root/cmd/BonfyreCMS/content-types/$type.json" "$schemas/"; done
cmsdb="$tmp/cms.db"; "$cms" schema migrate --db "$cmsdb" --schemas "$schemas" >/dev/null
entry(){ "$cms" entry create "$1" "$2" --ns root --db "$cmsdb" --schemas "$schemas" >/dev/null; }
entry pack_artifact '{"pack":"arrival-app","canonical_name":"Arrival Studio","publisher":"fixture","layer":"official","kind":"application","purpose":"Front arrival fixture","governed_by":"fixture","cannot_infer":"fixture only","status":"published"}'
entry program_template '{"template_id":"arrival-program","name":"Arrival Program","archetype":"fixture","ordinal":1,"subject_kinds":[],"semantic_slots":[],"capability_families":[],"owner_bindings":[],"surface_policy":"native","page_policy":"program","description":"Front arrival Program fixture","cannot_infer":"fixture only","status":"published"}'
entry work_example '{"example_id":"arrival-example","pack":"arrival-app","territories":"live life shop work","position":1,"kicker":"Finished work","headline":"A finished arrival example","dek":"Fixture outcome","made_with":"Fixture material","observed_at":"2026-10-24T12:00:00Z","picture":"/assets/world-land.svg","picture_alt":"Fixture","routes":"/examples/arrival-example","status":"published"}'
entry community_body '{"community":"arrival-community","subject_address":"topic:arrival","title":"Arrival Community","description":"Fixture community","visibility":"public","cannot_infer":"fixture only","status":"published"}'
entry presentation_template '{"template":"arrival-template","reference_source":"fixture","reference_name":"Arrival Editorial","reference_url":"https://example.test/template","reference_categories":"editorial","family":"editorial","renderer_mix":"publication","density":"roomy","hero_mode":"editorial","navigation_mode":"shared","media_mode":"mixed","typography_mode":"editorial","card_mode":"ruled","chrome_mode":"native","territory_affinity":"","semantic_roles":{},"layout_tokens":{},"acceptance_contract":"fixture","cannot_infer":"presentation only","status":"published"}'
entry site_presentation '{"site_id":"arrival-profile","host":"arrival.example.test","subject_address":"person:arrival","profile":"ghost:arrival-template","territory":"live","navigation":[],"status":"published"}'
entry internet_source '{"family":"arrival","source":"arrival-feed","label":"Arrival Feed","says":"Fixture public source","state":"observed","front_door":"yes","status":"published"}'
entry world '{"world":"arrival-world","title":"Arrival World","character":"scrapbook","lede":"Fixture world","sources":"arrival-feed","cannot_infer":"fixture only","status":"published"}'
entry world '{"world":"arrival-world-twin","title":"Arrival World","character":"scrapbook","lede":"Duplicate-title fixture world","sources":"arrival-feed","cannot_infer":"fixture only","status":"published"}'
matrix="$tmp/matrix.db"; "$web" bootstrap-owned --db "$matrix" >/dev/null
port=$(python3 -c "import socket; s=socket.socket(); s.bind(('127.0.0.1',0)); print(s.getsockname()[1]); s.close()")
BONFYRE_CMS_BINARY="$cms" BONFYRE_CMS_DB="$cmsdb" BONFYRE_CMS_SCHEMAS="$schemas" "$web" serve --db "$matrix" --auth-db "$tmp/auth.db" --port "$port" --site-root "$root/site/matrix" >"$tmp/server.log" 2>&1 &
pid=$!; base="http://127.0.0.1:$port"; attempt=0
until curl -fsS -o /dev/null -H 'Host: aurekai.work' "$base/healthz" 2>/dev/null; do attempt=$((attempt+1)); [ "$attempt" -lt 100 ] && kill -0 "$pid" 2>/dev/null || { cat "$tmp/server.log" >&2; fail 'server did not become ready'; }; sleep 0.05; done
for territory in live life shop work; do curl -fsS -H "Host: aurekai.$territory" -H 'Accept: text/html' "$base/" >"$tmp/$territory.html"; done
# The public brand must not leak into a person's site or an unrelated Host.
curl -sS -H 'Host: independent.example.test' -H 'Accept: text/html' "$base/" >"$tmp/independent.html" || :
if grep -q 'data-brand-masthead' "$tmp/independent.html"; then
    fail 'unrelated host inherited the Aurekai brand'
fi
answer=$(curl -sS -o "$tmp/native-brand.css" -w '%{http_code} %{content_type}' -H 'Host: aurekai.work' "$base/assets/front-brand.css")
case "$answer" in
    "200 text/css"*) cmp -s "$tmp/native-brand.css" "$root/site/matrix/front-brand.css" || fail 'brand CSS does not equal source-owned bytes' ;;
    *) fail "native shared brand asset returned $answer" ;;
esac
python3 - "$tmp" <<'PY'
import pathlib,re,sys
root=pathlib.Path(sys.argv[1]); problems=[]; ledes=set()
def check(ok,msg):
    if not ok: problems.append(msg)
doors=['/people','/communities','/apps?context=self','/programs?context=self','/sources','/w','/organisations']
brand={
    'work':('Build the business people remember.','/apps?context=self'),
    'life':('Make your corner of the internet your own.','/people'),
    'live':('Give your work more places to go.','/channels'),
    'shop':('Build the business behind every product.','/retail')
}
values=['Arrival Program','Arrival Community','Arrival Feed','Arrival World','arrival.example.test']
for territory in ('live','life','shop','work'):
    page=(root/f'{territory}.html').read_text()
    check(f'data-territory="{territory}"' in page,f'{territory}: arrival territory')
    check(page.count('data-brand-masthead')==1,f'{territory}: native brand experience is mounted exactly once')
    check(f'data-brand-territory="{territory}"' in page,f'{territory}: exact host brand')
    check('<title>'+brand[territory][0]+'</title>' in page,f'{territory}: source-owned title')
    check('property="og:title" content="'+brand[territory][0]+'"' in page,f'{territory}: social preview title')
    check('property="og:description"' in page,f'{territory}: social description')
    check(f'<link rel="canonical" href="https://aurekai.{territory}/">' in page,f'{territory}: exact canonical address')

    check('Built on software.<br>Driven by Aurekai.' in page,f'{territory}: enduring Aurekai signature')
    check(brand[territory][0] in page,f'{territory}: correct territory-specific headline')
    check(f'href="{brand[territory][1]}"' in page,f'{territory}: real destination-owned primary action')
    check('href="https://nickgonzales76017.github.io/bonfyre/about/"' in page,f'{territory}: working company story')
    check(page.count('<nav class="af-brand-nav"')==1,f'{territory}: one visible four-apex navigation')
    for apex in ('work','life','live','shop'):
        check(f'href="https://aurekai.{apex}/"' in page,f'{territory}: link to {apex}')
    check(page.count('aria-current="page"')==1,f'{territory}: exactly one active apex')
    check(f'href="https://aurekai.{territory}/" aria-current="page"' in page,f'{territory}: active apex is correct')
    check('href="/assets/front-brand.css"' in page,f'{territory}: shared cached stylesheet')
    check('af-brand-title' in page and 'af-brand-links' in page,f'{territory}: editorial hierarchy and real actions')
    check('data-brand-masthead' in page and 'data-brand-territory='+territory in page,f'{territory}: scoped native art direction')
    check(len(re.findall(r'<article\b[^>]*\bdata-arrival-door\b',page))==7,f'{territory}: seven doors')
    primary=page.split('<details class="front-arrival-more"',1)[0]
    secondary=page.split('<details class="front-arrival-more"',1)[-1]
    check(primary.count('data-arrival-door')==4,f'{territory}: four territory-first doors')
    check(secondary.count('data-arrival-door')==3,f'{territory}: three discoverable secondary doors')
    check('data-arrival-templates' not in secondary,f'{territory}: no presentation showcase')
    check('Arrival Studio' not in page and 'Arrival Editorial' not in page,f'{territory}: no app or presentation catalogue')
    first={'work':'/apps?context=self','live':'/people','life':'/people','shop':'/sources'}[territory]
    check(f'href="{first}"' in primary,f'{territory}: first door is contextual')
    check(len(re.findall(r'data-arrival-icon[^>]*>\s*<svg\b',page))==7,f'{territory}: seven SVG icons')
    for href in doors: check(f'href="{href}"' in page,f'{territory}: door {href}')
    for value in values: check(value in page,f'{territory}: published {value}')
    check('/examples' not in page and 'A finished arrival example' not in page,f'{territory}: no global examples')
    check(page.count('<li>Arrival World</li>')==1,f'{territory}: world samples are distinct')
    for href in ('/profile','/apps?context=self','/programs?context=self'): check(f'href="{href}"' in page,f'{territory}: account {href}')
    m=re.search(r'data-arrival-lede>([^<]+)',page); check(bool(m),f'{territory}: lede')
    if m: ledes.add(m.group(1))
check(len(ledes)==4,'territory-specific ledes')
if problems:
    print('front arrival: FAIL',file=sys.stderr)
    for p in problems: print('  -',p,file=sys.stderr)
    raise SystemExit(1)
print('front arrival: PASS')
PY
