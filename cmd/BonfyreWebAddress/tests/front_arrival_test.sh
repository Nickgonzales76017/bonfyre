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
entry site_presentation '{"site_id":"member-arrival-work","host":"arrival.aurekai.work","subject_address":"person:arrival","profile":"member-profile-v1","territory":"work","navigation":[],"status":"published"}'
entry site_presentation '{"site_id":"member-arrival-life","host":"arrival.aurekai.life","subject_address":"person:arrival","profile":"member-profile-v1","territory":"life","navigation":[],"status":"published"}'
entry site_presentation '{"site_id":"member-mira-work","host":"mira.aurekai.work","subject_address":"person:mira","profile":"member-profile-v1","territory":"work","navigation":[],"status":"published"}'
entry site_presentation '{"site_id":"member-draft","host":"draft.aurekai.work","subject_address":"person:draft","profile":"member-profile-v1","territory":"work","navigation":[],"status":"draft"}'
entry internet_source '{"family":"arrival","source":"arrival-feed","label":"Arrival Feed","says":"Fixture public source","state":"observed","front_door":"yes","status":"published"}'
entry world '{"world":"arrival-world","title":"Arrival World","character":"scrapbook","lede":"Fixture world","sources":"arrival-feed","cannot_infer":"fixture only","status":"published"}'
entry world '{"world":"arrival-world-twin","title":"Arrival World","character":"scrapbook","lede":"Duplicate-title fixture world","sources":"arrival-feed","cannot_infer":"fixture only","status":"published"}'
matrix="$tmp/matrix.db"; "$web" bootstrap-owned --db "$matrix" >/dev/null
port=$(python3 -c "import socket; s=socket.socket(); s.bind(('127.0.0.1',0)); print(s.getsockname()[1]); s.close()")
BONFYRE_CMS_BINARY="$cms" BONFYRE_CMS_DB="$cmsdb" BONFYRE_CMS_SCHEMAS="$schemas" "$web" serve --db "$matrix" --auth-db "$tmp/auth.db" --port "$port" --site-root "$root/site/matrix" >"$tmp/server.log" 2>&1 &
pid=$!; base="http://127.0.0.1:$port"; attempt=0
until curl -fsS -o /dev/null -H 'Host: aurekai.work' "$base/healthz" 2>/dev/null; do attempt=$((attempt+1)); [ "$attempt" -lt 100 ] && kill -0 "$pid" 2>/dev/null || { cat "$tmp/server.log" >&2; fail 'server did not become ready'; }; sleep 0.05; done
for territory in live life shop work; do curl -fsS -H "Host: aurekai.$territory" -H 'Accept: text/html' "$base/" >"$tmp/$territory.html"; done
python3 - "$tmp" <<'PY'
import pathlib,re,sys
root=pathlib.Path(sys.argv[1]); problems=[]; ledes=set()
def check(ok,msg):
    if not ok: problems.append(msg)
doors=['/people','/communities','/apps?context=self','/programs?context=self','/sources','/w','/organisations']
values=['Arrival Program','Arrival Community','Arrival Feed','Arrival World','arrival.aurekai.work']
for territory in ('live','life','shop','work'):
    page=(root/f'{territory}.html').read_text()
    check(f'data-territory="{territory}"' in page,f'{territory}: arrival territory')
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
    people=re.search(r'<article class="front-arrival-door" data-arrival-door><a class="front-arrival-main" href="/people".*?</article>',page,re.S)
    check(bool(people),f'{territory}: people door')
    if people:
        check('2 published' in people.group(0),f'{territory}: distinct people counted once')
        check('href="https://arrival.aurekai.work/"' in people.group(0),f'{territory}: member opens own work home')
        check('href="https://mira.aurekai.work/"' in people.group(0),f'{territory}: another member opens own home')
        check('arrival.aurekai.life' not in people.group(0),f'{territory}: same person not sampled twice')
        check('arrival.example.test' not in people.group(0) and 'draft.aurekai.work' not in people.group(0),f'{territory}: unrelated and draft sites excluded')
    check('href="/community?community=arrival-community"' in page,f'{territory}: community example links to its own page')
    check('href="/sources?source=arrival-feed"' in page,f'{territory}: source example links to its own record')
    check('href="/w/arrival-world"' in page,f'{territory}: world example links to its own publication')
    check('/examples' not in page and 'A finished arrival example' not in page,f'{territory}: no global examples')
    check(page.count('<a href="/w/arrival-world">Arrival World</a>')==1,f'{territory}: world samples distinct and linked')
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
