#!/bin/sh
# CMS SourceCollection + admitted SubjectFabric reader + normalized item stream
# must become the route WebAddress actually serves.
set -eu

web=${1:?web-address binary required}
case "$web" in /*) ;; *) web="$(pwd)/$web" ;; esac
root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
cms="$root/cmd/BonfyreCMS/bonfyre-cms"
auth="$root/cmd/BonfyreAuth/bonfyre-auth"
compiler="$root/cmd/BonfyreExternalEstate/tools/compile_subject_projection.mjs"
verify="$root/cmd/BonfyreExternalEstate/tools/verify_subject_projection.mjs"
[ -x "$cms" ] || { echo 'compiled subject route web: SKIP (cms not built)'; exit 0; }
[ -x "$auth" ] || { echo 'compiled subject route web: SKIP (auth not built)'; exit 0; }

tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-compiled-route.XXXXXX")
pid=''
cleanup() { [ -n "$pid" ] && kill "$pid" 2>/dev/null || true; rm -rf "$tmp"; }
trap cleanup EXIT INT TERM
mkdir -p "$tmp/schemas" "$tmp/saved-type"
cp "$root/cmd/BonfyreCMS/content-types/source_collection.json" "$tmp/schemas/"
cp "$root/cmd/BonfyreCMS/content-types/internet_source.json" "$tmp/schemas/"
cp "$root/cmd/BonfyreCMS/content-types/presentation_template.json" "$tmp/schemas/"
cp "$root/cmd/BonfyreCMS/content-types/saved_item.json" "$tmp/saved-type/"
cmsdb="$tmp/cms.db"
fabric="$tmp/fabric.db"
matrix="$tmp/matrix.db"
"$cms" schema migrate --db "$cmsdb" --schemas "$tmp/schemas" >/dev/null
"$cms" entry create source_collection \
  '{"collection_id":"fixture-exact-discover","subject_address":"institution:aurekai:live","source_selector":{"domains":["discover"],"source_refs":["included"]},"material_kind":["PublicationOccurrence"],"refresh_policy":"fixture","status":"published"}' \
  --ns root --db "$cmsdb" --schemas "$tmp/schemas" >/dev/null
"$cms" entry create internet_source \
  '{"family":"news","source":"included","label":"Included source","says":"fixture","url":"https://included.example/feed","twin":"included-twin","state":"observed","extract":"items","title_field":"title","meta_field":"published","link_field":"url","summary_field":"body","image_field":"","image_base":"","sub_field":"","body_regime":"retained-public","status":"published","tier":"warm","kind":"text"}' \
  --ns root --db "$cmsdb" --schemas "$tmp/schemas" >/dev/null
"$cms" entry create internet_source \
  '{"family":"news","source":"excluded","label":"Excluded source","says":"fixture","url":"https://excluded.example/feed","twin":"excluded-twin","state":"observed","extract":"items","title_field":"title","meta_field":"published","link_field":"url","image_field":"","image_base":"","sub_field":"","body_regime":"retained-public","status":"published","tier":"warm","kind":"text"}' \
  --ns root --db "$cmsdb" --schemas "$tmp/schemas" >/dev/null
"$cms" entry create presentation_template \
  '{"template":"ghost-fixture-newsroom","reference_source":"ghost","reference_name":"Fixture newsroom","reference_url":"https://ghost.org/themes/","reference_categories":"news","family":"newsroom","renderer_mix":"publication","density":"comfortable","hero_mode":"lead","navigation_mode":"inline","media_mode":"image","typography_mode":"editorial","card_mode":"plain","chrome_mode":"quiet","semantic_roles":{},"layout_tokens":{"ground":"#f9f0e1","ink":"#111111","accent":"#e15920"},"acceptance_contract":"presentation only","cannot_infer":"reference candidate is not a copied theme","status":"reference_candidate"}' \
  --ns root --db "$cmsdb" --schemas "$tmp/schemas" >/dev/null
# A CMS fixture that fails a fresh readonly open must not be mistaken for
# an absent SourceCollection by the downstream native projection compiler.
test -s "$cmsdb"
sqlite3 -readonly "$cmsdb" "SELECT COUNT(*) FROM internet_source" | grep -qx 2
sqlite3 -readonly "$cmsdb" "SELECT COUNT(*) FROM source_collection" | grep -qx 1
mkdir -p "$tmp/foreign/receipts" "$tmp/foreign/bodies"
python3 - "$tmp/foreign" <<'PY'
import json,os,sys
root=sys.argv[1]
with open(os.path.join(root,"receipts","excluded-twin.json"),"w") as fh:
    json.dump({"body_path":os.path.join(root,"bodies","excluded.body")},fh)
with open(os.path.join(root,"receipts","included-twin.json"),"w") as fh:
    json.dump({"body_path":os.path.join(root,"bodies","included.body")},fh)
with open(os.path.join(root,"bodies","included.body"),"w") as fh:
    json.dump({"items":[{"title":"COMPILED EXACT SOURCE ITEM","body":"The retained public reading for this item.","published":"2026-09-29T00:00:00Z","url":"https://included.example/item-one"}]},fh)
with open(os.path.join(root,"bodies","excluded.body"),"w") as fh:
    json.dump({"items":[{"title":"SAME FAMILY MUST NOT LEAK","published":"2026-09-29T00:00:00Z","url":"https://excluded.example/item-two"}]},fh)
PY

BONFYRE_CMS_DB="$cmsdb" BONFYRE_FOREIGN_ROOT="$tmp/foreign" \
  "$web" census-items >"$tmp/items.jsonl"
python3 - "$tmp/items.jsonl" <<'PY'
import json,sys
rows=[json.loads(line) for line in open(sys.argv[1])]
assert len(rows)==2,rows
assert {row["source"] for row in rows}=={"included","excluded"},rows
assert next(row for row in rows if row["source"]=="included")["body"]=="The retained public reading for this item.",rows
PY

python3 - "$fabric" "$tmp/page.json" \
  "$root/cmd/BonfyreWebAddress/generated/native-page-composition.json" <<'PY'
import json,sqlite3,sys
fabric,page,generated=sys.argv[1:]
db=sqlite3.connect(fabric)
db.executescript("""
CREATE TABLE subject_bundle(subject TEXT,scope TEXT,bundle_digest TEXT);
CREATE TABLE subject_reader(
 subject TEXT,scope TEXT,census_source TEXT,root_id TEXT,channel_id TEXT,material TEXT,
 protocol TEXT,state TEXT,last_contributed TEXT,contributions INTEGER,first_seen TEXT);
INSERT INTO subject_bundle VALUES('institution:aurekai:live','public','fixture-bundle');
INSERT INTO subject_reader VALUES('institution:aurekai:live','public','included','root-included','channel-included','publication','RSS','admitted','',0,'');
INSERT INTO subject_reader VALUES('institution:aurekai:live','public','excluded','root-excluded','channel-excluded','publication','RSS','admitted','',0,'');
""")
db.commit();db.close()
program=json.load(open(generated))
for route in program["programs"]:
    if route["route"]=="/discover":
        route["title"]="Compiled SourceCollection Discover"
        route["lede"]="A route compiled from one exact CMS collection."
program["programs"]=[route for route in program["programs"] if route["route"]=="/discover"]
with open(page,"w") as fh:
    json.dump(program,fh)
PY

surface="$root/cmd/BonfyreWebAddress/generated/native-surface-programs.json"
bun "$compiler" --fabric-db "$fabric" --cms-db "$cmsdb" \
  --items "$tmp/items.jsonl" --page-composition "$tmp/page.json" \
  --surface-programs "$surface" --manifest "$tmp/manifest.json" >/dev/null
bun "$verify" --fabric-db "$fabric" --cms-db "$cmsdb" \
  --items "$tmp/items.jsonl" --page-composition "$tmp/page.json" \
  --surface-programs "$surface" --manifest "$tmp/manifest.json" >/dev/null
included_key=$(python3 -c 'import json,sys; print(next(json.loads(line)["key"] for line in open(sys.argv[1]) if json.loads(line)["source"]=="included"))' "$tmp/items.jsonl")
excluded_key=$(python3 -c 'import json,sys; print(next(json.loads(line)["key"] for line in open(sys.argv[1]) if json.loads(line)["source"]=="excluded"))' "$tmp/items.jsonl")
python3 - "$fabric" <<'PY'
import sqlite3,sys
db=sqlite3.connect(sys.argv[1])
row=db.execute("SELECT basis,reader_count,item_count FROM subject_route WHERE subject='institution:aurekai:live' AND route='/discover'").fetchone()
assert row==("scene_source_collection_exact",1,1),row
db.close()
PY

"$web" bootstrap-owned --db "$matrix" >/dev/null
port=$(python3 -c "import socket; s=socket.socket(); s.bind(('127.0.0.1',0)); print(s.getsockname()[1]); s.close()")
BONFYRE_AUTH_BINARY="$auth" BONFYRE_CMS_BINARY="$cms" BONFYRE_CMS_DB="$cmsdb" BONFYRE_CMS_SCHEMAS="$tmp/schemas" \
BONFYRE_SAVED_ITEM_SCHEMAS="$tmp/saved-type" \
BONFYRE_FOREIGN_ROOT="$tmp/foreign" \
BONFYRE_SUBJECT_FABRIC_DB="$fabric" BONFYRE_PUBLIC_PROJECTION_DB="$tmp/public-projections.db" \
  "$web" serve --db "$matrix" --auth-db "$tmp/auth.db" --port "$port" \
  --site-root "$root/site/matrix" >"$tmp/server.log" 2>&1 &
pid=$!
attempt=0
until curl -sS -o /dev/null -H 'Host: aurekai.live' "http://127.0.0.1:$port/healthz" 2>/dev/null; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 80 ] || ! kill -0 "$pid" 2>/dev/null; then
    echo 'compiled subject route web: server did not start' >&2
    exit 1
  fi
  sleep 0.05
done
code=$(curl -sS -o "$tmp/discover.html" -w '%{http_code}' \
  -H 'Host: aurekai.live' -H 'Accept: text/html' "http://127.0.0.1:$port/discover")
[ "$code" = 200 ]
python3 - "$tmp/discover.html" "$included_key" <<'PY'
import sys
html=open(sys.argv[1]).read(); key=sys.argv[2]
assert "Compiled SourceCollection Discover" in html
assert "A route compiled from one exact CMS collection." in html
assert "COMPILED EXACT SOURCE ITEM" in html
assert "SAME FAMILY MUST NOT LEAK" not in html
assert "/o/included/"+key in html
assert 'data-bf-profile="ghost-fixture-newsroom"' in html
assert 'data-bf-standing="reference_candidate"' in html
assert '--w-ground:#f9f0e1;' in html
assert 'type="application/atom+xml"' in html and 'href="/feed.atom"' in html
PY
# A committed route revision must invalidate the anonymous WebAddress cache
# immediately even when CMS/ForeignTwin/archive mtimes never move.
# This is the real downstream boundary: the compiler's material is the
# request's next HTML generation, not a 60-second stale approximation.
before_fingerprint=$(sqlite3 "$tmp/public-projections.db" \
  "SELECT source_fingerprint FROM public_projection WHERE closure_key LIKE 'public-page|aurekai.live|/discover||%';")
[ -n "$before_fingerprint" ] || { echo 'public route cache did not materialize' >&2; exit 1; }
cms_mtime=$(python3 -c 'import os,sys;print(os.stat(sys.argv[1]).st_mtime_ns)' "$cmsdb")
cp "$tmp/page.json" "$tmp/page-original.json"
python3 - "$tmp/page.json" <<'PY'
import json,sys
path=sys.argv[1]
d=json.load(open(path))
for row in d["programs"]:
    if row["route"]=="/discover":
        row["title"]="Compiled route generation two"
with open(path,"w") as f: json.dump(d,f)
PY
bun "$compiler" --fabric-db "$fabric" --cms-db "$cmsdb" \
  --items "$tmp/items.jsonl" --page-composition "$tmp/page.json" \
  --surface-programs "$surface" --manifest "$tmp/manifest.json" > "$tmp/route-changed-receipt.json"
bun -e '
const r=await Bun.file(process.argv[1]).json();
if(r.owner_operations!==1) throw Error("changed subject route did not compile");
' "$tmp/route-changed-receipt.json"
curl -fsS -H 'Host: aurekai.live' -H 'Accept: text/html' \
  "http://127.0.0.1:$port/discover" > "$tmp/changed-route.html"
grep -q 'Compiled route generation two' "$tmp/changed-route.html"
! grep -q 'Compiled SourceCollection Discover' "$tmp/changed-route.html"
changed_fingerprint=$(sqlite3 "$tmp/public-projections.db" \
  "SELECT source_fingerprint FROM public_projection WHERE closure_key LIKE 'public-page|aurekai.live|/discover||%';")
[ -n "$changed_fingerprint" ] && [ "$changed_fingerprint" != "$before_fingerprint" ] || {
  echo 'subject projection route revision left cached public HTML current' >&2; exit 1;
}
[ "$(python3 -c 'import os,sys;print(os.stat(sys.argv[1]).st_mtime_ns)' "$cmsdb")" = "$cms_mtime" ] || {
  echo 'test accidentally changed CMS rather than just route projection' >&2; exit 1;
}
cp "$tmp/page-original.json" "$tmp/page.json"
bun "$compiler" --fabric-db "$fabric" --cms-db "$cmsdb" \
  --items "$tmp/items.jsonl" --page-composition "$tmp/page.json" \
  --surface-programs "$surface" --manifest "$tmp/manifest.json" >/dev/null
curl -fsS -H 'Host: aurekai.live' -H 'Accept: text/html' \
  "http://127.0.0.1:$port/discover" > "$tmp/restored-route.html"
grep -q 'Compiled SourceCollection Discover' "$tmp/restored-route.html"
! grep -q 'Compiled route generation two' "$tmp/restored-route.html"
# A material-only revision with the same source and item identity also changes
# the route generation. This guards against treating membership as content.
content_fingerprint=$(sqlite3 "$tmp/public-projections.db" \
  "SELECT source_fingerprint FROM public_projection WHERE closure_key LIKE 'public-page|aurekai.live|/discover||%';")
cp "$tmp/items.jsonl" "$tmp/items-original.jsonl"
python3 - "$tmp/items.jsonl" <<'PY'
import json,sys
path=sys.argv[1]
rows=[json.loads(line) for line in open(path)]
for row in rows:
    if row["source"]=="included":
        row["title"]="UPDATED RETAINED ITEM SAME ID"
with open(path,"w") as f:
    for row in rows: f.write(json.dumps(row)+"\n")
PY
bun "$compiler" --fabric-db "$fabric" --cms-db "$cmsdb" \
  --items "$tmp/items.jsonl" --page-composition "$tmp/page.json" \
  --surface-programs "$surface" --manifest "$tmp/manifest.json" >/dev/null
curl -fsS -H 'Host: aurekai.live' -H 'Accept: text/html' \
  "http://127.0.0.1:$port/discover" > "$tmp/content-changed.html"
grep -q 'UPDATED RETAINED ITEM SAME ID' "$tmp/content-changed.html"
! grep -q 'COMPILED EXACT SOURCE ITEM' "$tmp/content-changed.html"
[ "$(sqlite3 "$tmp/public-projections.db" \
  "SELECT source_fingerprint FROM public_projection WHERE closure_key LIKE 'public-page|aurekai.live|/discover||%';")" != "$content_fingerprint" ]
cp "$tmp/items-original.jsonl" "$tmp/items.jsonl"
bun "$compiler" --fabric-db "$fabric" --cms-db "$cmsdb" \
  --items "$tmp/items.jsonl" --page-composition "$tmp/page.json" \
  --surface-programs "$surface" --manifest "$tmp/manifest.json" >/dev/null
curl -fsS -H 'Host: aurekai.live' -H 'Accept: text/html' \
  "http://127.0.0.1:$port/discover" > "$tmp/content-restored.html"
grep -q 'COMPILED EXACT SOURCE ITEM' "$tmp/content-restored.html"
! grep -q 'UPDATED RETAINED ITEM SAME ID' "$tmp/content-restored.html"


feed_code=$(curl -sS -o "$tmp/feed.atom" -w '%{http_code}' \
  -H 'Host: aurekai.live' "http://127.0.0.1:$port/feed.atom")
[ "$feed_code" = 200 ]
python3 - "$tmp/feed.atom" "$included_key" <<'PY'
import sys,xml.etree.ElementTree as ET
xml=open(sys.argv[1]).read(); key=sys.argv[2]
root=ET.fromstring(xml)
entries=root.findall("{http://www.w3.org/2005/Atom}entry")
assert len(entries)==1,xml
assert "COMPILED EXACT SOURCE ITEM" in xml
assert "SAME FAMILY MUST NOT LEAK" not in xml
assert "https://aurekai.live/o/included/"+key in xml
PY
feed_tag=$(curl -sS -D - -o /dev/null -H 'Host: aurekai.live' \
  "http://127.0.0.1:$port/feed.atom" | tr -d '\r' | sed -n 's/^ETag: //p')
[ -n "$feed_tag" ]
[ "$(curl -sS -o /dev/null -w '%{http_code}' -H 'Host: aurekai.live' \
  -H "If-None-Match: $feed_tag" "http://127.0.0.1:$port/feed.atom")" = 304 ]
other_feed=$(curl -sS -H 'Host: aurekai.life' "http://127.0.0.1:$port/feed.atom")
case "$other_feed" in *COMPILED\ EXACT\ SOURCE\ ITEM*) echo 'compiled feed crossed territory' >&2; exit 1;; esac
item_code=$(curl -sS -o "$tmp/item.html" -w '%{http_code}' \
  -H 'Host: aurekai.live' -H 'Accept: text/html' \
  "http://127.0.0.1:$port/o/included/$included_key")
[ "$item_code" = 200 ]
python3 - "$tmp/item.html" <<'PY'
import sys
html=open(sys.argv[1]).read()
assert "COMPILED EXACT SOURCE ITEM" in html
assert "The retained public reading for this item." in html
assert "Its current version may differ." in html
PY
# A public item can have several material records. An unselected material
# must not enter the detail page; an explicitly selected representation can.
python3 - "$fabric" "$included_key" <<'PY'
import sqlite3,sys
db=sqlite3.connect(sys.argv[1]); key=sys.argv[2]
columns=[row[1] for row in db.execute("PRAGMA table_info(subject_material_body)")]
base=db.execute("SELECT * FROM subject_material_body WHERE source='included' AND item_key=? AND material='publication'",(key,)).fetchone()
assert base
values=dict(zip(columns,base));values.update(material='photo',image='https://images.example.test/compiled-photo.jpg')
db.execute("INSERT INTO subject_material_body("+','.join(columns)+") VALUES("+','.join('?' for _ in columns)+")",[values[c] for c in columns])
db.execute("INSERT INTO subject_material_member VALUES('institution:aurekai:live','public','included',?,'photo')",(key,));db.commit();db.close()
PY
curl -fsS -H 'Host: aurekai.live' -H 'Accept: text/html' "http://127.0.0.1:$port/o/included/$included_key" > "$tmp/unselected-media.html"
! grep -q 'compiled-photo.jpg' "$tmp/unselected-media.html"
sqlite3 "$fabric" "INSERT INTO subject_route_item VALUES('institution:aurekai:live','public','/discover',1,'included','$included_key','photo');"
curl -fsS -H 'Host: aurekai.live' -H 'Accept: text/html' "http://127.0.0.1:$port/o/included/$included_key" > "$tmp/selected-photo.html"
grep -q 'https://images.example.test/compiled-photo.jpg' "$tmp/selected-photo.html"
python3 - "$fabric" "$included_key" <<'PY'
import sqlite3,sys
db=sqlite3.connect(sys.argv[1]); key=sys.argv[2]
columns=[row[1] for row in db.execute("PRAGMA table_info(subject_material_body)")]
base=db.execute("SELECT * FROM subject_material_body WHERE source='included' AND item_key=? AND material='publication'",(key,)).fetchone()
values=dict(zip(columns,base));values.update(material='video',asset='https://media.example.test/compiled-video.mp4')
db.execute("INSERT INTO subject_material_body("+','.join(columns)+") VALUES("+','.join('?' for _ in columns)+")",[values[c] for c in columns])
db.execute("INSERT INTO subject_material_member VALUES('institution:aurekai:live','public','included',?,'video')",(key,))
db.execute("INSERT INTO subject_route_item VALUES('institution:aurekai:live','public','/discover',2,'included',?,'video')",(key,));db.commit();db.close()
PY
# Exercise the compiled Watch cut, including a thumbnail-only video row.
python3 - "$fabric" "$included_key" <<'PY'
import sqlite3,sys
db=sqlite3.connect(sys.argv[1]); key=sys.argv[2]
columns=[row[1] for row in db.execute("PRAGMA table_info(subject_route)")]
row=db.execute("SELECT * FROM subject_route WHERE subject='institution:aurekai:live' AND route='/discover'").fetchone()
values=dict(zip(columns,row));values['route']='/watch'
db.execute("INSERT INTO subject_route("+','.join(columns)+") VALUES("+','.join('?' for _ in columns)+")",[values[c] for c in columns])
db.execute("INSERT INTO subject_route_region(subject,scope,route,ordinal,role,material,topology,language,min_items,max_items) VALUES('institution:aurekai:live','public','/watch',0,'grid','video','grid','',1,40)")
db.execute("INSERT INTO subject_route_item VALUES('institution:aurekai:live','public','/watch',0,'included',?,'video')",(key,))
columns=[row[1] for row in db.execute("PRAGMA table_info(subject_material_body)")]
row=db.execute("SELECT * FROM subject_material_body WHERE source='included' AND item_key=? AND material='publication'",(key,)).fetchone()
values=dict(zip(columns,row));values.update(item_key='aaaaaaaaaaaaaaaaaaaaaaaa',material='video',title='Schedule without playable media',href='https://included.example/schedule',image='https://images.example.test/schedule.jpg',asset='')
db.execute("INSERT INTO subject_material_body("+','.join(columns)+") VALUES("+','.join('?' for _ in columns)+")",[values[c] for c in columns])
db.execute("INSERT INTO subject_material_member VALUES('institution:aurekai:live','public','included','aaaaaaaaaaaaaaaaaaaaaaaa','video')")
db.execute("INSERT INTO subject_route_item VALUES('institution:aurekai:live','public','/watch',1,'included','aaaaaaaaaaaaaaaaaaaaaaaa','video')")
values.update(item_key='bbbbbbbbbbbbbbbbbbbbbbbb',material='publication',title='Compiled schedule publication without video',image='',asset='')
db.execute("INSERT INTO subject_material_body("+','.join(columns)+") VALUES("+','.join('?' for _ in columns)+")",[values[c] for c in columns])
db.execute("INSERT INTO subject_material_member VALUES('institution:aurekai:live','public','included','bbbbbbbbbbbbbbbbbbbbbbbb','publication')")
db.execute("INSERT INTO subject_route_item VALUES('institution:aurekai:live','public','/watch',2,'included','bbbbbbbbbbbbbbbbbbbbbbbb','publication')")
db.commit();db.close()
PY
curl -fsS -H 'Host: aurekai.live' -H 'Accept: text/html' "http://127.0.0.1:$port/watch" > "$tmp/compiled-watch.html"
grep -q 'COMPILED EXACT SOURCE ITEM' "$tmp/compiled-watch.html"
! grep -q 'Schedule without playable media' "$tmp/compiled-watch.html"
! grep -q 'Compiled schedule publication without video' "$tmp/compiled-watch.html"
curl -fsS -H 'Host: aurekai.live' -H 'Accept: text/html' "http://127.0.0.1:$port/o/included/$included_key" > "$tmp/selected-video.html"
grep -q '<video controls' "$tmp/selected-video.html"
grep -q 'https://media.example.test/compiled-video.mp4' "$tmp/selected-video.html"
# The compiled Boards cut keeps pictures and excludes non-picture material.
python3 - "$fabric" "$included_key" <<'PY'
import sqlite3,sys
db=sqlite3.connect(sys.argv[1]); key=sys.argv[2]
columns=[row[1] for row in db.execute("PRAGMA table_info(subject_route)")]
row=db.execute("SELECT * FROM subject_route WHERE subject='institution:aurekai:live' AND route='/discover'").fetchone()
values=dict(zip(columns,row));values['route']='/board'
db.execute("INSERT INTO subject_route("+','.join(columns)+") VALUES("+','.join('?' for _ in columns)+")",[values[c] for c in columns])
db.execute("INSERT INTO subject_route_region(subject,scope,route,ordinal,role,material,topology,language,min_items,max_items) VALUES('institution:aurekai:live','public','/board',0,'wall','photo','wall','',1,40)")
db.execute("INSERT INTO subject_route_item VALUES('institution:aurekai:live','public','/board',0,'included',?,'photo')",(key,))
db.execute("INSERT INTO subject_route_item VALUES('institution:aurekai:live','public','/board',1,'included','bbbbbbbbbbbbbbbbbbbbbbbb','publication')")
db.commit();db.close()
PY
curl -fsS -H 'Host: aurekai.live' -H 'Accept: text/html' "http://127.0.0.1:$port/board" > "$tmp/compiled-board.html"
grep -q 'compiled-photo.jpg' "$tmp/compiled-board.html"
! grep -q 'Compiled schedule publication without video' "$tmp/compiled-board.html"
sqlite3 "$fabric" "DELETE FROM subject_route_item WHERE route='/board'; DELETE FROM subject_route_region WHERE route='/board'; DELETE FROM subject_route WHERE route='/board';"
sqlite3 "$fabric" "DELETE FROM subject_route_item WHERE route='/watch'; DELETE FROM subject_route_region WHERE route='/watch'; DELETE FROM subject_route WHERE route='/watch'; DELETE FROM subject_material_member WHERE source='included' AND item_key IN ('aaaaaaaaaaaaaaaaaaaaaaaa','bbbbbbbbbbbbbbbbbbbbbbbb'); DELETE FROM subject_material_body WHERE source='included' AND item_key IN ('aaaaaaaaaaaaaaaaaaaaaaaa','bbbbbbbbbbbbbbbbbbbbbbbb'); DELETE FROM subject_route_item WHERE source='included' AND item_key='$included_key' AND material IN ('photo','video'); DELETE FROM subject_material_member WHERE source='included' AND item_key='$included_key' AND material IN ('photo','video'); DELETE FROM subject_material_body WHERE source='included' AND item_key='$included_key' AND material IN ('photo','video');"

excluded_code=$(curl -sS -o /dev/null -w '%{http_code}' \
  -H 'Host: aurekai.live' -H 'Accept: text/html' \
  "http://127.0.0.1:$port/o/excluded/$excluded_key")
[ "$excluded_code" = 404 ]
other_host_code=$(curl -sS -o /dev/null -w '%{http_code}' \
  -H 'Host: aurekai.life' -H 'Accept: text/html' \
  "http://127.0.0.1:$port/o/included/$included_key")
[ "$other_host_code" = 404 ]

python3 - "$port" "$cmsdb" "$included_key" "$excluded_key" <<'PY'
import json,sqlite3,subprocess,sys
port,cmsdb,included_key,excluded_key=sys.argv[1:]
base=f"http://127.0.0.1:{port}"
included="included/"+included_key
excluded="excluded/"+excluded_key
def request(path,method="GET",host="aurekai.live",token=None,data=None,origin=None):
    cmd=["curl","-sS","-i","-X",method,"-H",f"Host: {host}"]
    if token: cmd += ["-H",f"Cookie: bonfyre_session={token}"]
    if origin: cmd += ["-H",f"Origin: http://{origin}"]
    if path in ("/signup","/login"): cmd += ["-H","Accept: application/json","-H","Content-Type: application/json"]
    if data is not None: cmd += ["--data",data]
    raw=subprocess.run(cmd+[base+path],capture_output=True,check=True).stdout.decode()
    head,_,body=raw.partition("\r\n\r\n")
    return int(head.split(" ")[1]),head,body
def login(email):
    password="a-long-enough-passphrase"
    payload=json.dumps({"email":email,"name":"Reader","password":password})
    code,_,_=request("/signup","POST",data=payload)
    assert code in (200,201),code
    code,_,body=request("/login","POST",data=json.dumps({"email":email,"password":password}))
    assert code==200,(code,body)
    return json.loads(body)["session"]["token"]
reader=login("compiled-reader@foundry.test")
other=login("other-reader@foundry.test")
code,_,page=request("/o/"+included,token=reader)
assert code==200 and 'action="/o/save"' in page,(code,page[:500])
code,_,_=request("/o/save","POST",token=reader,origin="aurekai.live",data=f"item={included}&return=/o/{included}")
assert code==303,code
db=sqlite3.connect(cmsdb)
rows=db.execute("SELECT actor,item_ref,title,summary,href FROM saved_item").fetchall()
assert rows==[("user:1",included,"COMPILED EXACT SOURCE ITEM","The retained public reading for this item.","https://included.example/item-one")],rows
code,_,saved=request("/saved",token=reader)
assert code==200 and "COMPILED EXACT SOURCE ITEM" in saved,(code,saved[:500])
code,_,other_saved=request("/saved",token=other)
assert code==200 and "COMPILED EXACT SOURCE ITEM" not in other_saved,code
for item,host in ((excluded,"aurekai.live"),(included,"aurekai.life")):
    code,_,_=request("/o/save","POST",host=host,token=reader,origin=host,data=f"item={item}&return=/saved")
    assert code==404,(item,host,code)
assert db.execute("SELECT COUNT(*) FROM saved_item").fetchone()[0]==1
code,_,_=request("/o/save","POST",token=reader,origin="evil.test",data=f"item={included}&return=/saved")
assert code==403,code
db.close()
PY

python3 - "$tmp/items.jsonl" <<'PY'
import json,sys
path=sys.argv[1]
rows=[json.loads(line) for line in open(path)]
with open(path,"w") as out:
    for row in rows:
        if row["source"]!="included": out.write(json.dumps(row)+"\n")
PY
bun "$compiler" --fabric-db "$fabric" --cms-db "$cmsdb" \
  --items "$tmp/items.jsonl" --page-composition "$tmp/page.json" \
  --surface-programs "$surface" --manifest "$tmp/manifest.json" >/dev/null
bun "$verify" --fabric-db "$fabric" --cms-db "$cmsdb" \
  --items "$tmp/items.jsonl" --page-composition "$tmp/page.json" \
  --surface-programs "$surface" --manifest "$tmp/manifest.json" >/dev/null
python3 - "$port" "$cmsdb" "$included_key" <<'PY'
import json,sqlite3,subprocess,sys
port,cmsdb,key=sys.argv[1:]
base=f"http://127.0.0.1:{port}"
ref="included/"+key
def call(path,method="GET",email=None,data=None):
    cmd=["curl","-sS","-i","-X",method,"-H","Host: aurekai.live"]
    if email:
        password="a-long-enough-passphrase"
        login=subprocess.run(["curl","-sS","-X","POST","-H","Host: aurekai.live",
                              "-H","Content-Type: application/json","--data",
                              json.dumps({"email":email,"password":password}),base+"/login"],
                             capture_output=True,check=True).stdout
        token=json.loads(login)["session"]["token"]
        cmd += ["-H",f"Cookie: bonfyre_session={token}"]
    if data is not None: cmd += ["-H","Origin: http://aurekai.live","--data",data]
    raw=subprocess.run(cmd+[base+path],capture_output=True,check=True).stdout.decode()
    head,_,body=raw.partition("\r\n\r\n")
    return int(head.split(" ")[1]),body
code,body=call("/feed.atom")
assert code==200 and "COMPILED EXACT SOURCE ITEM" not in body,(code,body)
code,_=call("/o/"+ref)
assert code==404,code
code,body=call("/o/"+ref,email="compiled-reader@foundry.test")
assert code==200 and "You saved this" in body and "COMPILED EXACT SOURCE ITEM" in body,(code,body[:500])
code,_=call("/o/"+ref,email="other-reader@foundry.test")
assert code==404,code
code,_=call("/o/save","POST",email="other-reader@foundry.test",data=f"item={ref}&return=/saved")
assert code==404,code
db=sqlite3.connect(cmsdb)
assert db.execute("SELECT COUNT(*) FROM saved_item").fetchone()==(1,)
code,_=call("/o/unsave","POST",email="compiled-reader@foundry.test",data=f"item={ref}&return=/saved")
assert code==303,code
assert db.execute("SELECT COUNT(*) FROM saved_item").fetchone()==(0,)
code,_=call("/o/"+ref,email="compiled-reader@foundry.test")
assert code==404,code
db.close()
PY

echo 'compiled subject route web: PASS'
