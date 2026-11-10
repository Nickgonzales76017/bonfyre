#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
tool="$root/cmd/BonfyreExternalEstate/tools/compile_subject_projection.mjs"
verify="$root/cmd/BonfyreExternalEstate/tools/verify_subject_projection.mjs"
legacy_verify="$root/cmd/BonfyreExternalEstate/tools/verify_subject_projection.py.archive"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

fabric="$tmp/fabric.db"
cms="$tmp/cms.db"
items="$tmp/items.jsonl"
page="$tmp/page.json"
surface="$tmp/surface.json"

python3 - "$fabric" "$cms" <<'PY'
import json,sqlite3,sys
fabric,cms=sys.argv[1:3]
db=sqlite3.connect(fabric)
db.executescript("""
CREATE TABLE subject_bundle(subject TEXT,scope TEXT,bundle_digest TEXT);
CREATE TABLE subject_reader(
 subject TEXT,scope TEXT,census_source TEXT,root_id TEXT,channel_id TEXT,material TEXT,
 protocol TEXT,state TEXT,last_contributed TEXT,contributions INTEGER,first_seen TEXT);
CREATE TABLE subject_scene(scene_key TEXT,subject TEXT,scope TEXT);
CREATE TABLE subject_query(scene_key TEXT,ordinal INTEGER,query_json TEXT);
INSERT INTO subject_bundle VALUES('institution:aurekai:live','public','d1');
INSERT INTO subject_reader VALUES('institution:aurekai:live','public','s1','r1','c1','publication','RSS','admitted','',0,'');
INSERT INTO subject_reader VALUES('institution:aurekai:live','public','s2','r2','c2','publication','RSS','admitted','',0,'');
INSERT INTO subject_reader VALUES('institution:aurekai:live','public','s3','r3','c3','publication','JSON_API','admitted','',0,'');
INSERT INTO subject_scene VALUES('scene-live','institution:aurekai:live','public');
""")
db.execute("INSERT INTO subject_query VALUES(?,?,?)",( "scene-live",0,json.dumps({
 "source":"application_binding","id":"b1","application_kind":"discover",
 "source_collection":"sc-discover","material_query":{"kind":"publication"}
})))
db.execute("INSERT INTO subject_query VALUES(?,?,?)",( "scene-live",1,json.dumps({
 "source":"application_binding","id":"b2","application_kind":"spatial",
 "source_collection":"sc-spatial","material_query":{"kind":"place"}
})))
db.commit();db.close()

db=sqlite3.connect(cms)
db.executescript("""
CREATE TABLE source_collection(
 id INTEGER PRIMARY KEY, collection_id TEXT,subject_address TEXT,
 source_selector TEXT,material_kind TEXT,refresh_policy TEXT,status TEXT);
""")
db.execute("INSERT INTO source_collection VALUES(1,?,?,?,?,?,?)",(
 "sc-discover","institution:aurekai:live",
 json.dumps({"domains":["discover"],"source_refs":["s1"]}),
 json.dumps(["PublicationOccurrence"]),"daily","published"))
db.execute("INSERT INTO source_collection VALUES(2,?,?,?,?,?,?)",(
 "sc-spatial","institution:aurekai:live",
 json.dumps({"domains":["spatial"],"source_refs":["s1"]}),
 json.dumps(["PublicationOccurrence"]),"daily","published"))
db.commit();db.close()
PY

cat >"$items" <<'JSONL'
{"source":"s1","family":"news","key":"one","kind":"story","title":"Exact source item","by":"A","publisher":"One","href":"https://one.test/a","image":"","asset":"","body":"body one","ident":"i1","about":"","rights":"","where":"Dallas","at":100,"placed":true,"lon":-96.8,"lat":32.8}
{"source":"s2","family":"news","key":"two","kind":"story","title":"SAME FAMILY MUST NOT LEAK","by":"B","publisher":"Two","href":"https://two.test/b","image":"","asset":"","body":"body two","ident":"i2","about":"","rights":"","where":"","at":200,"placed":false}
{"source":"s3","family":"packages","key":"three","kind":"story","title":"Compatibility package","by":"C","publisher":"Three","href":"https://three.test/c","image":"","asset":"","body":"body three","ident":"i3","about":"","rights":"","where":"","at":300,"placed":false}
JSONL

cat >"$page" <<'JSON'
{
  "regions":[
    {"role":"feed","material":"mix","topology":"feed","language":"stream","min_items":1,"max_items":50},
    {"role":"table","material":"publication","topology":"table","language":"rows","min_items":1,"max_items":50},
    {"role":"places","material":"place","topology":"map","language":"map","min_items":1,"max_items":50}
  ],
  "programs":[
    {"route":"/discover","families":["news"],"source_limit":48,"regions":["feed"]},
    {"route":"/releases","families":["packages"],"source_limit":20,"regions":["table"]},
    {"route":"/parcels","families":["hazards"],"source_limit":16,"regions":["places"]}
  ]
}
JSON

cat >"$surface" <<'JSON'
{"programs":[
 {"program":"discover","route":"/discover","renderer":"pixelfed_stream"},
 {"program":"spatial","route":"/parcels","renderer":"maplibre_spatial"}
]}
JSON

bun "$tool" --fabric-db "$fabric" --cms-db "$cms" --items "$items" \
  --page-composition "$page" --surface-programs "$surface" --manifest "$tmp/manifest.json" >/dev/null

python3 - "$fabric" "$tmp/manifest.json" <<'PY'
import json,sqlite3,sys
db=sqlite3.connect(sys.argv[1])
manifest=json.load(open(sys.argv[2]))

discover=db.execute(
 "select basis,reader_count,item_count from subject_route where subject=? and route='/discover'",
 ("institution:aurekai:live",)).fetchone()
assert discover==("scene_source_collection_exact",1,1),discover
assert db.execute(
 "select count(*) from subject_route_item r join subject_material m on "
 "m.subject=r.subject and m.scope=r.scope and m.source=r.source and m.item_key=r.item_key and m.material=r.material "
 "where r.route='/discover' and m.title='Exact source item'"
).fetchone()[0]==1
assert db.execute(
 "select count(*) from subject_route_item r join subject_material m on "
 "m.subject=r.subject and m.scope=r.scope and m.source=r.source and m.item_key=r.item_key and m.material=r.material "
 "where r.route='/discover' and m.title='SAME FAMILY MUST NOT LEAK'"
).fetchone()[0]==0

releases=db.execute(
 "select basis,reader_count,item_count from subject_route where subject=? and route='/releases'",
 ("institution:aurekai:live",)).fetchone()
assert releases==("legacy_page_program_family_precompiled",1,1),releases
assert db.execute(
 "select m.title from subject_route_item r join subject_material m on "
 "m.subject=r.subject and m.scope=r.scope and m.source=r.source and m.item_key=r.item_key and m.material=r.material "
 "where r.route='/releases'"
).fetchone()[0]=="Compatibility package"

parcels=db.execute(
 "select basis,reader_count,item_count from subject_route where subject=? and route='/parcels'",
 ("institution:aurekai:live",)).fetchone()
assert parcels==("scene_source_collection_exact",1,1),parcels
place=db.execute(
 "select m.material,m.title,m.placed,m.lon,m.lat from subject_route_item r join subject_material m on "
 "m.subject=r.subject and m.scope=r.scope and m.source=r.source and m.item_key=r.item_key and m.material=r.material "
 "where r.route='/parcels'"
).fetchone()
assert place[0:3]==("place","Exact source item",1),place
assert abs(place[3]+96.8)<0.0001 and abs(place[4]-32.8)<0.0001,place

assert db.execute("select count(*) from subject_material").fetchone()[0]==4
assert manifest["basis_counts"]["scene_source_collection_exact"]==2
assert manifest["basis_counts"]["legacy_page_program_family_precompiled"]==1
assert manifest["schema"]=="bonfyre.subject-projection-precompute.v4"
assert len(manifest["compiler_sha256"])==64
assert len(manifest["input_digest"])==64
assert len(manifest["projection_digest"])==64
assert manifest["input_snapshot"]["fabric"]["subject_bundle"]==[["institution:aurekai:live","public","d1"]]
assert manifest["input_snapshot"]["published_source_collections"][0][0]=="sc-discover"
build=db.execute("SELECT input_digest,projection_digest,manifest_json FROM subject_projection_build WHERE build_id=1").fetchone()
assert build[0:2]==(manifest["input_digest"],manifest["projection_digest"])
assert json.loads(build[2])==manifest
assert manifest["projection_tables"]["subject_route"]["rows"]==3
db.close()
PY
bun "$verify" --fabric-db "$fabric" --cms-db "$cms" --items "$items" \
  --page-composition "$page" --surface-programs "$surface" --manifest "$tmp/manifest.json" >"$tmp/bun-verify.json"
# The next standing clock tick must reuse a complete, independently verified
# projection rather than drop and repopulate every material and route table.
# A changed reader's telemetry is not a changed route source membership.
python3 - "$fabric" "$tmp/manifest.json" "$tmp/stable-before.json" <<'PY'
import json,os,sqlite3,sys
db=sqlite3.connect(sys.argv[1])
row=db.execute("SELECT compiled_at,input_digest,projection_digest FROM subject_projection_build WHERE build_id=1").fetchone()
json.dump({"build":row,"mtime_ns":os.stat(sys.argv[1]).st_mtime_ns,
 "manifest":open(sys.argv[2]).read()},open(sys.argv[3],"w"))
db.close()
PY
bun "$tool" --fabric-db "$fabric" --cms-db "$cms" --items "$items" \
  --page-composition "$page" --surface-programs "$surface" --manifest "$tmp/manifest.json" >"$tmp/verified-reuse.json"
bun -e '
const r=await Bun.file(process.argv[1]).json();
if(r.owner_operations!==0 || r.skipped!=="verified_input_closure_and_committed_projection_unchanged" ||
   r.routes!==3 || r.route_items!==3) throw Error("unchanged route rebuilt: "+JSON.stringify(r));
' "$tmp/verified-reuse.json"
python3 - "$fabric" "$tmp/manifest.json" "$tmp/stable-before.json" <<'PY'
import json,os,sqlite3,sys
prior=json.load(open(sys.argv[3]))
db=sqlite3.connect(sys.argv[1])
assert db.execute("SELECT compiled_at,input_digest,projection_digest FROM subject_projection_build WHERE build_id=1").fetchone()==tuple(prior["build"])
assert os.stat(sys.argv[1]).st_mtime_ns==prior["mtime_ns"],"verified reuse wrote into the material owner DB"
assert open(sys.argv[2]).read()==prior["manifest"]
db.close()
PY
# Change irrelevant reader telemetry in the native Fabric store: projector
# dependencies exclude this field, so this is not a global DB invalidation.
python3 - "$fabric" <<'PY'
import sqlite3,sys
db=sqlite3.connect(sys.argv[1])
db.execute("UPDATE subject_reader SET last_contributed='2026-10-14' WHERE census_source='s1'")
db.commit();db.close()
PY
bun "$tool" --fabric-db "$fabric" --cms-db "$cms" --items "$items" \
  --page-composition "$page" --surface-programs "$surface" --manifest "$tmp/manifest.json" >"$tmp/telemetry-reuse.json"
bun -e '
const r=await Bun.file(process.argv[1]).json();
if(r.owner_operations!==0) throw Error("independent reader telemetry caused global route replacement");
' "$tmp/telemetry-reuse.json"

if [ "${BONFYRE_VERIFY_LEGACY:-0}" = 1 ]; then
  python3 "$legacy_verify" --fabric-db "$fabric" --cms-db "$cms" --items "$items" \
    --page-composition "$page" --surface-programs "$surface" --manifest "$tmp/manifest.json" >"$tmp/python-verify.json"
  diff -u "$tmp/python-verify.json" "$tmp/bun-verify.json"
fi

# A retained source coordinate can remain stable while its renderer-visible
# material changes.  Lineage and the compiled route must both notice that
# change; otherwise a rebuild has no honest invalidation coordinate.
cp "$tmp/manifest.json" "$tmp/first-manifest.json"
python3 - "$items" <<'PY'
import json,sys
rows=[json.loads(line) for line in open(sys.argv[1]) if line.strip()]
rows[0]["title"]="Exact source item, corrected"
with open(sys.argv[1],"w") as fh:
    for row in rows:
        fh.write(json.dumps(row,sort_keys=True)+"\n")
PY
if bun "$verify" --fabric-db "$fabric" --cms-db "$cms" --items "$items" \
  --page-composition "$page" --surface-programs "$surface" --manifest "$tmp/first-manifest.json" >/dev/null 2>&1; then
  echo "changed item frontier unexpectedly passed native verification" >&2
  exit 1
fi
bun "$tool" --fabric-db "$fabric" --cms-db "$cms" --items "$items" \
  --page-composition "$page" --surface-programs "$surface" --manifest "$tmp/second-manifest.json" >/dev/null
python3 - "$tmp/first-manifest.json" "$tmp/second-manifest.json" <<'PY'
import json,sys
first=json.load(open(sys.argv[1]))
second=json.load(open(sys.argv[2]))
assert first["input_digest"] != second["input_digest"]
assert first["projection_digest"] != second["projection_digest"]
assert first["input_snapshot"]["items_sha256"] != second["input_snapshot"]["items_sha256"]
PY

# The rendered route contract is also part of the cut, even when its item
# membership does not change.
python3 - "$page" <<'PY'
import json,sys
path=sys.argv[1]
program=json.load(open(path))
program["programs"][0]["title"]="Discover, revised"
with open(path,"w") as fh:
    json.dump(program,fh)
PY
bun "$tool" --fabric-db "$fabric" --cms-db "$cms" --items "$items" \
  --page-composition "$page" --surface-programs "$surface" --manifest "$tmp/third-manifest.json" >/dev/null
python3 - "$tmp/second-manifest.json" "$tmp/third-manifest.json" <<'PY'
import json,sys
second=json.load(open(sys.argv[1]))
third=json.load(open(sys.argv[2]))
assert second["input_digest"] != third["input_digest"]
assert second["projection_digest"] != third["projection_digest"]
PY

# A failed replacement must leave the previously published route tables
# readable. A duplicate route fails after the compiler has begun filling the
# new tables, exercising the rollback rather than merely input validation.
python3 - "$page" "$tmp/bad-page.json" <<'PY'
import json,sys
program=json.load(open(sys.argv[1]))
program["programs"].append(dict(program["programs"][0]))
with open(sys.argv[2],"w") as fh:
    json.dump(program,fh)
PY
if bun "$tool" --fabric-db "$fabric" --cms-db "$cms" --items "$items" \
  --page-composition "$tmp/bad-page.json" --surface-programs "$surface" >/dev/null 2>&1; then
  echo "duplicate route unexpectedly compiled" >&2
  exit 1
fi
python3 - "$fabric" "$tmp/third-manifest.json" <<'PY'
import sqlite3,sys
import json
db=sqlite3.connect(sys.argv[1])
route=db.execute("SELECT title,item_count FROM subject_route WHERE route='/discover'").fetchone()
assert route==("Discover, revised",1),route
assert db.execute("SELECT count(*) FROM subject_route").fetchone()[0]==3
assert db.execute("SELECT count(*) FROM subject_route_item").fetchone()[0]==3
build=db.execute("SELECT projection_digest,manifest_json FROM subject_projection_build WHERE build_id=1").fetchone()
assert build[0]==json.loads(build[1])["projection_digest"]
assert build[0]==json.load(open(sys.argv[2]))["projection_digest"]
db.close()
PY
bun "$verify" --fabric-db "$fabric" --cms-db "$cms" --items "$items" \
  --page-composition "$page" --surface-programs "$surface" --manifest "$tmp/third-manifest.json" >/dev/null
cp "$fabric" "$tmp/tampered-fabric.db"
python3 - "$tmp/tampered-fabric.db" <<'PY'
import sqlite3,sys
db=sqlite3.connect(sys.argv[1])
db.execute("UPDATE subject_material_body SET title='unwitnessed change' WHERE source='s1'")
db.commit();db.close()
PY
if bun "$verify" --fabric-db "$tmp/tampered-fabric.db" --cms-db "$cms" --items "$items" \
  --page-composition "$page" --surface-programs "$surface" --manifest "$tmp/third-manifest.json" >/dev/null 2>&1; then
  echo "changed projection rows unexpectedly passed native verification" >&2
  exit 1
fi

full="$tmp/full-fabric.db"
cp "$fabric" "$full"
bun "$tool" --fabric-db "$full" --cms-db "$cms" --items "$items" \
  --page-composition "$root/cmd/BonfyreWebAddress/generated/native-page-composition.json" \
  --surface-programs "$root/cmd/BonfyreWebAddress/generated/native-surface-programs.json" >/dev/null
python3 - "$full" <<'PY'
import sqlite3,sys
db=sqlite3.connect(sys.argv[1])
roots=db.execute("select route from subject_route where subject=? and route='/'",
                 ("institution:aurekai:live",)).fetchall()
assert roots==[("/",)], roots
db.close()
PY

# The standing estate runner must make compilation and verification one
# recorded step, so a missing or stale build is a failed automation outcome.
state="$tmp/automation"
mkdir -p "$state/work"
cp "$items" "$state/work/census-items.jsonl"
bun "$root/scripts/bonfyre_automate.mjs" --only subject-projection \
  --root "$root" --state "$state" --cms "$cms" --subject-fabric-db "$full" >/dev/null
python3 - "$state" <<'PY'
import glob,json,os,sys
runs=glob.glob(os.path.join(sys.argv[1],"runs","*.json"))
assert len(runs)==1,runs
steps=json.load(open(runs[0]))["steps"]
assert len(steps)==1 and steps[0]["step"]=="subject-projection" and steps[0]["ok"],steps
assert steps[0]["outcome"]["verified"]["verified"] is True,steps
PY
bun "$root/scripts/bonfyre_automate.mjs" --only subject-projection \
  --root "$root" --state "$state" --cms "$cms" --subject-fabric-db "$full" >/dev/null
python3 - "$state" <<'PY'
import glob,json,os,sys
runs=[json.load(open(path)) for path in glob.glob(os.path.join(sys.argv[1],"runs","*.json"))]
assert len(runs)==2,runs
reused=[run['steps'][0]['outcome'] for run in runs if run['steps'][0]['outcome'].get('reused_verified_owner_cut')]
assert len(reused)==1,reused
assert reused[0]['compiled']['owner_operations']==0,reused
assert reused[0]['verified']['verified'] is True,reused
PY

if bun "$root/scripts/bonfyre_automate.mjs" --only census-items,subject-projection \
  --root "$root" --state "$state" --cms "$cms" --subject-fabric-db "$full" \
  --bin "$tmp/missing-bin" >/dev/null; then
  echo "failed item generation unexpectedly allowed a projection rebuild" >&2
  exit 1
fi
python3 - "$state" "$full" <<'PY'
import glob,json,os,sqlite3,sys
state,fabric=sys.argv[1:]
runs=[json.load(open(path)) for path in glob.glob(os.path.join(state,"runs","*.json"))]
failed=[run for run in runs if len(run["steps"])==2]
assert len(failed)==1,failed
assert [(row["step"],row["ok"]) for row in failed[0]["steps"]]==[
    ("census-items",False),("subject-projection",False)]
assert "required current-run step did not complete" in failed[0]["steps"][1]["error"]
assert "Exact source item, corrected" in open(os.path.join(state,"work","census-items.jsonl")).read()
db=sqlite3.connect(fabric)
assert db.execute("SELECT count(*) FROM subject_projection_build").fetchone()[0]==1
db.close()
PY

# One source item can belong to several subjects without copying its full
# title/body/media payload. The compatibility view must reconstruct each
# subject membership, while a conflicting same-coordinate payload refuses.
compact="$tmp/shared-material-fabric.db"
cp "$fabric" "$compact"
python3 - "$compact" <<'PY'
import sqlite3,sys
db=sqlite3.connect(sys.argv[1])
db.execute("INSERT INTO subject_bundle VALUES('institution:aurekai:shop','public','shared-fixture')")
row=db.execute("SELECT * FROM subject_reader WHERE census_source='s1' LIMIT 1").fetchone()
assert row,row
db.execute("INSERT INTO subject_reader VALUES(%s)" % ",".join("?" for _ in row),
           ('institution:aurekai:shop',)+row[1:])
db.commit();db.close()
PY
bun "$tool" --fabric-db "$compact" --cms-db "$cms" --items "$items" \
  --page-composition "$page" --surface-programs "$surface" >/dev/null
python3 - "$compact" <<'PY'
import sqlite3,sys
db=sqlite3.connect(sys.argv[1])
assert db.execute("SELECT type FROM sqlite_master WHERE name='subject_material'").fetchone()==('view',)
members=db.execute("SELECT count(*) FROM subject_material_member").fetchone()[0]
bodies=db.execute("SELECT count(*) FROM subject_material_body").fetchone()[0]
assert members>bodies,(members,bodies)
assert db.execute("SELECT count(*) FROM subject_material").fetchone()[0]==members
assert db.execute("SELECT count(DISTINCT subject) FROM subject_material WHERE source='s1'").fetchone()[0]==2
db.close()
PY
bun "$verify" --fabric-db "$compact" --cms-db "$cms" --items "$items" \
  --page-composition "$page" --surface-programs "$surface" >/dev/null

# An existing estate still has the old physical table. Its next compile must
# replace that table atomically with the compact compatibility view.
legacy="$tmp/legacy-material-fabric.db"
cp "$compact" "$legacy"
python3 - "$legacy" <<'PY'
import sqlite3,sys
db=sqlite3.connect(sys.argv[1])
db.execute("CREATE TABLE legacy_material AS SELECT * FROM subject_material")
db.execute("DROP VIEW subject_material")
db.execute("ALTER TABLE legacy_material RENAME TO subject_material")
db.commit();db.close()
PY
bun "$tool" --fabric-db "$legacy" --cms-db "$cms" --items "$items" \
  --page-composition "$page" --surface-programs "$surface" >/dev/null
python3 - "$legacy" <<'PY'
import sqlite3,sys
db=sqlite3.connect(sys.argv[1])
assert db.execute("SELECT type FROM sqlite_master WHERE name='subject_material'").fetchone()==('view',)
assert db.execute("SELECT count(*) FROM subject_material").fetchone()==db.execute("SELECT count(*) FROM subject_material_member").fetchone()
db.close()
PY
bun "$verify" --fabric-db "$legacy" --cms-db "$cms" --items "$items" \
  --page-composition "$page" --surface-programs "$surface" >/dev/null
python3 - "$items" "$tmp/conflicting-items.jsonl" <<'PY'
import json,sys
rows=[json.loads(line) for line in open(sys.argv[1])]
conflict=dict(rows[0]); conflict['title']='A different payload at the same coordinate'
with open(sys.argv[2],'w') as out:
    for row in rows+[conflict]: out.write(json.dumps(row)+'\n')
PY
if bun "$tool" --fabric-db "$compact" --cms-db "$cms" --items "$tmp/conflicting-items.jsonl" \
  --page-composition "$page" --surface-programs "$surface" >/dev/null 2>&1; then
  echo "conflicting normalized item unexpectedly compiled" >&2
  exit 1
fi
bun "$verify" --fabric-db "$compact" --cms-db "$cms" --items "$items" \
  --page-composition "$page" --surface-programs "$surface" >/dev/null

echo "subject_projection_precompute_test: PASS"
