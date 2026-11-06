#!/bin/sh
# What this does not prove:
# - some checks run in embedded Python, which this estate has retired; they say nothing about a native path.
# Precomputed subject fabric keeps the dense closure, not merely the visible page slice.
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
tool="$root/cmd/BonfyreExternalEstate/tools/precompute_subject_fabric.mjs"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

cms="$tmp/cms.db"
frontier="$tmp/frontier.db"
scoped="$tmp/scoped.db"
out="$tmp/subject-fabric.db"
fake="$tmp/scoped-demand"

python3 - "$cms" "$frontier" "$scoped" <<'PY'
import json, sqlite3, sys
cms, frontier, scoped = sys.argv[1:4]

db=sqlite3.connect(cms)
db.executescript("""
CREATE TABLE site_presentation(subject_address TEXT,site_id TEXT,status TEXT);
CREATE TABLE surface_scene(
 scene_key TEXT,subject_address TEXT,world TEXT,site_id TEXT,actor_scope TEXT,
 source_digest TEXT,focus TEXT,lenses TEXT,world_material TEXT,fragments TEXT,
 slots TEXT,bindings TEXT,stages TEXT,renderers TEXT,patterns TEXT,presentation TEXT,
 operations TEXT,interaction TEXT,runtime_queries TEXT,dependencies TEXT,status TEXT);
INSERT INTO site_presentation VALUES('world:test','site:test','published');
""")
scene={
 "scene_key":"site:site:test|world:|subject:world:test|scope:public",
 "subject_address":"world:test","world":"","site_id":"site:test","actor_scope":"public",
 "source_digest":"scene-digest","focus":{"subject_address":"world:test"},
 "lenses":["discover","watch"],"world_material":{},
 "fragments":[{"fragment_id":"f1","surface_grammar":"discover","renderer_family":"river","action_aperture":"Save,Share"}],
 "slots":[],
 "bindings":[{"binding_id":"b1","application_kind":"map","material_query":{"kind":"place"},"source_collection":"local","action_aperture":"Route"}],
 "stages":[{"stage_id":"s1","application_kind":"event","material_queries":[{"kind":"event"},{"kind":"offer"}],"renderer":"agenda","actions":["RSVP"]}],
 "renderers":[{"renderer":"river"},{"renderer":"agenda"}],
 "patterns":[{"pattern":"nearby","reads_organ":"Place","reads_type":"Place","organ_command":"near","effect_class":"QUERY"}],
 "presentation":[],"operations":{"apertures":["Save"]},"interaction":{},
 "runtime_queries":{},"dependencies":["cms:world:test","ForeignTwin:https://example.org/feed"],
}
db.execute("INSERT INTO surface_scene VALUES("+",".join("?" for _ in range(21))+")",(
 scene["scene_key"],scene["subject_address"],scene["world"],scene["site_id"],scene["actor_scope"],
 scene["source_digest"],json.dumps(scene["focus"]),json.dumps(scene["lenses"]),json.dumps(scene["world_material"]),
 json.dumps(scene["fragments"]),json.dumps(scene["slots"]),json.dumps(scene["bindings"]),json.dumps(scene["stages"]),
 json.dumps(scene["renderers"]),json.dumps(scene["patterns"]),json.dumps(scene["presentation"]),
 json.dumps(scene["operations"]),json.dumps(scene["interaction"]),json.dumps(scene["runtime_queries"]),
 json.dumps(scene["dependencies"]),"published"))
db.commit();db.close()

db=sqlite3.connect(frontier)
db.executescript("""
CREATE TABLE root(root_id TEXT PRIMARY KEY,host TEXT,name TEXT,identity TEXT);
CREATE TABLE channel(channel_id TEXT PRIMARY KEY,family_id TEXT,root_id TEXT,address TEXT,exact_address TEXT,title TEXT,material TEXT,protocol TEXT,state TEXT,truth_level TEXT,refusal TEXT,virtual INTEGER,census_source TEXT,credential TEXT,rights TEXT,cadence TEXT,discovered_from TEXT,body_sha256 TEXT,identity TEXT,first_seen TEXT,last_seen TEXT,last_contributed TEXT,contributions INTEGER);
CREATE TABLE channel_census(channel_id TEXT,census_source TEXT,material TEXT);
INSERT INTO root VALUES('r1','example.org','Example','publisher:example');
INSERT INTO root VALUES('r2','events.example.net','Events','publisher:events');
INSERT INTO channel VALUES('c1','f1','r1','https://example.org/feed','https://example.org/feed','','publication','RSS','admitted','','',0,'news','','declared','','','','ext-1','','','',0);
INSERT INTO channel VALUES('c2','f2','r2','https://events.example.net/events.ics','https://events.example.net/events.ics','','event','ICS','admitted','','',0,'calendar','','declared','','','','ext-2','','','',0);
INSERT INTO channel_census VALUES('c1','news','publication');
INSERT INTO channel_census VALUES('c1','news-alt','publication');
INSERT INTO channel_census VALUES('c2','calendar','event');
""")
db.commit();db.close()

db=sqlite3.connect(scoped)
db.executescript("""
CREATE TABLE closure_dependency(subject TEXT,scope TEXT,dependency_ref TEXT,basis TEXT);
INSERT INTO closure_dependency VALUES('world:test','public','ForeignTwin:https://example.org/feed','foreign_twin');
INSERT INTO closure_dependency VALUES('world:test','public','cms:world:test','cms');
""")
db.commit();db.close()
PY

cat >"$fake" <<'SH'
#!/bin/sh
cat <<'JSON'
{"schema":"bonfyre-scoped-demand-closure.v3","subject":"world:test","scope":"public","active_packs":[{"pack":"night","activation":"a1"}],"pack_demands":[{"identity":"d1","pack":"night","demand_kind":"news","requirement":"local","regime":"REMOTE","candidate_groups":[{"source":"news","candidates":[{"identity":"ext-1","owner_ref":"ExternalEstate"}]}],"state":"candidate_cut"}],"world_demands":[{"identity":"w1","dependencies":[{"dependency":"cms:internet_source:calendar","candidates":[{"identity":"ext-2","owner_ref":"ExternalEstate"}]}],"state":"candidate_cut"}],"counts":{"candidate_survivors":2},"truth":"scoped_candidate_cut_exact_owner_recheck_required"}
JSON
SH
chmod +x "$fake"

first=$(bun "$tool" --cms-db "$cms" --demand-db "$cms" --frontier-db "$frontier" \
  --scoped-demand-bin "$fake" --scene-db "$cms" --scoped-frontier-db "$scoped" --out "$out")

python3 - "$out" <<'PY'
import json,sqlite3,sys
db=sqlite3.connect(sys.argv[1])
assert db.execute("select count(*) from subject_bundle").fetchone()[0]==1
counts=json.loads(db.execute("select counts_json from subject_bundle").fetchone()[0])
assert counts["candidate_occurrences"]==2, counts
assert counts["mapped_candidate_occurrences"]==2, counts
assert counts["source_roots"]==2, counts
assert counts["channels"]==2, counts
assert counts["readers"]==3, counts
assert counts["scenes"]==1, counts
assert counts["lenses"]==2, counts
assert counts["applications"]==2, counts
assert counts["renderers"]==2, counts
assert counts["material_queries"]==4, counts
assert counts["actions"]==4, counts
assert db.execute("select count(*) from subject_candidate").fetchone()[0]==2
assert db.execute("select count(*) from subject_root").fetchone()[0]==2
assert db.execute("select count(*) from subject_reader").fetchone()[0]==3
assert {r[0] for r in db.execute("select census_source from subject_reader")}=={"news","news-alt","calendar"}
assert db.execute("select count(*) from subject_query").fetchone()[0]==4
assert db.execute("select count(*) from subject_dependency").fetchone()[0]==4
assert {r[0] for r in db.execute("select action from subject_action")}=={"Save","Share","Route","RSVP"}
db.close()
PY

sqlite3 "$out" "DELETE FROM subject_action"
repair=$(bun "$tool" --repair-actions-from-scenes "$out")
printf '%s\n' "$repair" | grep -q '"schema":"bonfyre.subject-action-repair.v1"'
printf '%s\n' "$repair" | grep -q '"actions":4'
python3 - "$out" <<'PY'
import sqlite3,sys
db=sqlite3.connect(sys.argv[1])
assert {r[0] for r in db.execute("select action from subject_action")}=={"Save","Share","Route","RSVP"}
db.close()
PY

second=$(bun "$tool" --cms-db "$cms" --demand-db "$cms" --frontier-db "$frontier" \
  --scoped-demand-bin "$fake" --scene-db "$cms" --scoped-frontier-db "$scoped" --out "$out")

printf '%s\n' "$first" | grep -Eq '"bundles_changed":[[:space:]]*1'
printf '%s\n' "$second" | grep -Eq '"bundles_unchanged":[[:space:]]*1'

echo "subject_fabric_precompute_test: PASS"
