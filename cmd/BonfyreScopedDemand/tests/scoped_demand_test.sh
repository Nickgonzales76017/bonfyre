#!/bin/sh
set -eu
bin=${1:?scoped-demand binary required}
workers=${2:?lowering-workers binary required}
root=$(mktemp -d)
trap 'rm -rf "$root"' EXIT
db="$root/address.db"
sqlite3 "$db" <<'SQL'
CREATE TABLE candidate_advertisement(identity TEXT PRIMARY KEY,candidate_kind TEXT,owner_ref TEXT,exact_verifier TEXT,lowerer_ref TEXT,validity_frontier TEXT,canonical_json TEXT);
CREATE TABLE candidate_advertisement_term(identity TEXT,axis TEXT,term TEXT);
CREATE TABLE candidate_advertisement_dependency(identity TEXT,dependency_ref TEXT);
CREATE TABLE occurrence(id TEXT PRIMARY KEY,source_ref TEXT,primary_url TEXT);
INSERT INTO candidate_advertisement VALUES
('cms:pack_activation:deep','pack','CMS:pack_activation:deep-ellum-nightlife','CMS:publication','PackGraph:activation','','{}'),
('cms:pack_activation:other','pack','CMS:pack_activation:other','CMS:publication','PackGraph:activation','','{}'),
('cms:pack_demand:stage','pack_demand','CMS:pack_demand:deep-ellum-nightlife','CMS:publication','MaterialCompiler:pack_demand','','{}'),
('cms:pack_demand:capacity','pack_demand','CMS:pack_demand:deep-ellum-nightlife','CMS:publication','MaterialCompiler:pack_demand','','{}'),
('cms:world:deep-ellum','world_publication','CMS:world:deep-ellum','CMS:publication','SurfaceIR:world','','{}'),
('cms:world:world-only','world_publication','CMS:world:world-only','CMS:publication','SurfaceIR:world','','{}'),
('external:deep','external_occurrence','ExternalEstate:external:deep','ForeignTwin:admission','MaterialCompiler:external','','{}'),
('external:other','external_occurrence','ExternalEstate:external:other','ForeignTwin:admission','MaterialCompiler:external','','{}');
INSERT INTO candidate_advertisement_term VALUES
('cms:pack_activation:deep','pack','deep-ellum-nightlife'),
('cms:pack_activation:deep','subject','world-deep-ellum'),
('cms:pack_activation:deep','scope','world-deep-ellum'),
('cms:pack_activation:other','pack','other'),
('cms:pack_activation:other','subject','world-other'),
('cms:pack_activation:other','scope','world-other'),
('cms:pack_demand:stage','pack','deep-ellum-nightlife'),
('cms:pack_demand:stage','demand','internet'),
('cms:pack_demand:stage','requirement','stage-times'),
('cms:pack_demand:stage','source','deep-ellum-feed'),
('cms:pack_demand:stage','regime','remote'),
('cms:pack_demand:capacity','pack','deep-ellum-nightlife'),
('cms:pack_demand:capacity','demand','internet'),
('cms:pack_demand:capacity','requirement','late-capacity'),
('cms:pack_demand:capacity','regime','remote'),
('cms:world:deep-ellum','subject','world-deep-ellum'),
('cms:world:world-only','subject','world-world-only');
INSERT INTO candidate_advertisement_dependency VALUES
('cms:world:deep-ellum','cms:internet_source:deep-ellum-feed'),
('cms:world:deep-ellum','cms:publication_source:deep-ellum-feed'),
('cms:world:deep-ellum','ForeignTwin:https://example.test/deep-ellum.rss'),
('cms:world:world-only','ForeignTwin:https://example.test/other.rss');
INSERT INTO occurrence VALUES
('external:deep','deep-ellum-feed','https://example.test/deep-ellum.rss'),
('external:other','other-feed','https://example.test/other.rss');
SQL

# Pack and World independently select the same source. Both semantic closures
# remain satisfied while the physical candidate and handoff are deduplicated.
"$bin" resolve --db "$db" --subject world:deep-ellum --scope world:deep-ellum > "$root/out.json"
grep -q '"active_packs":1' "$root/out.json"
grep -q '"pack_demands":2' "$root/out.json"
grep -q '"world_publications":1' "$root/out.json"
grep -q '"candidate_survivors":1' "$root/out.json"
grep -q '"unresolved_demands":1' "$root/out.json"
grep -q '"handoffs_dispatched":0' "$root/out.json"
grep -q '"identity":"external:deep"' "$root/out.json"
grep -q '"reused":true' "$root/out.json"
! grep -q '"identity":"external:other"' "$root/out.json"
grep -q '"requirement":"late-capacity".*"state":"unresolved"' "$root/out.json"

"$bin" dispatch --db "$db" --subject world:deep-ellum --scope world:deep-ellum --dir "$root/handoffs" > "$root/dispatched.json"
grep -q '"candidate_survivors":1' "$root/dispatched.json"
grep -q '"handoffs_dispatched":1' "$root/dispatched.json"
[ "$(find "$root/handoffs/MaterialCompiler-external" -name '*.json' | wc -l | tr -d ' ')" = 1 ]
handoff=$(find "$root/handoffs/MaterialCompiler-external" -name '*.json' | head -1)
grep -q '"identity":"external:deep"' "$handoff"
mkdir "$root/demands"
"$workers" drain --db "$root/derived.db" --lowerer-dir "$root/handoffs" --external-estate "$db" --demand-dir "$root/demands" --limit 8 > "$root/lowered.json"
grep -q '"handled":1' "$root/lowered.json"
[ "$(sqlite3 "$root/derived.db" "SELECT count(*) FROM material_observation_demand WHERE identity='external:deep' AND topic='https://example.test/deep-ellum.rss' AND state='queued';")" = 1 ]

# A World with no Pack still reaches its own exact declared ForeignTwin topic.
"$bin" resolve --db "$db" --subject world:world-only --scope world:world-only > "$root/world-only.json"
grep -q '"active_packs":0' "$root/world-only.json"
grep -q '"pack_demands":0' "$root/world-only.json"
grep -q '"world_publications":1' "$root/world-only.json"
grep -q '"candidate_survivors":1' "$root/world-only.json"
grep -q '"unresolved_demands":0' "$root/world-only.json"
grep -q '"identity":"external:other"' "$root/world-only.json"
! grep -q '"identity":"external:deep"' "$root/world-only.json"
echo 'PackGraph + WorldPublication -> scoped ExternalEstate -> MaterialCompiler demand: PASS'
