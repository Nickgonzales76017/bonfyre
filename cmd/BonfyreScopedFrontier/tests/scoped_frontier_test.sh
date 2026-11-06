#!/bin/sh
set -eu
bin=${1:?scoped-frontier binary required}
root=$(mktemp -d)
trap 'rm -rf "$root"' EXIT
address="$root/address.db"
state="$root/frontier.db"
sqlite3 "$address" <<'SQL'
CREATE TABLE candidate_advertisement(identity TEXT PRIMARY KEY,candidate_kind TEXT,owner_ref TEXT,exact_verifier TEXT,lowerer_ref TEXT,validity_frontier TEXT,canonical_json TEXT);
CREATE TABLE candidate_advertisement_dependency(identity TEXT,dependency_ref TEXT);
CREATE TABLE candidate_advertisement_term(identity TEXT,axis TEXT,term TEXT);
INSERT INTO candidate_advertisement VALUES('cms:pack_activation:deep','pack','CMS:pack_activation:deep','CMS:publication','PackGraph:activation','','{}'),('cms:pack_activation:jrss','pack','CMS:pack_activation:jrss','CMS:publication','PackGraph:activation','','{}');
INSERT INTO candidate_advertisement_dependency VALUES('cms:pack_activation:deep','cms:pack_head:deep-ellum-nightlife'),('cms:pack_demand:deep','cms:pack_head:deep-ellum-nightlife'),('cms:world:deep','ForeignTwin:https://example.test/deep.rss'),('external:deep','ForeignTwin:https://example.test/deep.rss'),('cms:pack_activation:jrss','cms:pack_head:jrss-release'),('cms:pack_demand:jrss','cms:pack_head:jrss-release'),('cms:world:jrss','ForeignTwin:https://example.test/jrss.json'),('external:jrss','ForeignTwin:https://example.test/jrss.json'),('external:new','ForeignTwin:https://example.test/new.rss');
INSERT INTO candidate_advertisement_term VALUES('cms:pack_activation:deep','condition','venue-open'),('cms:pack_activation:deep','subject','world-deep-ellum'),('cms:pack_activation:deep','scope','world-deep-ellum'),('cms:pack_activation:jrss','condition','release-window'),('cms:pack_activation:jrss','subject','project-jrss'),('cms:pack_activation:jrss','scope','release-main');
SQL

cat > "$root/deep.json" <<'JSON'
{"schema":"bonfyre-scoped-demand-closure.v3","subject":"world:deep-ellum","scope":"world:deep-ellum","active_packs":[{"pack":"deep-ellum-nightlife","activation":"cms:pack_activation:deep"}],"pack_demands":[{"identity":"cms:pack_demand:deep","candidate_groups":[{"source":"deep-feed","candidates":[{"identity":"external:deep"}]}]}],"world_demands":[{"identity":"cms:world:deep","dependencies":[{"dependency":"ForeignTwin:https://example.test/deep.rss","candidates":[{"identity":"external:deep"}]}]}],"truth":"scoped_candidate_cut_exact_owner_recheck_required"}
JSON
cat > "$root/jrss.json" <<'JSON'
{"schema":"bonfyre-scoped-demand-closure.v3","subject":"project:jrss","scope":"release:main","active_packs":[{"pack":"jrss-release","activation":"cms:pack_activation:jrss"}],"pack_demands":[{"identity":"cms:pack_demand:jrss","candidate_groups":[{"source":"jrss-feed","candidates":[{"identity":"external:jrss"}]}]}],"world_demands":[{"identity":"cms:world:jrss","dependencies":[{"dependency":"ForeignTwin:https://example.test/jrss.json","candidates":[{"identity":"external:jrss"}]}]}],"truth":"scoped_candidate_cut_exact_owner_recheck_required"}
JSON
"$bin" track --db "$state" --address-db "$address" --closure "$root/deep.json" > "$root/deep-track.json"
"$bin" track --db "$state" --address-db "$address" --closure "$root/jrss.json" > "$root/jrss-track.json"
[ "$(sqlite3 "$state" 'SELECT count(*) FROM closure_frontier;')" = 2 ]
[ "$(sqlite3 "$state" "SELECT count(*) FROM closure_dependency WHERE dependency_ref='PackCondition:venue-open:world-deep-ellum:world-deep-ellum';")" = 1 ]
[ "$(sqlite3 "$state" "SELECT count(*) FROM closure_dependency WHERE dependency_ref='PackCondition:release-window:project-jrss:release-main';")" = 1 ]

"$bin" affected --db "$state" --write 'ForeignTwin:https://example.test/deep.rss' > "$root/deep-hit.json"
grep -q '"subject":"world:deep-ellum"' "$root/deep-hit.json"
! grep -q '"subject":"project:jrss"' "$root/deep-hit.json"
cat > "$root/delta.json" <<'JSON'
{"schema":"bonfyre-fact-delta.v1","delta_id":"mixed-change","writes":["cms:pack_head:deep-ellum-nightlife","ForeignTwin:https://example.test/jrss.json"]}
JSON
"$bin" delta --db "$state" --delta "$root/delta.json" > "$root/delta-out.json"
grep -q '"affected_count":2' "$root/delta-out.json"

cat > "$root/deep-v2.json" <<'JSON'
{"schema":"bonfyre-scoped-demand-closure.v3","subject":"world:deep-ellum","scope":"world:deep-ellum","active_packs":[],"pack_demands":[],"world_demands":[{"identity":"external:new","dependencies":[{"dependency":"ForeignTwin:https://example.test/new.rss","candidates":[{"identity":"external:new"}]}]}],"truth":"scoped_candidate_cut_exact_owner_recheck_required"}
JSON
"$bin" track --db "$state" --address-db "$address" --closure "$root/deep-v2.json" > "$root/retrack.json"
[ "$(sqlite3 "$state" "SELECT count(*) FROM closure_dependency WHERE dependency_ref='PackCondition:venue-open:world-deep-ellum:world-deep-ellum';")" = 1 ]
"$bin" affected --db "$state" --write 'PackCondition:venue-open:world-deep-ellum:world-deep-ellum' | grep -q '"affected_count":1'
"$bin" affected --db "$state" --write 'ForeignTwin:https://example.test/deep.rss' | grep -q '"affected_count":0'
"$bin" affected --db "$state" --write 'ForeignTwin:https://example.test/new.rss' | grep -q '"affected_count":1'

cat > "$root/bad.json" <<'JSON'
{"schema":"bonfyre-scoped-demand-closure.v3","subject":"world:deep-ellum","scope":"world:deep-ellum","truth":"observation"}
JSON
! "$bin" track --db "$state" --address-db "$address" --closure "$root/bad.json" >/dev/null 2>&1
"$bin" affected --db "$state" --write 'ForeignTwin:https://example.test/new.rss' | grep -q '"affected_count":1'
"$bin" affected --db "$state" --write 'ForeignTwin:https://example.test/irrelevant' > "$root/miss.json"
grep -q '"affected_count":0' "$root/miss.json"
echo 'Scoped closure frontier -> exact FactDelta reheat + blocked-pack thaw: PASS'
