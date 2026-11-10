#!/bin/sh
set -eu
bin=${1:?MaterialLease binary required}
root=$(mktemp -d)
trap 'rm -rf "$root"' EXIT
address="$root/address.db"
lease="$root/lease.db"
sqlite3 "$address" <<'SQL'
CREATE TABLE candidate_advertisement(identity TEXT PRIMARY KEY,candidate_kind TEXT,owner_ref TEXT,exact_verifier TEXT,lowerer_ref TEXT,validity_frontier TEXT,canonical_json TEXT);
INSERT INTO candidate_advertisement VALUES('external:shared','external_occurrence','ExternalEstate:external:shared','ForeignTwin:admission','MaterialCompiler:external','','{}'),('external:new','external_occurrence','ExternalEstate:external:new','ForeignTwin:admission','MaterialCompiler:external','','{}');
SQL

cat > "$root/deep.json" <<'JSON'
{"schema":"bonfyre-scoped-demand-closure.v3","subject":"world:deep-ellum","scope":"world:deep-ellum","pack_demands":[{"identity":"cms:pack_demand:deep","candidate_groups":[{"source":"shared","candidates":[{"identity":"external:shared","owner_ref":"ExternalEstate:external:shared"}]}]}],"world_demands":[],"truth":"scoped_candidate_cut_exact_owner_recheck_required"}
JSON
cat > "$root/jrss.json" <<'JSON'
{"schema":"bonfyre-scoped-demand-closure.v3","subject":"project:jrss","scope":"release:main","pack_demands":[],"world_demands":[{"identity":"cms:world:jrss","dependencies":[{"dependency":"ForeignTwin:https://example.test/shared","candidates":[{"identity":"external:shared","owner_ref":"ExternalEstate:external:shared"}]}]}],"truth":"scoped_candidate_cut_exact_owner_recheck_required"}
JSON
cat > "$root/empty-deep.json" <<'JSON'
{"schema":"bonfyre-scoped-demand-closure.v3","subject":"world:deep-ellum","scope":"world:deep-ellum","pack_demands":[],"world_demands":[],"truth":"scoped_candidate_cut_exact_owner_recheck_required"}
JSON
cat > "$root/jrss-new.json" <<'JSON'
{"schema":"bonfyre-scoped-demand-closure.v3","subject":"project:jrss","scope":"release:main","pack_demands":[{"identity":"cms:pack_demand:jrss","candidate_groups":[{"source":"new","candidates":[{"identity":"external:new","owner_ref":"ExternalEstate:external:new"}]}]}],"world_demands":[],"truth":"scoped_candidate_cut_exact_owner_recheck_required"}
JSON

"$bin" sync --db "$lease" --address-db "$address" --closure "$root/deep.json" > "$root/deep-sync.json"
grep -q '"physical_acquires":1' "$root/deep-sync.json"
"$bin" sync --db "$lease" --address-db "$address" --closure "$root/jrss.json" > "$root/jrss-sync.json"
grep -q '"physical_acquires":0' "$root/jrss-sync.json"
[ "$(sqlite3 "$lease" "SELECT count(*) FROM material_lease WHERE identity='external:shared';")" = 2 ]
[ "$(sqlite3 "$lease" "SELECT count(*) FROM material_transition WHERE action='acquire' AND identity='external:shared';")" = 1 ]

mkdir "$root/acquire" "$root/cancel"
"$bin" drain --db "$lease" --acquire-dir "$root/acquire" --cancel-dir "$root/cancel" > "$root/drain1.json"
grep -q '"transitions_emitted":1' "$root/drain1.json"
[ "$(find "$root/acquire/MaterialCompiler-external" -name '*.json' | wc -l | tr -d ' ')" = 1 ]
grep -q '"identity":"external:shared"' "$root/acquire/MaterialCompiler-external"/*.json
[ "$(find "$root/cancel" -type f | wc -l | tr -d ' ')" = 0 ]

"$bin" sync --db "$lease" --address-db "$address" --closure "$root/empty-deep.json" > "$root/deep-off.json"
grep -q '"physical_releases":0' "$root/deep-off.json"
[ "$(sqlite3 "$lease" "SELECT count(*) FROM material_lease WHERE identity='external:shared';")" = 1 ]
[ "$(sqlite3 "$lease" "SELECT count(*) FROM material_transition WHERE action='release';")" = 0 ]

"$bin" sync --db "$lease" --address-db "$address" --closure "$root/jrss-new.json" > "$root/jrss-new-sync.json"
grep -q '"physical_acquires":1' "$root/jrss-new-sync.json"
grep -q '"physical_releases":1' "$root/jrss-new-sync.json"
[ "$(sqlite3 "$lease" "SELECT count(*) FROM material_lease WHERE identity='external:shared';")" = 0 ]
[ "$(sqlite3 "$lease" "SELECT count(*) FROM material_lease WHERE identity='external:new';")" = 1 ]
"$bin" drain --db "$lease" --acquire-dir "$root/acquire" --cancel-dir "$root/cancel" > "$root/drain2.json"
grep -q '"transitions_emitted":2' "$root/drain2.json"
[ "$(find "$root/cancel/MaterialCompiler-external" -name '*.json' | wc -l | tr -d ' ')" = 1 ]
grep -q '"identity":"external:shared"' "$root/cancel/MaterialCompiler-external"/*.json
grep -q '"refcount_before":1,"refcount_after":0' "$root/cancel/MaterialCompiler-external"/*.json
grep -q '"identity":"external:new"' "$root/acquire/MaterialCompiler-external"/*.json

before=$(sqlite3 "$lease" 'SELECT count(*) FROM material_transition;')
"$bin" sync --db "$lease" --address-db "$address" --closure "$root/jrss-new.json" > "$root/idempotent.json"
grep -q '"physical_acquires":0' "$root/idempotent.json"
grep -q '"physical_releases":0' "$root/idempotent.json"
[ "$(sqlite3 "$lease" 'SELECT count(*) FROM material_transition;')" = "$before" ]

printf '%s\n' '{"schema":"bad","subject":"project:jrss","scope":"release:main","truth":"observation"}' > "$root/bad.json"
! "$bin" sync --db "$lease" --address-db "$address" --closure "$root/bad.json" >/dev/null 2>&1
[ "$(sqlite3 "$lease" "SELECT count(*) FROM material_lease WHERE subject='project:jrss' AND identity='external:new';")" = 1 ]
echo 'Scoped material leases -> shared physical acquire / last-release cancel: PASS'
