#!/bin/sh
set -eu
bin=${1:?binary required}
root=$(mktemp -d)
trap 'rm -rf "$root"' EXIT

"$bin" audit > "$root/audit.json"
grep -q '"surfaces":22' "$root/audit.json"
grep -q '"coverage_axes":9' "$root/audit.json"
grep -q '"shallow_owner_sets":0' "$root/audit.json"
grep -q '"shallow_axis_owner_sets":0' "$root/audit.json"
grep -q '"world_unbound":0' "$root/audit.json"
grep -q '"duplicate_routes":0' "$root/audit.json"
grep -q '"broken_transitions":0' "$root/audit.json"
[ "$("$bin" list | wc -l | tr -d ' ')" = 22 ]
[ "$("$bin" axes | wc -l | tr -d ' ')" = 9 ]
"$bin" closure tonight world:deep-ellum > "$root/tonight.json"
grep -q '"world":"world:deep-ellum"' "$root/tonight.json"
grep -q 'BonfyreMaterialWeatherGate' "$root/tonight.json"
grep -q '"stage"' "$root/tonight.json"
"$bin" closure packs world:deep-ellum > "$root/packs.json"
grep -q 'BonfyreScopedLeaseReheat' "$root/packs.json"
"$bin" closure market world:deep-ellum > "$root/market.json"
grep -q 'CommerceChain' "$root/market.json"
grep -q 'settlement' "$root/market.json"
"$bin" closure /habitat/ world:deep-ellum > "$root/habitat.json"
grep -q 'ActorPresentationCell' "$root/habitat.json"
command -v sqlite3 >/dev/null 2>&1 || { echo "sqlite3 CLI is required for owner-state maturity fixture" >&2; exit 1; }
estate="$root/estate"
mkdir -p "$estate"
sqlite3 "$estate/cms.db" <<'SQL'
CREATE TABLE internet_source(source TEXT,status TEXT,publisher_ident TEXT,publisher_name TEXT);
INSERT INTO internet_source VALUES('feed-lab','published','Q1','Lab Test');
CREATE TABLE site_presentation(status TEXT,subject_address TEXT);
INSERT INTO site_presentation VALUES('published','institution:publisher:lab-test');
CREATE TABLE program_instance(status TEXT,owner_subject TEXT);
INSERT INTO program_instance VALUES('published','user:1');
CREATE TABLE source_collection(status TEXT,subject_address TEXT);
INSERT INTO source_collection VALUES('published','user:1');
CREATE TABLE surface_scene(status TEXT);
INSERT INTO surface_scene VALUES('published');
CREATE TABLE publication_occurrence(status TEXT,assertion_state TEXT,updated_at TEXT);
INSERT INTO publication_occurrence VALUES('published','OBSERVED','2026-10-26T00:00:00Z');
SQL
sqlite3 "$estate/items.db" <<'SQL'
CREATE TABLE item(source TEXT,publisher TEXT);
INSERT INTO item VALUES('feed-lab','Lab Test'),('feed-lab','Lab Test'),('feed-lab','Lab Test');
SQL
sqlite3 "$estate/frontier.db" <<'SQL'
CREATE TABLE channel(state TEXT,census_source TEXT);
INSERT INTO channel VALUES('active','feed-lab');
SQL
sqlite3 "$estate/subject_fabric.db" <<'SQL'
CREATE TABLE subject_bundle(subject TEXT);
INSERT INTO subject_bundle VALUES('institution:publisher:lab-test');
CREATE TABLE subject_reader(subject TEXT,census_source TEXT,state TEXT);
INSERT INTO subject_reader VALUES('institution:publisher:lab-test','feed-lab','active');
CREATE TABLE subject_action(action TEXT);
INSERT INTO subject_action VALUES('Save');
CREATE TABLE subject_scene(scene_key TEXT);
INSERT INTO subject_scene VALUES('scene:lab');
SQL
"$bin" estate-audit "$estate" > "$root/estate.json"
grep -q '"maturity_debt":0' "$root/estate.json"
grep -q '"reader_coverage_bps":10000' "$root/estate.json"
grep -q '"cross_axis_open":2' "$root/estate.json"
grep -q '"account":"unmeasured"' "$root/estate.json"
grep -q '"evidence":"unmeasured"' "$root/estate.json"
grep -q '"whole_estate_ready":false' "$root/estate.json"
grep -q '"lake":{"measured":true,' "$root/estate.json"
grep -q '"items_with_subject":3,"item_subject_bps":10000,"subjects":1,"subjects_with_site":1,"subjects_with_reader":1,"subjects_with_site_and_reader":1' "$root/estate.json"
"$bin" lake-check "$estate" --write-floor "$root/lake.floor" > "$root/lake.tsv"
if grep -qE '^(FAIL|NOTE)' "$root/lake.tsv"; then echo "a held lake reported a regression" >&2; exit 1; fi
grep -q '^items_with_subject 3$' "$root/lake.floor"
set +e
"$bin" estate-gate "$estate" >/dev/null
gate_rc=$?
set -e
[ "$gate_rc" -ne 0 ] || { echo "estate gate accepted unresolved account/evidence axes" >&2; exit 1; }
account="$root/account-work.db"
sqlite3 "$account" <<'SQL'
CREATE TABLE account_work(id TEXT,subject TEXT,scope TEXT);
INSERT INTO account_work VALUES('task:1','user:1','self');
CREATE TABLE account_work_run(work_id TEXT,run_id TEXT,worker_id TEXT,state TEXT,created_ms INTEGER,updated_ms INTEGER);
INSERT INTO account_work_run VALUES('task:1','run:1','Aurekai assistant','completed',1,2);
CREATE TABLE account_work_step(work_id TEXT,node_id TEXT,operator_id TEXT,state TEXT,attempt INTEGER,updated_ms INTEGER,output_ref TEXT NOT NULL DEFAULT '');
INSERT INTO account_work_step VALUES('task:1','render','RenderAgent','complete',1,2,'artifact://fixture/output');
CREATE TABLE account_work_meta(key TEXT PRIMARY KEY,value INTEGER NOT NULL);
INSERT INTO account_work_meta VALUES('indexed_ms',CAST((julianday('now')-2440587.5)*86400000 AS INTEGER));
SQL
sqlite3 "$estate/meter.db" <<'SQL'
CREATE TABLE events(id INTEGER PRIMARY KEY,cost REAL);
INSERT INTO events(cost) VALUES(NULL);
CREATE TABLE rate(id INTEGER PRIMARY KEY);
INSERT INTO rate VALUES(1);
SQL
BONFYRE_ACCOUNT_WORK_DB="$account" "$bin" estate-audit "$estate" > "$root/account.json"
grep -q '"cross_axis_open":1' "$root/account.json"
grep -q '"account":"covered"' "$root/account.json"
grep -q '"outputs":1' "$root/account.json"
grep -q '"meter_unpriced_events":1' "$root/account.json"
set +e
BONFYRE_ACCOUNT_WORK_DB="$account" "$bin" estate-gate "$estate" >/dev/null
evidence_gate_rc=$?
set -e
[ "$evidence_gate_rc" -ne 0 ] || { echo "estate gate accepted unresolved evidence" >&2; exit 1; }
sqlite3 "$estate/control.db" <<'SQL'
CREATE TABLE evidence_relations(evidence TEXT,kind TEXT,claim TEXT);
INSERT INTO evidence_relations VALUES('receipt:fixture','supports','publication-occurrence:fixture:observed');
CREATE TABLE foreign_twin_observations(receipt_id TEXT,predicate TEXT);
INSERT INTO foreign_twin_observations VALUES('receipt:fixture','source-observed');
CREATE TABLE frontier_layers(subject_resource TEXT);
CREATE TABLE solved_invariants(invariant_id TEXT,truth_plane TEXT,status TEXT);
INSERT INTO solved_invariants VALUES('estate-observation:fixture','measured','cooled');
CREATE TABLE known_noncauses(noncause_id TEXT);
CREATE TABLE estate_evidence_sync_receipts(
  sync_id TEXT PRIMARY KEY,
  publication_count INTEGER NOT NULL,
  publication_watermark TEXT NOT NULL,
  considered INTEGER NOT NULL,
  exact INTEGER NOT NULL,
  unresolved INTEGER NOT NULL,
  synced_at TEXT NOT NULL
);
INSERT INTO estate_evidence_sync_receipts VALUES(
  'fixture-sync',1,'2026-10-26T00:00:00Z',1,1,0,'2026-10-26T00:01:00Z'
);
SQL
BONFYRE_ACCOUNT_WORK_DB="$account" "$bin" estate-audit "$estate" > "$root/evidence.json"
grep -q '"cross_axis_open":0' "$root/evidence.json"
grep -q '"evidence":"covered"' "$root/evidence.json"
grep -q '"measured_invariants":1' "$root/evidence.json"
grep -q '"proven_invariants":0' "$root/evidence.json"
grep -q '"sync_current":true' "$root/evidence.json"
BONFYRE_ACCOUNT_WORK_DB="$account" "$bin" estate-gate "$estate" >/dev/null
sqlite3 "$estate/subject_fabric.db" "DELETE FROM subject_reader"
if "$bin" estate-gate "$estate" >"$root/gap.json" 2>/dev/null; then
  echo "estate gate accepted a missing subject-reader backfill" >&2
  exit 1
fi
grep -q '"without_reader":1' "$root/gap.json"
cp "$root/lake.floor" "$root/lake.floor.before"
"$bin" lake-check "$estate" --floor "$root/lake.floor" --write-floor "$root/lake.floor" > "$root/lake-regress.tsv"
grep -q '^FAIL	estate	subject-lake	items_with_subject fell from 3 to 0$' "$root/lake-regress.tsv"
grep -q '^FAIL	estate	subject-lake	1 addressed publishers have no reader$' "$root/lake-regress.tsv"
cmp -s "$root/lake.floor" "$root/lake.floor.before" || { echo "a regressed cut lowered the lake floor" >&2; exit 1; }
BONFYRE_ACCOUNT_WORK_DB="$account" "$bin" backfill-plan "$estate" > "$root/covered-plan.json"
if grep -q '"owner":"Fabric/AurekaiActivity/BonfyreMeter"' "$root/covered-plan.json"; then
  echo "covered account axis returned after unrelated reader debt" >&2
  exit 1
fi
if grep -q '"owner":"EvidenceGraph/ProofFrontierGraph/ForeignTwinGraph"' "$root/covered-plan.json"; then
  echo "covered evidence axis returned after unrelated reader debt" >&2
  exit 1
fi
export BONFYRE_ACCOUNT_WORK_DB="$root/absent-account.db"
"$bin" backfill-plan "$estate" > "$root/plan.json"
grep -q '"owner":"SubjectFabric"' "$root/plan.json"
grep -q 'reconcile publisher_readers' "$root/plan.json"
grep -q '"owner":"Fabric/AurekaiActivity/BonfyreMeter"' "$root/plan.json"
if grep -q '"owner":"EvidenceGraph/ProofFrontierGraph/ForeignTwinGraph"' "$root/plan.json"; then
  echo "covered evidence axis returned when only account was made unmeasured" >&2
  exit 1
fi
grep -q '"cross_axis_open":' "$root/plan.json"
echo '22 surface contracts + 9 estate maturity axes + owner-state backfill gate + source-lake regression floor: PASS'
