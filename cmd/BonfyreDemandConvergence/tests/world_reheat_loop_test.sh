#!/bin/sh
set -eu

scoped=${1:?ScopedDemand binary required}
workers=${2:?LoweringWorkers binary required}
websub=${3:?WebSub binary required}
estate=${4:?ExternalEstate binary required}
web=${5:?WebAddress binary required}

root=$(mktemp -d)
trap 'rm -rf "$root"' EXIT
mkdir -p "$root/initial" "$root/demands" "$root/observer-bin" "$root/foreign" "$root/forwarded"

topic='http://127.0.0.1:19876/deep-ellum.rss'
other='http://127.0.0.1:19876/other.rss'

cat > "$root/ads.jsonl" <<JSONL
{"identity":"external:deep","candidate_kind":"external_occurrence","owner_ref":"ExternalEstate:external:deep","exact_verifier":"ForeignTwin:admission","lowerer_ref":"MaterialCompiler:external","validity_frontier":"fixture","terms":["source:deep-ellum-feed"],"dependencies":["ForeignTwin:$topic"]}
{"identity":"external:other","candidate_kind":"external_occurrence","owner_ref":"ExternalEstate:external:other","exact_verifier":"ForeignTwin:admission","lowerer_ref":"MaterialCompiler:external","validity_frontier":"fixture","terms":["source:other-feed"],"dependencies":["ForeignTwin:$other"]}
{"identity":"cms:pack_activation:nightlife","candidate_kind":"pack","owner_ref":"CMS:pack_activation:nightlife","exact_verifier":"CMS:publication","lowerer_ref":"PackGraph:activation","validity_frontier":"fixture","terms":["pack:nightlife","subject:world-deep-ellum","scope:world-deep-ellum"],"dependencies":["cms:pack_head:nightlife"]}
{"identity":"cms:pack_demand:nightlife:internet:stage-times","candidate_kind":"pack_demand","owner_ref":"CMS:pack_demand:nightlife","exact_verifier":"CMS:publication","lowerer_ref":"MaterialCompiler:pack_demand","validity_frontier":"fixture","terms":["pack:nightlife","demand:internet","requirement:stage-times","source:deep-ellum-feed","regime:remote"],"dependencies":["cms:pack_head:nightlife"]}
{"identity":"cms:world:deep-ellum","candidate_kind":"world_publication","owner_ref":"CMS:world:deep-ellum","exact_verifier":"CMS:publication","lowerer_ref":"SurfaceIR:world","validity_frontier":"fixture","terms":["subject:world-deep-ellum"],"dependencies":["ForeignTwin:$topic"]}
{"identity":"cms:world:unrelated","candidate_kind":"world_publication","owner_ref":"CMS:world:unrelated","exact_verifier":"CMS:publication","lowerer_ref":"SurfaceIR:world","validity_frontier":"fixture","terms":["subject:world-unrelated"],"dependencies":["ForeignTwin:$other"]}
JSONL

"$estate" import-advertisements --db "$root/address.db" --advertisements "$root/ads.jsonl" > "$root/imported.json"
sqlite3 "$root/address.db" "INSERT INTO occurrence(id,observed_name,source_ref,primary_url,source_kind,section,raw_json) VALUES
('external:deep','Deep Ellum feed','deep-ellum-feed','$topic','rss','nightlife','{}'),
('external:other','Other feed','other-feed','$other','rss','other','{}');"

# Two already-derived public Worlds are present before the source changes.
sqlite3 "$root/projection.db" "PRAGMA foreign_keys=ON;
CREATE TABLE public_projection(closure_key TEXT PRIMARY KEY,source_fingerprint TEXT NOT NULL,response BLOB NOT NULL,response_digest TEXT NOT NULL,compiled_at INTEGER NOT NULL);
CREATE TABLE public_projection_dependency(closure_key TEXT NOT NULL REFERENCES public_projection(closure_key) ON DELETE CASCADE,dependency TEXT NOT NULL,PRIMARY KEY(closure_key,dependency));
CREATE INDEX public_projection_dependency_lookup ON public_projection_dependency(dependency);
INSERT INTO public_projection VALUES('public-world|/w/deep-ellum','old-deep','deep-v1','d1',strftime('%s','now'));
INSERT INTO public_projection VALUES('public-world|/w/unrelated','old-other','other-v1','d2',strftime('%s','now'));
INSERT INTO public_projection_dependency VALUES('public-world|/w/deep-ellum','cms:world:deep-ellum');
INSERT INTO public_projection_dependency VALUES('public-world|/w/unrelated','cms:world:unrelated');"

# One scoped semantic closure produces one physical observation request.
"$scoped" dispatch --db "$root/address.db" --subject world:deep-ellum --scope world:deep-ellum --dir "$root/initial" > "$root/scoped.json"
grep -q '"active_packs":1' "$root/scoped.json"
grep -q '"pack_demands":1' "$root/scoped.json"
grep -q '"world_publications":1' "$root/scoped.json"
grep -q '"candidate_survivors":1' "$root/scoped.json"
grep -q '"handoffs_dispatched":1' "$root/scoped.json"
"$workers" drain --db "$root/derived.db" --lowerer-dir "$root/initial" --external-estate "$root/address.db" --demand-dir "$root/demands" --limit 8 > "$root/requested.json"
grep -q '"handled":1' "$root/requested.json"
[ "$(sqlite3 "$root/derived.db" "SELECT count(*) FROM material_observation_demand WHERE identity='external:deep' AND topic='$topic';")" = 1 ]

# WebSub owns dirty-source coalescing. The fixture observer is only the exact
# ForeignTwin boundary; it creates the receipt WebSub requires before a FactDelta.
BONFYRE_WEBSUB_DB="$root/websub.db" BONFYRE_WEBSUB_ALLOW_INSECURE_TEST=1 \
  "$websub" accept-demands --dir "$root/demands" > "$root/accepted.json"
grep -q '"accepted_demands":1' "$root/accepted.json"
cat > "$root/observer-bin/bonfyre-project" <<'SH'
#!/bin/sh
topic=''
receipt=''
while [ "$#" -gt 0 ]; do
  case "$1" in
    observe) shift; shift; topic=$1 ;;
    --receipt) shift; receipt=$1 ;;
  esac
  shift
done
[ -n "$topic" ] && [ -n "$receipt" ]
printf '{"schema":"bonfyre-foreign-observation.v1","source_url":"%s","observed_at":"2026-09-15T00:00:00Z","observation_kind":"boundary_response"}\n' "$topic" > "$receipt"
SH
chmod +x "$root/observer-bin/bonfyre-project"
BONFYRE_WEBSUB_DB="$root/websub.db" \
BONFYRE_WEBSUB_ALLOW_INSECURE_TEST=1 \
BONFYRE_BIN_DIR="$root/observer-bin" \
BONFYRE_FOREIGN_ROOT="$root/foreign" \
BONFYRE_FACT_DELTAS="$root/fact-deltas" \
  "$websub" drain --limit 8 > "$root/observed.json"
grep -q '"foreign_twin_fact_deltas":1' "$root/observed.json"
grep -q "\"writes\":\\[\"ForeignTwin:$topic\"\\]" "$root/fact-deltas"/*.json

# The exact ForeignTwin write now cuts only its two declared dependents:
# the external material and the World projection. The unrelated World is absent.
"$estate" fact-delta --db "$root/address.db" --delta "$root/fact-deltas"/*.json > "$root/cut.json"
grep -q '"affected_count":2' "$root/cut.json"
grep -q '"identity":"external:deep"' "$root/cut.json"
grep -q '"identity":"cms:world:deep-ellum"' "$root/cut.json"
! grep -q 'cms:world:unrelated' "$root/cut.json"
[ "$(sqlite3 "$root/address.db" "SELECT count(*) FROM candidate_lowering_queue WHERE state='queued';")" = 2 ]

"$estate" drain-lowering-queue --db "$root/address.db" --dir "$root/forwarded" --limit 8 > "$root/forwarded.json"
grep -q '"lowering_handoffs_dispatched":2' "$root/forwarded.json"
[ "$(find "$root/forwarded/SurfaceIR-world" -name '*.json' | wc -l | tr -d ' ')" = 1 ]
[ "$(find "$root/forwarded/MaterialCompiler-external" -name '*.json' | wc -l | tr -d ' ')" = 1 ]

# SurfaceIR retires only the affected derived World; the unrelated public body
# survives. It does not render eagerly or mutate the World owner.
"$web" public-projection-drain --cache-db "$root/projection.db" --lowerer-dir "$root/forwarded" --limit 8 > "$root/retired.json"
grep -q '"handoffs_handled":1' "$root/retired.json"
[ "$(sqlite3 "$root/projection.db" "SELECT count(*) FROM public_projection WHERE closure_key='public-world|/w/deep-ellum';")" = 0 ]
[ "$(sqlite3 "$root/projection.db" "SELECT count(*) FROM public_projection WHERE closure_key='public-world|/w/unrelated';")" = 1 ]

# MaterialCompiler sees the causal ForeignTwin write and materializes the receipt
# instead of recursively asking WebSub to observe the same source again.
"$workers" drain --db "$root/derived.db" --lowerer-dir "$root/forwarded" --external-estate "$root/address.db" --foreign-receipts "$root/foreign/receipts" --demand-dir "$root/demands" --limit 8 > "$root/materialized.json"
grep -q '"handled":1' "$root/materialized.json"
[ "$(sqlite3 "$root/derived.db" "SELECT count(*) FROM material_foreign_receipt_artifact WHERE identity='external:deep' AND topic='$topic' AND observation_kind='boundary_response';")" = 1 ]
[ "$(find "$root/demands" -name '*.json' ! -name '*.handled' | wc -l | tr -d ' ')" = 0 ]

# Replay is idempotent at the AddressPlane receipt boundary.
"$estate" fact-delta --db "$root/address.db" --delta "$root/fact-deltas"/*.json > "$root/replay.json"
grep -q '"state":"already_processed"' "$root/replay.json"
[ "$(sqlite3 "$root/address.db" "SELECT count(*) FROM candidate_lowering_queue;")" = 2 ]

echo 'Pack/World demand -> ForeignTwin -> FactDelta -> selective World reheat: PASS'
