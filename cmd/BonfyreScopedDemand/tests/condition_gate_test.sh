#!/bin/sh
set -eu

resolver=${1:?ScopedDemand resolver required}
condition=${2:?PackCondition binary required}
estate=${3:?ExternalEstate binary required}
root=$(mktemp -d)
trap 'rm -rf "$root"' EXIT
db="$root/address.db"
cdb="$root/conditions.db"

cat > "$root/ads.jsonl" <<'JSONL'
{"identity":"external:nightlife","candidate_kind":"external_occurrence","owner_ref":"ExternalEstate:external:nightlife","exact_verifier":"ForeignTwin:admission","lowerer_ref":"MaterialCompiler:external","validity_frontier":"fixture","terms":["source:nightlife-shared"],"dependencies":["ForeignTwin:https://example.test/nightlife.rss"]}
{"identity":"external:base","candidate_kind":"external_occurrence","owner_ref":"ExternalEstate:external:base","exact_verifier":"ForeignTwin:admission","lowerer_ref":"MaterialCompiler:external","validity_frontier":"fixture","terms":["source:base-feed"],"dependencies":["ForeignTwin:https://example.test/base.rss"]}
{"identity":"cms:pack_activation:nightlife:deep","candidate_kind":"pack","owner_ref":"CMS:pack_activation:nightlife","exact_verifier":"CMS:publication","lowerer_ref":"PackGraph:activation","validity_frontier":"fixture","terms":["pack:nightlife","subject:world-deep-ellum","scope:world-deep-ellum","condition:venue-open","condition:staffing-ready"],"dependencies":["cms:pack_head:nightlife"]}
{"identity":"cms:pack_activation:nightlife:bishop","candidate_kind":"pack","owner_ref":"CMS:pack_activation:nightlife","exact_verifier":"CMS:publication","lowerer_ref":"PackGraph:activation","validity_frontier":"fixture","terms":["pack:nightlife","subject:world-bishop-arts","scope:world-bishop-arts","condition:venue-open","condition:staffing-ready"],"dependencies":["cms:pack_head:nightlife"]}
{"identity":"cms:pack_demand:nightlife:internet:events","candidate_kind":"pack_demand","owner_ref":"CMS:pack_demand:nightlife","exact_verifier":"CMS:publication","lowerer_ref":"MaterialCompiler:pack_demand","validity_frontier":"fixture","terms":["pack:nightlife","demand:internet","requirement:events","source:nightlife-shared","regime:remote"],"dependencies":["cms:pack_head:nightlife"]}
{"identity":"cms:pack_activation:base:other","candidate_kind":"pack","owner_ref":"CMS:pack_activation:base","exact_verifier":"CMS:publication","lowerer_ref":"PackGraph:activation","validity_frontier":"fixture","terms":["pack:base","subject:world-other","scope:world-other"],"dependencies":["cms:pack_head:base"]}
{"identity":"cms:pack_demand:base:internet:base","candidate_kind":"pack_demand","owner_ref":"CMS:pack_demand:base","exact_verifier":"CMS:publication","lowerer_ref":"MaterialCompiler:pack_demand","validity_frontier":"fixture","terms":["pack:base","demand:internet","requirement:base","source:base-feed","regime:remote"],"dependencies":["cms:pack_head:base"]}
JSONL

"$estate" import-advertisements --db "$db" --advertisements "$root/ads.jsonl" > "$root/imported.json"
sqlite3 "$db" "INSERT INTO occurrence(id,observed_name,source_ref,primary_url,source_kind,section,raw_json) VALUES
('external:nightlife','Nightlife','nightlife-shared','https://example.test/nightlife.rss','rss','nightlife','{}'),
('external:base','Base','base-feed','https://example.test/base.rss','rss','base','{}');"

# Conditional activation without a truth owner is blocked, not assumed true.
"$resolver" resolve --db "$db" --subject world:deep-ellum --scope world:deep-ellum > "$root/no-store.json"
grep -q '"active_packs":0' "$root/no-store.json"
grep -q '"conditional_packs":1' "$root/no-store.json"
grep -q '"blocked_packs":1' "$root/no-store.json"
grep -q '"pack_demands":0' "$root/no-store.json"
grep -q '"candidate_survivors":0' "$root/no-store.json"

"$condition" init --db "$cdb" > /dev/null
# Truth for another scope cannot satisfy Deep Ellum.
"$condition" assert --db "$cdb" --condition venue-open --subject world:bishop-arts \
  --scope world:bishop-arts --state satisfied --generation 1 --basis evidence:bishop:venue > /dev/null
"$condition" assert --db "$cdb" --condition staffing-ready --subject world:bishop-arts \
  --scope world:bishop-arts --state satisfied --generation 1 --basis evidence:bishop:staff > /dev/null
"$resolver" resolve --db "$db" --condition-db "$cdb" --subject world:deep-ellum \
  --scope world:deep-ellum > "$root/wrong-scope.json"
grep -q '"active_packs":0' "$root/wrong-scope.json"
grep -q '"blocked_packs":1' "$root/wrong-scope.json"

# ALL declared conditions are required.
"$condition" assert --db "$cdb" --condition venue-open --subject world:deep-ellum \
  --scope world:deep-ellum --state satisfied --generation 1 --basis evidence:deep:venue > /dev/null
"$resolver" resolve --db "$db" --condition-db "$cdb" --subject world:deep-ellum \
  --scope world:deep-ellum > "$root/partial.json"
grep -q '"active_packs":0' "$root/partial.json"
grep -q '"blocked_packs":1' "$root/partial.json"

"$condition" assert --db "$cdb" --condition staffing-ready --subject world:deep-ellum \
  --scope world:deep-ellum --state unsatisfied --generation 1 --basis evidence:deep:staff:closed > /dev/null
"$resolver" resolve --db "$db" --condition-db "$cdb" --subject world:deep-ellum \
  --scope world:deep-ellum > "$root/false.json"
grep -q '"active_packs":0' "$root/false.json"
grep -q '"blocked_packs":1' "$root/false.json"

# A newer exact condition generation opens only the addressed World.
"$condition" assert --db "$cdb" --condition staffing-ready --subject world:deep-ellum \
  --scope world:deep-ellum --state satisfied --generation 2 --basis evidence:deep:staff:open > /dev/null
"$resolver" resolve --db "$db" --condition-db "$cdb" --subject world:deep-ellum \
  --scope world:deep-ellum > "$root/deep-open.json"
grep -q '"active_packs":1' "$root/deep-open.json"
grep -q '"conditional_packs":1' "$root/deep-open.json"
grep -q '"blocked_packs":0' "$root/deep-open.json"
grep -q '"pack_demands":1' "$root/deep-open.json"
grep -q '"candidate_survivors":1' "$root/deep-open.json"
grep -q '"identity":"external:nightlife"' "$root/deep-open.json"

"$resolver" resolve --db "$db" --condition-db "$cdb" --subject world:bishop-arts \
  --scope world:bishop-arts > "$root/bishop-open.json"
grep -q '"active_packs":1' "$root/bishop-open.json"
grep -q '"candidate_survivors":1' "$root/bishop-open.json"

# Unconditional packs remain compatible and do not require a condition store.
"$resolver" resolve --db "$db" --subject world:other --scope world:other > "$root/base.json"
grep -q '"active_packs":1' "$root/base.json"
grep -q '"conditional_packs":0' "$root/base.json"
grep -q '"blocked_packs":0' "$root/base.json"
grep -q '"identity":"external:base"' "$root/base.json"

echo 'ScopedDemand exact PackCondition gate: PASS'
