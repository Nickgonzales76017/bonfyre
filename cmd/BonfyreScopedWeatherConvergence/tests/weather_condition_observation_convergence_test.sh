#!/bin/sh
set -eu
condition=${1:?condition binary required}
outbox=${2:?condition outbox required}
bridge=${3:?condition delta bridge required}
demand=${4:?scoped demand required}
frontier=${5:?scoped frontier required}
lease=${6:?material lease required}
reheat=${7:?scoped lease reheat required}
weather_gate=${8:?material weather gate required}
workers=${9:?lowering workers required}
shift 9
websub=${1:?websub required}
lease_gate=${2:?websub lease gate required}
root=$(mktemp -d)
trap 'rm -rf "$root"' EXIT
address="$root/address.db"
state="$root/frontier.db"
conditions="$root/conditions.db"
leases="$root/leases.db"
mkdir "$root/events1" "$root/events2" "$root/events3" "$root/events4" "$root/events5" \
      "$root/facts2" "$root/facts3" "$root/facts4" "$root/facts5" "$root/closures" \
      "$root/acq1" "$root/can1" "$root/acq2" "$root/can2" "$root/acq3" "$root/can3" \
      "$root/acq4" "$root/can4" "$root/weather-out" "$root/demands" "$root/foreign"

sqlite3 "$address" <<'SQL'
CREATE TABLE candidate_advertisement(identity TEXT PRIMARY KEY,candidate_kind TEXT,owner_ref TEXT,exact_verifier TEXT,lowerer_ref TEXT,validity_frontier TEXT,canonical_json TEXT);
CREATE TABLE candidate_advertisement_term(identity TEXT,axis TEXT,term TEXT);
CREATE TABLE candidate_advertisement_dependency(identity TEXT,dependency_ref TEXT);
CREATE TABLE occurrence(id TEXT PRIMARY KEY,source_ref TEXT,primary_url TEXT);
INSERT INTO candidate_advertisement VALUES
('cms:pack_activation:deep','pack','CMS:pack_activation:deep','CMS:publication','PackGraph:activation','','{}'),
('cms:pack_demand:deep','pack_demand','CMS:pack_demand:deep','CMS:publication','MaterialCompiler:pack_demand','','{}'),
('external:deep','external_occurrence','ExternalEstate:external:deep','ForeignTwin:admission','MaterialCompiler:external','','{}');
INSERT INTO candidate_advertisement_term VALUES
('cms:pack_activation:deep','pack','deep-ellum-nightlife'),
('cms:pack_activation:deep','subject','world-deep-ellum'),
('cms:pack_activation:deep','scope','world-deep-ellum'),
('cms:pack_activation:deep','condition','venue-open'),
('cms:pack_demand:deep','pack','deep-ellum-nightlife'),
('cms:pack_demand:deep','demand','internet'),
('cms:pack_demand:deep','requirement','stage-times'),
('cms:pack_demand:deep','source','deep-feed'),
('cms:pack_demand:deep','regime','remote');
INSERT INTO candidate_advertisement_dependency VALUES
('cms:pack_activation:deep','cms:pack_head:deep-ellum-nightlife'),
('cms:pack_demand:deep','cms:pack_head:deep-ellum-nightlife'),
('external:deep','ExternalEstate:external:deep'),
('external:deep','ForeignTwin:http://127.0.0.1:19876/deep.rss');
INSERT INTO occurrence VALUES('external:deep','deep-feed','http://127.0.0.1:19876/deep.rss');
SQL
cat > "$root/frozen.json" <<'JSON'
{"schema":"bonfyre-resource-weather.v1","sources":[{"source":"deep-feed","tier":"FROZEN","decision":"frozen"}]}
JSON
cat > "$root/hot.json" <<'JSON'
{"schema":"bonfyre-resource-weather.v1","sources":[{"source":"deep-feed","tier":"HOT","decision":"due"}]}
JSON

"$condition" init --db "$conditions" >/dev/null
"$condition" assert --db "$conditions" --condition venue-open --subject world:deep-ellum --scope world:deep-ellum --state satisfied --generation 1 --basis evidence:door:1 >/dev/null
"$outbox" --db "$conditions" --dir "$root/events1" >/dev/null
"$demand" resolve --db "$address" --condition-db "$conditions" --subject world:deep-ellum --scope world:deep-ellum > "$root/initial.json"
"$frontier" track --db "$state" --address-db "$address" --closure "$root/initial.json" >/dev/null
"$lease" sync --db "$leases" --address-db "$address" --closure "$root/initial.json" >/dev/null
"$lease" drain --db "$leases" --acquire-dir "$root/acq1" --cancel-dir "$root/can1" >/dev/null
"$weather_gate" drain --db "$root/weather.db" --address-db "$address" --lease-db "$leases" --weather "$root/frozen.json" --acquire-dir "$root/acq1" --cancel-dir "$root/can1" --out-dir "$root/weather-out" > "$root/frozen.out"
grep -q '"held":1' "$root/frozen.out"
[ "$(find "$root/weather-out" -name '*.json' | wc -l | tr -d ' ')" = 0 ]

"$condition" assert --db "$conditions" --condition venue-open --subject world:deep-ellum --scope world:deep-ellum --state unsatisfied --generation 2 --basis evidence:door:2 >/dev/null
"$outbox" --db "$conditions" --dir "$root/events2" >/dev/null
"$bridge" translate --input "$root/events2" --out "$root/facts2" >/dev/null
delta2=$(find "$root/facts2" -name '*.json' | head -1)
"$reheat" reheat --frontier-db "$state" --address-db "$address" --lease-db "$leases" --condition-db "$conditions" --delta "$delta2" --scoped-demand "$demand" --scoped-frontier "$frontier" --material-lease "$lease" --acquire-dir "$root/acq2" --cancel-dir "$root/can2" --closure-dir "$root/closures" >/dev/null
"$weather_gate" drain --db "$root/weather.db" --address-db "$address" --lease-db "$leases" --weather "$root/hot.json" --acquire-dir "$root/acq1" --cancel-dir "$root/can2" --out-dir "$root/weather-out" > "$root/cancel-held.out"
grep -q '"pending_cancelled":1' "$root/cancel-held.out"
grep -q '"admitted":0' "$root/cancel-held.out"

"$condition" assert --db "$conditions" --condition venue-open --subject world:deep-ellum --scope world:deep-ellum --state satisfied --generation 3 --basis evidence:door:3 >/dev/null
"$outbox" --db "$conditions" --dir "$root/events3" >/dev/null
"$bridge" translate --input "$root/events3" --out "$root/facts3" >/dev/null
delta3=$(find "$root/facts3" -name '*.json' | head -1)
"$reheat" reheat --frontier-db "$state" --address-db "$address" --lease-db "$leases" --condition-db "$conditions" --delta "$delta3" --scoped-demand "$demand" --scoped-frontier "$frontier" --material-lease "$lease" --acquire-dir "$root/acq3" --cancel-dir "$root/can3" --closure-dir "$root/closures" > "$root/on-hot.out"
grep -q '"physical_acquires":1' "$root/on-hot.out"
"$weather_gate" drain --db "$root/weather.db" --address-db "$address" --lease-db "$leases" --weather "$root/hot.json" --acquire-dir "$root/acq3" --cancel-dir "$root/can3" --out-dir "$root/weather-out" > "$root/admit.out"
grep -q '"admitted":1' "$root/admit.out"
[ "$(find "$root/weather-out/MaterialCompiler-external" -name '*.json' | wc -l | tr -d ' ')" = 1 ]
"$workers" drain --db "$root/derived.db" --lowerer-dir "$root/weather-out" --external-estate "$address" --demand-dir "$root/demands" --limit 8 > "$root/lowered.out"
grep -q '"handled":1' "$root/lowered.out"
BONFYRE_WEBSUB_DB="$root/websub.db" BONFYRE_WEBSUB_ALLOW_INSECURE_TEST=1 "$websub" accept-demands --dir "$root/demands" > "$root/accepted.out"
grep -q '"accepted_demands":1' "$root/accepted.out"
[ "$(sqlite3 "$root/websub.db" "SELECT count(*) FROM source_dirty WHERE topic='http://127.0.0.1:19876/deep.rss' AND state='pending';")" = 1 ]

"$condition" assert --db "$conditions" --condition venue-open --subject world:deep-ellum --scope world:deep-ellum --state unsatisfied --generation 4 --basis evidence:door:4 >/dev/null
"$outbox" --db "$conditions" --dir "$root/events4" >/dev/null
"$bridge" translate --input "$root/events4" --out "$root/facts4" >/dev/null
delta4=$(find "$root/facts4" -name '*.json' | head -1)
"$reheat" reheat --frontier-db "$state" --address-db "$address" --lease-db "$leases" --condition-db "$conditions" --delta "$delta4" --scoped-demand "$demand" --scoped-frontier "$frontier" --material-lease "$lease" --acquire-dir "$root/acq4" --cancel-dir "$root/can4" --closure-dir "$root/closures" > "$root/off-pending.out"
grep -q '"physical_releases":1' "$root/off-pending.out"
"$lease_gate" apply --cancel-dir "$root/can4" --handoff-dir "$root/weather-out" --demand-dir "$root/demands" --websub-db "$root/websub.db" --lease-db "$leases" > "$root/gated.out"
grep -q '"suppressed":1' "$root/gated.out"
grep -q '"pending_topics_removed":1' "$root/gated.out"
[ "$(sqlite3 "$root/websub.db" "SELECT count(*) FROM source_dirty WHERE topic='http://127.0.0.1:19876/deep.rss';")" = 0 ]
BONFYRE_WEBSUB_DB="$root/websub.db" BONFYRE_WEBSUB_ALLOW_INSECURE_TEST=1 "$websub" drain --limit 8 > "$root/no-observe.out"
grep -q '"observed":0' "$root/no-observe.out"

"$condition" assert --db "$conditions" --condition venue-open --subject world:deep-ellum --scope world:deep-ellum --state satisfied --generation 5 --basis evidence:door:5 >/dev/null
"$outbox" --db "$conditions" --dir "$root/events5" >/dev/null
"$bridge" translate --input "$root/events5" --out "$root/facts5" >/dev/null
delta5=$(find "$root/facts5" -name '*.json' | head -1)
mkdir "$root/acq5" "$root/can5" "$root/weather-out2" "$root/demands2"
"$reheat" reheat --frontier-db "$state" --address-db "$address" --lease-db "$leases" --condition-db "$conditions" --delta "$delta5" --scoped-demand "$demand" --scoped-frontier "$frontier" --material-lease "$lease" --acquire-dir "$root/acq5" --cancel-dir "$root/can5" --closure-dir "$root/closures" >/dev/null
"$weather_gate" drain --db "$root/weather.db" --address-db "$address" --lease-db "$leases" --weather "$root/hot.json" --acquire-dir "$root/acq5" --cancel-dir "$root/can5" --out-dir "$root/weather-out2" > "$root/reacquire.out"
grep -q '"admitted":1' "$root/reacquire.out"
"$workers" drain --db "$root/derived2.db" --lowerer-dir "$root/weather-out2" --external-estate "$address" --demand-dir "$root/demands2" --limit 8 > /dev/null
BONFYRE_WEBSUB_DB="$root/websub.db" BONFYRE_WEBSUB_ALLOW_INSECURE_TEST=1 "$websub" accept-demands --dir "$root/demands2" > "$root/reaccepted.out"
grep -q '"accepted_demands":1' "$root/reaccepted.out"
[ "$(sqlite3 "$root/websub.db" "SELECT count(*) FROM source_dirty WHERE topic='http://127.0.0.1:19876/deep.rss' AND state='pending';")" = 1 ]

echo 'condition + weather + lease + WebSub cancellation convergence: PASS'
