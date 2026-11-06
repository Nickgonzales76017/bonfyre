#!/bin/sh
set -eu

condition=${1:?PackCondition binary required}
outbox=${2:?PackCondition outbox binary required}
root=$(mktemp -d)
trap 'rm -rf "$root"' EXIT
db="$root/conditions.db"
mkdir "$root/deltas"

"$condition" init --db "$db" > /dev/null
"$condition" assert --db "$db" --condition venue-open --subject world:deep-ellum \
  --scope world:deep-ellum --state unsatisfied --generation 1 --basis evidence:door:closed > /dev/null
"$condition" assert --db "$db" --condition venue-open --subject world:deep-ellum \
  --scope world:deep-ellum --state satisfied --generation 2 --basis evidence:door:open > /dev/null
"$condition" assert --db "$db" --condition staffing-ready --subject world:deep-ellum \
  --scope world:deep-ellum --state satisfied --generation 1 --basis evidence:staff:ready > /dev/null

[ "$(sqlite3 "$db" 'SELECT count(*) FROM pack_condition_outbox WHERE emitted=0;')" = 3 ]
"$outbox" --db "$db" --dir "$root/deltas" --limit 16 > "$root/drain.json"
grep -q '"emitted":3' "$root/drain.json"
[ "$(find "$root/deltas" -name '*.json' | wc -l | tr -d ' ')" = 3 ]
[ "$(sqlite3 "$db" 'SELECT count(*) FROM pack_condition_outbox WHERE emitted=0;')" = 0 ]
[ "$(sqlite3 "$db" 'SELECT count(*) FROM pack_condition_outbox WHERE emitted=1;')" = 3 ]
grep -q '"schema":"bonfyre-pack-condition-delta.v1"' "$root/deltas"/*.json
grep -q '"generation":2' "$root/deltas"/*.json
grep -q '"truth":"accepted_condition_change_notification_not_authority"' "$root/deltas"/*.json

# No duplicate delivery after the durable emitted bit has advanced.
"$outbox" --db "$db" --dir "$root/deltas" --limit 16 > "$root/replay.json"
grep -q '"emitted":0' "$root/replay.json"
[ "$(find "$root/deltas" -name '*.json' | wc -l | tr -d ' ')" = 3 ]

echo 'PackCondition transactional outbox: PASS'
