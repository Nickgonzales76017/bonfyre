#!/bin/sh
set -eu

bin=${1:?PackCondition binary required}
root=$(mktemp -d)
trap 'rm -rf "$root"' EXIT
db="$root/conditions.db"

"$bin" init --db "$db" > "$root/init.json"
grep -q '"state":"ready"' "$root/init.json"

"$bin" assert --db "$db" --condition venue-open --subject world:deep-ellum \
  --scope world:deep-ellum --state satisfied --generation 1 --basis evidence:door-sensor:1 > "$root/a1.json"
grep -q '"state":"accepted"' "$root/a1.json"
"$bin" check --db "$db" --condition venue-open --subject world:deep-ellum \
  --scope world:deep-ellum > "$root/check1.json"
grep -q '"state":"satisfied"' "$root/check1.json"
grep -q '"generation":1' "$root/check1.json"

"$bin" assert --db "$db" --condition venue-open --subject world:deep-ellum \
  --scope world:deep-ellum --state satisfied --generation 1 --basis evidence:door-sensor:1 > "$root/idempotent.json"
grep -q '"state":"idempotent"' "$root/idempotent.json"

! "$bin" assert --db "$db" --condition venue-open --subject world:deep-ellum \
  --scope world:deep-ellum --state unsatisfied --generation 1 --basis evidence:door-sensor:1 \
  > "$root/conflict.out" 2> "$root/conflict.err"
grep -q 'conflicting generation' "$root/conflict.err"

"$bin" assert --db "$db" --condition venue-open --subject world:deep-ellum \
  --scope world:deep-ellum --state unsatisfied --generation 2 --basis evidence:door-sensor:2 > "$root/a2.json"
grep -q '"state":"accepted"' "$root/a2.json"
! "$bin" assert --db "$db" --condition venue-open --subject world:deep-ellum \
  --scope world:deep-ellum --state satisfied --generation 1 --basis evidence:stale \
  > "$root/stale.out" 2> "$root/stale.err"
grep -q 'stale generation' "$root/stale.err"

"$bin" check --db "$db" --condition venue-open --subject world:deep-ellum \
  --scope world:deep-ellum > "$root/check2.json"
grep -q '"state":"unsatisfied"' "$root/check2.json"
grep -q '"generation":2' "$root/check2.json"
grep -q '"basis":"evidence:door-sensor:2"' "$root/check2.json"

"$bin" check --db "$db" --condition venue-open --subject world:bishop-arts \
  --scope world:bishop-arts > "$root/wrong-scope.json"
grep -q '"state":"unknown"' "$root/wrong-scope.json"
grep -q '"generation":0' "$root/wrong-scope.json"

! "$bin" assert --db "$db" --condition venue-open --subject world:deep-ellum \
  --scope world:deep-ellum --state satisfied --generation 3 --basis '' > /dev/null 2>&1

[ "$(sqlite3 "$db" 'SELECT count(*) FROM pack_condition_state;')" = 1 ]
[ "$(sqlite3 "$db" "SELECT state||':'||generation||':'||basis FROM pack_condition_state;")" = 'unsatisfied:2:evidence:door-sensor:2' ]

echo 'PackCondition monotonic exact truth: PASS'
