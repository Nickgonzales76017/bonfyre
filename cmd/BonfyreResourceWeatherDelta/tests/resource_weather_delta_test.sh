#!/bin/sh
set -eu
bin=${1:?binary required}
root=$(mktemp -d)
trap 'rm -rf "$root"' EXIT
mkdir "$root/out"
cat > "$root/old.json" <<'JSON'
{"schema":"bonfyre-resource-weather.v1","sources":[{"source":"feed-a","tier":"FROZEN"},{"source":"feed-b","tier":"COLD"},{"source":"feed-c","tier":"WARM"}]}
JSON
cat > "$root/new.json" <<'JSON'
{"schema":"bonfyre-resource-weather.v1","sources":[{"source":"feed-a","tier":"HOT"},{"source":"feed-b","tier":"COLD"},{"source":"feed-d","tier":"WARM"}]}
JSON
"$bin" diff --old "$root/old.json" --new "$root/new.json" --out "$root/out" > "$root/result.json"
grep -q '"changed":3' "$root/result.json"
grep -q '"unchanged":1' "$root/result.json"
grep -q '"added":1' "$root/result.json"
grep -q '"removed":1' "$root/result.json"
[ "$(find "$root/out" -name '*.json' | wc -l | tr -d ' ')" = 3 ]
grep -R -q '"writes":\["ResourceWeather:feed-a"\]' "$root/out"
grep -R -q '"from_tier":"FROZEN","to_tier":"HOT"' "$root/out"
grep -R -q '"writes":\["ResourceWeather:feed-c"\]' "$root/out"
grep -R -q '"to_tier":"absent"' "$root/out"
grep -R -q '"writes":\["ResourceWeather:feed-d"\]' "$root/out"
grep -R -q '"from_tier":"absent"' "$root/out"
"$bin" diff --old "$root/old.json" --new "$root/new.json" --out "$root/out" > "$root/retry.json"
[ "$(find "$root/out" -name '*.json' | wc -l | tr -d ' ')" = 3 ]
grep -R -q '"previous_snapshot_digest":"[0-9a-f]\{64\}"' "$root/out"
grep -R -q '"snapshot_digest":"[0-9a-f]\{64\}"' "$root/out"

# A later transition with the same source/tier pair is not the old FactDelta:
# its snapshot bodies are different, so it must receive a new causal identity.
sed 's/"sources"/"frontier":"two","sources"/' "$root/old.json" > "$root/old-again.json"
sed 's/"sources"/"frontier":"three","sources"/' "$root/new.json" > "$root/new-again.json"
"$bin" diff --old "$root/old-again.json" --new "$root/new-again.json" --out "$root/out" > "$root/again.json"
[ "$(find "$root/out" -name '*.json' | wc -l | tr -d ' ')" = 6 ]
[ "$(grep -R '"writes":\["ResourceWeather:feed-a"\]' "$root/out" | sed -n 's/.*"delta_id":"\([^"]*\)".*/\1/p' | sort -u | wc -l | tr -d ' ')" = 2 ]

# Project's scheduler frontier is physical standing, not a semantic fact. A
# HOT source becoming due must enter only its matching physical lease path.
printf '%s\n' '{"schema":"bonfyre-resource-weather.v1","sources":[{"source":"feed-due","tier":"HOT","decision":"not due"}]}' > "$root/not-due.json"
printf '%s\n' '{"schema":"bonfyre-resource-weather.v1","sources":[{"source":"feed-due","tier":"HOT","decision":"due"}]}' > "$root/due.json"
"$bin" diff --old "$root/not-due.json" --new "$root/due.json" --out "$root/due-out" > "$root/due-result.json"
grep -q '"changed":1' "$root/due-result.json"
grep -R -q '"from_tier":"HOT","to_tier":"HOT"' "$root/due-out"
grep -R -q '"from_decision":"not due","to_decision":"due"' "$root/due-out"

printf '%s\n' '{"schema":"not-resource-weather","sources":[]}' > "$root/bad.json"
if "$bin" diff --old "$root/bad.json" --new "$root/new.json" --out "$root/out"; then
  echo 'accepted non-ResourceWeather snapshot' >&2
  exit 1
fi
echo 'ResourceWeather snapshot diff -> targeted FactDelta: PASS'
