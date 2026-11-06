#!/bin/sh
# One executable boundary for the six native organs that previously had only
# self-tests.  These are callers, not substitutes for their owners: every
# fixture exercises a refusal as well as the narrow result the organ may say.
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
TMP=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-organ-callers.XXXXXX")
trap 'rm -rf "$TMP"' EXIT HUP INT TERM

fail() { echo "organ_callers_test: FAIL: $*" >&2; exit 1; }
has() { case "$1" in *"$2"*) ;; *) fail "missing $2";; esac; }

actors="$ROOT/cmd/BonfyreActors/bonfyre-actors"
origin="$ROOT/cmd/BonfyreOrigin/bonfyre-origin"
agent="$ROOT/cmd/BonfyreAgentContract/bonfyre-agent-contract"
standing="$ROOT/cmd/BonfyreStanding/bonfyre-standing"
household="$ROOT/cmd/BonfyreHousehold/bonfyre-household"

for binary in "$actors" "$origin" "$agent" "$standing" "$household"; do
  test -x "$binary" || fail "build the native caller first: $binary"
done

actor_db="$TMP/actors.db"
"$actors" upsert --db "$actor_db" --id org:example --kind organization --name Example \
  --provenance registry > "$TMP/actor-org.json"
"$actors" upsert --db "$actor_db" --id person:alex --kind person --name Alex \
  --org org:example --provenance byline > "$TMP/actor-person.json"
"$actors" relate --db "$actor_db" --from org:example --kind employs --to person:alex \
  --provenance org-chart > "$TMP/actor-edge.json"
first=$("$actors" corroborate --db "$actor_db" --id person:alex --source publisher:one)
repeat=$("$actors" corroborate --db "$actor_db" --id person:alex --source publisher:one)
second=$("$actors" corroborate --db "$actor_db" --id person:alex --source publisher:two)
has "$first" '"newly_independent":true'
has "$repeat" '"newly_independent":false'
has "$second" '"independent_sources":2'

if "$origin" record --db "$TMP/origin.db" --occurrence derived:bad --origin derived \
  --subject subject:one --at 2026-09-21T00:00:00Z --source compiler:one > "$TMP/origin-bad.json" 2>/dev/null; then
  fail "input-less derivation was accepted"
fi
has "$(cat "$TMP/origin-bad.json")" 'must name the exact inputs'
"$origin" record --db "$TMP/origin.db" --occurrence observed:one --origin observed \
  --subject subject:one --at 2026-09-21T00:00:00Z --source source:one > /dev/null
"$origin" record --db "$TMP/origin.db" --occurrence derived:one --origin derived \
  --subject subject:one --at 2026-09-21T00:01:00Z --from observed:one > /dev/null
origin_result=$("$origin" get --db "$TMP/origin.db" --occurrence derived:one)
has "$origin_result" '"origin":"derived"'
has "$origin_result" '"caused_by_us":false'

printf '%s\n' \
  'session|session:one|profile:review|work:root' \
  'event|1|model_turn|subject|work:root|2026-09-21T00:00:00Z|provider:test|model:test|verify:test' \
  'receipt|receipt:one|work:root|occurrence:one|completed|1|1' > "$TMP/session.txt"
agent_result=$("$agent" --file "$TMP/session.txt")
has "$agent_result" '"session_valid":"ok"'
has "$agent_result" '"receipt_covers_session":"ok"'

household_result=$("$household" bind --domain housing --listing listing:one \
  --source-file household/housing/current-listings.jsonl --source-ref receipt:one \
  --document '{"listing":"one"}' --observed availability --prove availability)
has "$household_result" '"address":"household:housing:listing:listing:one"'
has "$household_result" '"observed":true,"proven":false'

printf '%s\n' \
  'available|WATCH|live|0.1|1|currently offered' \
  'unavailable|WATCH|live|1.0|0|not currently offered' \
  'invented|NOT_A_DECLARED_ACTION|live|100.0|1|must be dropped' > "$TMP/candidates.txt"
printf '%s\n' 'unavailable|dwelled|100' > "$TMP/attention.txt"
standing_result=$("$standing" --territory live --candidates "$TMP/candidates.txt" --attention "$TMP/attention.txt")
has "$standing_result" '"candidates":2'
case "$standing_result" in
  *'"subject":"available"'*'"subject":"unavailable"'*) ;;
  *) fail "attention lifted an unavailable opportunity over an available one" ;;
esac

echo "organ_callers_test: PASS"
