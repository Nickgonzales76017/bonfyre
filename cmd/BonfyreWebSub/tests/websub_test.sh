#!/bin/sh
set -eu

bin=${1:?native WebSub binary required}
scratch=$(mktemp -d)
pid=''
cleanup() {
  [ -z "$pid" ] || kill "$pid" 2>/dev/null || true
  [ -z "$pid" ] || wait "$pid" 2>/dev/null || true
  rm -rf "$scratch"
}
trap cleanup EXIT INT TERM

bun tests/websub_fixture.mjs "$scratch" &
pid=$!
for _ in $(seq 1 50); do [ -s "$scratch/port" ] && break; sleep 0.05; done
[ -s "$scratch/port" ]
port=$(cat "$scratch/port")
topic="http://127.0.0.1:$port/topic"

BONFYRE_WEBSUB_DB="$scratch/websub.db" \
BONFYRE_PUBLIC_ORIGIN="http://127.0.0.1:18999" \
BONFYRE_WEBSUB_ALLOW_INSECURE_TEST=1 \
  "$bin" discover "$topic" >"$scratch/discover.json"
grep -q '"pushable":true' "$scratch/discover.json"

BONFYRE_WEBSUB_DB="$scratch/websub.db" \
BONFYRE_PUBLIC_ORIGIN="http://127.0.0.1:18999" \
BONFYRE_WEBSUB_ALLOW_INSECURE_TEST=1 \
  "$bin" subscribe "$topic" --authority-receipt receipt:test:websub >"$scratch/subscribe.json"
grep -q '"state":"pending"' "$scratch/subscribe.json"
token=$(sed -n 's/.*\/websub\/\([0-9a-f]*\)".*/\1/p' "$scratch/subscribe.json")
[ -n "$token" ]
grep -q 'hub.mode=subscribe' "$scratch/forms"

BONFYRE_WEBSUB_DB="$scratch/websub.db" \
  "$bin" list >"$scratch/list.json"
grep -q 'receipt:test:websub' "$scratch/list.json"
! grep -q '"secret"' "$scratch/list.json"

# Renewal must retain one callback/token identity; it is a lease extension,
# not another source subscription competing to re-read the same body.
sqlite3 "$scratch/websub.db" "UPDATE subscriptions SET state='active', lease_expires=strftime('%s','now')+10 WHERE token='$token';"
BONFYRE_WEBSUB_DB="$scratch/websub.db" \
BONFYRE_PUBLIC_ORIGIN="http://127.0.0.1:18999" \
BONFYRE_WEBSUB_ALLOW_INSECURE_TEST=1 \
  "$bin" reconcile --window 60 >"$scratch/reconcile.json"
grep -q '"renewed_same_identity":1' "$scratch/reconcile.json"
[ "$(sqlite3 "$scratch/websub.db" 'SELECT count(*) FROM subscriptions;')" = 1 ]

BONFYRE_WEBSUB_DB="$scratch/websub.db" \
BONFYRE_PUBLIC_ORIGIN="http://127.0.0.1:18999" \
BONFYRE_WEBSUB_ALLOW_INSECURE_TEST=1 \
  "$bin" unsubscribe "$token" >"$scratch/unsubscribe.json"
grep -q '"state":"unsubscribe_pending"' "$scratch/unsubscribe.json"
grep -q 'hub.mode=unsubscribe' "$scratch/forms"

# A verified callback only writes SourceDirty. The bounded drain worker is the
# one place that invokes ForeignTwin observation, and coalesces one source row.
mkdir -p "$scratch/bin" "$scratch/foreign"
cat >"$scratch/bin/bonfyre-project" <<'SH'
#!/bin/sh
printf '%s\n' "$*" >> "$BONFYRE_TEST_OBSERVES"
receipt=''
topic=''
while [ "$#" -gt 0 ]; do
  case "$1" in
    observe) shift; [ "$#" -gt 1 ] || exit 2; shift; topic=$1 ;;
    --receipt) shift; [ "$#" -gt 0 ] || exit 2; receipt=$1 ;;
  esac
  shift
done
[ -n "$receipt" ]
printf '{"schema":"bonfyre-foreign-observation.v1","source_url":"%s","observation_kind":"boundary_response"}\n' "$topic" > "$receipt"
exit 0
SH
chmod +x "$scratch/bin/bonfyre-project"
sqlite3 "$scratch/websub.db" "INSERT INTO source_dirty(token,topic,frontier,state,dirty_at) VALUES('$token','$topic','test','pending',strftime('%s','now'));"
BONFYRE_WEBSUB_DB="$scratch/websub.db" \
BONFYRE_BIN_DIR="$scratch/bin" \
BONFYRE_FOREIGN_ROOT="$scratch/foreign" \
BONFYRE_FACT_DELTAS="$scratch/fact-deltas" \
BONFYRE_TEST_OBSERVES="$scratch/observes" \
  "$bin" drain --limit 1 >"$scratch/drain.json"
grep -q '"observed":1' "$scratch/drain.json"
grep -q '"foreign_twin_fact_deltas":1' "$scratch/drain.json"
grep -q 'foreign observe' "$scratch/observes"
[ "$(sqlite3 "$scratch/websub.db" 'SELECT count(*) FROM source_dirty;')" = 0 ]
[ "$(find "$scratch/fact-deltas" -name '*.json' | wc -l | tr -d ' ')" = 1 ]
grep -q '"writes":\["ForeignTwin:http://127.0.0.1:' "$scratch/fact-deltas"/*.json
[ "$(sqlite3 "$scratch/websub.db" 'SELECT count(*) FROM foreign_delta;')" = 0 ]

# MaterialCompiler has no network privilege. Fifty separately addressed
# requests for the same topic are accepted by the WebSub-owned ingress as one
# durable SourceDirty row, then one bounded ForeignTwin observation.  The
# requests are test fixtures, not retained foreign state.
mkdir "$scratch/demands"
i=1
while [ "$i" -le 50 ]; do
  printf '{"schema":"bonfyre-foreign-observation-demand.v1","identity":"fixture:material:%s","topic":"%s","truth":"observation_requested_not_performed"}\n' "$i" "$topic" > "$scratch/demands/$i.json"
  i=$((i + 1))
done
BONFYRE_WEBSUB_DB="$scratch/websub.db" \
BONFYRE_WEBSUB_ALLOW_INSECURE_TEST=1 \
  "$bin" accept-demands --dir "$scratch/demands" --limit 64 >"$scratch/accept-demands.json"
grep -q '"accepted_demands":50' "$scratch/accept-demands.json"
grep -q '"coalesced_topics":1' "$scratch/accept-demands.json"
[ "$(sqlite3 "$scratch/websub.db" "SELECT count(*) FROM source_dirty WHERE topic='$topic' AND demand_count=50;")" = 1 ]
[ "$(find "$scratch/demands" -name '*.json' ! -name '*.handled' | wc -l | tr -d ' ')" = 0 ]
BONFYRE_WEBSUB_DB="$scratch/websub.db" \
BONFYRE_BIN_DIR="$scratch/bin" \
BONFYRE_FOREIGN_ROOT="$scratch/foreign" \
BONFYRE_FACT_DELTAS="$scratch/fact-deltas" \
BONFYRE_TEST_OBSERVES="$scratch/demand-observes" \
  "$bin" drain --limit 16 >"$scratch/demand-drain.json"
grep -q '"observed":1' "$scratch/demand-drain.json"
[ "$(wc -l < "$scratch/demand-observes" | tr -d ' ')" = 1 ]

# A malformed request remains visible for owner inspection and cannot make the
# queue dirty, much less trigger an observation.
printf '{"schema":"bonfyre-foreign-observation-demand.v1","identity":"fixture:bad","topic":"%s","truth":"wrong"}\n' "$topic" > "$scratch/demands/reject.json"
if BONFYRE_WEBSUB_DB="$scratch/websub.db" BONFYRE_WEBSUB_ALLOW_INSECURE_TEST=1 "$bin" accept-demands --dir "$scratch/demands" >/dev/null 2>&1; then
  echo 'malformed material demand unexpectedly accepted' >&2
  exit 1
fi
[ -f "$scratch/demands/reject.json" ]

if BONFYRE_WEBSUB_DB="$scratch/reject.db" BONFYRE_WEBSUB_ALLOW_INSECURE_TEST=1 "$bin" subscribe "$topic" >/dev/null 2>&1; then
  echo 'subscription without authority receipt unexpectedly succeeded' >&2
  exit 1
fi

echo 'native WebSub adapter: PASS'
