#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
: "${BONFYRE_STATE_DIR:=/tmp/bonfyre-teardown}"
export BONFYRE_STATE_DIR

runtime="$BONFYRE_STATE_DIR/runtime"
state="$BONFYRE_STATE_DIR/state"
rm -rf "$BONFYRE_STATE_DIR"
mkdir -p "$runtime/cache" "$runtime/workers" "$runtime/sockets" "$runtime/projections" "$runtime/plugins" "$runtime/tmp"
printf 'lock\n' >"$runtime/runtime.lock"
printf 'credential\n' >"$runtime/ephemeral-credentials"
: >"$runtime/bonfyre.sock"

# Create durable mission, artifact, event, and receipt evidence before removal.
BONFYRE_STATE_DIR="$BONFYRE_STATE_DIR" sh "$root/tests/fabric_smoke.sh" >/dev/null
test -f "$state/fabric.db"
mission_count=$(sqlite3 "$state/fabric.db" 'SELECT count(*) FROM missions;')
receipt_count=$(sqlite3 "$state/fabric.db" 'SELECT count(*) FROM receipts;')
test "$mission_count" -gt 0
test "$receipt_count" -gt 0

BONFYRE_STATE_DIR="$state" "$root/programs/bonfyre/bonfyre" system teardown --runtime-root "$runtime" --state-root "$state" --preserve-durable | grep -q '^state=teardown-complete$'
test ! -e "$runtime/runtime.lock"
test ! -e "$runtime/ephemeral-credentials"
test ! -e "$runtime/bonfyre.sock"
for directory in cache workers sockets projections plugins tmp; do test ! -e "$runtime/$directory"; done
test -f "$state/fabric.db"
test "$(sqlite3 "$state/fabric.db" 'SELECT count(*) FROM missions;')" -eq "$mission_count"
test "$(sqlite3 "$state/fabric.db" 'SELECT count(*) FROM receipts;')" -eq "$receipt_count"

# Idempotent repeated teardown, then verify the preserved state is readable.
BONFYRE_STATE_DIR="$state" "$root/programs/bonfyre/bonfyre" system teardown --runtime-root "$runtime" --state-root "$state" --preserve-durable >/dev/null
BONFYRE_STATE_DIR="$state" "$root/programs/bonfyre/bonfyre" mission show acceptance | grep -q '^status=complete$'
