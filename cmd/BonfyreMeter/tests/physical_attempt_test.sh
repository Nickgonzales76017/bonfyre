#!/bin/sh
# A metered event is an observation of one physical attempt.  This is a
# contract test: a missing receipt/attempt/realization cannot accrue, replay
# cannot duplicate usage, and reused identity cannot quietly change payload.
set -eu

BIN=${1:-"$(dirname "$0")/../bonfyre-meter"}
TMP=${TMPDIR:-/tmp}/bonfyre-meter-attempt-$$
trap 'rm -rf "$TMP"' EXIT INT TERM
mkdir -p "$TMP"
DB="$TMP/meter.db"

if "$BIN" record --db "$DB" --key test --op read --provider test --at 2026-09-24T00:00:00Z >/dev/null 2>&1; then
    echo "missing receipt/attempt/realization was accepted" >&2
    exit 1
fi

"$BIN" admit-rate --db "$DB" --provider test --op read --unit query --amount 0.25 \
    --currency USD --effective-at 2026-09-01T00:00:00Z --evidence "test evidence" >/dev/null

record() {
    "$BIN" record --db "$DB" --key test --op read --provider test --bytes "$1" \
        --receipt receipt:test-1 --attempt-ref attempt:test-1 --realization-ref r2:test \
        --at 2026-09-24T00:00:00Z >/dev/null
}
record 24
record 24

events=$(sqlite3 "$DB" "SELECT count(*) FROM events")
[ "$events" = "1" ] || { echo "exact replay created $events events" >&2; exit 1; }

if record 25; then
    echo "changed payload was accepted under one physical attempt" >&2
    exit 1
fi

sqlite3 "$DB" "SELECT cost = 0.25 AND rate_id IS NOT NULL AND physical_attempt_ref = 'attempt:test-1' AND realization_ref = 'r2:test' FROM events" \
    | grep -qx 1
echo "meter physical-attempt contract: PASS"
