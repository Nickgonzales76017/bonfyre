#!/bin/sh
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
state="${BONFYRE_STATE_DIR:-/tmp/bonfyre-workgraph-restart}/restart-$$"
fabric="$root/programs/bonfyre/bonfyre"
mkdir -p "$state"
export BONFYRE_STATE_DIR="$state"
make -C "$root/engine/core" >/dev/null
make -C "$root/programs/bonfyre" >/dev/null
"$fabric" fabric init >/dev/null
"$fabric" mission create restart-proof >/dev/null
"$fabric" work add restart-proof task core.identity --retries 1 >/dev/null
claim=$("$fabric" work claim-next abandoned-worker --lease-ms 60)
old_token=$(printf '%s\n' "$claim" | sed -n 's/^claim_token=//p')
deadline=$(printf '%s\n' "$claim" | sed -n 's/^lease_expires_at_ms=//p')
# Every command below is a new process; no in-memory scheduler state survives.
"$fabric" work status restart-proof task | grep -q '^status=running$'
while [ "$(($(date +%s) * 1000))" -le "$deadline" ]; do :; done
"$fabric" work reap-expired restart-proof >/dev/null
claim=$("$fabric" work claim-next resumed-worker --lease-ms 30000)
token=$(printf '%s\n' "$claim" | sed -n 's/^claim_token=//p')
test "$token" != "$old_token"
"$fabric" work complete restart-proof task resumed-worker "$token" | grep -q '^status=complete$'
"$fabric" work resume restart-proof >/dev/null
"$fabric" work status restart-proof task | grep -q '^status=complete$'
