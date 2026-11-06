#!/bin/sh
# Verifies the actual Restate endpoint protocol, not a hand-written manifest.
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
adapter="$root/services/bonfyre-restate-native"
log=$(mktemp)
body=$(mktemp)
pid=''
cleanup() {
    [ -n "$pid" ] && kill "$pid" 2>/dev/null || true
    rm -f "$log" "$body"
}
trap cleanup EXIT INT TERM

make -C "$adapter" release >/dev/null
"$adapter/target/release/bonfyre-restate-native" >"$log" 2>&1 &
pid=$!
sleep 1
curl --http2-prior-knowledge -fsS \
    -H 'accept: application/vnd.restate.endpointmanifest.v4+json' \
    http://127.0.0.1:9080/discover >"$body"
grep -q '"name":"BonfyreWorkGraph"' "$body"
grep -q '"name":"run_two_nodes"' "$body"
grep -q '"protocolMode":"BIDI_STREAM"' "$body"
echo "restate native discovery: passed"
