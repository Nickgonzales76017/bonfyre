#!/bin/sh
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
state="${BONFYRE_STATE_DIR:-/tmp/bonfyre-workgraph-fanout}/fanout-$$"
fabric="$root/programs/bonfyre/bonfyre"
mkdir -p "$state"
export BONFYRE_STATE_DIR="$state"
make -C "$root/engine/core" >/dev/null
make -C "$root/programs/bonfyre" >/dev/null
"$fabric" fabric init >/dev/null
"$fabric" mission create fanout-proof >/dev/null
"$fabric" work add fanout-proof parent core.identity >/dev/null
claim=$("$fabric" work claim fanout-proof parent parent-worker)
token=$(printf '%s\n' "$claim" | sed -n 's/^claim_token=//p')
"$fabric" work complete fanout-proof parent parent-worker "$token" >/dev/null
"$fabric" work fanout fanout-proof parent group core.identity child 2 --failure-policy fail >/dev/null
"$fabric" work add fanout-proof join core.identity --fanout-group group --fanin-required 1 >/dev/null
"$fabric" work fanin fanout-proof join | grep -q '^status=blocked$'

claim=$("$fabric" work claim fanout-proof child-1 worker-1)
token=$(printf '%s\n' "$claim" | sed -n 's/^claim_token=//p')
"$fabric" work complete fanout-proof child-1 worker-1 "$token" >/dev/null
"$fabric" work fanin fanout-proof join | grep -q '^status=blocked$'
claim=$("$fabric" work claim fanout-proof child-2 worker-2)
token=$(printf '%s\n' "$claim" | sed -n 's/^claim_token=//p')
"$fabric" work complete fanout-proof child-2 worker-2 "$token" >/dev/null
released=$("$fabric" work fanin fanout-proof join)
printf '%s\n' "$released" | grep -q '^status=ready$'
receipt=$(printf '%s\n' "$released" | sed -n 's/^receipt_id=//p')
"$fabric" receipt show "$receipt" | grep -q '\"transition\":\"fanin_released\"'

# Re-evaluation and restart preserve one ready release state.
"$fabric" work fanin fanout-proof join | grep -q '^status=ready$'
"$fabric" work status fanout-proof join | grep -q '^status=ready$'
