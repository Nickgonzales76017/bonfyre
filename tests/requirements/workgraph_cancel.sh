#!/bin/sh
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
state="${BONFYRE_STATE_DIR:-/tmp/bonfyre-workgraph-cancel}/cancel-$$"
fabric="$root/programs/bonfyre/bonfyre"
mkdir -p "$state"
export BONFYRE_STATE_DIR="$state"
make -C "$root/engine/core" >/dev/null
make -C "$root/programs/bonfyre" >/dev/null
"$fabric" fabric init >/dev/null
"$fabric" mission create cancel-proof >/dev/null
"$fabric" work add cancel-proof queued core.identity >/dev/null
"$fabric" work cancel-node cancel-proof queued | grep -q '^status=cancelled$'
"$fabric" work add cancel-proof running core.identity >/dev/null
claim=$("$fabric" work claim cancel-proof running worker-a)
token=$(printf '%s\n' "$claim" | sed -n 's/^claim_token=//p')
"$fabric" work cancel-node cancel-proof running | grep -q '^status=cancel_requested$'
if "$fabric" work complete cancel-proof running worker-a "$token" >/dev/null 2>&1; then
    echo 'cancel-requested node completed normally' >&2
    exit 1
fi
"$fabric" work cancel-node cancel-proof running worker-a "$token" | grep -q '^status=cancelled$'
cancel_history=$("$fabric" work history cancel-proof running | awk '{
    from="";
    to="";
    for (i = 1; i <= NF; ++i) {
        if ($i ~ /^from_status=/) {
            from = substr($i, 13);
        }
        if ($i ~ /^to_status=/) {
            to = substr($i, 11);
        }
    }
    if (from == "running" || from == "cancel_requested") {
        print from " -> " to;
    }
}')
[ "$cancel_history" = "running -> cancel_requested
cancel_requested -> cancelled" ]

"$fabric" mission create mission-cancel >/dev/null
"$fabric" work add mission-cancel a core.identity >/dev/null
"$fabric" work add mission-cancel b core.identity >/dev/null
"$fabric" work cancel-mission mission-cancel >/dev/null
"$fabric" work status mission-cancel a | grep -q '^status=cancelled$'
"$fabric" work status mission-cancel b | grep -q '^status=cancelled$'
