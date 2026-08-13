#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
state="${BONFYRE_STATE_DIR:-/tmp/bonfyre-workgraph-reap-multi}/reap-$$"
fabric="$root/programs/bonfyre/bonfyre"
mkdir -p "$state"

make -C "$root/engine/core" >/dev/null
make -C "$root/programs/bonfyre" >/dev/null
export BONFYRE_STATE_DIR="$state"
"$fabric" fabric init >/dev/null

latest_deadline=0
for mission in reap-alpha reap-beta; do
    "$fabric" mission create "$mission" >/dev/null
    "$fabric" work add "$mission" parent core.identity --retries 0 >/dev/null
    "$fabric" work add "$mission" child core.identity >/dev/null
    "$fabric" work dependency "$mission" child parent --policy continue >/dev/null
    claim=$("$fabric" work claim "$mission" parent "worker-$mission" --lease-ms 80)
    deadline=$(printf '%s\n' "$claim" | sed -n 's/^lease_expires_at_ms=//p')
    if [ "$deadline" -gt "$latest_deadline" ]; then
        latest_deadline=$deadline
    fi
done

while [ "$(($(date +%s) * 1000))" -le "$latest_deadline" ]; do :; done
"$fabric" work reap-expired >/dev/null

for mission in reap-alpha reap-beta; do
    "$fabric" work status "$mission" parent | grep -q '^status=dead_letter$'
    "$fabric" work status "$mission" child | grep -q '^status=ready$'
    "$fabric" mission show "$mission" | grep -q '^status=running$'
    history=$("$fabric" work history "$mission" parent | awk '{
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
        if (from == "ready" || from == "running") {
            print from " -> " to;
        }
    }')
    [ "$history" = "ready -> running
running -> dead_letter" ]
done
