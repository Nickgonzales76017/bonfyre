#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
state="${BONFYRE_STATE_DIR:-/tmp/bonfyre-workgraph-retry}/retry-$$"
fabric="$root/programs/bonfyre/bonfyre"
mkdir -p "$state"

make -C "$root/engine/core" >/dev/null
make -C "$root/programs/bonfyre" >/dev/null
export BONFYRE_STATE_DIR="$state"
"$fabric" fabric init >/dev/null

"$fabric" mission create retry-success >/dev/null
"$fabric" work add retry-success task core.identity --retries 1 --backoff-base-ms 20 --backoff-max-ms 20 >/dev/null
claim=$("$fabric" work claim-next worker-a --lease-ms 30000)
token=$(printf '%s\n' "$claim" | sed -n 's/^claim_token=//p')
failed=$("$fabric" work fail retry-success task worker-a "$token" --class transient --message unavailable)
deadline=$(printf '%s\n' "$failed" | sed -n 's/^next_attempt_at_ms=//p')
printf '%s\n' "$failed" | grep -q '^status=retry_wait$'
if "$fabric" work claim-next worker-b --lease-ms 30000 >/dev/null 2>&1; then
    echo 'retry was claimable before its persisted deadline' >&2
    exit 1
fi
while [ "$(($(date +%s) * 1000))" -lt "$deadline" ]; do :; done
claim=$("$fabric" work claim-next worker-b --lease-ms 30000)
token=$(printf '%s\n' "$claim" | sed -n 's/^claim_token=//p')
complete=$("$fabric" work complete retry-success task worker-b "$token")
printf '%s\n' "$complete" | grep -q '^attempt=2$'
printf '%s\n' "$complete" | grep -q '^status=complete$'

retry_history=$("$fabric" work history retry-success task | awk '{
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
    if (from == "ready" || from == "running" || from == "retry_wait") {
        print from " -> " to;
    }
}')
[ "$retry_history" = "ready -> running
running -> retry_wait
retry_wait -> running
running -> complete" ]

"$fabric" mission create retry-exhausted >/dev/null
"$fabric" work add retry-exhausted task core.identity --retries 1 --backoff-base-ms 20 --backoff-max-ms 20 >/dev/null
claim=$("$fabric" work claim retry-exhausted task worker-a --lease-ms 30000)
token=$(printf '%s\n' "$claim" | sed -n 's/^claim_token=//p')
failed=$("$fabric" work fail retry-exhausted task worker-a "$token" --class resource --message pressure)
deadline=$(printf '%s\n' "$failed" | sed -n 's/^next_attempt_at_ms=//p')
while [ "$(($(date +%s) * 1000))" -lt "$deadline" ]; do :; done
claim=$("$fabric" work claim retry-exhausted task worker-b --lease-ms 30000)
token=$(printf '%s\n' "$claim" | sed -n 's/^claim_token=//p')
dead=$("$fabric" work fail retry-exhausted task worker-b "$token" --class timeout --message exhausted)
printf '%s\n' "$dead" | grep -q '^attempt=2$'
printf '%s\n' "$dead" | grep -q '^status=dead_letter$'
if "$fabric" work claim retry-exhausted task worker-c >/dev/null 2>&1; then
    echo 'dead-lettered node was claimable' >&2
    exit 1
fi

"$fabric" mission create retry-permanent >/dev/null
"$fabric" work add retry-permanent task core.identity --retries 5 >/dev/null
claim=$("$fabric" work claim retry-permanent task worker-a)
token=$(printf '%s\n' "$claim" | sed -n 's/^claim_token=//p')
permanent=$("$fabric" work fail retry-permanent task worker-a "$token" --class permanent --message invalid)
printf '%s\n' "$permanent" | grep -q '^status=dead_letter$'
printf '%s\n' "$permanent" | grep -q '^next_attempt_at_ms=0$'
