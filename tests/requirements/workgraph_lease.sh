#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
state="${BONFYRE_STATE_DIR:-/tmp/bonfyre-workgraph-lease}/lease-$$"
fabric="$root/programs/bonfyre/bonfyre"
mkdir -p "$state"

make -C "$root/engine/core" >/dev/null
make -C "$root/programs/bonfyre" >/dev/null
export BONFYRE_STATE_DIR="$state"

"$fabric" fabric init >/dev/null
"$fabric" mission create lease-proof >/dev/null
added=$("$fabric" work add lease-proof task core.identity --retries 1 --backoff-base-ms 20)
first=$("$fabric" work claim-next worker-a --lease-ms 80)
token_a=$(printf '%s\n' "$first" | sed -n 's/^claim_token=//p')
deadline=$(printf '%s\n' "$first" | sed -n 's/^lease_expires_at_ms=//p')
receipt_add=$(printf '%s\n' "$added" | sed -n 's/^receipt_id=//p')
receipt_claim_a=$(printf '%s\n' "$first" | sed -n 's/^receipt_id=//p')
test -n "$token_a"

if "$fabric" work claim-next worker-b --lease-ms 30000 >/dev/null 2>&1; then
    echo 'second worker claimed an active lease' >&2
    exit 1
fi
while [ "$(($(date +%s) * 1000))" -le "$deadline" ]; do :; done
expired=$("$fabric" work reap-expired lease-proof)
receipt_expired=$(printf '%s\n' "$expired" | sed -n 's/^receipt_id=//p')
second=$("$fabric" work claim-next worker-b --lease-ms 30000)
token_b=$(printf '%s\n' "$second" | sed -n 's/^claim_token=//p')
receipt_claim_b=$(printf '%s\n' "$second" | sed -n 's/^receipt_id=//p')
test "$token_a" != "$token_b"

for stale in renew complete fail; do
    case "$stale" in
        renew) args="lease-proof task worker-a $token_a --lease-ms 30000" ;;
        complete) args="lease-proof task worker-a $token_a" ;;
        fail) args="lease-proof task worker-a $token_a --class timeout --message stale" ;;
    esac
    if "$fabric" work "$stale" $args >/dev/null 2>&1; then
        echo "stale worker unexpectedly succeeded: $stale" >&2
        exit 1
    fi
done

completed=$("$fabric" work complete lease-proof task worker-b "$token_b")
receipt_complete=$(printf '%s\n' "$completed" | sed -n 's/^receipt_id=//p')
printf '%s\n' "$completed" | grep -q '^status=complete$'

previous=''
for receipt in "$receipt_add" "$receipt_claim_a" "$receipt_expired" "$receipt_claim_b" "$receipt_complete"; do
    shown=$("$fabric" receipt show "$receipt")
    printf '%s\n' "$shown" | grep -F "$token_a" >/dev/null && exit 1
    printf '%s\n' "$shown" | grep -F "$token_b" >/dev/null && exit 1
    printf '%s\n' "$shown" | grep -q '^chain_hash=.'
    if [ -n "$previous" ]; then
        printf '%s\n' "$shown" | grep -q "^previous_receipt_id=$previous$"
    fi
    previous=$receipt
done

transitions='node_added claimed lease_expired claimed completed'
index=1
for receipt in "$receipt_add" "$receipt_claim_a" "$receipt_expired" "$receipt_claim_b" "$receipt_complete"; do
    expected=$(printf '%s\n' "$transitions" | cut -d ' ' -f "$index")
    "$fabric" receipt show "$receipt" | grep -q "\\\"transition\\\":\\\"$expected\\\""
    index=$((index + 1))
done
