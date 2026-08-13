#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
state="${BONFYRE_STATE_DIR:-/tmp/bonfyre-workgraph-compensation}/compensation-$$"
fabric="$root/programs/bonfyre/bonfyre"
mkdir -p "$state"
export BONFYRE_STATE_DIR="$state/runtime"

make -C "$root/engine/core" >/dev/null
make -C "$root/programs/bonfyre" >/dev/null
"$fabric" fabric init >/dev/null

printf 'prepared payload\n' > "$state/prepared-input.txt"
prepared_target="$state/prepared-target.txt"
"$fabric" mission create rollback-proof >/dev/null
"$fabric" work add rollback-proof task core.identity >/dev/null
claim=$("$fabric" work claim rollback-proof task worker-a --lease-ms 30000)
execution_token=$(printf '%s\n' "$claim" | sed -n 's/^claim_token=//p')
"$fabric" work effect-prepare rollback-proof task worker-a "$execution_token" \
    effect-a "$prepared_target" --adapter derive-file \
    --input-artifact-uri "$state/prepared-input.txt" \
    --verification-policy sha256 --rollback-contract remove-created-target \
    --authority worker-a >/dev/null
[ ! -e "$prepared_target" ]
effect=$($fabric work effect-status rollback-proof task effect-a)
printf '%s\n' "$effect" | grep -q '^adapter_id=derive-file$'
printf '%s\n' "$effect" | grep -q "^target_uri=$prepared_target$"
printf '%s\n' "$effect" | grep -q '^effect_state=prepared$'
prepared_state=$(printf '%s\n' "$effect" | sed -n 's/^prepared_state=//p')
[ -f "$prepared_state" ]
"$fabric" work cancel-node rollback-proof task >/dev/null
compensation_claim=$("$fabric" work compensation-claim rollback-worker \
    --mission rollback-proof --lease-ms 30000)
compensation_token=$(printf '%s\n' "$compensation_claim" | sed -n 's/^claim_token=//p')
"$fabric" work compensate rollback-proof task rollback-worker \
    "$compensation_token" effect-a --result success | grep -q '^status=cancelled$'
[ ! -e "$prepared_state" ]
[ ! -e "$prepared_target" ]
"$fabric" work effect-status rollback-proof task effect-a | grep -q '^effect_state=rolled_back$'

printf 'committed payload\n' > "$state/committed-input.txt"
committed_target="$state/committed-target.txt"
"$fabric" mission create compensate-proof >/dev/null
"$fabric" work add compensate-proof task core.identity >/dev/null
claim=$("$fabric" work claim compensate-proof task execution-worker --lease-ms 80)
execution_token=$(printf '%s\n' "$claim" | sed -n 's/^claim_token=//p')
execution_deadline=$(printf '%s\n' "$claim" | sed -n 's/^lease_expires_at_ms=//p')
"$fabric" work effect-prepare compensate-proof task execution-worker "$execution_token" \
    effect-b "$committed_target" --adapter publish-local \
    --input-artifact-uri "$state/committed-input.txt" \
    --verification-policy sha256 --rollback-contract remove-created-target \
    --authority execution-worker >/dev/null
"$fabric" work effect-commit compensate-proof task execution-worker \
    "$execution_token" effect-b >/dev/null
cmp "$state/committed-input.txt" "$committed_target"
"$fabric" work effect-status compensate-proof task effect-b | grep -q '^effect_state=committed$'
"$fabric" work cancel-node compensate-proof task | grep -q '^status=cancel_requested$'

# Every command starts a new process. Wait for the persisted execution deadline, then
# restart through the public reaper and claim the independent compensation lease.
while [ "$(($(date +%s) * 1000))" -le "$execution_deadline" ]; do :; done
"$fabric" work reap-expired compensate-proof | grep -q '^status=cancelled$'
if "$fabric" work complete compensate-proof task execution-worker \
    "$execution_token" >/dev/null 2>&1; then
    echo 'expired execution worker completed after compensation recovery began' >&2
    exit 1
fi
compensation_claim=$("$fabric" work compensation-claim compensation-worker \
    --mission compensate-proof --lease-ms 30000)
compensation_token=$(printf '%s\n' "$compensation_claim" | sed -n 's/^claim_token=//p')
printf '%s\n' "$compensation_claim" | grep -q '^effect_id=effect-b$'
compensated=$("$fabric" work compensate compensate-proof task compensation-worker \
    "$compensation_token" effect-b --result success)
printf '%s\n' "$compensated" | grep -q '^status=cancelled$'
[ ! -e "$committed_target" ]
"$fabric" work effect-status compensate-proof task effect-b | grep -q '^effect_state=compensated$'

# The same fenced completion is idempotent and does not run the adapter twice.
"$fabric" work compensate compensate-proof task compensation-worker \
    "$compensation_token" effect-b --result success | grep -q '^status=cancelled$'
[ ! -e "$committed_target" ]

for receipt in \
    "$(printf '%s\n' "$compensation_claim" | sed -n 's/^receipt_id=//p')" \
    "$(printf '%s\n' "$compensated" | sed -n 's/^receipt_id=//p')"; do
    shown=$("$fabric" receipt show "$receipt")
    if printf '%s\n' "$shown" | grep -F "$execution_token" >/dev/null; then
        echo 'raw execution token leaked into compensation evidence' >&2
        exit 1
    fi
    if printf '%s\n' "$shown" | grep -F "$compensation_token" >/dev/null; then
        echo 'raw compensation token leaked into evidence' >&2
        exit 1
    fi
done
