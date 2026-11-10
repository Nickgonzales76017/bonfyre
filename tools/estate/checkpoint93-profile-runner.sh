#!/usr/bin/env bash
set -u
IFS=$'\n\t'

PROFILE="${1:-}"
ROOT=/opt/bonfyre
BUN=/home/ubuntu/.bun/bin/bun
RECEIPT_HELPER="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)/checkpoint93-profile-receipt.mjs"
OUTPUT="$ROOT/output/phase94-hypercycle"
PROFILE_DIR="$OUTPUT/$PROFILE"
RECEIPTS="$OUTPUT/profile-run-receipts.jsonl"

mkdir -p "$PROFILE_DIR"
touch "$RECEIPTS"
# When this run began, so the failure summary reports this run rather than
# the receipt file's whole history.
RUN_STAMP="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

export BONFYRE_EXTERNAL_READS_ENABLED=1
export BONFYRE_EXTERNAL_WRITES_ENABLED=0
export BONFYRE_LIVE_APPLY_ENABLED=0
export BONFYRE_HUMAN_INTERRUPTS_FOR_READS=0

# Optional internal activation remains bounded to reversible/internal effects.
export BONFYRE_INTERNAL_ACTIVE="${BONFYRE_INTERNAL_ACTIVE:-0}"
export BONFYRE_REVERSIBLE_INTERNAL_WRITES_ENABLED="$BONFYRE_INTERNAL_ACTIVE"
export BONFYRE_AUTHORITATIVE_INTERNAL_WRITES_ENABLED=0
export BONFYRE_EXTERNAL_REVERSIBLE_EFFECTS_ENABLED=0
export BONFYRE_EXTERNAL_IRREVERSIBLE_EFFECTS_ENABLED=0

tool_list() {
  case "$PROFILE" in
    skill-promotion)
      cat <<'EOF'
live-promotion-credit-autonomy-total-cycle.ts
live-skill-lab-flow.ts
EOF
      ;;
    provider-credit)
      cat <<'EOF'
live-provider-budget-ledger.ts
live-real-value-provider-budget-measurements.ts
live-cloud-provider-readiness.ts
EOF
      ;;
    provider-market)
      cat <<'EOF'
live-provider-workload-router.ts
live-provider-call-firehose.ts
live-provider-aggression-runtime.ts
live-provider-aggression-truth-learning.ts
EOF
      ;;
    lambda-model)
      cat <<'EOF'
live-lambda-tensor-model-fusion.ts
live-lambda-tensor-kv-cache.ts
live-lambda-tensor-compression.ts
live-lambda-tensor-max-cycle.ts
EOF
      ;;
    resource-activation)
      cat <<'EOF'
live-provider-resource-harvest.ts
live-resource-activation-max.ts
EOF
      ;;
    api-publicgap)
      cat <<'EOF'
live-fused-api-shoutout-publicgap.ts
live-api-twin-max-cycle.ts
EOF
      ;;
    measured-finance)
      cat <<'EOF'
live-real-value-model-quorum.ts
live-measured-finance-routes.ts
live-real-value-lambda-tensor-efficiency.ts
EOF
      ;;
    cloud-placement)
      cat <<'EOF'
live-cloud-env-readiness.ts
live-cloud-provider-readiness.ts
live-worker-cloud-lane-selection.ts
live-cloud-fabric-runtime.ts
EOF
      ;;
    workcell-swarm)
      cat <<'EOF'
live-autonomous-worker-mesh.ts
live-autonomous-material-first-run.ts
EOF
      ;;
    graph-closure)
      cat <<'EOF'
live-institutional-closure-flow.ts
live-provider-powered-closure-expansion.ts
live-command-capability-closure.ts
EOF
      ;;
    truth-replay)
      cat <<'EOF'
live-lambda-tensor-truth-replay.ts
live-oracle-autonomous-loop-smoke.ts
EOF
      ;;
    media-shoutout)
      cat <<'EOF'
live-fused-api-shoutout-publicgap.ts
live-skill-lab-flow.ts
EOF
      ;;
    release-attestation)
      return 0
      ;;
    *)
      echo "Unknown profile: $PROFILE" >&2
      return 2
      ;;
  esac
}

append_receipt() {
  local tool="$1"
  local rc="$2"
  local started="$3"
  local completed="$4"
  local duration="$5"
  local state="$6"
  local log_path="$7"

  "$BUN" "$RECEIPT_HELPER" append "$RECEIPTS" "$PROFILE" "$tool" "$rc" \
    "$started" "$completed" "$duration" "$state" "$log_path"
}

release_attestation() {
  local log_file="$PROFILE_DIR/release-attestation-$(date -u +%Y%m%dT%H%M%SZ).log"

  {
    set -e

    CURRENT="$(readlink -f /opt/bonfyre/releases/command-plane-current)"
    test -d "$CURRENT"

    ELF_COUNT="$(
      find "$CURRENT/cmd" \
        -type f \
        -perm -111 \
        -print0 |
      xargs -0 file |
      grep -c 'ELF 64-bit LSB'
    )"

    test "$ELF_COUNT" -eq 93

    find "$CURRENT" \
      -type f \
      -exec sha256sum {} + \
      >/tmp/phase94-current-sha256.txt

    if [ -f "$CURRENT/sha256-manifest.txt" ]; then
      (
        cd "$CURRENT"
        sha256sum -c sha256-manifest.txt
      ) || true
    fi

    file /opt/bonfyre/bonfyred
    systemctl is-active bonfyred.service
    systemctl is-active bonfyre-continuation-loop.timer
    systemctl is-active bonfyre-autonomous-worker-loop.timer
    curl -fsS http://127.0.0.1:3045/health

    echo "current=$CURRENT"
    echo "command_elfs=$ELF_COUNT"
  } >"$log_file" 2>&1

  local rc=$?
  local state=passed
  [ "$rc" -eq 0 ] || state=failed

  append_receipt \
    release-attestation \
    "$rc" \
    "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    0 \
    "$state" \
    "$log_file"

  return "$rc"
}

if [ "$PROFILE" = release-attestation ]; then
  release_attestation
  exit $?
fi

overall=0

while IFS= read -r tool; do
  [ -n "$tool" ] || continue

  path="$ROOT/tools/$tool"
  started_epoch="$(date +%s)"
  started="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  run_id="$(date -u +%Y%m%dT%H%M%SZ)"
  log_file="$PROFILE_DIR/${tool%.ts}-${run_id}.log"

  if [ ! -f "$path" ]; then
    completed="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    append_receipt \
      "$tool" \
      127 \
      "$started" \
      "$completed" \
      0 \
      missing_tool \
      "$log_file"
    continue
  fi

  (
    cd "$ROOT"
    timeout 3600 "$BUN" "$path"
  ) >"$log_file" 2>&1

  rc=$?
  completed_epoch="$(date +%s)"
  completed="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  duration=$((completed_epoch - started_epoch))

  if [ "$rc" -eq 0 ]; then
    state=passed
  elif [ "$rc" -eq 124 ]; then
    state=timeout
    overall=1
  else
    state=failed
    overall=1
  fi

  append_receipt \
    "$tool" \
    "$rc" \
    "$started" \
    "$completed" \
    "$duration" \
    "$state" \
    "$log_file"
done < <(tool_list)

# Say why, where systemd can see it.
#
# Everything above goes to per-tool log files, so a failing profile exited 1 with
# an empty journal: "Main process exited, code=exited, status=1/FAILURE" and not
# one word about which of three tools failed or what it said. Three units sat in
# `failed`, and the only way to learn anything was to already know these receipts
# existed and where.
#
# One line per failing tool of this run, with its own last error, on stderr.
if [ "$overall" -ne 0 ]; then
  echo "profile $PROFILE failed; tools that did not pass:" >&2
  "$BUN" "$RECEIPT_HELPER" summary "$RECEIPTS" "$PROFILE" "$RUN_STAMP" >&2
fi

exit "$overall"
