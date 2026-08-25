#!/usr/bin/env bash
set -euo pipefail
export PATH="$HOME/.bun/bin:$HOME/.local/bin:$PATH"
export BONFYRE_EXTERNAL_WRITES_ENABLED="0"
export BONFYRE_LIVE_APPLY_ENABLED="0"

# Set ROOT to the strongest candidate printed by recovery.log.
ROOT="${BONFYRE_RECOVERED_ROOT:?Set BONFYRE_RECOVERED_ROOT to the discovered source root}"
cd "$ROOT"

echo "Reviewing source before build..."
test -f build.zig
test -f src/opportunity_workflow_runtime.zig
test -f tools/opportunity-runtime-lib.ts

echo "Building..."
zig build

echo
echo "Start the runtime in a separate terminal:"
echo "  cd '$ROOT' && ./zig-out/bin/bonfyred serve --port 3045"
echo
echo "Then regenerate the local opportunity estate with external writes disabled:"
cat <<'COMMANDS'
BONFYRE_BASE_URL=http://127.0.0.1:3045 \
BONFYRE_EXTERNAL_READS_ENABLED=0 \
bun tools/live-opportunity-drilldown.ts

BONFYRE_BASE_URL=http://127.0.0.1:3045 \
BONFYRE_EXTERNAL_READS_ENABLED=0 \
bun tools/live-compound-opportunity-workflows.ts

BONFYRE_BASE_URL=http://127.0.0.1:3045 \
BONFYRE_EXTERNAL_READS_ENABLED=0 \
bun tools/live-native-form-extraction.ts

BONFYRE_BASE_URL=http://127.0.0.1:3045 \
BONFYRE_EXTERNAL_READS_ENABLED=0 \
bun tools/live-reusable-field-autofill.ts
COMMANDS

echo
echo "Only after the local estate is recovered, rerun live verification with:"
echo "  BONFYRE_EXTERNAL_READS_ENABLED=1 BONFYRE_EXTERNAL_WRITES_ENABLED=0 ..."
