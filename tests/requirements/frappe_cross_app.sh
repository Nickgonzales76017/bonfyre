#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
compiler="$root/cmd/BonfyreFrappeCompiler/bonfyre-frappe-compiler"
output=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-frappe-cross-app.XXXXXX")
trap 'rm -rf "$output"' EXIT

test -x "$compiler"
for family in frappe erpnext crm hrms helpdesk lms wiki drive insights; do
  "$compiler" --phase 1 -r "$root" --emit-pack "$output/$family.apppack.json" \
    "$root/integrations/frappe-bench/apps/$family" >"$output/$family.log"
  python3 - "$output/$family.apppack.json" <<'PY'
import json, sys
pack = json.load(open(sys.argv[1]))
if pack.get("kind") != "AppPack":
    raise SystemExit("not an AppPack")
if not pack.get("source_revision"):
    raise SystemExit("AppPack source revision is absent")
PY
done

# A pack is deliberately insufficient: the native cross-app executor must
# create linked application records and a Fabric receipt.  Keep this assertion
# concrete so the acceptance report names the missing runtime behavior rather
# than failing because this test file was absent.
runtime="$root/programs/bonfyre/bonfyre"
make -C "$root/engine/core" >/dev/null
make -C "$root/programs/bonfyre" >/dev/null

# The first worker completes four domain nodes, claims the fifth, and waits.
# Killing that process leaves a real persisted lease for restart reclamation.
"$runtime" app cross-transition "$output" --lease-ms 80 --hold-after 4 \
  >"$output/interrupted.out" 2>"$output/interrupted.err" &
worker_pid=$!
while kill -0 "$worker_pid" 2>/dev/null && ! grep -q '^node_claimed=step-05$' "$output/interrupted.out" 2>/dev/null; do :; done
grep -q '^node_claimed=step-05$' "$output/interrupted.out" || {
  cat "$output/interrupted.err" >&2
  echo 'cross-app worker did not reach the interruption fence' >&2
  exit 1
}
mission=$(sed -n 's#^mission=bonfyre://mission/##p' "$output/interrupted.out")
deadline=$(sed -n 's/^lease_expires_at_ms=//p' "$output/interrupted.out")
kill -9 "$worker_pid"
wait "$worker_pid" 2>/dev/null || true
while [ "$(($(date +%s) * 1000))" -le "$deadline" ]; do :; done

if "$runtime" app cross-transition "$output" --mission "$mission" --lease-ms 30000 \
  >"$output/transition.out" 2>"$output/transition.err"; then
  grep -q '^receipt=bonfyre://receipt/' "$output/transition.out" || {
    echo 'cross-app executor returned without a receipt' >&2; exit 1;
  }
  test -n "$mission"
  db="$BONFYRE_STATE_DIR/fabric.db"
  sqlite3 "$db" "SELECT count(*) = 11 FROM application_records WHERE record_id LIKE '$mission-%';" | grep -qx 1
  sqlite3 "$db" "SELECT count(DISTINCT family) = 9 FROM application_records WHERE record_id LIKE '$mission-%';" | grep -qx 1
  # The Frappe platform context is the sole root; every transition node links
  # to its predecessor and carries the common receipt and event.
  sqlite3 "$db" "SELECT count(*) = 10 FROM application_records WHERE record_id LIKE '$mission-%' AND receipt_id IS NOT NULL AND event_id IS NOT NULL AND parent_uri IS NOT NULL;" | grep -qx 1
  sqlite3 "$db" "SELECT count(*) = 11 FROM workgraph_nodes WHERE mission_id='$mission' AND status='complete';" | grep -qx 1
  sqlite3 "$db" "SELECT count(*) = 1 FROM workgraph_transitions WHERE mission_id='$mission' AND node_id='step-05' AND from_status='running' AND to_status='retry_wait';" | grep -qx 1
  sqlite3 "$db" "SELECT count(*) = 1 FROM application_records WHERE record_id='$mission-11' AND family='insights' AND record_type='AnalyticalProjection';" | grep -qx 1
  sqlite3 "$db" "SELECT status = 'complete' AND workgraph_cursor = 'terminal' FROM missions WHERE id='$mission';" | grep -qx 1
else
  cat "$output/transition.err" >&2
  echo 'native cross-app executor is unavailable' >&2
  exit 1
fi
