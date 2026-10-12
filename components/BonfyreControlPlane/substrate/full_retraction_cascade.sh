#!/usr/bin/env bash
# End-to-end retraction cascade witness (mandate SS2 + SS3), through REAL components:
#
#   Evidence -1        -> proof retracts        (native C: retraction_cascade_test)
#   ProofFrontier -1   -> ReachableCapacity -1  (live DBSP: reachable_capacity_live)
#   provider/reach -1  -> route withdrawn       (DBSP: route_set_daemon)
#
# No manual synchronization: each stage maintains its consequence incrementally.
# Skips a stage cleanly if its Rust binary is not built.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
CP="$(cd "$HERE/.." && pwd)"
CORE="$(cd "$CP/../../engine/core" && pwd)"
PROBE="$HOME/.bonfyre/substrates/v6.1/feldera/probe/target/release"

pass=0

# --- link 1: Evidence -1 -> proof retracts (native) ---
if [ -x "$CORE/obj/retraction_cascade_test" ] || (cd "$CORE" && make obj/retraction_cascade_test >/dev/null 2>&1); then
  if "$CORE/obj/retraction_cascade_test" | grep -q PASS; then
    echo "link1 Evidence -1 -> proof retracts: PASS"; pass=$((pass+1))
  else echo "link1: FAIL"; exit 1; fi
else echo "link1: skipped (native test not built)"; fi

# --- link 2: ProofFrontier -1 -> ReachableCapacity -1 (live DBSP) ---
if [ -x "$PROBE/reachable_capacity_live" ]; then
  if (cd "$CP" && python3 proof_retraction_cascade.py | grep -q "CASCADE: PASS"); then
    echo "link2 ProofFrontier -1 -> ReachableCapacity -1: PASS"; pass=$((pass+1))
  else echo "link2: FAIL"; exit 1; fi
else echo "link2: skipped (reachable_capacity_live not built)"; fi

# --- link 3: provider -1 -> route withdrawn (DBSP) ---
if [ -x "$PROBE/route_set_daemon" ]; then
  out=$(printf 'A\tfpq-generate\tprovider-mac\n+\tprovider-mac\n-\tprovider-mac\n' | "$PROBE/route_set_daemon")
  up=$(echo "$out"   | grep '"delta":"up"'   | grep -o '"count":[0-9]*' | grep -o '[0-9]*')
  down=$(echo "$out" | grep '"delta":"down"' | grep -o '"count":[0-9]*' | grep -o '[0-9]*')
  if [ "$up" = "1" ] && [ "$down" = "0" ]; then
    echo "link3 provider -1 -> route withdrawn: PASS (up=$up down=$down)"; pass=$((pass+1))
  else echo "link3: FAIL (up=$up down=$down)"; exit 1; fi
else echo "link3: skipped (route_set_daemon not built)"; fi

echo "FULL RETRACTION CASCADE: $pass/3 links proven (Evidence -1 -> proof -1 -> reachable -1 -> route -1)"
