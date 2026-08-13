#!/bin/sh
# Proves the Feldera source/sink contract (feldera/readiness_pipeline.sql +
# scripts/bonfyre-feldera-export) is semantically identical to the live
# deterministic readiness adapter, using the deterministic adapter as the
# semantic oracle: real mission state changes -> real NDJSON export via
# the real source-binding script -> an independent Python recomputation of
# the same aggregates feldera/readiness_pipeline.sql's views define ->
# exact match against the live `readiness` CLI output.
#
# This proves "deterministic readiness result A == independently
# recomputed result A'" for the exact computation Feldera's SQL program
# defines, without a running Feldera instance. The only remaining
# unverified step is swapping the independent Python recomputation for
# real Feldera incremental execution once local disk capacity allows it
# (feldera.local_image_pull=capacity_pending).
set -eu

. "$(dirname -- "$0")/fabric_test_lib.sh"
fabric_test_bootstrap

mission="m-feldera-parity"
"$requirement_fabric" mission create "$mission" >/dev/null
"$requirement_fabric" work add "$mission" node-a command.hash >/dev/null
"$requirement_fabric" work add "$mission" node-b command.hash >/dev/null

echo "== induce real state: claim one node (running), leave the other ready =="
"$requirement_fabric" work claim "$mission" node-a worker-parity --lease-ms 3600000 >/dev/null

echo "== induce a real effect backlog entry =="
"$requirement_fabric" effect request "$mission" derive-file "bonfyre://local/parity-test.txt" >/dev/null

echo "== real source export via the real binding script =="
export_dir="$requirement_runtime/feldera-export"
"$requirement_root/scripts/bonfyre-feldera-export" "$requirement_db" "$export_dir" >/dev/null

echo "== live readiness (the deterministic oracle) =="
live_mission=$("$requirement_fabric" readiness mission "$mission")
live_effect_backlog=$("$requirement_fabric" readiness effect-backlog "$mission")

echo "== independent recomputation from the exported NDJSON, mirroring feldera/readiness_pipeline.sql =="
python3 - "$export_dir" "$mission" "$live_mission" "$live_effect_backlog" <<'PY'
import json
import sys

export_dir, mission, live_mission_json, live_effect_json = sys.argv[1:5]

def load(name):
    rows = []
    with open(f"{export_dir}/{name}.ndjson") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line)["insert"])
    return rows

nodes = [r for r in load("workgraph_nodes") if r["mission_id"] == mission]
effects = [r for r in load("effects") if r["mission_id"] == mission]

# Mirrors bf_readiness_mission in feldera/readiness_pipeline.sql exactly.
recomputed_mission = {
    "mission_id": mission,
    "total_nodes": len(nodes),
    "complete_nodes": sum(1 for n in nodes if n["status"] == "complete"),
    "blocked_nodes": sum(1 for n in nodes if n["status"] == "blocked"),
    "failed_nodes": sum(1 for n in nodes if n["status"] == "failed"),
    "running_nodes": sum(1 for n in nodes if n["status"] == "running"),
}

# Mirrors bf_readiness_effect_backlog exactly.
backlog_count = sum(1 for e in effects if e["state"] not in ("committed", "compensated"))
recomputed_effect_backlog = {"mission_id": mission, "backlog_count": backlog_count} if backlog_count else None

live_mission = json.loads(live_mission_json)["rows"]
live_effect_backlog = json.loads(live_effect_json)["rows"]

assert len(live_mission) == 1, f"expected exactly 1 live mission row, got {live_mission}"
assert live_mission[0] == recomputed_mission, (
    f"parity mismatch: live={live_mission[0]!r} recomputed={recomputed_mission!r}"
)

assert recomputed_effect_backlog is not None, "expected a real effect backlog entry from the induced effect request"
assert len(live_effect_backlog) == 1, f"expected exactly 1 live effect-backlog row, got {live_effect_backlog}"
assert live_effect_backlog[0] == recomputed_effect_backlog, (
    f"parity mismatch: live={live_effect_backlog[0]!r} recomputed={recomputed_effect_backlog!r}"
)

print(f"parity confirmed: mission={recomputed_mission}")
print(f"parity confirmed: effect_backlog={recomputed_effect_backlog}")
PY

echo 'feldera source/sink parity: passed'
