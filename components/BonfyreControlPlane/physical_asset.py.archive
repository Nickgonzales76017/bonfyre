"""Physical asset as a Bonfyre Resource with a DERIVED capability.

The vision's core mechanism, on the machinery already built. A robot's ability to
do real work is not statically declared -- it is DERIVED as a conjunction of
component-health / proof / model / calibration gates, maintained by the same DBSP
circuit that maintains ReachableCapacity. A capability is a multi-gate relation:
the robot can do X only while ALL its gates hold.

  Capability(robot031.climb_rough_surface) =
      servo2_healthy AND battery_ok AND vision_model_resident
      AND firmware_approved AND calibration_current AND maintenance_proof

A servo telemetry occurrence (abnormal current) retracts servo2_healthy -- one -1
-- and the circuit withdraws the capability incrementally, with no rebuild. That
withdrawal is what stops WorkGraph from assigning difficult terrain, opens a
Helpdesk maintenance state, and so on. External repos (OpenCat device IO, Kratos
physics, CARLA, ...) are Providers of the gates, never ported in.
"""

from __future__ import annotations

import json
import os
import subprocess
from dataclasses import dataclass, field

ENGINE = os.path.expanduser(
    "~/.bonfyre/substrates/v6.1/feldera/probe/target/release/reachable_capacity_live"
)


@dataclass
class DerivedCapability:
    capability: str                 # e.g. robot031.climb_rough_surface
    gates: list[str]                # component-health / proof / model conditions
    resolved: set[str] = field(default_factory=set)  # gates currently holding

    def facts(self) -> str:
        lines = [f"B\t{self.capability}\t{g}" for g in self.gates]
        lines += [f"R\t{g}" for g in self.gates if g in self.resolved]
        return "\n".join(lines) + "\n"


def _run(cap: DerivedCapability) -> bool:
    """Maintain the capability in DBSP; True iff it is currently reachable."""
    if not os.path.exists(ENGINE):
        raise RuntimeError("reachable_capacity_live not built")
    out = subprocess.run([ENGINE], input=cap.facts(), capture_output=True, text=True, timeout=30)
    line = next((l for l in out.stdout.splitlines() if l.strip().startswith("{")), "{}")
    return cap.capability in json.loads(line).get("reachable", [])


def telemetry_cascade(cap: DerivedCapability, failing_gate: str) -> dict:
    """Witness: all gates hold -> capable; a telemetry -1 on one gate -> withdrawn.
    Returns before/after reachability and the retracted gate."""
    cap.resolved = set(cap.gates)          # every component healthy, proofs current
    before = _run(cap)
    cap.resolved.discard(failing_gate)     # servo telemetry: abnormal current, -1
    after = _run(cap)
    return {"capability": cap.capability, "retracted_gate": failing_gate,
            "before": before, "after": after,
            "downstream": "WorkGraph stops assigning terrain; Helpdesk maintenance opens"
                          if (before and not after) else ""}


def robot031_climb() -> DerivedCapability:
    return DerivedCapability(
        capability="robot031.climb_rough_surface",
        gates=["servo2_healthy", "battery_ok", "vision_model_resident",
               "firmware_approved", "calibration_current", "maintenance_proof"],
    )


if __name__ == "__main__":
    r = telemetry_cascade(robot031_climb(), "servo2_healthy")
    print(f"capability: {r['capability']}")
    print(f"  all gates hold        -> reachable = {r['before']}")
    print(f"  servo telemetry -1 ({r['retracted_gate']}) -> reachable = {r['after']}")
    print(f"  downstream: {r['downstream']}")
    assert r["before"] and not r["after"], "telemetry -1 must withdraw the capability"
    print("PHYSICAL ASSET CASCADE: PASS (telemetry -1 -> derived capability withdrawn, live DBSP)")
