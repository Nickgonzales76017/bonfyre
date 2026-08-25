"""The full physical-asset operational loop, Bonfyre-native, end to end.

Beyond the capability withdrawal already proven: a servo telemetry -1 does not just
retract a capability -- it opens the maintenance cascade (a Helpdesk ticket, a spare
requirement) and, when the component is replaced (+1), the capability RECOVERS. All
on the real machinery: native Occurrence (telemetry), the live DBSP circuit (derived
capability), and native app records (HD Ticket in Helpdesk's real DocType grammar).
No external hardware is required to prove the loop; when real OpenCat/Kratos connect
at the Provider boundary, the same code runs.

  servo telemetry -1  (native Occurrence)
  -> derived capability withdrawn  (DBSP)
  -> maintenance HD Ticket opens   (native app record)
  -> servo replaced +1
  -> derived capability RECOVERS   (DBSP)
"""

from __future__ import annotations

import physical_asset as pa


def maintenance_ticket(robot: str, component: str, reason: str) -> dict:
    """A maintenance demand shaped by Helpdesk's real HD Ticket DocType (no bench)."""
    import frappe_native_records as fnr
    fields = fnr.doctype_fields("helpdesk", "HD Ticket")
    src = {
        "subject": f"{robot}: replace {component}",
        "raised_by": robot,
        "status": "Open",
        "ticket_type": "Maintenance",
        "priority": "High",
        "description": f"Derived capability withdrawn: {reason}. Replace {component}.",
    }
    rec = {f: src.get(f, "") for f in fields} if fields else dict(src)
    rec["_bonfyre_ref"] = f"maintenance:{robot}:{component}"
    return rec


def operate(robot_cap: pa.DerivedCapability, failing_gate: str, component: str) -> dict:
    """Run the full loop: fail -> withdraw -> maintenance -> repair -> recover."""
    robot = robot_cap.capability.split(".")[0]

    # 1. healthy: all gates hold -> capable
    robot_cap.resolved = set(robot_cap.gates)
    healthy = pa._run(robot_cap)

    # 2. servo telemetry -1 -> capability withdrawn
    robot_cap.resolved.discard(failing_gate)
    degraded = pa._run(robot_cap)

    # 3. the withdrawal opens a maintenance demand (Helpdesk grammar)
    ticket = maintenance_ticket(robot, component,
                                f"{failing_gate} retracted by telemetry") if (healthy and not degraded) else None

    # 4. component replaced (+1) -> capability recovers
    robot_cap.resolved.add(failing_gate)
    recovered = pa._run(robot_cap)

    return {"robot": robot, "capability": robot_cap.capability,
            "healthy": healthy, "degraded": degraded, "recovered": recovered,
            "maintenance_ticket": ticket}


if __name__ == "__main__":
    r = operate(pa.robot031_climb(), "servo2_healthy", "servo-2 (S-88419)")
    print(f"asset: {r['robot']}  capability: {r['capability']}")
    print(f"  healthy   -> {r['healthy']}")
    print(f"  telemetry -1 -> capable = {r['degraded']}  (withdrawn)")
    t = r["maintenance_ticket"]
    print(f"  maintenance opened: {t['subject']!r} priority={t.get('priority')} "
          f"ref={t['_bonfyre_ref']}")
    print(f"  component replaced +1 -> capable = {r['recovered']}  (recovered)")
    assert r["healthy"] and not r["degraded"] and r["recovered"], "full loop must hold"
    assert r["maintenance_ticket"], "capability loss must open a maintenance demand"
    print("FLEET OPERATIONS LOOP: PASS (telemetry -1 -> withdraw -> maintenance -> repair -> recover)")
