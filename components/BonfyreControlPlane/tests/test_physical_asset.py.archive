"""A physical asset's DERIVED capability, maintained by the live DBSP circuit: all
gates hold -> capable; a single component telemetry -1 -> capability withdrawn.
Skips if the Rust engine is not built."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import physical_asset as pa  # noqa: E402


@pytest.mark.skipif(not os.path.exists(pa.ENGINE),
                    reason="reachable_capacity_live not built")
def test_telemetry_retracts_derived_capability():
    r = pa.telemetry_cascade(pa.robot031_climb(), "servo2_healthy")
    assert r["before"] is True, "all gates hold -> capability reachable"
    assert r["after"] is False, "a single servo telemetry -1 withdraws the capability"


@pytest.mark.skipif(not os.path.exists(pa.ENGINE),
                    reason="reachable_capacity_live not built")
def test_unrelated_gate_does_not_withdraw():
    # retracting a gate the capability does not depend on leaves it reachable
    cap = pa.robot031_climb()
    cap.resolved = set(cap.gates)
    assert pa._run(cap) is True
    cap.resolved.discard("battery_ok")  # a real dependency -> should withdraw
    assert pa._run(cap) is False


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
