"""The full physical-asset operational loop: telemetry -1 withdraws a derived
capability, opens maintenance, and repair (+1) recovers it. Skips if the DBSP
engine is not built."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import physical_asset as pa  # noqa: E402
import fleet_operations as fo  # noqa: E402


@pytest.mark.skipif(not os.path.exists(pa.ENGINE),
                    reason="reachable_capacity_live not built")
def test_full_loop_withdraw_maintain_recover():
    r = fo.operate(pa.robot031_climb(), "servo2_healthy", "servo-2 (S-88419)")
    assert r["healthy"] is True
    assert r["degraded"] is False, "telemetry -1 must withdraw the capability"
    assert r["recovered"] is True, "repair (+1) must recover the capability"
    t = r["maintenance_ticket"]
    assert t is not None, "capability loss must open a maintenance demand"
    assert t["raised_by"] == "robot031"
    assert "servo-2" in t["subject"]


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
