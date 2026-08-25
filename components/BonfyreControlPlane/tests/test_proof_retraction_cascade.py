"""The proof-retraction cascade runs through the real DBSP circuit: challenging a
proof withdraws the reachable opportunity, incrementally. Skips if the Rust
circuit is not built."""

import os
import sqlite3
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import proof_retraction_cascade as prc  # noqa: E402


@pytest.mark.skipif(not os.path.exists(prc.LIVE),
                    reason="reachable_capacity_live not built (cargo build --release)")
def test_proof_retraction_withdraws_reachable():
    r = prc.witness(sqlite3.connect(":memory:"))
    assert prc.OPP in r["before"].get("reachable", []), "proven -> reachable"
    assert prc.OPP not in r["after"].get("reachable", []), "challenged -> withdrawn (-1)"
    assert r["before"]["count"] == 1 and r["after"]["count"] == 0


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
