"""The economic estate: money flows, commitments (not deliveries), non-cash value
(never dollars), and projected costs (not booked)."""

import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import economic_graphs as ecg


def _db():
    db = sqlite3.connect(":memory:")
    ecg.ensure_schema(db)
    return db


def test_money_edges_are_typed():
    db = _db()
    ecg.record_money(db, src="nsf", relation="grants", dst="lab", amount=50000.0)
    assert ecg.money_out(db, "nsf") == [("grants", "lab", 50000.0)]
    with pytest.raises(ValueError):
        ecg.record_money(db, src="a", relation="vibes", dst="b")


def test_commitment_is_not_a_delivery():
    db = _db()
    ecg.record_commitment(db, commitment_id="po1", committer="dept", beneficiary="lab",
                          resource="gpu-hours", authority="budget-owner")
    assert ecg.commitment_delivered(db, "po1") is False   # existing != delivered
    ecg.mark_delivered(db, "po1")
    assert ecg.commitment_delivered(db, "po1") is True


def test_value_is_not_cash():
    db = _db()
    ecg.record_value(db, src="bernstein-adoption", kind="unlocks", dst="reviewer-access")
    ecg.record_value(db, src="reviewer-access", kind="strengthens_evidence", dst="acm-eligibility")
    reach = dict(ecg.value_reachable(db, "bernstein-adoption"))
    assert "reviewer-access" in reach.values() or ("unlocks", "reviewer-access") in \
        ecg.value_reachable(db, "bernstein-adoption")
    assert "acm-eligibility" in [d for _k, d in ecg.value_reachable(db, "bernstein-adoption")]
    with pytest.raises(ValueError):
        ecg.record_value(db, src="a", kind="pays", dst="b")   # money kind rejected here


def test_projected_cost_is_not_booked():
    db = _db()
    ecg.record_cost(db, mission="run", resource="compute", amount=12.0)
    ecg.record_cost(db, mission="run", resource="human-review", amount=3.0)
    assert ecg.mission_cost(db, "run") == 15.0
    assert ecg.is_booked(db, "run", "compute") is False   # projected, not booked
