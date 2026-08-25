"""The composition loop actually runs FPQ -- not just gates it.

Composition and the fence were already real. This locks in the other half: a
fused representation organism, on dispatch, executes real FPQ compute and the
run promotes BonfyreFPQ from its own receipt. Skips without the real binary and
fixture, because a passing test here must mean the command ran.
"""

import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import capability_catalog as cccat
import capability_closure as cc
import fpq_evidence as fe

pytestmark = pytest.mark.skipif(
    not fe.FPQ.exists() or not fe.FIXTURE.exists(),
    reason="requires real bonfyre-fpq and the tiny-f16 golden fixture",
)


def test_representation_workload_runs_and_promotes():
    db = sqlite3.connect(":memory:")
    cccat.ensure_schema(db)
    cccat.declare(db, cccat.Capability(
        public_name="BonfyreFPQ", maturity="health_probed", location=str(fe.FPQ)))

    out = cc.execute_representation_workload(db)

    assert out["executed"] is True
    assert out["all_good"] is True
    assert out["worst_mse"] < fe.MSE_CEILING
    assert out["promoted_BonfyreFPQ"] is True
    assert out["receipt"].startswith("report:sha256:")

    mat = db.execute(
        "SELECT maturity FROM capability_identities WHERE public_name='BonfyreFPQ'"
    ).fetchone()[0]
    assert mat == "workload_proven"  # earned by a real run, not asserted
    db.close()


def test_missing_binary_makes_no_claim(monkeypatch):
    # if the binary or fixture is absent, the workload reports honestly rather
    # than inventing a promotion.
    monkeypatch.setattr(cc.fq, "FIXTURE", Path("/nonexistent/tiny.gguf"))
    db = sqlite3.connect(":memory:")
    cccat.ensure_schema(db)
    out = cc.execute_representation_workload(db)
    assert out["executed"] is False
    db.close()
