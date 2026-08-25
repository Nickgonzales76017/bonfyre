"""The DBSP bridge reproduces the Python reachability answer from real state --
so the maintained relation, not the Python loop, is the source of truth."""

import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import proof_frontier as pf
import reachability_bridge as rb

pytestmark = pytest.mark.skipif(
    not rb.engine_available(), reason="requires the built reachable_capacity_live engine")


def _control(tmp_path, proven: bool):
    p = tmp_path / "control.db"
    con = sqlite3.connect(str(p))
    pf.ensure_schema(con)
    if proven:
        pf.set_layer(con, "model:fpq-fixture-tiny-f16", 2, "reconstruction", "proven",
                     subject_profile="fpq-v3-coord-qjl", witness_ref="w")
    con.commit()
    con.close()
    return p


def test_facts_include_structure_and_resolved(tmp_path):
    facts = rb.build_facts(control_db=_control(tmp_path, proven=True), pack=rb.PACK)
    assert "B\tfpq-reconstruction-evidence\t" in facts   # structure line
    assert "\nR\t" in facts                               # at least one resolved blocker


def test_bridge_marks_fpq_reachable_when_layer_proven(tmp_path):
    r = rb.run_bridge(control_db=_control(tmp_path, proven=True), pack=rb.PACK)
    assert "fpq-reconstruction-evidence" in r.reachable
    # the human/authority-gated ones stay blocked
    assert "celld-upstream-series" not in r.reachable


def test_bridge_blocks_fpq_when_layer_unproven(tmp_path):
    r = rb.run_bridge(control_db=_control(tmp_path, proven=False), pack=rb.PACK)
    assert "fpq-reconstruction-evidence" not in r.reachable


def test_dbsp_reproduces_the_python_answer(tmp_path):
    # the decisive check: the maintained DBSP relation and the Python computation
    # agree exactly on real state -- so Python can retire to export.
    ctrl = _control(tmp_path, proven=True)
    assert rb.run_bridge(control_db=ctrl, pack=rb.PACK).reachable == \
        rb.python_reachable(control_db=ctrl, pack=rb.PACK)
