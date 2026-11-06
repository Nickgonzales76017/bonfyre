"""Reachability is computed from real state and written as a publishable file."""

import json
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import actors
import proof_frontier as pf
import reachability_publish as rp


def _control(tmp_path):
    p = tmp_path / "control.db"
    con = sqlite3.connect(str(p))
    actors.ensure_schema(con)
    pf.ensure_schema(con)
    # make the fpq reconstruction layer proven, as the real db has it
    pf.set_layer(con, "model:fpq-fixture-tiny-f16", 2, "reconstruction", "proven",
                 subject_profile="fpq-v3-coord-qjl", witness_ref="w")
    con.commit()
    con.close()
    return p


def test_reachability_reflects_real_state(tmp_path):
    data = rp.build_reachability(
        control_db=_control(tmp_path), pack=rp.PACK, fabric=tmp_path / "no-fabric.db")
    assert "summary" in data and "opportunities" in data
    # the fpq evidence opportunity is reachable (its layer is proven); the
    # human/authority-gated ones are blocked -- computed, not asserted.
    assert "fpq-reconstruction-evidence" in data["summary"]["reachable_now"]
    assert "celld-upstream-series" in data["summary"]["blocked"]


def test_write_reachability_file(tmp_path):
    out = rp.write_reachability_file(
        control_db=_control(tmp_path), pack=rp.PACK, out_dir=tmp_path / "proj",
        fabric=tmp_path / "no-fabric.db")
    assert out.exists()
    loaded = json.loads(out.read_text())
    assert loaded["opportunities"]["fpq-reconstruction-evidence"]["status"] == "reachable_now"


def test_work_done_resolves_against_real_fabric_workgraph(tmp_path):
    import opportunity as opp
    # a fabric with a completed WorkGraph node resolves a work_done blocker;
    # the local control db has no matching work_item.
    control = sqlite3.connect(":memory:")
    fabric = sqlite3.connect(":memory:")
    fabric.execute("CREATE TABLE workgraph_nodes(mission_id TEXT, node_id TEXT, "
                   "operator_id TEXT, status TEXT, updated_at_ms INTEGER)")
    fabric.execute("INSERT INTO workgraph_nodes VALUES('m','evidence-pack','op','complete',1)")
    fabric.commit()
    o = opp.Opportunity("pub", "publish",
                        blockers=(opp.Blocker(opp.WORK_DONE, "evidence-pack"),))
    # without the fabric: unknown -> blocked
    assert opp.reachable_capacity(control, [o], [])["pub"].status == opp.BLOCKED
    # with the real fabric node complete: reachable
    ev = opp.reachable_capacity(control, [o], [], fabric_db=fabric)["pub"]
    assert ev.status == opp.REACHABLE_NOW
