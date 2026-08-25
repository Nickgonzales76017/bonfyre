"""The forwarding plane gates the closure: a blackholed or unauthorized demand is
refused before DisCIPL composes anything."""

import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import address_plane as ap
import capability_closure as cc
import proof_frontier as pf


def _db_with_estate():
    db = sqlite3.connect(":memory:")
    db.execute("CREATE TABLE estate_catalog(family TEXT, estate TEXT)")
    db.executemany("INSERT INTO estate_catalog VALUES(?,?)",
                   [("BonfyreFPQ", "model"), ("BonfyreHash", "artifact")])
    db.commit()
    pf.ensure_schema(db)
    return db


def test_blackholed_demand_is_refused_before_composition():
    db = _db_with_estate()
    pf.record_noncause(db, pf.KnownNonCause(
        noncause_id="nc", hypothesis="quantization", subject_scope="model:x",
        experiment="fp16 passthrough still garbage"))
    demand = ap.RouteDemand(estates=("model",), subject="model:x",
                            forbidden_hypotheses=("quantization corruption",))
    org = cc.route_then_close(db, demand, src_family="T_FPQ", dst_family="T_KVCACHE")
    # refused at the forwarding plane -- no composition, no hops
    assert org.authorized is False and org.hops == ()
    assert "forwarding plane rejected" in org.verdict_reason


def test_empty_estate_demand_is_refused():
    db = _db_with_estate()
    demand = ap.RouteDemand(estates=("nonexistent-estate",))
    org = cc.route_then_close(db, demand, src_family="T_FPQ", dst_family="T_KVCACHE")
    assert org.authorized is False and org.hops == ()
