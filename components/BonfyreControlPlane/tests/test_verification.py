"""Verification ledger: independence gate, threshold promotion, no self-bootstrap,
and the closed feedback loop fortification -> corroborate -> promote -> graph."""

import os
import sqlite3
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import actors  # noqa: E402
import verification as v  # noqa: E402


def _db() -> sqlite3.Connection:
    db = sqlite3.connect(":memory:")
    actors.ensure_schema(db)
    actors.upsert_actor(db, actors.Actor("org:x", "organization", "X",
                                          confidence="asserted", provenance="initial claim"))
    return db


def test_repeat_source_is_not_independent():
    db = _db()
    assert v.record_corroboration(db, "org:x", source="github") is True
    assert v.record_corroboration(db, "org:x", source="github", evidence_ref="e2") is False
    assert v.independent_sources(db, "org:x") == 1


def test_below_threshold_stays_asserted():
    db = _db()
    v.record_corroboration(db, "org:x", source="github")
    st = v.promote_if_corroborated(db, "org:x", threshold=2)
    assert st.confidence == "asserted"
    assert st.gap == 1 and not st.promotable


def test_promotes_at_threshold():
    db = _db()
    v.record_corroboration(db, "org:x", source="github")
    v.record_corroboration(db, "org:x", source="public-web")
    st = v.promote_if_corroborated(db, "org:x", threshold=2)
    assert st.confidence == "verified"
    assert st.gap == 0
    # provenance records the independent sources -- not a self-assertion
    row = db.execute("SELECT provenance FROM actor_nodes WHERE actor_id='org:x'").fetchone()
    assert "independent sources" in row[0] and "github" in row[0]


def test_promotion_is_idempotent():
    db = _db()
    for s in ("a", "b"):
        v.record_corroboration(db, "org:x", source=s)
    v.promote_if_corroborated(db, "org:x", threshold=2)
    st = v.promote_if_corroborated(db, "org:x", threshold=2)  # again
    assert st.confidence == "verified"


def test_closed_loop_retires_fortification_leverage():
    """The feedback loop: a load-bearing asserted actor is a verify target; once
    independently corroborated and promoted, it drops off the plan."""
    import fortification as fort
    db = sqlite3.connect(":memory:")
    actors.ensure_schema(db)
    actors.upsert_actor(db, actors.Actor("org:hub", "organization", "Hub",
                                         confidence="asserted", provenance="claim"))
    for i in range(4):
        fid = f"org:f{i}"
        actors.upsert_actor(db, actors.Actor(fid, "organization", f"F{i}",
                                             confidence="verified", provenance="c"))
        actors.add_edge(db, fid, "funds", "org:hub", provenance="t")

    before = {a.target for a in fort.build_plan(db).actions if a.kind == "verify"}
    assert "org:hub" in before, "load-bearing asserted hub should be a verify target"

    v.record_corroboration(db, "org:hub", source="registry")
    v.record_corroboration(db, "org:hub", source="public-web")
    st = v.promote_if_corroborated(db, "org:hub", threshold=2)
    assert st.confidence == "verified"

    after = {a.target for a in fort.build_plan(db).actions if a.kind == "verify"}
    assert "org:hub" not in after, "verified hub must drop off the verify plan -- loop closed"


if __name__ == "__main__":
    import pytest
    raise SystemExit(pytest.main([__file__, "-q"]))
