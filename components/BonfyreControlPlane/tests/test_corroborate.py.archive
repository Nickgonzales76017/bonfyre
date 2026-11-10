"""Graph-derived corroboration: verified neighbors attest an asserted actor,
recorded as real evidence, never auto-promoted (the human line)."""

import os
import sqlite3
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import actors  # noqa: E402
import corroborate_from_graph as cg  # noqa: E402
import verification as verif  # noqa: E402


def _db() -> sqlite3.Connection:
    db = sqlite3.connect(":memory:")
    actors.ensure_schema(db)
    # hub is asserted; two verified funders and one asserted funder point at it
    actors.upsert_actor(db, actors.Actor("org:hub", "organization", "Hub",
                                         confidence="asserted", provenance="t"))
    actors.upsert_actor(db, actors.Actor("org:v1", "organization", "V1",
                                         confidence="verified", provenance="t"))
    actors.upsert_actor(db, actors.Actor("org:v2", "organization", "V2",
                                         confidence="verified", provenance="t"))
    actors.upsert_actor(db, actors.Actor("org:a1", "organization", "A1",
                                         confidence="asserted", provenance="t"))
    for f in ("org:v1", "org:v2", "org:a1"):
        actors.add_edge(db, f, "funds", "org:hub", provenance="t")
    return db


def test_only_verified_neighbors_corroborate():
    db = _db()
    r = cg.corroborate(db, threshold=2)
    # two verified funders -> two independent corroborations; the asserted one is ignored
    assert verif.independent_sources(db, "org:hub") == 2
    assert "org:hub" in r.ready_for_human


def test_never_auto_promotes():
    db = _db()
    cg.corroborate(db, threshold=2)
    # corroboration recorded, but confidence stays asserted -- verification is human
    row = db.execute("SELECT confidence FROM actor_nodes WHERE actor_id='org:hub'").fetchone()
    assert row[0] == "asserted", "graph corroboration must never promote to verified"


def test_idempotent():
    db = _db()
    cg.corroborate(db, threshold=2)
    r2 = cg.corroborate(db, threshold=2)
    # re-running records no new independent sources (same verified neighbors)
    assert r2.recorded == 0
    assert verif.independent_sources(db, "org:hub") == 2


if __name__ == "__main__":
    import pytest
    raise SystemExit(pytest.main([__file__, "-q"]))
