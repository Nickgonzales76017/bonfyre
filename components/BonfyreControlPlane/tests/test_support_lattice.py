"""CollapseFront correctness: hand-checkable AND/OR support, OR-robustness (a
conclusion with two independent paths survives losing the intermediary), and the
transpose identity between collapse_front and critical_support."""

import os
import sqlite3
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import actors  # noqa: E402
import support_lattice as sl  # noqa: E402


def _graph() -> sqlite3.Connection:
    db = sqlite3.connect(":memory:")
    actors.ensure_schema(db)
    # F funds O directly, and F funds M funds O (a second, independent path).
    for aid, kind in [("org:f", "organization"), ("org:o", "organization"),
                      ("org:m", "organization"), ("person:p1", "person")]:
        actors.upsert_actor(db, actors.Actor(aid, kind, aid, provenance="test"))
    edge = lambda a, k, b: actors.add_edge(db, a, k, b, provenance="test")
    edge("org:f", "funds", "org:o")
    edge("org:f", "funds", "org:m")
    edge("org:m", "funds", "org:o")
    edge("org:o", "employs", "person:p1")
    return db


def test_or_robustness_intermediary_is_not_critical():
    lat = sl.build_lattice(_graph())
    reach_fo = "reach:org:f=>org:o"
    assert reach_fo in lat.conclusions
    # reach F=>O stands on two independent supports (direct + via M)
    assert len(lat.nodes[reach_fo].children) >= 2
    crit = sl.critical_support(lat, reach_fo)
    # F and O are load-bearing for every path; M is not (direct path survives).
    assert "org:f" in crit and "org:o" in crit
    assert "org:m" not in crit, "intermediary must not be critical when a direct path exists"


def test_collapse_front_hits_only_dependents():
    lat = sl.build_lattice(_graph())
    front = set(sl.collapse_front(lat, "org:m"))
    # retracting M kills the M edges and reaches through M ...
    assert "reach:org:f=>org:m" in front
    assert "reach:org:m=>org:o" in front
    # ... but NOT reach F=>O (direct path holds)
    assert "reach:org:f=>org:o" not in front


def test_universal_endpoint_collapses_broadly():
    lat = sl.build_lattice(_graph())
    front = set(sl.collapse_front(lat, "org:o"))
    # O is an endpoint of nearly everything; reach F=>O must collapse with it.
    assert "reach:org:f=>org:o" in front
    assert "edge:org:f|funds|org:o" in front


def test_transpose_identity():
    """c in collapse_front(g) iff label(g) in critical_support(c) -- same matrix."""
    lat = sl.build_lattice(_graph())
    m = sl.collapse_matrix(lat)
    for ground_label, collapsed in m.items():
        for c in collapsed:
            assert ground_label in sl.critical_support(lat, c), (
                f"transpose broken: {ground_label} collapses {c} but is not in its "
                f"critical_support"
            )


def test_ground_with_no_dependents_has_empty_front():
    lat = sl.build_lattice(_graph())
    # p1 is only ever a target (O employs P1); nothing reaches THROUGH p1, so
    # its front is just the edge/reach that names it, never a broad cascade.
    front = set(sl.collapse_front(lat, "person:p1"))
    assert "edge:org:o|employs|person:p1" in front
    assert "reach:org:o=>person:p1" in front
    # it does not collapse the F/M/O funding conclusions
    assert "reach:org:f=>org:o" not in front


if __name__ == "__main__":
    import pytest
    raise SystemExit(pytest.main([__file__, "-q"]))
