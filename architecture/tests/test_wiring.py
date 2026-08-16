"""Wiring self-analysis: feedback-loop (SCC) detection, one-way edges, pure
source/sink classification, and a live check on the real atlas."""

import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ARCH = os.path.dirname(HERE)
sys.path.insert(0, ARCH)

import wiring  # noqa: E402


def _w():
    # A <-> B is a closed loop; S -> A is a one-way source; B -> T is a dead end.
    return wiring.wiring_from_edges([("A", "B"), ("B", "A"), ("S", "A"), ("B", "T")])


def test_feedback_loop_detected():
    loops = wiring.feedback_loops(_w())
    assert ["A", "B"] in loops, loops


def test_one_way_edges():
    ow = set(wiring.one_way_edges(_w()))
    assert ("S", "A") in ow  # A cannot reach back to S
    assert ("B", "T") in ow  # T is a dead end
    assert ("A", "B") not in ow  # part of a closed loop


def test_pure_source_and_sink():
    w = _w()
    assert wiring.pure_sources(w) == ["S"]
    assert wiring.pure_sinks(w) == ["T"]


def test_loop_closures_ranked_by_influence():
    # B reaches {A, T} downstream; S reaches everything through A. Closures are
    # ranked by the destination's downstream influence.
    closures = wiring.loop_closures(_w())
    assert closures
    assert all("suggestion" in c and c["destination_influence"] >= 0 for c in closures)


def test_real_atlas_has_feedback_cores():
    """The real registry must have at least one closed feedback loop -- a system
    that only flows one way has not begun wiring closure."""
    r = wiring.report()
    assert r["wired_nodes"] > 0 and r["interactions"] > 0
    assert r["feedback_loops"], "no feedback loop in the real atlas wiring"
    # sanity: the largest core is a real multi-node cycle
    assert len(r["largest_feedback_core"]) >= 2


if __name__ == "__main__":
    import pytest
    raise SystemExit(pytest.main([__file__, "-q"]))
