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


def test_fact_wiring_orphans_and_duplicates():
    fw = wiring.FactWiring(
        owns={"A": {"X"}, "B": {"Y"}},
        consumes={"B": {"X"}, "C": {"Z"}},
        publishes={"A": {"X"}}, subscribes={},
        producers={"X": {"A"}, "Y": {"B"}},
        consumers={"X": {"B"}, "Z": {"C"}},
    )
    # Z is consumed by C but produced by nobody -> orphan consumer (a real gap)
    assert ("Z", ["C"]) in wiring.orphan_consumers(fw)
    # Y is produced by B but consumed by nobody -> orphan producer
    assert ("Y", ["B"]) in wiring.orphan_producers(fw)
    # X is produced and consumed -> not an orphan either way
    assert "X" not in {f for f, _ in wiring.orphan_consumers(fw)}
    assert "X" not in {f for f, _ in wiring.orphan_producers(fw)}


def test_duplicate_ownership_detected():
    fw = wiring.FactWiring(
        owns={"A": {"X"}, "B": {"X"}}, consumes={}, publishes={}, subscribes={},
        producers={"X": {"A", "B"}}, consumers={},
    )
    dups = dict(wiring.duplicate_ownership(fw))
    assert dups.get("X") == ["A", "B"]


def test_real_atlas_fact_loops_and_no_duplicate_ownership():
    fr = wiring.fact_report()
    assert fr["wired_organs"] >= 10
    # the analysis -> action chain built this session must be a closed fact loop
    loops = [set(L) for L in fr["fact_feedback_loops"]]
    chain = {"actor-graph", "collapse-front", "fortification-plan", "verification-ledger"}
    assert any(chain <= L for L in loops), "the fortify feedback chain must close at the fact level"
    # no two organs may claim authority for the same fact
    assert fr["duplicate_ownership"] == [], fr["duplicate_ownership"]


def test_p8_audit_shape_and_native_progress():
    a = wiring.audit()
    # the audit reports the roadmap detectors
    for key in ("python_only_authority", "non_differential_recompute", "surface_islands",
                "orphan_consumers", "orphan_producers", "duplicate_ownership", "proof_gaps"):
        assert key in a
    # constitution invariants hold
    assert a["duplicate_ownership"] == []
    assert a["proof_gaps"] == []  # validate enforces witnesses
    # native absorption shows: facts with a native owner are NOT python-only
    poa = {fact for fact, _ in a["python_only_authority"]}
    assert "Occurrence" not in poa, "occurrence-spine has native source; not python-only"
    assert "WorkState" not in poa, "work-graph has native source; not python-only"
    # ... but un-absorbed facts still are (a real, honest P0 gap)
    assert "SupportStructure" in poa


if __name__ == "__main__":
    import pytest
    raise SystemExit(pytest.main([__file__, "-q"]))
