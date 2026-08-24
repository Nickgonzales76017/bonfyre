"""One tick of the fused machine runs end to end on a synthetic control plane and
produces a briefing. Idempotent."""

import os
import sqlite3
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import actors  # noqa: E402
import external_events as ee  # noqa: E402
import bonfyre_tick as bt  # noqa: E402


def _control_db() -> str:
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    db = sqlite3.connect(path)
    actors.ensure_schema(db)
    ee.ensure_schema(db)
    actors.upsert_actor(db, actors.Actor("org:hub", "organization", "Hub",
                                         confidence="asserted", provenance="t"))
    for i, conf in enumerate(("verified", "verified", "asserted")):
        fid = f"org:f{i}"
        actors.upsert_actor(db, actors.Actor(fid, "organization", f"F{i}",
                                             confidence=conf, provenance="t"))
        actors.add_edge(db, fid, "funds", "org:hub", provenance="t")
    ee.observe(db, source="github", actor="org:hub", event_kind="inbound_reply",
               subject_ref="x")
    db.commit()
    db.close()
    return path


def test_tick_runs_and_reports():
    path = _control_db()
    try:
        r = bt.tick(path)
    finally:
        os.unlink(path)
    # the pending occurrence got folded
    assert r["occurrences_folded"] == 1
    # the two verified funders corroborated the hub -> ready for human
    assert "org:hub" in r["ready_for_human_verification"]
    # a verify target list came back
    assert isinstance(r["verify_targets"], list)
    # the briefing renders
    md = bt.briefing(r)
    assert "Bonfyre tick" in md and "Next actions" in md


def test_tick_is_idempotent():
    path = _control_db()
    try:
        bt.tick(path)
        r2 = bt.tick(path)  # second run
    finally:
        os.unlink(path)
    # nothing left to fold, no new corroborations
    assert r2["occurrences_folded"] == 0
    assert r2["corroborations_recorded"] == 0


if __name__ == "__main__":
    import pytest
    raise SystemExit(pytest.main([__file__, "-q"]))
