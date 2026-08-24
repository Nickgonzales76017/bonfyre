"""Virtual query directories: sets compute from real tables and materialize as a
browsable tree. Hermetic -- builds its own control db."""

import json
import os
import sqlite3
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import actors  # noqa: E402
import external_events as ee  # noqa: E402
import fabric_queries as fq  # noqa: E402


def _control_db() -> str:
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    db = sqlite3.connect(path)
    actors.ensure_schema(db)
    ee.ensure_schema(db)
    # a hub org three funders point at; the hub is unverified
    actors.upsert_actor(db, actors.Actor("org:hub", "organization", "Hub",
                                         confidence="asserted", provenance="t"))
    for i in range(3):
        fid = f"org:f{i}"
        actors.upsert_actor(db, actors.Actor(fid, "organization", f"F{i}",
                                             confidence="verified", provenance="t"))
        actors.add_edge(db, fid, "funds", "org:hub", provenance="t")
    # one pending occurrence
    db.execute(
        "INSERT INTO external_event_log(digest,observed_at,recorded_at,source,actor,"
        "event_kind,subject_ref) VALUES('d','2026-01-01T00:00:00+00:00',"
        "'2026-01-01T00:00:00+00:00','github','org:hub','inbound_reply','x')")
    db.commit()
    db.close()
    fq._CACHE.clear()
    return path


def test_sets_compute_from_real_tables():
    path = _control_db()
    try:
        sets = {qs.name: qs for qs in fq.compute_all(path)}
    finally:
        os.unlink(path)
    # the unverified hub is present
    assert any(m["id"] == "org:hub" for m in sets["Unverified-Actors"].members)
    # the hub is load-bearing (three funders rest on it)
    assert any(m["id"] == "org:hub" for m in sets["Load-Bearing"].members)
    # it is a verify target with a corroboration gap
    rtv = {m["id"]: m for m in sets["Ready-To-Verify"].members}
    assert "org:hub" in rtv and rtv["org:hub"]["gap"] == 2
    # the one pending occurrence shows
    assert len(sets["Pending-Occurrences"].members) == 1


def test_materialize_writes_browsable_tree():
    path = _control_db()
    out = tempfile.mkdtemp()
    try:
        sets = fq.compute_all(path)
        root = fq.materialize(sets, out)
        # top index + one dir per set with its own index
        top = json.load(open(os.path.join(root, "index.json")))
        assert len(top["queries"]) == len(fq.REGISTRY)
        for qs in sets:
            idx = json.load(open(os.path.join(root, qs.name, "index.json")))
            assert idx["count"] == len(qs.members)
            assert idx["name"] == qs.name
    finally:
        os.unlink(path)


if __name__ == "__main__":
    import pytest
    raise SystemExit(pytest.main([__file__, "-q"]))
