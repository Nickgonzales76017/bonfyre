"""Organs publishing their declared facts into the fabric: facts read from their
live authoritative tables, materialized as a projection. Hermetic."""

import json
import os
import sqlite3
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import actors  # noqa: E402
import external_events as ee  # noqa: E402
import fabric_facts as ff  # noqa: E402


def _control_db() -> str:
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    db = sqlite3.connect(path)
    actors.ensure_schema(db)
    ee.ensure_schema(db)
    actors.upsert_actor(db, actors.Actor("org:a", "organization", "A", provenance="t"))
    actors.upsert_actor(db, actors.Actor("person:b", "person", "B", provenance="t"))
    actors.add_edge(db, "org:a", "employs", "person:b", provenance="t")
    db.execute(
        "INSERT INTO external_event_log(digest,observed_at,recorded_at,source,actor,"
        "event_kind,subject_ref) VALUES('d1','2026-01-01T00:00:00+00:00',"
        "'2026-01-01T00:00:00+00:00','github','org:a','inbound_reply','x')")
    db.commit()
    db.close()
    return path


def test_facts_read_from_live_tables():
    path = _control_db()
    try:
        facts = {f.name: f for f in ff.compute_all(path)}
    finally:
        os.unlink(path)
    assert facts["Actor"].owner == "actor-graph"
    assert len(facts["Actor"].members) == 2
    assert len(facts["Relation"].members) == 1
    assert facts["Relation"].members[0]["kind"] == "employs"
    # the inbound reply is CRM's communication event
    assert len(facts["CommunicationEvent"].members) == 1
    assert facts["CommunicationEvent"].members[0]["actor"] == "org:a"
    # empty facts are honestly empty, not fabricated
    assert facts["Corroboration"].members == []


def test_materialize_fact_tree():
    path = _control_db()
    out = tempfile.mkdtemp()
    try:
        facts = ff.compute_all(path)
        root = ff.materialize(facts, out)
        top = json.load(open(os.path.join(root, "index.json")))
        assert len(top["facts"]) == len(ff.REGISTRY)
        actor_idx = json.load(open(os.path.join(root, "Actor", "index.json")))
        assert actor_idx["owner"] == "actor-graph" and actor_idx["count"] == 2
    finally:
        os.unlink(path)


if __name__ == "__main__":
    import pytest
    raise SystemExit(pytest.main([__file__, "-q"]))
