"""Fortification planner: leverage ranking and the verified/asserted split, on a
hand-built graph so the ranking is deterministic."""

import os
import sqlite3
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import actors  # noqa: E402
import fortification as fort  # noqa: E402


def _graph() -> sqlite3.Connection:
    db = sqlite3.connect(":memory:")
    actors.ensure_schema(db)
    # A hub org that five funders point at; the hub is only asserted.
    actors.upsert_actor(db, actors.Actor("org:hub", "organization", "Hub",
                                          confidence="asserted", provenance="t"))
    for i in range(5):
        fid = f"org:funder{i}"
        actors.upsert_actor(db, actors.Actor(fid, "organization", f"Funder{i}",
                                              confidence="asserted", provenance="t"))
        actors.add_edge(db, fid, "funds", "org:hub", provenance="t")
    # A verified peripheral actor with a lone relationship.
    actors.upsert_actor(db, actors.Actor("org:solid", "organization", "Solid",
                                         confidence="verified", provenance="t"))
    actors.add_edge(db, "org:solid", "funds", "org:hub", provenance="t")
    return db


def test_hub_is_top_verify_priority():
    plan = fort.build_plan(_graph())
    verify = [a for a in plan.actions if a.kind == "verify"]
    assert verify, "expected verify actions"
    assert verify[0].target == "org:hub", "the load-bearing asserted hub ranks first"
    assert verify[0].leverage >= 5


def test_verified_actor_not_a_verify_target():
    plan = fort.build_plan(_graph())
    verify_targets = {a.target for a in plan.actions if a.kind == "verify"}
    assert "org:solid" not in verify_targets, "a verified actor needs no verification"


def test_plan_renders_and_serializes():
    plan = fort.build_plan(_graph())
    md = fort.plan_markdown(plan)
    assert "fortification plan" in md.lower()
    assert "Hub" in md
    import json
    obj = json.loads(fort.plan_json(plan))
    assert obj["actor_count"] == 7
    assert any(a["kind"] == "verify" and a["target"] == "org:hub" for a in obj["actions"])


if __name__ == "__main__":
    import pytest
    raise SystemExit(pytest.main([__file__, "-q"]))
