"""RelationshipGraph: an evidence-bearing trajectory, per profile. A stage is
reached only when recorded; profiles are independent; the opportunity engine can
require a stage and have it decided against real progress."""

import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import opportunity as opp
import relationship as rel


def _db():
    db = sqlite3.connect(":memory:")
    rel.ensure_schema(db)
    return db


def test_advance_forward_and_stage_at_least():
    db = _db()
    rel.record(db, rel.Relationship("r", actor="chernistry", profile="bernstein", stage="contacted"))
    assert rel.stage_at_least(db, "chernistry", "bernstein", "replied") is False
    assert rel.advance(db, "r", "collaborated", evidence="merged PRs") is True
    assert rel.stage_at_least(db, "chernistry", "bernstein", "engaged") is True
    assert rel.stage_at_least(db, "chernistry", "bernstein", "adopted") is False
    traj = rel.trajectory(db, "r")
    assert traj[-1]["to"] == "collaborated" and traj[-1]["evidence"] == "merged PRs"


def test_no_silent_regress():
    db = _db()
    rel.record(db, rel.Relationship("r", actor="a", profile="p", stage="engaged"))
    assert rel.advance(db, "r", "contacted") is False           # backward refused
    assert rel.advance(db, "r", "contacted", allow_regress=True) is True  # deliberate


def test_profiles_are_independent():
    db = _db()
    rel.record(db, rel.Relationship("r1", actor="x", profile="bernstein", stage="collaborated"))
    # collaboration on one profile says nothing about another
    assert rel.stage_at_least(db, "x", "bernstein", "collaborated") is True
    assert rel.stage_at_least(db, "x", "acm.editor", "engaged") is None  # unknown, not False-as-fact


def test_unknown_relationship_is_none():
    db = _db()
    assert rel.stage_at_least(db, "nobody", "p", "engaged") is None


def test_opportunity_relationship_blocker_resolves_on_advance():
    db = _db()
    o = opp.Opportunity("acm", "ACM submission", blockers=(
        opp.Blocker(opp.RELATIONSHIP_STAGE, subject="acm.editor",
                    actor="acm-editor", layer="engaged"),
    ))
    assert opp.reachable_capacity(db, [o], [])["acm"].status == opp.BLOCKED
    rel.record(db, rel.Relationship("acm-rel", actor="acm-editor", profile="acm.editor",
                                    stage="contacted"))
    assert opp.reachable_capacity(db, [o], [])["acm"].status == opp.BLOCKED  # not far enough
    rel.advance(db, "acm-rel", "engaged", evidence="reply thread")
    assert opp.reachable_capacity(db, [o], [])["acm"].status == opp.REACHABLE_NOW
