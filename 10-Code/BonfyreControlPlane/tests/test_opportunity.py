"""UnlockGraph over real state: reachability is decided against actor
verification, the proof frontier, and work state -- not asserted."""

import datetime as dt
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import actors
import opportunity as opp
import proof_frontier as pf
import work_graph as wg

UTC = dt.timezone.utc
NOW = dt.datetime(2026, 8, 15, tzinfo=UTC)


def _db():
    db = sqlite3.connect(":memory:")
    actors.ensure_schema(db)
    pf.ensure_schema(db)
    wg.ensure_schema(db)
    return db


def _actor(db, actor_id, confidence):
    actors.upsert_actor(db, actors.Actor(
        actor_id=actor_id, node_kind=actors.ORGANIZATION, display_name=actor_id,
        confidence=confidence, provenance="test"), now=NOW)


def _satisfy_work(db, subject):
    wg.register_plane(db, "p")
    wg.enqueue(db, source_plane="p", target_plane="p", item_kind="k",
               subject_ref=subject, reason="", source_ref="s", priority=1.0, now=NOW)
    db.execute("UPDATE work_items SET state='satisfied' WHERE subject_ref=?", (subject,))
    db.commit()


def test_identity_blocker_flips_on_real_verification():
    db = _db()
    _actor(db, "acm-editor", actors.ASSERTED)
    o = opp.Opportunity("acm-submit", "ACM submission",
                        blockers=(opp.Blocker(opp.IDENTITY_VERIFICATION, "acm-editor"),))
    ev = opp.reachable_capacity(db, [o], [])["acm-submit"]
    assert ev.status == opp.BLOCKED
    assert ev.hard_blockers[0].kind == opp.IDENTITY_VERIFICATION

    # verify the actor in real state -> the same opportunity is now reachable.
    _actor(db, "acm-editor", actors.VERIFIED)
    ev2 = opp.reachable_capacity(db, [o], [])["acm-submit"]
    assert ev2.status == opp.REACHABLE_NOW
    assert ev2.open_blockers == ()


def test_authorized_unlock_requiring_real_work_makes_unlockable():
    db = _db()
    _satisfy_work(db, "evidence-pack")
    o = opp.Opportunity("grant", "Grant application",
                        blockers=(opp.Blocker(opp.PROOF_LAYER, "model:x", detail="",
                                              profile="p", layer="reconstruction"),))
    # the proof layer is NOT proven, but an authorized unlock exists whose only
    # precondition -- a real, satisfied work item -- holds.
    u = opp.Unlock("prove-it", opp.PROOF_LAYER, "model:x", action="run the fixture",
                   authorized=True, requires=(("blocker", opp.WORK_DONE, "evidence-pack"),))
    # add the work_done blocker to the opportunity so it is resolved in state
    o = opp.Opportunity("grant", "Grant application", blockers=(
        opp.Blocker(opp.PROOF_LAYER, "model:x", profile="p", layer="reconstruction"),
        opp.Blocker(opp.WORK_DONE, "evidence-pack"),
    ))
    ev = opp.reachable_capacity(db, [o], [u])["grant"]
    # work_done resolved; proof_layer covered by the authorized unlock -> unlockable
    assert ev.status == opp.UNLOCKABLE
    assert ("proof_layer", "model:x") in ev.covered_by


def test_unauthorized_unlock_does_not_unlock():
    # the law: an unlock existing is not the same as it being authorized.
    db = _db()
    o = opp.Opportunity("o", "o", blockers=(opp.Blocker(opp.AUTHORITY, "sign-off"),))
    u = opp.Unlock("u", opp.AUTHORITY, "sign-off", action="ask", authorized=False)
    ev = opp.reachable_capacity(db, [o], [u])["o"]
    assert ev.status == opp.BLOCKED
    assert ev.hard_blockers[0].kind == opp.AUTHORITY


def test_unknown_substrate_blocker_is_never_assumed_satisfied():
    db = _db()
    o = opp.Opportunity("o", "o", blockers=(opp.Blocker(opp.BUDGET, "line-item"),))
    ev = opp.reachable_capacity(db, [o], [])["o"]
    assert ev.status == opp.BLOCKED  # budget has no substrate -> open, not satisfied


def test_opportunity_depends_on_another_reachable_opportunity():
    db = _db()
    _actor(db, "reviewer", actors.VERIFIED)
    # A is reachable now (verified reviewer). B is blocked by a service that an
    # authorized unlock provides, but only once A is reachable.
    a = opp.Opportunity("A", "reviewer access",
                        blockers=(opp.Blocker(opp.IDENTITY_VERIFICATION, "reviewer"),))
    b = opp.Opportunity("B", "equipment",
                        blockers=(opp.Blocker(opp.SERVICE_BOUND, "gpu-lease"),))
    u = opp.Unlock("lease-via-A", opp.SERVICE_BOUND, "gpu-lease", action="lease",
                   authorized=True, requires=(("opportunity", "A"),))
    evals = opp.reachable_capacity(db, [a, b], [u])
    assert evals["A"].status == opp.REACHABLE_NOW
    assert evals["B"].status == opp.UNLOCKABLE
    summary = opp.capacity_summary(evals)
    assert summary[opp.REACHABLE_NOW] == ["A"] and summary[opp.UNLOCKABLE] == ["B"]


def test_capacity_summary_partitions_all():
    db = _db()
    _actor(db, "v", actors.VERIFIED)
    reach = opp.Opportunity("r", "r", blockers=(opp.Blocker(opp.IDENTITY_VERIFICATION, "v"),))
    stuck = opp.Opportunity("s", "s", blockers=(opp.Blocker(opp.HUMAN_APPROVAL, "dean"),))
    evals = opp.reachable_capacity(db, [reach, stuck], [])
    summary = opp.capacity_summary(evals)
    assert summary[opp.REACHABLE_NOW] == ["r"]
    assert summary[opp.BLOCKED] == ["s"]
