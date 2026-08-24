"""AuthorityGraph: a permission is held only through a real, in-window,
non-revoked edge -- never inferred, and expiry/revocation/scope are enforced."""

import datetime as dt
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import authority as au
import opportunity as opp

UTC = dt.timezone.utc
T0 = dt.datetime(2026, 8, 15, 12, 0, tzinfo=UTC)


def _db():
    db = sqlite3.connect(":memory:")
    au.ensure_schema(db)
    return db


def test_granted_permission_is_held():
    db = _db()
    au.grant(db, au.AuthorityEdge("e1", actor="nick", permission=au.PUBLISH,
                                  subject="acm-paper", evidence="signed"))
    assert au.has_authority(db, "nick", au.PUBLISH, "acm-paper") is True
    # a different permission over the same subject is not implied
    assert au.has_authority(db, "nick", au.COMMIT, "acm-paper") is False


def test_expired_and_revoked_grant_nothing():
    db = _db()
    au.grant(db, au.AuthorityEdge("e1", actor="a", permission=au.ACT, subject="s",
                                  effective_from="2026-08-01T00:00:00+00:00",
                                  expires_at="2026-08-10T00:00:00+00:00"))
    assert au.has_authority(db, "a", au.ACT, "s", at=T0) is False  # window passed
    au.grant(db, au.AuthorityEdge("e2", actor="a", permission=au.ACT, subject="s"))
    assert au.has_authority(db, "a", au.ACT, "s", at=T0) is True
    au.revoke(db, "e2")
    assert au.has_authority(db, "a", au.ACT, "s", at=T0) is False


def test_purpose_scoping():
    db = _db()
    au.grant(db, au.AuthorityEdge("e", actor="a", permission=au.SPEND, subject="budget",
                                  purpose="travel"))
    assert au.has_authority(db, "a", au.SPEND, "budget", purpose="travel") is True
    assert au.has_authority(db, "a", au.SPEND, "budget", purpose="equipment") is False
    # a general (empty-purpose) grant answers any purpose
    au.grant(db, au.AuthorityEdge("e2", actor="b", permission=au.SPEND, subject="budget"))
    assert au.has_authority(db, "b", au.SPEND, "budget", purpose="anything") is True


def test_delegation_only_from_a_delegable_edge():
    db = _db()
    au.grant(db, au.AuthorityEdge("root", actor="dean", permission=au.APPROVE,
                                  subject="grant", scope="dept/cs", delegable=True))
    child = au.delegate(db, from_edge_id="root", to_actor="chair",
                        new_edge_id="d1", scope="dept/cs")
    assert child is not None
    assert au.has_authority(db, "chair", au.APPROVE, "grant") is True
    # a non-delegable edge cannot be re-granted
    au.grant(db, au.AuthorityEdge("leaf", actor="chair", permission=au.APPROVE,
                                  subject="grant", delegable=False))
    assert au.delegate(db, from_edge_id="leaf", to_actor="x", new_edge_id="d2") is None


def test_authority_blocker_resolves_against_the_graph():
    db = _db()
    o = opp.Opportunity("celld", "send celld series", blockers=(
        opp.Blocker(opp.AUTHORITY, subject="celld-ip-assignment",
                    actor="nick", permission=au.COMMIT, detail="ip-assignment"),
    ))
    before = opp.reachable_capacity(db, [o], [])["celld"]
    assert before.status == opp.BLOCKED  # no authority edge yet

    au.grant(db, au.AuthorityEdge("ip", actor="nick", permission=au.COMMIT,
                                  subject="celld-ip-assignment", purpose="ip-assignment",
                                  evidence="explicit authorization"))
    after = opp.reachable_capacity(db, [o], [])["celld"]
    assert after.status == opp.REACHABLE_NOW  # the real grant resolves the blocker
