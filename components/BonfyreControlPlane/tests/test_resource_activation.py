"""ResourceActivationGraph: a resource is usable only at ACTIVATED, reached one
gated step at a time -- qualification, eligibility, activate authority, mechanism
binding -- with authority checked against the real graph."""

import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import authority as au
import opportunity as opp
import resource_activation as ra


def _db():
    db = sqlite3.connect(":memory:")
    ra.ensure_schema(db)
    au.ensure_schema(db)
    return db


def test_activation_walks_the_state_machine():
    db = _db()
    assert ra.activation_state(db, "gpu-a").state == ra.GAP  # nothing recorded

    ra.record_candidate(db, ra.ResourceCandidate("gpu-a", mechanism=ra.GPU_LEASE,
                                                 activate_actor="nick"))
    assert ra.activation_state(db, "gpu-a").state == ra.CANDIDATE  # unqualified

    ra.record_candidate(db, ra.ResourceCandidate("gpu-a", mechanism=ra.GPU_LEASE,
                                                 qualified=True, activate_actor="nick"))
    assert ra.activation_state(db, "gpu-a").state == ra.QUALIFIED  # not eligible

    ra.record_candidate(db, ra.ResourceCandidate("gpu-a", mechanism=ra.GPU_LEASE,
                                                 qualified=True, eligible=True,
                                                 activate_actor="nick"))
    st = ra.activation_state(db, "gpu-a")
    assert st.state == ra.ELIGIBLE and "activate authority" in st.missing  # no authority

    au.grant(db, au.AuthorityEdge("e", actor="nick", permission=au.ACTIVATE, subject="gpu-a"))
    st = ra.activation_state(db, "gpu-a")
    assert st.state == ra.AUTHORIZED  # authorized but mechanism not bound

    st = ra.activation_state(db, "gpu-a", bound_services=frozenset({"gpu-a"}))
    assert st.state == ra.ACTIVATED and st.activated is True


def test_authority_is_not_short_circuited():
    # eligible + qualified + bound but NO activate authority -> not activated.
    db = _db()
    ra.record_candidate(db, ra.ResourceCandidate("svc", qualified=True, eligible=True,
                                                 activate_actor="a"))
    st = ra.activation_state(db, "svc", bound_services=frozenset({"svc"}))
    assert st.state == ra.ELIGIBLE and not st.activated


def test_opportunity_resource_blocker_resolves_only_when_activated():
    db = _db()
    ra.record_candidate(db, ra.ResourceCandidate("cluster", qualified=True, eligible=True,
                                                 activate_actor="nick"))
    au.grant(db, au.AuthorityEdge("e", actor="nick", permission=au.ACTIVATE, subject="cluster"))
    o = opp.Opportunity("run-eval", "run the evaluation", blockers=(
        opp.Blocker(opp.RESOURCE_ACTIVE, subject="cluster"),
    ))
    # authorized but not yet bound -> blocked
    assert opp.reachable_capacity(db, [o], [])["run-eval"].status == opp.BLOCKED
    # bind the mechanism -> activated -> reachable
    ev = opp.reachable_capacity(db, [o], [], bound_services=frozenset({"cluster"}))["run-eval"]
    assert ev.status == opp.REACHABLE_NOW
