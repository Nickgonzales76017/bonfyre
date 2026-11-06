"""The watcher republishes exactly on a real state change, and stays quiet
otherwise -- the Feldera role at coarse grain."""

import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import actors
import authority as au
import proof_frontier as pf
import reachability_watch as rw


def _control(tmp_path):
    p = tmp_path / "control.db"
    con = sqlite3.connect(str(p))
    actors.ensure_schema(con)
    pf.ensure_schema(con)
    au.ensure_schema(con)
    con.commit()
    con.close()
    return p


def test_signature_changes_only_on_relevant_change(tmp_path):
    ctrl = _control(tmp_path)
    nofab = tmp_path / "no-fabric.db"
    s1 = rw.state_signature(ctrl, nofab)
    assert s1 == rw.state_signature(ctrl, nofab)  # stable when nothing changes
    # a real change: verify an actor
    con = sqlite3.connect(str(ctrl))
    actors.upsert_actor(con, actors.Actor(actor_id="a", node_kind=actors.ORGANIZATION,
                        display_name="a", confidence=actors.VERIFIED, provenance="t"))
    con.close()
    assert rw.state_signature(ctrl, nofab) != s1


def test_tick_republishes_once_then_stays_quiet(tmp_path):
    ctrl = _control(tmp_path)
    nofab = tmp_path / "no-fabric.db"
    sig_file = tmp_path / "sig"

    first = rw.tick(control_db=ctrl, fabric=nofab, state_file=sig_file)
    assert first.changed is True and first.published_digest

    # no state change -> no republish
    second = rw.tick(control_db=ctrl, fabric=nofab, state_file=sig_file)
    assert second.changed is False

    # grant an authority (real change) -> republish again
    con = sqlite3.connect(str(ctrl))
    au.grant(con, au.AuthorityEdge("e", actor="x", permission=au.COMMIT, subject="s"))
    con.close()
    third = rw.tick(control_db=ctrl, fabric=nofab, state_file=sig_file)
    assert third.changed is True
