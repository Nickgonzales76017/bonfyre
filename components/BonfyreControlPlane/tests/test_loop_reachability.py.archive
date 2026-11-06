"""Reachability gates the pipeline: only a reachable-now opportunity drives the
AtomicForm -> submit-ready loop; a blocked one is refused with its blocker named,
and the loop never crosses the human line (draft only)."""

import datetime as dt
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import actors
import atomic_forms as af
import institution_loop as il
import opportunity as opp

NOW = dt.datetime(2026, 8, 15, tzinfo=dt.timezone.utc)


def _control_db(tmp_path, confidence):
    p = tmp_path / "control.db"
    con = sqlite3.connect(str(p))
    actors.ensure_schema(con)
    actors.upsert_actor(con, actors.Actor(
        actor_id="editor", node_kind=actors.ORGANIZATION, display_name="Editor",
        confidence=confidence, provenance="test"), now=NOW)
    con.commit()
    con.close()
    return p


def _ready_form(form_id):
    # no required fields, evidence, authority or review gate -> submit-ready.
    return af.AtomicForm(form_id=form_id, title="t")


def test_reachable_opportunity_drives_the_loop(tmp_path):
    control = _control_db(tmp_path, actors.VERIFIED)
    o = opp.Opportunity("pub", "publish",
                        blockers=(opp.Blocker(opp.IDENTITY_VERIFICATION, "editor"),))
    results = il.drive_reachable(
        [o], [], {"pub": _ready_form("pub")},
        control_db=control, fabric_db=tmp_path / "none.db", cms_db=tmp_path / "cms.db")
    r = results[0]
    assert r.status == opp.REACHABLE_NOW
    assert r.drove is True
    assert r.loop.form_ready is True  # compiled to submit-ready (draft; CMS optional)


def test_blocked_opportunity_is_refused(tmp_path):
    control = _control_db(tmp_path, actors.ASSERTED)  # not verified -> blocked
    o = opp.Opportunity("pub", "publish",
                        blockers=(opp.Blocker(opp.IDENTITY_VERIFICATION, "editor"),))
    results = il.drive_reachable(
        [o], [], {"pub": _ready_form("pub")},
        control_db=control, fabric_db=tmp_path / "none.db", cms_db=tmp_path / "cms.db")
    r = results[0]
    assert r.status == opp.BLOCKED
    assert r.drove is False
    assert "identity_verification" in r.reason


def test_reachable_without_a_form_does_not_drive(tmp_path):
    control = _control_db(tmp_path, actors.VERIFIED)
    o = opp.Opportunity("pub", "publish",
                        blockers=(opp.Blocker(opp.IDENTITY_VERIFICATION, "editor"),))
    results = il.drive_reachable(
        [o], [], {},  # no form bound
        control_db=control, fabric_db=tmp_path / "none.db", cms_db=tmp_path / "cms.db")
    assert results[0].drove is False
    assert "no form bound" in results[0].reason
