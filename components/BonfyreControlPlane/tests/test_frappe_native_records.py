"""Native app-record write-back: Bonfyre state shaped by real DocType fields,
carrying the canonical identity -- no bench, no pairwise store. Skips if the
Frappe apps are not checked out."""

import os
import sqlite3
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import actors  # noqa: E402
import external_events as ee  # noqa: E402
import frappe_native_records as fnr  # noqa: E402

_HAS_CRM = os.path.isdir(os.path.join(fnr.APPS, "crm"))
_HAS_HD = os.path.isdir(os.path.join(fnr.APPS, "helpdesk"))


def _db() -> str:
    import tempfile
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    db = sqlite3.connect(path)
    actors.ensure_schema(db)
    ee.ensure_schema(db)
    actors.upsert_actor(db, actors.Actor("org:acme", "organization", "Acme Corp",
                                         confidence="verified", provenance="t"))
    ee.observe(db, source="github", actor="org:acme", event_kind="inbound_reply",
               subject_ref="issue#1")
    db.commit()
    db.close()
    return path


@pytest.mark.skipif(not _HAS_CRM, reason="crm app not present")
def test_leads_shaped_by_real_doctype_fields():
    path = _db()
    try:
        fields = fnr.doctype_fields("crm", "CRM Lead")
        recs = fnr.project_leads(path)
    finally:
        os.unlink(path)
    assert "first_name" in fields and "status" in fields, "real CRM Lead schema"
    assert len(recs) == 1
    r = recs[0]
    # the record has exactly the DocType fields, plus the canonical identity ref
    assert set(fields).issubset(set(r.keys()))
    assert r["_bonfyre_ref"] == "org:acme", "record projects the canonical identity"
    assert r["first_name"] == "Acme" and r["status"] == "Verified"


@pytest.mark.skipif(not _HAS_HD, reason="helpdesk app not present")
def test_tickets_from_occurrences():
    path = _db()
    try:
        recs = fnr.project_tickets(path)
    finally:
        os.unlink(path)
    assert len(recs) == 1
    assert recs[0]["_bonfyre_ref"] == "occurrence:1"
    assert recs[0]["raised_by"] == "org:acme"


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
