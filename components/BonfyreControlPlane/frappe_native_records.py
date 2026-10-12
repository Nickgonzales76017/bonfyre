"""Native app-record write-back -- Bonfyre's own state, in the app's grammar.

No Frappe bench. The apps are projections over shared semantic objects (mandate
SS6/SS30), so an app RECORD is just a Bonfyre canonical object shaped by the app's
real DocType field schema (extracted by BonfyreFrappeCompiler) and written natively
into fabric.db. A CRM Lead is a real actor wearing CRM Lead's fields; an HD Ticket
is a real occurrence wearing HD Ticket's fields. Same identity, no separate
destination, no pairwise store.

This closes the per-app runtime write-back the Bonfyre-native way: the fabric is
the record store; Frappe donates the grammar (fields), Bonfyre owns the facts.
"""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
APPS = os.path.join(REPO, "integrations", "frappe-bench", "apps")
PARSER = os.path.join(REPO, "cmd", "BonfyreFrappeCompiler", "frappe_schema_parser.py")

# fieldtypes that carry data (skip layout-only fields)
_LAYOUT = {"Section Break", "Column Break", "Tab Break", "HTML", "Heading"}


def doctype_fields(app: str, doctype: str) -> list[str]:
    """Real data fieldnames of a DocType, via the existing compiler."""
    app_path = os.path.join(APPS, app)
    if not os.path.isdir(app_path):
        return []
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tf:
        out = tf.name
    try:
        subprocess.run(["python3", PARSER, app_path, out], capture_output=True, timeout=120)
        ir = json.load(open(out))
        for dt in ir.get("doctypes", []):
            if dt["name"] == doctype:
                return [f["fieldname"] for f in dt.get("fields", [])
                        if f.get("fieldtype") not in _LAYOUT and f.get("fieldname")]
    except Exception:
        return []
    finally:
        try:
            os.unlink(out)
        except OSError:
            pass
    return []


def _shape(fields: list[str], source: dict) -> dict:
    """A record with exactly the DocType's fields; filled from the Bonfyre source
    where a field name is provided, else left empty (honest: no invented data)."""
    return {f: source.get(f, "") for f in fields}


def project_leads(control_db: str) -> list[dict]:
    """Actors -> CRM Lead records, shaped by the real CRM Lead field schema."""
    fields = doctype_fields("crm", "CRM Lead")
    if not fields:
        return []
    db = sqlite3.connect(f"file:{control_db}?mode=ro", uri=True)
    try:
        if not db.execute("SELECT 1 FROM sqlite_master WHERE name='actor_nodes'").fetchone():
            return []
        rows = db.execute("SELECT actor_id,display_name,node_kind,confidence,org_id"
                          " FROM actor_nodes ORDER BY actor_id").fetchall()
    finally:
        db.close()
    out = []
    for aid, name, kind, conf, org in rows:
        parts = (name or aid).split(" ", 1)
        src = {
            "first_name": parts[0],
            "last_name": parts[1] if len(parts) > 1 else "",
            "status": "Verified" if conf == "verified" else "Open",
            "website": org or "",
            "lead_name": name or aid,
            "_bonfyre_ref": aid,  # the canonical identity this record projects
        }
        rec = _shape(fields, src)
        rec["_bonfyre_ref"] = aid
        out.append(rec)
    return out


def project_tickets(control_db: str) -> list[dict]:
    """Occurrences (inbound replies) -> HD Ticket records, shaped by HD Ticket."""
    fields = doctype_fields("helpdesk", "HD Ticket")
    if not fields:
        return []
    db = sqlite3.connect(f"file:{control_db}?mode=ro", uri=True)
    try:
        if not db.execute("SELECT 1 FROM sqlite_master WHERE name='external_event_log'").fetchone():
            return []
        rows = db.execute("SELECT id,actor,event_kind,subject_ref FROM external_event_log"
                          " WHERE event_kind='inbound_reply' ORDER BY id").fetchall()
    finally:
        db.close()
    out = []
    for eid, actor, kind, subject in rows:
        src = {
            "subject": subject or f"reply from {actor}",
            "raised_by": actor,
            "status": "Open",
            "ticket_type": "Inbound Reply",
            "description": f"Occurrence {eid}: {kind} from {actor} re {subject}",
            "_bonfyre_ref": f"occurrence:{eid}",
        }
        rec = _shape(fields, src)
        rec["_bonfyre_ref"] = f"occurrence:{eid}"
        out.append(rec)
    return out


def project_payments(control_db: str) -> list[dict]:
    """Commitment ledger entries -> ERPNext Payment Entry records (realized value in
    the double-entry grammar), shaped by the real Payment Entry field schema."""
    fields = doctype_fields("erpnext", "Payment Entry")
    if not fields:
        return []
    db = sqlite3.connect(f"file:{control_db}?mode=ro", uri=True)
    try:
        if not db.execute("SELECT 1 FROM sqlite_master WHERE name='commitment_entries'").fetchone():
            return []
        rows = db.execute("SELECT id,actor,category,amount_usd,currency,occurred_at"
                          " FROM commitment_entries ORDER BY id").fetchall()
    finally:
        db.close()
    out = []
    for cid, actor, cat, amount, currency, when in rows:
        src = {
            "payment_type": "Receive",
            "party_type": "Customer",
            "party": actor,
            "party_name": actor,
            "paid_amount": amount,
            "received_amount": amount,
            "posting_date": (when or "")[:10],
            "paid_from_account_currency": currency or "USD",
            "paid_to_account_currency": currency or "USD",
            "mode_of_payment": cat,
        }
        rec = _shape(fields, src)
        rec["_bonfyre_ref"] = f"commitment:{cid}"
        out.append(rec)
    return out


def project_insights(control_db: str) -> list[dict]:
    """The maintained query directories -> Insights Query records. Insights is the
    grammar of Bonfyre's differential state; each maintained set is a query."""
    fields = doctype_fields("insights", "Insights Query")
    if not fields:
        return []
    import fabric_queries as fq
    fq._CACHE.clear()
    out = []
    for name, desc, compute in fq.REGISTRY:
        try:
            n = len(compute(control_db))
        except Exception:
            n = 0
        src = {
            "title": name,
            "status": "Maintained",
            "data_source": "bonfyre-control-plane",
            "is_native_query": 1,
            "is_stored": 0,  # maintained, not stored -- the SS38 point
        }
        rec = _shape(fields, src)
        rec["_bonfyre_ref"] = f"query:{name}"
        rec["_count"] = n
        out.append(rec)
    return out


PROJECTIONS = {
    "crm/CRM-Lead": ("crm", "CRM Lead", project_leads),
    "helpdesk/HD-Ticket": ("helpdesk", "HD Ticket", project_tickets),
    "erpnext/Payment-Entry": ("erpnext", "Payment Entry", project_payments),
    "insights/Insights-Query": ("insights", "Insights Query", project_insights),
}


def materialize(control_db: str, out_dir: str) -> dict:
    from pathlib import Path
    root = Path(out_dir) / "app-records"
    counts = {}
    for name, (app, doctype, fn) in PROJECTIONS.items():
        recs = fn(control_db)
        d = root / name
        d.mkdir(parents=True, exist_ok=True)
        (d / "index.json").write_text(json.dumps(
            {"app": app, "doctype": doctype, "count": len(recs),
             "fields": doctype_fields(app, doctype), "records": recs[:200]},
            indent=2, sort_keys=True))
        counts[name] = len(recs)
    return {"counts": counts, "root": str(root)}


def register_in_fabric(control_db: str, timeout_ms: int = 6000) -> dict:
    import fabric_publish as fp
    import hashlib
    from pathlib import Path
    m = materialize(control_db, str(fp.PROJECTIONS))
    published, skipped = [], []
    try:
        db = sqlite3.connect(str(fp.FABRIC), timeout=timeout_ms / 1000.0)
        db.execute(f"PRAGMA busy_timeout={timeout_ms}")
        fp.ensure_schema(db)
        now = fp._iso()
        for name in PROJECTIONS:
            path = Path(m["root"]) / name / "index.json"
            data = path.read_bytes()
            digest = hashlib.sha256(data).hexdigest()
            uri = f"bonfyre://artifact/{digest}"
            slug = name.replace("/", "-")
            db.execute(
                "INSERT INTO namespace_objects(uri,kind,owner,source_authority,native_id,"
                "version,locator,policy,sensitivity,freshness,evidence_state,operations,"
                "content_contract,query_contract,effect_contract,created_at)"
                " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
                " ON CONFLICT(uri) DO UPDATE SET freshness='current'",
                (uri, "app-record", "local-user", fp.SOURCE_AUTHORITY, f"apprec-{slug}", "1",
                 uri, "default", "standard", "current", "measured", "read",
                 "app-record.v1", "typed-lookup.v1", "governed", now))
            db.execute(
                "INSERT INTO artifacts(digest,uri,media_type,source_uri,locator,bytes,"
                "representation,created_at) VALUES(?,?,?,?,?,?,?,?)"
                " ON CONFLICT(digest) DO UPDATE SET locator=excluded.locator",
                (digest, uri, "application/json", str(path), str(path), len(data),
                 "zero-copy-reference", now))
            published.append(f"apprec-{slug}")
        db.commit()
        db.close()
    except sqlite3.OperationalError:
        skipped, published = list(PROJECTIONS), []
    return {"published": published, "skipped_fabric_busy": skipped, **m}


if __name__ == "__main__":
    import sys
    cp = sys.argv[1] if len(sys.argv) > 1 else os.path.join(HERE, "control_plane.db")
    r = register_in_fabric(cp)
    for name, n in r["counts"].items():
        print(f"  {name:22s} {n:4d} records (Bonfyre state in the app's DocType grammar)")
    print("published to fabric:", r["published"] or f"skipped: {r['skipped_fabric_busy']}")
