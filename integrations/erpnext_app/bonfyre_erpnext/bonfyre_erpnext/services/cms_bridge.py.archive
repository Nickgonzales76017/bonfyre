"""Bridge to the Bonfyre CMS's erp_document table (bonfyre_cms.db).

Mirrors submitted ERPNext documents into the CMS content store so the
estate's content-family/lineage tooling can see them. Uses the site's
private/files path -- falls back to the fixture path under this app's own
private/files when frappe isn't running a real site (local dev/testing).
"""

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

DB_RELATIVE_PATH = Path("bonfyre_erpnext") / "cms" / "bonfyre_cms.db"


def _db_path() -> Path:
    try:
        import frappe

        return Path(frappe.get_site_path("private", "files", *DB_RELATIVE_PATH.parts))
    except Exception:
        return Path(__file__).resolve().parents[3] / "private" / "files" / DB_RELATIVE_PATH


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(str(_db_path()))
    conn.row_factory = sqlite3.Row
    return conn


def mirror_document(doc, entry_status: str = "submitted") -> None:
    """Upsert a submitted ERPNext document into erp_document."""
    now = datetime.now(timezone.utc).isoformat()
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO erp_document
                (namespace, created_at, updated_at, status, doctype, docname,
                 title, customer_name, supplier_name, entry_status, doc_payload)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "root",
                now,
                now,
                "published",
                doc.doctype,
                doc.name,
                getattr(doc, "title", None) or doc.name,
                getattr(doc, "customer_name", None) or getattr(doc, "customer", None),
                getattr(doc, "supplier_name", None) or getattr(doc, "supplier", None),
                entry_status,
                json.dumps(doc.as_dict(), default=str),
            ),
        )
        conn.commit()


def recent_documents(limit: int = 20) -> list[dict]:
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT id, doctype, docname, title, customer_name, supplier_name,
                   entry_status, created_at, updated_at
            FROM erp_document
            ORDER BY updated_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [dict(row) for row in rows]
