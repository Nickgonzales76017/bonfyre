"""Bridge to the Bonfyre machine/entity graph (bonfyre_entity.db).

Records ERPNext parties (customers, suppliers) as observations against the
shared entity graph, keyed by a stable entity id derived from the doctype
they came from.
"""

import sqlite3
import time
from pathlib import Path

DB_RELATIVE_PATH = Path("bonfyre_erpnext") / "machine" / "entity" / "bonfyre_entity.db"


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


def record_party_observation(entity_id: str, display: str, modal: str, value: str, source: str) -> None:
    """Upsert the entity, then append an observation (e.g. modal='email', value=customer email)."""
    now = int(time.time())
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO entities (id, display, kind, first_seen, last_seen, obs_count)
            VALUES (?, ?, 'organization', ?, ?, 1)
            ON CONFLICT(id) DO UPDATE SET
                last_seen = excluded.last_seen,
                obs_count = obs_count + 1
            """,
            (entity_id, display, now, now),
        )
        conn.execute(
            "INSERT INTO observations (entity, modal, value, source, ts) VALUES (?, ?, ?, ?, ?)",
            (entity_id, modal, value, source, now),
        )
        conn.commit()
