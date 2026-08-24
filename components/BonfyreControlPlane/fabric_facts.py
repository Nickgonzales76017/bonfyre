"""Each organ publishing its declared facts into the fabric -- running, not a map.

The WiringSpec says actor-graph publishes Actor/Relation, occurrence-spine
publishes Occurrence/OccurrenceProjection, crm publishes CommunicationEvent,
verification-ledger publishes Corroboration. This makes those declarations REAL:
it reads each fact from its live authoritative table and emits it into the fabric
as a browsable projection, so the manifest's `publishes` edges are actual data
flowing into BonfyreFS, not a diagram.

Only facts with real backing data are emitted. An organ whose source table is
empty (or absent) publishes an empty fact -- honestly empty, never fabricated.
Facts without a durable store yet (ERPNext Value, HRMS HumanCapacity) are not
invented here; they appear when their app actually produces them.
"""

from __future__ import annotations

import json
import os
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

HERE = os.path.dirname(os.path.abspath(__file__))
CONTROL_DB = os.path.join(HERE, "control_plane.db")


@dataclass
class Fact:
    name: str
    owner: str          # the organ authoritative for it
    members: list[dict]


def _ro(db_path: str) -> sqlite3.Connection:
    return sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)


def _has(db: sqlite3.Connection, table: str) -> bool:
    return db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                      (table,)).fetchone() is not None


def _rows(control_db: str, table: str, sql: str, to_member: Callable) -> list[dict]:
    db = _ro(control_db)
    try:
        if not _has(db, table):
            return []
        return [to_member(r) for r in db.execute(sql)]
    finally:
        db.close()


def f_actor(control_db: str) -> list[dict]:
    return _rows(control_db, "actor_nodes",
                 "SELECT actor_id,display_name,node_kind,confidence,org_id FROM actor_nodes"
                 " ORDER BY actor_id",
                 lambda r: {"id": r[0], "label": r[1] or r[0], "kind": r[2],
                            "confidence": r[3], "org": r[4]})


def f_relation(control_db: str) -> list[dict]:
    return _rows(control_db, "actor_edges",
                 "SELECT from_id,edge_kind,to_id,confidence FROM actor_edges"
                 " ORDER BY from_id,edge_kind,to_id",
                 lambda r: {"id": f"{r[0]}|{r[1]}|{r[2]}", "from": r[0], "kind": r[1],
                            "to": r[2], "confidence": r[3]})


def f_occurrence(control_db: str) -> list[dict]:
    return _rows(control_db, "external_event_log",
                 "SELECT id,source,actor,event_kind,subject_ref,observed_at FROM external_event_log"
                 " ORDER BY id",
                 lambda r: {"id": str(r[0]), "source": r[1], "actor": r[2],
                            "kind": r[3], "subject": r[4], "observed_at": r[5]})


def f_occurrence_projection(control_db: str) -> list[dict]:
    return _rows(control_db, "occurrence_projection",
                 "SELECT event_id,actor,event_kind,status FROM occurrence_projection"
                 " ORDER BY event_id",
                 lambda r: {"id": str(r[0]), "actor": r[1], "kind": r[2], "status": r[3]})


def f_communication_event(control_db: str) -> list[dict]:
    # CRM's communication events are the inbound replies on the occurrence spine.
    return _rows(control_db, "external_event_log",
                 "SELECT id,actor,subject_ref,observed_at FROM external_event_log"
                 " WHERE event_kind='inbound_reply' ORDER BY id",
                 lambda r: {"id": str(r[0]), "actor": r[1], "subject": r[2],
                            "observed_at": r[3], "channel": "github"})


def f_corroboration(control_db: str) -> list[dict]:
    return _rows(control_db, "actor_corroborations",
                 "SELECT actor_id,source,evidence_ref,corroborated_at FROM actor_corroborations"
                 " ORDER BY actor_id,source",
                 lambda r: {"id": f"{r[0]}|{r[1]}", "actor": r[0], "source": r[1],
                            "evidence": r[2], "at": r[3]})


# fact -> (owning organ, compute). Only store-backed facts with real data live here.
REGISTRY: list[tuple[str, str, Callable[[str], list[dict]]]] = [
    ("Actor", "actor-graph", f_actor),
    ("Relation", "actor-graph", f_relation),
    ("Occurrence", "occurrence-spine", f_occurrence),
    ("OccurrenceProjection", "occurrence-spine", f_occurrence_projection),
    ("CommunicationEvent", "crm", f_communication_event),
    ("Corroboration", "verification-ledger", f_corroboration),
]


def compute_all(control_db: str = CONTROL_DB) -> list[Fact]:
    return [Fact(name, owner, fn(control_db)) for name, owner, fn in REGISTRY]


def materialize(facts: list[Fact], out_dir: str) -> str:
    root = Path(out_dir) / "facts"
    root.mkdir(parents=True, exist_ok=True)
    for f in facts:
        d = root / f.name
        d.mkdir(parents=True, exist_ok=True)
        (d / "index.json").write_text(json.dumps({
            "fact": f.name, "owner": f.owner, "count": len(f.members),
            "members": f.members,
        }, indent=2, sort_keys=True))
    (root / "index.json").write_text(json.dumps({
        "facts": [{"fact": f.name, "owner": f.owner, "count": len(f.members)} for f in facts],
    }, indent=2, sort_keys=True))
    return str(root)


def publish(control_db: str = CONTROL_DB, out_dir: str | None = None) -> dict:
    """Compute and materialize the fact projection tree (always runs)."""
    import fabric_publish as fp
    out_dir = out_dir or str(fp.PROJECTIONS)
    facts = compute_all(control_db)
    root = materialize(facts, out_dir)
    return {"facts": {f.name: len(f.members) for f in facts}, "root": root}


def register_in_fabric(control_db: str = CONTROL_DB, timeout_ms: int = 2000) -> dict:
    """Opt-in: register each fact projection in fabric.db so BonfyreFS serves it.
    Single bounded write; fails fast if the live daemon holds the db."""
    import fabric_publish as fp
    import hashlib
    facts = compute_all(control_db)
    root = materialize(facts, str(fp.PROJECTIONS))
    published, skipped = [], []
    try:
        db = sqlite3.connect(str(fp.FABRIC), timeout=timeout_ms / 1000.0)
        db.execute(f"PRAGMA busy_timeout={timeout_ms}")
        fp.ensure_schema(db)
        now = fp._iso()
        for f in facts:
            path = Path(root) / f.name / "index.json"
            data = path.read_bytes()
            digest = hashlib.sha256(data).hexdigest()
            uri = f"bonfyre://artifact/{digest}"
            db.execute(
                "INSERT INTO namespace_objects"
                "(uri,kind,owner,source_authority,native_id,version,locator,policy,"
                " sensitivity,freshness,evidence_state,operations,content_contract,"
                " query_contract,effect_contract,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
                " ON CONFLICT(uri) DO UPDATE SET freshness='current'",
                (uri, "fact", "local-user", fp.SOURCE_AUTHORITY, f"fact-{f.name}", "1",
                 uri, "default", "standard", "current", "measured", "read",
                 f"fact.{f.name}.v1", "typed-lookup.v1", "governed", now))
            db.execute(
                "INSERT INTO artifacts(digest,uri,media_type,source_uri,locator,bytes,"
                "representation,created_at) VALUES(?,?,?,?,?,?,?,?)"
                " ON CONFLICT(digest) DO UPDATE SET locator=excluded.locator",
                (digest, uri, "application/json", str(path), str(path), len(data),
                 "zero-copy-reference", now))
            published.append(f"fact-{f.name}")
        db.commit()
        db.close()
    except sqlite3.OperationalError:
        skipped, published = [f.name for f in facts], []
    return {"published": published, "skipped_fabric_busy": skipped, "root": root}


if __name__ == "__main__":
    cp = sys.argv[1] if len(sys.argv) > 1 else CONTROL_DB
    for f in compute_all(cp):
        print(f"{f.name:22s} {len(f.members):3d}  (owner: {f.owner})")
