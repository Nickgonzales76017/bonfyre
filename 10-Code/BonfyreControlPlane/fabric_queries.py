"""Virtual query directories (riff SS38), served live by BonfyreFS.

These directories are not stored. Each is a maintained set computed from real
state -- the same analyses the wiring map declares (CollapseFront, fortification,
verification, the occurrence spine, the atlas wiring gaps) -- materialized as a
browsable tree under the fabric and published into fabric.db so BonfyreFS serves
it. Re-run to refresh; the set reflects current truth, never a stored copy.

  /queries/
    Unverified-Actors/      actors a human has not confirmed
    Load-Bearing/           actors many live conclusions rest on (CollapseFront)
    Ready-To-Verify/        fortification verify targets, by leverage
    Pending-Occurrences/    observed, not yet folded
    Fragile-Capacity/       value conclusions on a single point of failure
    Wiring-Gaps/            orphan producers/consumers from the atlas wiring analysis

Every member is real and traceable to its source table or analysis; nothing here
is fabricated to fill a directory.
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
REPO = os.path.dirname(os.path.dirname(HERE))
CONTROL_DB = os.path.join(HERE, "control_plane.db")
ATLAS_INDEX = os.path.join(REPO, "architecture", "atlas.index.json")


@dataclass
class QuerySet:
    name: str
    description: str
    members: list[dict]


def _ro(db_path: str) -> sqlite3.Connection:
    return sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)


def _table_exists(db: sqlite3.Connection, name: str) -> bool:
    return db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone() is not None


def q_unverified_actors(control_db: str) -> list[dict]:
    db = _ro(control_db)
    try:
        if not _table_exists(db, "actor_nodes"):
            return []
        rows = db.execute(
            "SELECT actor_id, display_name, node_kind, org_id FROM actor_nodes"
            " WHERE confidence!='verified' ORDER BY actor_id"
        ).fetchall()
    finally:
        db.close()
    return [{"id": r[0], "label": r[1] or r[0], "kind": r[2], "org": r[3]} for r in rows]


def q_pending_occurrences(control_db: str) -> list[dict]:
    db = _ro(control_db)
    try:
        if not _table_exists(db, "external_event_log"):
            return []
        rows = db.execute(
            "SELECT id, actor, event_kind, subject_ref FROM external_event_log"
            " WHERE projected_at IS NULL ORDER BY id"
        ).fetchall()
    finally:
        db.close()
    return [{"id": str(r[0]), "label": f"{r[1]} {r[2]}", "actor": r[1],
             "kind": r[2], "subject": r[3]} for r in rows]


_CACHE: dict[str, tuple] = {}


def _lattice_matrix(control_db: str):
    """Build the lattice and its collapse matrix ONCE per db (expensive: a fixpoint
    per ground). Both load-bearing and fragile-capacity derive from this, so we
    never recompute a full collapse per conclusion."""
    if control_db in _CACHE:
        return _CACHE[control_db]
    import support_lattice as sl
    db = _ro(control_db)
    try:
        # 2-hop lattice: fast enough for a live query directory, and load-bearing /
        # fragility are already well captured at 2 hops (funds->employs chains).
        lat = sl.build_lattice(db, max_path_len=2)
    finally:
        db.close()
    matrix = sl.collapse_matrix(lat)  # {ground_label: set(collapsed conclusion ids)}
    # invert once: conclusion id -> the ground labels whose solo retraction collapses it
    critical: dict[str, list[str]] = {}
    for ground_label, collapsed in matrix.items():
        for c in collapsed:
            critical.setdefault(c, []).append(ground_label)
    _CACHE[control_db] = (sl, lat, matrix, critical)
    return _CACHE[control_db]


def q_load_bearing(control_db: str) -> list[dict]:
    try:
        _sl, _lat, matrix, _crit = _lattice_matrix(control_db)
    except Exception:
        return []
    ranked = sorted(((lbl, len(c)) for lbl, c in matrix.items()), key=lambda x: x[1], reverse=True)
    return [{"id": lbl, "label": lbl, "leverage": n} for lbl, n in ranked[:20] if n > 1]


def q_ready_to_verify(control_db: str) -> list[dict]:
    """Verify targets: asserted (unconfirmed) actors ranked by leverage, straight
    from the cached collapse matrix + actor confidence + the corroboration ledger.
    Avoids fortification.build_plan (which recomputes a full 3-hop collapse)."""
    try:
        _sl, _lat, matrix, _crit = _lattice_matrix(control_db)
    except Exception:
        return []
    db = _ro(control_db)
    try:
        meta = {r[0]: {"display": r[1] or r[0], "confidence": r[2] or "asserted"}
                for r in db.execute("SELECT actor_id, display_name, confidence FROM actor_nodes")}
        corr = {}
        if _table_exists(db, "actor_corroborations"):
            corr = {r[0]: r[1] for r in db.execute(
                "SELECT actor_id, COUNT(DISTINCT source) FROM actor_corroborations GROUP BY actor_id")}
    finally:
        db.close()
    out = []
    for aid, collapsed in matrix.items():
        m = meta.get(aid)
        if not m or m["confidence"] == "verified" or len(collapsed) <= 1:
            continue
        c = corr.get(aid, 0)
        out.append({"id": aid, "label": m["display"], "leverage": len(collapsed),
                    "corroborations": c, "gap": max(2 - c, 0)})
    out.sort(key=lambda d: d["leverage"], reverse=True)
    return out[:15]


def q_fragile_capacity(control_db: str) -> list[dict]:
    try:
        _sl, lat, _matrix, critical = _lattice_matrix(control_db)
    except Exception:
        return []
    out = []
    for c in sorted(lat.conclusions):
        if not c.startswith("reach:"):
            continue
        crit = sorted(critical.get(c, []))
        if 0 < len(crit) <= 2:
            out.append({"id": c, "label": lat.labels.get(c, c), "critical_support": crit})
        if len(out) >= 40:
            break
    return out


def q_wiring_gaps(_control_db: str) -> list[dict]:
    if not os.path.exists(ATLAS_INDEX):
        return []
    sys.path.insert(0, os.path.join(REPO, "architecture"))
    try:
        import wiring
        fr = wiring.fact_report(ATLAS_INDEX)
    except Exception:
        return []
    out = []
    for f, cons in fr["orphan_consumers"]:
        out.append({"id": f"consume:{f}", "label": f, "gap": "orphan_consumer",
                    "detail": f"expected by {', '.join(cons)}, provided by nobody"})
    for f, prod in fr["orphan_producers"]:
        out.append({"id": f"produce:{f}", "label": f, "gap": "orphan_producer",
                    "detail": f"produced by {', '.join(prod)}, consumed by nobody"})
    return out


REGISTRY: list[tuple[str, str, Callable[[str], list[dict]]]] = [
    ("Unverified-Actors", "Actors a human has not confirmed (confidence != verified).", q_unverified_actors),
    ("Load-Bearing", "Actors many live conclusions rest on -- CollapseFront leverage.", q_load_bearing),
    ("Ready-To-Verify", "Fortification verify targets, ranked by leverage, with corroboration gap.", q_ready_to_verify),
    ("Pending-Occurrences", "Observed occurrences not yet folded into campaign state.", q_pending_occurrences),
    ("Fragile-Capacity", "Value conclusions standing on a single point of failure.", q_fragile_capacity),
    ("Wiring-Gaps", "Orphan producers/consumers from the atlas wiring analysis.", q_wiring_gaps),
]


def compute_all(control_db: str = CONTROL_DB) -> list[QuerySet]:
    return [QuerySet(name, desc, fn(control_db)) for name, desc, fn in REGISTRY]


def materialize(sets: list[QuerySet], out_dir: str) -> str:
    """Write the query tree: queries/<Set>/index.json + one file per member."""
    root = Path(out_dir) / "queries"
    root.mkdir(parents=True, exist_ok=True)
    for qs in sets:
        d = root / qs.name
        d.mkdir(parents=True, exist_ok=True)
        (d / "index.json").write_text(json.dumps({
            "name": qs.name, "description": qs.description,
            "count": len(qs.members), "members": qs.members,
        }, indent=2, sort_keys=True))
    (root / "index.json").write_text(json.dumps({
        "queries": [{"name": qs.name, "description": qs.description, "count": len(qs.members)}
                    for qs in sets],
    }, indent=2, sort_keys=True))
    return str(root)


def publish(control_db: str = CONTROL_DB, out_dir: str | None = None) -> dict:
    """Compute and materialize the browsable query tree. This is the primary
    deliverable and always runs -- the maintained sets as real files under the
    fabric projections dir (the POSIX view; Finder/CLI browse it directly)."""
    import fabric_publish as fp
    out_dir = out_dir or str(fp.PROJECTIONS)
    sets = compute_all(control_db)
    root = materialize(sets, out_dir)
    return {"sets": {qs.name: len(qs.members) for qs in sets}, "root": root}


def register_in_fabric(control_db: str = CONTROL_DB, timeout_ms: int = 2000) -> dict:
    """Opt-in: register each query set in fabric.db so the BonfyreFS namespace
    serves it. The live daemon holds the database, so this is a SINGLE bounded
    write (no long retry) that fails fast if the db is busy -- never forces the
    daemon off it. Run when the daemon is idle or down."""
    import fabric_publish as fp
    import hashlib
    sets = compute_all(control_db)
    root = materialize(sets, str(fp.PROJECTIONS))
    published, skipped = [], []
    try:
        db = sqlite3.connect(str(fp.FABRIC), timeout=timeout_ms / 1000.0)
        db.execute(f"PRAGMA busy_timeout={timeout_ms}")
        fp.ensure_schema(db)
        now = fp._iso()
        for qs in sets:
            path = Path(root) / qs.name / "index.json"
            data = path.read_bytes()
            digest = hashlib.sha256(data).hexdigest()
            uri = f"bonfyre://artifact/{digest}"
            db.execute(
                "INSERT INTO namespace_objects"
                "(uri,kind,owner,source_authority,native_id,version,locator,policy,"
                " sensitivity,freshness,evidence_state,operations,content_contract,"
                " query_contract,effect_contract,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
                " ON CONFLICT(uri) DO UPDATE SET freshness='current'",
                (uri, "collection", "local-user", fp.SOURCE_AUTHORITY, f"query-{qs.name}",
                 "1", uri, "default", "standard", "current", "measured", "read",
                 "fabric-query.v1", "typed-lookup.v1", "governed", now))
            db.execute(
                "INSERT INTO artifacts(digest,uri,media_type,source_uri,locator,bytes,"
                "representation,created_at) VALUES(?,?,?,?,?,?,?,?)"
                " ON CONFLICT(digest) DO UPDATE SET locator=excluded.locator",
                (digest, uri, "application/json", str(path), str(path), len(data),
                 "zero-copy-reference", now))
            published.append(f"query-{qs.name}")
        db.commit()  # the daemon holds the db; if this fails nothing persisted
        db.close()
    except sqlite3.OperationalError:
        # commit/insert lost the race with the live daemon -> nothing was written
        skipped, published = [qs.name for qs in sets], []
    return {"published": published, "skipped_fabric_busy": skipped, "root": root}


if __name__ == "__main__":
    cp = sys.argv[1] if len(sys.argv) > 1 else CONTROL_DB
    for qs in compute_all(cp):
        print(f"{qs.name:22s} {len(qs.members):3d}  {qs.description}")
