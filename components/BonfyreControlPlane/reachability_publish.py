"""Publish live reachable capacity into the fabric BonfyreFS serves.

The opportunity engine already computes, over real state, which opportunities are
reachable now, which are unlockable, and which are blocked and why. That answer
was trapped in the control plane. This projects it into the fabric as a content
artifact, so ``/tmp/estate-mnt`` shows what is reachable right now -- the same
mount that shows missions and the architecture atlas.

Re-running it after any state change (a verification, a proven layer, a granted
authority) republishes the current answer. That is the Feldera role expressed at
the coarse grain the control plane can honor today: the consequence of a change
to the underlying state, surfaced as a live file. A true incremental view over
the fabric's own tables is the next refinement.
"""

from __future__ import annotations

import datetime as dt
import json
import sqlite3
from pathlib import Path

import fabric_publish as fp
import opportunity as opp

CONTROL_DB = Path(__file__).resolve().parent / "control_plane.db"
PACK = (Path(__file__).resolve().parent.parent.parent
        / "packs" / "institutional-opportunities" / "opportunities.yaff")
PROJECTIONS = Path.home() / ".bonfyre" / "estate-fabric" / "projections"


def build_reachability(
    control_db: Path = CONTROL_DB, pack: Path = PACK, fabric: Path = fp.FABRIC
) -> dict:
    """Compute the current reachable capacity from the opportunities pack.

    work_done blockers are decided against the real fabric WorkGraph when the
    fabric is present, so reachability reflects the system's own work, not a
    shadow copy."""
    opps, unlocks = opp.load_pack(pack.read_text())
    db = sqlite3.connect(str(control_db))
    fabric_con = sqlite3.connect(str(fabric)) if fabric.exists() else None
    try:
        evals = opp.reachable_capacity(db, opps, unlocks, fabric_db=fabric_con)
    finally:
        db.close()
        if fabric_con is not None:
            fabric_con.close()
    return {
        "generated_at": dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
                        .isoformat().replace("+00:00", "Z"),
        "summary": opp.capacity_summary(evals),
        "opportunities": {oid: ev.to_dict() for oid, ev in evals.items()},
    }


def write_reachability_file(
    control_db: Path = CONTROL_DB, pack: Path = PACK, out_dir: Path = PROJECTIONS,
    fabric: Path = fp.FABRIC,
) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    data = build_reachability(control_db, pack, fabric)
    path = out_dir / "reachable-capacity.json"
    path.write_text(json.dumps(data, indent=2, sort_keys=True))
    return path


def publish_reachability(
    fabric: Path = fp.FABRIC, control_db: Path = CONTROL_DB, pack: Path = PACK
) -> fp.Published:
    """Snapshot-and-export the current reachability into the fabric.

    The maintained model is the Feldera/DBSP ReachableCapacity relation
    (substrate/feldera_reachable_capacity.rs), which withdraws an opportunity the
    instant a resolving fact retracts. This function is the export side: it takes
    a snapshot of the current answer and writes it as a content-addressed fabric
    artifact for BonfyreFS. Once the DBSP relation is fed the live fabric deltas,
    this stops computing and becomes pure export of the maintained relation."""
    path = write_reachability_file(control_db, pack, fabric=fabric)
    db = sqlite3.connect(str(fabric), timeout=60)
    db.execute("PRAGMA busy_timeout=60000")
    fp.ensure_schema(db)
    try:
        return fp.publish_file(
            db, name="reachable-capacity", content_path=path,
            content_contract="reachable-capacity.v1", dedupe=True)
    finally:
        db.close()
