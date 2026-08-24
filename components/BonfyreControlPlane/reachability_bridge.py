"""The live fabric -> DBSP delta bridge, with backfill from previous collection.

This is where reachability stops being computed in Python and starts being
maintained by the DBSP relation. It reads every opportunity's blockers, resolves
each against the REAL accumulated state -- verified actors, the proof frontier,
authority grants, relationship stages, resource activation, and the fabric's own
workgraph_nodes -- and feeds the currently-resolved facts to the DBSP
ReachableCapacity engine (reachable_capacity_live). The engine returns the
reachable set.

"Backfill from previous collection" is the whole point of the seed pass: the
resolved facts are not just new events, they are the full current truth folded
out of control_plane.db and the fabric. So the maintained relation starts from
everything already known, not from empty.

The bridge's output is checked to reproduce opportunity.reachable_capacity's
answer exactly -- so the DBSP relation, not the Python loop, becomes the source
of truth, and publish_reachability becomes pure export.
"""

from __future__ import annotations

import json
import sqlite3
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import fabric_publish as fp
import opportunity as opp

CONTROL_DB = Path(__file__).resolve().parent / "control_plane.db"
PACK = (Path(__file__).resolve().parent.parent.parent
        / "packs" / "institutional-opportunities" / "opportunities.yaff")
ENGINE = (Path.home() / ".bonfyre" / "substrates" / "v6.1" / "feldera"
          / "probe" / "target" / "release" / "reachable_capacity_live")


def engine_available() -> bool:
    return ENGINE.exists()


def _blocker_id(b: opp.Blocker) -> str:
    """A stable id for a blocker: the same real-world blocker across opportunities
    maps to the same id, so a single resolved fact clears it everywhere."""
    return "|".join([b.kind, b.subject, b.profile, b.layer, b.actor, b.permission])


def build_facts(
    control_db: Path = CONTROL_DB, pack: Path = PACK,
    *, bound_services: frozenset[str] = frozenset(),
) -> str:
    """Fold current state into DBSP facts: B lines (structure) and R lines
    (blockers resolved by the backfilled real state)."""
    opps, _unlocks = opp.load_pack(pack.read_text())
    control = sqlite3.connect(str(control_db)) if control_db.exists() else sqlite3.connect(":memory:")
    fabric = sqlite3.connect(str(fp.FABRIC)) if fp.FABRIC.exists() else None
    lines: list[str] = []
    seen_resolved: set[str] = set()
    try:
        for o in opps:
            for b in o.blockers:
                bid = _blocker_id(b)
                lines.append(f"B\t{o.opp_id}\t{bid}")
                if opp.blocker_resolved(control, b, bound_services=bound_services,
                                        fabric_db=fabric) is True and bid not in seen_resolved:
                    lines.append(f"R\t{bid}")
                    seen_resolved.add(bid)
    finally:
        control.close()
        if fabric is not None:
            fabric.close()
    return "\n".join(lines)


@dataclass(frozen=True)
class BridgeResult:
    reachable: tuple[str, ...]
    count: int


def run_bridge(
    control_db: Path = CONTROL_DB, pack: Path = PACK,
    *, bound_services: frozenset[str] = frozenset(),
) -> BridgeResult:
    """Feed backfilled facts to the DBSP engine and read the reachable set."""
    if not ENGINE.exists():
        raise RuntimeError("reachable_capacity_live engine not built")
    facts = build_facts(control_db, pack, bound_services=bound_services)
    proc = subprocess.run([str(ENGINE)], input=facts, capture_output=True, text=True, timeout=60)
    line = next((l for l in (proc.stdout + proc.stderr).splitlines() if l.strip().startswith("{")), "{}")
    d = json.loads(line)
    return BridgeResult(reachable=tuple(sorted(d.get("reachable", []))), count=d.get("count", 0))


def publish_maintained(
    fabric: Path = fp.FABRIC, control_db: Path = CONTROL_DB, pack: Path = PACK,
) -> fp.Published:
    """Export the DBSP-maintained reachable set into the fabric.

    This is the export side once the relation is maintained: the answer comes
    from the DBSP ReachableCapacity engine over backfilled real state, and this
    just snapshots it as a content-addressed fabric artifact for BonfyreFS. The
    Python computation is no longer the source -- it is a cross-check."""
    import datetime as _dt
    r = run_bridge(control_db, pack)
    data = {
        "source": "feldera-dbsp-maintained",
        "generated_at": _dt.datetime.now(_dt.timezone.utc).replace(microsecond=0)
                        .isoformat().replace("+00:00", "Z"),
        "reachable": list(r.reachable),
        "count": r.count,
    }
    out_dir = Path.home() / ".bonfyre" / "estate-fabric" / "projections"
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "reachable-capacity.json"
    path.write_text(json.dumps(data, indent=2, sort_keys=True))
    db = sqlite3.connect(str(fabric), timeout=60)
    db.execute("PRAGMA busy_timeout=60000")
    fp.ensure_schema(db)
    try:
        return fp.publish_file(db, name="reachable-capacity", content_path=path,
                               content_contract="reachable-capacity.v1", dedupe=True)
    finally:
        db.close()


def python_reachable(
    control_db: Path = CONTROL_DB, pack: Path = PACK,
    *, bound_services: frozenset[str] = frozenset(),
) -> tuple[str, ...]:
    """The Python answer, for cross-checking that DBSP reproduces it."""
    opps, unlocks = opp.load_pack(pack.read_text())
    control = sqlite3.connect(str(control_db)) if control_db.exists() else sqlite3.connect(":memory:")
    fabric = sqlite3.connect(str(fp.FABRIC)) if fp.FABRIC.exists() else None
    try:
        evals = opp.reachable_capacity(control, opps, unlocks,
                                       bound_services=bound_services, fabric_db=fabric)
    finally:
        control.close()
        if fabric is not None:
            fabric.close()
    return tuple(sorted(opp.capacity_summary(evals)[opp.REACHABLE_NOW]))
