"""Cross-plane work with a lifecycle, and a plane identity that must exist.

Run 6's cross-plane queue was write-only. At freeze it held 242 rows, every one
of them `open`: nothing was ever claimed, satisfied or closed. 185 of those were
generic `receipt_recursion` fan-out, so each Context Cut re-presented work that
other planes had already done, which is a large part of why the frontier kept
paying to re-read the same state.

Two changes make that structurally impossible rather than tidied up afterward:

1. An item moves through a state machine. Illegal transitions raise instead of
   silently leaving a row `open` forever, and a claim is a *lease* -- a plane
   killed by pipe deadlock or ENOSPC returns its work automatically.
2. `target_plane` is checked against the registered planes at insert time. Run 6
   accumulated an item aimed at a plane called `coordinator`, which never
   existed; the governor could only report `invalid_targets=1` after the fact.
   An unroutable item is now rejected at the boundary.
"""

from __future__ import annotations

import datetime as dt
import sqlite3
from dataclasses import dataclass
from typing import Iterable, Optional, Sequence

UTC = dt.timezone.utc

OPEN = "open"
LEASED = "leased"
EFFECTED = "effected"
SATISFIED = "satisfied"
BLOCKED_EXTERNAL = "blocked_external"
SUPERSEDED = "superseded"
INVALIDATED = "invalidated"
FAILED = "failed"

TERMINAL = frozenset({SATISFIED, SUPERSEDED, INVALIDATED, FAILED})

# The only moves that exist. Anything else is a bug, not a state.
TRANSITIONS: dict[str, frozenset[str]] = {
    OPEN: frozenset({LEASED, BLOCKED_EXTERNAL, SUPERSEDED, INVALIDATED, FAILED}),
    LEASED: frozenset(
        {OPEN, EFFECTED, BLOCKED_EXTERNAL, SUPERSEDED, INVALIDATED, FAILED}
    ),
    EFFECTED: frozenset({SATISFIED, BLOCKED_EXTERNAL, SUPERSEDED, INVALIDATED, FAILED}),
    BLOCKED_EXTERNAL: frozenset({OPEN, SUPERSEDED, INVALIDATED, FAILED}),
    SATISFIED: frozenset(),
    SUPERSEDED: frozenset(),
    INVALIDATED: frozenset(),
    FAILED: frozenset(),
}

DEFAULT_LEASE = dt.timedelta(minutes=45)

SCHEMA = """
CREATE TABLE IF NOT EXISTS work_planes(
  plane_id TEXT PRIMARY KEY,
  active INTEGER NOT NULL DEFAULT 1,
  registered_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS work_items(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at TEXT NOT NULL,
  source_plane TEXT NOT NULL,
  target_plane TEXT NOT NULL REFERENCES work_planes(plane_id),
  item_kind TEXT NOT NULL,
  subject_ref TEXT NOT NULL,
  reason TEXT NOT NULL DEFAULT '',
  source_ref TEXT NOT NULL DEFAULT '',
  priority REAL NOT NULL DEFAULT 0,
  state TEXT NOT NULL DEFAULT 'open',
  leased_by TEXT,
  lease_expires_at TEXT,
  superseded_by INTEGER REFERENCES work_items(id),
  closed_at TEXT,
  detail TEXT NOT NULL DEFAULT '',
  UNIQUE(source_plane,target_plane,item_kind,subject_ref,source_ref)
);
CREATE TABLE IF NOT EXISTS work_transitions(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  item_id INTEGER NOT NULL REFERENCES work_items(id),
  from_state TEXT NOT NULL,
  to_state TEXT NOT NULL,
  actor TEXT NOT NULL DEFAULT '',
  occurred_at TEXT NOT NULL,
  detail TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_work_items_ready
  ON work_items(target_plane, state, priority);
"""


class UnknownPlane(ValueError):
    """Raised when work is aimed at a plane that does not exist."""


class IllegalTransition(ValueError):
    """Raised when a caller tries a move the lifecycle does not allow."""


def _iso(moment: dt.datetime) -> str:
    return moment.astimezone(UTC).replace(microsecond=0).isoformat()


def _parse(text: Optional[str]) -> Optional[dt.datetime]:
    if not text:
        return None
    try:
        parsed = dt.datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


@dataclass(frozen=True)
class WorkItem:
    id: int
    source_plane: str
    target_plane: str
    item_kind: str
    subject_ref: str
    reason: str
    source_ref: str
    priority: float
    state: str
    leased_by: Optional[str] = None
    lease_expires_at: Optional[dt.datetime] = None
    superseded_by: Optional[int] = None


def ensure_schema(db: sqlite3.Connection) -> None:
    db.executescript(SCHEMA)
    db.execute("PRAGMA foreign_keys=ON")
    db.commit()


def register_plane(
    db: sqlite3.Connection, plane_id: str, now: Optional[dt.datetime] = None
) -> None:
    db.execute(
        "INSERT OR IGNORE INTO work_planes(plane_id,active,registered_at) VALUES(?,1,?)",
        (plane_id, _iso(now or dt.datetime.now(UTC))),
    )
    db.commit()


def known_planes(db: sqlite3.Connection) -> set[str]:
    return {
        row[0] for row in db.execute("SELECT plane_id FROM work_planes WHERE active=1")
    }


def _record_transition(
    db: sqlite3.Connection,
    item_id: int,
    from_state: str,
    to_state: str,
    actor: str,
    moment: dt.datetime,
    detail: str = "",
) -> None:
    db.execute(
        "INSERT INTO work_transitions(item_id,from_state,to_state,actor,occurred_at,detail)"
        " VALUES(?,?,?,?,?,?)",
        (item_id, from_state, to_state, actor, _iso(moment), detail),
    )


def enqueue(
    db: sqlite3.Connection,
    *,
    source_plane: str,
    target_plane: str,
    item_kind: str,
    subject_ref: str,
    reason: str = "",
    source_ref: str = "",
    priority: float = 0.0,
    now: Optional[dt.datetime] = None,
) -> Optional[int]:
    """Add work, refusing anything that cannot be routed.

    Returns the new item id, or None when this exact consequence is already
    tracked. Run 6's table carried the same uniqueness constraint, so its 185
    `receipt_recursion` rows were distinct rather than duplicated -- the defect
    was that none of them could ever close, not that they repeated. The guard
    stays because dropping the constraint while adding a lifecycle would turn a
    survivable problem into an unbounded one.
    """
    if target_plane not in known_planes(db):
        raise UnknownPlane(
            f"{target_plane!r} is not a registered plane; "
            f"known: {sorted(known_planes(db))}"
        )
    moment = now or dt.datetime.now(UTC)
    existing = db.execute(
        "SELECT id,state FROM work_items WHERE source_plane=? AND target_plane=?"
        " AND item_kind=? AND subject_ref=? AND source_ref=?",
        (source_plane, target_plane, item_kind, subject_ref, source_ref),
    ).fetchone()
    if existing is not None:
        return None
    cursor = db.execute(
        "INSERT INTO work_items"
        "(created_at,source_plane,target_plane,item_kind,subject_ref,reason,"
        " source_ref,priority,state)"
        " VALUES(?,?,?,?,?,?,?,?,'open')",
        (
            _iso(moment),
            source_plane,
            target_plane,
            item_kind,
            subject_ref,
            reason,
            source_ref,
            priority,
        ),
    )
    item_id = int(cursor.lastrowid)
    _record_transition(db, item_id, "", OPEN, source_plane, moment, reason[:400])
    db.commit()
    return item_id


def _transition(
    db: sqlite3.Connection,
    item_id: int,
    to_state: str,
    actor: str,
    moment: dt.datetime,
    detail: str = "",
    extra: Optional[dict] = None,
) -> None:
    row = db.execute("SELECT state FROM work_items WHERE id=?", (item_id,)).fetchone()
    if row is None:
        raise ValueError(f"no work item {item_id}")
    from_state = row[0]
    if to_state not in TRANSITIONS[from_state]:
        raise IllegalTransition(
            f"item {item_id}: {from_state} -> {to_state} is not a legal move"
        )
    assignments = {"state": to_state}
    if to_state in TERMINAL:
        assignments["closed_at"] = _iso(moment)
    if to_state != LEASED:
        assignments["leased_by"] = None
        assignments["lease_expires_at"] = None
    if extra:
        assignments.update(extra)
    columns = ",".join(f"{name}=?" for name in assignments)
    db.execute(
        f"UPDATE work_items SET {columns} WHERE id=?",
        (*assignments.values(), item_id),
    )
    _record_transition(db, item_id, from_state, to_state, actor, moment, detail)
    db.commit()


def claim(
    db: sqlite3.Connection,
    plane: str,
    *,
    limit: int = 1,
    lease: dt.timedelta = DEFAULT_LEASE,
    now: Optional[dt.datetime] = None,
) -> list[WorkItem]:
    """Lease the highest-priority open work for a plane.

    Expired leases are reclaimed first, so work held by a plane that died never
    becomes permanently invisible.
    """
    moment = now or dt.datetime.now(UTC)
    reap_expired_leases(db, now=moment)
    rows = db.execute(
        "SELECT id FROM work_items WHERE target_plane=? AND state='open'"
        " ORDER BY priority DESC, id ASC LIMIT ?",
        (plane, limit),
    ).fetchall()
    claimed = []
    for (item_id,) in rows:
        _transition(
            db,
            item_id,
            LEASED,
            plane,
            moment,
            extra={
                "leased_by": plane,
                "lease_expires_at": _iso(moment + lease),
            },
        )
        claimed.append(get(db, item_id))
    return claimed


def mark_effected(
    db: sqlite3.Connection,
    item_id: int,
    actor: str,
    detail: str = "",
    now: Optional[dt.datetime] = None,
) -> None:
    _transition(db, item_id, EFFECTED, actor, now or dt.datetime.now(UTC), detail)


def satisfy(
    db: sqlite3.Connection,
    item_id: int,
    actor: str,
    receipt_ref: str,
    now: Optional[dt.datetime] = None,
) -> None:
    """Close work against a receipt. This is the transition Run 6 never had."""
    if not receipt_ref:
        raise ValueError("satisfying work requires a receipt reference")
    _transition(
        db, item_id, SATISFIED, actor, now or dt.datetime.now(UTC), receipt_ref
    )


def block_external(
    db: sqlite3.Connection,
    item_id: int,
    actor: str,
    detail: str,
    now: Optional[dt.datetime] = None,
) -> None:
    _transition(db, item_id, BLOCKED_EXTERNAL, actor, now or dt.datetime.now(UTC), detail)


def unblock(
    db: sqlite3.Connection, item_id: int, actor: str, now: Optional[dt.datetime] = None
) -> None:
    _transition(db, item_id, OPEN, actor, now or dt.datetime.now(UTC))


def supersede(
    db: sqlite3.Connection,
    item_id: int,
    replacement_id: int,
    actor: str,
    now: Optional[dt.datetime] = None,
) -> None:
    _transition(
        db,
        item_id,
        SUPERSEDED,
        actor,
        now or dt.datetime.now(UTC),
        f"superseded by {replacement_id}",
        extra={"superseded_by": replacement_id},
    )


def invalidate(
    db: sqlite3.Connection,
    item_id: int,
    actor: str,
    detail: str,
    now: Optional[dt.datetime] = None,
) -> None:
    _transition(db, item_id, INVALIDATED, actor, now or dt.datetime.now(UTC), detail)


def fail(
    db: sqlite3.Connection,
    item_id: int,
    actor: str,
    detail: str,
    now: Optional[dt.datetime] = None,
) -> None:
    _transition(db, item_id, FAILED, actor, now or dt.datetime.now(UTC), detail)


def reap_expired_leases(
    db: sqlite3.Connection, now: Optional[dt.datetime] = None
) -> int:
    """Return work whose holder never came back."""
    moment = now or dt.datetime.now(UTC)
    rows = db.execute(
        "SELECT id,leased_by FROM work_items WHERE state='leased'"
        " AND lease_expires_at IS NOT NULL AND lease_expires_at <= ?",
        (_iso(moment),),
    ).fetchall()
    for item_id, holder in rows:
        _transition(
            db,
            item_id,
            OPEN,
            "lease-reaper",
            moment,
            f"lease held by {holder} expired",
        )
    return len(rows)


def get(db: sqlite3.Connection, item_id: int) -> WorkItem:
    row = db.execute(
        "SELECT id,source_plane,target_plane,item_kind,subject_ref,reason,source_ref,"
        "priority,state,leased_by,lease_expires_at,superseded_by"
        " FROM work_items WHERE id=?",
        (item_id,),
    ).fetchone()
    if row is None:
        raise ValueError(f"no work item {item_id}")
    return WorkItem(
        id=row[0],
        source_plane=row[1],
        target_plane=row[2],
        item_kind=row[3],
        subject_ref=row[4],
        reason=row[5],
        source_ref=row[6],
        priority=row[7],
        state=row[8],
        leased_by=row[9],
        lease_expires_at=_parse(row[10]),
        superseded_by=row[11],
    )


def open_count(db: sqlite3.Connection, plane: Optional[str] = None) -> int:
    if plane:
        row = db.execute(
            "SELECT COUNT(*) FROM work_items WHERE state='open' AND target_plane=?",
            (plane,),
        ).fetchone()
    else:
        row = db.execute("SELECT COUNT(*) FROM work_items WHERE state='open'").fetchone()
    return int(row[0])


def state_counts(db: sqlite3.Connection) -> dict[str, int]:
    return {
        row[0]: row[1]
        for row in db.execute("SELECT state, COUNT(*) FROM work_items GROUP BY state")
    }
