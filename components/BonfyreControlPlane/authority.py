"""AuthorityGraph: who may do what, to what, for what purpose, until when.

The atlas carried this at ``architectural`` while many other architectures
already leaned on it through forbidden inferences -- ``affiliation -> authority``,
``available -> authorized``, ``service active -> bind authority``. The
opportunity engine could not even decide an ``authority`` blocker, because there
was no graph to ask. This builds it, and makes that blocker decidable.

An AuthorityEdge is a typed, scoped, expiring grant with evidence behind it. A
permission is never assumed: an actor holds it only through a non-revoked,
in-window edge for that exact subject and (when named) that purpose. Two laws:

  * authority expires and can be revoked -- an edge outside its window or marked
    revoked grants nothing;
  * delegation composes only under constrained scope -- an actor may re-grant a
    permission only from an edge that is itself delegable, and only within its
    scope.

Nothing here infers authority from affiliation, availability, or presence.
"""

from __future__ import annotations

import datetime as dt
import sqlite3
from dataclasses import dataclass
from typing import Optional

UTC = dt.timezone.utc

# The permission vocabulary (atlas AuthorityGraph). Deliberately broad: observe
# and act are not the same right, and neither implies the other.
OBSERVE = "observe"
REFERENCE = "reference"
BIND = "bind"
ACTIVATE = "activate"
ACT = "act"
REVIEW = "review"
APPROVE = "approve"
COMMIT = "commit"
DELEGATE = "delegate"
REVOKE = "revoke"
PUBLISH = "publish"
SPEND = "spend"
SHARE = "share"
DISCLOSE = "disclose"

PERMISSIONS = frozenset({
    OBSERVE, REFERENCE, BIND, ACTIVATE, ACT, REVIEW, APPROVE, COMMIT,
    DELEGATE, REVOKE, PUBLISH, SPEND, SHARE, DISCLOSE,
})

SCHEMA = """
CREATE TABLE IF NOT EXISTS authority_edges(
  edge_id TEXT PRIMARY KEY,
  actor TEXT NOT NULL,
  permission TEXT NOT NULL,
  subject TEXT NOT NULL,
  purpose TEXT NOT NULL DEFAULT '',
  scope TEXT NOT NULL DEFAULT '',
  effective_from TEXT,
  expires_at TEXT,
  delegable INTEGER NOT NULL DEFAULT 0,
  revoked INTEGER NOT NULL DEFAULT 0,
  evidence TEXT NOT NULL DEFAULT '',
  originating_authority TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL
);
"""


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


def ensure_schema(db: sqlite3.Connection) -> None:
    db.executescript(SCHEMA)
    db.commit()


@dataclass(frozen=True)
class AuthorityEdge:
    edge_id: str
    actor: str
    permission: str
    subject: str
    purpose: str = ""
    scope: str = ""
    effective_from: Optional[str] = None
    expires_at: Optional[str] = None
    delegable: bool = False
    evidence: str = ""
    originating_authority: str = ""

    def __post_init__(self) -> None:
        if self.permission not in PERMISSIONS:
            raise ValueError(f"unknown permission {self.permission!r}")


def grant(db: sqlite3.Connection, edge: AuthorityEdge, now: Optional[dt.datetime] = None) -> None:
    ensure_schema(db)
    db.execute(
        "INSERT INTO authority_edges"
        "(edge_id,actor,permission,subject,purpose,scope,effective_from,expires_at,"
        " delegable,revoked,evidence,originating_authority,created_at)"
        " VALUES(?,?,?,?,?,?,?,?,?,0,?,?,?)"
        " ON CONFLICT(edge_id) DO UPDATE SET"
        "  actor=excluded.actor, permission=excluded.permission, subject=excluded.subject,"
        "  purpose=excluded.purpose, scope=excluded.scope, effective_from=excluded.effective_from,"
        "  expires_at=excluded.expires_at, delegable=excluded.delegable, evidence=excluded.evidence,"
        "  originating_authority=excluded.originating_authority",
        (edge.edge_id, edge.actor, edge.permission, edge.subject, edge.purpose, edge.scope,
         edge.effective_from, edge.expires_at, int(edge.delegable), edge.evidence,
         edge.originating_authority, _iso(now or dt.datetime.now(UTC))),
    )
    db.commit()


def revoke(db: sqlite3.Connection, edge_id: str) -> bool:
    ensure_schema(db)
    cur = db.execute("UPDATE authority_edges SET revoked=1 WHERE edge_id=?", (edge_id,))
    db.commit()
    return cur.rowcount > 0


def has_authority(
    db: sqlite3.Connection,
    actor: str,
    permission: str,
    subject: str,
    *,
    purpose: Optional[str] = None,
    at: Optional[dt.datetime] = None,
) -> bool:
    """Does ``actor`` hold ``permission`` over ``subject`` right now?

    True only if a non-revoked edge matches actor+permission+subject, its window
    contains ``at``, and -- when a purpose is asked for -- the edge's purpose is
    empty (general) or matches. Never inferred from anything else.
    """
    if not _table_exists(db, "authority_edges"):
        return False
    moment = at or dt.datetime.now(UTC)
    for row in db.execute(
        "SELECT purpose,effective_from,expires_at FROM authority_edges"
        " WHERE actor=? AND permission=? AND subject=? AND revoked=0",
        (actor, permission, subject),
    ):
        edge_purpose, eff, exp = row
        if purpose is not None and edge_purpose and edge_purpose != purpose:
            continue
        start, end = _parse(eff), _parse(exp)
        if start and moment < start:
            continue
        if end and moment > end:
            continue
        return True
    return False


def delegate(
    db: sqlite3.Connection,
    *,
    from_edge_id: str,
    to_actor: str,
    new_edge_id: str,
    scope: str = "",
    now: Optional[dt.datetime] = None,
) -> Optional[AuthorityEdge]:
    """Re-grant a permission from a delegable edge, within its scope.

    Returns the new edge, or None if the source edge is missing, revoked, or not
    delegable -- delegation never composes from a non-delegable grant.
    """
    ensure_schema(db)
    row = db.execute(
        "SELECT permission,subject,scope,delegable,revoked FROM authority_edges WHERE edge_id=?",
        (from_edge_id,),
    ).fetchone()
    if row is None:
        return None
    permission, subject, src_scope, delegable, revoked = row
    if revoked or not delegable:
        return None
    # constrained scope: the delegated scope may not exceed the source scope.
    if src_scope and scope and scope != src_scope and not scope.startswith(src_scope):
        return None
    edge = AuthorityEdge(
        edge_id=new_edge_id, actor=to_actor, permission=permission, subject=subject,
        scope=scope or src_scope, delegable=False, originating_authority=from_edge_id,
    )
    grant(db, edge, now=now)
    return edge


def _table_exists(db: sqlite3.Connection, name: str) -> bool:
    return db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone() is not None
