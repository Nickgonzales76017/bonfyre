"""RelationshipGraph: where along the discovery-to-adoption trajectory a real
relationship sits -- not a CRM pipeline.

The atlas carried this at ``architectural``, but it is the missing link the
opportunity chain kept pointing at: an ``identity_verification`` blocker is
answered by actor confidence, and confidence is *earned* through a relationship
that advances. This models that advance as an ordered, evidence-bearing
trajectory, per profile, so an opportunity can require "the ACM editor
relationship is at least engaged" and have that decided against real state.

The trajectory is deliberately monotone in intent but not assumed: a stage is
only reached when something records it, with evidence. Two relationships to the
same actor under different profiles are independent -- collaboration on Bernstein
does not make one an ACM editor.
"""

from __future__ import annotations

import datetime as dt
import sqlite3
from dataclasses import dataclass
from typing import Optional

UTC = dt.timezone.utc

# The trajectory, low to high. Reaching a stage does not imply the ones above it;
# it does imply the ones below (you cannot collaborate without having engaged).
STAGES = (
    "discovered", "observed", "contacted", "replied", "engaged", "assigned",
    "reviewed", "corrected", "collaborated", "committed", "adopted", "converted",
)
_RANK = {name: i for i, name in enumerate(STAGES)}

SCHEMA = """
CREATE TABLE IF NOT EXISTS relationships(
  rel_id TEXT PRIMARY KEY,
  actor TEXT NOT NULL,
  profile TEXT NOT NULL DEFAULT '',
  stage TEXT NOT NULL DEFAULT 'discovered',
  evidence TEXT NOT NULL DEFAULT '',
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS relationship_transitions(
  rel_id TEXT NOT NULL,
  from_stage TEXT NOT NULL,
  to_stage TEXT NOT NULL,
  evidence TEXT NOT NULL DEFAULT '',
  at TEXT NOT NULL
);
"""


def _iso(moment: dt.datetime) -> str:
    return moment.astimezone(UTC).replace(microsecond=0).isoformat()


def ensure_schema(db: sqlite3.Connection) -> None:
    db.executescript(SCHEMA)
    db.commit()


@dataclass(frozen=True)
class Relationship:
    rel_id: str
    actor: str
    profile: str = ""
    stage: str = "discovered"
    evidence: str = ""

    def __post_init__(self) -> None:
        if self.stage not in _RANK:
            raise ValueError(f"unknown stage {self.stage!r}")


def record(db: sqlite3.Connection, rel: Relationship, now: Optional[dt.datetime] = None) -> None:
    ensure_schema(db)
    db.execute(
        "INSERT INTO relationships(rel_id,actor,profile,stage,evidence,updated_at)"
        " VALUES(?,?,?,?,?,?)"
        " ON CONFLICT(rel_id) DO UPDATE SET actor=excluded.actor, profile=excluded.profile,"
        "  stage=excluded.stage, evidence=excluded.evidence, updated_at=excluded.updated_at",
        (rel.rel_id, rel.actor, rel.profile, rel.stage, rel.evidence,
         _iso(now or dt.datetime.now(UTC))),
    )
    db.commit()


def advance(
    db: sqlite3.Connection, rel_id: str, to_stage: str, *,
    evidence: str = "", now: Optional[dt.datetime] = None, allow_regress: bool = False,
) -> bool:
    """Move a relationship to a stage, recording the transition with evidence.

    Refuses to move backward unless explicitly allowed (a relationship can be
    corrected, but that is a deliberate act, not a silent slip)."""
    if to_stage not in _RANK:
        raise ValueError(f"unknown stage {to_stage!r}")
    ensure_schema(db)
    row = db.execute("SELECT stage FROM relationships WHERE rel_id=?", (rel_id,)).fetchone()
    if row is None:
        return False
    current = row[0]
    if not allow_regress and _RANK[to_stage] < _RANK[current]:
        return False
    moment = _iso(now or dt.datetime.now(UTC))
    db.execute("UPDATE relationships SET stage=?, evidence=?, updated_at=? WHERE rel_id=?",
               (to_stage, evidence, moment, rel_id))
    db.execute("INSERT INTO relationship_transitions(rel_id,from_stage,to_stage,evidence,at)"
               " VALUES(?,?,?,?,?)", (rel_id, current, to_stage, evidence, moment))
    db.commit()
    return True


def stage_at_least(db: sqlite3.Connection, actor: str, profile: str, stage: str) -> Optional[bool]:
    """Has any relationship with this actor under this profile reached at least
    ``stage``? None when there is no relationship recorded at all (unknown, never
    assumed)."""
    if not _table_exists(db, "relationships") or stage not in _RANK:
        return None
    rows = db.execute(
        "SELECT stage FROM relationships WHERE actor=? AND profile=?", (actor, profile)
    ).fetchall()
    if not rows:
        return None
    return any(_RANK[r[0]] >= _RANK[stage] for r in rows)


def trajectory(db: sqlite3.Connection, rel_id: str) -> list[dict]:
    if not _table_exists(db, "relationship_transitions"):
        return []
    return [
        {"from": r[0], "to": r[1], "evidence": r[2], "at": r[3]}
        for r in db.execute(
            "SELECT from_stage,to_stage,evidence,at FROM relationship_transitions"
            " WHERE rel_id=? ORDER BY at", (rel_id,))
    ]


def _table_exists(db: sqlite3.Connection, name: str) -> bool:
    return db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone() is not None
