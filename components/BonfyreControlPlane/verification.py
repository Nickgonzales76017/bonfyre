"""Verification ledger -- the gated mechanism behind a fortification "verify" action.

Fortification says *verify Tarbell Center* because 279 live conclusions rest on
an only-asserted actor. This is what makes acting on that possible without
laundering the confidence tier: an actor is promoted asserted -> verified only
when it carries enough INDEPENDENT corroboration, and never on its own say-so.

  * a corroboration is (actor, source, evidence_ref) -- who/what attests it, and
    the artifact that backs the attestation;
  * independence is by distinct source: two attestations from the same source are
    one corroboration, so an actor cannot bootstrap its own verification;
  * promotion requires >= k independent sources (default 2). Below that the actor
    stays asserted and verification_gap() says how many more are needed.

Promotion writes back through actors.upsert_actor, so a verified actor drops off
the CollapseFront verify list -- fortification then shows the risk actually
retired, not merely noted.
"""

from __future__ import annotations

import datetime as dt
import sqlite3
from dataclasses import dataclass

import actors

UTC = dt.timezone.utc
DEFAULT_THRESHOLD = 2

SCHEMA = """
CREATE TABLE IF NOT EXISTS actor_corroborations(
  actor_id TEXT NOT NULL REFERENCES actor_nodes(actor_id),
  source TEXT NOT NULL,
  evidence_ref TEXT NOT NULL DEFAULT '',
  note TEXT NOT NULL DEFAULT '',
  corroborated_at TEXT NOT NULL,
  PRIMARY KEY(actor_id, source)
);
CREATE INDEX IF NOT EXISTS idx_corroboration_actor ON actor_corroborations(actor_id);
"""


def _iso(moment: dt.datetime | None = None) -> str:
    return (moment or dt.datetime.now(UTC)).astimezone(UTC).replace(microsecond=0).isoformat()


def ensure_schema(db: sqlite3.Connection) -> None:
    db.executescript(SCHEMA)
    db.execute("PRAGMA foreign_keys=ON")
    db.commit()


@dataclass(frozen=True)
class VerificationState:
    actor_id: str
    confidence: str
    independent_sources: int
    threshold: int

    @property
    def gap(self) -> int:
        return max(self.threshold - self.independent_sources, 0)

    @property
    def promotable(self) -> bool:
        return self.independent_sources >= self.threshold


def record_corroboration(
    db: sqlite3.Connection,
    actor_id: str,
    *,
    source: str,
    evidence_ref: str = "",
    note: str = "",
    now: dt.datetime | None = None,
) -> bool:
    """Record one attestation. Returns True if it is a NEW independent source.

    A repeated source updates its evidence in place and does not raise the
    independent-source count -- an actor cannot corroborate itself twice.
    """
    ensure_schema(db)
    if not source:
        raise ValueError("a corroboration must name its source")
    exists = db.execute(
        "SELECT 1 FROM actor_corroborations WHERE actor_id=? AND source=?",
        (actor_id, source),
    ).fetchone()
    db.execute(
        "INSERT INTO actor_corroborations(actor_id,source,evidence_ref,note,corroborated_at)"
        " VALUES(?,?,?,?,?)"
        " ON CONFLICT(actor_id,source) DO UPDATE SET"
        "  evidence_ref=excluded.evidence_ref, note=excluded.note",
        (actor_id, source, evidence_ref, note, _iso(now)),
    )
    db.commit()
    return exists is None


def independent_sources(db: sqlite3.Connection, actor_id: str) -> int:
    ensure_schema(db)
    row = db.execute(
        "SELECT COUNT(DISTINCT source) FROM actor_corroborations WHERE actor_id=?",
        (actor_id,),
    ).fetchone()
    return int(row[0])


def _confidence(db: sqlite3.Connection, actor_id: str) -> str:
    row = db.execute(
        "SELECT confidence FROM actor_nodes WHERE actor_id=?", (actor_id,)
    ).fetchone()
    return row[0] if row else "asserted"


def verification_state(
    db: sqlite3.Connection, actor_id: str, threshold: int = DEFAULT_THRESHOLD
) -> VerificationState:
    return VerificationState(
        actor_id=actor_id,
        confidence=_confidence(db, actor_id),
        independent_sources=independent_sources(db, actor_id),
        threshold=threshold,
    )


def promote_if_corroborated(
    db: sqlite3.Connection,
    actor_id: str,
    *,
    threshold: int = DEFAULT_THRESHOLD,
    now: dt.datetime | None = None,
) -> VerificationState:
    """Promote asserted -> verified iff independently corroborated to threshold.

    Idempotent, monotone: never demotes, never promotes below threshold. Rebuilds
    the actor row through actors.upsert_actor so the confidence change is a normal
    graph write the rest of the system already understands.
    """
    state = verification_state(db, actor_id, threshold)
    if state.confidence == "verified" or not state.promotable:
        return state
    row = db.execute(
        "SELECT actor_id,node_kind,display_name,role,org_id,provenance,detail"
        " FROM actor_nodes WHERE actor_id=?",
        (actor_id,),
    ).fetchone()
    if row is None:
        return state
    sources = [
        r[0] for r in db.execute(
            "SELECT source FROM actor_corroborations WHERE actor_id=? ORDER BY source",
            (actor_id,),
        )
    ]
    actors.upsert_actor(db, actors.Actor(
        actor_id=row[0], node_kind=row[1], display_name=row[2], role=row[3] or "",
        org_id=row[4], confidence=actors.VERIFIED,
        provenance=f"corroborated by {len(sources)} independent sources: {', '.join(sources)}",
        detail=row[6] or "",
    ), now=now)
    return verification_state(db, actor_id, threshold)


def verification_gaps(
    db: sqlite3.Connection, actor_ids: list[str], threshold: int = DEFAULT_THRESHOLD
) -> dict[str, VerificationState]:
    return {aid: verification_state(db, aid, threshold) for aid in actor_ids}
