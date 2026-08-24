"""ShadowInstitutionGraph: what we know about an institution before a relationship.

The atlas is emphatic that this is not a ForeignTwin (an externally-owned
*resource* projection) and not authority. A ShadowInstitution is epistemic: a
public, observed picture of an institution -- its apparent hierarchy, its public
actors, forms, and resources -- assembled before any relationship or authority
is established. Observing that a person chairs a committee is not being granted
anything by them.

Its whole value is a conversion pipeline, and that pipeline is decided against
real relationship state, not asserted:

    observed                 recorded from public sources
    shadow                   public structure mapped (attachments present)
    relationship_established a relationship with the contact reached >= engaged
    active                   that relationship reached >= collaborated

So a shadow institution becomes active only when RelationshipGraph says a real
relationship advanced -- never because the public picture got more detailed.
"""

from __future__ import annotations

import datetime as dt
import json
import sqlite3
from dataclasses import dataclass, field
from typing import Optional, Sequence

import relationship as rel

UTC = dt.timezone.utc

OBSERVED = "observed"
SHADOW = "shadow"
RELATIONSHIP_ESTABLISHED = "relationship_established"
ACTIVE = "active"
ORDER = (OBSERVED, SHADOW, RELATIONSHIP_ESTABLISHED, ACTIVE)

SCHEMA = """
CREATE TABLE IF NOT EXISTS shadow_institutions(
  inst_id TEXT PRIMARY KEY,
  name TEXT NOT NULL DEFAULT '',
  contact_actor TEXT NOT NULL DEFAULT '',
  profile TEXT NOT NULL DEFAULT '',
  observed_hierarchy TEXT NOT NULL DEFAULT '[]',
  public_attachments TEXT NOT NULL DEFAULT '[]',
  evidence TEXT NOT NULL DEFAULT '',
  updated_at TEXT NOT NULL
);
"""


def _iso(m: dt.datetime) -> str:
    return m.astimezone(UTC).replace(microsecond=0).isoformat()


def ensure_schema(db: sqlite3.Connection) -> None:
    db.executescript(SCHEMA)
    db.commit()


@dataclass(frozen=True)
class ShadowInstitution:
    inst_id: str
    name: str = ""
    contact_actor: str = ""
    profile: str = ""
    observed_hierarchy: Sequence[str] = field(default_factory=tuple)
    public_attachments: Sequence[str] = field(default_factory=tuple)
    evidence: str = ""


def record(db: sqlite3.Connection, inst: ShadowInstitution, now: Optional[dt.datetime] = None) -> None:
    ensure_schema(db)
    db.execute(
        "INSERT INTO shadow_institutions"
        "(inst_id,name,contact_actor,profile,observed_hierarchy,public_attachments,evidence,updated_at)"
        " VALUES(?,?,?,?,?,?,?,?)"
        " ON CONFLICT(inst_id) DO UPDATE SET name=excluded.name,"
        "  contact_actor=excluded.contact_actor, profile=excluded.profile,"
        "  observed_hierarchy=excluded.observed_hierarchy,"
        "  public_attachments=excluded.public_attachments, evidence=excluded.evidence,"
        "  updated_at=excluded.updated_at",
        (inst.inst_id, inst.name, inst.contact_actor, inst.profile,
         json.dumps(list(inst.observed_hierarchy)), json.dumps(list(inst.public_attachments)),
         inst.evidence, _iso(now or dt.datetime.now(UTC))),
    )
    db.commit()


@dataclass(frozen=True)
class ConversionStage:
    inst_id: str
    stage: str
    detail: str


def conversion_stage(db: sqlite3.Connection, inst_id: str) -> ConversionStage:
    """The furthest conversion stage this shadow institution has reached.

    Observation stages come from the shadow record; the relationship stages come
    from RelationshipGraph -- so 'active' requires a real, advanced relationship,
    not a richer public picture."""
    if not _table_exists(db, "shadow_institutions"):
        return ConversionStage(inst_id, OBSERVED, "not recorded")
    row = db.execute(
        "SELECT contact_actor,profile,public_attachments FROM shadow_institutions WHERE inst_id=?",
        (inst_id,),
    ).fetchone()
    if row is None:
        return ConversionStage(inst_id, OBSERVED, "not recorded")
    contact, profile, attachments_json = row
    attachments = json.loads(attachments_json or "[]")

    if not attachments:
        return ConversionStage(inst_id, OBSERVED, "public structure not yet mapped")

    # relationship stages are decided by the relationship graph, never assumed.
    established = contact and rel.stage_at_least(db, contact, profile, "engaged")
    if not established:
        return ConversionStage(inst_id, SHADOW, "no relationship at >= engaged")
    collaborated = rel.stage_at_least(db, contact, profile, "collaborated")
    if not collaborated:
        return ConversionStage(inst_id, RELATIONSHIP_ESTABLISHED, "relationship engaged, not yet collaborated")
    return ConversionStage(inst_id, ACTIVE, "relationship collaborated")


def is_active(db: sqlite3.Connection, inst_id: str) -> bool:
    return conversion_stage(db, inst_id).stage == ACTIVE


def _table_exists(db: sqlite3.Connection, name: str) -> bool:
    return db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone() is not None
