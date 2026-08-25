"""Actors and the several different kinds of edge between them.

The operating model's second law: BonfyreGraph owns graph mechanics, not graph
meaning. AuthorityEdge, EvidenceEdge, SocialEdge, WorkDependency,
ArtifactLineage, ProviderFallback and OpportunityUnlock all serialize as
`(from, relation, to)` and are not the same thing. Collapsing them into one
"relationship" table is a named future error, so the edge kind is part of the
primary key here and there is no untyped `link()`.

Provenance is mandatory on every node and edge. Run 6's relationship data was
good precisely because it recorded how each contact was found; a CRM that
cannot distinguish "read this on their team page" from "a model inferred it"
degrades into confident nonsense within a few autonomous passes.
"""

from __future__ import annotations

import datetime as dt
import sqlite3
from dataclasses import dataclass
from typing import Iterable, Optional

UTC = dt.timezone.utc

# Node kinds.
ORGANIZATION = "organization"
PERSON = "person"
PUBLICATION = "publication"
PROGRAM = "program"

NODE_KINDS = frozenset({ORGANIZATION, PERSON, PUBLICATION, PROGRAM})

# Edge kinds, kept deliberately distinct.
EMPLOYS = "employs"  # SocialEdge / org chart
AUTHORITY_OVER = "authority_over"  # AuthorityEdge: who can decide
EVIDENCE_FOR = "evidence_for"  # EvidenceEdge: artifact supports claim
FUNDS = "funds"  # MoneyGraph
PUBLISHES_IN = "publishes_in"  # DistributionGraph
CONTACT_OF = "contact_of"  # RelationshipGraph: we know them
OPPORTUNITY_UNLOCK = "opportunity_unlock"  # OpportunityGraph

EDGE_KINDS = frozenset(
    {
        EMPLOYS,
        AUTHORITY_OVER,
        EVIDENCE_FOR,
        FUNDS,
        PUBLISHES_IN,
        CONTACT_OF,
        OPPORTUNITY_UNLOCK,
    }
)

# How well we actually know something. `asserted` means a human or a document
# said it; `inferred` means a model concluded it and it has not been checked.
VERIFIED = "verified"
ASSERTED = "asserted"
INFERRED = "inferred"

CONFIDENCE_LEVELS = frozenset({VERIFIED, ASSERTED, INFERRED})

SCHEMA = """
CREATE TABLE IF NOT EXISTS actor_nodes(
  actor_id TEXT PRIMARY KEY,
  node_kind TEXT NOT NULL,
  display_name TEXT NOT NULL,
  role TEXT NOT NULL DEFAULT '',
  org_id TEXT,
  confidence TEXT NOT NULL DEFAULT 'asserted',
  provenance TEXT NOT NULL,
  detail TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS actor_edges(
  from_id TEXT NOT NULL REFERENCES actor_nodes(actor_id),
  edge_kind TEXT NOT NULL,
  to_id TEXT NOT NULL REFERENCES actor_nodes(actor_id),
  confidence TEXT NOT NULL DEFAULT 'asserted',
  provenance TEXT NOT NULL,
  detail TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL,
  PRIMARY KEY(from_id, edge_kind, to_id)
);
CREATE INDEX IF NOT EXISTS idx_actor_edges_kind ON actor_edges(edge_kind, to_id);
"""


def _iso(moment: dt.datetime) -> str:
    return moment.astimezone(UTC).replace(microsecond=0).isoformat()


@dataclass(frozen=True)
class Actor:
    actor_id: str
    node_kind: str
    display_name: str
    role: str = ""
    org_id: Optional[str] = None
    confidence: str = ASSERTED
    provenance: str = ""
    detail: str = ""

    def __post_init__(self) -> None:
        if self.node_kind not in NODE_KINDS:
            raise ValueError(f"unknown node_kind {self.node_kind!r}")
        if self.confidence not in CONFIDENCE_LEVELS:
            raise ValueError(f"unknown confidence {self.confidence!r}")
        if not self.provenance:
            raise ValueError(f"{self.actor_id}: provenance is mandatory")


def ensure_schema(db: sqlite3.Connection) -> None:
    db.executescript(SCHEMA)
    db.execute("PRAGMA foreign_keys=ON")
    db.commit()


def upsert_actor(
    db: sqlite3.Connection, actor: Actor, now: Optional[dt.datetime] = None
) -> None:
    db.execute(
        "INSERT INTO actor_nodes"
        "(actor_id,node_kind,display_name,role,org_id,confidence,provenance,detail,created_at)"
        " VALUES(?,?,?,?,?,?,?,?,?)"
        " ON CONFLICT(actor_id) DO UPDATE SET"
        "  display_name=excluded.display_name, role=excluded.role,"
        "  org_id=excluded.org_id, confidence=excluded.confidence,"
        "  provenance=excluded.provenance, detail=excluded.detail",
        (
            actor.actor_id,
            actor.node_kind,
            actor.display_name,
            actor.role,
            actor.org_id,
            actor.confidence,
            actor.provenance,
            actor.detail,
            _iso(now or dt.datetime.now(UTC)),
        ),
    )
    db.commit()


def add_edge(
    db: sqlite3.Connection,
    from_id: str,
    edge_kind: str,
    to_id: str,
    *,
    provenance: str,
    confidence: str = ASSERTED,
    detail: str = "",
    now: Optional[dt.datetime] = None,
) -> None:
    if edge_kind not in EDGE_KINDS:
        raise ValueError(f"unknown edge_kind {edge_kind!r}")
    if confidence not in CONFIDENCE_LEVELS:
        raise ValueError(f"unknown confidence {confidence!r}")
    if not provenance:
        raise ValueError("provenance is mandatory on edges")
    db.execute(
        "INSERT INTO actor_edges"
        "(from_id,edge_kind,to_id,confidence,provenance,detail,created_at)"
        " VALUES(?,?,?,?,?,?,?)"
        " ON CONFLICT(from_id,edge_kind,to_id) DO UPDATE SET"
        "  confidence=excluded.confidence, provenance=excluded.provenance,"
        "  detail=excluded.detail",
        (
            from_id,
            edge_kind,
            to_id,
            confidence,
            provenance,
            detail,
            _iso(now or dt.datetime.now(UTC)),
        ),
    )
    db.commit()


def neighbours(
    db: sqlite3.Connection, actor_id: str, edge_kind: Optional[str] = None
) -> list[tuple[str, str, str]]:
    if edge_kind:
        rows = db.execute(
            "SELECT from_id,edge_kind,to_id FROM actor_edges"
            " WHERE (from_id=? OR to_id=?) AND edge_kind=?",
            (actor_id, actor_id, edge_kind),
        )
    else:
        rows = db.execute(
            "SELECT from_id,edge_kind,to_id FROM actor_edges WHERE from_id=? OR to_id=?",
            (actor_id, actor_id),
        )
    return [tuple(row) for row in rows]


def people_at(db: sqlite3.Connection, org_id: str) -> list[Actor]:
    rows = db.execute(
        "SELECT actor_id,node_kind,display_name,role,org_id,confidence,provenance,detail"
        " FROM actor_nodes WHERE org_id=? AND node_kind='person' ORDER BY actor_id",
        (org_id,),
    ).fetchall()
    return [
        Actor(
            actor_id=row[0],
            node_kind=row[1],
            display_name=row[2],
            role=row[3],
            org_id=row[4],
            confidence=row[5],
            provenance=row[6],
            detail=row[7],
        )
        for row in rows
    ]


def unverified(db: sqlite3.Connection) -> list[str]:
    """Everything a human has not confirmed. Outreach should read this first."""
    return [
        row[0]
        for row in db.execute(
            "SELECT actor_id FROM actor_nodes WHERE confidence!='verified'"
            " ORDER BY actor_id"
        )
    ]
