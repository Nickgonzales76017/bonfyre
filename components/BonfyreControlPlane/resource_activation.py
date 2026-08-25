"""ResourceActivationGraph: how a currently-unavailable resource becomes usable.

The last architectural node in the opportunity family. Activation is
world-expansion: a capability gap does not become a usable resource by being
wished for. It walks an ordered state machine -- candidate, qualified, eligible,
authorized, activated -- and only a resource that reaches ``activated`` is
something ProviderGraph may see. Each step is decided against real substrate:
authority through AuthorityGraph, the final binding through an activation
mechanism (a bound service, a granted lease).

The law the atlas already recorded: an unlock existing does not make a resource
free or authorized, and being available is not being activated. Nothing here
short-circuits authority.
"""

from __future__ import annotations

import datetime as dt
import sqlite3
from dataclasses import dataclass
from typing import Optional

import authority

UTC = dt.timezone.utc

# Ordered activation states, low to high. A resource is usable only at ACTIVATED.
GAP = "gap"
CANDIDATE = "candidate"
QUALIFIED = "qualified"
ELIGIBLE = "eligible"
AUTHORIZED = "authorized"
ACTIVATED = "activated"
ORDER = (GAP, CANDIDATE, QUALIFIED, ELIGIBLE, AUTHORIZED, ACTIVATED)

# Activation mechanisms (each a concrete lifecycle elsewhere).
SERVICE = "service"
GPU_LEASE = "gpu_lease"
HUMAN = "human"
VOUCHER = "voucher"
CREDENTIAL = "credential"

SCHEMA = """
CREATE TABLE IF NOT EXISTS resource_candidates(
  resource_id TEXT PRIMARY KEY,
  mechanism TEXT NOT NULL DEFAULT 'service',
  qualified INTEGER NOT NULL DEFAULT 0,
  eligible INTEGER NOT NULL DEFAULT 0,
  activate_actor TEXT NOT NULL DEFAULT '',
  detail TEXT NOT NULL DEFAULT '',
  updated_at TEXT NOT NULL
);
"""


def _iso(m: dt.datetime) -> str:
    return m.astimezone(UTC).replace(microsecond=0).isoformat()


def ensure_schema(db: sqlite3.Connection) -> None:
    db.executescript(SCHEMA)
    db.commit()


@dataclass(frozen=True)
class ResourceCandidate:
    resource_id: str
    mechanism: str = SERVICE
    qualified: bool = False
    eligible: bool = False
    activate_actor: str = ""
    detail: str = ""


def record_candidate(db: sqlite3.Connection, cand: ResourceCandidate,
                     now: Optional[dt.datetime] = None) -> None:
    ensure_schema(db)
    db.execute(
        "INSERT INTO resource_candidates"
        "(resource_id,mechanism,qualified,eligible,activate_actor,detail,updated_at)"
        " VALUES(?,?,?,?,?,?,?)"
        " ON CONFLICT(resource_id) DO UPDATE SET mechanism=excluded.mechanism,"
        "  qualified=excluded.qualified, eligible=excluded.eligible,"
        "  activate_actor=excluded.activate_actor, detail=excluded.detail,"
        "  updated_at=excluded.updated_at",
        (cand.resource_id, cand.mechanism, int(cand.qualified), int(cand.eligible),
         cand.activate_actor, cand.detail, _iso(now or dt.datetime.now(UTC))),
    )
    db.commit()


@dataclass(frozen=True)
class ActivationState:
    resource_id: str
    state: str
    missing: tuple[str, ...]

    @property
    def activated(self) -> bool:
        return self.state == ACTIVATED


def activation_state(
    db: sqlite3.Connection, resource_id: str, *, bound_services: frozenset[str] = frozenset()
) -> ActivationState:
    """The furthest activation state a resource has reached, and what blocks the
    next step. Authority is checked against the graph; the final binding against
    the activation mechanism (a bound service today)."""
    if not _table_exists(db, "resource_candidates"):
        return ActivationState(resource_id, GAP, ("no candidate recorded",))
    row = db.execute(
        "SELECT mechanism,qualified,eligible,activate_actor FROM resource_candidates"
        " WHERE resource_id=?", (resource_id,)
    ).fetchone()
    if row is None:
        return ActivationState(resource_id, GAP, ("no candidate recorded",))
    mechanism, qualified, eligible, actor = row

    if not qualified:
        return ActivationState(resource_id, CANDIDATE, ("qualification",))
    if not eligible:
        return ActivationState(resource_id, QUALIFIED, ("eligibility",))
    if not (actor and authority.has_authority(db, actor, authority.ACTIVATE, resource_id)):
        return ActivationState(resource_id, ELIGIBLE, ("activate authority",))
    if resource_id not in bound_services:
        return ActivationState(resource_id, AUTHORIZED, (f"{mechanism} binding",))
    return ActivationState(resource_id, ACTIVATED, ())


def is_activated(
    db: sqlite3.Connection, resource_id: str, *, bound_services: frozenset[str] = frozenset()
) -> bool:
    return activation_state(db, resource_id, bound_services=bound_services).activated


def _table_exists(db: sqlite3.Connection, name: str) -> bool:
    return db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone() is not None
