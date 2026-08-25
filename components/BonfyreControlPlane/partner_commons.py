"""Partner Commons: narrow external specialties plus observed relationship state.

Profiles describe a boundary role and its explicit non-ownership.  Observations
are append-only facts about remote state; the latest observation is a projection,
never authority to mutate the remote institution.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass

import foreign


SCHEMA = """
CREATE TABLE IF NOT EXISTS partner_profiles(
  partner_id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  specialty TEXT NOT NULL,
  connects_to TEXT NOT NULL,
  does_not_own TEXT NOT NULL,
  remote_url TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS partner_observations(
  observation_id TEXT PRIMARY KEY,
  partner_id TEXT NOT NULL REFERENCES partner_profiles(partner_id),
  state TEXT NOT NULL,
  observed_at TEXT NOT NULL,
  source_ref TEXT NOT NULL,
  detail TEXT NOT NULL DEFAULT ''
);
"""


@dataclass(frozen=True)
class PartnerProfile:
    partner_id: str
    name: str
    specialty: str
    connects_to: str
    does_not_own: str
    remote_url: str


@dataclass(frozen=True)
class PartnerObservation:
    observation_id: str
    partner_id: str
    state: str
    observed_at: str
    source_ref: str
    detail: str = ""


def ensure_schema(db: sqlite3.Connection) -> None:
    db.executescript(SCHEMA)
    db.execute("PRAGMA foreign_keys=ON")
    db.commit()


def record_profile(db: sqlite3.Connection, profile: PartnerProfile) -> None:
    if not profile.does_not_own:
        raise ValueError(f"{profile.partner_id}: semantic non-ownership is required")
    ensure_schema(db)
    db.execute(
        "INSERT INTO partner_profiles"
        "(partner_id,name,specialty,connects_to,does_not_own,remote_url)"
        " VALUES(?,?,?,?,?,?) ON CONFLICT(partner_id) DO UPDATE SET"
        " name=excluded.name,specialty=excluded.specialty,"
        " connects_to=excluded.connects_to,does_not_own=excluded.does_not_own,"
        " remote_url=excluded.remote_url",
        (profile.partner_id, profile.name, profile.specialty, profile.connects_to,
         profile.does_not_own, profile.remote_url),
    )
    db.commit()


def record_observation(db: sqlite3.Connection, observation: PartnerObservation) -> None:
    if not observation.source_ref:
        raise ValueError("partner observations require a source reference")
    ensure_schema(db)
    db.execute(
        "INSERT INTO partner_observations"
        "(observation_id,partner_id,state,observed_at,source_ref,detail)"
        " VALUES(?,?,?,?,?,?) ON CONFLICT(observation_id) DO UPDATE SET"
        " state=excluded.state,observed_at=excluded.observed_at,"
        " source_ref=excluded.source_ref,detail=excluded.detail",
        (observation.observation_id, observation.partner_id, observation.state,
         observation.observed_at, observation.source_ref, observation.detail),
    )
    db.commit()


def as_foreign_twin(db: sqlite3.Connection, partner_id: str) -> foreign.ForeignTwin:
    row = db.execute(
        "SELECT partner_id,remote_url FROM partner_profiles WHERE partner_id=?",
        (partner_id,),
    ).fetchone()
    if row is None:
        raise KeyError(partner_id)
    return foreign.ForeignTwin(
        remote_resource_id=f"partner:{row[0]}",
        source_institution=row[0],
        url=row[1],
        profile="partner-commons/v1",
        rights=(foreign.OBSERVE, foreign.REFERENCE),
    )


def latest_state(db: sqlite3.Connection, partner_id: str) -> str | None:
    row = db.execute(
        "SELECT state FROM partner_observations WHERE partner_id=?"
        " ORDER BY observed_at DESC,observation_id DESC LIMIT 1",
        (partner_id,),
    ).fetchone()
    return row[0] if row else None
