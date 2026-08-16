"""Provider topology above the ProviderStateFold -- bids, placement, parity.

  ProviderGraph       capability bids scored on a metric VECTOR (no scalar
                      collapse); tournament selects; fallback tree orders alternates
  PlacementGraph      where a selected realization runs/resides
  RuntimeParityGraph  which carriers are PROVEN semantically equivalent (an
                      equal-cost multipath group)

Forbidden inferences enforced: available is not authorized, cheapest is not
valid, placeable is not authorized to run there, a carrier running is not parity.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from typing import Optional

SCHEMA = """
CREATE TABLE IF NOT EXISTS provider_bids(
  capability TEXT NOT NULL, provider TEXT NOT NULL, metric TEXT NOT NULL,
  PRIMARY KEY(capability, provider));
CREATE TABLE IF NOT EXISTS placements(
  resource TEXT PRIMARY KEY, location TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS runtime_parity(
  capability TEXT NOT NULL, carrier_a TEXT NOT NULL, carrier_b TEXT NOT NULL,
  proven INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY(capability, carrier_a, carrier_b));
"""

# metric dimensions -- kept as a vector, never collapsed to one scalar
METRIC_DIMS = (
    "cost", "quality", "latency", "trust", "authority", "locality",
    "semantic_loss", "availability",
)


def ensure_schema(db: sqlite3.Connection) -> None:
    db.executescript(SCHEMA)
    db.commit()


# ------------------------------------------------------------------ ProviderGraph

def record_bid(db: sqlite3.Connection, *, capability: str, provider: str, metric: dict[str, float]) -> None:
    ensure_schema(db)
    db.execute("INSERT OR REPLACE INTO provider_bids(capability,provider,metric) VALUES(?,?,?)",
               (capability, provider, json.dumps(metric)))
    db.commit()


def bids(db: sqlite3.Connection, capability: str) -> list[tuple[str, dict[str, float]]]:
    return [(p, json.loads(m)) for p, m in db.execute(
        "SELECT provider,metric FROM provider_bids WHERE capability=?", (capability,))]


def tournament(db: sqlite3.Connection, capability: str, *, weights: Optional[dict[str, float]] = None,
               required_authority: bool = True) -> list[tuple[str, float]]:
    """Rank bids by a weighted combination of the metric vector. The weights are a
    policy choice applied at selection -- the metric vector is preserved intact.
    A bid without the authority dimension satisfied is not selectable when
    required_authority (available is not authorized)."""
    w = weights or {d: 1.0 for d in METRIC_DIMS}
    ranked: list[tuple[str, float]] = []
    for provider, metric in bids(db, capability):
        if required_authority and metric.get("authority", 0.0) <= 0.0:
            continue
        score = sum(w.get(d, 0.0) * metric.get(d, 0.0) for d in METRIC_DIMS)
        ranked.append((provider, score))
    return sorted(ranked, key=lambda t: (-t[1], t[0]))


def fallback_tree(db: sqlite3.Connection, capability: str, **kw) -> list[str]:
    """The ordered alternates: the tournament ranking is the fallback order."""
    return [p for p, _s in tournament(db, capability, **kw)]


# ----------------------------------------------------------------- PlacementGraph

def record_placement(db: sqlite3.Connection, *, resource: str, location: str) -> None:
    ensure_schema(db)
    db.execute("INSERT OR REPLACE INTO placements(resource,location) VALUES(?,?)", (resource, location))
    db.commit()


def placement_of(db: sqlite3.Connection, resource: str) -> Optional[str]:
    row = db.execute("SELECT location FROM placements WHERE resource=?", (resource,)).fetchone()
    return row[0] if row else None


# -------------------------------------------------------------- RuntimeParityGraph

def record_parity(db: sqlite3.Connection, *, capability: str, carrier_a: str, carrier_b: str,
                  proven: bool) -> None:
    ensure_schema(db)
    a, b = sorted((carrier_a, carrier_b))
    db.execute("INSERT OR REPLACE INTO runtime_parity(capability,carrier_a,carrier_b,proven) VALUES(?,?,?,?)",
               (capability, a, b, int(proven)))
    db.commit()


def parity_group(db: sqlite3.Connection, capability: str) -> list[tuple[str, str]]:
    """Carrier pairs PROVEN equivalent -- an equal-cost multipath group. A pair
    that merely ran is not in the group unless proven."""
    return [(a, b) for a, b in db.execute(
        "SELECT carrier_a,carrier_b FROM runtime_parity WHERE capability=? AND proven=1", (capability,))]
