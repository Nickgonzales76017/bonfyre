"""The economic estate as real relations -- causal value, not just ledgers.

Four distinct graphs, each with the run6/run7 forbidden inference enforced:
  MoneyGraph              causal money relations (funds/pays/owes/grants/costs)
  CommitmentGraph         resource commitments with conditions, expiry, authority
  RecursiveValueGraph     vector-valued non-cash value; NO implicit cash conversion
  InstitutionalCostGraph  recursive mission cost allocation across resources

A ledger records what happened; these model the causal structure. Nothing here
converts value to cash or a commitment to a delivery on its own.
"""

from __future__ import annotations

import json
import sqlite3
from typing import Iterable, Optional

SCHEMA = """
CREATE TABLE IF NOT EXISTS money_edges(
  src TEXT NOT NULL, relation TEXT NOT NULL, dst TEXT NOT NULL, amount REAL,
  PRIMARY KEY(src, relation, dst));
CREATE TABLE IF NOT EXISTS commitments(
  commitment_id TEXT PRIMARY KEY, committer TEXT, beneficiary TEXT, resource TEXT,
  conditions TEXT, expires_at TEXT, authority TEXT, status TEXT NOT NULL DEFAULT 'open');
CREATE TABLE IF NOT EXISTS value_edges(
  src TEXT NOT NULL, kind TEXT NOT NULL, dst TEXT NOT NULL,
  PRIMARY KEY(src, kind, dst));
CREATE TABLE IF NOT EXISTS mission_costs(
  mission TEXT NOT NULL, resource TEXT NOT NULL, amount REAL NOT NULL, booked INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY(mission, resource));
"""

MONEY_RELATIONS = frozenset({"funds", "pays", "owes", "grants", "subsidizes", "costs", "earns"})
# non-cash value kinds -- never a dollar amount
VALUE_KINDS = frozenset({
    "unlocks", "prefills", "increases_eligibility", "strengthens_evidence",
    "decreases_cost", "decreases_time", "creates_capacity", "increases_distribution",
    "creates_relationship_option", "creates_learning_option",
})


def ensure_schema(db: sqlite3.Connection) -> None:
    db.executescript(SCHEMA)
    db.commit()


# ------------------------------------------------------------------- MoneyGraph

def record_money(db: sqlite3.Connection, *, src: str, relation: str, dst: str,
                 amount: Optional[float] = None) -> None:
    if relation not in MONEY_RELATIONS:
        raise ValueError(f"unknown money relation {relation!r}")
    ensure_schema(db)
    db.execute("INSERT OR REPLACE INTO money_edges(src,relation,dst,amount) VALUES(?,?,?,?)",
               (src, relation, dst, amount))
    db.commit()


def money_out(db: sqlite3.Connection, src: str) -> list[tuple[str, str, Optional[float]]]:
    return [(rel, dst, amt) for rel, dst, amt in db.execute(
        "SELECT relation,dst,amount FROM money_edges WHERE src=?", (src,))]


# ---------------------------------------------------------------- CommitmentGraph

def record_commitment(db: sqlite3.Connection, *, commitment_id: str, committer: str,
                      beneficiary: str, resource: str, conditions: str = "",
                      expires_at: str = "", authority: str = "") -> None:
    ensure_schema(db)
    db.execute(
        "INSERT OR REPLACE INTO commitments"
        "(commitment_id,committer,beneficiary,resource,conditions,expires_at,authority,status)"
        " VALUES(?,?,?,?,?,?,?,'open')",
        (commitment_id, committer, beneficiary, resource, conditions, expires_at, authority))
    db.commit()


def commitment_delivered(db: sqlite3.Connection, commitment_id: str) -> bool:
    """A commitment is not a delivery. It counts as delivered only when explicitly
    marked so -- never inferred from the commitment existing."""
    row = db.execute("SELECT status FROM commitments WHERE commitment_id=?", (commitment_id,)).fetchone()
    return bool(row) and row[0] == "delivered"


def mark_delivered(db: sqlite3.Connection, commitment_id: str) -> None:
    db.execute("UPDATE commitments SET status='delivered' WHERE commitment_id=?", (commitment_id,))
    db.commit()


# ------------------------------------------------------------- RecursiveValueGraph

def record_value(db: sqlite3.Connection, *, src: str, kind: str, dst: str) -> None:
    if kind not in VALUE_KINDS:
        raise ValueError(f"unknown value kind {kind!r} (value is not cash)")
    ensure_schema(db)
    db.execute("INSERT OR REPLACE INTO value_edges(src,kind,dst) VALUES(?,?,?)", (src, kind, dst))
    db.commit()


def value_reachable(db: sqlite3.Connection, src: str) -> list[tuple[str, str]]:
    """What this unlocks/strengthens/creates -- transitively, still never cash."""
    seen: list[tuple[str, str]] = []
    visited: set[str] = set()
    frontier = [src]
    while frontier:
        cur = frontier.pop()
        for kind, dst in db.execute("SELECT kind,dst FROM value_edges WHERE src=?", (cur,)):
            if dst not in visited:
                visited.add(dst)
                seen.append((kind, dst))
                frontier.append(dst)
    return seen


# ---------------------------------------------------------- InstitutionalCostGraph

def record_cost(db: sqlite3.Connection, *, mission: str, resource: str, amount: float) -> None:
    ensure_schema(db)
    db.execute("INSERT OR REPLACE INTO mission_costs(mission,resource,amount,booked) VALUES(?,?,?,0)",
               (mission, resource, amount))
    db.commit()


def mission_cost(db: sqlite3.Connection, mission: str) -> float:
    return sum(a for (a,) in db.execute("SELECT amount FROM mission_costs WHERE mission=?", (mission,)))


def is_booked(db: sqlite3.Connection, mission: str, resource: str) -> bool:
    """A projected cost is not a booked accounting entry until posted with
    authority -- never inferred from the projection."""
    row = db.execute("SELECT booked FROM mission_costs WHERE mission=? AND resource=?",
                     (mission, resource)).fetchone()
    return bool(row) and row[0] == 1
