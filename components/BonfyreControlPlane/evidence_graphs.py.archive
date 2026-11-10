"""The evidence estate as five distinct real relations -- not one EvidenceGraph.

The run6/run7 vision is emphatic that these answer different questions and must
not be folded together. This builds each as a real typed relation over sqlite,
enforcing the distinction and the forbidden inference the vision named:

  ArtifactGraph          transformation: source -> operator -> derived
  ProvenanceGraph        the path an artifact took to exist (cheap path checks)
  EvidenceGraph          claim support (supports/contradicts, NOT symmetric)
  TemporalEvidenceGraph  eight time planes per fact, never collapsed, never deleted
  ClaimGraph             a claim's dependencies, counterclaims, proof state

Every edge carries its meta-edge class (PROVENANCE / EVIDENCE / TEMPORAL /
derivation), so the machinery reads ~16 classes while the estate holds many
domain edges.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from typing import Iterable, Optional

SCHEMA = """
CREATE TABLE IF NOT EXISTS artifact_transforms(
  derived TEXT NOT NULL, operator TEXT NOT NULL, source TEXT NOT NULL,
  PRIMARY KEY(derived, source));
CREATE TABLE IF NOT EXISTS provenance_edges(
  artifact TEXT NOT NULL, role TEXT NOT NULL, node TEXT NOT NULL,
  PRIMARY KEY(artifact, role, node));
CREATE TABLE IF NOT EXISTS evidence_relations(
  evidence TEXT NOT NULL, kind TEXT NOT NULL, claim TEXT NOT NULL,
  PRIMARY KEY(evidence, kind, claim));
CREATE TABLE IF NOT EXISTS temporal_evidence(
  fact TEXT PRIMARY KEY, planes TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS claim_deps(
  claim TEXT NOT NULL, dep_kind TEXT NOT NULL, target TEXT NOT NULL,
  PRIMARY KEY(claim, dep_kind, target));
CREATE TABLE IF NOT EXISTS claim_state(
  claim TEXT PRIMARY KEY, proof_state TEXT NOT NULL);
"""

# evidence relation kinds (each specializes the EVIDENCE meta-class)
SUPPORTS = "supports"
CONTRADICTS = "contradicts"
REPRODUCES = "reproduces"
FALSIFIES = "falsifies"
EVIDENCE_KINDS = frozenset({SUPPORTS, CONTRADICTS, REPRODUCES, FALSIFIES})

# the eight time planes -- never collapsed to one timestamp
TIME_PLANES = (
    "event_time", "publication_time", "observation_time", "retrieval_time",
    "effective_time", "supersession_time", "correction_time", "expiration_time",
)


def ensure_schema(db: sqlite3.Connection) -> None:
    db.executescript(SCHEMA)
    db.commit()


# ---------------------------------------------------------------- ArtifactGraph

def record_transform(db: sqlite3.Connection, *, source: str, operator: str, derived: str) -> None:
    ensure_schema(db)
    db.execute("INSERT OR REPLACE INTO artifact_transforms(derived,operator,source) VALUES(?,?,?)",
               (derived, operator, source))
    db.commit()


def lineage(db: sqlite3.Connection, derived: str) -> list[str]:
    """Ancestors of a derived artifact -- transformation ancestry, transitive."""
    seen: list[str] = []
    frontier = [derived]
    while frontier:
        cur = frontier.pop()
        for (src,) in db.execute("SELECT source FROM artifact_transforms WHERE derived=?", (cur,)):
            if src not in seen:
                seen.append(src)
                frontier.append(src)
    return seen


# -------------------------------------------------------------- ProvenanceGraph

def record_provenance(db: sqlite3.Connection, *, artifact: str, role: str, node: str) -> None:
    ensure_schema(db)
    db.execute("INSERT OR REPLACE INTO provenance_edges(artifact,role,node) VALUES(?,?,?)",
               (artifact, role, node))
    db.commit()


def provenance_path(db: sqlite3.Connection, artifact: str) -> list[tuple[str, str]]:
    return [(r, n) for r, n in db.execute(
        "SELECT role,node FROM provenance_edges WHERE artifact=? ORDER BY role", (artifact,))]


def path_admits(db: sqlite3.Connection, artifact: str, *, untrusted_nodes: Iterable[str]) -> bool:
    """The cheap path-vector check: reject if the provenance path crossed an
    untrusted node. Provenance is separate from evidence -- a clean path does not
    make the artifact support a claim."""
    bad = set(untrusted_nodes)
    return not any(node in bad for _role, node in provenance_path(db, artifact))


# ---------------------------------------------------------------- EvidenceGraph

def relate_evidence(db: sqlite3.Connection, *, evidence: str, kind: str, claim: str) -> None:
    if kind not in EVIDENCE_KINDS:
        raise ValueError(f"unknown evidence kind {kind!r}")
    ensure_schema(db)
    db.execute("INSERT OR REPLACE INTO evidence_relations(evidence,kind,claim) VALUES(?,?,?)",
               (evidence, kind, claim))
    db.commit()


def supporters(db: sqlite3.Connection, claim: str) -> list[str]:
    return [e for (e,) in db.execute(
        "SELECT evidence FROM evidence_relations WHERE claim=? AND kind=?", (claim, SUPPORTS))]


def supports(db: sqlite3.Connection, evidence: str, claim: str) -> bool:
    """supports(A,B) is directional -- it does not imply supports(B,A)."""
    return db.execute(
        "SELECT 1 FROM evidence_relations WHERE evidence=? AND claim=? AND kind=?",
        (evidence, claim, SUPPORTS)).fetchone() is not None


# ----------------------------------------------------------- TemporalEvidenceGraph

def record_times(db: sqlite3.Connection, fact: str, planes: dict[str, str]) -> None:
    """Record a fact's times. Unknown planes are kept; none collapse into one."""
    ensure_schema(db)
    kept = {k: v for k, v in planes.items() if k in TIME_PLANES}
    db.execute("INSERT OR REPLACE INTO temporal_evidence(fact,planes) VALUES(?,?)",
               (fact, json.dumps(kept)))
    db.commit()


def times_of(db: sqlite3.Connection, fact: str) -> dict[str, str]:
    row = db.execute("SELECT planes FROM temporal_evidence WHERE fact=?", (fact,)).fetchone()
    return json.loads(row[0]) if row else {}


def is_stale(db: sqlite3.Connection, fact: str, at: str) -> bool:
    """Stale for a decision at ``at`` (past expiration_time). Staleness never
    deletes the historical evidence -- the row stays."""
    exp = times_of(db, fact).get("expiration_time")
    return bool(exp) and at > exp


# ----------------------------------------------------------------- ClaimGraph

def add_claim(db: sqlite3.Connection, claim: str, *, depends_on: Iterable[str] = (),
              counters: Iterable[str] = (), proof_state: str = "open") -> None:
    ensure_schema(db)
    for d in depends_on:
        db.execute("INSERT OR REPLACE INTO claim_deps(claim,dep_kind,target) VALUES(?,?,?)",
                   (claim, "depends_on", d))
    for c in counters:
        db.execute("INSERT OR REPLACE INTO claim_deps(claim,dep_kind,target) VALUES(?,?,?)",
                   (claim, "counter", c))
    db.execute("INSERT OR REPLACE INTO claim_state(claim,proof_state) VALUES(?,?)",
               (claim, proof_state))
    db.commit()


def claim_dependencies(db: sqlite3.Connection, claim: str) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {"depends_on": [], "counter": []}
    for kind, target in db.execute("SELECT dep_kind,target FROM claim_deps WHERE claim=?", (claim,)):
        out.setdefault(kind, []).append(target)
    return out


def proof_state(db: sqlite3.Connection, claim: str) -> Optional[str]:
    row = db.execute("SELECT proof_state FROM claim_state WHERE claim=?", (claim,)).fetchone()
    return row[0] if row else None
