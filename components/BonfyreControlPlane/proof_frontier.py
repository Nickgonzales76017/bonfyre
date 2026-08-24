"""Governing execution with settled knowledge, not just remembering it.

The FPQ history is the motivating case. "FPQ is broken" was repeatedly an
invalid diagnosis: a failure at the runtime or transformer layer sent the next
agent back to rewrite the encoder, undoing work that earlier experiments had
already proven correct. Two of those experiments produced durable negative
knowledge that kept being ignored:

  - a fully-lossless FP16 passthrough still produced garbage, ruling out
    quantization as the cause;
  - routing Q/K/V through the trusted native reconstruction path matched the
    reference exactly, ruling out FPQ tensor reconstruction.

Memory recorded that those happened. Nothing stopped the next worker from
re-opening the encoder anyway. The missing primitive is not better memory; it
is an admission check that converts settled results into a mutation boundary.

Three epistemic classes, kept distinct:

    Memory      "this occurred"
    Evidence    "this is supported by these witnesses"
    Invariant   "until explicitly invalidated, future plans must respect this"

This module owns the third. A SolvedInvariant cools a layer against evidence
and names the only conditions that may reheat it. A KnownNonCause records a
hypothesis an experiment eliminated. A ProofFrontier orders the layers of a
subject so the first open one is unambiguous. A RegressionFence answers the one
question that matters before any edit: is this mutation permitted, given what is
already proven?
"""

from __future__ import annotations

import datetime as dt
import json
import sqlite3
from dataclasses import dataclass, field
from typing import Iterable, Optional, Sequence

UTC = dt.timezone.utc

# A cooled invariant is settled. Challenged means a reheat condition fired and it
# is being re-examined. Invalidated means an experiment overturned it.
COOLED = "cooled"
CHALLENGED = "challenged"
INVALIDATED = "invalidated"

INVARIANT_STATUS = (COOLED, CHALLENGED, INVALIDATED)

# A frontier layer is proven, open (the work), or blocked behind an open one.
PROVEN = "proven"
OPEN = "open"
BLOCKED = "blocked"
UNTESTED = "untested"

LAYER_STATUS = (PROVEN, OPEN, BLOCKED, UNTESTED)

# The four truth planes. A comment claiming a property is DECLARED; only a
# witness makes it PROVEN. Keeping these apart is the discipline the FPQ math
# claims most needed.
DECLARED = "declared"
IMPLEMENTED = "implemented"
MEASURED = "measured"
PROVEN_PLANE = "proven"

TRUTH_PLANES = (DECLARED, IMPLEMENTED, MEASURED, PROVEN_PLANE)

ALLOW = "allow"
DENY = "deny"

SCHEMA = """
CREATE TABLE IF NOT EXISTS solved_invariants(
  invariant_id TEXT PRIMARY KEY,
  subject_resource TEXT NOT NULL,
  subject_profile TEXT NOT NULL DEFAULT '',
  subject_hashes TEXT NOT NULL DEFAULT '[]',
  layer TEXT NOT NULL,
  statement TEXT NOT NULL,
  truth_plane TEXT NOT NULL DEFAULT 'measured',
  status TEXT NOT NULL DEFAULT 'cooled',
  proof_refs TEXT NOT NULL DEFAULT '[]',
  reheat_conditions TEXT NOT NULL DEFAULT '[]',
  proven_at TEXT,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS known_noncauses(
  noncause_id TEXT PRIMARY KEY,
  hypothesis TEXT NOT NULL,
  subject_scope TEXT NOT NULL,
  experiment TEXT NOT NULL,
  witness_ref TEXT NOT NULL DEFAULT '',
  invalidation_conditions TEXT NOT NULL DEFAULT '[]',
  recorded_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS frontier_layers(
  subject_resource TEXT NOT NULL,
  subject_profile TEXT NOT NULL DEFAULT '',
  ordinal INTEGER NOT NULL,
  layer TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'untested',
  witness_ref TEXT NOT NULL DEFAULT '',
  detail TEXT NOT NULL DEFAULT '',
  PRIMARY KEY(subject_resource, subject_profile, layer)
);
"""


def _iso(moment: dt.datetime) -> str:
    return moment.astimezone(UTC).replace(microsecond=0).isoformat()


def ensure_schema(db: sqlite3.Connection) -> None:
    db.executescript(SCHEMA)
    db.commit()


# --------------------------------------------------------------- invariants


@dataclass(frozen=True)
class SolvedInvariant:
    invariant_id: str
    subject_resource: str
    layer: str
    statement: str
    subject_profile: str = ""
    subject_hashes: Sequence[str] = field(default_factory=tuple)
    truth_plane: str = MEASURED
    status: str = COOLED
    proof_refs: Sequence[str] = field(default_factory=tuple)
    reheat_conditions: Sequence[str] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        if self.status not in INVARIANT_STATUS:
            raise ValueError(f"unknown status {self.status!r}")
        if self.truth_plane not in TRUTH_PLANES:
            raise ValueError(f"unknown truth_plane {self.truth_plane!r}")
        # A cooled invariant with no way back is the failure this exists to
        # prevent in reverse: something settled forever that reality can never
        # reopen. Every cooled invariant must name what reheats it.
        if self.status == COOLED and not self.reheat_conditions:
            raise ValueError(
                f"{self.invariant_id}: a cooled invariant needs reheat conditions"
            )


def record_invariant(
    db: sqlite3.Connection, invariant: SolvedInvariant, now: Optional[dt.datetime] = None
) -> None:
    moment = now or dt.datetime.now(UTC)
    db.execute(
        "INSERT INTO solved_invariants"
        "(invariant_id,subject_resource,subject_profile,subject_hashes,layer,"
        " statement,truth_plane,status,proof_refs,reheat_conditions,proven_at,updated_at)"
        " VALUES(?,?,?,?,?,?,?,?,?,?,?,?)"
        " ON CONFLICT(invariant_id) DO UPDATE SET"
        "  subject_profile=excluded.subject_profile, subject_hashes=excluded.subject_hashes,"
        "  layer=excluded.layer, statement=excluded.statement, truth_plane=excluded.truth_plane,"
        "  status=excluded.status, proof_refs=excluded.proof_refs,"
        "  reheat_conditions=excluded.reheat_conditions, updated_at=excluded.updated_at",
        (
            invariant.invariant_id,
            invariant.subject_resource,
            invariant.subject_profile,
            json.dumps(list(invariant.subject_hashes)),
            invariant.layer,
            invariant.statement,
            invariant.truth_plane,
            invariant.status,
            json.dumps(list(invariant.proof_refs)),
            json.dumps(list(invariant.reheat_conditions)),
            _iso(moment),
            _iso(moment),
        ),
    )
    db.commit()


def challenge_invariant(
    db: sqlite3.Connection,
    invariant_id: str,
    reheat_signal: str,
    now: Optional[dt.datetime] = None,
) -> bool:
    """Move a cooled invariant to challenged, but only if the signal is one it
    actually named. A generic symptom does not challenge a cooled layer."""
    row = db.execute(
        "SELECT status, reheat_conditions FROM solved_invariants WHERE invariant_id=?",
        (invariant_id,),
    ).fetchone()
    if row is None:
        raise ValueError(f"no invariant {invariant_id}")
    conditions = json.loads(row[1] or "[]")
    if not any(reheat_signal.strip().lower() in c.lower() for c in conditions):
        return False
    db.execute(
        "UPDATE solved_invariants SET status='challenged', updated_at=? WHERE invariant_id=?",
        (_iso(now or dt.datetime.now(UTC)), invariant_id),
    )
    db.commit()
    return True


def cooled_invariants(
    db: sqlite3.Connection, subject_resource: str, layer: Optional[str] = None
) -> list[SolvedInvariant]:
    query = (
        "SELECT invariant_id,subject_resource,subject_profile,subject_hashes,layer,"
        "statement,truth_plane,status,proof_refs,reheat_conditions"
        " FROM solved_invariants WHERE subject_resource=? AND status='cooled'"
    )
    params: list[object] = [subject_resource]
    if layer is not None:
        query += " AND layer=?"
        params.append(layer)
    return [
        SolvedInvariant(
            invariant_id=r[0],
            subject_resource=r[1],
            subject_profile=r[2],
            subject_hashes=tuple(json.loads(r[3] or "[]")),
            layer=r[4],
            statement=r[5],
            truth_plane=r[6],
            status=r[7],
            proof_refs=tuple(json.loads(r[8] or "[]")),
            reheat_conditions=tuple(json.loads(r[9] or "[]")),
        )
        for r in db.execute(query, params)
    ]


# -------------------------------------------------------------- non-causes


@dataclass(frozen=True)
class KnownNonCause:
    noncause_id: str
    hypothesis: str
    subject_scope: str
    experiment: str
    witness_ref: str = ""
    invalidation_conditions: Sequence[str] = field(default_factory=tuple)


def record_noncause(
    db: sqlite3.Connection, noncause: KnownNonCause, now: Optional[dt.datetime] = None
) -> None:
    db.execute(
        "INSERT INTO known_noncauses"
        "(noncause_id,hypothesis,subject_scope,experiment,witness_ref,"
        " invalidation_conditions,recorded_at)"
        " VALUES(?,?,?,?,?,?,?)"
        " ON CONFLICT(noncause_id) DO UPDATE SET"
        "  hypothesis=excluded.hypothesis, subject_scope=excluded.subject_scope,"
        "  experiment=excluded.experiment, witness_ref=excluded.witness_ref,"
        "  invalidation_conditions=excluded.invalidation_conditions",
        (
            noncause.noncause_id,
            noncause.hypothesis,
            noncause.subject_scope,
            noncause.experiment,
            noncause.witness_ref,
            json.dumps(list(noncause.invalidation_conditions)),
            _iso(now or dt.datetime.now(UTC)),
        ),
    )
    db.commit()


def noncauses_for(db: sqlite3.Connection, subject_scope: str) -> list[KnownNonCause]:
    return [
        KnownNonCause(
            noncause_id=r[0],
            hypothesis=r[1],
            subject_scope=r[2],
            experiment=r[3],
            witness_ref=r[4],
            invalidation_conditions=tuple(json.loads(r[5] or "[]")),
        )
        for r in db.execute(
            "SELECT noncause_id,hypothesis,subject_scope,experiment,witness_ref,"
            "invalidation_conditions FROM known_noncauses WHERE subject_scope=?",
            (subject_scope,),
        )
    ]


# --------------------------------------------------------------- frontier


def set_layer(
    db: sqlite3.Connection,
    subject_resource: str,
    ordinal: int,
    layer: str,
    status: str,
    *,
    subject_profile: str = "",
    witness_ref: str = "",
    detail: str = "",
) -> None:
    if status not in LAYER_STATUS:
        raise ValueError(f"unknown layer status {status!r}")
    db.execute(
        "INSERT INTO frontier_layers"
        "(subject_resource,subject_profile,ordinal,layer,status,witness_ref,detail)"
        " VALUES(?,?,?,?,?,?,?)"
        " ON CONFLICT(subject_resource,subject_profile,layer) DO UPDATE SET"
        "  ordinal=excluded.ordinal, status=excluded.status,"
        "  witness_ref=excluded.witness_ref, detail=excluded.detail",
        (subject_resource, subject_profile, ordinal, layer, status, witness_ref, detail),
    )
    db.commit()


def first_open_layer(
    db: sqlite3.Connection, subject_resource: str, subject_profile: str = ""
) -> Optional[tuple[int, str]]:
    """The lowest-ordinal layer that is open. Everything below it is proven and
    therefore off-limits; everything above it is blocked until it closes."""
    row = db.execute(
        "SELECT ordinal, layer FROM frontier_layers"
        " WHERE subject_resource=? AND subject_profile=? AND status='open'"
        " ORDER BY ordinal ASC LIMIT 1",
        (subject_resource, subject_profile),
    ).fetchone()
    return (row[0], row[1]) if row else None


def _layer_ordinal(
    db: sqlite3.Connection, subject_resource: str, subject_profile: str, layer: str
) -> Optional[int]:
    row = db.execute(
        "SELECT ordinal FROM frontier_layers"
        " WHERE subject_resource=? AND subject_profile=? AND layer=?",
        (subject_resource, subject_profile, layer),
    ).fetchone()
    return row[0] if row else None


# ---------------------------------------------------------- the fence


@dataclass(frozen=True)
class FenceDecision:
    verdict: str
    reason: str
    first_open: Optional[str] = None
    matched_noncause: Optional[str] = None

    @property
    def allowed(self) -> bool:
        return self.verdict == ALLOW


def authorize_mutation(
    db: sqlite3.Connection,
    *,
    subject_resource: str,
    target_layer: str,
    observed_failure: str,
    subject_profile: str = "",
    reheat_signal: str = "",
) -> FenceDecision:
    """May an edit to ``target_layer`` proceed, given what is settled?

    This is the check that would have stopped every "fix FPQ" loop. A garbage
    generation (an L7 symptom) does not authorize an L1 encoder rewrite while
    the encoder's layer is cooled and the real open divergence is higher up.

    Rules, in order:
      1. If the observed failure matches a KnownNonCause for this subject, the
         target is not the cause -- deny and say which experiment eliminated it.
      2. If the target layer is below the first open layer, it is proven ground
         -- deny, unless a reheat_signal actually satisfies a cooled invariant
         on that layer.
      3. Otherwise the target is at or above the frontier -- allow.
    """
    scope = f"{subject_resource}:{subject_profile}" if subject_profile else subject_resource
    failure = observed_failure.strip().lower()

    for noncause in noncauses_for(db, scope) + noncauses_for(db, subject_resource):
        if noncause.hypothesis and (
            target_layer.lower() in noncause.hypothesis.lower()
            or noncause.hypothesis.lower() in failure
        ):
            return FenceDecision(
                verdict=DENY,
                reason=(
                    f"{noncause.experiment} already eliminated this: "
                    f"{noncause.hypothesis}"
                ),
                matched_noncause=noncause.noncause_id,
            )

    open_layer = first_open_layer(db, subject_resource, subject_profile)
    target_ord = _layer_ordinal(db, subject_resource, subject_profile, target_layer)

    if open_layer is not None and target_ord is not None and target_ord < open_layer[0]:
        # Below the frontier. Only a genuine reheat signal on a cooled invariant
        # for this layer reopens it.
        if reheat_signal:
            for inv in cooled_invariants(db, subject_resource, target_layer):
                if challenge_invariant(db, inv.invariant_id, reheat_signal):
                    return FenceDecision(
                        verdict=ALLOW,
                        reason=(
                            f"reheat signal {reheat_signal!r} satisfied "
                            f"{inv.invariant_id}; layer reopened"
                        ),
                        first_open=open_layer[1],
                    )
        return FenceDecision(
            verdict=DENY,
            reason=(
                f"{target_layer} is proven and below the open layer "
                f"{open_layer[1]}; the failure {observed_failure!r} is a symptom, "
                f"not authorization to reopen it"
            ),
            first_open=open_layer[1],
        )

    return FenceDecision(
        verdict=ALLOW,
        reason=(
            f"{target_layer} is at or above the open frontier"
            + (f" ({open_layer[1]})" if open_layer else "")
        ),
        first_open=open_layer[1] if open_layer else None,
    )


def frontier_report(
    db: sqlite3.Connection, subject_resource: str, subject_profile: str = ""
) -> dict:
    layers = [
        {"ordinal": r[0], "layer": r[1], "status": r[2], "witness": r[3]}
        for r in db.execute(
            "SELECT ordinal,layer,status,witness_ref FROM frontier_layers"
            " WHERE subject_resource=? AND subject_profile=? ORDER BY ordinal",
            (subject_resource, subject_profile),
        )
    ]
    scope = f"{subject_resource}:{subject_profile}" if subject_profile else subject_resource
    opened = first_open_layer(db, subject_resource, subject_profile)
    seen = {}
    for n in noncauses_for(db, scope) + noncauses_for(db, subject_resource):
        seen[n.noncause_id] = n.hypothesis
    return {
        "subject": subject_resource,
        "profile": subject_profile,
        "layers": layers,
        "first_open_layer": opened[1] if opened else None,
        "known_noncauses": sorted(set(seen.values())),
        "mutation_ceiling": opened[1] if opened else "(all proven)",
    }
