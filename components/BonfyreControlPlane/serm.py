"""SERM: reduce existing measurements into a sufficiency-and-honesty verdict.

The atlas is clear that SERM is a reduction architecture, not the measurements
themselves: a measurement contract, reduced against evidence, yields a
sufficiency audit and a provider-honesty audit, and *produces* ProofFrontier /
EvidenceGraph consequences.

The important discipline, given the substrate is mature: SERM does not re-run
benchmarks or re-derive settled facts. It consumes measurements that already
exist -- the recorded FPQ fixture evidence, the cooled invariants and proven
layers in the proof frontier -- and reduces them. A capability demand names the
layers it needs proven; SERM checks they are covered by witness-backed
measurements within the frontier, and audits that every measured/proven claim is
actually backed by a witness rather than asserted. Laundering a plane is
dishonest, and SERM is where that is caught at the measurement level -- the same
law the atlas enforces structurally.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from typing import Optional, Sequence

import proof_frontier as pf


@dataclass(frozen=True)
class MeasurementContract:
    contract_id: str
    capability_demand: str
    subject_resource: str
    subject_profile: str = ""
    required_layers: Sequence[str] = field(default_factory=tuple)


@dataclass(frozen=True)
class Measurement:
    layer: str
    plane: str                       # measured | proven (from the frontier)
    witness_refs: tuple[str, ...]
    source: str                      # invariant id or frontier layer


@dataclass(frozen=True)
class SERMResult:
    contract_id: str
    sufficient: bool
    honest: bool
    covered: tuple[str, ...]
    missing: tuple[str, ...]
    laundered: tuple[str, ...]       # claims at measured/proven with no witness
    verdict: str

    def to_dict(self) -> dict:
        return {
            "contract": self.contract_id, "sufficient": self.sufficient,
            "honest": self.honest, "covered": list(self.covered),
            "missing": list(self.missing), "laundered": list(self.laundered),
            "verdict": self.verdict,
        }


def collect_measurements(
    db: sqlite3.Connection, subject_resource: str, subject_profile: str = ""
) -> list[Measurement]:
    """Read the measurements the system already holds for a subject: proven
    frontier layers with a witness, and cooled measured/proven invariants."""
    pf.ensure_schema(db)
    out: list[Measurement] = []
    for r in db.execute(
        "SELECT layer,status,witness_ref FROM frontier_layers"
        " WHERE subject_resource=? AND subject_profile=? AND status='proven'",
        (subject_resource, subject_profile),
    ):
        layer, _status, witness = r
        out.append(Measurement(layer=layer, plane="proven",
                               witness_refs=tuple(w for w in [witness] if w),
                               source=f"frontier:{layer}"))
    for r in db.execute(
        "SELECT invariant_id,layer,truth_plane,proof_refs FROM solved_invariants"
        " WHERE subject_resource=? AND status='cooled'",
        (subject_resource,),
    ):
        inv_id, layer, plane, proof_refs_json = r
        if plane in (pf.MEASURED, pf.PROVEN_PLANE):
            out.append(Measurement(layer=layer, plane=plane,
                                   witness_refs=tuple(json.loads(proof_refs_json or "[]")),
                                   source=inv_id))
    return out


def reduce(db: sqlite3.Connection, contract: MeasurementContract) -> SERMResult:
    """Reduce a contract against existing measurements.

    Sufficient: every required layer is covered by a witness-backed measurement.
    Honest: no measurement claims measured/proven without a witness -- a claim
    with no witness is a laundered plane, and it fails the honesty audit even if
    the layer is otherwise 'covered'."""
    measurements = collect_measurements(db, contract.subject_resource, contract.subject_profile)

    # honesty first: a witness-less measured/proven claim is laundering.
    laundered = tuple(sorted(
        f"{m.source}:{m.layer}" for m in measurements if not m.witness_refs
    ))
    honest = not laundered

    backed = {m.layer for m in measurements if m.witness_refs}
    covered = tuple(sorted(l for l in contract.required_layers if l in backed))
    missing = tuple(sorted(l for l in contract.required_layers if l not in backed))
    sufficient = not missing

    if sufficient and honest:
        verdict = "sufficient: all required layers covered by witness-backed measurements"
    elif not honest:
        verdict = f"dishonest: {len(laundered)} claim(s) assert a plane with no witness"
    else:
        verdict = f"insufficient: {len(missing)} required layer(s) unproven: {', '.join(missing)}"

    return SERMResult(
        contract_id=contract.contract_id, sufficient=sufficient, honest=honest,
        covered=covered, missing=missing, laundered=laundered, verdict=verdict,
    )


def record_sufficiency(
    db: sqlite3.Connection, contract: MeasurementContract, result: SERMResult
) -> Optional[str]:
    """The ProofFrontier consequence: when a contract is sufficient and honest,
    cool a sufficiency invariant citing the covered measurements. Returns the
    invariant id, or None when the reduction did not earn it."""
    if not (result.sufficient and result.honest):
        return None
    inv_id = f"serm.sufficiency.{contract.contract_id}"
    pf.record_invariant(db, pf.SolvedInvariant(
        invariant_id=inv_id,
        subject_resource=contract.subject_resource,
        subject_profile=contract.subject_profile,
        layer="sufficiency",
        statement=(f"capability demand '{contract.capability_demand}' is sufficiently "
                   f"evidenced: {', '.join(result.covered)} covered by witnesses"),
        truth_plane=pf.MEASURED,
        status=pf.COOLED,
        proof_refs=tuple(f"serm:covered:{l}" for l in result.covered),
        reheat_conditions=("a required layer is reheated", "the measurement contract changes"),
    ))
    return inv_id
