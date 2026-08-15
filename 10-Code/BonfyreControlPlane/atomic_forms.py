"""AtomicForm: the human boundary as a compiled, evidence-backed package.

The essay's §23 point -- an application form is not fields, it is eligibility,
required evidence, autofillable verified facts, authorizations, and review
gates, compiled into a submit-ready package. This is the seam where human input,
the fabric, the actor graph, and authority meet.

It ties four things built this session:
  fields        autofilled from verified actors in the actor graph, so a fact we
                confirmed is not retyped -- and an unverified actor cannot autofill.
  evidence      each slot is backed by a content-addressed fabric artifact, the
                same artifacts ForeignTwin materialization produces, so a foreign
                projection or a real patch becomes an evidence slot.
  authority     a form requiring an authority is not submit-ready until it is
                present -- the proof-frontier discipline applied to submission.
  submit        a package is ready only when every required field, evidence slot,
                authority, and review gate is satisfied; otherwise it reports the
                exact blockers, never a false green.

An AtomicForm never submits itself. It compiles the package and says whether a
human may. The submit action is an external effect and stays a human decision.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from typing import Optional, Sequence

VERIFIED = "verified"  # only a verified actor may autofill a field


@dataclass
class Field:
    name: str
    kind: str
    required: bool = True
    value: Optional[str] = None
    autofill_actor: Optional[str] = None  # actor_id whose display_name fills it


@dataclass
class EvidenceSlot:
    name: str
    required: bool = True
    artifact_digest: Optional[str] = None  # a fabric artifact
    from_twin: Optional[str] = None         # or a ForeignTwin projection


@dataclass
class AtomicForm:
    form_id: str
    title: str
    fields: list[Field] = field(default_factory=list)
    evidence: list[EvidenceSlot] = field(default_factory=list)
    authority_required: list[str] = field(default_factory=list)
    authorities_present: list[str] = field(default_factory=list)
    review_gate: Optional[str] = None
    review_passed: bool = False


@dataclass(frozen=True)
class SubmitReadiness:
    form_id: str
    ready: bool
    blockers: tuple[str, ...]

    @property
    def blocker_count(self) -> int:
        return len(self.blockers)


def autofill(form: AtomicForm, actors_db: sqlite3.Connection) -> int:
    """Fill fields from verified actors. An unverified actor never autofills.

    The same discipline the actor graph enforces: a fact read from a live
    directory (verified) can populate a form; a fact only asserted from a brief
    cannot, because a form submitted on a guess is worse than a blank one.
    """
    filled = 0
    for f in form.fields:
        if f.value is not None or not f.autofill_actor:
            continue
        row = actors_db.execute(
            "SELECT display_name, confidence FROM actor_nodes WHERE actor_id=?",
            (f.autofill_actor,),
        ).fetchone()
        if row and row[1] == VERIFIED:
            f.value = row[0]
            filled += 1
    return filled


def submit_ready(form: AtomicForm) -> SubmitReadiness:
    """Compile the package. Ready only when everything required is satisfied."""
    blockers: list[str] = []

    for f in form.fields:
        if f.required and (f.value is None or f.value == ""):
            hint = f" (autofill actor {f.autofill_actor} not verified)" if f.autofill_actor else ""
            blockers.append(f"field '{f.name}' is empty{hint}")

    for slot in form.evidence:
        if slot.required and not (slot.artifact_digest or slot.from_twin):
            blockers.append(f"evidence slot '{slot.name}' has no artifact")

    for authority in form.authority_required:
        if authority not in form.authorities_present:
            blockers.append(f"authority '{authority}' not present")

    if form.review_gate and not form.review_passed:
        blockers.append(f"review gate '{form.review_gate}' not passed")

    return SubmitReadiness(form.form_id, not blockers, tuple(blockers))
