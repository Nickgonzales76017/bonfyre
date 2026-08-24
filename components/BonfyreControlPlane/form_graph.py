"""FormGraph: the structure of a form, distinct from the AtomicForm it compiles to.

The atlas records the relation the code never had: ``AtomicForm = compile(FormGraph,
state)`` -- not ``AtomicForm == FormGraph``. A FormGraph is the reusable shape of
a form: its fields, its evidence slots, and the dependencies between them. The
AtomicForm is what you get when that shape is compiled against real values and
state -- an obligation machine that knows exactly what is still required *now*.

The capability AtomicForm lacked and this adds is the FieldDependencyGraph: a
field can be required only when a condition on another field holds. A grant form's
"equipment justification" is required only when "requests equipment" is yes. The
compile step resolves those conditions against the current values, so the
resulting AtomicForm's required-set is correct for this submission, not the
worst case.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

import atomic_forms as af


@dataclass(frozen=True)
class RequiredWhen:
    """A field is required only when ``field == value``. Absent => always required
    (subject to base_required)."""
    field_name: str
    equals: str


@dataclass(frozen=True)
class FieldSpec:
    name: str
    kind: str = "text"
    base_required: bool = True
    required_when: Optional[RequiredWhen] = None
    autofill_actor: str = ""


@dataclass(frozen=True)
class EvidenceSpec:
    name: str
    required: bool = True
    match_kind: str = ""
    match_name: tuple = ()
    match_tags: tuple = ()


@dataclass(frozen=True)
class FormGraph:
    form_id: str
    title: str
    fields: tuple[FieldSpec, ...] = ()
    evidence: tuple[EvidenceSpec, ...] = ()
    authority_required: tuple[str, ...] = ()
    review_gate: Optional[str] = None


def effective_required(spec: FieldSpec, values: dict[str, str]) -> bool:
    """Is this field required, given current values? Base requiredness, gated by
    any conditional dependency."""
    if not spec.base_required:
        return False
    cond = spec.required_when
    if cond is None:
        return True
    return values.get(cond.field_name) == cond.equals


def compile(
    fg: FormGraph,
    values: Optional[dict[str, str]] = None,
    *,
    authorities_present: Optional[list[str]] = None,
    review_passed: bool = False,
) -> af.AtomicForm:
    """Compile a FormGraph against values and state into an AtomicForm.

    The heart of the relation: each field's required flag is resolved from its
    dependency against the given values, so a conditionally-required field that
    the condition does not trigger is simply not a blocker. Evidence matchers and
    authority requirements pass straight through to the obligation machine.
    """
    vals = values or {}
    fields = [
        af.Field(
            name=spec.name, kind=spec.kind,
            required=effective_required(spec, vals),
            value=vals.get(spec.name),
            autofill_actor=spec.autofill_actor or None,
        )
        for spec in fg.fields
    ]
    evidence = [
        af.EvidenceSlot(
            name=e.name, required=e.required,
            match_kind=e.match_kind, match_name=tuple(e.match_name), match_tags=tuple(e.match_tags),
        )
        for e in fg.evidence
    ]
    return af.AtomicForm(
        form_id=fg.form_id, title=fg.title, fields=fields, evidence=evidence,
        authority_required=list(fg.authority_required),
        authorities_present=list(authorities_present or []),
        review_gate=fg.review_gate, review_passed=review_passed,
    )


def required_fields(fg: FormGraph, values: dict[str, str]) -> list[str]:
    """Which fields are required under these values -- the FieldDependencyGraph
    resolved. Useful for explaining a form without compiling it."""
    return [s.name for s in fg.fields if effective_required(s, values)]
