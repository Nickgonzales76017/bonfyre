"""FormGraph compiles to an AtomicForm; a conditionally-required field is a
blocker only when its condition holds. AtomicForm is compile(FormGraph, state)."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import atomic_forms as af
import form_graph as fg


def _grant_form():
    return fg.FormGraph(
        form_id="grant", title="Grant application",
        fields=(
            fg.FieldSpec("applicant", base_required=True),
            fg.FieldSpec("requests_equipment", base_required=True),
            # required only when equipment is requested
            fg.FieldSpec("equipment_justification", base_required=True,
                         required_when=fg.RequiredWhen("requests_equipment", "yes")),
        ),
    )


def test_conditional_field_not_required_when_condition_absent():
    form = _grant_form()
    values = {"applicant": "Nick", "requests_equipment": "no"}
    assert fg.required_fields(form, values) == ["applicant", "requests_equipment"]
    compiled = fg.compile(form, values)
    # equipment_justification is not required here, so the form is submit-ready
    assert af.submit_ready(compiled).ready is True


def test_conditional_field_becomes_a_blocker_when_condition_holds():
    form = _grant_form()
    values = {"applicant": "Nick", "requests_equipment": "yes"}  # triggers the dependency
    assert "equipment_justification" in fg.required_fields(form, values)
    compiled = fg.compile(form, values)  # justification not provided
    readiness = af.submit_ready(compiled)
    assert readiness.ready is False
    assert any("equipment_justification" in b for b in readiness.blockers)


def test_compile_is_not_identity_with_the_graph():
    # AtomicForm = compile(FormGraph, state), not AtomicForm == FormGraph.
    form = _grant_form()
    compiled = fg.compile(form, {"requests_equipment": "yes", "applicant": "N",
                                 "equipment_justification": "microscope"})
    assert isinstance(compiled, af.AtomicForm)
    assert af.submit_ready(compiled).ready is True  # all required fields present


def test_evidence_matchers_pass_through():
    form = fg.FormGraph(
        form_id="f", title="t",
        evidence=(fg.EvidenceSpec("measurement", match_kind="measurement", match_name=("fpq",)),),
    )
    compiled = fg.compile(form, {})
    slot = compiled.evidence[0]
    assert slot.has_matcher and slot.match_kind == "measurement"
