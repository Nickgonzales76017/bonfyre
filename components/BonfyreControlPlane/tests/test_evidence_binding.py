"""EvidenceBindingGraph: a requirement binds a candidate only by a semantic test,
with a witness -- never by position, never twice."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import evidence_binding as eb


def _cands():
    return [
        eb.EvidenceCandidate("d1", kind="measurement", name="fpq roundtrip report", tags=("fpq",)),
        eb.EvidenceCandidate("d2", kind="patch", name="celld series", tags=("celld",)),
        eb.EvidenceCandidate("d3", kind="measurement", name="latency benchmark", tags=("perf",)),
    ]


def test_all_matchers_must_hold():
    req = eb.EvidenceRequirement("r", kind="measurement", name_contains=("fpq",))
    b = eb.bind(req, _cands())
    assert b is not None and b.digest == "d1"
    assert "kind=measurement" in b.witness and "name~fpq" in b.witness


def test_a_requirement_with_no_matcher_binds_nothing():
    # the anti-positional law: no matcher -> no bind, even with candidates present.
    req = eb.EvidenceRequirement("r")
    assert eb.bind(req, _cands()) is None
    assert eb.matches(req, _cands()[0]) is None


def test_kind_alone_is_ambiguous_but_still_by_test_not_position():
    req = eb.EvidenceRequirement("r", kind="measurement")
    b = eb.bind(req, _cands())
    # binds a measurement (d1 or d3), chosen deterministically -- never a patch.
    assert b is not None and b.digest in {"d1", "d3"}


def test_most_specific_candidate_wins():
    cands = [
        eb.EvidenceCandidate("loose", kind="measurement", name="report"),
        eb.EvidenceCandidate("tight", kind="measurement", name="fpq report", tags=("fpq",)),
    ]
    req = eb.EvidenceRequirement("r", kind="measurement", name_contains=("fpq",), tags=("fpq",))
    assert eb.bind(req, cands).digest == "tight"


def test_bind_all_is_a_matching_not_a_broadcast():
    reqs = [
        eb.EvidenceRequirement("evidence", kind="measurement", name_contains=("fpq",)),
        eb.EvidenceRequirement("any_measurement", kind="measurement"),
        eb.EvidenceRequirement("the_patch", kind="patch"),
    ]
    result = eb.bind_all(reqs, _cands())
    assert result.all_bound
    # the specific fpq requirement takes d1; the loose one cannot also take d1
    assert result.bound["evidence"].digest == "d1"
    assert result.bound["any_measurement"].digest == "d3"
    assert result.bound["the_patch"].digest == "d2"


def test_unbound_requirements_are_reported():
    reqs = [eb.EvidenceRequirement("needs_receipt", kind="receipt")]
    result = eb.bind_all(reqs, _cands())
    assert not result.all_bound
    assert result.unbound == ("needs_receipt",)
