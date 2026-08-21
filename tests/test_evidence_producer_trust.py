from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "evidence_producer_trust.py"
spec = importlib.util.spec_from_file_location("evidence_producer_trust", SCRIPT)
assert spec is not None and spec.loader is not None
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)


def identity(digest: str = "a" * 40, algorithm: str = "git-sha1"):
    return {"algorithm": algorithm, "digest": digest}


def observation():
    return {
        "expected_producer_locator": ".github/workflows/verify.yml",
        "observed_producer_locator": ".github/workflows/verify.yml",
        "expected_subject": "commit:abc123",
        "observed_subject": "commit:abc123",
        "current_subject": "commit:abc123",
        "trusted_producer_definition": identity("a" * 40),
        "observed_producer_definition": identity("a" * 40),
        "artifact_identity": identity("b" * 64, "sha256"),
    }


def test_honest_observation_requires_artifact_producer_and_subject_identity():
    result = module.evaluate(observation())

    assert result["ok"] is True
    assert result["reason"] == "trusted"
    assert result["producer"]["definition_match"] is True
    assert result["subject"]["current_match"] is True
    assert result["artifact"]["identity_bound"] is True
    assert result["artifact"]["identity_sufficient_for_producer_trust"] is False


def test_valid_artifact_from_modified_producer_fails_closed():
    value = observation()
    value["observed_producer_definition"] = identity("c" * 40)

    result = module.evaluate(value)

    assert result["ok"] is False
    assert result["reason"] == "producer_definition_changed"
    assert result["artifact"]["identity"] == identity("b" * 64, "sha256")
    assert result["claim_boundary"]["artifact_identity"] is True
    assert result["claim_boundary"]["producer_authorized"] is False


def test_same_name_spoof_at_wrong_locator_fails_before_artifact_claim():
    value = observation()
    value["observed_producer_locator"] = ".github/workflows/spoof/verify.yml"

    result = module.evaluate(value)

    assert result["ok"] is False
    assert result["reason"] == "producer_locator_mismatch"
    assert result["artifact"]["identity_sufficient_for_producer_trust"] is False


@pytest.mark.parametrize(
    ("field", "replacement"),
    [
        ("observed_subject", "commit:old"),
        ("current_subject", "commit:new"),
    ],
)
def test_stale_or_wrong_subject_rejects_replay(field, replacement):
    value = observation()
    value[field] = replacement

    result = module.evaluate(value)

    assert result["ok"] is False
    assert result["reason"] == "stale_or_wrong_subject"


def test_digest_algorithm_is_part_of_definition_identity():
    value = observation()
    value["observed_producer_definition"] = identity("a" * 40, "foreign-git-sha1")

    result = module.evaluate(value)

    assert result["ok"] is False
    assert result["reason"] == "producer_definition_changed"


@pytest.mark.parametrize(
    "bad_identity",
    [
        {"algorithm": "git-sha1", "digest": "not-hex"},
        {"algorithm": "BAD SPACE", "digest": "a" * 40},
        {"algorithm": "git-sha1", "digest": "a" * 7},
        {"algorithm": "git-sha1", "digest": "a" * 40, "authority": True},
    ],
)
def test_malformed_foreign_identity_does_not_get_normalized_into_trust(bad_identity):
    value = observation()
    value["observed_producer_definition"] = bad_identity

    with pytest.raises(module.TrustInputError):
        module.evaluate(value)


def test_semantic_correctness_and_effect_safety_are_not_upgraded_by_trust():
    result = module.evaluate(observation())

    assert result["claim_boundary"]["producer_authorized"] is True
    assert result["claim_boundary"]["artifact_semantically_correct"] is False
    assert result["claim_boundary"]["effect_safe"] is False
