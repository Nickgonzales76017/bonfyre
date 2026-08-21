from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "agent_trace_0_1_import.py"
spec = importlib.util.spec_from_file_location("agent_trace_import", SCRIPT)
assert spec is not None and spec.loader is not None
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def minimal():
    return {
        "version": "0.1.0",
        "id": "550e8400-e29b-41d4-a716-446655440000",
        "timestamp": "2026-01-25T10:00:00Z",
        "vcs": {
            "type": "git",
            "revision": "a" * 40,
        },
        "tool": {"name": "cursor", "version": "2.4.0"},
        "files": [
            {
                "path": "src/app.ts",
                "conversations": [
                    {
                        "url": "https://example.invalid/conversations/1",
                        "contributor": {
                            "type": "ai",
                            "model_id": "example/model-v1",
                        },
                        "ranges": [
                            {
                                "start_line": 1,
                                "end_line": 5,
                                "content_hash": "murmur3:9f2e8a1b",
                            }
                        ],
                    }
                ],
            }
        ],
    }


def test_projection_preserves_attribution_without_upgrading_claims():
    result = module.project(minimal())

    assert result["claim_boundary"]["attribution"] is True
    for unsupported in (
        "task_success",
        "semantic_correctness",
        "authority_compliance",
        "effect_safety",
        "provider_fidelity",
        "evidence_sufficiency",
        "quality_assessment",
        "legal_ownership",
        "training_data_provenance",
    ):
        assert result["claim_boundary"][unsupported] is False

    contributor = result["files"][0]["conversations"][0]["ranges"][0]["contributor"]
    assert contributor["actor_class"] == "agent"
    assert contributor["foreign_model_id"] == "example/model-v1"
    assert contributor["provider_fidelity_claimed"] is False


def test_foreign_hash_remains_algorithm_qualified_not_security_identity():
    result = module.project(minimal())
    digest = result["files"][0]["conversations"][0]["ranges"][0]["foreign_content_hash"]

    assert digest == {
        "algorithm": "murmur3",
        "foreign_digest": "9f2e8a1b",
        "bonfyre_security_identity": False,
    }


def test_conversation_and_related_urls_are_references_not_dereferenced():
    trace = minimal()
    trace["files"][0]["conversations"][0]["related"] = [
        {"type": "session", "url": "https://example.invalid/session/2"}
    ]

    result = module.project(trace)
    conversation = result["files"][0]["conversations"][0]

    assert conversation["dereferenced"] is False
    assert conversation["related"][0]["dereferenced"] is False


def test_metadata_is_bound_but_not_automatically_promoted():
    trace = minimal()
    trace["metadata"] = {"vendor.private": {"workspace_id": "opaque"}}

    result = module.project(trace)

    assert len(result["foreign_metadata_sha256"]) == 64
    assert result["foreign_metadata_promoted"] is False
    assert "workspace_id" not in str(result)


@pytest.mark.parametrize(
    "path",
    ["/etc/passwd", "../escape.py", "C:\\Users\\x.py", "./relative.py"],
)
def test_foreign_file_paths_must_be_clean_repo_relative(path):
    trace = minimal()
    trace["files"][0]["path"] = path

    with pytest.raises(module.ImportError):
        module.project(trace)


def test_range_override_preserves_handoff_without_mutating_conversation_identity():
    trace = minimal()
    trace["files"][0]["conversations"][0]["ranges"][0]["contributor"] = {
        "type": "mixed"
    }

    result = module.project(trace)
    contributor = result["files"][0]["conversations"][0]["ranges"][0]["contributor"]

    assert contributor["actor_class"] == "mixed"
    assert contributor["foreign_contributor_type"] == "mixed"


def test_git_revision_shape_is_validated():
    trace = minimal()
    trace["vcs"]["revision"] = "short"

    with pytest.raises(module.ImportError):
        module.project(trace)


def test_content_hash_must_name_its_algorithm():
    trace = minimal()
    trace["files"][0]["conversations"][0]["ranges"][0]["content_hash"] = "deadbeef"

    with pytest.raises(module.ImportError):
        module.project(trace)
