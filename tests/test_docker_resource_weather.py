from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "docker_resource_weather.py"
spec = importlib.util.spec_from_file_location("docker_resource_weather", SCRIPT)
assert spec is not None and spec.loader is not None
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)


def test_buildkit_aggregation_keeps_private_shared_mutable_distinct():
    result = module.aggregate_buildkit(
        [
            {
                "ID": "a",
                "Size": "100",
                "Reclaimable": True,
                "Shared": False,
                "Mutable": False,
                "Type": "regular",
                "LastUsedAt": "2026-08-01T00:00:00Z",
            },
            {
                "ID": "b",
                "Size": "200",
                "Reclaimable": True,
                "Shared": True,
                "Mutable": False,
                "Type": "source.git.checkout",
                "LastUsedAt": "2026-08-02T00:00:00Z",
            },
            {
                "ID": "c",
                "Size": "300",
                "Reclaimable": False,
                "Shared": False,
                "Mutable": True,
                "Type": "exec.cachemount",
            },
        ]
    )

    assert result["record_sum_bytes"] == 600
    assert result["reclaimable_record_sum_bytes"] == 300
    assert result["private_reclaimable_record_sum_bytes"] == 100
    assert result["shared_reclaimable_record_sum_bytes"] == 200
    assert result["mutable_record_sum_bytes"] == 300
    assert result["types"]["regular"]["record_sum_bytes"] == 100
    assert result["oldest_last_used_at"] == "2026-08-01T00:00:00Z"


def test_policy_prefers_buildkit_min_free_space_without_volumes():
    buildkit = {"reclaimable_record_sum_bytes": 12 * module.GIB}
    effects = module.policy_candidates(
        free_bytes=4 * module.GIB,
        target_free_bytes=20 * module.GIB,
        buildkit=buildkit,
    )

    assert effects[0]["command"] == [
        "docker",
        "buildx",
        "prune",
        "--force",
        "--min-free-space",
        "20gb",
    ]
    assert effects[1]["command_family"] == "docker system prune"
    assert effects[1]["volume_deletion_included"] is False


def test_no_gc_candidate_when_target_is_already_met():
    assert (
        module.policy_candidates(
            free_bytes=21 * module.GIB,
            target_free_bytes=20 * module.GIB,
            buildkit={"reclaimable_record_sum_bytes": 99 * module.GIB},
        )
        == []
    )


def test_system_df_sanitizer_drops_names_and_ids():
    rows = module.sanitize_system_df(
        [
            {
                "Type": "Images",
                "TotalCount": 10,
                "Active": 2,
                "Size": "8GB",
                "Reclaimable": "6GB (75%)",
                "Name": "private-registry.example/secret-project",
                "ID": "opaque",
            }
        ]
    )

    assert rows == [
        {
            "Type": "Images",
            "TotalCount": 10,
            "Active": 2,
            "Size": "8GB",
            "Reclaimable": "6GB (75%)",
        }
    ]


def test_ndjson_parser_rejects_non_object_records():
    try:
        module._json_records('{"Size":"1"}\n[1,2]\n', "fixture")
    except module.ObservationError as exc:
        assert "not an object" in str(exc) or "malformed JSON" in str(exc)
    else:
        raise AssertionError("non-object NDJSON must fail closed")


def test_buildkit_description_is_not_promoted_by_aggregator():
    result = module.aggregate_buildkit(
        [
            {
                "ID": "a",
                "Description": "RUN --mount=type=secret,id=token private-build-step",
                "Size": "1",
                "Reclaimable": True,
                "Shared": False,
                "Mutable": False,
                "Type": "regular",
            }
        ]
    )

    assert "private-build-step" not in str(result)
    assert "Description" not in str(result)
