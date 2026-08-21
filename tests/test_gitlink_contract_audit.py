from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "gitlink_contract_audit.py"
spec = importlib.util.spec_from_file_location("gitlink_contract_audit", SCRIPT)
assert spec is not None and spec.loader is not None
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)


def stage(*records: tuple[str, str, str]) -> bytes:
    return b"".join(
        f"{mode} {sha} 0\t{path}\0".encode()
        for mode, sha, path in records
    )


def test_parse_stage_records_selects_only_mode_160000():
    raw = stage(
        ("100644", "a" * 40, "README.md"),
        ("160000", "b" * 40, "10-Code/BonfyreCMS"),
        ("160000", "c" * 40, "vendor/hvm4"),
    )

    assert module.parse_stage_records(raw) == {
        "10-Code/BonfyreCMS": "b" * 40,
        "vendor/hvm4": "c" * 40,
    }


def test_orphan_gitlink_is_distinct_from_declared_submodule():
    declarations, initial = module.parse_gitmodules(
        '[submodule "vendor/hvm4"]\n'
        '    path = vendor/hvm4\n'
        '    url = https://github.com/HigherOrderCO/HVM4.git\n'
    )
    result = module.audit_from_records(
        {
            "10-Code/BonfyreCMS": "b" * 40,
            "vendor/hvm4": "c" * 40,
        },
        declarations,
        initial,
    )

    assert result["ok"] is False
    assert result["finding_count"] == 1
    assert result["findings"][0]["kind"] == "orphan_gitlink"
    assert result["findings"][0]["path"] == "10-Code/BonfyreCMS"
    assert result["mutation_performed"] is False


def test_declared_path_without_gitlink_and_missing_url_are_separate_findings():
    declarations, initial = module.parse_gitmodules(
        '[submodule "ghost"]\n'
        '    path = vendor/ghost\n'
    )
    result = module.audit_from_records({}, declarations, initial)

    assert result["finding_kinds"] == {
        "declaration_missing_url": 1,
        "declaration_without_gitlink": 1,
    }


def test_duplicate_declaration_path_is_not_collapsed_silently():
    declarations, initial = module.parse_gitmodules(
        '[submodule "one"]\n'
        '    path = vendor/shared\n'
        '    url = https://example.invalid/one.git\n'
        '[submodule "two"]\n'
        '    path = vendor/shared\n'
        '    url = https://example.invalid/two.git\n'
    )
    result = module.audit_from_records(
        {"vendor/shared": "d" * 40},
        declarations,
        initial,
    )

    assert result["finding_kinds"] == {"duplicate_declaration_path": 1}


def test_dirty_module_paths_do_not_gain_canonical_identity():
    declarations, initial = module.parse_gitmodules(
        '[submodule "dirty"]\n'
        '    path = vendor/../outside\n'
        '    url = https://example.invalid/dirty.git\n'
    )
    result = module.audit_from_records({}, declarations, initial)

    assert declarations == []
    assert result["finding_kinds"] == {"invalid_declaration_path": 1}


def test_missing_module_path_is_classified_without_mutation():
    declarations, initial = module.parse_gitmodules(
        '[submodule "nameless"]\n'
        '    url = https://example.invalid/nameless.git\n'
    )
    result = module.audit_from_records({}, declarations, initial)

    assert result["finding_kinds"] == {"declaration_missing_path": 1}
    assert result["mutation_performed"] is False
