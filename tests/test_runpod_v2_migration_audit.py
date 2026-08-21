from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "runpod_v2_migration_audit.py"
spec = importlib.util.spec_from_file_location("runpod_audit", SCRIPT)
assert spec is not None and spec.loader is not None
audit = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = audit
spec.loader.exec_module(audit)


def test_direct_v1_and_graphql_hits_do_not_echo_secret(tmp_path: Path) -> None:
    path = tmp_path / "a.py"
    path.write_text(
        'url="https://api.runpod.io/graphql"\n'
        'old="https://api.runpod.io/v1/pods?token=SECRET-CANARY"\n',
        encoding="utf-8",
    )

    result = audit.scan(tmp_path, [path])

    assert result["finding_count"] >= 2
    assert "SECRET-CANARY" not in json.dumps(result)
    assert result["migration_state"] == "OBLIGATIONS_PRESENT"


def test_clean_v2_reference_is_clean(tmp_path: Path) -> None:
    path = tmp_path / "client.py"
    path.write_text('BASE="https://api.runpod.io/v2"\n', encoding="utf-8")

    result = audit.scan(tmp_path, [path])

    assert result["finding_count"] == 0
    assert result["migration_state"] == "CLEAN"


def test_binary_and_large_files_are_not_scanned(tmp_path: Path, monkeypatch) -> None:
    binary = tmp_path / "x.py"
    binary.write_bytes(b"\x00https://api.runpod.io/graphql")
    huge = tmp_path / "huge.py"
    huge.write_text("x" * 20, encoding="utf-8")
    monkeypatch.setattr(audit, "MAX_FILE_BYTES", 10)

    result = audit.scan(tmp_path, [binary, huge])

    assert result["finding_count"] == 0
    assert result["skipped_large"] == [{"path": "huge.py", "bytes": 20}]


def test_line_family_and_evidence_digest_are_stable(tmp_path: Path) -> None:
    path = tmp_path / "x.md"
    path.write_text("one\nRunpod GraphQL migration\n", encoding="utf-8")

    first = audit.scan(tmp_path, [path])
    second = audit.scan(tmp_path, [path])

    assert first["findings"] == second["findings"]
    finding = first["findings"][0]
    assert finding["line"] == 2
    assert finding["legacy_family"] == "graphql"
    assert len(finding["evidence_sha256"]) == 64
