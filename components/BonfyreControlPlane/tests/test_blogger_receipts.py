import json
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import blogger_receipts
import evidence_graphs as evidence


def test_safe_blogger_receipt_projects_once_into_occurrence_and_evidence(tmp_path):
    receipt = tmp_path / "blogger-42.json"
    receipt.write_text(
        json.dumps(
            {
                "id": "42",
                "published": "2026-08-21T03:30:00Z",
                "updated": "2026-08-21T03:30:00Z",
                "url": "https://example.blogspot.com/2026/08/evidence.html",
                "title": "Evidence",
                "status": "LIVE",
            }
        )
    )
    db = sqlite3.connect(":memory:")
    first = blogger_receipts.project_receipt(
        db, receipt_path=receipt, source_refs=["sha256:abc"]
    )
    second = blogger_receipts.project_receipt(
        db, receipt_path=receipt, source_refs=["sha256:abc"]
    )

    assert first["event_id"] is not None
    assert second["event_id"] is None
    assert db.execute("SELECT COUNT(*) FROM external_event_log").fetchone()[0] == 1
    assert evidence.supports(db, receipt.as_posix(), first["receipt_claim"])
    assert evidence.supports(
        db,
        "https://example.blogspot.com/2026/08/evidence.html",
        first["public_claim"],
    )
    assert evidence.lineage(db, first["post_ref"]) == ["sha256:abc"]
    times = evidence.times_of(db, first["post_ref"])
    assert times["publication_time"] != times["retrieval_time"]


def test_receipt_rejects_unexpected_api_fields(tmp_path):
    receipt = tmp_path / "unsafe.json"
    receipt.write_text(
        json.dumps(
            {
                "id": "42",
                "published": "2026-08-21T03:30:00Z",
                "url": "https://example.blogspot.com/post",
                "title": "Evidence",
                "access_token": "must-not-persist",
            }
        )
    )
    db = sqlite3.connect(":memory:")
    try:
        blogger_receipts.project_receipt(db, receipt_path=receipt, source_refs=[])
    except ValueError as exc:
        assert "unsafe Blogger receipt keys" in str(exc)
    else:
        raise AssertionError("unsafe API fields must be rejected")
