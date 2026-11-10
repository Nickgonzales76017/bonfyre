"""Project safe Blogger API receipts into the occurrence and evidence spines."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sqlite3
from pathlib import Path

import evidence_graphs as evidence
import external_events

UTC = dt.timezone.utc
SAFE_RECEIPT_KEYS = {
    "id",
    "blog",
    "published",
    "updated",
    "url",
    "title",
    "status",
}


def _time(value: str) -> dt.datetime:
    parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def project_receipt(
    db: sqlite3.Connection,
    *,
    receipt_path: Path,
    source_refs: list[str],
    recorded_at: dt.datetime | None = None,
) -> dict[str, object]:
    receipt = json.loads(receipt_path.read_text())
    unexpected = set(receipt) - SAFE_RECEIPT_KEYS
    if unexpected:
        raise ValueError(f"unsafe Blogger receipt keys: {sorted(unexpected)}")
    for required in ("id", "published", "url", "title"):
        if not receipt.get(required):
            raise ValueError(f"Blogger receipt missing {required!r}")

    post_id = str(receipt["id"])
    post_ref = f"blogger-post:{post_id}"
    receipt_ref = receipt_path.as_posix()
    observed_at = _time(str(receipt["published"]))
    now = recorded_at or dt.datetime.now(UTC)
    payload = {
        key: receipt[key]
        for key in ("url", "title", "status", "published", "updated")
        if receipt.get(key) is not None
    }
    payload["source_refs"] = sorted(source_refs)

    external_events.ensure_schema(db)
    event_id = external_events.observe(
        db,
        source="blogger",
        actor="Bonfÿre/Aurekai publication surface",
        event_kind=external_events.OUTBOUND_SENT,
        subject_ref=post_ref,
        observed_at=observed_at,
        payload=payload,
        evidence_ref=receipt_ref,
        now=now,
    )

    evidence.record_provenance(
        db, artifact=post_ref, role="external_surface", node="blogger"
    )
    evidence.record_provenance(
        db, artifact=post_ref, role="api_receipt", node=receipt_ref
    )
    for source_ref in source_refs:
        evidence.record_transform(
            db,
            source=source_ref,
            operator="bin/blogger_publish.py",
            derived=post_ref,
        )
    public_claim = f"{post_ref}:publicly-addressable"
    receipt_claim = f"{post_ref}:api-acknowledged"
    evidence.relate_evidence(
        db, evidence=receipt_ref, kind=evidence.SUPPORTS, claim=receipt_claim
    )
    evidence.relate_evidence(
        db, evidence=str(receipt["url"]), kind=evidence.SUPPORTS, claim=public_claim
    )
    evidence.record_times(
        db,
        post_ref,
        {
            "publication_time": observed_at.isoformat(),
            "observation_time": now.astimezone(UTC).isoformat(),
            "retrieval_time": now.astimezone(UTC).isoformat(),
        },
    )
    evidence.add_claim(db, receipt_claim, proof_state="measured")
    evidence.add_claim(db, public_claim, proof_state="measured")
    return {
        "event_id": event_id,
        "post_ref": post_ref,
        "receipt_claim": receipt_claim,
        "public_claim": public_claim,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--source-ref", action="append", default=[])
    args = parser.parse_args()
    with sqlite3.connect(args.db) as db:
        result = project_receipt(
            db,
            receipt_path=args.receipt,
            source_refs=args.source_ref,
        )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
