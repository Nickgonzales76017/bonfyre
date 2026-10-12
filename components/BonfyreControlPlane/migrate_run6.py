#!/usr/bin/env python3
"""Import the frozen Run 6 state into the typed control plane.

Reads capital.db read-only and writes a separate control_plane.db. The freeze is
evidence and is never mutated: if this migration is wrong, rerunning it costs
nothing and the original record still says exactly what it said.

Usage:
    python3 migrate_run6.py [--dry-run]
"""

from __future__ import annotations

import argparse
import datetime as dt
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import capability_catalog as cc
import external_events as ee
import provider_state as ps
import work_graph as wg

UTC = dt.timezone.utc

CAPITAL_DB = (
    Path.home() / "Library/Application Support/Bonfyre/CapitalGym/capital.db"
)
CONTROL_DB = Path(__file__).resolve().parent / "control_plane.db"


def open_source() -> sqlite3.Connection:
    db = sqlite3.connect(f"file:{CAPITAL_DB}?mode=ro", uri=True)
    db.row_factory = sqlite3.Row
    return db


def parse_ts(text: str | None) -> dt.datetime | None:
    if not text:
        return None
    cleaned = text.strip().replace(" ", "T", 1) if " " in text.strip() else text.strip()
    try:
        parsed = dt.datetime.fromisoformat(cleaned)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def migrate(dry_run: bool = False) -> dict:
    source = open_source()
    target = sqlite3.connect(":memory:" if dry_run else str(CONTROL_DB))
    wg.ensure_schema(target)
    ps.ensure_schema(target)
    ee.ensure_schema(target)
    cc.ensure_schema(target)

    report: dict = {}

    # 1. Planes become typed identities before any work can reference them.
    planes = [r["plane_id"] for r in source.execute("SELECT plane_id FROM run6_plane_state")]
    for plane in planes:
        wg.register_plane(target, plane)
    report["planes_registered"] = len(planes)

    # 2. Queue rows become work items. Unroutable ones are refused here rather
    #    than counted by a governor afterwards.
    imported = duplicates = rejected = 0
    rejected_targets: dict[str, int] = {}
    for row in source.execute(
        "SELECT * FROM run6_cross_plane_queue ORDER BY id"
    ):
        try:
            item = wg.enqueue(
                target,
                source_plane=row["source_plane"],
                target_plane=row["target_plane"],
                item_kind=row["item_kind"],
                subject_ref=row["subject_ref"],
                reason=row["reason"],
                source_ref=row["source_ref"],
                priority=row["priority"],
                now=parse_ts(row["created_at"]) or dt.datetime.now(UTC),
            )
        except wg.UnknownPlane:
            rejected += 1
            rejected_targets[row["target_plane"]] = (
                rejected_targets.get(row["target_plane"], 0) + 1
            )
            continue
        if item is None:
            duplicates += 1
        else:
            imported += 1
    report["work_imported"] = imported
    report["work_duplicates_collapsed"] = duplicates
    report["work_rejected_unroutable"] = rejected
    report["rejected_targets"] = rejected_targets

    # 3. Mutable provider rows become the observations that imply them. The
    #    frozen row is a projection; what we can honestly reconstruct is the
    #    manual pause and the hard-capacity hit it recorded.
    provider_events = 0
    for row in source.execute("SELECT * FROM run6_provider_state"):
        provider = row["provider"]
        started = parse_ts(row["last_started_at"])
        succeeded = parse_ts(row["last_success_at"])
        if succeeded:
            ps.record(target, provider, ps.SUCCESS, observed_at=succeeded)
            provider_events += 1
        if row["hard_capacity_hits"]:
            kind, reset = ps.classify_failure(row["last_error"] or "usage limit")
            ps.record(
                target,
                provider,
                ps.HARD_CAPACITY,
                observed_at=started or dt.datetime.now(UTC),
                reset_at=reset,
                detail=row["last_error"] or "",
            )
            provider_events += 1
        if "manual_pause" in (row["status"] or ""):
            ps.record(
                target,
                provider,
                ps.MANUAL_PAUSE,
                observed_at=parse_ts(row["updated_at"]) or dt.datetime.now(UTC),
                detail=row["last_error"] or "manual freeze",
            )
            provider_events += 1
    report["provider_observations"] = provider_events
    report["provider_states"] = {
        provider: ps.current_state(target, provider).status
        for provider in {r["provider"] for r in source.execute("SELECT provider FROM run6_provider_state")}
    }

    # 4. Economic facts move into typed categories that cannot be summed.
    ledger_rows = 0
    try:
        for row in source.execute(
            "SELECT name,asking_value,realized_value,status,waiting_on"
            " FROM commercial_opportunities"
            if _has_column(source, "commercial_opportunities", "name")
            else "SELECT problem AS name,asking_value,realized_value,status,waiting_on"
            " FROM commercial_opportunities"
        ):
            ee.record_commitment(
                target,
                actor=row["waiting_on"] or "unknown",
                category=ee.QUALIFIED_ASK,
                amount_usd=float(row["asking_value"] or 0),
                detail=str(row["name"] or "")[:300],
            )
            ledger_rows += 1
            if float(row["realized_value"] or 0) > 0:
                ee.record_commitment(
                    target,
                    actor=row["waiting_on"] or "unknown",
                    category=ee.REALIZED_CASH,
                    amount_usd=float(row["realized_value"]),
                )
                ledger_rows += 1
    except sqlite3.Error as error:
        report["ledger_error"] = str(error)
    report["ledger_entries"] = ledger_rows
    report["ledger_totals"] = ee.ledger_totals(target)

    report["work_states"] = wg.state_counts(target)
    source.close()
    if not dry_run:
        target.close()
    return report


def _has_column(db: sqlite3.Connection, table: str, column: str) -> bool:
    return any(
        row[1] == column for row in db.execute(f"PRAGMA table_info({table})").fetchall()
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    report = migrate(dry_run=args.dry_run)
    import json

    print(json.dumps(report, indent=2, default=str))


if __name__ == "__main__":
    main()
