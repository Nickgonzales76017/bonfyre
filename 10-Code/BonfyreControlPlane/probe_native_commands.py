#!/usr/bin/env python3
"""Compile the real capability catalog from cmd/Bonfyre*.

Import tooling, which the constitutional rule permits in Python. It adds no new
semantics: the ladder and the callable/proven floors come from the frozen
reference, and the same rungs exist natively in bf_control.h.

Why this exists: `native_tool_inventory` reports 0 tools while 94 binaries are
built and runnable. A plane reading that inventory concludes the capability does
not exist and starts probing PATH -- the exact Run 6 pathology, arriving through
a broken transport rather than a missing build. Recording what is actually on
disk, with the transport failure recorded as its own deficit, is the fix.

    python3 probe_native_commands.py [--db control_plane.db]
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sqlite3
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import capability_catalog as cc

UTC = dt.timezone.utc
REPO_ROOT = Path(__file__).resolve().parents[2]
CMD_ROOT = REPO_ROOT / "cmd"
DEFAULT_DB = Path(__file__).resolve().parent / "control_plane.db"

# A binary that answers --help or --version is resolvable and health-probed, but
# nothing more. `--help` working means nothing about whether it does its job, so
# nothing here is ever promoted past health_probed by this script.
HEALTH_TIMEOUT_SECONDS = 10


def discover() -> list[tuple[str, Path]]:
    """Public identity -> built binary, for every cmd/Bonfyre* that has one."""
    found: list[tuple[str, Path]] = []
    if not CMD_ROOT.is_dir():
        return found
    for directory in sorted(CMD_ROOT.iterdir()):
        if not directory.is_dir() or not directory.name.startswith("Bonfyre"):
            continue
        for candidate in sorted(directory.iterdir()):
            if not candidate.is_file():
                continue
            if candidate.suffix in {".c", ".h", ".o", ".md", ".sh"}:
                continue
            if candidate.name == "Makefile":
                continue
            if os.access(candidate, os.X_OK):
                found.append((directory.name, candidate))
                break
    return found


def health_probe(binary: Path) -> tuple[bool, str]:
    """Does it answer at all? Returns (responded, short detail)."""
    for flag in ("--version", "--help"):
        try:
            completed = subprocess.run(
                [str(binary), flag],
                capture_output=True,
                text=True,
                timeout=HEALTH_TIMEOUT_SECONDS,
                cwd=str(binary.parent),
            )
        except (subprocess.TimeoutExpired, OSError) as error:
            return False, f"{flag}: {type(error).__name__}"
        output = (completed.stdout or "") + (completed.stderr or "")
        if output.strip():
            first = output.strip().splitlines()[0][:120]
            return True, f"{flag}: {first}"
    return False, "no output from --version or --help"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=str(DEFAULT_DB))
    parser.add_argument("--no-health", action="store_true",
                        help="record resolvability only, do not execute anything")
    args = parser.parse_args()

    db = sqlite3.connect(args.db)
    cc.ensure_schema(db)
    now = dt.datetime.now(UTC)

    discovered = discover()
    healthy = 0

    for identity, binary in discovered:
        maturity = "resolvable"
        detail = ""
        if not args.no_health:
            responded, detail = health_probe(binary)
            if responded:
                maturity = "health_probed"
                healthy += 1
        cc.declare(
            db,
            cc.Capability(
                public_name=identity,
                kind="command",
                maturity=maturity,
                location=str(binary),
                placement="local",
                # Nothing here has been exercised on an ordinary workload by
                # this script, so nothing claims workload_proven.
                proof_ref="",
                cost_hint="",
            ),
            now=now,
        )
        if detail:
            db.execute(
                "UPDATE capability_identities SET probe_detail=? WHERE public_name=?",
                (detail, identity),
            )
            db.commit()

    summary = cc.summary(db)
    report = {
        "discovered": len(discovered),
        "health_probed": healthy,
        "catalog": summary,
        "inventory_disagreement": {
            "native_tool_inventory_count": 0,
            "binaries_on_disk": len(discovered),
            "note": (
                "MCP native_tool_inventory reports 0 while these are built and "
                "runnable from a shell. The MCP server is denied access to the "
                "repository directory by the OS, so the transport is the "
                "deficit, not the capability."
            ),
        },
    }
    db.close()
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
