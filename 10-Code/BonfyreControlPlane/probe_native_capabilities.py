#!/usr/bin/env python3
"""Record what each native command actually advertises.

The MCP inventory derived capabilities from a ten-keyword substring table, so
eighty of ninety families reported `[]`. An empty list reads as "this command
does nothing" when it means "nobody wrote a hint for it" -- the same
identity-versus-reality gap the capability ladder exists to close.

Commands know their own surface: they print it when asked. This runs each one,
extracts the subcommands it advertises, and writes a manifest the MCP server
reads, alongside the family manifest the installer writes.

    python3 probe_native_capabilities.py
"""

from __future__ import annotations

import json
import os
import re
import signal
import subprocess
from pathlib import Path

BIN_ROOT = Path.home() / ".bonfyre" / "bin"
MANIFEST = BIN_ROOT / ".capabilities.json"
FAMILIES = BIN_ROOT / ".families.json"

TIMEOUT_SECONDS = 5

# Commands that ignore --help and start working instead. Probing them costs a
# full timeout per flag and tells us nothing, so they are recorded as
# unprobeable rather than repeatedly waited on. That they ignore --help is
# itself worth fixing: it makes them hostile to any inventory.
IGNORES_HELP = {"bonfyre-watch", "bonfyre-qwen-fpq", "bonfyre-qwen-f-p-q"}

# Words that appear in usage text but are not subcommands.
NOISE = {
    "usage", "options", "commands", "command", "help", "version", "flags",
    "example", "examples", "where", "note", "notes", "see", "and", "or",
    "the", "a", "an", "file", "path", "dir", "out", "in",
}



def run_bounded(argv: list[str], *, cwd: Path) -> str:
    """Run a command, and actually reclaim it when it overruns.

    subprocess.run(timeout=) is not enough here. It kills the direct child, but
    `capture_output` keeps reading a pipe that grandchildren still hold open, so
    the call blocks past its own timeout -- the same pipe deadlock that hung Run
    6's supervisor on work that had already finished.

    The process gets its own session, so the whole group can be signalled, and
    the pipe is closed before waiting so no surviving grandchild can hold it.
    """
    process = subprocess.Popen(
        argv,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        cwd=str(cwd),
        start_new_session=True,
    )
    try:
        output, _ = process.communicate(timeout=TIMEOUT_SECONDS)
        return output or ""
    except subprocess.TimeoutExpired:
        for signal_number in (signal.SIGTERM, signal.SIGKILL):
            try:
                os.killpg(process.pid, signal_number)
            except (ProcessLookupError, PermissionError):
                break
            try:
                process.wait(timeout=2)
                break
            except subprocess.TimeoutExpired:
                continue
        # Drop our end of the pipe regardless; a wedged grandchild must not
        # keep this process waiting on it.
        if process.stdout is not None:
            process.stdout.close()
        return ""


def advertised_subcommands(binary: Path) -> list[str]:
    """Subcommands the binary names in its own usage text.

    Both streams are read: several commands print usage to stderr, which is how
    an earlier attempt to consume this output silently got nothing.
    """
    if binary.name in IGNORES_HELP:
        return []
    text = ""
    # Never invoke with no arguments. A watcher or server started bare will
    # simply run, and the probe hangs until its timeout -- which is what a
    # first pass across 88 binaries did.
    for flag in (["--help"], ["-h"]):
        try:
            text = run_bounded([str(binary), *flag], cwd=binary.parent)
        except OSError:
            continue
        if text.strip():
            break
    if not text.strip():
        return []

    name = re.escape(binary.name)
    found: list[str] = []
    # "  bonfyre-queue enqueue TYPE INPUT" -> enqueue
    for match in re.finditer(rf"{name}\s+([a-z][a-z0-9-]{{1,30}})", text):
        found.append(match.group(1))
    # "  enqueue TYPE INPUT" inside a usage block, where the binary name is
    # only printed on the first line.
    for line in text.splitlines():
        stripped = line.strip()
        candidate = re.match(r"^([a-z][a-z0-9-]{2,30})\s{2,}\S", stripped)
        if candidate:
            found.append(candidate.group(1))

    ordered: list[str] = []
    for item in found:
        if item in NOISE or item.startswith("-") or item == binary.name:
            continue
        if item not in ordered:
            ordered.append(item)
    return ordered


def main() -> None:
    try:
        families = json.loads(FAMILIES.read_text())
    except (OSError, ValueError):
        families = {}

    manifest: dict[str, dict] = {}
    probed = silent = 0

    for binary in sorted(BIN_ROOT.iterdir()):
        if not binary.is_file() or binary.name.startswith("."):
            continue
        if not os.access(binary, os.X_OK):
            continue
        subcommands = advertised_subcommands(binary)
        family = families.get(binary.name, "")
        if binary.name in IGNORES_HELP:
            manifest[binary.name] = {
                "family": family,
                "subcommands": [],
                "probed": False,
                "reason": "ignores --help and runs; probing it would block",
            }
            silent += 1
            continue
        # Absent means never probed; empty means probed and it advertised
        # nothing. Those are different facts and must not collapse.
        manifest[binary.name] = {
            "family": family,
            "subcommands": subcommands,
            "probed": True,
        }
        if subcommands:
            probed += 1
        else:
            silent += 1

    MANIFEST.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    print(
        json.dumps(
            {
                "binaries": len(manifest),
                "advertised_a_surface": probed,
                "advertised_nothing": silent,
                "manifest": str(MANIFEST),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
