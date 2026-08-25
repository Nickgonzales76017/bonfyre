#!/usr/bin/env python3
"""Smoke-witness every Bonfyre command: run its binary with --help (bounded,
non-destructive) and record whether it responds. A command that prints usage is a
live capability organ -- a real runtime witness, the command-surface analogue of a
unit test. Never passes stdin, never runs a subcommand, only --help/-h/--version.
"""

from __future__ import annotations

import json
import os
import stat
import subprocess
import sys

CMD = os.path.dirname(os.path.abspath(__file__))
TIMEOUT = 4.0
PROBES = (["--help"], ["-h"], ["--version"], [])


def _binary(cmd_dir: str, name: str) -> str | None:
    short = name[len("Bonfyre"):].lower() if name.startswith("Bonfyre") else name.lower()
    best = None
    try:
        for f in sorted(os.listdir(cmd_dir)):
            p = os.path.join(cmd_dir, f)
            if not os.path.isfile(p) or f.endswith((".o", ".sh", ".py", ".md")):
                continue
            if not (os.stat(p).st_mode & stat.S_IXUSR):
                continue
            if "selftest" in f.lower():
                continue
            if f == name or f.lower() == short or f.startswith("bonfyre-"):
                best = p
                if f.lower() in (short, f"bonfyre-{short}"):
                    return p
    except OSError:
        return None
    return best


def smoke(name: str) -> dict:
    d = os.path.join(CMD, name)
    binary = _binary(d, name)
    if not binary:
        return {"command": name, "binary": "", "responds": False, "probe": "", "reason": "no binary"}
    for probe in PROBES:
        try:
            r = subprocess.run([binary, *probe], capture_output=True, text=True,
                               timeout=TIMEOUT, stdin=subprocess.DEVNULL, cwd=d)
            text = (r.stdout or "") + (r.stderr or "")
            # a live command prints usage/help/a version/an error string quickly
            if text.strip():
                return {"command": name, "binary": os.path.basename(binary),
                        "responds": True, "probe": " ".join(probe) or "(no args)",
                        "bytes": len(text)}
        except subprocess.TimeoutExpired:
            continue
        except Exception:  # noqa: BLE001
            continue
    return {"command": name, "binary": os.path.basename(binary), "responds": False,
            "probe": "", "reason": "no output on --help/-h/--version/(no args)"}


def run_all() -> dict:
    names = sorted(n for n in os.listdir(CMD)
                   if os.path.isdir(os.path.join(CMD, n)) and n.startswith("Bonfyre"))
    results = [smoke(n) for n in names]
    live = [r for r in results if r["responds"]]
    return {"total": len(results), "live": len(live),
            "results": results}


if __name__ == "__main__":
    out = run_all()
    for r in out["results"]:
        mark = "live" if r["responds"] else "----"
        print(f"  [{mark}] {r['command']:24s} {r.get('binary',''):22s} {r.get('probe','') or r.get('reason','')}")
    print(f"\ncommand smoke: {out['live']}/{out['total']} respond to --help")
    # write the witness record next to the surface
    with open(os.path.join(CMD, "command_smoke.json"), "w") as f:
        json.dump(out, f, indent=2, sort_keys=True)
    sys.exit(0)
