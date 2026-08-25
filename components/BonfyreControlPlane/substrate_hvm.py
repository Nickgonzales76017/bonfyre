"""HVM substrate adapter: bounded recursive reduction, really invoked.

Of the 14 Reality Foundry substrates, HVM is the one with an invokable binary on
this system (`hvm`, the Higher-order Virtual Machine). This is a thin, honest
adapter over it -- not a reimplementation. It hands HVM a net, runs a bounded
reduction, and parses back the result and the interaction count (ITRS), which is
the real telemetry of the reduction.

The substrate owns the reduction *mechanics*; it does not own the meaning of what
it reduces. A reduction completing is not proof the reduced structure was correct
-- that stays with whatever architecture selected the structure (e.g.
FPQSeedProgramGraph). This adapter reports the mechanics and nothing more.
"""

from __future__ import annotations

import re
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


def _hvm_bin() -> Optional[str]:
    cargo = Path.home() / ".cargo" / "bin" / "hvm"
    if cargo.exists():
        return str(cargo)
    return shutil.which("hvm")


HVM = _hvm_bin()


def available() -> bool:
    return HVM is not None


@dataclass(frozen=True)
class Reduction:
    program: str
    result: str
    interactions: int          # ITRS -- the bounded reduction's real cost
    ok: bool
    raw: str


def reduce(program: str, *, timeout: int = 30) -> Reduction:
    """Run a bounded HVM reduction over an HVM2 net and parse its telemetry.

    ``program`` is HVM2 core text (e.g. ``@main = 42``). Returns the reduced
    result and the interaction count. Raises if HVM is not installed -- this
    adapter never fakes a reduction it did not run."""
    if HVM is None:
        raise RuntimeError("hvm not installed")
    with tempfile.NamedTemporaryFile("w", suffix=".hvm", delete=False) as fh:
        fh.write(program if program.endswith("\n") else program + "\n")
        path = fh.name
    try:
        proc = subprocess.run([HVM, "run", path], capture_output=True, text=True, timeout=timeout)
    finally:
        Path(path).unlink(missing_ok=True)
    raw = proc.stdout + proc.stderr
    m_res = re.search(r"Result:\s*(.+)", raw)
    m_itr = re.search(r"ITRS:\s*(\d+)", raw)
    return Reduction(
        program=program,
        result=(m_res.group(1).strip() if m_res else ""),
        interactions=int(m_itr.group(1)) if m_itr else 0,
        ok=bool(m_res) and proc.returncode == 0,
        raw=raw,
    )
