"""Feldera DBSP adapter: the reachability recompute as a real incremental view.

The control plane's watcher recomputes reachability in full on every change. This
is the substrate doing it properly: a DBSP circuit over the proof frontier's
proven-layer set, maintained incrementally. Proven layers stream in as a Z-set; a
reheat is a retraction; the view updates on each delta without recomputing. Built
from ~/.bonfyre/substrates/v6.1/feldera/probe/src/bin/reachability_incremental.rs
against the real dbsp crate.

This adapter runs the built circuit and reads back its telemetry -- never fakes
it. It is the incremental counterpart the reachability_watch docstring promised:
the true Feldera role, over reachability's core input.
"""

from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

_FELDERA_REL = Path.home() / ".bonfyre" / "substrates" / "v6.1" / "feldera" / "probe" / "target" / "release"
FELDERA_BIN = _FELDERA_REL / "reachability_incremental"
REACHABLE_BIN = _FELDERA_REL / "reachable_capacity"


def available() -> bool:
    return FELDERA_BIN.exists()


def reachable_capacity_available() -> bool:
    return REACHABLE_BIN.exists()


@dataclass(frozen=True)
class IncrementalView:
    view: str
    proven_total: tuple[int, ...]
    organism_route: tuple[int, ...]
    fpq_present: tuple[int, ...]
    cascade: str
    state: str
    raw: str

    @property
    def ok(self) -> bool:
        return self.state == "passed"

    @property
    def organism_withdrew_on_reheat(self) -> bool:
        """The organism route rose to 3 and fell after the SLI reheat."""
        return len(self.organism_route) >= 4 and max(self.organism_route) == 3 \
            and self.organism_route[-1] < 3

    @property
    def fpq_survived(self) -> bool:
        """FPQ's proof was never retracted by the SLI reheat."""
        return bool(self.fpq_present) and all(v >= 1 for v in self.fpq_present)


def run_incremental_view(timeout: int = 30) -> IncrementalView:
    """Run the DBSP reachability circuit and parse its incremental telemetry.

    The circuit maintains the FPQ->SLI->KV organism route under insert and
    retract: it rises as layers are proven and the organism withdraws when the
    SLI proof is reheated -- while the FPQ proof survives. Raises if the circuit
    is not built; never invents a result."""
    if not FELDERA_BIN.exists():
        raise RuntimeError("feldera reachability circuit not built")
    proc = subprocess.run([str(FELDERA_BIN)], capture_output=True, text=True, timeout=timeout)
    raw = (proc.stdout + proc.stderr).strip()
    line = next((l for l in raw.splitlines() if l.strip().startswith("{")), "{}")
    d = json.loads(line)
    return IncrementalView(
        view=d.get("view", ""),
        proven_total=tuple(d.get("proven_total", [])),
        organism_route=tuple(d.get("organism_route", [])),
        fpq_present=tuple(d.get("fpq_present", [])),
        cascade=d.get("cascade", ""),
        state=d.get("state", ""),
        raw=raw,
    )


@dataclass(frozen=True)
class ReachableCapacity:
    reachable_count: tuple[int, ...]
    organism_reachable: tuple[int, ...]
    maintained: str
    state: str

    @property
    def ok(self) -> bool:
        return self.state == "passed"

    @property
    def withdrawn_on_reheat(self) -> bool:
        """The organism was reachable, then a resolution retraction withdrew it."""
        seq = self.organism_reachable
        return bool(seq) and max(seq) == 1 and seq[-1] == 0


def run_reachable_capacity(timeout: int = 30) -> ReachableCapacity:
    """Run the maintained ReachableCapacity DBSP relation.

    Opportunities become reachable when all their blockers resolve, and are
    withdrawn incrementally when a resolution retracts (a proof reheat, an
    authority revoke) -- via antijoin over delta streams, not recompute. This is
    reachability maintained, not computed."""
    if not REACHABLE_BIN.exists():
        raise RuntimeError("feldera ReachableCapacity relation not built")
    proc = subprocess.run([str(REACHABLE_BIN)], capture_output=True, text=True, timeout=timeout)
    raw = (proc.stdout + proc.stderr).strip()
    line = next((l for l in raw.splitlines() if l.strip().startswith("{")), "{}")
    d = json.loads(line)
    return ReachableCapacity(
        reachable_count=tuple(d.get("reachable_count", [])),
        organism_reachable=tuple(d.get("organism_reachable", [])),
        maintained=d.get("maintained", ""),
        state=d.get("state", ""),
    )
