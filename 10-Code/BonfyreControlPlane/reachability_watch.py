"""Keep reachable-capacity current: republish only when the state changes.

The last piece of the Feldera role at the grain the control plane honors: a
recompute is triggered by a change in the state a reachability answer depends on,
not by a hand call and not on a blind timer. This fingerprints exactly those
inputs -- actor verification, the proof frontier, authority grants, relationship
stages, resource activation, and the fabric's own WorkGraph -- and republishes
into the fabric the moment the fingerprint moves. A quiet system produces no
writes; a real change produces exactly one.

It is honest about being coarse: it recomputes the whole answer on any relevant
change rather than maintaining an incremental view. Promoting this to a true DBSP
incremental view over these tables is what the real Feldera substrate
(~/.bonfyre/substrates/v6.1/feldera) is for; this is the working bridge until
that is wired.
"""

from __future__ import annotations

import hashlib
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import fabric_publish as fp
import reachability_bridge as rbridge
import reachability_publish as rp

# The tables a reachability answer actually depends on. Missing ones are skipped
# -- the fingerprint reflects what exists.
CONTROL_DEPS = (
    "actor_nodes", "frontier_layers", "authority_edges",
    "relationships", "resource_candidates", "work_items",
)
FABRIC_DEPS = ("workgraph_nodes",)

STATE_FILE = Path.home() / ".bonfyre" / "estate-fabric" / "projections" / ".reachability.sig"


def _table_exists(con: sqlite3.Connection, name: str) -> bool:
    return con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone() is not None


def _table_fingerprint(con: sqlite3.Connection, table: str) -> str:
    if not _table_exists(con, table):
        return f"{table}:absent"
    rows = con.execute(f"SELECT * FROM {table}").fetchall()
    h = hashlib.sha256()
    for row in sorted(repr(r) for r in rows):
        h.update(row.encode("utf-8", "replace"))
    return f"{table}:{len(rows)}:{h.hexdigest()[:16]}"


def state_signature(
    control_db: Path = rp.CONTROL_DB, fabric: Path = fp.FABRIC
) -> str:
    """A deterministic fingerprint of the reachability-relevant state."""
    parts: list[str] = []
    con = sqlite3.connect(str(control_db))
    try:
        parts += [_table_fingerprint(con, t) for t in CONTROL_DEPS]
    finally:
        con.close()
    # The fabric part is 'absent' whether the file is missing or the table is --
    # so publishing (which creates the fabric file) never flips the signature and
    # triggers a spurious recompute. Never connect to a missing file.
    if fabric.exists():
        fcon = sqlite3.connect(str(fabric))
        try:
            parts += [_table_fingerprint(fcon, t) for t in FABRIC_DEPS]
        finally:
            fcon.close()
    else:
        parts += [f"{t}:absent" for t in FABRIC_DEPS]
    return hashlib.sha256("|".join(parts).encode()).hexdigest()


@dataclass(frozen=True)
class TickResult:
    changed: bool
    signature: str
    published_digest: Optional[str] = None


def tick(
    control_db: Path = rp.CONTROL_DB, fabric: Path = fp.FABRIC, state_file: Path = STATE_FILE
) -> TickResult:
    """Republish reachability iff the state fingerprint changed since last tick."""
    sig = state_signature(control_db, fabric)
    prev = state_file.read_text().strip() if state_file.exists() else ""
    if sig == prev:
        return TickResult(changed=False, signature=sig)
    # prefer the DBSP-maintained relation as the source of truth; fall back to the
    # Python computation only when the engine is not built.
    if rbridge.engine_available():
        published = rbridge.publish_maintained(fabric=fabric, control_db=control_db)
    else:
        published = rp.publish_reachability(fabric=fabric, control_db=control_db)
    state_file.parent.mkdir(parents=True, exist_ok=True)
    state_file.write_text(sig)
    return TickResult(changed=True, signature=sig, published_digest=published.digest)


def watch(interval: float = 10.0, *, control_db: Path = rp.CONTROL_DB,
          fabric: Path = fp.FABRIC, state_file: Path = STATE_FILE) -> None:
    """Poll for state changes and republish on each real change. Runs until killed."""
    while True:
        try:
            result = tick(control_db, fabric, state_file)
            if result.changed:
                print(f"[reachability-watch] republished on change: {result.published_digest[:12]}",
                      flush=True)
        except Exception as exc:  # keep the daemon alive across transient fabric locks
            print(f"[reachability-watch] tick error: {exc}", flush=True)
        time.sleep(interval)


def main() -> None:
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--once", action="store_true", help="tick once and exit")
    p.add_argument("--interval", type=float, default=10.0)
    a = p.parse_args()
    if a.once:
        r = tick()
        print("changed" if r.changed else "no change", r.signature[:16])
    else:
        watch(a.interval)


if __name__ == "__main__":
    main()
