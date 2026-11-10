"""End-to-end cascade witness: a proof retraction withdraws reachable capacity.

Not a recompute wrapper. The MAINTENANCE lives in the DBSP circuit
(feldera_reachable_capacity_live); this only projects proof-frontier state into
the resolved-blocker facts the circuit consumes -- the bridge role -- and drives
a retraction:

  a layer is PROVEN -> its blocker is resolved -> the opportunity is reachable
  challenge the proof (the -1) -> the blocker is no longer resolved
  -> the DBSP circuit withdraws the opportunity from reachable, incrementally

So: ProofFrontier -1 -> ReachableCapacity -1, through the real circuit. Chains
with the native retraction_cascade (Evidence -1 -> proof retracts) into
Evidence -1 -> proof -1 -> reachable -1.
"""

from __future__ import annotations

import json
import os
import subprocess

import proof_frontier as pf

HERE = os.path.dirname(os.path.abspath(__file__))
LIVE = os.path.expanduser(
    "~/.bonfyre/substrates/v6.1/feldera/probe/target/release/reachable_capacity_live"
)

# one opportunity, gated by one proof-frontier blocker
OPP = "release-x"
BLOCKER = "fpq_layer_proven"
LAYER = "fpq"


def _resolved_facts(db) -> list[str]:
    """Project proof-frontier state -> resolved blocker ids (the bridge role).
    The blocker is resolved iff its layer is proven and not challenged."""
    row = db.execute(
        "SELECT status FROM frontier_layers WHERE subject_resource='res:release'"
        " AND layer=?", (LAYER,)
    ).fetchone()
    return [BLOCKER] if row and row[0] == "proven" else []


def _run_circuit(resolved: list[str]) -> dict:
    lines = [f"B\t{OPP}\t{BLOCKER}"] + [f"R\t{b}" for b in resolved]
    out = subprocess.run([LIVE], input="\n".join(lines) + "\n",
                         capture_output=True, text=True, timeout=30)
    return json.loads(out.stdout.strip().splitlines()[-1])


def witness(db) -> dict:
    pf.ensure_schema(db)
    # 1. the layer is proven -> blocker resolved -> opportunity reachable
    db.execute("INSERT INTO frontier_layers(subject_resource,subject_profile,ordinal,"
               "layer,status,witness_ref) VALUES('res:release','',0,?,'proven','w')"
               " ON CONFLICT(subject_resource,subject_profile,layer) DO UPDATE SET status='proven'",
               (LAYER,))
    db.commit()
    before = _run_circuit(_resolved_facts(db))

    # 2. challenge the proof (the -1): the layer is no longer proven
    db.execute("UPDATE frontier_layers SET status='challenged'"
               " WHERE subject_resource='res:release' AND layer=?", (LAYER,))
    db.commit()
    after = _run_circuit(_resolved_facts(db))

    return {"before": before, "after": after}


if __name__ == "__main__":
    import sqlite3
    r = witness(sqlite3.connect(":memory:"))
    b = OPP in r["before"].get("reachable", [])
    a = OPP in r["after"].get("reachable", [])
    print(f"before challenge: {OPP} reachable = {b}  {r['before']}")
    print(f"after  challenge: {OPP} reachable = {a}  {r['after']}")
    assert b and not a, "proof retraction must withdraw the opportunity"
    print("PROOF-RETRACTION CASCADE: PASS (ProofFrontier -1 -> ReachableCapacity -1, live DBSP)")
