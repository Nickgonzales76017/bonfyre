"""Derive real corroborations from the graph -- the gathering half of verification.

Fortification says "verify these load-bearing actors". Verification needs
INDEPENDENT corroboration. Some of that already exists in the graph: an asserted
actor that multiple VERIFIED actors have a typed edge to is independently
attested -- each verified neighbor is a distinct source vouching that the actor
exists and stands in that relationship.

This records those corroborations into the verification ledger with real
provenance (the edge). It does the gathering work the machine can do. It does
NOT promote asserted -> verified: `verified` means a human confirmed it (the
constitution's human line), and a graph heuristic is not a human. Instead it
surfaces actors whose corroboration has reached the threshold -- ready for a
human to confirm, which is where the leverage is retired.

Only verified neighbors count as sources. An asserted neighbor cannot corroborate
an asserted actor -- two unconfirmed claims are not independent attestation.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass

import verification as verif


@dataclass
class GraphCorroborationResult:
    recorded: int
    actors_touched: int
    ready_for_human: list[str]  # asserted actors now at/above the corroboration threshold


def _verified(db: sqlite3.Connection) -> dict[str, str]:
    return {r[0]: r[1] for r in db.execute("SELECT actor_id, confidence FROM actor_nodes")}


def verified_neighbors(db: sqlite3.Connection) -> dict[str, list[tuple[str, str]]]:
    """asserted actor -> [(verified_neighbor_id, edge_kind)] distinct by neighbor."""
    conf = _verified(db)
    out: dict[str, dict[str, str]] = {}
    for frm, kind, to in db.execute("SELECT from_id, edge_kind, to_id FROM actor_edges"):
        if conf.get(frm) == "verified" and conf.get(to) == "asserted":
            out.setdefault(to, {}).setdefault(frm, kind)
        if conf.get(to) == "verified" and conf.get(frm) == "asserted":
            out.setdefault(frm, {}).setdefault(to, kind)
    return {a: sorted(neigh.items()) for a, neigh in out.items()}


def corroborate(db: sqlite3.Connection, threshold: int = verif.DEFAULT_THRESHOLD) -> GraphCorroborationResult:
    """Record graph-derived corroborations. Never promotes; surfaces the ready set."""
    verif.ensure_schema(db)
    neighbors = verified_neighbors(db)
    recorded = 0
    ready: list[str] = []
    for actor_id, sources in neighbors.items():
        for neighbor_id, kind in sources:
            if verif.record_corroboration(
                db, actor_id, source=neighbor_id,
                evidence_ref=f"edge:{kind}",
                note="verified neighbor attests this actor via a typed relationship",
            ):
                recorded += 1
        state = verif.verification_state(db, actor_id, threshold)
        if state.confidence != "verified" and state.independent_sources >= threshold:
            ready.append(actor_id)
    return GraphCorroborationResult(
        recorded=recorded, actors_touched=len(neighbors), ready_for_human=sorted(ready),
    )


if __name__ == "__main__":
    import sys
    path = sys.argv[1] if len(sys.argv) > 1 else "control_plane.db"
    con = sqlite3.connect(path)
    r = corroborate(con)
    con.close()
    print(f"recorded {r.recorded} graph corroborations across {r.actors_touched} actors")
    print(f"ready for human verification ({len(r.ready_for_human)}): {', '.join(r.ready_for_human)}")
