"""The second axis: what computational law each command preserves.

The estate catalog answers "what domain is this in". This answers the deeper
question the archive's second deep read poses: the 91 commands are not 90 more
features, they are public faces of a small number of computational calculi that
later work kept rediscovering under new names. FPQ encoded a representation law;
Net a coupled-dynamics law; Physics/Reason a branchable-state law; DisCIPL a
composition law; Hash/Canon/Graph/Family/Entity five distinct identity laws.

Collapsing these is the exact error the plural-fabric correction warns against.
The estate axis stops "Bonfyre does not know what it has". This axis stops the
subtler amnesia: rebuilding a calculus from the outside because its command-level
meaning was never named. Each command has a primary law; a command that carries
more than one keeps its identity, and the note records the spanning ones.

Built on estate_catalog's real probed binaries, so this is a classification over
what exists, not a wish list.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import estate_catalog as ec

# Primary computational law per capability family (bare name, no Bonfyre prefix).
# Grouped by the four calculi the archive identifies, plus the cross-cutting
# identity, representation, artifact, boundary and economic laws.
LAWS: dict[str, tuple[str, ...]] = {
    # Calculus A -- semantic composition: what may compose, at what loss.
    "composition": ("Discipl", "Capability", "Gate"),
    # Calculus B -- causal/durable execution, as a stack of distinct semantics.
    "causal_execution": (
        "Flow", "Pipeline", "Run", "Queue", "Orchestrate", "Runtime", "Swarm",
        "Compete", "Control",
    ),
    # Calculus C -- coupled state dynamics and branchable trajectories.
    "coupled_dynamics": ("Net", "Physics", "Reason", "Leapfrog"),
    # Calculus D -- evidence, selection, evolution, reinterpretation.
    "evidence_evolution": ("Learn", "Proof", "Discipl", "Time", "Compete", "Project"),
    # Identity is not one question: five commands answer five different ones.
    "identity": ("Hash", "Canon", "Graph", "Family", "Entity", "Auth"),
    # Representation: expensive state to compact executable structure (FPQ's law).
    "representation": (
        "FPQ", "FPQx", "QwenFPQ", "Quant", "SLI", "FlashQLA", "SAE", "KVCache",
        "Compress", "Model", "Infer", "Gen", "MoE",
    ),
    # Artifact derivation: source to fragments to materialized realization.
    "artifact_derivation": (
        "Ingest", "Stitch", "Layer", "Pack", "Emit", "Paragraph", "MFADict",
        "Render", "Repurpose", "Brief",
    ),
    # Retrieval / latent observability.
    "retrieval_observability": ("Embed", "Vec", "Index", "Query", "Tag", "WeaviateIndex"),
    # Boundary crossing: observe, bind, reconcile, route, transport -- distinct verbs.
    "boundary_crossing": (
        "Wire", "Watch", "Sync", "Distribute", "Proxy", "Tel", "MoQ", "API",
        "Space", "Tier",
    ),
    # Economic measurement, kept separate from any one "business module".
    "economic": ("Meter", "Ledger", "Economy", "Finance", "Pay", "Offer", "Outreach"),
    # Media/scene transformation as its own creative law.
    "media_transform": (
        "Transcribe", "TranscriptClean", "TranscriptFamily", "Segment", "Clips",
        "Narrate", "Tone", "VideoDemux", "FrameExtract", "SceneDetect",
        "MediaPrep", "SpeechLoop", "Violence",
    ),
    # Temporal / continuity.
    "temporal_continuity": ("Time",),
    # Surface / human projection.
    "surface_projection": ("Surface", "CMS", "CLI"),
}

# The four calculi the archive groups these under, for the higher view.
CALCULI = {
    "A_composition": ("composition", "identity"),
    "B_causal_execution": ("causal_execution", "artifact_derivation", "surface_projection"),
    "C_coupled_dynamics": ("coupled_dynamics", "representation", "retrieval_observability"),
    "D_evidence_evolution": ("evidence_evolution", "economic", "temporal_continuity"),
}

SCHEMA = """
CREATE TABLE IF NOT EXISTS command_laws(
  family TEXT PRIMARY KEY,
  law TEXT NOT NULL,
  calculus TEXT NOT NULL DEFAULT ''
);
"""


def _law_for(family: str) -> str:
    bare = family[len("Bonfyre"):] if family.startswith("Bonfyre") else family
    for law, members in LAWS.items():
        if bare in members:
            return law
    return "unclassified_law"


def _calculus_for(law: str) -> str:
    for calc, laws in CALCULI.items():
        if law in laws:
            return calc
    return ""


def build(db: sqlite3.Connection) -> dict:
    db.executescript(SCHEMA)
    ec.ensure_schema(db)
    ec.build(db)  # ensure the estate catalog is current
    counts: dict[str, int] = {}
    for (family,) in db.execute("SELECT family FROM estate_catalog"):
        law = _law_for(family)
        db.execute(
            "INSERT INTO command_laws(family,law,calculus) VALUES(?,?,?)"
            " ON CONFLICT(family) DO UPDATE SET law=excluded.law, calculus=excluded.calculus",
            (family, law, _calculus_for(law)),
        )
        counts[law] = counts.get(law, 0) + 1
    db.commit()
    return counts


def by_law(db: sqlite3.Connection, law: str) -> list[str]:
    return [r[0] for r in db.execute(
        "SELECT family FROM command_laws WHERE law=? ORDER BY family", (law,))]


def summary(db: sqlite3.Connection) -> dict[str, int]:
    return {r[0]: r[1] for r in db.execute(
        "SELECT law, COUNT(*) FROM command_laws GROUP BY law ORDER BY 2 DESC")}


def main() -> None:
    import argparse, json
    p = argparse.ArgumentParser()
    p.add_argument("--db", default=str(Path(__file__).resolve().parent / "control_plane.db"))
    p.add_argument("--law")
    a = p.parse_args()
    db = sqlite3.connect(a.db)
    if a.law:
        db.executescript(SCHEMA)
        print("  " + "  ".join(by_law(db, a.law)))
    else:
        print(json.dumps(build(db), indent=2))
    db.close()


if __name__ == "__main__":
    main()
