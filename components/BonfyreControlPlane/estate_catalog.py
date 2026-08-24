"""What does Bonfyre actually have, by domain estate.

The plural-fabric architecture says Bonfyre is not one calculus but many domain
estates over a shared foundation: model, artifact, media, execution, data/graph,
distributed/device, economy, surface, governance, institution, context. Recent
work kept drifting toward one abstraction swallowing them. The catalog is the
opposite discipline, and the one anti-amnesia mechanism the architecture blesses
as non-universal: it lets the system answer "what do I have in the model estate"
from real built binaries rather than a guess.

This reads the probe manifest written by probe_native_capabilities (real
binaries, real advertised surfaces) and assigns each a primary estate. A command
that legitimately spans estates keeps its identity -- the classification is a
routing hint, never a claim that BonfyreNet is only networking. Evidence is the
binary's own name and advertised verbs, so the mapping is checkable, not decreed.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

BIN_ROOT = Path.home() / ".bonfyre" / "bin"
MANIFEST = BIN_ROOT / ".capabilities.json"
FAMILIES = BIN_ROOT / ".families.json"

# Domain estates from the plural-fabric architecture. Order matters only for
# display; a binary belongs to exactly one primary estate, chosen by the first
# family key that matches. Families that genuinely span estates are noted, not
# forced.
ESTATES: list[tuple[str, tuple[str, ...]]] = [
    ("model", (
        "Infer", "Gen", "Quant", "FPQ", "FPQx", "QwenFPQ", "QwenFpq", "SLI",
        "FlashQLA", "KVCache", "SAE", "Embed", "Vec", "Reason", "Physics",
        "Leapfrog", "MoE", "MoQ", "Model",
    )),
    ("media_scene", (
        "VideoDemux", "FrameExtract", "SceneDetect", "DetectObjects",
        "Transcribe", "TranscriptClean", "TranscriptFamily", "Segment",
        "Clips", "Narrate", "Tone", "Render", "Repurpose", "Violence",
        "MediaPrep", "SpeechLoop",
    )),
    ("artifact", (
        "Hash", "Canon", "Fragment", "Stitch", "Pack", "Emit", "Ingest",
        "Layer", "Paragraph", "MFADict",
    )),
    ("execution", (
        "Flow", "Pipeline", "Run", "Queue", "Orchestrate", "Runtime", "Gate",
        "Swarm", "Compete",
    )),
    ("data_graph", (
        "Graph", "Index", "Query", "Entity", "Family", "Tag", "WeaviateIndex",
    )),
    ("distributed_device", (
        "Net", "Wire", "Sync", "Distribute", "Proxy", "Tel", "Space", "Tier",
        "API",
    )),
    ("economy_exchange", (
        "Meter", "Ledger", "Economy", "Finance", "Offer", "Pay",
    )),
    ("surface_human", (
        "CMS", "Surface", "Brief", "Outreach", "CLI",
    )),
    ("governance_proof", (
        "Proof", "Discipl", "Learn", "Auth", "Capability", "Control", "Project",
    )),
    ("context_continuity", (
        "Compress", "Time", "Watch",
    )),
    # Not command estates but distinct fabrics the architecture names on their
    # own: FS is an OS-interoperability materialization phenotype; the Frappe
    # compiler drives the nine-application estate.
    ("cross_domain_fabric", ("FS",)),
    ("application", ("FrappeCompiler",)),
]

SCHEMA = """
CREATE TABLE IF NOT EXISTS estate_catalog(
  family TEXT PRIMARY KEY,
  binary TEXT NOT NULL DEFAULT '',
  estate TEXT NOT NULL,
  shape TEXT NOT NULL DEFAULT '',
  subcommands TEXT NOT NULL DEFAULT '[]',
  synopsis TEXT NOT NULL DEFAULT '',
  classified_by TEXT NOT NULL DEFAULT 'name'
);
"""


@dataclass(frozen=True)
class Entry:
    family: str
    binary: str
    estate: str
    shape: str
    subcommands: tuple[str, ...]
    synopsis: str


def _estate_for(family: str) -> tuple[str, str]:
    # Families carry the Bonfyre prefix (BonfyreFPQ); the estate keys are the
    # bare capability names (FPQ).
    bare = family[len("Bonfyre"):] if family.startswith("Bonfyre") else family
    for estate, families in ESTATES:
        if bare in families:
            return estate, "name"
    return "unclassified", "none"


def ensure_schema(db: sqlite3.Connection) -> None:
    db.executescript(SCHEMA)
    db.commit()


def build(db: sqlite3.Connection) -> dict:
    """Classify every probed binary into its primary estate."""
    ensure_schema(db)
    try:
        manifest = json.loads(MANIFEST.read_text())
    except (OSError, ValueError):
        manifest = {}

    counts: dict[str, int] = {}
    for binary_name, entry in sorted(manifest.items()):
        family = entry.get("family") or ""
        if not family:
            continue
        estate, how = _estate_for(family)
        subs = entry.get("subcommands") or []
        db.execute(
            "INSERT INTO estate_catalog"
            "(family,binary,estate,shape,subcommands,synopsis,classified_by)"
            " VALUES(?,?,?,?,?,?,?)"
            " ON CONFLICT(family) DO UPDATE SET"
            "  binary=excluded.binary, estate=excluded.estate, shape=excluded.shape,"
            "  subcommands=excluded.subcommands, synopsis=excluded.synopsis,"
            "  classified_by=excluded.classified_by",
            (
                family,
                binary_name,
                estate,
                entry.get("shape", ""),
                json.dumps(subs),
                (entry.get("synopsis") or "")[:200],
                how,
            ),
        )
        counts[estate] = counts.get(estate, 0) + 1
    db.commit()
    return counts


def estate(db: sqlite3.Connection, name: str) -> list[Entry]:
    """Everything Bonfyre actually has in one estate."""
    return [
        Entry(
            family=r[0],
            binary=r[1],
            estate=r[2],
            shape=r[3],
            subcommands=tuple(json.loads(r[4] or "[]")),
            synopsis=r[5],
        )
        for r in db.execute(
            "SELECT family,binary,estate,shape,subcommands,synopsis"
            " FROM estate_catalog WHERE estate=? ORDER BY family",
            (name,),
        )
    ]


def summary(db: sqlite3.Connection) -> dict[str, int]:
    return {
        r[0]: r[1]
        for r in db.execute(
            "SELECT estate, COUNT(*) FROM estate_catalog GROUP BY estate ORDER BY 2 DESC"
        )
    }


def unclassified(db: sqlite3.Connection) -> list[str]:
    return [
        r[0]
        for r in db.execute(
            "SELECT family FROM estate_catalog WHERE estate='unclassified' ORDER BY family"
        )
    ]


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=str(Path(__file__).resolve().parent / "control_plane.db"))
    parser.add_argument("--estate", help="list one estate's contents")
    args = parser.parse_args()

    db = sqlite3.connect(args.db)
    if args.estate:
        ensure_schema(db)
        for e in estate(db, args.estate):
            verbs = ",".join(e.subcommands[:6]) or e.synopsis[:50]
            print(f"  {e.family:24} {e.shape:11} {verbs}")
    else:
        counts = build(db)
        print(json.dumps(counts, indent=2))
        unc = unclassified(db)
        if unc:
            print("unclassified:", unc)
    db.close()


if __name__ == "__main__":
    main()
