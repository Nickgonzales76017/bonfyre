"""Fill DisCIPL with the real command estate: families, kinds, bindings.

Until now DisCIPL composed only model-internal families (T_MOE_ROUTER). The
factor move is to seed it with the full command estate so it composes real
command pipelines -- ingest to transcribe to segment to repurpose -- and the
closure binds each hop to its actual binary.

Three things filled, per the instruction:

  families   each command becomes a DisCIPL family (T_INGEST ...), an actor
             carrying its probed subcommands as capabilities and its real
             binary as source_ref.
  kinds      each command's consumed and produced artifact kinds, from its real
             synopsis. Contracts are generated wherever one command's output
             kind is another's input kind -- so the composition edges are
             derived from actual data flow, not decreed.
  bindings   family -> exact binary, written into the actor's source_ref, so a
             composed hop binds the command that owns it rather than the first
             one in its estate.

Writes directly into the DisCIPL DB, then the native `bonfyre-discipl propose`
composes over it. This does not reimplement composition; it gives the native
engine a real graph to compose.
"""

from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path

BIN = Path.home() / ".bonfyre" / "bin"
DISCIPL_DB = Path("/tmp/discipl-root/discipl.db")

# (consumed kinds, produced kinds) per command, from real synopses.
# A command with no pipeline role is omitted -- this is the composable estate.
COMMAND_KINDS: dict[str, tuple[tuple[str, ...], tuple[str, ...]]] = {
    "Ingest":          (("file", "url", "audio", "text", "image"), ("artifact",)),
    "Canon":           (("artifact", "code"), ("canonical", "structural_id")),
    "Hash":            (("artifact",), ("content_id",)),
    "Graph":           (("artifact",), ("lineage",)),
    "Entity":          (("artifact", "observation"), ("identity",)),
    "Transcribe":      (("audio",), ("transcript",)),
    "TranscriptClean": (("transcript",), ("clean_transcript",)),
    "Segment":         (("transcript", "clean_transcript"), ("segments",)),
    "Tone":            (("audio",), ("tone_profile",)),
    "Brief":           (("transcript", "clean_transcript", "segments"), ("brief",)),
    "Narrate":         (("brief", "text"), ("audio",)),
    "Repurpose":       (("brief",), ("social",)),
    "Render":          (("brief", "artifact"), ("rendered",)),
    "Emit":            (("artifact", "brief", "rendered"), ("deliverable",)),
    "Embed":           (("text", "clean_transcript"), ("embedding",)),
    "Vec":             (("embedding",), ("vector_index",)),
    "Index":           (("artifact",), ("search_index",)),
    "Query":           (("search_index", "vector_index"), ("results",)),
    "Tag":             (("text", "artifact"), ("tags",)),
    "Stitch":          (("artifact",), ("composite",)),
    "Pack":            (("artifact", "composite"), ("pack",)),
    "FPQ":             (("model", "tensor"), ("compressed",)),
    "SLI":             (("compressed",), ("execution",)),
    "KVCache":         (("execution",), ("kv_state",)),
    "Meter":           (("artifact", "execution", "deliverable"), ("meter_event",)),
    "Ledger":          (("meter_event",), ("accounting",)),
    "Proof":           (("artifact", "brief", "deliverable"), ("proof",)),
}

# command -> real binary basename, resolved from the bin dir.
_BINARY = {
    "Ingest": "bonfyre-ingest", "Canon": "bonfyre-canon", "Hash": "bonfyre-hash",
    "Graph": "bonfyre-graph", "Entity": "bonfyre-entity", "Transcribe": "bonfyre-transcribe",
    "TranscriptClean": "bonfyre-transcript-clean", "Segment": "bonfyre-segment",
    "Tone": "bonfyre-tone", "Brief": "bonfyre-brief", "Narrate": "bonfyre-narrate",
    "Repurpose": "bonfyre-repurpose", "Render": "bonfyre-render", "Emit": "bonfyre-emit",
    "Embed": "bonfyre-embed", "Vec": "bonfyre-vec", "Index": "bonfyre-index",
    "Query": "bonfyre-query", "Tag": "bonfyre-tag", "Stitch": "bonfyre-stitch",
    "Pack": "bonfyre-pack", "FPQ": "bonfyre-fpq", "SLI": "bonfyre-sli",
    "KVCache": "bonfyre-kvcache", "Meter": "bonfyre-meter", "Ledger": "bonfyre-ledger",
    "Proof": "bonfyre-proof",
}


def _family(command: str) -> str:
    return "T_" + command.upper()


def seed(db_path: Path = DISCIPL_DB, capabilities_db: Path | None = None) -> dict:
    """Fill actors (families+kinds+bindings) and contracts (data-flow edges)."""
    if not db_path.exists():
        raise RuntimeError("DisCIPL DB not initialized; run bonfyre-discipl init")

    caps: dict[str, list[str]] = {}
    manifest = BIN / ".capabilities.json"
    if manifest.exists():
        for entry in json.loads(manifest.read_text()).values():
            fam = entry.get("family", "")
            if fam.startswith("Bonfyre"):
                caps[fam[len("Bonfyre"):]] = entry.get("subcommands") or []

    con = sqlite3.connect(str(db_path))
    now = str(int(time.time()))
    actors = contracts = 0

    for command, (consumes, produces) in COMMAND_KINDS.items():
        binary = BIN / _BINARY[command]
        con.execute(
            "INSERT OR REPLACE INTO discipl_actors"
            "(actor_id,actor_type,family,domain,modality,capabilities_json,"
            " role_affordances_json,confidence,uncertainty,cost,latency,source_ref,created_at)"
            " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                f"actor:{command}", "command", _family(command), "command", "artifact",
                json.dumps(caps.get(command, [])),
                json.dumps({"can_execute": True, "can_verify": command in ("Proof", "Hash", "Canon")}),
                0.8, 0.1, 0.1, 0.1, str(binary), now,
            ),
        )
        actors += 1

    # Contracts: an edge wherever a produced kind is a consumed kind elsewhere.
    for src, (_, produces) in COMMAND_KINDS.items():
        for dst, (consumes, _) in COMMAND_KINDS.items():
            if src == dst:
                continue
            shared = set(produces) & set(consumes)
            if not shared:
                continue
            con.execute(
                "INSERT OR REPLACE INTO discipl_contracts"
                "(contract_id,src_family,dst_family,relationship,directionality,"
                " required_bridge,preconditions_json,postconditions_json,failure_modes_json,"
                " success_prior,transform_cost,semantic_loss,created_at)"
                " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    f"contract:{src}->{dst}", _family(src), _family(dst),
                    "feeds", "directed", "",
                    json.dumps(sorted(shared)), json.dumps(sorted(shared)), json.dumps([]),
                    0.7, 0.08, 0.03, now,
                ),
            )
            contracts += 1

    con.commit()
    con.close()
    return {"actors": actors, "contracts": contracts,
            "families": len(COMMAND_KINDS), "binaries": len(_BINARY)}


if __name__ == "__main__":
    print(json.dumps(seed(), indent=2))
