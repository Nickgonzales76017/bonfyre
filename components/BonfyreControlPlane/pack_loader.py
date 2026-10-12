"""Load a declarative pack into the control plane. No campaign-specific code.

The architectural test this exists to satisfy: if instantiating a campaign
requires new Python files, the kernel is missing a primitive. Tarbell was first
written as a bespoke `campaigns/tarbell.py` with hardcoded people, branches and
dates. That was the wrong shape -- a campaign is a scope and a body of data, not
a source tree.

So this loader knows about actors, edges, work and watches, and knows nothing
about Tarbell. A second campaign should add a directory under `packs/`, not a
module here.

Pack syntax is YaFF as used elsewhere in the estate: a header line, then blocks
introduced by `<kind> <id>` with indented `key value` lines.
"""

from __future__ import annotations

import datetime as dt
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import actors
import scheduling as sch
import work_graph as wg

UTC = dt.timezone.utc


@dataclass
class Block:
    kind: str
    ident: str
    args: list[str] = field(default_factory=list)
    fields: dict[str, str] = field(default_factory=dict)


@dataclass
class Pack:
    header_kind: str
    header_name: str
    meta: dict[str, str]
    blocks: list[Block]

    def of(self, kind: str) -> list[Block]:
        return [block for block in self.blocks if block.kind == kind]


# Block kinds that carry an identifier plus indented fields. Anything else on a
# non-indented line is pack-level metadata.
_BLOCK_KINDS = {"actor", "work", "watch", "edge"}


def parse_pack(text: str) -> Pack:
    lines = text.splitlines()
    header_kind = header_name = ""
    meta: dict[str, str] = {}
    blocks: list[Block] = []
    current: Optional[Block] = None

    for raw in lines:
        if not raw.strip() or raw.lstrip().startswith("#"):
            continue
        indented = raw[0].isspace()
        stripped = raw.strip()
        key, _, value = stripped.partition(" ")

        if indented:
            if current is not None:
                current.fields[key] = value.strip()
            continue

        current = None
        if not header_kind:
            header_kind, header_name = key, value.strip()
            continue
        if key in _BLOCK_KINDS:
            parts = value.split()
            block = Block(kind=key, ident=parts[0] if parts else "", args=parts[1:])
            blocks.append(block)
            current = block
        else:
            meta[key] = value.strip()

    return Pack(header_kind, header_name, meta, blocks)


def _parse_ts(text: str) -> Optional[dt.datetime]:
    if not text:
        return None
    try:
        parsed = dt.datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def load_actors(db: sqlite3.Connection, pack: Pack, now: dt.datetime) -> dict:
    actors.ensure_schema(db)
    provenance = pack.meta.get("provenance", "")
    if not provenance:
        raise ValueError(f"{pack.header_name}: packs declaring actors need provenance")
    authority = pack.meta.get("authority", actors.ASSERTED)

    loaded = 0
    for block in pack.of("actor"):
        actors.upsert_actor(
            db,
            actors.Actor(
                actor_id=block.ident,
                node_kind=block.fields.get("node_kind", actors.ORGANIZATION),
                display_name=block.fields.get("display_name", block.ident),
                role=block.fields.get("role", ""),
                org_id=block.fields.get("org_id"),
                confidence=authority,
                provenance=provenance,
                detail=block.fields.get("detail", ""),
            ),
            now=now,
        )
        loaded += 1

    edges = 0
    for block in pack.of("edge"):
        if len(block.args) < 2:
            raise ValueError(f"edge {block.ident}: expected '<from> <kind> <to>'")
        actors.add_edge(
            db,
            block.ident,
            block.args[0],
            block.args[1],
            provenance=provenance,
            confidence=authority,
            detail=block.fields.get("detail", ""),
            now=now,
        )
        edges += 1

    return {"actors": loaded, "edges": edges}


def load_work(db: sqlite3.Connection, pack: Pack, now: dt.datetime) -> dict:
    wg.ensure_schema(db)
    source_plane = pack.meta.get("source_plane")
    item_kind = pack.meta.get("item_kind", pack.header_name)
    source_ref = pack.meta.get("source_ref", f"pack:{pack.header_name}")
    if not source_plane:
        raise ValueError(f"{pack.header_name}: work packs need a source_plane")

    queued = skipped = 0
    for block in pack.of("work"):
        item = wg.enqueue(
            db,
            source_plane=source_plane,
            target_plane=block.fields["target_plane"],
            item_kind=item_kind,
            subject_ref=block.ident,
            reason=block.fields.get("reason", ""),
            source_ref=source_ref,
            priority=float(block.fields.get("priority", 0)),
            now=now,
        )
        if item is None:
            skipped += 1
        else:
            queued += 1
    return {"queued": queued, "already_present": skipped}


def load_watches(db: sqlite3.Connection, pack: Pack, now: dt.datetime) -> dict:
    sch.ensure_schema(db)
    loaded = 0
    for block in pack.of("watch"):
        sch.schedule(
            db,
            block.ident,
            block.fields.get("subject", block.ident),
            temperature=block.fields.get("temperature", sch.WARM),
            reheat_at=_parse_ts(block.fields.get("reheat_at", "")),
            reheat_on=block.fields.get("reheat_on", ""),
            detail=block.fields.get("detail", ""),
            now=now,
        )
        loaded += 1
    return {"watches": loaded}


_LOADERS = {
    "actor": load_actors,
    "work": load_work,
    "watch": load_watches,
}


def load_directory(
    db: sqlite3.Connection, directory: Path, now: Optional[dt.datetime] = None
) -> dict:
    """Load every .yaff in a pack directory, dispatching on what it declares."""
    moment = now or dt.datetime.now(UTC)
    report: dict = {"pack": directory.name}
    for path in sorted(directory.glob("*.yaff")):
        pack = parse_pack(path.read_text())
        for kind, loader in _LOADERS.items():
            if pack.of(kind):
                for key, value in loader(db, pack, moment).items():
                    report[key] = report.get(key, 0) + value
                break
    return report
