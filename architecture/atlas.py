#!/usr/bin/env python3
"""The Architecture Atlas engine: a lossless registry whose simplified views are
generated, never canonical.

The rule this enforces mechanically, so it cannot rot into prose:

  * Collapse is a *view* operation only. A name like ``Provider`` or ``Model`` is
    legal only as a :class:`View` that reversibly expands to its children. The
    view never replaces its children in the registry.
  * Every architecture states its own node/edge ontology, invariants, and -- the
    highest-value field -- what it may NOT be inferred to imply
    (``forbidden_inference``). ``low reconstruction error`` does not become
    ``good generation``; ``available`` does not become ``authorized``.
  * Maturity cannot be laundered by a parent label. An architecture claiming
    ``measured`` or ``proven`` must carry real witnesses, or validation fails.

Everything downstream -- diagrams, README sections, BonfyreFS introspection,
repo-cleanup deletion criteria, CI, MCP queries -- consumes this registry
instead of re-summarising the system from memory. This module is deliberately
dependency-free (standard library only) so CI and the MCP node can run it
without the control-plane environment.

    python3 atlas.py validate         # parse + enforce the constitution (CI gate)
    python3 atlas.py loss             # what the atlas knows it is missing
    python3 atlas.py maturity         # honest maturity rollup, no laundering
    python3 atlas.py expand <view>    # reversible view expansion (zoom, not delete)
    python3 atlas.py view <view>      # render a collapsed, expandable diagram
    python3 atlas.py export           # atlas.index.json for MCP / BonfyreFS
"""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parent

# The maturity ladder, low to high. It mirrors the proof-frontier truth planes:
# a claim is architectural (named only), declared (a contract exists),
# implemented (code runs), measured (a witness measured it), or proven (a proof
# artifact backs it). measured and proven require witnesses -- that guard is the
# whole point.
MATURITY_LADDER = ("architectural", "declared", "implemented", "measured", "proven")
_MATURITY_RANK = {name: i for i, name in enumerate(MATURITY_LADDER)}
WITNESS_REQUIRED_AT = _MATURITY_RANK["measured"]

# Fields inside a block that accumulate into a list when repeated. Everything
# else is a scalar (last value wins).
_LIST_FIELDS = {
    "node_types", "edge_types", "state_types", "internal_calculus", "child",
    "invariant", "forbidden_inference", "interacts", "computational_form",
    "native_format", "command", "app", "service", "external_binding", "surface",
    "source_path", "witness", "known_gap", "cooling_path", "reheat_condition",
    "source_lineage", "historical_name", "supersedes", "superseded_by",
    # interaction-block lists
    "preserves", "drops", "authority", "evidence", "invalidates_on", "binding",
    # view-block lists
    "expands_to",
    # phenotype-block lists
    "consumes", "produces", "placement_constraint",
    # meta-edge-class lists
    "specialized_by",
    # WiringSpec: typed semantic-fact flow between organs (the wiring constitution)
    "owns", "publishes", "subscribes",
}


@dataclass
class Block:
    kind: str            # architecture | view | interaction
    ident: str
    scalars: dict[str, str] = field(default_factory=dict)
    lists: dict[str, list[str]] = field(default_factory=dict)
    source_file: str = ""

    def get(self, key: str, default: str = "") -> str:
        return self.scalars.get(key, default)

    def lst(self, key: str) -> list[str]:
        return self.lists.get(key, [])


def parse_file(path: Path) -> list[Block]:
    """Parse one atlas YaFF file into its blocks.

    Grammar (same shape as the pack loader): an ``atlas <name>`` header, then
    top-level ``<block-kind> <ident>`` lines each followed by indented
    ``key value`` fields. Repeated list-field keys accumulate.
    """
    blocks: list[Block] = []
    current: Block | None = None
    header_seen = False
    for raw in path.read_text().splitlines():
        if not raw.strip() or raw.lstrip().startswith("#"):
            continue
        stripped = raw.strip()
        key, _, value = stripped.partition(" ")
        value = value.strip()
        if raw[0].isspace():
            if current is None:
                continue
            if key in _LIST_FIELDS:
                current.lists.setdefault(key, []).append(value)
            else:
                current.scalars[key] = value
            continue
        # top-level line
        if not header_seen and key == "atlas":
            header_seen = True
            continue
        if key in ("architecture", "view", "interaction", "phenotype", "edge_class"):
            current = Block(kind=key, ident=value, source_file=path.name)
            blocks.append(current)
        else:
            current = None  # unknown top-level line: ignore, stay strict elsewhere
    return blocks


@dataclass
class Atlas:
    architectures: dict[str, Block] = field(default_factory=dict)
    views: dict[str, Block] = field(default_factory=dict)
    interactions: dict[str, Block] = field(default_factory=dict)
    phenotypes: dict[str, Block] = field(default_factory=dict)
    edge_classes: dict[str, Block] = field(default_factory=dict)

    @classmethod
    def load(cls, root: Path = ROOT) -> "Atlas":
        atlas = cls()
        for path in sorted(root.rglob("*.yaff")):
            for block in parse_file(path):
                if block.kind == "architecture":
                    atlas.architectures[block.ident] = block
                elif block.kind == "view":
                    atlas.views[block.ident] = block
                elif block.kind == "interaction":
                    atlas.interactions[block.ident] = block
                elif block.kind == "phenotype":
                    atlas.phenotypes[block.ident] = block
                elif block.kind == "edge_class":
                    atlas.edge_classes[block.ident] = block
        return atlas

    # -- reversible views ---------------------------------------------------

    def expand(self, view_id: str) -> list[str]:
        v = self.views.get(view_id)
        return v.lst("expands_to") if v else []

    def views_of(self, architecture_id: str) -> list[str]:
        """Reverse index: which views a given architecture rolls up into. This is
        what makes collapse reversible -- a child always knows its parents."""
        return sorted(
            vid for vid, v in self.views.items()
            if architecture_id in v.lst("expands_to")
        )

    # -- validation: the constitution as code -------------------------------

    def validate(self) -> list[str]:
        errors: list[str] = []
        seen: set[str] = set()
        for aid, a in self.architectures.items():
            if aid in seen:
                errors.append(f"architecture {aid}: duplicate id")
            seen.add(aid)
            for req in ("canonical_name", "family", "domain", "semantic_question", "maturity"):
                if not a.get(req):
                    errors.append(f"architecture {aid}: missing required field '{req}'")
            maturity = a.get("maturity")
            if maturity and maturity not in _MATURITY_RANK:
                errors.append(f"architecture {aid}: unknown maturity '{maturity}'")
            elif maturity and _MATURITY_RANK[maturity] >= WITNESS_REQUIRED_AT and not a.lst("witness"):
                # the anti-laundering rule: no measured/proven claim without a witness.
                errors.append(
                    f"architecture {aid}: maturity '{maturity}' claims a measurement "
                    f"but carries no witness -- maturity laundering"
                )

        # a view must expand only to real architectures, and to at least two of
        # them (a one-child view is a rename hiding as a collapse).
        for vid, v in self.views.items():
            children = v.lst("expands_to")
            if len(children) < 2:
                errors.append(f"view {vid}: must expand to >=2 architectures (is a rename, not a view)")
            for child in children:
                if child not in self.architectures:
                    errors.append(f"view {vid}: expands to unknown architecture '{child}'")

        # an interaction must connect two real architectures.
        for iid, it in self.interactions.items():
            for end in ("source", "destination"):
                ref = it.get(end)
                if not ref:
                    errors.append(f"interaction {iid}: missing '{end}'")
                elif ref not in self.architectures:
                    errors.append(f"interaction {iid}: {end} '{ref}' is not a registered architecture")

        # a phenotype must describe a real architecture and name what it produces.
        for pid, ph in self.phenotypes.items():
            ref = ph.get("architecture")
            if not ref:
                errors.append(f"phenotype {pid}: missing 'architecture'")
            elif ref not in self.architectures:
                errors.append(f"phenotype {pid}: architecture '{ref}' is not registered")
            if not ph.lst("produces"):
                errors.append(f"phenotype {pid}: must name what it produces")

        # WiringSpec: a semantic fact has at most one authoritative owner. Two
        # organs claiming to own one fact is duplicate ownership -- two sources of
        # truth for the same thing, the exact wiring bug the constitution forbids.
        fact_owners: dict[str, list[str]] = {}
        for aid, a in self.architectures.items():
            for fact in a.lst("owns"):
                fact_owners.setdefault(fact, []).append(aid)
        for fact, owners in sorted(fact_owners.items()):
            if len(owners) > 1:
                errors.append(
                    f"WiringSpec: fact '{fact}' is owned by {len(owners)} "
                    f"architectures ({', '.join(sorted(owners))}) -- duplicate ownership"
                )
        return errors

    # -- loss queries: what the atlas knows it is missing --------------------

    def loss(self) -> dict[str, list[str]]:
        endpoints: set[str] = set()
        for it in self.interactions.values():
            endpoints.update({it.get("source"), it.get("destination")})
        phenotyped = {ph.get("architecture") for ph in self.phenotypes.values()}
        report: dict[str, list[str]] = {
            "no_cannot_infer": [],
            "no_interaction_contract": [],
            "no_lineage": [],
            "no_native_format": [],
            "unwitnessed": [],
            "superseded_but_present": [],
            "substrate_without_phenotype": [],
            "fact_orphan_produced": [],
            "fact_orphan_consumed": [],
        }
        for aid, a in self.architectures.items():
            if not a.lst("forbidden_inference"):
                report["no_cannot_infer"].append(aid)
            if aid not in endpoints:
                report["no_interaction_contract"].append(aid)
            if not a.lst("source_lineage"):
                report["no_lineage"].append(aid)
            if not a.lst("native_format"):
                report["no_native_format"].append(aid)
            if _MATURITY_RANK.get(a.get("maturity"), 0) >= WITNESS_REQUIRED_AT and not a.lst("witness"):
                report["unwitnessed"].append(aid)
            if a.get("superseded_by"):
                report["superseded_but_present"].append(aid)
            if a.get("family") == "substrate" and aid not in phenotyped:
                report["substrate_without_phenotype"].append(aid)

        # WiringSpec gaps: a produced fact nobody consumes, or a consumed fact
        # nobody provides. These are the orphan producer/consumer edges the wiring
        # constitution wants closed.
        producers: set[str] = set()
        consumers: set[str] = set()
        for a in self.architectures.values():
            producers.update(a.lst("owns"))
            producers.update(a.lst("publishes"))
            consumers.update(a.lst("consumes"))
            consumers.update(a.lst("subscribes"))
        report["fact_orphan_produced"] = sorted(producers - consumers)
        report["fact_orphan_consumed"] = sorted(consumers - producers)
        return report

    def maturity_rollup(self) -> dict[str, dict[str, int]]:
        rollup: dict[str, dict[str, int]] = {}
        for a in self.architectures.values():
            fam = a.get("family", "?")
            m = a.get("maturity", "?")
            rollup.setdefault(fam, {}).setdefault(m, 0)
            rollup[fam][m] += 1
        return rollup

    def export(self) -> dict:
        return {
            "architectures": {
                aid: {
                    "canonical_name": a.get("canonical_name"),
                    "family": a.get("family"),
                    "domain": a.get("domain"),
                    "layer": a.get("layer"),
                    "semantic_question": a.get("semantic_question"),
                    "maturity": a.get("maturity"),
                    "node_types": a.lst("node_types"),
                    "edge_types": a.lst("edge_types"),
                    "internal_calculi": a.lst("internal_calculus"),
                    "forbidden_inferences": a.lst("forbidden_inference"),
                    "invariants": a.lst("invariant"),
                    "source_paths": a.lst("source_path"),
                    "witnesses": a.lst("witness"),
                    "commands": a.lst("command"),
                    "native_formats": a.lst("native_format"),
                    "known_gaps": a.lst("known_gap"),
                    "owns": a.lst("owns"),
                    "consumes": a.lst("consumes"),
                    "publishes": a.lst("publishes"),
                    "subscribes": a.lst("subscribes"),
                    "views": self.views_of(aid),
                }
                for aid, a in sorted(self.architectures.items())
            },
            "views": {
                vid: {"expands_to": v.lst("expands_to"), "collapsed_count": len(v.lst("expands_to"))}
                for vid, v in sorted(self.views.items())
            },
            "interactions": {
                iid: {
                    "source": it.get("source"), "destination": it.get("destination"),
                    "relation": it.get("relation"),
                    "preserves": it.lst("preserves"),
                    "drops": it.lst("drops"),
                    "forbidden_inferences": it.lst("forbidden_inference"),
                    "invalidates_on": it.lst("invalidates_on"),
                }
                for iid, it in sorted(self.interactions.items())
            },
            "phenotypes": {
                pid: {
                    "architecture": ph.get("architecture"),
                    "consumes": ph.lst("consumes"),
                    "produces": ph.lst("produces"),
                    "deterministic": ph.get("deterministic"),
                    "incremental": ph.get("incremental"),
                    "distributed": ph.get("distributed"),
                    "supports_retraction": ph.get("supports_retraction"),
                    "proof_class": ph.get("proof_class"),
                    "effect_class": ph.get("effect_class"),
                }
                for pid, ph in sorted(self.phenotypes.items())
            },
            "edge_classes": {
                eid: {"meaning": ec.get("meaning"), "specialized_by": ec.lst("specialized_by")}
                for eid, ec in sorted(self.edge_classes.items())
            },
        }


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #


def _cmd_validate(atlas: Atlas) -> int:
    errors = atlas.validate()
    if errors:
        print(f"✗ atlas invalid: {len(errors)} error(s)")
        for e in errors:
            print(f"    {e}")
        return 1
    print(f"✓ atlas valid: {len(atlas.architectures)} architectures, "
          f"{len(atlas.views)} views, {len(atlas.interactions)} interactions, "
          f"{len(atlas.phenotypes)} phenotypes, {len(atlas.edge_classes)} edge-classes")
    return 0


def _cmd_loss(atlas: Atlas) -> int:
    loss = atlas.loss()
    for key, items in loss.items():
        if items:
            print(f"{key} ({len(items)}): {', '.join(sorted(items))}")
    if not any(loss.values()):
        print("no architecture loss detected")
    return 0


def _cmd_maturity(atlas: Atlas) -> int:
    for fam, counts in sorted(atlas.maturity_rollup().items()):
        parts = ", ".join(f"{m}:{n}" for m, n in sorted(counts.items(), key=lambda kv: -_MATURITY_RANK.get(kv[0], 0)))
        print(f"{fam:16} {parts}")
    return 0


def _cmd_expand(atlas: Atlas, view_id: str) -> int:
    children = atlas.expand(view_id)
    if not children:
        print(f"no such view (or empty): {view_id}")
        return 1
    print(f"{view_id} expands to {len(children)}:")
    for c in children:
        a = atlas.architectures.get(c)
        name = a.get("canonical_name") if a else "(UNKNOWN)"
        mat = a.get("maturity") if a else "?"
        print(f"    {c:32} {name:34} [{mat}]")
    return 0


def _cmd_view(atlas: Atlas, view_id: str) -> int:
    children = atlas.expand(view_id)
    if not children:
        print(f"no such view: {view_id}")
        return 1
    print("┌" + "─" * 52 + "┐")
    print(f"│ {view_id:<50} │")
    print(f"│ collapsed: {len(children):>2} architectures  ([expand] to zoom){' ':<8} │")
    print("└" + "─" * 52 + "┘")
    for c in children:
        a = atlas.architectures.get(c)
        print(f"    └─ {a.get('canonical_name') if a else c}")
    return 0


def _cmd_export(atlas: Atlas) -> int:
    out = ROOT / "atlas.index.json"
    out.write_text(json.dumps(atlas.export(), indent=2, sort_keys=True))
    print(f"✓ wrote {out.relative_to(ROOT.parent)} "
          f"({len(atlas.architectures)} architectures)")
    return 0


def build_fs(atlas: Atlas, out: Path) -> int:
    """Write the BonfyreFS introspection tree: the architecture, inspectable.

    Mirrors the /Bonfyre/Actual/Graphs/ namespace -- one directory per
    architecture with plain-text views a human (or `cat`) can read, plus
    Queries/ directories computed from loss() so architecture gaps are walkable,
    not buried. The FUSE mount serves the same generator; this makes it real on
    disk today.
    """
    import shutil
    graphs = out / "Graphs"
    if graphs.exists():
        shutil.rmtree(graphs)
    written = 0
    for aid, a in atlas.architectures.items():
        fam = a.get("family", "unfiled")
        d = graphs / fam / (a.get("canonical_name") or aid)
        d.mkdir(parents=True, exist_ok=True)
        (d / "spec.md").write_text(
            f"# {a.get('canonical_name')}  [{a.get('maturity')}]\n\n"
            f"- id: {aid}\n- domain: {a.get('domain')}\n- layer: {a.get('layer')}\n\n"
            f"**Question:** {a.get('semantic_question')}\n\n{a.get('purpose', '')}\n"
        )
        (d / "cannot-infer.txt").write_text("\n".join(a.lst("forbidden_inference")) + "\n")
        (d / "witnesses.txt").write_text("\n".join(a.lst("witness")) + "\n")
        (d / "sources.txt").write_text("\n".join(a.lst("source_path")) + "\n")
        (d / "views.txt").write_text("\n".join(atlas.views_of(aid)) + "\n")
        written += 1

    # Query directories: the atlas's knowledge of its own gaps, as folders.
    q = out / "Queries" / "Architecture"
    q.mkdir(parents=True, exist_ok=True)
    loss = atlas.loss()
    buckets = {
        "Unwitnessed": loss["unwitnessed"],
        "No-Cannot-Infer": loss["no_cannot_infer"],
        "No-Interaction-Contract": loss["no_interaction_contract"],
        "No-Native-Format": loss["no_native_format"],
        "No-Lineage": loss["no_lineage"],
        "Unbuilt": sorted(a for a, blk in atlas.architectures.items() if blk.get("maturity") == "architectural"),
        "Superseded-But-Present": loss["superseded_but_present"],
    }
    for name, items in buckets.items():
        (q / f"{name}.txt").write_text("\n".join(sorted(items)) + "\n")
    return written


def _cmd_fs(atlas: Atlas, out_dir: str) -> int:
    out = Path(out_dir)
    written = build_fs(atlas, out)
    print(f"✓ wrote introspection tree to {out}/Graphs ({written} architectures) "
          f"and {out}/Queries/Architecture")
    return 0


def main(argv: list[str]) -> int:
    cmd = argv[1] if len(argv) > 1 else "validate"
    atlas = Atlas.load()
    if cmd == "validate":
        return _cmd_validate(atlas)
    if cmd == "loss":
        return _cmd_loss(atlas)
    if cmd == "maturity":
        return _cmd_maturity(atlas)
    if cmd == "expand":
        return _cmd_expand(atlas, argv[2]) if len(argv) > 2 else 1
    if cmd == "view":
        return _cmd_view(atlas, argv[2]) if len(argv) > 2 else 1
    if cmd == "export":
        return _cmd_export(atlas)
    if cmd == "fs":
        return _cmd_fs(atlas, argv[2] if len(argv) > 2 else str(ROOT / "fs"))
    if cmd == "wiring":
        return _cmd_wiring()
    print(f"unknown command: {cmd}")
    return 1


def _cmd_wiring() -> int:
    """Graph analysis over the atlas's own interactions: feedback loops vs one-way
    flows, and the highest-value loops still to close."""
    import wiring
    r = wiring.report()
    print(f"wiring: {r['wired_nodes']} wired architectures, {r['interactions']} interactions")
    print(f"feedback loops closed: {len(r['feedback_loops'])} "
          f"| one-way flows: {len(r['one_way_edges'])}")
    for loop in r["feedback_loops"]:
        print(f"  loop: {' -> '.join(loop)} ->")
    print(f"pure sources (emit, nothing returns): {', '.join(r['pure_sources']) or 'none'}")
    print(f"pure sinks (dead ends): {', '.join(r['pure_sinks']) or 'none'}")
    print("highest-value loops to close (by destination influence):")
    for c in r["top_loop_closures"]:
        print(f"  {c['one_way']}  [infl {c['destination_influence']}] -- {c['suggestion']}")

    fr = wiring.fact_report()
    print()
    print(f"-- WiringSpec fact flow: {fr['wired_organs']} organs, {len(fr['facts'])} facts --")
    print("orphan consumers (organ expects a fact NOBODY provides -- wire these):")
    for f, cons in fr["orphan_consumers"]:
        print(f"  {f} <- expected by {', '.join(cons)}")
    print("orphan producers (published, nobody consumes -- terminal surface or unwired):")
    for f, prod in fr["orphan_producers"]:
        print(f"  {f} -> from {', '.join(prod)}")
    if fr["duplicate_ownership"]:
        print("DUPLICATE OWNERSHIP (two organs claim one fact):")
        for f, owners in fr["duplicate_ownership"]:
            print(f"  {f}: {', '.join(owners)}")
    print("fact feedback loops (closed producer/consumer cycles):")
    for loop in fr["fact_feedback_loops"]:
        print(f"  {' <-> '.join(loop)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
