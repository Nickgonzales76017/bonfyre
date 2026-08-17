"""Wiring self-analysis -- the atlas run as a graph over its own interactions.

atlas.py validate checks each architecture; atlas.py loss lists the ones with no
interaction contract at all. Neither looks at the *structure* of the interactions
that do exist. This does, because the next compression is wiring closure and the
sharpest question is the one the system riff calls the biggest target:

    which flows are one-way (A -> B), and which are closed feedback loops
    (A -> B -> ... -> A)?  A mature organ is not one that emits; it is one whose
    effect returns as evidence and changes what it does next.

Over the interaction graph (source -> destination) this computes:

  * feedback_loops     -- strongly connected components of size > 1 (and self
                          loops): the parts of the system that already close.
  * one_way_edges      -- an interaction A -> B with no path B -> ... -> A: a
                          flow that has not been closed into a loop yet.
  * pure_sources       -- emit into the graph but nothing flows back to them.
  * pure_sinks         -- consume but feed nothing downstream (dead ends).
  * loop_closures      -- one-way edges ranked by the downstream influence of the
                          destination: closing B -> ... -> A on a high-influence B
                          is the highest-value wiring to add next.

It names concrete wiring to build; it does not invent architectures.
"""

from __future__ import annotations

import json
import os
from collections import defaultdict
from dataclasses import dataclass, field

HERE = os.path.dirname(os.path.abspath(__file__))


@dataclass
class Wiring:
    nodes: set[str]
    edges: list[tuple[str, str]]  # (source, destination)
    out: dict[str, set[str]] = field(default_factory=lambda: defaultdict(set))
    inn: dict[str, set[str]] = field(default_factory=lambda: defaultdict(set))
    relation: dict[tuple[str, str], str] = field(default_factory=dict)


def wiring_from_edges(edges: list[tuple[str, str]]) -> Wiring:
    nodes: set[str] = set()
    out: dict[str, set[str]] = defaultdict(set)
    inn: dict[str, set[str]] = defaultdict(set)
    relation: dict[tuple[str, str], str] = {}
    norm: list[tuple[str, str]] = []
    for s, d in edges:
        nodes.add(s)
        nodes.add(d)
        out[s].add(d)
        inn[d].add(s)
        relation[(s, d)] = ""
        norm.append((s, d))
    return Wiring(nodes=nodes, edges=norm, out=out, inn=inn, relation=relation)


def load_wiring(index_path: str | None = None) -> Wiring:
    index_path = index_path or os.path.join(HERE, "atlas.index.json")
    with open(index_path) as fh:
        index = json.load(fh)
    interactions = index["interactions"]
    it = interactions.values() if isinstance(interactions, dict) else interactions

    nodes: set[str] = set()
    edges: list[tuple[str, str]] = []
    out: dict[str, set[str]] = defaultdict(set)
    inn: dict[str, set[str]] = defaultdict(set)
    relation: dict[tuple[str, str], str] = {}
    for i in it:
        s, d = i.get("source"), i.get("destination")
        if not s or not d:
            continue
        nodes.add(s)
        nodes.add(d)
        edges.append((s, d))
        out[s].add(d)
        inn[d].add(s)
        relation[(s, d)] = i.get("relation", "")
    return Wiring(nodes=nodes, edges=edges, out=out, inn=inn, relation=relation)


def _sccs(nodes: set[str], out: dict[str, set[str]]) -> list[list[str]]:
    """Tarjan strongly connected components (iterative)."""
    index_of: dict[str, int] = {}
    low: dict[str, int] = {}
    on_stack: set[str] = set()
    stack: list[str] = []
    result: list[list[str]] = []
    counter = 0

    for root in nodes:
        if root in index_of:
            continue
        work = [(root, iter(sorted(out.get(root, ()))))]
        index_of[root] = low[root] = counter
        counter += 1
        stack.append(root)
        on_stack.add(root)
        while work:
            node, it = work[-1]
            advanced = False
            for w in it:
                if w not in index_of:
                    index_of[w] = low[w] = counter
                    counter += 1
                    stack.append(w)
                    on_stack.add(w)
                    work.append((w, iter(sorted(out.get(w, ())))))
                    advanced = True
                    break
                elif w in on_stack:
                    low[node] = min(low[node], index_of[w])
            if advanced:
                continue
            work.pop()
            if work:
                parent = work[-1][0]
                low[parent] = min(low[parent], low[node])
            if low[node] == index_of[node]:
                comp = []
                while True:
                    w = stack.pop()
                    on_stack.discard(w)
                    comp.append(w)
                    if w == node:
                        break
                result.append(comp)
    return result


def _reachable(start: str, out: dict[str, set[str]]) -> set[str]:
    seen: set[str] = set()
    stack = [start]
    while stack:
        n = stack.pop()
        for w in out.get(n, ()):
            if w not in seen:
                seen.add(w)
                stack.append(w)
    return seen


def feedback_loops(w: Wiring) -> list[list[str]]:
    loops = [sorted(c) for c in _sccs(w.nodes, w.out) if len(c) > 1]
    # self-loops are trivial feedback too
    for n in w.nodes:
        if n in w.out.get(n, set()):
            loops.append([n])
    return sorted(loops, key=len, reverse=True)


def one_way_edges(w: Wiring) -> list[tuple[str, str]]:
    """Edges A -> B where B cannot reach back to A (the flow never closes)."""
    out = []
    for s, d in w.edges:
        if s not in _reachable(d, w.out):
            out.append((s, d))
    return sorted(set(out))


def pure_sources(w: Wiring) -> list[str]:
    return sorted(n for n in w.nodes if w.out.get(n) and not w.inn.get(n))


def pure_sinks(w: Wiring) -> list[str]:
    return sorted(n for n in w.nodes if w.inn.get(n) and not w.out.get(n))


def loop_closures(w: Wiring) -> list[dict]:
    """One-way edges ranked by the destination's downstream influence -- closing a
    return path on a high-influence destination is the highest-value loop to add."""
    ranked = []
    for s, d in one_way_edges(w):
        influence = len(_reachable(d, w.out))
        ranked.append({
            "one_way": f"{s} -> {d}",
            "relation": w.relation.get((s, d), ""),
            "destination_influence": influence,
            "suggestion": f"close a path {d} -> ... -> {s} to make this a feedback loop",
        })
    ranked.sort(key=lambda r: r["destination_influence"], reverse=True)
    return ranked


@dataclass
class FactWiring:
    owns: dict[str, set[str]]        # organ -> facts it is authoritative for
    consumes: dict[str, set[str]]    # organ -> facts it reads
    publishes: dict[str, set[str]]   # organ -> facts it exports
    subscribes: dict[str, set[str]]  # organ -> deltas it needs
    producers: dict[str, set[str]]   # fact -> organs that own/publish it
    consumers: dict[str, set[str]]   # fact -> organs that consume/subscribe it


def load_fact_wiring(index_path: str | None = None) -> FactWiring:
    index_path = index_path or os.path.join(HERE, "atlas.index.json")
    with open(index_path) as fh:
        index = json.load(fh)
    archs = index["architectures"]
    items = archs.items() if isinstance(archs, dict) else [(a.get("canonical_name"), a) for a in archs]

    owns: dict[str, set[str]] = {}
    consumes: dict[str, set[str]] = {}
    publishes: dict[str, set[str]] = {}
    subscribes: dict[str, set[str]] = {}
    producers: dict[str, set[str]] = defaultdict(set)
    consumers: dict[str, set[str]] = defaultdict(set)
    for aid, a in items:
        o = set(a.get("owns", []))
        c = set(a.get("consumes", []))
        p = set(a.get("publishes", []))
        s = set(a.get("subscribes", []))
        if not (o or c or p or s):
            continue
        owns[aid], consumes[aid], publishes[aid], subscribes[aid] = o, c, p, s
        for f in o | p:
            producers[f].add(aid)
        for f in c | s:
            consumers[f].add(aid)
    return FactWiring(owns, consumes, publishes, subscribes, dict(producers), dict(consumers))


def orphan_producers(fw: FactWiring) -> list[tuple[str, list[str]]]:
    """(fact, producing organs) for a fact published/owned but consumed by nobody --
    published into the void. A terminal surface fact is a legitimate exception."""
    out = []
    for fact, prod in sorted(fw.producers.items()):
        if fact not in fw.consumers:
            out.append((fact, sorted(prod)))
    return out


def orphan_consumers(fw: FactWiring) -> list[tuple[str, list[str]]]:
    """(fact, consuming organs) for a fact consumed but owned/published by nobody --
    the sharp gap: an organ expects state no organ provides. Wire this."""
    out = []
    for fact, cons in sorted(fw.consumers.items()):
        if fact not in fw.producers:
            out.append((fact, sorted(cons)))
    return out


def duplicate_ownership(fw: FactWiring) -> list[tuple[str, list[str]]]:
    """(fact, owners) where more than one organ claims authority for a fact."""
    owners: dict[str, list[str]] = defaultdict(list)
    for organ, facts in fw.owns.items():
        for f in facts:
            owners[f].append(organ)
    return [(f, sorted(o)) for f, o in sorted(owners.items()) if len(o) > 1]


def unowned_published(fw: FactWiring) -> list[tuple[str, list[str]]]:
    """(fact, publishers) for a fact published but OWNED by nobody -- exported state
    with no authoritative source, a weak edge waiting to drift."""
    out = []
    owned = {f for facts in fw.owns.values() for f in facts}
    pub: dict[str, list[str]] = defaultdict(list)
    for organ, facts in fw.publishes.items():
        for f in facts:
            pub[f].append(organ)
    for f, publishers in sorted(pub.items()):
        if f not in owned:
            out.append((f, sorted(publishers)))
    return out


def fact_flow(fw: FactWiring) -> Wiring:
    """Organ-to-organ graph: A -> B when A produces a fact B consumes."""
    edges: list[tuple[str, str]] = []
    for fact, prod in fw.producers.items():
        for a in prod:
            for b in fw.consumers.get(fact, ()):
                if a != b:
                    edges.append((a, b))
    return wiring_from_edges(sorted(set(edges)))


def report(index_path: str | None = None) -> dict:
    w = load_wiring(index_path)
    loops = feedback_loops(w)
    return {
        "wired_nodes": len(w.nodes),
        "interactions": len(w.edges),
        "feedback_loops": loops,
        "largest_feedback_core": max(loops, key=len) if loops else [],
        "one_way_edges": [f"{s} -> {d}" for s, d in one_way_edges(w)],
        "pure_sources": pure_sources(w),
        "pure_sinks": pure_sinks(w),
        "top_loop_closures": loop_closures(w)[:8],
    }


def fact_report(index_path: str | None = None) -> dict:
    fw = load_fact_wiring(index_path)
    ff = fact_flow(fw)
    loops = [sorted(c) for c in _sccs(ff.nodes, ff.out) if len(c) > 1]
    return {
        "wired_organs": len(fw.owns),
        "facts": sorted(set(fw.producers) | set(fw.consumers)),
        "orphan_producers": orphan_producers(fw),
        "orphan_consumers": orphan_consumers(fw),
        "duplicate_ownership": duplicate_ownership(fw),
        "unowned_published": unowned_published(fw),
        "fact_feedback_loops": sorted(loops, key=len, reverse=True),
    }


def to_markdown(index_path: str | None = None) -> str:
    """A generated view of the wiring self-analysis -- the interaction graph and
    the WiringSpec fact flow, feedback loops and gaps. Regenerate with
    `atlas.py wiring-doc`; never hand-edit."""
    r = report(index_path)
    fr = fact_report(index_path)
    lines = [
        "# Wiring map (generated)",
        "",
        "Generated by `python3 architecture/atlas.py wiring-doc` from the atlas. Do",
        "not hand-edit. It shows how the estate wires into itself -- feedback loops",
        "(an organ whose effect returns as evidence) versus one-way flows.",
        "",
        "## Interaction graph",
        "",
        f"- {r['wired_nodes']} interacting architectures, {r['interactions']} interactions",
        f"- feedback loops closed: {len(r['feedback_loops'])}; one-way flows: {len(r['one_way_edges'])}",
        "",
        "## WiringSpec fact flow",
        "",
        f"- {fr['wired_organs']} organs carry a WiringSpec, over {len(fr['facts'])} shared facts",
        f"- duplicate ownership: {len(fr['duplicate_ownership'])} (validate fails if > 0)",
        "",
        "### Closed fact feedback loops",
        "",
    ]
    for loop in fr["fact_feedback_loops"]:
        lines.append(f"- **{len(loop)} organs**: {' ↔ '.join(loop)}")
    lines += ["", "### Open gaps (the next wiring to build)", ""]
    if fr["orphan_consumers"]:
        lines.append("Orphan consumers — an organ expects a fact nobody provides:")
        for f, cons in fr["orphan_consumers"]:
            lines.append(f"- `{f}` ← {', '.join(cons)}")
        lines.append("")
    if fr["orphan_producers"]:
        lines.append("Orphan producers — published, nobody consumes (terminal surface or unwired):")
        for f, prod in fr["orphan_producers"]:
            lines.append(f"- `{f}` → {', '.join(prod)}")
    lines.append("")
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    import pprint
    pprint.pp(report())
