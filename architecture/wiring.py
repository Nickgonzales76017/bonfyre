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


if __name__ == "__main__":
    import pprint
    pprint.pp(report())
