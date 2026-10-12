"""CollapseFront -- the retraction dual of reachability.

Reachability answers "what can I reach from here." Nothing in the system
answered the dual: for any ground fact, *which conclusions rest on it and go
dark if it is withdrawn* -- and, transposed, for any conclusion, *which single
facts is it load-bearing on.* That is what this builds.

A conclusion is not a free-floating assertion; it stands on a support structure.
We model that structure as a monotone AND/OR lattice over the actor graph:

  * a ground is an actor node (it exists / is who we say);
  * an edge conclusion (A --kind--> B) is an AND of its two endpoints -- lose
    either actor and the edge cannot stand;
  * a reach conclusion (A can fund / employ / decide-through to O) is an OR over
    every path that establishes it, and each path is an AND of its hop edges.
    Two independent authority paths to the same org means the reach survives
    losing one -- collapse requires losing all.

collapse_front(ground g)      = { conclusions that become false if g alone is retracted }
critical_support(conclusion c)= { grounds whose solo retraction collapses c }

These are the two projections of one boolean matrix M[c][g] = "c collapses when g
is retracted," so they are exact transposes. critical_support is the operational
form of the constitution's no-maturity-laundering rule: retract a witness and the
CollapseFront names every claim that was resting on it with no independent
support. A laundered claim -- one whose only support is a parent's label -- shows
up as collapsing under a retraction it should have survived.

This is deliberately monotone (support only, no negation): adding a fact never
removes a conclusion, so the fixpoint is well-defined and order-independent.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from typing import Iterable, Optional

# Edge kinds that carry support downstream (a directed "can reach" capacity).
# contact_of / publishes_in are relationships, not capacity chains, so a reach
# does not propagate through them.
REACH_EDGE_KINDS = ("funds", "employs", "authority_over", "opportunity_unlock")


@dataclass
class _Node:
    nid: str
    op: str  # "ground" | "and" | "or"
    children: list[str] = field(default_factory=list)


@dataclass
class Lattice:
    nodes: dict[str, _Node]
    grounds: set[str]
    conclusions: set[str]
    # human label per node, for readable output
    labels: dict[str, str] = field(default_factory=dict)

    def parents_of(self, nid: str) -> list[str]:
        return [n.nid for n in self.nodes.values() if nid in n.children]


def _ground_id(actor_id: str) -> str:
    return f"actor:{actor_id}"


def _edge_id(frm: str, kind: str, to: str) -> str:
    return f"edge:{frm}|{kind}|{to}"


def build_lattice(db: sqlite3.Connection, max_path_len: int = 3) -> Lattice:
    """Build the AND/OR support lattice from the actor graph.

    reach conclusions are enumerated up to max_path_len hops over REACH_EDGE_KINDS.
    The lattice is finite and acyclic-by-construction: a reach node of length k
    only depends on reach nodes of length < k.
    """
    nodes: dict[str, _Node] = {}
    grounds: set[str] = set()
    conclusions: set[str] = set()
    labels: dict[str, str] = {}

    actor_ids = [r[0] for r in db.execute("SELECT actor_id FROM actor_nodes")]
    for aid in actor_ids:
        gid = _ground_id(aid)
        nodes[gid] = _Node(gid, "ground")
        grounds.add(gid)
        labels[gid] = aid

    # edge conclusions: AND of the two endpoints (only when both are recorded).
    known = set(actor_ids)
    edges: list[tuple[str, str, str]] = []
    for frm, kind, to in db.execute("SELECT from_id, edge_kind, to_id FROM actor_edges"):
        if frm not in known or to not in known:
            continue
        edges.append((frm, kind, to))
        eid = _edge_id(frm, kind, to)
        if eid not in nodes:
            nodes[eid] = _Node(eid, "and", [_ground_id(frm), _ground_id(to)])
            conclusions.add(eid)
            labels[eid] = f"{frm} --{kind}--> {to}"

    # adjacency over reach-carrying edges
    out_edges: dict[str, list[tuple[str, str]]] = {}
    for frm, kind, to in edges:
        if kind in REACH_EDGE_KINDS:
            out_edges.setdefault(frm, []).append((kind, to))

    # reach conclusions: reach(A -> O). OR over each first hop A --kind--> X,
    # where that hop's support is AND(edge(A,kind,X), reach(X -> O)) for len>1,
    # or just edge(A,kind,X) when X == O (length 1). Enumerate by increasing len.
    def reach_id(a: str, o: str) -> str:
        return f"reach:{a}=>{o}"

    # Collect all (a, o) reachable within max_path_len and the supports per length.
    # supports_by_pair[(a,o)] = list of AND-support node ids (one per distinct path).
    supports_by_pair: dict[tuple[str, str], list[str]] = {}

    def enumerate_paths(a: str, path: list[tuple[str, str, str]], visited: set[str]):
        if len(path) >= 1:
            o = path[-1][2]
            # build an AND node for this specific path
            path_children = [_edge_id(f, k, t) for (f, k, t) in path]
            if len(path_children) == 1:
                support_node = path_children[0]  # the edge itself is the support
            else:
                pid = "andpath:" + ">".join(path_children)
                if pid not in nodes:
                    nodes[pid] = _Node(pid, "and", path_children)
                    labels[pid] = " & ".join(labels[c] for c in path_children)
                support_node = pid
            supports_by_pair.setdefault((a, o), []).append(support_node)
        if len(path) >= max_path_len:
            return
        cur = path[-1][2] if path else a
        for kind, nxt in out_edges.get(cur, []):
            if nxt in visited:
                continue
            enumerate_paths(a, path + [(cur, kind, nxt)], visited | {nxt})

    starts = set(out_edges.keys())
    for a in starts:
        enumerate_paths(a, [], {a})

    for (a, o), supports in supports_by_pair.items():
        rid = reach_id(a, o)
        # dedup supports, keep order
        seen: set[str] = set()
        uniq = [s for s in supports if not (s in seen or seen.add(s))]
        nodes[rid] = _Node(rid, "or", uniq)
        conclusions.add(rid)
        labels[rid] = f"reach {a} => {o}"

    return Lattice(nodes=nodes, grounds=grounds, conclusions=conclusions, labels=labels)


def _evaluate(lat: Lattice, false_grounds: set[str]) -> dict[str, bool]:
    """Monotone fixpoint: value of every node with the given grounds set false."""
    val: dict[str, bool] = {}
    # grounds
    for gid in lat.grounds:
        val[gid] = gid not in false_grounds
    # iterate ALL non-ground nodes to fixpoint (internal andpath nodes included,
    # not just the user-facing conclusions). Monotone -> converges.
    remaining = {nid for nid in lat.nodes if nid not in lat.grounds}
    changed = True
    while changed and remaining:
        changed = False
        done = []
        for nid in list(remaining):
            node = lat.nodes[nid]
            child_vals = [val.get(c) for c in node.children]
            if any(cv is None for cv in child_vals):
                continue  # a child not yet evaluated; revisit
            if node.op == "and":
                v = all(child_vals)
            else:  # or
                v = any(child_vals)
            val[nid] = v
            done.append(nid)
            changed = True
        for nid in done:
            remaining.discard(nid)
    # any node still unresolved (cycle guard -- shouldn't happen) defaults to its
    # optimistic value under available children
    for nid in remaining:
        node = lat.nodes[nid]
        child_vals = [val.get(c, True) for c in node.children]
        val[nid] = all(child_vals) if node.op == "and" else any(child_vals)
    return val


def collapse_front(lat: Lattice, ground_actor_id: str) -> list[str]:
    """Conclusions that become false if this ground actor alone is retracted.

    Returns conclusion node ids, sorted. Empty if the ground carries nothing.
    """
    gid = _ground_id(ground_actor_id)
    if gid not in lat.grounds:
        raise KeyError(f"unknown ground actor {ground_actor_id!r}")
    base = _evaluate(lat, set())
    after = _evaluate(lat, {gid})
    collapsed = [c for c in lat.conclusions if base.get(c, True) and not after.get(c, True)]
    return sorted(collapsed)


def critical_support(lat: Lattice, conclusion_id: str) -> list[str]:
    """Ground actors whose SOLO retraction collapses this conclusion.

    The load-bearing set: single points of failure. A conclusion with no critical
    ground has at least two independent supports (it is robust to any one loss).
    """
    if conclusion_id not in lat.conclusions:
        raise KeyError(f"unknown conclusion {conclusion_id!r}")
    base = _evaluate(lat, set())
    if not base.get(conclusion_id, True):
        return []  # already unsupported
    critical = []
    for gid in lat.grounds:
        after = _evaluate(lat, {gid})
        if not after.get(conclusion_id, True):
            critical.append(lat.labels.get(gid, gid))
    return sorted(critical)


def collapse_matrix(lat: Lattice) -> dict[str, set[str]]:
    """M[ground_actor] = set of conclusion ids that collapse if it alone is retracted.

    collapse_front and critical_support are the two projections of this matrix.
    """
    m: dict[str, set[str]] = {}
    base = _evaluate(lat, set())
    for gid in lat.grounds:
        after = _evaluate(lat, {gid})
        collapsed = {c for c in lat.conclusions if base.get(c, True) and not after.get(c, True)}
        if collapsed:
            m[lat.labels.get(gid, gid)] = collapsed
    return m
