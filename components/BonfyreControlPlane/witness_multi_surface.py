"""All-surface witness (mandate SS31): ONE identity, many grammars, no private copy.

Over the LIVE fabric projections BonfyreFS serves, prove a single ResourceRef
(one actor) appears through multiple independent surface grammars -- a fact, a
load-bearing query directory, a verify-plan directory -- all deriving from the one
canonical control-plane actor. No surface owns a private copy: every grammar
points at the same actor_id, and the fact has exactly one owner (actor-graph).
"""

from __future__ import annotations

import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
PROJ = os.path.expanduser("~/.bonfyre/estate-fabric/projections")


def _members(kind: str, name: str) -> list[dict]:
    path = os.path.join(PROJ, kind, name, "index.json")
    if not os.path.exists(path):
        return []
    return json.load(open(path)).get("members", [])


def surfaces_of(actor_id: str) -> dict:
    """Which served grammars currently expose this actor."""
    found = {}
    facts = _members("facts", "Actor")
    if any(m.get("id") == actor_id for m in facts):
        found["fact:Actor"] = "the actor as a canonical fact (owner: actor-graph)"
    lb = _members("queries", "Load-Bearing")
    if any(m.get("id") == actor_id for m in lb):
        found["query:Load-Bearing"] = "a CollapseFront leverage projection"
    rtv = _members("queries", "Ready-To-Verify")
    if any(m.get("id") == actor_id for m in rtv):
        found["query:Ready-To-Verify"] = "a fortification verify-plan projection"
    uv = _members("queries", "Unverified-Actors")
    if any(m.get("id") == actor_id for m in uv):
        found["query:Unverified-Actors"] = "the unconfirmed-actors projection"
    # the same identity as a real Frappe app record (CRM Lead), no bench, no copy
    path = os.path.join(PROJ, "app-records", "crm", "CRM-Lead", "index.json")
    if os.path.exists(path):
        recs = json.load(open(path)).get("records", [])
        if any(r.get("_bonfyre_ref") == actor_id for r in recs):
            found["app-record:CRM-Lead"] = "the actor wearing CRM Lead's real DocType fields"
    return found


def single_owner(fact: str) -> list[str]:
    """From the wiring: how many organs OWN this fact (must be exactly one)."""
    idx = json.load(open(os.path.join(REPO, "architecture", "atlas.index.json")))
    archs = idx["architectures"]
    items = archs.values() if isinstance(archs, dict) else archs
    return [a.get("canonical_name") for a in items if fact in a.get("owns", [])]


def witness(actor_id: str) -> dict:
    return {
        "actor": actor_id,
        "surfaces": surfaces_of(actor_id),
        "actor_owners": single_owner("Actor"),
    }


if __name__ == "__main__":
    aid = sys.argv[1] if len(sys.argv) > 1 else "org:tarbell-center"
    r = witness(aid)
    print(f"identity: {r['actor']}")
    for g, desc in r["surfaces"].items():
        print(f"  served via {g:26s} {desc}")
    print(f"Actor fact owners: {r['actor_owners']}")
    assert len(r["surfaces"]) >= 3, "identity must appear through >=3 grammars"
    assert len(r["actor_owners"]) == 1, "the fact must have exactly ONE owner (no pairwise copies)"
    print(f"ALL-SURFACE WITNESS: PASS ({len(r['surfaces'])} grammars, 1 owner, no private copy)")
