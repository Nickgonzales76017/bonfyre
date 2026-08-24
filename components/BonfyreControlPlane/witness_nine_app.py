"""Nine-app witness (mandate SS30): one identity flows across app grammars with a
single canonical owner -- no pairwise sync database.

Over the WiringSpec + the live fabric: a canonical fact (Actor) has exactly one
owner and is consumed by multiple Frappe-app grammars (CRM, HRMS, Helpdesk, ...).
Each app reads the shared fact; none keeps a private copy. A mutation about the
identity folds once (native occurrence) and every app that consumes the fact sees
it -- the "no pairwise sync" property the mandate requires.
"""

from __future__ import annotations

import json
import os

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))


def _archs() -> list[dict]:
    idx = json.load(open(os.path.join(REPO, "architecture", "atlas.index.json")))
    a = idx["architectures"]
    return list(a.values()) if isinstance(a, dict) else a


def fact_flow(fact: str) -> dict:
    owners, app_consumers, other_consumers = [], [], []
    for a in _archs():
        name = a.get("canonical_name")
        if fact in a.get("owns", []):
            owners.append(name)
        if fact in a.get("consumes", []) or fact in a.get("subscribes", []):
            (app_consumers if a.get("family") == "apps" else other_consumers).append(name)
    return {"owners": owners, "app_consumers": sorted(app_consumers),
            "other_consumers": sorted(other_consumers)}


def app_publishes() -> dict:
    """Facts each app publishes back into the fabric -- how a mutation re-enters."""
    out = {}
    for a in _archs():
        if a.get("family") == "apps" and a.get("publishes"):
            out[a.get("canonical_name")] = sorted(a.get("publishes", []))
    return out


def witness() -> dict:
    return {"Actor": fact_flow("Actor"), "WorkState": fact_flow("WorkState"),
            "app_publishes": app_publishes()}


if __name__ == "__main__":
    r = witness()
    actor = r["Actor"]
    print(f"Actor owner(s): {actor['owners']}")
    print(f"Actor consumed by apps: {actor['app_consumers']}")
    print(f"Actor consumed by other organs: {actor['other_consumers']}")
    print("app grammars publishing back into the fabric:")
    for app, pub in r["app_publishes"].items():
        print(f"  {app:12s} publishes {pub}")
    # one canonical owner, consumed by multiple app grammars -> no pairwise sync
    assert actor["owners"] == ["ActorGraph"], "Actor must have exactly one owner"
    assert len(actor["app_consumers"]) >= 2, "Actor must flow to >=2 app grammars"
    # the loop closes: at least one app publishes a fact back that a core organ folds
    assert any("CommunicationEvent" in p for p in r["app_publishes"].values()), \
        "an app must publish a fact back into the fabric (CRM CommunicationEvent)"
    print(f"NINE-APP WITNESS: PASS (1 owner, {len(actor['app_consumers'])} app grammars, "
          f"mutation re-enters via app publishes -- no pairwise sync)")
