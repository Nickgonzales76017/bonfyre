"""Fortification -- turn the institution's dependency structure into ranked action.

The reachability engine tells the loop what is reachable. It cannot tell a
*fragile* reachability (resting on one unconfirmed actor, one retraction from
blocked) from a *robust* one. CollapseFront can, so this composes the two into a
concrete, ranked plan over the REAL relationship graph:

  * VERIFY -- an asserted (unconfirmed) actor that many conclusions rest on. The
    whole subtree is standing on an assumption; confirming it removes the most
    latent risk. Leverage = how many live conclusions collapse if the assumption
    is wrong (its CollapseFront).

  * DIVERSIFY -- a high-value conclusion (funding, authority, an opportunity
    unlock) whose critical_support is a single actor. One relationship is a
    single point of failure; a second independent path makes the capacity robust.

This is not an audit. It emits the actual next moves, ranked by leverage, and
publishes the plan into the live fabric so the loop and a human read the same
priorities BonfyreFS serves.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path

import support_lattice as sl
import verification as verif

# Conclusion kinds that carry institutional value worth protecting.
VALUE_EDGE_KINDS = ("funds", "authority_over", "opportunity_unlock", "employs")


@dataclass
class Action:
    kind: str  # "verify" | "diversify"
    target: str
    display: str
    leverage: int
    confidence: str
    rationale: str
    protects: list[str] = field(default_factory=list)
    corroborations: int = 0  # independent sources recorded so far
    gap: int = 0             # more independent sources needed to promote


@dataclass
class Plan:
    actor_count: int
    conclusion_count: int
    actions: list[Action]
    robust_conclusions: int
    fragile_conclusions: int


def _actor_meta(db: sqlite3.Connection) -> dict[str, dict]:
    meta = {}
    for aid, name, conf, org in db.execute(
        "SELECT actor_id, display_name, confidence, org_id FROM actor_nodes"
    ):
        meta[aid] = {"display": name or aid, "confidence": conf or "asserted", "org": org}
    return meta


def _is_value(conclusion_id: str, labels: dict[str, str]) -> bool:
    if conclusion_id.startswith("edge:"):
        return any(f"--{k}--" in labels.get(conclusion_id, "") for k in VALUE_EDGE_KINDS)
    return conclusion_id.startswith("reach:")


def verification_priorities(db: sqlite3.Connection, lat: sl.Lattice,
                            meta: dict[str, dict], limit: int = 12) -> list[Action]:
    """Asserted actors ranked by how much live capacity rests on their unconfirmed
    assertion."""
    actions: list[Action] = []
    for aid, m in meta.items():
        if m["confidence"] == "verified":
            continue
        try:
            front = sl.collapse_front(lat, aid)
        except KeyError:
            continue
        value_front = [c for c in front if _is_value(c, lat.labels)]
        if not value_front:
            continue
        try:
            vs = verif.verification_state(db, aid)
            corr, gap = vs.independent_sources, vs.gap
        except sqlite3.Error:
            corr, gap = 0, verif.DEFAULT_THRESHOLD
        actions.append(Action(
            kind="verify", target=aid, display=m["display"], leverage=len(value_front),
            confidence=m["confidence"],
            rationale=(f"{len(value_front)} live conclusions rest on {aid!r}, which is "
                       f"only {m['confidence']}. {corr} independent corroboration(s) on "
                       f"file; {gap} more promotes it to verified."),
            protects=[lat.labels[c] for c in value_front[:6]],
            corroborations=corr, gap=gap,
        ))
    actions.sort(key=lambda a: a.leverage, reverse=True)
    return actions[:limit]


def diversification_targets(db: sqlite3.Connection, lat: sl.Lattice,
                            meta: dict[str, dict], limit: int = 12) -> list[Action]:
    """High-value conclusions standing on a single actor -- add a second path."""
    actions: list[Action] = []
    seen_single: set[str] = set()
    # invert: for each ground, the value conclusions it solely supports
    m = sl.collapse_matrix(lat)
    # label -> actor_id
    label_to_id = {meta[a]["display"]: a for a in meta}
    for ground_label, collapsed in m.items():
        value = [c for c in collapsed if _is_value(c, lat.labels)]
        if not value:
            continue
        # a conclusion is "fragile" if its critical_support is exactly this one ground
        for c in value:
            crit = sl.critical_support(lat, c)
            if len(crit) == 1 and c not in seen_single:
                seen_single.add(c)
                aid = label_to_id.get(crit[0], crit[0])
                conf = meta.get(aid, {}).get("confidence", "?")
                actions.append(Action(
                    kind="diversify", target=c, display=lat.labels[c], leverage=1,
                    confidence=conf,
                    rationale=(f"stands on the single actor {crit[0]!r} ({conf}); a second "
                               f"independent path makes this capacity robust."),
                    protects=[lat.labels[c]],
                ))
    # rank diversify by whether the sole support is unverified (worse) then label
    actions.sort(key=lambda a: (a.confidence == "verified", a.display))
    return actions[:limit]


def build_plan(db: sqlite3.Connection, max_path_len: int = 3) -> Plan:
    lat = sl.build_lattice(db, max_path_len=max_path_len)
    meta = _actor_meta(db)
    verify = verification_priorities(db, lat, meta)
    diversify = diversification_targets(db, lat, meta)
    # robustness census: a value conclusion is robust if no single ground collapses it
    value_conclusions = [c for c in lat.conclusions if _is_value(c, lat.labels)]
    fragile = 0
    for c in value_conclusions:
        if sl.critical_support(lat, c):
            # has at least one single point of failure among grounds that matter
            crit = sl.critical_support(lat, c)
            # endpoints are always critical for an edge; count "fragile" as reach/
            # multi-support conclusions with a solo-critical intermediary
            if c.startswith("reach:") and len(crit) <= 2:
                fragile += 1
    robust = max(len(value_conclusions) - fragile, 0)
    actions = verify + diversify
    return Plan(
        actor_count=len(meta),
        conclusion_count=len(lat.conclusions),
        actions=actions,
        robust_conclusions=robust,
        fragile_conclusions=fragile,
    )


def plan_markdown(plan: Plan) -> str:
    lines = [
        "# Institutional fortification plan",
        "",
        f"Derived from the live relationship graph: **{plan.actor_count} actors**, "
        f"**{plan.conclusion_count} conclusions**. Ranked by leverage over the "
        f"CollapseFront dependency structure -- highest-risk-removal first.",
        "",
        "## Verify (confirm a load-bearing assumption)",
        "",
    ]
    verify = [a for a in plan.actions if a.kind == "verify"]
    if not verify:
        lines.append("_No unconfirmed load-bearing actors._")
    for i, a in enumerate(verify, 1):
        lines.append(f"{i}. **{a.display}** (`{a.target}`, {a.confidence}) — "
                     f"leverage **{a.leverage}**. {a.rationale}")
        for p in a.protects:
            lines.append(f"   - protects: {p}")
    lines += ["", "## Diversify (remove a single point of failure)", ""]
    diversify = [a for a in plan.actions if a.kind == "diversify"]
    if not diversify:
        lines.append("_No single-actor value conclusions._")
    for i, a in enumerate(diversify, 1):
        lines.append(f"{i}. {a.display} — {a.rationale}")
    lines += ["", "---",
              f"robust value conclusions: {plan.robust_conclusions} · "
              f"fragile: {plan.fragile_conclusions}"]
    return "\n".join(lines) + "\n"


def plan_json(plan: Plan) -> str:
    return json.dumps({
        "actor_count": plan.actor_count,
        "conclusion_count": plan.conclusion_count,
        "robust_conclusions": plan.robust_conclusions,
        "fragile_conclusions": plan.fragile_conclusions,
        "actions": [
            {"kind": a.kind, "target": a.target, "display": a.display,
             "leverage": a.leverage, "confidence": a.confidence,
             "rationale": a.rationale, "protects": a.protects,
             "corroborations": a.corroborations, "gap": a.gap}
            for a in plan.actions
        ],
    }, indent=2, sort_keys=True)


def publish(control_db_path: str, out_dir: str | None = None) -> dict:
    """Compute the plan over a read-only copy and publish it into the live fabric."""
    import fabric_publish as fp

    out = Path(out_dir) if out_dir else (fp.PROJECTIONS)
    out.mkdir(parents=True, exist_ok=True)

    db = sqlite3.connect(f"file:{control_db_path}?mode=ro", uri=True)
    try:
        plan = build_plan(db)
    finally:
        db.close()

    md_path = out / "fortification-plan.md"
    json_path = out / "fortification-plan.json"
    md_path.write_text(plan_markdown(plan))
    json_path.write_text(plan_json(plan))

    fabric = sqlite3.connect(str(fp.FABRIC), timeout=60)
    fabric.execute("PRAGMA busy_timeout=60000")
    fp.ensure_schema(fabric)
    pub_md = fp.publish_file(fabric, name="fortification-plan-md", content_path=md_path,
                             media_type="text/markdown",
                             content_contract="fortification-plan.v1", dedupe=True)
    pub_json = fp.publish_file(fabric, name="fortification-plan", content_path=json_path,
                               content_contract="fortification-plan.v1", dedupe=True)
    fabric.close()
    return {"plan": plan, "published": [pub_md, pub_json],
            "paths": [str(md_path), str(json_path)]}


if __name__ == "__main__":
    import sys
    cp = sys.argv[1] if len(sys.argv) > 1 else "control_plane.db"
    db = sqlite3.connect(f"file:{cp}?mode=ro", uri=True)
    print(plan_markdown(build_plan(db)))
    db.close()
