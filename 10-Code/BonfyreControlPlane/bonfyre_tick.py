"""One tick of the fused machine -- the whole loop running on live state.

Not a diagram of the loop: it runs it. In order:

  1. fold pending occurrences into projections (occurrence spine)
  2. gather graph corroborations (verification, machine half; never promotes)
  3. recompute the CollapseFront fortification plan (what to verify next)
  4. materialize the virtual query directories + fact projections (BonfyreFS view)
  5. read the wiring self-analysis (is the estate one connected machine?)
  6. emit a briefing of current institutional state and the ranked next actions

Everything reads/writes only the control plane and the on-disk projection trees;
it never touches the daemon-held fabric.db (that registration is opt-in). Safe to
run repeatedly; each step is idempotent.
"""

from __future__ import annotations

import os
import sqlite3
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
CONTROL_DB = os.path.join(HERE, "control_plane.db")


def tick(control_db: str = CONTROL_DB) -> dict:
    report: dict = {}

    # 1. fold pending occurrences
    try:
        import datetime as _dt
        import external_events as ee
        now = _dt.datetime.now(ee.UTC)
        stamp = ee._iso(now)
        db = sqlite3.connect(control_db)
        try:
            ee.ensure_schema(db)
            db.execute("CREATE TABLE IF NOT EXISTS occurrence_projection("
                       "event_id INTEGER PRIMARY KEY, actor TEXT, event_kind TEXT,"
                       " status TEXT, projected_at TEXT)")
            folded = 0
            for ev in ee.unprojected(db):
                status = ee._STATUS_PROJECTION.get(ev.event_kind)
                if status:
                    db.execute("INSERT OR REPLACE INTO occurrence_projection"
                               "(event_id,actor,event_kind,status,projected_at)"
                               " VALUES(?,?,?,?,?)",
                               (ev.id, ev.actor, ev.event_kind, status, stamp))
                ee.mark_projected(db, ev.id, now=now)
                folded += 1
            db.commit()
            report["occurrences_folded"] = folded
        finally:
            db.close()
    except Exception as e:  # noqa: BLE001
        report["occurrences_folded"] = f"skip: {e}"

    # 2. gather graph corroborations (never promotes -- human line held)
    try:
        import corroborate_from_graph as cg
        db = sqlite3.connect(control_db)
        try:
            r = cg.corroborate(db)
        finally:
            db.close()
        report["corroborations_recorded"] = r.recorded
        report["ready_for_human_verification"] = r.ready_for_human
    except Exception as e:  # noqa: BLE001
        report["corroborations_recorded"] = f"skip: {e}"

    # 3. fortification top actions (from the fast cached matrix)
    try:
        import fabric_queries as fq
        fq._CACHE.clear()
        verify = fq.q_ready_to_verify(control_db)
        report["verify_targets"] = verify[:5]
    except Exception as e:  # noqa: BLE001
        report["verify_targets"] = f"skip: {e}"

    # 4. materialize the virtual query directories + fact projections
    try:
        import fabric_queries as fq
        import fabric_facts as ff
        report["queries"] = fq.publish(control_db)["sets"]
        report["facts"] = ff.publish(control_db)["facts"]
    except Exception as e:  # noqa: BLE001
        report["queries"] = f"skip: {e}"

    # 5. wiring self-analysis
    try:
        sys.path.insert(0, os.path.join(REPO, "architecture"))
        import wiring
        fr = wiring.fact_report()
        report["wiring"] = {
            "wired_organs": fr["wired_organs"],
            "facts": len(fr["facts"]),
            "feedback_loops": [len(L) for L in fr["fact_feedback_loops"]],
            "orphan_consumers": [f for f, _ in fr["orphan_consumers"]],
        }
    except Exception as e:  # noqa: BLE001
        report["wiring"] = f"skip: {e}"

    return report


def briefing(report: dict) -> str:
    lines = ["# Bonfyre tick", ""]
    lines.append(f"- occurrences folded: {report.get('occurrences_folded')}")
    lines.append(f"- corroborations recorded: {report.get('corroborations_recorded')}")
    ready = report.get("ready_for_human_verification") or []
    lines.append(f"- ready for human verification ({len(ready)}): {', '.join(ready) or 'none'}")
    w = report.get("wiring")
    if isinstance(w, dict):
        loops = w["feedback_loops"]
        lines.append(f"- wiring: {w['wired_organs']} organs, {w['facts']} facts, "
                     f"feedback core {max(loops) if loops else 0} organs")
    lines += ["", "## Next actions (verify these -- highest leverage first)", ""]
    vt = report.get("verify_targets")
    if isinstance(vt, list):
        for m in vt:
            lines.append(f"- **{m['label']}** — leverage {m['leverage']}, "
                         f"{m['corroborations']} corroborations, {m['gap']} more to confirm")
    q = report.get("queries")
    if isinstance(q, dict):
        lines += ["", "## Live query directories", ""]
        for name, n in q.items():
            lines.append(f"- `{name}/` — {n}")
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    cp = sys.argv[1] if len(sys.argv) > 1 else CONTROL_DB
    print(briefing(tick(cp)))
