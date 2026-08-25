"""Personal Data Engine — vault feedback loop for weekly reviews and project scoring."""

import argparse
import json
import re
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[1] / "_shared"))

from bonfyre_toolkit import now_stamp, write_json_report


def find_vault(path):
    vault = Path(path).expanduser()
    if not vault.exists():
        print(f"Vault not found: {vault}")
        raise SystemExit(1)
    return vault


def scan_logs(vault, start_date, end_date):
    """Scan daily logs within a date range."""
    logs_dir = vault / "06-Logs"
    if not logs_dir.exists():
        return []

    entries = []
    for f in sorted(logs_dir.glob("*.md")):
        # Filename is the date: 2026-04-02.md
        try:
            file_date = datetime.strptime(f.stem, "%Y-%m-%d").date()
        except ValueError:
            continue

        if start_date <= file_date <= end_date:
            text = f.read_text(errors="replace")
            entries.append({"date": f.stem, "file": str(f), "text": text, "lines": len(text.splitlines())})
    return entries


def scan_projects(vault):
    """Scan project files for status, tasks, and execution log signals."""
    projects_dir = vault / "02-Projects"
    if not projects_dir.exists():
        return []

    projects = []
    code_dirs = [path for path in (vault / "10-Code").iterdir()] if (vault / "10-Code").exists() else []
    proof_root = vault / "10-Code" / "LocalAITranscriptionService" / "samples" / "proof-deliverables"
    offer_snapshot_path = vault / "05-Monetization" / "_generated-offers.json"
    generated_offers = []
    if offer_snapshot_path.exists():
        payload = json.loads(offer_snapshot_path.read_text(encoding="utf-8"))
        offers = payload.get("offers", [])
        generated_offers = offers if isinstance(offers, list) else []
    distribution_signals = _scan_distribution_signals(vault, generated_offers)
    for f in sorted(projects_dir.glob("Project - *.md")):
        text = f.read_text(errors="replace")
        name = f.stem.replace("Project - ", "")

        # Extract frontmatter fields
        status = _extract_field(text, "status") or "unknown"
        priority = _extract_field(text, "priority") or "medium"
        stage = _extract_field(text, "stage") or "unknown"

        # Count completed tasks
        tasks_done = len(re.findall(r"- \[x\]", text))
        tasks_total = len(re.findall(r"- \[[ x]\]", text))

        # Count execution log entries
        log_entries = len(re.findall(r"^### \d{4}-\d{2}-\d{2}", text, re.MULTILINE))

        matching_code_dir = _find_matching_code_dir(name, code_dirs)
        has_code = matching_code_dir is not None
        proof_count = _count_project_proofs(name, matching_code_dir, proof_root)
        monetization_link_count = len(re.findall(r"05-Monetization|Offer - ", text))
        generated_offer_count = _count_project_offers(name, generated_offers)
        outreach = distribution_signals.get(name, {
            "outreach_sends": 0,
            "outreach_replied": 0,
            "outreach_positive": 0,
            "outreach_negative": 0,
            "outreach_waiting": 0,
            "followup_due": 0,
            "linked_offer_sends": 0,
        })

        projects.append({
            "name": name,
            "status": status,
            "priority": priority,
            "stage": stage,
            "tasks_done": tasks_done,
            "tasks_total": tasks_total,
            "log_entries": log_entries,
            "has_code": has_code,
            "code_dir": str(matching_code_dir) if matching_code_dir else None,
            "proof_count": proof_count,
            "monetization_link_count": monetization_link_count,
            "generated_offer_count": generated_offer_count,
            "outreach_sends": outreach["outreach_sends"],
            "outreach_replied": outreach["outreach_replied"],
            "outreach_positive": outreach["outreach_positive"],
            "outreach_negative": outreach["outreach_negative"],
            "outreach_waiting": outreach["outreach_waiting"],
            "followup_due": outreach["followup_due"],
            "linked_offer_sends": outreach["linked_offer_sends"],
        })
    return projects


def _extract_field(text, field):
    m = re.search(rf"^{field}:\s*(.+)$", text, re.MULTILINE)
    return m.group(1).strip() if m else None


def _normalize_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def _find_matching_code_dir(project_name, code_dirs):
    wanted = _normalize_key(project_name)
    best = None
    best_score = 0
    for path in code_dirs:
        if not path.is_dir():
            continue
        candidate = _normalize_key(path.name)
        if not candidate:
            continue
        score = 0
        if wanted == candidate:
            score = 100
        elif wanted in candidate or candidate in wanted:
            score = min(len(set(wanted) & set(candidate)), 90)
        else:
            tokens = {token for token in re.split(r"[^a-z0-9]+", project_name.lower()) if token}
            score = sum(1 for token in tokens if token and token in path.name.lower()) * 10
        if score > best_score:
            best = path
            best_score = score
    return best if best_score >= 20 else None


def _count_project_proofs(project_name, code_dir, proof_root):
    count = 0
    if code_dir:
        count += len(list(code_dir.glob("outputs/**/*.md")))
        if (code_dir / "samples" / "proof-deliverables").exists():
            count += len(list((code_dir / "samples" / "proof-deliverables").glob("*/proof-review.json")))
    normalized = _normalize_key(project_name)
    if proof_root.exists():
        for path in proof_root.glob("*/proof-summary.json"):
            payload = json.loads(path.read_text(encoding="utf-8"))
            joined = " ".join(str(payload.get(key, "")) for key in ("proof_slug", "proof_label"))
            if normalized and normalized in _normalize_key(joined):
                count += 1
    return count


def _count_project_offers(project_name, generated_offers):
    normalized = _normalize_key(project_name)
    count = 0
    for offer in generated_offers:
        joined = " ".join(str(offer.get(key, "")) for key in ("offer_name", "proof_asset", "buyer_segment"))
        if normalized and normalized in _normalize_key(joined):
            count += 1
    return count


def _scan_distribution_signals(vault, generated_offers):
    signals = {}

    def ensure(name):
        return signals.setdefault(name, {
            "outreach_sends": 0,
            "outreach_replied": 0,
            "outreach_positive": 0,
            "outreach_negative": 0,
            "outreach_waiting": 0,
            "followup_due": 0,
            "linked_offer_sends": 0,
        })

    # Distribution project owns total outreach execution.
    ensure("Quiet Distribution Engine")

    offer_map = {}
    for offer in generated_offers:
        offer_name = str(offer.get("offer_name", "")).strip()
        proof_asset = str(offer.get("proof_asset", "")).strip()
        if offer_name:
            offer_map[offer_name] = {
                "proof_asset": proof_asset,
                "project_names": {"Personal Market Layer"},
            }
            if proof_asset:
                offer_map[offer_name]["project_names"].add("Local AI Transcription Service")

    snapshot_path = vault / "05-Monetization" / "_distribution-pipeline-snapshot.json"
    if snapshot_path.exists():
        payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
        distribution = ensure("Quiet Distribution Engine")
        distribution["outreach_sends"] = int(payload.get("total_sends", 0) or 0)
        distribution["outreach_replied"] = int(payload.get("replied_count", 0) or 0)
        distribution["outreach_positive"] = int(payload.get("positive_reply_count", 0) or 0)
        distribution["outreach_waiting"] = int(payload.get("pending_count", 0) or 0)
        distribution["followup_due"] = int(payload.get("followup_due_count", 0) or 0)

        for row in payload.get("recent_sends", []):
            offer_name = str(row.get("offer_name") or "").strip()
            if not offer_name or offer_name not in offer_map:
                continue
            mapped = offer_map[offer_name]
            project_reply = str(row.get("reply_type") or "waiting")
            for project_name in mapped["project_names"]:
                project = ensure(project_name)
                project["outreach_sends"] += 1
                project["linked_offer_sends"] += 1
                if project_reply == "positive":
                    project["outreach_replied"] += 1
                    project["outreach_positive"] += 1
                elif project_reply == "negative":
                    project["outreach_replied"] += 1
                    project["outreach_negative"] += 1
                elif project_reply == "neutral":
                    project["outreach_replied"] += 1
                else:
                    project["outreach_waiting"] += 1
        return signals

    db_path = vault / "10-Code" / "QuietDistributionEngine" / "data" / "distribution.db"
    if not db_path.exists():
        return signals

    db = sqlite3.connect(str(db_path))
    db.row_factory = sqlite3.Row
    try:
        rows = db.execute("SELECT * FROM sends").fetchall()
    finally:
        db.close()

    now = datetime.now().astimezone()
    for row in rows:
        reply_type = row["reply_type"] or "waiting"
        distribution = ensure("Quiet Distribution Engine")
        distribution["outreach_sends"] += 1
        if reply_type == "positive":
            distribution["outreach_replied"] += 1
            distribution["outreach_positive"] += 1
        elif reply_type == "negative":
            distribution["outreach_replied"] += 1
            distribution["outreach_negative"] += 1
        elif reply_type == "neutral":
            distribution["outreach_replied"] += 1
        else:
            distribution["outreach_waiting"] += 1
            try:
                sent_at = datetime.fromisoformat(str(row["sent_at"]).replace("Z", "+00:00")).astimezone()
            except ValueError:
                sent_at = None
            if sent_at and (now - sent_at).days >= 2:
                distribution["followup_due"] += 1

        offer_name = str(row["offer_name"] or "").strip()
        if not offer_name or offer_name not in offer_map:
            continue

        mapped = offer_map[offer_name]
        for project_name in mapped["project_names"]:
            project = ensure(project_name)
            project["outreach_sends"] += 1
            project["linked_offer_sends"] += 1
            if reply_type == "positive":
                project["outreach_replied"] += 1
                project["outreach_positive"] += 1
            elif reply_type == "negative":
                project["outreach_replied"] += 1
                project["outreach_negative"] += 1
            elif reply_type == "neutral":
                project["outreach_replied"] += 1
            else:
                project["outreach_waiting"] += 1
                try:
                    sent_at = datetime.fromisoformat(str(row["sent_at"]).replace("Z", "+00:00")).astimezone()
                except ValueError:
                    sent_at = None
                if sent_at and (now - sent_at).days >= 2:
                    project["followup_due"] += 1

    return signals


def score_project(p):
    """Score a project 0-100 based on momentum signals."""
    score = 0

    # Activity score (log entries)
    score += min(p["log_entries"] * 10, 30)

    # Task completion
    if p["tasks_total"] > 0:
        score += int((p["tasks_done"] / p["tasks_total"]) * 25)

    if p["has_code"]:
        score += 10

    if p["proof_count"] > 0:
        score += min(20, p["proof_count"] * 6)

    if p["generated_offer_count"] > 0:
        score += min(15, p["generated_offer_count"] * 7)

    if p["monetization_link_count"] > 0:
        score += min(10, p["monetization_link_count"] * 3)

    if p["outreach_sends"] > 0:
        score += min(6, p["outreach_sends"] * 2)

    if p["outreach_replied"] > 0:
        score += min(12, p["outreach_replied"] * 5)

    if p["linked_offer_sends"] > 0:
        score += min(8, p["linked_offer_sends"] * 2)

    if p["outreach_positive"] > 0:
        score += min(15, p["outreach_positive"] * 8)

    if p["outreach_negative"] > 0:
        score -= min(8, p["outreach_negative"] * 3)

    if p["followup_due"] > 0:
        score += min(4, p["followup_due"] * 2)

    # Priority boost
    if p["priority"] == "high":
        score += 15
    elif p["priority"] == "medium":
        score += 8

    # Active status boost
    if p["status"] == "active":
        score += 15
    elif p["status"] == "planned":
        score += 5

    return min(score, 100)


def cmd_review(args):
    vault = find_vault(args.vault)
    week_start = datetime.strptime(args.week, "%Y-%m-%d").date()
    week_end = week_start + timedelta(days=6)

    logs = scan_logs(vault, week_start, week_end)
    projects = scan_projects(vault)

    print(f"# Weekly Review: {week_start} → {week_end}\n")

    # Log summary
    print(f"## Daily Logs")
    if logs:
        for log in logs:
            print(f"- {log['date']}: {log['lines']} lines")
    else:
        print("- No logs found for this week.")

    # Project momentum
    print(f"\n## Project Momentum")
    scored = [(p, score_project(p)) for p in projects]
    scored.sort(key=lambda x: x[1], reverse=True)

    for p, score in scored:
        bar = "█" * (score // 5) + "░" * (20 - score // 5)
        status_tag = f"[{p['status']}]"
        print(f"  {score:>3}  {bar}  {p['name']} {status_tag}")

    # Recommendations
    print(f"\n## Signals")
    active = [p for p, s in scored if p["status"] == "active"]
    stalled = [p for p, s in scored if s < 20 and p["status"] != "planned"]
    hot = [(p, s) for p, s in scored if s >= 60]

    if hot:
        print("  🔥 Hot:")
        for p, s in hot:
            print(f"    - {p['name']} (score: {s})")
    if stalled:
        print("  ⏸  Stalled:")
        for p in stalled:
            print(f"    - {p['name']}")
    if not hot and not stalled:
        print("  No strong signals yet — keep logging.")

    print(f"\n## Next Week Focus")
    if hot:
        print(f"  Double down on: {hot[0][0]['name']}")
    elif active:
        print(f"  Push forward: {active[0]['name']}")
    else:
        print("  Pick one project and log execution for 3+ days.")


def cmd_score(args):
    vault = find_vault(args.vault)
    projects = scan_projects(vault)

    scored = [(p, score_project(p)) for p in projects]
    scored.sort(key=lambda x: x[1], reverse=True)

    print(f"{'Score':<7} {'Status':<10} {'Tasks':<10} {'Logs':<6} {'Project'}")
    print("-" * 60)
    for p, score in scored:
        tasks = f"{p['tasks_done']}/{p['tasks_total']}" if p["tasks_total"] else "-"
        print(f"{score:<7} {p['status']:<10} {tasks:<10} {p['log_entries']:<6} {p['name']}")


def cmd_signals(args):
    vault = find_vault(args.vault)
    projects = scan_projects(vault)
    scored = [(p, score_project(p)) for p in projects]

    moving = [(p, s) for p, s in scored if s >= 40]
    stalled = [(p, s) for p, s in scored if 10 <= s < 40]
    dead = [(p, s) for p, s in scored if s < 10]

    print("## Moving")
    for p, s in sorted(moving, key=lambda x: x[1], reverse=True):
        print(f"  [{s:>3}] {p['name']}")

    print("\n## Stalled")
    for p, s in sorted(stalled, key=lambda x: x[1], reverse=True):
        print(f"  [{s:>3}] {p['name']}")

    print("\n## Dead")
    for p, s in dead:
        print(f"  [{s:>3}] {p['name']}")


def cmd_daily(args):
    today = datetime.now().strftime("%Y-%m-%d")
    print(f"---")
    print(f"type: log")
    print(f"created: {today}")
    print(f"tags:")
    print(f"  - log")
    print(f"  - daily")
    print(f"---")
    print(f"# {today}\n")
    print(f"## What I Did\n- \n")
    print(f"## What Worked\n- \n")
    print(f"## What Didn't\n- \n")
    print(f"## Proof Created\n- \n")
    print(f"## Tomorrow\n- ")


def cmd_status(args):
    vault = find_vault(args.vault)
    projects = scan_projects(vault)
    today = datetime.now().date()
    recent_logs = scan_logs(vault, today - timedelta(days=6), today)
    scored = [(p, score_project(p)) for p in projects]
    scored.sort(key=lambda x: x[1], reverse=True)
    hot = [{"name": p["name"], "score": score} for p, score in scored[:5]]
    payload = {
        "project": "PersonalDataEngine",
        "generated_at": now_stamp(),
        "recent_log_count": len(recent_logs),
        "project_count": len(projects),
        "active_project_count": sum(1 for p in projects if p["status"] == "active"),
        "proof_project_count": sum(1 for p in projects if p["proof_count"] > 0),
        "offer_project_count": sum(1 for p in projects if p["generated_offer_count"] > 0),
        "outreach_project_count": sum(1 for p in projects if p["outreach_sends"] > 0),
        "positive_reply_project_count": sum(1 for p in projects if p["outreach_positive"] > 0),
        "top_projects": hot,
    }
    report = write_json_report(__file__, "status.json", payload)
    print(f"Project: {payload['project']}")
    print(
        f"Projects: {payload['project_count']} | Active: {payload['active_project_count']} | "
        f"With proof: {payload['proof_project_count']} | With offers: {payload['offer_project_count']}"
    )
    print(
        f"With outreach: {payload['outreach_project_count']} | "
        f"With positive replies: {payload['positive_reply_project_count']}"
    )
    print(f"Recent logs: {payload['recent_log_count']}")
    if hot:
        print(f"Top project: {hot[0]['name']} ({hot[0]['score']})")
    print(f"Report: {report}")


def main():
    parser = argparse.ArgumentParser(description="Personal Data Engine")
    sub = parser.add_subparsers(dest="command")

    review_p = sub.add_parser("review", help="Generate weekly review")
    review_p.add_argument("--vault", required=True, help="Path to Obsidian vault")
    review_p.add_argument("--week", required=True, help="Week start date (YYYY-MM-DD)")

    score_p = sub.add_parser("score", help="Score projects by momentum")
    score_p.add_argument("--vault", required=True, help="Path to Obsidian vault")

    signals_p = sub.add_parser("signals", help="Signal summary")
    signals_p.add_argument("--vault", required=True, help="Path to Obsidian vault")

    status_p = sub.add_parser("status", help="Project status snapshot")
    status_p.add_argument("--vault", required=True, help="Path to Obsidian vault")

    sub.add_parser("daily", help="Generate daily log template")

    args = parser.parse_args()
    if args.command == "review":
        cmd_review(args)
    elif args.command == "score":
        cmd_score(args)
    elif args.command == "signals":
        cmd_signals(args)
    elif args.command == "status":
        cmd_status(args)
    elif args.command == "daily":
        cmd_daily(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
