"""AI + Overseas Labor Pipeline — hybrid fulfillment tracker with QA and margin reporting."""

import argparse
import sqlite3
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[1] / "_shared"))

from bonfyre_toolkit import data_path, now_stamp, write_json_report


DB_PATH = data_path(__file__, "pipeline.db")


def get_db():
    db = sqlite3.connect(str(DB_PATH))
    db.row_factory = sqlite3.Row
    db.execute("""CREATE TABLE IF NOT EXISTS jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        input_file TEXT NOT NULL,
        job_type TEXT NOT NULL DEFAULT 'transcription',
        created_at TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending',
        ai_cost REAL DEFAULT 0.0,
        review_cost REAL DEFAULT 0.0,
        sell_price REAL DEFAULT 0.0,
        reviewer TEXT,
        qa_result TEXT,
        qa_notes TEXT,
        reviewed_at TEXT,
        output_file TEXT
    )""")
    columns = {row["name"] for row in db.execute("PRAGMA table_info(jobs)").fetchall()}
    extra_columns = {
        "source_system": "TEXT",
        "source_job_id": "INTEGER",
        "source_service": "TEXT",
    }
    for column_name, column_type in extra_columns.items():
        if column_name not in columns:
            try:
                db.execute(f"ALTER TABLE jobs ADD COLUMN {column_name} {column_type}")
            except sqlite3.OperationalError as error:
                if "duplicate column name" not in str(error).lower():
                    raise
    db.commit()
    return db


def create_job(args):
    db = get_db()
    now = now_stamp()

    # Estimate AI cost based on file size
    input_path = Path(args.input)
    if not input_path.exists():
        print(f"Input not found: {args.input}")
        return

    size_kb = input_path.stat().st_size / 1024
    ai_cost = round(max(0.50, size_kb * 0.01), 2)  # rough estimate

    # Default sell prices by type
    sell_prices = {
        "transcription": 15.00,
        "summary": 20.00,
        "full-deliverable": 35.00,
    }
    sell_price = args.sell_price if args.sell_price is not None else sell_prices.get(args.type, 15.00)

    db.execute(
        """INSERT INTO jobs (
            input_file, job_type, created_at, ai_cost, sell_price, source_system, source_job_id, source_service
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            str(input_path),
            args.type,
            now,
            ai_cost,
            sell_price,
            args.source_system or "",
            args.source_job_id,
            args.service_name or "",
        ),
    )
    db.commit()
    job_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]
    print(f"Created job #{job_id}: {args.type} — {input_path.name}")
    print(f"  AI cost estimate: ${ai_cost:.2f}")
    print(f"  Sell price: ${sell_price:.2f}")
    print(f"  Status: pending review")
    if args.source_system:
        source_bits = [args.source_system]
        if args.source_job_id is not None:
            source_bits.append(f"job #{args.source_job_id}")
        if args.service_name:
            source_bits.append(args.service_name)
        print(f"  Source: {' | '.join(source_bits)}")


def list_jobs(args):
    db = get_db()
    status_filter = args.status
    if status_filter:
        rows = db.execute("SELECT * FROM jobs WHERE status = ? ORDER BY created_at DESC", (status_filter,)).fetchall()
    else:
        rows = db.execute("SELECT * FROM jobs ORDER BY created_at DESC").fetchall()

    if not rows:
        print("No jobs found.")
        return

    print(f"{'ID':<5} {'Type':<18} {'Status':<10} {'QA':<8} {'Reviewer':<12} {'Margin':<10} {'File'}")
    print("-" * 85)
    for r in rows:
        total_cost = (r["ai_cost"] or 0) + (r["review_cost"] or 0)
        margin = (r["sell_price"] or 0) - total_cost
        qa = r["qa_result"] or "-"
        reviewer = r["reviewer"] or "-"
        filename = Path(r["input_file"]).name
        source_label = ""
        if r["source_system"]:
            source_label = f" [{r['source_system']}"
            if r["source_job_id"] is not None:
                source_label += f":{r['source_job_id']}"
            source_label += "]"
        print(f"{r['id']:<5} {r['job_type']:<18} {r['status']:<10} {qa:<8} {reviewer:<12} ${margin:<9.2f} {filename}{source_label}")


def review_job(args):
    db = get_db()
    job = db.execute("SELECT * FROM jobs WHERE id = ?", (args.job_id,)).fetchone()
    if not job:
        print(f"Job #{args.job_id} not found.")
        return

    now = now_stamp()

    # Default review cost estimate
    review_cost = args.cost if args.cost else 2.00

    new_status = "completed" if args.result == "pass" else "needs-rework"

    db.execute(
        """UPDATE jobs SET
            status = ?,
            qa_result = ?,
            qa_notes = ?,
            reviewer = ?,
            review_cost = ?,
            reviewed_at = ?
           WHERE id = ?""",
        (new_status, args.result, args.notes or "", args.reviewer, review_cost, now, args.job_id),
    )
    db.commit()
    print(f"Job #{args.job_id}: {args.result.upper()} by {args.reviewer}")
    if args.notes:
        print(f"  Notes: {args.notes}")
    print(f"  Review cost: ${review_cost:.2f}")
    print(f"  Status: {new_status}")


def margin_report(args):
    db = get_db()
    rows = db.execute("SELECT * FROM jobs WHERE status = 'completed' ORDER BY created_at DESC").fetchall()

    if not rows:
        print("No completed jobs to report on.")
        return

    total_revenue = 0
    total_ai_cost = 0
    total_review_cost = 0

    print(f"\n{'ID':<5} {'Type':<18} {'Sell':<8} {'AI':<8} {'Review':<8} {'Margin':<8} {'Margin%'}")
    print("-" * 65)

    for r in rows:
        sell = r["sell_price"] or 0
        ai = r["ai_cost"] or 0
        review = r["review_cost"] or 0
        margin = sell - ai - review
        pct = (margin / sell * 100) if sell > 0 else 0

        total_revenue += sell
        total_ai_cost += ai
        total_review_cost += review

        print(f"{r['id']:<5} {r['job_type']:<18} ${sell:<7.2f} ${ai:<7.2f} ${review:<7.2f} ${margin:<7.2f} {pct:.0f}%")

    total_margin = total_revenue - total_ai_cost - total_review_cost
    total_pct = (total_margin / total_revenue * 100) if total_revenue > 0 else 0

    print("-" * 65)
    print(f"{'TOTAL':<24} ${total_revenue:<7.2f} ${total_ai_cost:<7.2f} ${total_review_cost:<7.2f} ${total_margin:<7.2f} {total_pct:.0f}%")
    print(f"\n  Jobs: {len(rows)} | Avg margin: ${total_margin / len(rows):.2f}/job")


def status_report(args):
    db = get_db()
    rows = db.execute("SELECT * FROM jobs ORDER BY created_at DESC").fetchall()
    counts = {"pending": 0, "completed": 0, "needs-rework": 0}
    by_type = {}
    for row in rows:
        status = row["status"]
        counts[status] = counts.get(status, 0) + 1
        job_type = row["job_type"]
        by_type[job_type] = by_type.get(job_type, 0) + 1

    total_sell = sum((row["sell_price"] or 0) for row in rows)
    total_cost = sum((row["ai_cost"] or 0) + (row["review_cost"] or 0) for row in rows)
    payload = {
        "project": "AIOverseasLaborPipeline",
        "total_jobs": len(rows),
        "counts": counts,
        "job_types": by_type,
        "source_counts": {},
        "estimated_revenue": round(total_sell, 2),
        "estimated_cost": round(total_cost, 2),
        "estimated_margin": round(total_sell - total_cost, 2),
    }
    for row in rows:
        source = row["source_system"] or "direct"
        payload["source_counts"][source] = payload["source_counts"].get(source, 0) + 1
    report = write_json_report(__file__, "status.json", payload)
    print(f"Project: {payload['project']}")
    print(f"Jobs: {payload['total_jobs']} | Pending: {counts.get('pending', 0)} | Completed: {counts.get('completed', 0)} | Rework: {counts.get('needs-rework', 0)}")
    if payload["source_counts"]:
        top_source = max(payload["source_counts"], key=payload["source_counts"].get)
        print(f"Top source: {top_source} ({payload['source_counts'][top_source]})")
    print(f"Estimated margin: ${payload['estimated_margin']:.2f}")
    print(f"Report: {report}")


def main():
    parser = argparse.ArgumentParser(description="AI + Overseas Labor Pipeline")
    sub = parser.add_subparsers(dest="command")

    create_p = sub.add_parser("create", help="Create a review job from AI output")
    create_p.add_argument("--input", required=True, help="Path to AI output file")
    create_p.add_argument("--type", default="transcription", choices=["transcription", "summary", "full-deliverable"])
    create_p.add_argument("--sell-price", type=float, help="Override default sell price")
    create_p.add_argument("--source-system", help="Upstream project/source of the job")
    create_p.add_argument("--source-job-id", type=int, help="Upstream job id")
    create_p.add_argument("--service-name", help="Upstream service or offer name")

    list_p = sub.add_parser("list", help="List jobs")
    list_p.add_argument("--status", choices=["pending", "completed", "needs-rework"])

    review_p = sub.add_parser("review", help="Record a QA review")
    review_p.add_argument("--job-id", required=True, type=int)
    review_p.add_argument("--result", required=True, choices=["pass", "fail"])
    review_p.add_argument("--reviewer", required=True)
    review_p.add_argument("--notes", default="")
    review_p.add_argument("--cost", type=float, help="Review cost override")

    sub.add_parser("margin", help="Margin report for completed jobs")
    sub.add_parser("status", help="Project status snapshot")

    args = parser.parse_args()

    if args.command == "create":
        create_job(args)
    elif args.command == "list":
        list_jobs(args)
    elif args.command == "review":
        review_job(args)
    elif args.command == "margin":
        margin_report(args)
    elif args.command == "status":
        status_report(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
