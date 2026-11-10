"""Service Arbitrage Hub — track services, margins, and arbitrage opportunities."""

import argparse
import sqlite3
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[1] / "_shared"))

from bonfyre_toolkit import data_path, now_stamp, write_json_report


DB_PATH = data_path(__file__, "arbitrage.db")
PIPELINE_DB_PATH = Path(__file__).resolve().parents[1] / "AIOverseasLaborPipeline" / "data" / "pipeline.db"


def get_db():
    db = sqlite3.connect(str(DB_PATH))
    db.row_factory = sqlite3.Row
    db.execute("""CREATE TABLE IF NOT EXISTS services (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE,
        category TEXT NOT NULL DEFAULT 'general',
        buy_price REAL NOT NULL,
        sell_price REAL NOT NULL,
        source TEXT NOT NULL,
        quality_notes TEXT DEFAULT '',
        created_at TEXT NOT NULL
    )""")
    db.execute("""CREATE TABLE IF NOT EXISTS jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        service_name TEXT NOT NULL,
        actual_cost REAL NOT NULL,
        sold_for REAL NOT NULL,
        quality TEXT NOT NULL DEFAULT 'pass',
        notes TEXT DEFAULT '',
        completed_at TEXT NOT NULL
    )""")
    columns = {row["name"] for row in db.execute("PRAGMA table_info(jobs)").fetchall()}
    if "handoff_pipeline_job_id" not in columns:
        try:
            db.execute("ALTER TABLE jobs ADD COLUMN handoff_pipeline_job_id INTEGER")
        except sqlite3.OperationalError as error:
            if "duplicate column name" not in str(error).lower():
                raise
    db.commit()
    return db


def get_pipeline_db():
    db = sqlite3.connect(str(PIPELINE_DB_PATH))
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
        output_file TEXT,
        source_system TEXT,
        source_job_id INTEGER,
        source_service TEXT
    )""")
    pipeline_columns = {row["name"] for row in db.execute("PRAGMA table_info(jobs)").fetchall()}
    extra_columns = {
        "source_system": "TEXT",
        "source_job_id": "INTEGER",
        "source_service": "TEXT",
    }
    for column_name, column_type in extra_columns.items():
        if column_name not in pipeline_columns:
            db.execute(f"ALTER TABLE jobs ADD COLUMN {column_name} {column_type}")
    db.commit()
    return db


def cmd_add(args):
    db = get_db()
    now = now_stamp()
    margin = args.sell_price - args.buy_price
    margin_pct = (margin / args.sell_price * 100) if args.sell_price > 0 else 0

    try:
        db.execute(
            "INSERT INTO services (name, category, buy_price, sell_price, source, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (args.name, args.category, args.buy_price, args.sell_price, args.source, now),
        )
        db.commit()
    except sqlite3.IntegrityError:
        db.execute(
            "UPDATE services SET category=?, buy_price=?, sell_price=?, source=? WHERE name=?",
            (args.category, args.buy_price, args.sell_price, args.source, args.name),
        )
        db.commit()
        print(f"Updated: {args.name}")
        return

    print(f"Added: {args.name}")
    print(f"  Buy:    ${args.buy_price:.2f}")
    print(f"  Sell:   ${args.sell_price:.2f}")
    print(f"  Margin: ${margin:.2f} ({margin_pct:.0f}%)")
    print(f"  Source: {args.source}")


def cmd_list(args):
    db = get_db()
    rows = db.execute("SELECT * FROM services ORDER BY sell_price - buy_price DESC").fetchall()

    if not rows:
        print("No services tracked yet. Use 'add' to add one.")
        return

    print(f"{'Service':<28} {'Buy':<8} {'Sell':<8} {'Margin':<8} {'%':<6} {'Source'}")
    print("-" * 72)
    for r in rows:
        margin = r["sell_price"] - r["buy_price"]
        pct = (margin / r["sell_price"] * 100) if r["sell_price"] > 0 else 0
        print(f"{r['name']:<28} ${r['buy_price']:<7.2f} ${r['sell_price']:<7.2f} ${margin:<7.2f} {pct:<5.0f}% {r['source']}")


def cmd_job(args):
    db = get_db()
    now = now_stamp()

    # Verify service exists
    svc = db.execute("SELECT * FROM services WHERE name = ?", (args.service,)).fetchone()
    if not svc:
        print(f"Service not found: {args.service}")
        print("Use 'list' to see tracked services or 'add' to create one.")
        return

    margin = args.sold_for - args.actual_cost
    margin_pct = (margin / args.sold_for * 100) if args.sold_for > 0 else 0

    db.execute(
        "INSERT INTO jobs (service_name, actual_cost, sold_for, quality, notes, completed_at) VALUES (?, ?, ?, ?, ?, ?)",
        (args.service, args.actual_cost, args.sold_for, args.quality, args.notes or "", now),
    )
    db.commit()

    print(f"Job logged: {args.service}")
    print(f"  Cost:    ${args.actual_cost:.2f}")
    print(f"  Sold:    ${args.sold_for:.2f}")
    print(f"  Margin:  ${margin:.2f} ({margin_pct:.0f}%)")
    print(f"  Quality: {args.quality}")


def cmd_report(args):
    db = get_db()
    services = db.execute("SELECT DISTINCT service_name FROM jobs").fetchall()

    if not services:
        print("No jobs logged yet.")
        return

    print(f"{'Service':<28} {'Jobs':<6} {'Revenue':<10} {'Cost':<10} {'Margin':<10} {'Avg %':<8} {'QA Fail'}")
    print("-" * 85)

    total_rev = 0
    total_cost = 0

    for svc in services:
        name = svc["service_name"]
        jobs = db.execute("SELECT * FROM jobs WHERE service_name = ?", (name,)).fetchall()
        revenue = sum(j["sold_for"] for j in jobs)
        cost = sum(j["actual_cost"] for j in jobs)
        margin = revenue - cost
        avg_pct = (margin / revenue * 100) if revenue > 0 else 0
        fails = sum(1 for j in jobs if j["quality"] == "fail")

        total_rev += revenue
        total_cost += cost

        print(f"{name:<28} {len(jobs):<6} ${revenue:<9.2f} ${cost:<9.2f} ${margin:<9.2f} {avg_pct:<7.0f}% {fails}")

    total_margin = total_rev - total_cost
    total_pct = (total_margin / total_rev * 100) if total_rev > 0 else 0
    print("-" * 85)
    print(f"{'TOTAL':<35} ${total_rev:<9.2f} ${total_cost:<9.2f} ${total_margin:<9.2f} {total_pct:.0f}%")


def cmd_handoff(args):
    db = get_db()
    job = db.execute("SELECT * FROM jobs WHERE id = ?", (args.job_id,)).fetchone()
    if not job:
        print(f"Job #{args.job_id} not found.")
        return
    if job["handoff_pipeline_job_id"]:
        print(f"Job #{args.job_id} already handed off as pipeline job #{job['handoff_pipeline_job_id']}.")
        return

    pipeline_db = get_pipeline_db()
    now = now_stamp()
    input_ref = f"arbitrage://{job['service_name']}/job-{job['id']}"
    qa_seed = "pass" if job["quality"] == "pass" else ""
    qa_notes = f"Created from ServiceArbitrageHub job #{job['id']}"
    if job["notes"]:
        qa_notes = f"{qa_notes} | {job['notes']}"

    pipeline_db.execute(
        """INSERT INTO jobs (
            input_file, job_type, created_at, status, ai_cost, review_cost, sell_price,
            qa_result, qa_notes, source_system, source_job_id, source_service
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            input_ref,
            args.type,
            now,
            "pending",
            job["actual_cost"],
            0.0,
            job["sold_for"],
            qa_seed,
            qa_notes,
            "ServiceArbitrageHub",
            job["id"],
            job["service_name"],
        ),
    )
    pipeline_db.commit()
    pipeline_job_id = pipeline_db.execute("SELECT last_insert_rowid()").fetchone()[0]

    db.execute(
        "UPDATE jobs SET handoff_pipeline_job_id = ? WHERE id = ?",
        (pipeline_job_id, job["id"]),
    )
    db.commit()

    print(f"Handed off arbitrage job #{job['id']} -> pipeline job #{pipeline_job_id}")
    print(f"  Service: {job['service_name']}")
    print(f"  Type: {args.type}")
    print(f"  Sell price: ${job['sold_for']:.2f}")


def cmd_opportunities(args):
    db = get_db()
    rows = db.execute("SELECT * FROM services ORDER BY (sell_price - buy_price) / sell_price DESC").fetchall()

    if not rows:
        print("No services tracked yet.")
        return

    print("## Best Arbitrage Opportunities\n")
    print(f"{'#':<4} {'Service':<28} {'Spread':<10} {'Margin%':<10} {'Source'}")
    print("-" * 60)
    for i, r in enumerate(rows, 1):
        spread = r["sell_price"] - r["buy_price"]
        pct = (spread / r["sell_price"] * 100) if r["sell_price"] > 0 else 0
        print(f"{i:<4} {r['name']:<28} ${spread:<9.2f} {pct:<9.0f}% {r['source']}")


def cmd_status(args):
    db = get_db()
    services = db.execute("SELECT * FROM services").fetchall()
    jobs = db.execute("SELECT * FROM jobs").fetchall()
    best = None
    best_pct = -1.0
    for row in services:
        spread = row["sell_price"] - row["buy_price"]
        pct = (spread / row["sell_price"]) if row["sell_price"] > 0 else 0.0
        if pct > best_pct:
            best_pct = pct
            best = row
    payload = {
        "project": "ServiceArbitrageHub",
        "service_count": len(services),
        "job_count": len(jobs),
        "handoff_count": sum(1 for job in jobs if job["handoff_pipeline_job_id"]),
        "best_service": best["name"] if best else None,
        "best_margin_pct": round(best_pct * 100, 1) if best else None,
    }
    report = write_json_report(__file__, "status.json", payload)
    print(f"Project: {payload['project']}")
    print(f"Services: {payload['service_count']} | Jobs: {payload['job_count']} | Handed off: {payload['handoff_count']}")
    if best:
        print(f"Best spread: {best['name']} ({payload['best_margin_pct']:.1f}%)")
    print(f"Report: {report}")


def main():
    parser = argparse.ArgumentParser(description="Service Arbitrage Hub")
    sub = parser.add_subparsers(dest="command")

    add_p = sub.add_parser("add", help="Add/update a service")
    add_p.add_argument("--name", required=True)
    add_p.add_argument("--buy-price", required=True, type=float)
    add_p.add_argument("--sell-price", required=True, type=float)
    add_p.add_argument("--source", required=True, help="Fulfillment source")
    add_p.add_argument("--category", default="general")

    sub.add_parser("list", help="List all services with margins")

    job_p = sub.add_parser("job", help="Log a completed job")
    job_p.add_argument("--service", required=True)
    job_p.add_argument("--actual-cost", required=True, type=float)
    job_p.add_argument("--sold-for", required=True, type=float)
    job_p.add_argument("--quality", default="pass", choices=["pass", "fail"])
    job_p.add_argument("--notes", default="")

    sub.add_parser("report", help="Margin report across services")
    sub.add_parser("opportunities", help="Best arbitrage opportunities")
    handoff_p = sub.add_parser("handoff", help="Hand off a sold job into AIOverseasLaborPipeline")
    handoff_p.add_argument("--job-id", required=True, type=int)
    handoff_p.add_argument("--type", default="full-deliverable", choices=["transcription", "summary", "full-deliverable"])
    sub.add_parser("status", help="Project status snapshot")

    args = parser.parse_args()
    cmds = {
        "add": cmd_add,
        "list": cmd_list,
        "job": cmd_job,
        "report": cmd_report,
        "opportunities": cmd_opportunities,
        "handoff": cmd_handoff,
        "status": cmd_status,
    }
    if args.command in cmds:
        cmds[args.command](args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
