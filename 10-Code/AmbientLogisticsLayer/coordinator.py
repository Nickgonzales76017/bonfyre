"""Ambient Logistics Layer — local coordination CLI."""

import argparse
import json
import os
import sqlite3
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(__file__), "logistics.db")


def get_db():
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    db.execute("""
        CREATE TABLE IF NOT EXISTS tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            type TEXT NOT NULL,
            description TEXT NOT NULL,
            requester TEXT,
            status TEXT DEFAULT 'pending',
            created_at TEXT NOT NULL,
            completed_at TEXT
        )
    """)
    db.commit()
    return db


def add_task(args):
    db = get_db()
    db.execute(
        "INSERT INTO tasks (type, description, requester, created_at) VALUES (?, ?, ?, ?)",
        (args.type, args.description, args.requester, datetime.now().isoformat()),
    )
    db.commit()
    print(f"Task added: {args.type} — {args.description}")


def list_tasks(args):
    db = get_db()
    status_filter = args.status or "pending"
    rows = db.execute(
        "SELECT * FROM tasks WHERE status = ? ORDER BY created_at DESC", (status_filter,)
    ).fetchall()
    if not rows:
        print(f"No {status_filter} tasks.")
        return
    for r in rows:
        print(f"  [{r['id']}] {r['type']}: {r['description']} ({r['requester'] or 'anon'}) — {r['status']}")


def complete_task(args):
    db = get_db()
    db.execute(
        "UPDATE tasks SET status = 'done', completed_at = ? WHERE id = ?",
        (datetime.now().isoformat(), args.id),
    )
    db.commit()
    print(f"Task {args.id} marked done.")


def main():
    parser = argparse.ArgumentParser(description="Ambient Logistics Layer")
    sub = parser.add_subparsers(dest="command")

    add = sub.add_parser("add", help="Add a task")
    add.add_argument("type", choices=["package", "errand", "delivery", "exchange"])
    add.add_argument("description")
    add.add_argument("--requester", default=None)

    ls = sub.add_parser("list", help="List tasks")
    ls.add_argument("--status", default="pending")

    done = sub.add_parser("done", help="Complete a task")
    done.add_argument("id", type=int)

    args = parser.parse_args()
    if args.command == "add":
        add_task(args)
    elif args.command == "list":
        list_tasks(args)
    elif args.command == "done":
        complete_task(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
