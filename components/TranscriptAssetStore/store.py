"""Transcript Asset Store — index and retrieve completed transcription jobs."""

import argparse
import json
import os
import shutil
from datetime import datetime

INDEX_FILE = "index.json"


class AssetStore:
    def __init__(self, store_dir):
        self.store_dir = store_dir
        self.index_path = os.path.join(store_dir, INDEX_FILE)
        os.makedirs(store_dir, exist_ok=True)
        self.index = self._load_index()

    def _load_index(self):
        if os.path.exists(self.index_path):
            with open(self.index_path) as f:
                return json.load(f)
        return []

    def _save_index(self):
        with open(self.index_path, "w") as f:
            json.dump(self.index, f, indent=2)

    def save(self, job_dir):
        """Copy a completed job into the asset store and index it."""
        name = os.path.basename(job_dir.rstrip("/"))
        dest = os.path.join(self.store_dir, name)

        if os.path.exists(dest):
            print(f"Already stored: {name}")
            return

        shutil.copytree(job_dir, dest)

        # Read metadata if available
        meta = {}
        manifest_path = os.path.join(dest, "manifest.json")
        if os.path.exists(manifest_path):
            with open(manifest_path) as f:
                meta = json.load(f)

        entry = {
            "id": name,
            "job_title": meta.get("jobTitle", name),
            "client_name": meta.get("clientName", ""),
            "stored_at": datetime.now().isoformat(),
            "created_at": meta.get("createdAt", ""),
            "path": dest,
        }
        self.index.append(entry)
        self._save_index()
        print(f"Stored: {name}")

    def search(self, query):
        """Search index by keyword across title, client, and id."""
        q = query.lower()
        return [
            e for e in self.index
            if q in e.get("job_title", "").lower()
            or q in e.get("client_name", "").lower()
            or q in e.get("id", "").lower()
        ]

    def list_entries(self, since=None):
        """List all entries, optionally filtered by date."""
        entries = self.index
        if since:
            entries = [e for e in entries if e.get("stored_at", "") >= since]
        return sorted(entries, key=lambda e: e.get("stored_at", ""), reverse=True)


def main():
    parser = argparse.ArgumentParser(description="Transcript Asset Store")
    sub = parser.add_subparsers(dest="command")

    save_cmd = sub.add_parser("save", help="Store a completed job")
    save_cmd.add_argument("job_dir", help="Path to job output directory")

    search_cmd = sub.add_parser("search", help="Search stored assets")
    search_cmd.add_argument("query", help="Search keyword")

    list_cmd = sub.add_parser("list", help="List stored assets")
    list_cmd.add_argument("--since", help="Filter by date (YYYY-MM-DD)")

    parser.add_argument("--store", default="assets", help="Asset store directory")
    args = parser.parse_args()

    store = AssetStore(args.store)

    if args.command == "save":
        store.save(args.job_dir)
    elif args.command == "search":
        results = store.search(args.query)
        if not results:
            print("No matches.")
        for r in results:
            print(f"  [{r['id']}] {r['job_title']} — {r['client_name'] or 'no client'} — {r['stored_at'][:10]}")
    elif args.command == "list":
        entries = store.list_entries(args.since)
        if not entries:
            print("No assets stored yet.")
        for e in entries:
            print(f"  [{e['id']}] {e['job_title']} — {e['stored_at'][:10]}")
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
