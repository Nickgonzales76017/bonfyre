#!/usr/bin/env python3
"""SQLiteGraphStore — Merkle-DAG artifact graph backed by SQLite.

Usage:
  sqlite_graph.py init <db>
  sqlite_graph.py add-atom <db> --id ID --hash HASH --media-type TYPE [--path PATH]
  sqlite_graph.py add-op <db> --id ID --op OP --inputs A,B --output OUT [--params '{}'] [--version V]
  sqlite_graph.py lineage <db> --id ID
  sqlite_graph.py diff <db1> <db2>
  sqlite_graph.py export <db> [--out artifact.json]
  sqlite_graph.py --dry-run ...
"""
import argparse
import hashlib
import json
import sqlite3
import sys
from pathlib import Path


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS atoms (
    atom_id TEXT PRIMARY KEY,
    content_hash TEXT NOT NULL,
    media_type TEXT NOT NULL,
    path TEXT,
    byte_size INTEGER,
    label TEXT
);
CREATE TABLE IF NOT EXISTS operators (
    operator_id TEXT PRIMARY KEY,
    op TEXT NOT NULL,
    inputs TEXT NOT NULL,       -- JSON array of ids
    output TEXT NOT NULL,
    params TEXT DEFAULT '{}',   -- JSON
    node_hash TEXT,
    version TEXT DEFAULT '1.0.0',
    deterministic INTEGER DEFAULT 1
);
CREATE TABLE IF NOT EXISTS realizations (
    realization_id TEXT PRIMARY KEY,
    media_type TEXT NOT NULL,
    path TEXT,
    content_hash TEXT,
    byte_size INTEGER,
    pinned INTEGER DEFAULT 0,
    produced_by TEXT,
    label TEXT
);
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT
);
"""


def canonical_json(obj) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def init_db(db_path: str):
    conn = sqlite3.connect(db_path)
    conn.executescript(SCHEMA_SQL)
    conn.execute("INSERT OR IGNORE INTO meta VALUES ('schema_version', '1.0.0')")
    conn.commit()
    conn.close()
    print(f"Initialized: {db_path}")


def add_atom(db_path, atom_id, content_hash, media_type, path=None):
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT OR REPLACE INTO atoms (atom_id, content_hash, media_type, path) VALUES (?,?,?,?)",
        (atom_id, content_hash, media_type, path),
    )
    conn.commit()
    conn.close()
    print(f"Added atom: {atom_id}")


def add_op(db_path, operator_id, op, inputs_csv, output, params_json="{}", version="1.0.0"):
    inputs = [i.strip() for i in inputs_csv.split(",")]
    conn = sqlite3.connect(db_path)
    # compute node_hash
    # get child hashes
    hashes = {}
    for row in conn.execute("SELECT atom_id, content_hash FROM atoms"):
        hashes[row[0]] = row[1]
    for row in conn.execute("SELECT operator_id, node_hash FROM operators WHERE node_hash IS NOT NULL"):
        hashes[row[0]] = row[1]
    parts = {"op": op, "params": json.loads(params_json), "inputs_hashes": sorted(hashes.get(i, "") for i in inputs), "version": version}
    node_hash = sha256(canonical_json(parts))
    conn.execute(
        "INSERT OR REPLACE INTO operators (operator_id, op, inputs, output, params, node_hash, version) VALUES (?,?,?,?,?,?,?)",
        (operator_id, op, json.dumps(inputs), output, params_json, node_hash, version),
    )
    conn.commit()
    conn.close()
    print(f"Added operator: {operator_id} (hash: {node_hash[:16]}…)")


def lineage(db_path, node_id):
    conn = sqlite3.connect(db_path)
    # Walk backwards from node_id
    visited = set()
    queue = [node_id]
    while queue:
        nid = queue.pop(0)
        if nid in visited:
            continue
        visited.add(nid)
        row = conn.execute("SELECT operator_id, op, inputs, output FROM operators WHERE operator_id=? OR output=?", (nid, nid)).fetchone()
        if row:
            print(f"  op: {row[1]}  inputs: {row[2]}  output: {row[3]}")
            for inp in json.loads(row[2]):
                queue.append(inp)
        atom = conn.execute("SELECT atom_id, media_type, path FROM atoms WHERE atom_id=?", (nid,)).fetchone()
        if atom:
            print(f"  atom: {atom[0]}  type: {atom[1]}  path: {atom[2]}")
    conn.close()


def export_json(db_path, out_path):
    conn = sqlite3.connect(db_path)
    manifest = {
        "schema_version": "1.0.0",
        "artifact_id": Path(db_path).stem,
        "artifact_type": "custom",
        "atoms": [],
        "operators": [],
        "realizations": [],
    }
    for row in conn.execute("SELECT atom_id, content_hash, media_type, path, byte_size, label FROM atoms"):
        manifest["atoms"].append({k: v for k, v in zip(["atom_id", "content_hash", "media_type", "path", "byte_size", "label"], row) if v is not None})
    for row in conn.execute("SELECT operator_id, op, inputs, output, params, node_hash, version, deterministic FROM operators"):
        op = {"operator_id": row[0], "op": row[1], "inputs": json.loads(row[2]), "output": row[3]}
        if row[4] and row[4] != "{}":
            op["params"] = json.loads(row[4])
        if row[5]:
            op["node_hash"] = row[5]
        if row[6]:
            op["version"] = row[6]
        manifest["operators"].append(op)
    for row in conn.execute("SELECT realization_id, media_type, path, content_hash, byte_size, pinned, produced_by, label FROM realizations"):
        manifest["realizations"].append({k: v for k, v in zip(["realization_id", "media_type", "path", "content_hash", "byte_size", "pinned", "produced_by", "label"], row) if v is not None})
    conn.close()

    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2, ensure_ascii=False)
        fh.write("\n")
    print(f"Exported: {out_path}")


def build_parser():
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true")
    sub = p.add_subparsers(dest="cmd")

    sub.add_parser("init").add_argument("db")

    aa = sub.add_parser("add-atom")
    aa.add_argument("db")
    aa.add_argument("--id", required=True)
    aa.add_argument("--hash", required=True)
    aa.add_argument("--media-type", required=True)
    aa.add_argument("--path", default=None)

    ao = sub.add_parser("add-op")
    ao.add_argument("db")
    ao.add_argument("--id", required=True)
    ao.add_argument("--op", required=True)
    ao.add_argument("--inputs", required=True)
    ao.add_argument("--output", required=True)
    ao.add_argument("--params", default="{}")
    ao.add_argument("--version", default="1.0.0")

    li = sub.add_parser("lineage")
    li.add_argument("db")
    li.add_argument("--id", required=True)

    ex = sub.add_parser("export")
    ex.add_argument("db")
    ex.add_argument("--out", default="artifact.json")
    return p


def main():
    args = build_parser().parse_args()
    if not args.cmd:
        print("Usage: sqlite_graph.py <init|add-atom|add-op|lineage|export> ...")
        return 1
    if args.dry_run:
        print(f"Would run: {args.cmd} on {args.db}")
        return 0
    if args.cmd == "init":
        init_db(args.db)
    elif args.cmd == "add-atom":
        add_atom(args.db, args.id, args.hash, args.media_type, args.path)
    elif args.cmd == "add-op":
        add_op(args.db, args.id, args.op, args.inputs, args.output, args.params, args.version)
    elif args.cmd == "lineage":
        lineage(args.db, args.id)
    elif args.cmd == "export":
        export_json(args.db, args.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
