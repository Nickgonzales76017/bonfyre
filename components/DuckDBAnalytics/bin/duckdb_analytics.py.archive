#!/usr/bin/env python3
"""DuckDBAnalytics — local analytics over artifact manifests and transcripts.

Usage:
  duckdb_analytics.py query <sql> [--data DIR] [--dry-run]
  duckdb_analytics.py family-stats <artifact.json> [--dry-run]
  duckdb_analytics.py bench-compare <dir1> <dir2> [--dry-run]
"""
import argparse
import json
import sys
from pathlib import Path


def build_parser():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd")

    q = sub.add_parser("query")
    q.add_argument("sql")
    q.add_argument("--data", type=Path, default=None)
    q.add_argument("--dry-run", action="store_true")

    fs = sub.add_parser("family-stats")
    fs.add_argument("manifest", type=Path)
    fs.add_argument("--dry-run", action="store_true")

    bc = sub.add_parser("bench-compare")
    bc.add_argument("dir1", type=Path)
    bc.add_argument("dir2", type=Path)
    bc.add_argument("--dry-run", action="store_true")
    return p


def family_stats_native(manifest_path: Path):
    """Compute family stats without DuckDB (pure Python fallback)."""
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    atoms = manifest.get("atoms", [])
    ops = manifest.get("operators", [])
    reals = manifest.get("realizations", [])
    targets = manifest.get("realization_targets", [])
    total_bytes = sum(a.get("byte_size", 0) for a in atoms) + sum(r.get("byte_size", 0) for r in reals)
    pinned = sum(1 for r in reals if r.get("pinned"))
    print(f"Family: {manifest.get('artifact_id', '?')}")
    print(f"  Atoms:           {len(atoms)}")
    print(f"  Operators:       {len(ops)}")
    print(f"  Realizations:    {len(reals)} ({pinned} pinned)")
    print(f"  Targets:         {len(targets)}")
    print(f"  Total bytes:     {total_bytes}")
    print(f"  Reuse potential: {len(targets)} unpinned targets can be derived")


def bench_compare_native(dir1: Path, dir2: Path):
    """Compare two directories of artifacts (naive vs family storage)."""
    def dir_size(d: Path) -> int:
        return sum(f.stat().st_size for f in d.rglob("*") if f.is_file())
    s1 = dir_size(dir1)
    s2 = dir_size(dir2)
    print(f"Dir A: {dir1}  -> {s1} bytes ({s1/1024:.1f} KB)")
    print(f"Dir B: {dir2}  -> {s2} bytes ({s2/1024:.1f} KB)")
    if s1 > 0:
        ratio = s2 / s1
        print(f"Ratio (B/A): {ratio:.3f}  ({(1-ratio)*100:.1f}% {'smaller' if ratio < 1 else 'larger'})")


def main():
    args = build_parser().parse_args()
    if not args.cmd:
        print("Usage: duckdb_analytics.py <query|family-stats|bench-compare> ...")
        return 1

    if args.dry_run:
        print(f"Would run: {args.cmd}")
        return 0

    if args.cmd == "query":
        try:
            import duckdb
            conn = duckdb.connect()
            if args.data:
                conn.execute(f"CREATE TABLE data AS SELECT * FROM read_json_auto('{args.data}/**/*.json')")
            result = conn.execute(args.sql).fetchall()
            for row in result:
                print(row)
        except ImportError:
            print("DuckDB not installed. Install: pip install duckdb")
            print(f"Would execute: {args.sql}")
            return 2

    elif args.cmd == "family-stats":
        family_stats_native(args.manifest)

    elif args.cmd == "bench-compare":
        bench_compare_native(args.dir1, args.dir2)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
