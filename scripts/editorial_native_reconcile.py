#!/usr/bin/env python3
"""Review-gated, resumable Journal draft admission into native BonfyreCMS.

Default is read-only. Never publishes, edits an existing row, or repairs a
conflicting record by guessing. A missing row is created through BonfyreCMS,
not by writing to its SQLite tables. The exact plan must be acknowledged.
"""
import argparse
import csv
from hashlib import sha256
import json
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys

PAGE_KEY = "route"
FRAGMENT_KEY = "coordinate"
NAMESPACE = "aurekai-journal"
JSON_FIELDS = {"semantic_tags", "projection_contract"}


class Held(Exception):
    pass


def rows(path, delimiter=","):
    with path.open(encoding="utf-8", newline="") as fp:
        reader = csv.DictReader(fp, delimiter=delimiter)
        if not reader.fieldnames or len(reader.fieldnames) != len(set(reader.fieldnames)):
            raise Held("Missing or duplicated headers: " + path.name)
        result = list(reader)
    if not result or any(None in row or any(v is None for v in row.values()) for row in result):
        raise Held("Empty or invalid rows: " + path.name)
    return result


def typed_record(row):
    record = {}
    for field, value in row.items():
        if field == "namespace" or value == "":
            continue
        if field == "position":
            if not value.isdecimal():
                raise Held("Invalid position")
            record[field] = int(value)
        elif field in JSON_FIELDS:
            try:
                record[field] = json.loads(value)
            except ValueError as exc:
                raise Held("Invalid " + field) from exc
        else:
            record[field] = value
    return record


def validate_bundle(root, bundle):
    pages = rows(bundle / "cms-site-page.csv")
    fragments = rows(bundle / "cms-site-fragment.csv")
    receipts = rows(bundle / "source-receipts.tsv", "\t")
    for kind, items, key in (("site_page", pages, PAGE_KEY),
                             ("site_fragment", fragments, FRAGMENT_KEY)):
        identities = [row.get(key, "") for row in items]
        if not all(identities) or len(set(identities)) != len(items):
            raise Held("Missing or duplicated " + kind + " identity")
        if any(row.get("namespace") != NAMESPACE or row.get("status") != "draft" for row in items):
            raise Held("Unsafe namespace or publication state in " + kind)
        for row in items:
            typed_record(row)
    page_routes = {p[PAGE_KEY] for p in pages}
    if any(f["page"] not in page_routes or f.get("audience") != "public" for f in fragments):
        raise Held("Orphaned or nonpublic fragment")
    source_digests = {}
    for frag in fragments:
        ref = frag.get("source_ref", "")
        path = Path(ref)
        if not ref.startswith("editorial/") or path.is_absolute() or ".." in path.parts:
            raise Held("Source path escaped editorial root: " + ref)
        source = root / path
        if not source.is_file() or source.is_symlink():
            raise Held("Missing or symlinked source: " + ref)
        digest = source_digests.setdefault(ref, sha256(source.read_bytes()).hexdigest())
        if frag.get("source_digest") != digest:
            raise Held("Stale source digest: " + ref)
    identities = set()
    for receipt in receipts:
        identity = receipt.get("subject_id", "")
        ref = receipt.get("source_ref", "")
        if not identity or identity in identities or receipt.get("status") != "draft":
            raise Held("Invalid source receipt")
        identities.add(identity)
        if ref not in source_digests or receipt.get("source_sha256") != source_digests[ref]:
            raise Held("Source receipt has unverified bytes: " + identity)
    articles = [p for p in pages if p.get("editorial_kind") == "article"]
    for page in articles:
        slug = page["route"].removeprefix("/blog/").strip("/")
        if not slug or page["route"] != "/blog/" + slug + "/":
            raise Held("Noncanonical article route")
        related = [f for f in fragments if f["page"] == page["route"]]
        for kind in ("article-body", "article-credit", "article-presentation"):
            if sum(f["kind"] == kind for f in related) != 1:
                raise Held("Missing or repeated " + kind + " for " + slug)
        selection = next(f for f in related if f["kind"] == "article-presentation")
        contract = typed_record(selection).get("projection_contract")
        if not isinstance(contract, dict) or contract.get("review_required") is not True or \
           contract.get("subject_id") != "post:" + slug or \
           contract.get("authored_source_sha256") != source_digests.get("editorial/posts/" + slug + ".md"):
            raise Held("Unreviewed or unbound art direction: " + slug)
        if "post:" + slug not in identities:
            raise Held("Article has no original-source receipt: " + slug)
    if len(receipts) != len(pages):
        raise Held("Source receipt / page count differs")
    return (("site_page", PAGE_KEY, pages), ("site_fragment", FRAGMENT_KEY, fragments))


def db_rows(db_path, kind, key):
    # SQLite URI read-only, even for the default report mode. Do not set WAL.
    conn = sqlite3.connect(db_path.resolve().as_uri() + "?mode=ro", uri=True, timeout=3)
    try:
        conn.row_factory = sqlite3.Row
        columns = {row[1] for row in conn.execute("PRAGMA table_info(" + kind + ")")}
        if not {"namespace", "status", key}.issubset(columns):
            raise Held("CMS table unavailable or incompatible: " + kind)
        return [dict(row) for row in conn.execute("SELECT * FROM " + kind + " WHERE namespace=?", (NAMESPACE,))]
    finally:
        conn.close()


def same_record(candidate, actual):
    prepared = typed_record(candidate)
    for key, raw in candidate.items():
        if key == "namespace":
            continue
        expected = prepared.get(key)
        if raw == "":
            if actual.get(key) not in (None, ""):
                return False
            continue
        real = actual.get(key)
        if key in JSON_FIELDS and real is not None:
            try:
                real = json.loads(real) if isinstance(real, str) else real
            except ValueError:
                return False
        elif key == "position":
            try:
                real = int(real)
            except (ValueError, TypeError):
                return False
        if real != expected:
            return False
    return True


def create_plan(db_path, groups):
    plan = []
    for kind, key, expected in groups:
        current = {}
        for row in db_rows(db_path, kind, key):
            value = row[key]
            current.setdefault(value, []).append(row)
        for candidate in expected:
            existing = current.get(candidate[key], [])
            if len(existing) > 1:
                action = "DUPLICATE"
            elif not existing:
                action = "MISSING"
            elif existing[0]["status"] != "draft":
                action = "PROTECTED"
            elif same_record(candidate, existing[0]):
                action = "IDENTICAL"
            else:
                action = "DRIFT"
            plan.append((kind, candidate[key], action, candidate))
    # Bind to both the exact artifact contents and current canonical state.
    stable = [(k, identity, action, typed_record(data)) for k, identity, action, data in plan]
    digest = sha256(json.dumps(stable, ensure_ascii=False, sort_keys=True,
                               separators=(",", ":")).encode()).hexdigest()
    return plan, digest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("."))
    parser.add_argument("--bundle", type=Path, default=Path("build/publication"))
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--schemas", type=Path, default=Path("cmd/BonfyreCMS/content-types"))
    parser.add_argument("--cms", type=Path, default=Path("cmd/BonfyreCMS/bonfyre-cms"))
    parser.add_argument("--apply-drafts", action="store_true")
    parser.add_argument("--expected-plan", default="")
    parser.add_argument("--batch-size", type=int, default=50)
    parser.add_argument("--minimum-free-mib", type=int, default=2048)
    args = parser.parse_args()
    try:
        groups = validate_bundle(args.root.resolve(), args.bundle.resolve())
        plan, digest = create_plan(args.db, groups)
        counts = {state: sum(r[2] == state for r in plan) for state in
                  ("MISSING", "IDENTICAL", "DRIFT", "PROTECTED", "DUPLICATE")}
        print("NATIVE_JOURNAL_DRAFT_PLAN\t" + digest)
        print("\t".join(f"{state}={num}" for state, num in counts.items()))
        for kind, identity, state, _ in plan:
            if state not in ("IDENTICAL",):
                print("\t".join((state, kind, identity)))
        if not args.apply_drafts:
            return 0 if not any(counts[s] for s in ("DRIFT", "PROTECTED", "DUPLICATE")) else 2
        if args.expected_plan != digest or not args.expected_plan:
            raise Held("Apply refused: exact current plan digest is required")
        if any(counts[s] for s in ("DRIFT", "PROTECTED", "DUPLICATE")):
            raise Held("Apply refused: existing record needs owner review")
        if not 1 <= args.batch_size <= 100 or args.minimum_free_mib < 0:
            raise Held("Invalid bounded resource settings")
        free = shutil.disk_usage(args.db.parent).free
        if free < args.minimum_free_mib * 1024 * 1024:
            raise Held(f"Apply refused: free space {free} bytes below reservation")
        if not args.cms.is_file() or not args.schemas.is_dir():
            raise Held("Native CMS binary or schema directory missing")
        written = 0
        for kind, identity, state, row in plan:
            if state != "MISSING" or written == args.batch_size:
                continue
            if shutil.disk_usage(args.db.parent).free < args.minimum_free_mib * 1024 * 1024:
                raise Held("Draft admission stopped at free-space reservation")
            record = json.dumps(typed_record(row), ensure_ascii=False, separators=(",", ":"))
            if len(record.encode()) > 256 * 1024:
                raise Held("CMS admission exceeds per-record bound: " + identity)
            # Refuse concurrent arrival, rather than overwrite or duplicate.
            existing = db_rows(args.db, kind, PAGE_KEY if kind == "site_page" else FRAGMENT_KEY)
            if any(x.get(PAGE_KEY if kind == "site_page" else FRAGMENT_KEY) == identity for x in existing):
                raise Held("Concurrent record changed: " + identity)
            cmd = [str(args.cms.resolve()), "entry", "create", kind, record, "--ns", NAMESPACE,
                   "--db", str(args.db.resolve()), "--schemas", str(args.schemas.resolve())]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            if result.returncode:
                raise Held("Native CMS rejected draft " + kind + ":" + identity + " (" + result.stderr[-300:] + ")")
            written += 1
            print("CREATED\t" + kind + "\t" + identity)
        print(f"DRAFT_ADMISSION\twritten={written}\tremaining={counts['MISSING']-written}\tpublication=held")
        return 0
    except (Held, sqlite3.Error, OSError, subprocess.TimeoutExpired) as exc:
        print("HELD\t" + str(exc), file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
