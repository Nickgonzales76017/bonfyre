#!/usr/bin/env python3
"""Read-only exact retrieval of a private ChatGPT ZIP through existing Bonfyre catalogs.

Original ZIP is authoritative; results are evidence leads, not Workboard mutations,
provider token accounting, or automatically verified task associations.
"""
import argparse
from collections import defaultdict
from contextlib import closing
import hashlib
import io
import json
from pathlib import Path
import sqlite3
import sys
import zipfile

PREFIX = "chatgpt-20261028-"

def refuse(message):
    raise SystemExit("REFUSED: " + message)

def database(path):
    if not path.is_file() or path.is_symlink():
        refuse("missing or symlinked catalog: " + str(path))
    return sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)

def provenance(root, names):
    with closing(database(root / (PREFIX + "member-manifest.sqlite3"))) as db:
        row = db.execute("SELECT path,size,mtime_ns,sha256 FROM archive").fetchone()
        members = db.execute("SELECT count(*) FROM member").fetchone()[0]
    if not row:
        refuse("source ZIP provenance missing")
    source, size, mtime, digest = row
    path = Path(source)
    if not path.is_file() or path.is_symlink():
        refuse("original ZIP missing or symlinked")
    stat = path.stat()
    if (stat.st_size, stat.st_mtime_ns) != (size, mtime):
        refuse("original ZIP changed since catalog creation")
    for name in names:
        with closing(database(root / (PREFIX + name + ".sqlite3"))) as db:
            value = db.execute("SELECT archive_sha256 FROM provenance").fetchone()
        if not value or value[0] != digest:
            refuse("catalog provenance mismatch: " + name)
    return path, members

def records_from_zip(z, filename, positions):
    """Stream selected 1-based source positions, no intermediate extraction."""
    if not (filename.startswith("conversations-") and filename.endswith(".json")) or "/" in filename:
        refuse("invalid conversation source path")
    outstanding = set(positions)
    output = {}
    with io.TextIOWrapper(z.open(filename), encoding="utf8") as stream:
        decoder = json.JSONDecoder()
        buffer = ""
        begun = False
        finished = False
        index = 0
        while outstanding:
            buffer = buffer.lstrip(" \r\n\t" + ("," if begun else ""))
            if not begun:
                if not buffer:
                    more = stream.read(1 << 20)
                    if not more:
                        refuse("truncated JSON array")
                    buffer += more
                    continue
                if buffer[0] != "[":
                    refuse("not a conversation array")
                buffer = buffer[1:]
                begun = True
                continue
            if buffer.startswith("]"):
                break
            try:
                record, end = decoder.raw_decode(buffer)
            except json.JSONDecodeError:
                if finished:
                    refuse("truncated conversation record")
                more = stream.read(1 << 20)
                if not more:
                    finished = True
                else:
                    buffer += more
                continue
            index += 1
            if index in outstanding:
                output[index] = record
                outstanding.remove(index)
            buffer = buffer[end:]
    if outstanding:
        refuse("source positions missing: " + repr(sorted(outstanding)))
    return output

def part_rows(root, ids):
    if not ids:
        return []
    marks = ",".join("?" for _ in ids)
    sql = ("SELECT p.id,c.source_file,c.source_position,c.title,p.conversation_id,"
           "p.message_id,p.role,p.part_index,p.characters,p.utf8_bytes "
           "FROM part p JOIN conversation c ON c.id=p.conversation_id "
           "WHERE p.id IN (" + marks + ")")
    with closing(database(root / (PREFIX + "search.sqlite3"))) as db:
        raw = db.execute(sql, ids).fetchall()
    fields = ["id","source_file","source_position","title","conversation_id",
              "message_id","role","part_index","characters","bytes"]
    return [dict(zip(fields, values)) for values in raw]

def restore_rows(zip_path, rows):
    targets = defaultdict(set)
    for row in rows:
        targets[row["source_file"]].add(row["source_position"])
    found = {}
    with zipfile.ZipFile(zip_path) as z:
        for filename, positions in targets.items():
            for n, record in records_from_zip(z, filename, positions).items():
                found[(filename, n)] = record
    return found

def source_part(record, row):
    for key, node in (record.get("mapping") or {}).items():
        msg = (node or {}).get("message") or {}
        if (msg.get("id") or "mapping:" + str(key)) != row["message_id"]:
            continue
        parts = (msg.get("content") or {}).get("parts") or []
        n = row["part_index"]
        if n < 0 or n >= len(parts) or not isinstance(parts[n], str):
            refuse("indexed part is missing in original message")
        text = parts[n]
        if len(text) != row["characters"] or len(text.encode()) != row["bytes"]:
            refuse("indexed text dimensions disagree with source")
        return text
    refuse("indexed message is missing in original conversation")

def main():
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("--root", type=Path, default=Path.home()/".bonfyre/archive-recovery")
    commands = cli.add_subparsers(dest="command", required=True)
    commands.add_parser("status")
    search = commands.add_parser("search")
    search.add_argument("query")
    search.add_argument("--limit", type=int, default=5)
    search.add_argument("--role", choices=("user","assistant","system","tool"))
    search.add_argument("--title", help="Literal case-insensitive conversation-title substring")
    show = commands.add_parser("show")
    show.add_argument("part_id", type=int)
    code = commands.add_parser("code")
    code.add_argument("part_id", type=int)
    code.add_argument("block_index", type=int)
    task = commands.add_parser("task")
    task.add_argument("goal_id")
    task.add_argument("--limit", type=int, default=25)
    args = cli.parse_args()
    limit = getattr(args, "limit", 1)
    if limit < 1 or limit > 50:
        refuse("limit must be 1 through 50")
    root = args.root.expanduser()
    needed = ["search"]
    if args.command in ("status", "code"):
        needed.append("code-references")
    if args.command in ("status", "task"):
        needed.append("goal-references")
    zip_path, member_count = provenance(root, needed)
    if args.command == "status":
        with closing(database(root / (PREFIX + "search.sqlite3"))) as db:
            conversations = db.execute("SELECT count(*) FROM conversation").fetchone()[0]
            parts = db.execute("SELECT count(*) FROM part").fetchone()[0]
        with closing(database(root / (PREFIX + "code-references.sqlite3"))) as db:
            blocks, unique = db.execute("SELECT count(*),count(DISTINCT sha256) FROM code_block").fetchone()
        with closing(database(root / (PREFIX + "goal-references.sqlite3"))) as db:
            mentions, goals = db.execute("SELECT count(*),count(DISTINCT goal_id) FROM goal_reference").fetchone()
        with closing(database(root / (PREFIX + "member-manifest.sqlite3"))) as db:
            groups, waste = db.execute("SELECT count(*),sum(waste) FROM (SELECT sum(zip_bytes)-min(zip_bytes) waste FROM member WHERE is_directory=0 AND raw_bytes>0 GROUP BY sha256,raw_bytes HAVING count(*)>1)").fetchone()
        print(json.dumps(dict(members=member_count,conversations=conversations,text_parts=parts,
              detected_code_fences=blocks,unique_fence_hashes=unique,goal_mentions=mentions,
              mentioned_goal_ids=goals,duplicate_groups=groups,approx_duplicate_zip_bytes=waste,
              actual_token_costs="unavailable from the export",task_links="unreviewed"),indent=2))
    elif args.command == "task":
        with closing(database(root / (PREFIX + "goal-references.sqlite3"))) as db:
            sql = ("SELECT goal_id,conversation_id,message_id,role,conversation_file,"
                   "occurrences,goal_state_at_scan FROM goal_reference WHERE goal_id=? LIMIT ?")
            for record in db.execute(sql, (args.goal_id,limit)):
                print(json.dumps(dict(zip(("goal","conversation","message","role",
                      "source","occurrences","historical_state"),record))))
        print("Exact mentions are not task ownership or token-spend proof.",file=sys.stderr)
    elif args.command == "search":
        with closing(database(root / (PREFIX + "search.sqlite3"))) as db:
            try:
                sql = ("SELECT p.id FROM text_index JOIN part p ON p.id=text_index.rowid "
                       "JOIN conversation c ON c.id=p.conversation_id WHERE text_index MATCH ?")
                params = [args.query]
                if args.role:
                    sql += " AND p.role=?"
                    params.append(args.role)
                if args.title:
                    sql += " AND instr(lower(c.title),lower(?))>0"
                    params.append(args.title)
                sql += " ORDER BY bm25(text_index),p.id LIMIT ?"
                params.append(limit)
                ids = [x[0] for x in db.execute(sql,params)]
            except sqlite3.OperationalError as error:
                refuse("FTS query invalid: " + str(error))
        rows = part_rows(root,ids)
        by_id = {row["id"]:row for row in rows}
        ordered = [by_id[n] for n in ids if n in by_id]
        records = restore_rows(zip_path,ordered)
        for row in ordered:
            text = source_part(records[(row["source_file"],row["source_position"])],row)
            print(json.dumps(dict(part_id=row["id"],conversation=row["title"],
                 conversation_id=row["conversation_id"],message_id=row["message_id"],
                 role=row["role"],excerpt=text[:900],truncated=len(text)>900),ensure_ascii=False))
    elif args.command == "show":
        rows = part_rows(root,[args.part_id])
        if len(rows) != 1:
            refuse("text part not found")
        row = rows[0]
        record = restore_rows(zip_path,rows)[(row["source_file"],row["source_position"])]
        content = source_part(record,row)
        print(f"VERIFIED part={row['id']} conversation={row['conversation_id']} message={row['message_id']}",file=sys.stderr)
        sys.stdout.buffer.write(content.encode("utf8"))
    elif args.command == "code":
        with closing(database(root / (PREFIX + "code-references.sqlite3"))) as db:
            block = db.execute("SELECT body_start,body_end,sha256,utf8_bytes,language "
                 "FROM code_block WHERE part_id=? AND block_index=?",(args.part_id,args.block_index)).fetchone()
        rows = part_rows(root,[args.part_id])
        if not block or len(rows)!=1:
            refuse("code block or original part missing")
        row = rows[0]
        record = restore_rows(zip_path,rows)[(row["source_file"],row["source_position"])]
        content = source_part(record,row)
        start,end,digest,size,lang=block
        body = content[start:end].encode()
        if len(body)!=size or hashlib.sha256(body).hexdigest()!=digest:
            refuse("code block does not match the canonical ZIP")
        print(f"VERIFIED sha256={digest} language={lang} bytes={size}",file=sys.stderr)
        sys.stdout.buffer.write(body)

if __name__ == "__main__":
    main()
