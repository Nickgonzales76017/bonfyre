#!/usr/bin/env python3
"""Evidence-first ChatGPT -> Workboard -> Wiki projection.

Source message, exact task mention, reviewed task association, and private
wiki draft are distinct states. No automatic CMS publication or task writes.
"""
import argparse
from contextlib import closing
from datetime import datetime, timezone
import hashlib
import os
from pathlib import Path
import re
import sqlite3
import sys

from chatgpt_export_reader import PREFIX, database, part_rows, provenance, restore_rows, source_part, refuse

LEDGER = PREFIX+"reviewed-links.sqlite3"
SCHEMA = """
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS review_event(
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 source_sha256 TEXT NOT NULL,
 goal_id TEXT NOT NULL,
 conversation_id TEXT NOT NULL,
 message_id TEXT NOT NULL,
 part_id INTEGER NOT NULL,
 wiki_route TEXT NOT NULL,
 actor TEXT NOT NULL,
 operation TEXT NOT NULL CHECK(operation IN('review','revoke')),
 reviewed_at TEXT NOT NULL,
 UNIQUE(source_sha256,goal_id,conversation_id,message_id,wiki_route,operation,actor,reviewed_at)
);
CREATE INDEX IF NOT EXISTS review_goal ON review_event(source_sha256,goal_id,id);
CREATE INDEX IF NOT EXISTS review_conversation ON review_event(source_sha256,conversation_id,id);
"""
GOAL = re.compile(r"^[a-f0-9]{32}$")
ROUTE = re.compile(r"^notes/[a-z0-9][a-z0-9/-]{1,110}$")
ACTOR = re.compile(r"^[a-zA-Z0-9:_-]{2,128}$")

def readonly_ledger(root):
    path = root/LEDGER
    if not path.is_file():
        return None
    return database(path)

def open_ledger(root):
    path=root/LEDGER
    if path.is_symlink():
        refuse("review ledger is a symlink")
    conn=sqlite3.connect(path)
    conn.executescript(SCHEMA)
    conn.execute("PRAGMA busy_timeout=5000")
    return conn

def source_digest(root):
    with closing(database(root/(PREFIX+"member-manifest.sqlite3"))) as db:
        row=db.execute("SELECT sha256 FROM archive").fetchone()
    if not row or len(row[0])!=64:
        refuse("unavailable export SHA-256")
    return row[0]

def goal_rows(root,goal):
    with closing(database(root/(PREFIX+"goal-references.sqlite3"))) as db:
        rows=db.execute("""
          SELECT goal_id,conversation_id,message_id,role,conversation_file,
                 occurrences,goal_state_at_scan
          FROM goal_reference WHERE goal_id=? ORDER BY conversation_id,message_id
        """,(goal,)).fetchall()
    with closing(database(root/(PREFIX+"search.sqlite3"))) as db:
        result=[]
        for g,conv,msg,role,filename,n,st in rows:
            entry=db.execute("SELECT title,source_file,source_position FROM conversation WHERE id=?",(conv,)).fetchone()
            if not entry or entry[1]!=filename:
                refuse("unresolved or inconsistent exact conversation reference")
            part=db.execute("SELECT id FROM part WHERE conversation_id=? AND message_id=? AND role=? ORDER BY part_index LIMIT 1",(conv,msg,role)).fetchone()
            result.append(dict(goal=g,conversation=conv,message=msg,role=role,file=filename,
                               position=entry[2],title=entry[0],part_id=part[0] if part else None,
                               mention_count=n,historical_state=st))
    return result

def current_reviews(root,sha,goal=None,conversation=None):
    db=readonly_ledger(root)
    if db is None:
        return {}
    try:
        sql="SELECT goal_id,conversation_id,message_id,part_id,wiki_route,actor,operation,reviewed_at FROM review_event WHERE source_sha256=?"
        vals=[sha]
        if goal:
            sql+=" AND goal_id=?";vals.append(goal)
        if conversation:
            sql+=" AND conversation_id=?";vals.append(conversation)
        sql+=" ORDER BY id"
        current={}
        for g,c,m,p,w,a,op,when in db.execute(sql,vals):
            current[(g,c,m,w)]=dict(goal=g,conversation=c,message=m,part_id=p,
                                   wiki_route=w,actor=a,operation=op,reviewed_at=when)
        return {k:v for k,v in current.items() if v["operation"]=="review"}
    finally:
        db.close()

def inspect(root,goal,conv=None):
    sha=source_digest(root)
    mentions=[x for x in goal_rows(root,goal) if not conv or x["conversation"]==conv]
    reviewed=current_reviews(root,sha,goal,conv)
    print("SOURCE\t"+sha)
    print("GOAL\t"+goal)
    print("EXACT_MENTIONS\t"+str(len(mentions)))
    print("REVIEWED_LINKS\t"+str(len(reviewed)))
    for m in mentions:
        print("MENTION\t"+m["conversation"]+"\t"+m["message"]+"\tpart="+str(m["part_id"])+
              "\t"+str(m["mention_count"])+"\t"+m["title"].replace("\t"," "))
    for v in reviewed.values():
        print("REVIEWED\t"+v["conversation"]+"\t"+v["message"]+"\t"+
              v["wiki_route"]+"\tby="+v["actor"])
    print("NOTICE\tA mention is not task ownership, completion or a published wiki claim.")

def exact_evidence(root,goal,conversation,message,part_id):
    if not GOAL.fullmatch(goal):
        refuse("goal ID must be 32 lowercase hex")
    matches=[x for x in goal_rows(root,goal) if x["conversation"]==conversation and x["message"]==message]
    if not matches:
        refuse("goal does not have an indexed exact message mention")
    match=matches[0]
    rows=part_rows(root,[part_id])
    if len(rows)!=1 or rows[0]["conversation_id"]!=conversation or rows[0]["message_id"]!=message:
        refuse("part is not from that exact source message")
    source,_=provenance(root,["search","goal-references"])
    # Explicit review is an attribution event: re-hash the original ZIP, not just its size/mtime.
    digest=hashlib.sha256()
    with source.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            digest.update(chunk)
    if digest.hexdigest()!=source_digest(root):
        refuse("original ZIP digest changed; attribution not admitted")
    record=restore_rows(source,rows)[(rows[0]["source_file"],rows[0]["source_position"])]
    text=source_part(record,rows[0])
    if goal not in text.lower():
        refuse("indexed goal mention could not be verified against source part")
    return match,text

def review(root,goal,conversation,message,part_id,route,actor,operation):
    if not ROUTE.fullmatch(route) or ".." in route or "//" in route:
        refuse("wiki route must be a private notes/<slug> route")
    if not ACTOR.fullmatch(actor):
        refuse("review actor must be an explicit stable identity")
    exact_evidence(root,goal,conversation,message,part_id)
    sha=source_digest(root)
    if operation=="revoke" and (goal,conversation,message,route) not in current_reviews(root,sha,goal,conversation):
        refuse("cannot revoke a link that is not active")
    with closing(open_ledger(root)) as db:
        db.execute("BEGIN IMMEDIATE")
        db.execute("""INSERT INTO review_event
          (source_sha256,goal_id,conversation_id,message_id,part_id,wiki_route,
           actor,operation,reviewed_at) VALUES(?,?,?,?,?,?,?,?,?)""",
          (sha,goal,conversation,message,part_id,route,actor,operation,
           datetime.now(timezone.utc).isoformat()))
        db.commit()
    print(("REVIEWED" if operation=="review" else "REVOKED")+"\t"+goal+"\t"+conversation+"\t"+route)
    print("CMS_UNCHANGED\tWiki draft still requires an explicit account-owned review.")

def draft(root,goal,conv,route):
    sha=source_digest(root)
    rows=[v for v in current_reviews(root,sha,goal,conv).values() if v["wiki_route"]==route]
    if not rows:
        refuse("no active reviewed link for this wiki route and task")
    refs=goal_rows(root,goal)
    title=next((x["title"] for x in refs if x["conversation"]==conv),conv)
    if not title or not title.strip():
        title=conv
    print("# "+title.replace("\n"," ")+"\n")
    print("Canonical type: wiki_page")
    print("Route: "+route)
    print("Lifecycle: draft")
    print("Published: false")
    print("Allow guest: false")
    print("Source ZIP SHA-256: "+sha)
    print("\n## Evidence (reviewed links)\n")
    for r in rows:
        print("- Goal "+goal+", conversation "+conv+", message "+r["message"]+
              ", source part "+str(r["part_id"])+", reviewed by "+r["actor"])
    print("\n## Reviewed knowledge\n")
    print("Pending substantive editorial review; an assistant's historical message is source evidence, not accepted knowledge.")
    print("\n## Task state\n")
    print("Read the current Workboard owner before stating a task's status. A historical mention does not imply completion.")
    print("\nThis is a private CMS wiki_page draft outline, not a published wiki entry.")

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--root",type=Path,default=Path.home()/".bonfyre/archive-recovery")
    sub=p.add_subparsers(dest="command",required=True)
    goals=sub.add_parser("goals");goals.add_argument("--limit",type=int,default=20)
    show=sub.add_parser("inspect");show.add_argument("--goal",required=True);show.add_argument("--conversation")
    for name in ("review","revoke","wiki-draft"):
        s=sub.add_parser(name);s.add_argument("--goal",required=True)
        s.add_argument("--conversation",required=True)
        s.add_argument("--wiki-route",required=True)
        if name!="wiki-draft":
            s.add_argument("--message",required=True)
            s.add_argument("--part",required=True,type=int)
            s.add_argument("--actor",required=True)
    a=p.parse_args()
    root=a.root.expanduser()
    provenance(root,["search","goal-references"])
    if a.command=="goals":
        if a.limit<1 or a.limit>100:
            refuse("goal list limit must be 1-100")
        with closing(database(root/(PREFIX+"goal-references.sqlite3"))) as db:
            rows=db.execute("SELECT goal_id,count(*),sum(occurrences) FROM goal_reference GROUP BY goal_id ORDER BY count(*) DESC,goal_id LIMIT ?",(a.limit,)).fetchall()
        print("GOAL\tMESSAGES\tMENTIONS")
        for goal,messages,mentions in rows:
            print("%s\t%d\t%d"%(goal,messages,mentions))
    elif a.command=="inspect":
        inspect(root,a.goal,a.conversation)
    elif a.command=="wiki-draft":
        draft(root,a.goal,a.conversation,a.wiki_route)
    else:
        review(root,a.goal,a.conversation,a.message,a.part,a.wiki_route,a.actor,a.command)

if __name__=="__main__":
    main()
