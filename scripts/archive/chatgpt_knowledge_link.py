#!/usr/bin/env python3
"""Private, evidence-first conversation -> Wiki dossier.

A source citation is NOT a Workboard task link or an accepted Wiki claim.
Task mentions remain separate in the existing goal-reference catalog.
"""
import argparse
from contextlib import closing
from datetime import datetime, timezone
import hashlib
import html
import os
from pathlib import Path
import re
import sqlite3
import sys

from chatgpt_export_reader import (
    PREFIX, database, part_rows, provenance, restore_rows, source_part, refuse
)
from chatgpt_evidence_graph import current_reviews, source_digest

LEDGER = PREFIX + "knowledge-links.sqlite3"
ROUTE = re.compile(r"^notes/[a-z0-9][a-z0-9/-]{1,110}$")
ACTOR = re.compile(r"^[a-zA-Z0-9:_-]{2,128}$")
SQL = """CREATE TABLE IF NOT EXISTS source_link_event (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 source_sha256 TEXT NOT NULL,
 conversation_id TEXT NOT NULL,
 message_id TEXT NOT NULL,
 part_id INTEGER NOT NULL,
 wiki_route TEXT NOT NULL,
 actor TEXT NOT NULL,
 operation TEXT NOT NULL CHECK(operation IN ('cite','revoke')),
 recorded_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS source_link_scope ON source_link_event(source_sha256,conversation_id,id);"""

def read_links(root, sha, conversation):
    db_path = root / LEDGER
    if not db_path.exists():
        return {}
    with closing(database(db_path)) as db:
        sql = ("SELECT conversation_id,message_id,part_id,wiki_route,actor,operation,recorded_at "
               "FROM source_link_event WHERE source_sha256=? AND conversation_id=? ORDER BY id")
        newest = {}
        for conv,msg,part,route,actor,operation,when in db.execute(sql,(sha,conversation)):
            newest[(msg,part,route)] = dict(conversation=conv,message=msg,part_id=part,
                route=route,actor=actor,operation=operation,recorded_at=when)
        return {key:value for key,value in newest.items() if value["operation"]=="cite"}

def exact_part(root, part_id, verify_archive=True):
    rows = part_rows(root,[part_id])
    if len(rows)!=1:
        refuse("exact source part not indexed")
    path,_ = provenance(root,["search"])
    # Full source rehash is mandatory for review writes; read-only cases use catalog provenance.
    sha=source_digest(root)
    if verify_archive:
        digest=hashlib.sha256()
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1<<20),b""):
                digest.update(chunk)
        if digest.hexdigest()!=sha:
            refuse("original ZIP SHA-256 changed; source link not admitted")
    row=rows[0]
    record=restore_rows(path,rows)[(row["source_file"],row["source_position"])]
    return row,source_part(record,row),sha

def cite(root, part_id, route, actor, operation):
    if not ROUTE.fullmatch(route) or ".." in route or "//" in route:
        refuse("wiki route must be a private notes/<slug> route")
    if not ACTOR.fullmatch(actor):
        refuse("reviewer identity required")
    row,_,sha=exact_part(root,part_id)
    active=read_links(root,sha,row["conversation_id"])
    key=(row["message_id"],part_id,route)
    if operation=="revoke" and key not in active:
        refuse("source citation is not active")
    path=root/LEDGER
    if path.is_symlink():
        refuse("knowledge ledger cannot be a symlink")
    with sqlite3.connect(path) as db:
        db.executescript(SQL)
        db.execute("BEGIN IMMEDIATE")
        db.execute("""INSERT INTO source_link_event
            (source_sha256,conversation_id,message_id,part_id,wiki_route,actor,operation,recorded_at)
            VALUES(?,?,?,?,?,?,?,?)""",
            (sha,row["conversation_id"],row["message_id"],part_id,route,actor,operation,
             datetime.now(timezone.utc).isoformat()))
        db.commit()
    print(("SOURCE_CITED" if operation=="cite" else "SOURCE_REVOKED")+
          "\tpart="+str(part_id)+"\t"+route)
    print("CMS_UNCHANGED\tNo task ownership, accepted fact or publication inferred.")

def case(root,part_id,route=None,limit=24):
    if route and (not ROUTE.fullmatch(route) or ".." in route or "//" in route):
        refuse("invalid private Wiki route")
    row,source_text,sha=exact_part(root,part_id,verify_archive=False)
    conv=row["conversation_id"]
    with closing(database(root/(PREFIX+"search.sqlite3"))) as db:
        rec=db.execute("SELECT title FROM conversation WHERE id=?",(conv,)).fetchone()
        before=db.execute("SELECT id,message_id,role FROM part WHERE conversation_id=? AND id<=? ORDER BY id DESC LIMIT ?",
                          (conv,part_id,max(1,limit//2))).fetchall()
        after=db.execute("SELECT id,message_id,role FROM part WHERE conversation_id=? AND id>? ORDER BY id LIMIT ?",
                         (conv,part_id,limit-len(before))).fetchall()
        siblings=list(reversed(before))+after
    if not rec:
        refuse("conversation missing from catalog")
    linked=read_links(root,sha,conv)
    with closing(database(root/(PREFIX+"goal-references.sqlite3"))) as db:
        goals=db.execute(
            "SELECT DISTINCT goal_id FROM goal_reference WHERE conversation_id=? ORDER BY goal_id",
            (conv,)).fetchall()
    goals=[g[0] for g in goals]
    reviewed=current_reviews(root,sha,conversation=conv)
    esc=lambda x: html.escape(str(x),quote=True)
    label="Reviewed source citation" if route and (row["message_id"],part_id,route) in linked else "Source candidate — not reviewed"
    item_list="".join(
        '<li class="part'+(' chosen' if part==part_id else '')+
        '"><span>'+esc(role)+'</span><strong>Part '+str(part)+'</strong><small>'+esc(message)+'</small></li>'
        for part,message,role in siblings
    )
    if not item_list:
        item_list="<li>No related indexed parts</li>"
    task_list="".join(
        '<li><strong>'+esc(goal)+'</strong><span>Exact ID mentioned; work owner/state not verified</span></li>'
        for goal in goals
    ) or "<li>No exact goal IDs indexed for this conversation</li>"
    reviewed_list="".join(
        '<li>Goal '+esc(v["goal"])+' → '+esc(v["wiki_route"])+
        '<span>Reviewed task association, not task completion</span></li>'
        for v in reviewed.values()
    ) or "<li>No reviewed task-to-Wiki associations</li>"
    links_list="".join(
        '<li>'+esc(v["route"])+'<span>source part '+str(v["part_id"])+
        ', reviewed by '+esc(v["actor"])+'</span></li>' for v in linked.values()
    ) or "<li>No reviewed source citations</li>"
    css="""
    :root{color-scheme:dark;--bg:#111923;--panel:#1b2734;--edge:#324356;--text:#f2f5f8;--quiet:#adbfcd;--accent:#9fd8ce}
    *{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font:15px/1.5 system-ui,sans-serif}
    header{border-bottom:1px solid var(--edge);padding:24px max(20px,4vw)}h1{margin:.4rem 0;font-size:clamp(22px,2.8vw,34px)}
    .eyebrow{letter-spacing:.12em;text-transform:uppercase;color:var(--accent);font-size:12px}
    .sub{color:var(--quiet);overflow-wrap:anywhere}.layout{display:grid;grid-template-columns:minmax(0,1.55fr) minmax(290px,1fr);gap:20px;padding:22px max(20px,4vw)}
    section{background:var(--panel);border:1px solid var(--edge);border-radius:16px;padding:22px;margin-bottom:18px}
    h2{font-size:17px;margin:0 0 12px}h3{font-size:14px}ul{list-style:none;padding:0;margin:0}
    li{border-bottom:1px solid var(--edge);padding:9px 0;overflow-wrap:anywhere}li:last-child{border:none}
    li span,li small{display:block;color:var(--quiet);font-size:12px}.part{display:grid;grid-template-columns:75px 95px 1fr;gap:8px}
    .part small,.part span{display:inline}.chosen{color:var(--accent)}pre{white-space:pre-wrap;overflow-wrap:anywhere;font:13px/1.6 ui-monospace,Menlo,monospace;max-height:65vh;overflow:auto}
    .badge{border:1px solid var(--accent);display:inline-block;color:var(--accent);border-radius:100px;padding:4px 12px;font-size:12px}
    details summary{cursor:pointer;color:var(--accent)}footer{padding:20px max(20px,4vw);border-top:1px solid var(--edge);color:var(--quiet);font-size:12px}
    @media(max-width:900px){.layout{display:block}.part{grid-template-columns:65px 80px 1fr}}"""
    return ('<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">'
        '<meta name="referrer" content="no-referrer">'
        '<title>Source Case — '+esc(rec[0])+'</title><style>'+css+'</style></head><body>'
        '<header><div class="eyebrow">Bonfÿre / Private Evidence Case</div><h1>'+esc(rec[0])+
        '</h1><p class="sub">Conversation '+esc(conv)+' · Message '+esc(row["message_id"])+
        '</p><span class="badge">'+esc(label)+'</span></header><main class="layout">'
        '<div><section><h2>01 / Original source</h2><p class="sub">Exact recovered part '+str(part_id)+
        ' · '+esc(row["role"])+' · '+str(len(source_text.encode("utf8")))+' source bytes</p>'
        '<details open><summary>Recovered original message</summary><pre>'+esc(source_text)+'</pre></details></section>'
        '<section><h2>02 / Conversation structure</h2><ul>'+item_list+
        '</ul><p class="sub">Nearby '+str(len(siblings))+' indexed parts shown; not the full branch graph.</p></section></div>'
        '<aside><section><h2>03 / Work evidence</h2><p class="sub">Historical exact mentions are not live work state.</p><ul>'+
        task_list+'</ul><h3>Reviewed task associations</h3><ul>'+reviewed_list+'</ul></section>'
        '<section><h2>04 / Knowledge review</h2><p class="sub">Wiki route: '+esc(route or "not selected")+
        '</p><ul>'+links_list+'</ul><p class="sub">Draft only; no article claim or publication automatically admitted.</p></section>'
        '<section><h2>05 / Native continuations</h2><p><a href="https://aurekai.work/work-board?context=self" rel="noreferrer">Account Workboard</a> · <a href="https://aurekai.work/apps?context=self" rel="noreferrer">Installed applications</a></p><p>Review this source separately from task ownership, then explicitly cite it into a private Wiki route when appropriate.</p></section></aside>'
        '</main><footer>Source SHA-256 '+esc(sha)+' · Local private projection, not a published Aurekai surface · No external requests</footer></body></html>')

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root",type=Path,default=Path.home()/".bonfyre/archive-recovery")
    commands=parser.add_subparsers(dest="command",required=True)
    for name in ("case","cite","revoke"):
        command=commands.add_parser(name)
        command.add_argument("--part",type=int,required=True)
        command.add_argument("--wiki-route")
        if name=="case":
            command.add_argument("--limit",type=int,default=24)
            command.add_argument("--out",type=Path)
        else:
            command.add_argument("--actor",required=True)
    a=parser.parse_args()
    root=a.root.expanduser()
    if a.command=="case":
        if a.limit<1 or a.limit>100:
            refuse("part limit must be 1-100")
        page=case(root,a.part,a.wiki_route,a.limit)
        if a.out:
            if not a.out.parent.is_dir() or a.out.exists() or a.out.is_symlink():
                refuse("destination unavailable or already exists; never overwrite")
            fd=os.open(str(a.out),os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
            with os.fdopen(fd,"w",encoding="utf8") as file:
                file.write(page)
            print("PRIVATE_CASE\t"+str(a.out))
        else:
            print(page)
    else:
        if not a.wiki_route:
            refuse("--wiki-route required")
        cite(root,a.part,a.wiki_route,a.actor,a.command)

if __name__=="__main__":
    main()
