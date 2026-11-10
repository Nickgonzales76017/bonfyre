#!/usr/bin/env python3
"""Source-grounded knowledge reconstruction, not an HTML transcript viewer.

Output: private, queryable SQLite evidence graph. All semantic categories are
candidates; no Wiki publication, task completion or owner proof is inferred.
"""
import argparse
from collections import defaultdict
from contextlib import closing
from datetime import datetime, timezone
import hashlib
import os
from pathlib import Path
import re
import sqlite3
import zipfile

from chatgpt_export_reader import PREFIX, database, provenance, records_from_zip, refuse

SCHEMA = """
CREATE TABLE provenance(source_sha256 TEXT NOT NULL, scope TEXT NOT NULL);
CREATE TABLE conversation(id TEXT PRIMARY KEY,title TEXT NOT NULL);
CREATE TABLE message(id TEXT PRIMARY KEY,conversation_id TEXT NOT NULL,parent_id TEXT,
                     branch TEXT NOT NULL,role TEXT NOT NULL,ordinal INTEGER NOT NULL);
CREATE TABLE assertion(id INTEGER PRIMARY KEY,part_id INTEGER NOT NULL,message_id TEXT NOT NULL,
                       offset_start INTEGER NOT NULL,offset_end INTEGER NOT NULL,
                       topic TEXT NOT NULL,kind TEXT NOT NULL,text TEXT NOT NULL,
                       text_sha256 TEXT NOT NULL,review_state TEXT NOT NULL DEFAULT 'candidate');
CREATE TABLE structure(id INTEGER PRIMARY KEY,part_id INTEGER NOT NULL,message_id TEXT NOT NULL,
                       parent_id INTEGER,depth INTEGER NOT NULL,label TEXT NOT NULL);
CREATE TABLE relation(source INTEGER NOT NULL,target INTEGER NOT NULL,
                      kind TEXT NOT NULL,review_state TEXT NOT NULL DEFAULT 'candidate');
CREATE TABLE review_event(id INTEGER PRIMARY KEY,assertion_id INTEGER NOT NULL,
   assertion_sha256 TEXT NOT NULL,actor TEXT NOT NULL,
   verdict TEXT NOT NULL CHECK(verdict IN ('include','exclude','needs_owner')),
   rationale TEXT NOT NULL,reviewed_at TEXT NOT NULL);
CREATE INDEX review_target ON review_event(assertion_id,id);
CREATE VIEW current_review AS SELECT r.* FROM review_event r
  WHERE r.id=(SELECT MAX(id) FROM review_event WHERE assertion_id=r.assertion_id);
CREATE INDEX by_kind ON assertion(kind);
CREATE INDEX by_part ON assertion(part_id);
CREATE VIEW requested_work AS SELECT * FROM assertion WHERE kind IN ('request','constraint','decision_claim');
CREATE VIEW verification_queue AS SELECT * FROM assertion WHERE kind='historical_status_claim';
"""
FENCE=re.compile(r"^\s*(" + chr(96)*3 + r"|~~~)")
HEADING=re.compile(r"^\s{0,3}#{1,5}\s+(.+)$")
BULLET=re.compile(r"^\s*(?:[-*•]\s+|\d+[.)]\s+)")
ARROW=re.compile(r"^(\s*)(?:->\s*|→\s*)(.+)$")
WORDS=re.compile(r"[A-Za-z0-9]{3,}")
def classify(text,role):
    t=text.casefold()
    if text.rstrip().endswith("?"):return "question"
    if re.search(r"\b(?:do not|don't|must not|never|cannot|should not|without)\b",t):return "constraint"
    if role=="user" and re.search(r"\b(must|need|build|make|implement|fix|connect|require|ensure|stop)\b",t):return "request"
    if re.search(r"\b(decided|approved|we chose|decision:)\b",t):return "decision_claim"
    if re.search(r"\b(implemented|deployed|shipped|completed|passed|live|verified)\b",t):return "historical_status_claim"
    if re.search(r"\b(should|could|would|might|propose|consider)\b",t):return "proposal"
    return "source_claim"
def fragments(text):
    fenced=False;offset=0;topic="General"
    for line in text.splitlines(keepends=True):
        if FENCE.match(line):fenced=not fenced;offset+=len(line);continue
        if fenced:offset+=len(line);continue
        heading=HEADING.match(line)
        if heading:topic=heading.group(1).strip()[:120];offset+=len(line);continue
        bullet=BULLET.match(line)
        body=line[bullet.end():].strip() if bullet else line.strip()
        if len(body)>=28 and len(WORDS.findall(body))>=4:
            start=offset+line.index(body)
            yield (start,start+len(body),topic,body)
        offset+=len(line)
def hierarchy(text):
    fenced=False;previous=None
    for line in text.splitlines():
        if FENCE.match(line):fenced=not fenced;previous=None;continue
        if not fenced:continue
        m=ARROW.match(line)
        if m:
            if previous:yield -1,previous
            yield len(m.group(1)),m.group(2).strip()[:180]
        previous=(line.strip() if not m and re.fullmatch(r'[A-Za-z][A-Za-z0-9 /_-]{2,100}',line.strip()) else None)
def source_hash(path):
    h=hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda:f.read(1<<20),b""):h.update(chunk)
    return h.hexdigest()
def build(root,query,title,out,max_conversations,max_parts):
    if out.exists() or out.is_symlink() or not out.parent.is_dir():refuse("output must be a new file")
    archive,_=provenance(root,["search"])
    with closing(database(root/(PREFIX+"member-manifest.sqlite3"))) as db:
        expected=db.execute("SELECT sha256 FROM archive").fetchone()[0]
    if source_hash(archive)!=expected:refuse("original export SHA-256 differs from catalog")
    with closing(database(root/(PREFIX+"search.sqlite3"))) as db:
        sql=("SELECT DISTINCT c.id,c.title,c.source_file,c.source_position FROM conversation c "
             "JOIN part p ON p.conversation_id=c.id JOIN text_index ON text_index.rowid=p.id "
             "WHERE text_index MATCH ?")
        params=[query]
        if title:sql+=" AND instr(lower(c.title),lower(?))>0";params.append(title)
        sql+=" ORDER BY c.id LIMIT ?";params.append(max_conversations+1)
        selected=db.execute(sql,params).fetchall()
        if len(selected)>max_conversations:refuse("too many conversations; narrow query")
        parts={}
        for conv,_,_,_ in selected:
            rows=db.execute("SELECT id,message_id,part_index,role,characters,utf8_bytes FROM part WHERE conversation_id=? ORDER BY id LIMIT ?",
                            (conv,max_parts+1)).fetchall()
            if len(rows)>max_parts:refuse("conversation exceeds max-parts")
            parts[conv]=rows
    if not selected:refuse("no matching conversation")
    targets=defaultdict(set)
    for _,_,filename,pos in selected:targets[filename].add(pos)
    records={}
    with zipfile.ZipFile(archive) as z:
        for filename,positions in targets.items():
            for pos,record in records_from_zip(z,filename,positions).items():
                records[(filename,pos)]=record
    fd=os.open(str(out),os.O_WRONLY|os.O_EXCL|os.O_CREAT,0o600);os.close(fd)
    try:
        with closing(sqlite3.connect(out)) as db:
            db.executescript(SCHEMA)
            db.execute("INSERT INTO provenance VALUES(?,?)",(expected,query))
            repeated={}
            topic_index=defaultdict(list)
            for conv,title,filename,pos in selected:
                record=records[(filename,pos)]
                db.execute("INSERT INTO conversation VALUES(?,?)",(conv,title))
                mapping=record.get("mapping") or {}
                main=set();cursor=record.get("current_node")
                while cursor in mapping and cursor not in main:
                    main.add(cursor);cursor=(mapping[cursor] or {}).get("parent")
                by_id={}
                for ordinal,(nodeid,node) in enumerate(mapping.items()):
                    if not isinstance(node,dict) or not isinstance(node.get("message"),dict):
                        continue
                    m=node["message"]
                    msgid=m.get("id") or "mapping:"+nodeid
                    by_id[msgid]=(nodeid,node,m,ordinal)
                for msgid,(nodeid,node,m,ordinal) in by_id.items():
                    parent=(mapping.get(node.get("parent")) or {}).get("message") or {}
                    parentid=parent.get("id")
                    if parentid not in by_id:parentid=None
                    role=(m.get("author") or {}).get("role") or "unknown"
                    db.execute("INSERT INTO message VALUES(?,?,?,?,?,?)",
                               (msgid,conv,parentid,"main" if nodeid in main else ("alternate" if main else "unresolved"),role,ordinal))
                for part_id,msgid,partno,role,indexed_chars,indexed_bytes in parts[conv]:
                    if msgid not in by_id:refuse("indexed source message is missing: "+str(msgid))
                    items=(by_id[msgid][2].get("content") or {}).get("parts") or []
                    if partno<0 or partno>=len(items) or not isinstance(items[partno],str):
                        refuse("indexed source part is missing: "+str(part_id))
                    source=items[partno]
                    if len(source)!=indexed_chars or len(source.encode("utf8"))!=indexed_bytes:
                        refuse("indexed source text dimensions disagree: "+str(part_id))
                    source_role=(by_id[msgid][2].get("author") or {}).get("role") or role
                    stack=[]
                    for depth,label in hierarchy(source):
                        while stack and stack[-1][0]>=depth:stack.pop()
                        parent=stack[-1][1] if stack else None
                        db.execute("INSERT INTO structure(part_id,message_id,parent_id,depth,label) VALUES(?,?,?,?,?)",
                                   (part_id,msgid,parent,depth,label))
                        sid=db.execute("SELECT last_insert_rowid()").fetchone()[0]
                        stack.append((depth,sid))
                    for start,end,topic,text in fragments(source):
                        sha=hashlib.sha256(text.encode()).hexdigest()
                        db.execute("INSERT INTO assertion(part_id,message_id,offset_start,offset_end,topic,kind,text,text_sha256) VALUES(?,?,?,?,?,?,?,?)",
                                   (part_id,msgid,start,end,topic,classify(text,source_role),text,sha))
                        aid=db.execute("SELECT last_insert_rowid()").fetchone()[0]
                        if sha in repeated:
                            db.execute("INSERT INTO relation(source,target,kind) VALUES(?,?,'exact_repeat')",(repeated[sha],aid))
                        else:repeated[sha]=aid
                        if topic!='General':
                            tokens=set(re.findall(r'[a-z0-9]{4,}',text.casefold()))-{'that','this','with','from','should','would','could','into','have','been','there','their','where','which'}
                            branch='main' if by_id[msgid][0] in main else ('alternate' if main else 'unresolved')
                            polarity=bool(re.search(r'\b(?:not|never|cannot|without|instead|no longer)\b',text,re.I))
                            for old,words,old_branch,old_pol in topic_index[topic.casefold()][-14:]:
                                common=len(tokens & words)
                                if common>=3 and common/max(1,len(tokens | words))>=0.28:
                                    edge=('branch_alternative' if branch!=old_branch and 'unresolved' not in (branch,old_branch)
                                          else 'possible_tension' if polarity!=old_pol else 'related_claim')
                                    db.execute('INSERT INTO relation(source,target,kind) VALUES(?,?,?)',(old,aid,edge))
                            topic_index[topic.casefold()].append((aid,tokens,branch,polarity))
            db.commit()
        print("KNOWLEDGE_GRAPH "+str(out))
        with closing(sqlite3.connect(out)) as db:
            for table in ("conversation","message","assertion","structure","relation"):
                print(table.upper()+" "+str(db.execute("SELECT count(*) FROM "+table).fetchone()[0]))
        print("All extracted classifications remain UNREVIEWED. No Workboard or Wiki write.")
    except BaseException:
        out.unlink(missing_ok=True)
        raise
def dossier(out):
    with closing(sqlite3.connect(out.resolve().as_uri()+"?mode=ro",uri=True)) as db:
        print("# Reconstruction — decisions, work and open questions")
        sha,scope=db.execute("SELECT source_sha256,scope FROM provenance").fetchone()
        print("\nSource SHA-256: "+sha+"\nSearch: "+scope)
        for typ,heading in [("request","Requested work"),("constraint","Constraints"),
                            ("decision_claim","Decision claims to review"),("proposal","Design options"),
                            ("historical_status_claim","Status requiring owner verification"),("question","Questions")]:
            rows=db.execute("""SELECT a.id,a.text_sha256,a.text,a.part_id,m.branch,m.role,COALESCE(r.verdict,'candidate') FROM assertion a
                               JOIN message m ON a.message_id=m.id
                               LEFT JOIN current_review r ON r.assertion_id=a.id
                               WHERE a.kind=? ORDER BY CASE m.role WHEN 'user' THEN 0 ELSE 1 END,a.id DESC LIMIT 20""",(typ,)).fetchall()
            print("\n## "+heading+"\n")
            if not rows:print("No candidates extracted.")
            for ident,sha,text,part,branch,role,verdict in rows:
                print('- ['+str(ident)+'] '+text[:280].replace('\n',' ')+'  \n  sha256 '+sha+' · part '+str(part)+' · '+role+' · '+branch+' · '+verdict)
        print("\nEvery assertion requires source review. Current work status requires its native owner.")
def editorial_review(path,assertion_id,sha,actor,verdict,reason):
    """Append-only editorial selection, not an implementation or release attestation."""
    if not re.fullmatch(r"[A-Za-z0-9:_-]{2,128}",actor) or len(reason.strip())<12:
        refuse("named actor and substantive review rationale are required")
    with closing(sqlite3.connect(path)) as db:
        db.execute("BEGIN IMMEDIATE")
        hit=db.execute("SELECT kind,text_sha256 FROM assertion WHERE id=?",(assertion_id,)).fetchone()
        if not hit or hit[1]!=sha:
            refuse("assertion or expected source hash changed")
        if hit[0]=="historical_status_claim" and verdict=="include":
            refuse("historical implementation claims require a current native owner receipt")
        db.execute("INSERT INTO review_event(assertion_id,assertion_sha256,actor,verdict,rationale,reviewed_at) VALUES(?,?,?,?,?,?)",
                   (assertion_id,sha,actor,verdict,reason.strip(),datetime.now(timezone.utc).isoformat()))
        db.commit()
    print("EDITORIAL_REVIEW "+str(assertion_id)+" "+verdict+"; no CMS, Workboard, or release effect.")

def wiki_draft(path,route):
    if not re.fullmatch(r"notes/[a-z0-9][a-z0-9/-]{1,110}",route) or ".." in route or "//" in route:
        refuse("a private notes/ route is required")
    with closing(sqlite3.connect(path.resolve().as_uri()+"?mode=ro",uri=True)) as db:
        sha=db.execute("SELECT source_sha256 FROM provenance").fetchone()[0]
        rows=db.execute("""SELECT a.kind,a.part_id,a.message_id,r.actor,r.rationale,a.text_sha256
          FROM current_review r JOIN assertion a ON a.id=r.assertion_id
          WHERE r.verdict='include' ORDER BY a.kind,r.id""").fetchall()
    print("# Reviewed knowledge proposal")
    print("\nRoute: "+route+"\nPublished: false\nAllow guest: false")
    print("Source export SHA-256: "+sha)
    print("\n## Editorially selected material\n")
    if not rows:print("No assertions reviewed for inclusion.")
    for kind,part,msg,actor,reason,h in rows:
        print("- **"+kind+"** — "+reason.replace("\n"," ")+
              "\n  - source part "+str(part)+", message "+msg+", assertion SHA-256 "+h+
              "\n  - reviewed by "+actor+"; implementation status unverified")
    print("\n## Admission boundaries\n")
    print("Source recovery does not establish owner-backed implementation, live task state, or publication.")
    print("A Wiki editor must still verify meaning, decide scope and explicitly save this as a private draft.")
def main():
    p=argparse.ArgumentParser(description=__doc__)
    cmd=p.add_subparsers(dest="command",required=True)
    b=cmd.add_parser("build");b.add_argument("--query",required=True)
    b.add_argument("--title");b.add_argument("--out",required=True,type=Path)
    b.add_argument("--root",type=Path,default=Path.home()/".bonfyre/archive-recovery")
    b.add_argument("--max-conversations",type=int,default=5)
    b.add_argument("--max-parts",type=int,default=500)
    r=cmd.add_parser("dossier");r.add_argument("--db",type=Path,required=True)
    v=cmd.add_parser("review");v.add_argument("--db",type=Path,required=True)
    v.add_argument("--assertion",type=int,required=True);v.add_argument("--sha256",required=True)
    v.add_argument("--actor",required=True);v.add_argument("--verdict",choices=("include","exclude","needs_owner"),required=True)
    v.add_argument("--reason",required=True)
    w=cmd.add_parser("wiki-draft");w.add_argument("--db",type=Path,required=True)
    w.add_argument("--route",required=True)
    a=p.parse_args()
    if a.command=="dossier":dossier(a.db)
    elif a.command=="review":
        editorial_review(a.db,a.assertion,a.sha256,a.actor,a.verdict,a.reason)
    elif a.command=="wiki-draft":wiki_draft(a.db,a.route)
    else:
        if not 1<=a.max_conversations<=50 or not 1<=a.max_parts<=2000:refuse("invalid bounds")
        build(a.root,a.query,a.title,a.out,a.max_conversations,a.max_parts)
if __name__=="__main__":main()
