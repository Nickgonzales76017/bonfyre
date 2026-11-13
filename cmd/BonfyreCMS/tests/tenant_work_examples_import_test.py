#!/usr/bin/env python3
"""End-to-end CLI contract for scoped work-example imports using a real SQLite DB.
Mock CMS accepts the same entry verbs but does not claim native binary parity.
"""
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import tempfile
import textwrap

ROOT = Path(__file__).resolve().parents[3]
INSTALL = ROOT / "cmd/BonfyreCMS/tools/install_work_examples.sh"

SQLITE_CMD = r'''#!/usr/bin/env python3
import sqlite3, sys
parts = sys.argv[1:]
if parts and parts[0] == "-readonly":
    parts.pop(0)
if len(parts) != 2 or not parts[1].lstrip().upper().startswith("SELECT "):
    sys.exit("read-only SQL interface refuses mutation")
db, query = parts
with sqlite3.connect("file:" + db + "?mode=ro", uri=True) as conn:
    for row in conn.execute(query):
        print("|".join("" if x is None else str(x) for x in row))
'''

CMS_CMD = r'''#!/usr/bin/env python3
import json, os, sqlite3, sys
a = sys.argv[1:]
db = a[a.index("--db")+1]
with sqlite3.connect(db) as conn:
    if a[:2] == ["schema", "migrate"]:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS program_template (
              id INTEGER PRIMARY KEY, namespace TEXT NOT NULL,
              template_id TEXT NOT NULL, status TEXT, description TEXT);
            CREATE TABLE IF NOT EXISTS work_example (
              id INTEGER PRIMARY KEY, namespace TEXT NOT NULL,
              example_id TEXT NOT NULL, program_template TEXT, headline TEXT);
        """)
    elif a[:2] == ["entry", "create"]:
        kind = a[2]; data = json.loads(a[3])
        ns = a[a.index("--ns")+1]
        if kind == "program_template":
            conn.execute("INSERT INTO program_template(namespace,template_id,status,description) VALUES(?,?,?,?)",
              (ns,data["template_id"],data["status"],data.get("description","")))
        elif kind == "work_example":
            conn.execute("INSERT INTO work_example(namespace,example_id,program_template,headline) VALUES(?,?,?,?)",
              (ns,data["example_id"],data["program_template"],data.get("headline","")))
        else:
            sys.exit("wrong type")
    elif a[:2] == ["entry", "update"]:
        if a[2] != "work_example":
            sys.exit("wrong update type")
        if os.environ.get("FAIL_CMS_UPDATE") == "1":
            sys.exit("simulated native update refusal")
        row_id=int(a[3]); data=json.loads(a[4])
        result=conn.execute("UPDATE work_example SET program_template=?, headline=? WHERE id=?",
          (data["program_template"],data.get("headline",""),row_id))
        if result.rowcount != 1: sys.exit("unknown scoped work record")
    else:
        sys.exit("unrecognized CMS call")
'''

def run(env, db):
    return subprocess.run(["sh", str(INSTALL), str(db)], env=env,
                          text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

def row(db, ns, kind, key):
    with sqlite3.connect(db) as conn:
        col = "example_id" if kind == "work_example" else "template_id"
        return conn.execute("SELECT * FROM " + kind + " WHERE namespace=? AND " + col + "=?",
                            (ns,key)).fetchone()

def verify():
    with tempfile.TemporaryDirectory(prefix="bf-work-namespace-") as folder:
        root=Path(folder)
        bin_dir=root/"bin"
        bin_dir.mkdir()
        for filename,code in [("sqlite3",SQLITE_CMD),("mock-cms",CMS_CMD)]:
            target=bin_dir/filename
            target.write_text(code)
            target.chmod(0o755)
        db=root/"cms.db"
        with sqlite3.connect(db) as conn:
            conn.executescript("""
                CREATE TABLE program_template (
                   id INTEGER PRIMARY KEY, namespace TEXT, template_id TEXT, status TEXT, description TEXT);
                CREATE TABLE work_example (
                   id INTEGER PRIMARY KEY, namespace TEXT, example_id TEXT, program_template TEXT, headline TEXT);
                INSERT INTO program_template(namespace,template_id,status) VALUES('root','shared','published');
                INSERT INTO work_example(namespace,example_id,program_template,headline)
                   VALUES('root','shared-work','shared','root owns this');
            """)
        program=root/"programs.jsonl"
        work=root/"work.jsonl"
        program.write_text(json.dumps({"template_id":"shared","status":"published","description":"tenant program"})+"\n")
        work.write_text(json.dumps({"example_id":"shared-work","program_template":"shared","headline":"tenant first"})+"\n")
        env={**os.environ,
             "PATH":str(bin_dir)+os.pathsep+os.environ.get("PATH",""),
             "BONFYRE_CMS_BINARY":str(bin_dir/"mock-cms"),
             "BONFYRE_WORK_EXAMPLES":str(work),
             "BONFYRE_WORK_EXAMPLE_PROGRAMS":str(program),
             "BONFYRE_CMS_NAMESPACE":"tenant_one"}
        orig=row(db,"root","work_example","shared-work")
        first=run(env,db)
        assert first.returncode==0, first.stderr
        tenant=row(db,"tenant_one","work_example","shared-work")
        assert tenant and tenant[-1]=="tenant first"
        assert row(db,"root","work_example","shared-work")==orig, "cross-tenant overwrite"
        assert row(db,"tenant_one","program_template","shared"), "program installed in wrong namespace"

        work.write_text(json.dumps({"example_id":"shared-work","program_template":"shared","headline":"tenant updated"})+"\n")
        fail=run({**env,"FAIL_CMS_UPDATE":"1"},db)
        assert fail.returncode!=0, "mock refused update but importer reported success"
        assert row(db,"tenant_one","work_example","shared-work")==tenant, "failed update destroyed existing row"
        retry=run(env,db)
        assert retry.returncode==0, retry.stderr
        updated=row(db,"tenant_one","work_example","shared-work")
        assert updated[0]==tenant[0] and updated[-1]=="tenant updated", "row identity not preserved"
        assert row(db,"root","work_example","shared-work")==orig, "root was modified"

        second=run({**env,"BONFYRE_CMS_NAMESPACE":"tenant_two"},db)
        assert second.returncode==0, second.stderr
        assert row(db,"tenant_two","work_example","shared-work")
        assert row(db,"tenant_one","work_example","shared-work")==updated, "other tenant contaminated"
        no_ns=run({k:v for k,v in env.items() if k!="BONFYRE_CMS_NAMESPACE"},db)
        assert no_ns.returncode != 0 and "requires BONFYRE_CMS_NAMESPACE" in no_ns.stderr
        unsafe=run({**env,"BONFYRE_CMS_NAMESPACE":"tenant' OR 1=1"},db)
        assert unsafe.returncode != 0 and "invalid CMS namespace" in unsafe.stderr
        missing=run(env,root/"not-existing.db")
        assert missing.returncode != 0 and not (root/"not-existing.db").exists()
        print("TENANT_WORK_EXAMPLES_PASS: namespace isolation, idempotent update, failure preservation, input rejection")

if __name__=="__main__":
    verify()
