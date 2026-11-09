#!/usr/bin/env python3
"""Isolated fixtures: planning, resumability, review gates and source fidelity."""
import csv
from hashlib import sha256
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from editorial_native_reconcile import validate_bundle, create_plan, Held

PAGE_FIELDS = ["namespace", "route", "title", "heading", "lede", "editorial_kind", "author_credit", "author_subject_ref", "status"]
FRAGMENT_FIELDS = ["namespace", "coordinate", "page", "slot", "audience", "kind", "position", "body", "projection_contract", "source_ref", "source_digest", "status"]
RECEIPT_FIELDS = ["subject_id", "canonical_url", "author_id", "series", "category", "source_ref", "source_sha256", "status"]


class ReconciliationTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.bundle = self.root / "build/publication"
        self.bundle.mkdir(parents=True)
        (self.root / "editorial/posts").mkdir(parents=True)
        (self.root / "editorial/authors").mkdir(parents=True)
        self.doc = "---\ntitle: Fixture\n---\nA real example.\n\n## Chapter\n\nOriginal text.\n"
        self.author = "---\nslug: desk\n---\nA credited editorial desk.\n"
        (self.root / "editorial/posts/fixture.md").write_text(self.doc)
        (self.root / "editorial/authors/desk.md").write_text(self.author)
        self.digest = sha256(self.doc.encode()).hexdigest()
        self.author_digest = sha256(self.author.encode()).hexdigest()
        page = "/blog/fixture/"
        author = "/blog/authors/desk/"
        self.pages = [dict(namespace="aurekai-journal", route=page, title="Fixture",
                           heading="Fixture", lede="", editorial_kind="article", author_credit="Desk",
                           author_subject_ref="author:desk", status="draft"),
                      dict(namespace="aurekai-journal", route=author, title="Desk",
                           heading="Desk", lede="", editorial_kind="author-profile", author_credit="Desk",
                           author_subject_ref="author:desk", status="draft")]
        def frag(name, route, kind, source_ref, digest, body, contract=""):
            return dict(namespace="aurekai-journal", coordinate="journal:"+name, page=route,
                        slot="main", audience="public", kind=kind, position="1", body=body,
                        projection_contract=contract, source_ref=source_ref,
                        source_digest=digest, status="draft")
        original = "editorial/posts/fixture.md"
        self.fragments = [frag("post:fixture:body", page, "article-body", original, self.digest, "## Chapter"),
                          frag("post:fixture:attribution", page, "article-credit", original, self.digest, "By Desk"),
                          frag("post:fixture:presentation", page, "article-presentation", original, self.digest,
                               "format: dossier", json.dumps({"review_required":True,"subject_id":"post:fixture",
                               "authored_source_sha256":self.digest})),
                          frag("author:desk:bio", author, "author-biography", "editorial/authors/desk.md",
                               self.author_digest, "Desk")]
        self.receipts = [dict(subject_id="post:fixture", canonical_url="https://example.test/blog/fixture/",
                              author_id="desk",series="",category="Test",source_ref=original,
                              source_sha256=self.digest,status="draft"),
                         dict(subject_id="author:desk", canonical_url="https://example.test/blog/authors/desk/",
                              author_id="desk",series="",category="Authors",source_ref="editorial/authors/desk.md",
                              source_sha256=self.author_digest,status="draft")]
        self.db = self.root / "cms.db"
        conn = sqlite3.connect(self.db)
        for table, fields, identity in (("site_page", PAGE_FIELDS, "route"), ("site_fragment", FRAGMENT_FIELDS, "coordinate")):
            cols = ", ".join(f'"{name}" TEXT' for name in fields)
            conn.execute(f'CREATE TABLE {table} (id INTEGER PRIMARY KEY, {cols}, UNIQUE(namespace, {identity}))')
        conn.commit(); conn.close()
        self.cms = self.root / "fake-cms.py"
        self.cms.write_text('''#!/usr/bin/env python3
import json, sqlite3, sys
name=sys.argv[3]; row=json.loads(sys.argv[4]); ns=sys.argv[sys.argv.index('--ns')+1]; db=sys.argv[sys.argv.index('--db')+1]
row['namespace']=ns
for key,value in row.items():
 if isinstance(value,(list,dict)):row[key]=json.dumps(value,sort_keys=True)
cols=', '.join('"'+k+'"' for k in row)
marks=', '.join('?' for _ in row)
with sqlite3.connect(db) as conn:
 conn.execute('INSERT INTO '+name+' ('+cols+') VALUES ('+marks+')',tuple(row.values()))
''')
        self.cms.chmod(0o755)
        (self.root / "schemas").mkdir()
        self.save()

    def save(self):
        for filename, fields, data, separator in (
          ("cms-site-page.csv", PAGE_FIELDS, self.pages, ","),
          ("cms-site-fragment.csv", FRAGMENT_FIELDS, self.fragments, ","),
          ("source-receipts.tsv", RECEIPT_FIELDS, self.receipts, "\t")):
            with (self.bundle / filename).open('w',encoding='utf-8',newline='') as fp:
                writer=csv.DictWriter(fp,fieldnames=fields,delimiter=separator,lineterminator='\n')
                writer.writeheader();writer.writerows(data)

    def groups(self):
        return validate_bundle(self.root,self.bundle)

    def run_cli(self,*args):
        return subprocess.run([sys.executable,str(Path(__file__).resolve().parents[1] / "editorial_native_reconcile.py"),
             "--root",str(self.root),"--bundle",str(self.bundle),"--db",str(self.db),
             "--cms",str(self.cms),"--schemas",str(self.root/"schemas"),"--minimum-free-mib","0",*args],
             capture_output=True,text=True,timeout=20)

    def test_validates_original_bytes_and_full_review_bundle(self):
        groups=self.groups()
        self.assertEqual([len(x[2]) for x in groups],[2,4])
        plan,digest=create_plan(self.db,groups)
        self.assertEqual(set(x[2] for x in plan),{"MISSING"})
        self.assertEqual(len(digest),64)
        self.assertIn("MISSING=6",self.run_cli().stdout)

    def test_resume_without_ever_overwriting_a_draft(self):
        plan,digest=create_plan(self.db,self.groups())
        first=self.run_cli("--apply-drafts","--expected-plan",digest,"--batch-size","2")
        self.assertEqual(first.returncode,0,first.stderr)
        second_plan,second_digest=create_plan(self.db,self.groups())
        self.assertEqual(sum(x[2]=="IDENTICAL" for x in second_plan),2)
        self.assertEqual(sum(x[2]=="MISSING" for x in second_plan),4)
        second=self.run_cli("--apply-drafts","--expected-plan",second_digest,"--batch-size","4")
        self.assertEqual(second.returncode,0,second.stderr)
        completed,_=create_plan(self.db,self.groups())
        self.assertTrue(all(x[2]=="IDENTICAL" for x in completed))
        self.assertEqual(self.run_cli().returncode,0)

    def test_published_and_drift_are_review_held(self):
        conn=sqlite3.connect(self.db)
        published=dict(self.pages[0],status="published")
        conn.execute('INSERT INTO site_page (namespace,route,title,heading,lede,editorial_kind,author_credit,author_subject_ref,status) VALUES (?,?,?,?,?,?,?,?,?)',tuple(published.values()))
        conn.commit(); conn.close()
        plan,digest=create_plan(self.db,self.groups())
        self.assertIn("PROTECTED",[x[2] for x in plan])
        result=self.run_cli("--apply-drafts","--expected-plan",digest)
        self.assertEqual(result.returncode,2)
        self.assertIn("owner review",result.stderr)
        with sqlite3.connect(self.db) as conn:
            self.assertEqual(conn.execute('SELECT count(*) FROM site_fragment').fetchone()[0],0)

    def test_drift_blocks_admission(self):
        with sqlite3.connect(self.db) as conn:
            conn.execute('INSERT INTO site_page (namespace,route,title,heading,editorial_kind,status) VALUES (?,?,?,?,?,?)',
                         ("aurekai-journal","/blog/fixture/","Old title","Fixture","article","draft"))
        plan,digest=create_plan(self.db,self.groups())
        self.assertIn("DRIFT",[x[2] for x in plan])
        self.assertEqual(self.run_cli("--apply-drafts","--expected-plan",digest).returncode,2)

    def test_bad_digest_private_fragment_and_unsafe_status_hold(self):
        self.fragments[0]["source_digest"]="0"*64;self.save()
        with self.assertRaises(Held): self.groups()
        self.fragments[0]["source_digest"]=self.digest
        self.fragments[0]["audience"]="self";self.save()
        with self.assertRaises(Held):self.groups()
        self.fragments[0]["audience"]="public"
        self.pages[0]["status"]="published";self.save()
        with self.assertRaises(Held):self.groups()

    def test_exact_plan_required_even_with_enough_disk(self):
        self.assertEqual(self.run_cli("--apply-drafts","--expected-plan","0"*64).returncode,2)
        self.assertEqual(self.run_cli("--apply-drafts").returncode,2)


if __name__ == "__main__":
    unittest.main()
