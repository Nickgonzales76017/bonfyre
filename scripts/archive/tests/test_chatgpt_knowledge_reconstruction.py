#!/usr/bin/env python3
"""Synthetic tests for source-grounded branch/decision reconstruction."""
from contextlib import redirect_stdout
import io
import hashlib
import json
from pathlib import Path
import sqlite3
import sys
import unittest
import zipfile

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import test_chatgpt_export_reader as fixtures
from chatgpt_knowledge_reconstruction import classify,fragments,hierarchy,build,dossier,editorial_review,wiki_draft

class ReconstructionTest(unittest.TestCase):
 def setUp(self):
  self.fixture=fixtures.TestReader();self.fixture.setUp()
  self.root=self.fixture.root;self.addCleanup(self.fixture.tearDown)
  self.out=self.root/"knowledge.sqlite3"
 def test_signal_roles_separate(self):
  self.assertEqual(classify("We need to build an actual room model","user"),"request")
  self.assertEqual(classify("The rollout was completed yesterday","assistant"),"historical_status_claim")
  self.assertEqual(classify("This could support additional private worlds","assistant"),"proposal")
  self.assertEqual(classify("Do not publish private conversation content","user"),"constraint")
  self.assertEqual(classify("Never deploy without native owner review","user"),"constraint")
  self.assertEqual(classify("We must not build another public transcript copy","user"),"constraint")
  self.assertEqual(classify("Should this be shown to the user?","assistant"),"question")
 def test_provenance_offsets_and_code_exclusion(self):
  content="### Worlds\nNeed to connect private realms into the recursive graph.\n"
  content+="\x60\x60\x60text\nWorldGraph\n  -> personal realm\n    -> family room\n\x60\x60\x60\n"
  proposals=list(fragments(content))
  self.assertEqual(len(proposals),1)
  start,end,topic,value=proposals[0]
  self.assertEqual(content[start:end],value)
  self.assertEqual(topic,"Worlds")
  self.assertNotIn("family room",value)
  self.assertEqual(list(hierarchy(content)),[(-1,'WorldGraph'),(2,'personal realm'),(4,'family room')])
 def test_source_graph_smoke_and_no_overwrite(self):
  with redirect_stdout(io.StringIO()):
   build(self.root,"print",None,self.out,3,200)
  with sqlite3.connect(self.out) as db:
   self.assertEqual(db.execute("SELECT count(*) FROM conversation").fetchone()[0],1)
   self.assertEqual(db.execute("SELECT count(*) FROM message").fetchone()[0],1)
   self.assertEqual(db.execute("SELECT count(*) FROM provenance").fetchone()[0],1)
   self.assertEqual(db.execute("SELECT count(*) FROM assertion WHERE review_state!='candidate'").fetchone()[0],0)
  report=io.StringIO()
  with redirect_stdout(report):dossier(self.out)
  self.assertIn('Reconstruction',report.getvalue())
  with self.assertRaises(SystemExit):
   build(self.root,"print",None,self.out,3,200)
 def test_drift_and_refusal(self):
  with self.fixture.zpath.open("ab") as file:file.write(b"drift")
  with self.assertRaises(SystemExit):
   build(self.root,"print",None,self.out,3,200)
  self.assertFalse(self.out.exists())
 def test_catalog_part_dimensions_must_match_zip(self):
  with sqlite3.connect(self.root/"chatgpt-20261028-search.sqlite3") as db:
   db.execute("UPDATE part SET characters=characters+1 WHERE id=1")
  with self.assertRaises(SystemExit):
   build(self.root,"print",None,self.out,3,200)
  self.assertFalse(self.out.exists())
 def test_missing_indexed_message_refused(self):
  with sqlite3.connect(self.root/"chatgpt-20261028-search.sqlite3") as db:
   db.execute("UPDATE part SET message_id=? WHERE id=1",("nonexistent-message",))
  with self.assertRaises(SystemExit):
   build(self.root,"print",None,self.out,3,200)
  self.assertFalse(self.out.exists())
 def test_hard_conversation_bound(self):
  with self.assertRaises(SystemExit):
   build(self.root,"print",None,self.out,0,200)
 def test_actual_main_and_alternate_branch(self):
  z=self.fixture.zpath
  text1='# World graph\nWe need to connect personal worlds into a shared graph.'
  text2='## Implementation\nThe new world registry was deployed successfully yesterday.'
  with zipfile.ZipFile(z) as archive:
   item=json.loads(archive.read(self.fixture.source))[0]
  item['current_node']='y'
  item['mapping']['x']['message']['content']['parts']=[text1]
  item['mapping']['x']['message']['author']={'role':'user'}
  item['mapping']['y']={'parent':'x','message':{'id':'m2','author':{'role':'assistant'},'content':{'parts':[text2]}}}
  item['mapping']['z']={'parent':'x','message':{'id':'m3','author':{'role':'assistant'},'content':{'parts':['Alternative branch was not selected']}}}
  with zipfile.ZipFile(z,'w',zipfile.ZIP_DEFLATED) as archive:
    archive.writestr(self.fixture.source,json.dumps([item]))
  stat=z.stat();sha=hashlib.sha256(z.read_bytes()).hexdigest()
  with sqlite3.connect(self.root/'chatgpt-20261028-member-manifest.sqlite3') as db:
   db.execute('UPDATE archive SET size=?,mtime_ns=?,sha256=?',(stat.st_size,stat.st_mtime_ns,sha))
  for file in ('search','code-references','goal-references'):
   with sqlite3.connect(self.root/('chatgpt-20261028-'+file+'.sqlite3')) as db:
    db.execute('UPDATE provenance SET archive_sha256=?',(sha,))
    if file=='search':
     db.execute("UPDATE part SET role=?,characters=?,utf8_bytes=? WHERE id=1",
                ("user",len(text1),len(text1.encode())))
     db.execute('INSERT INTO part VALUES(2,?,?,?,?,?,?)',('c1','m2','assistant',0,len(text2),len(text2.encode())))
     db.execute('INSERT INTO text_index(rowid,text) VALUES(2,?)',(text2,))
  with redirect_stdout(io.StringIO()):build(self.root,'deployed',None,self.out,3,200)
  with sqlite3.connect(self.out) as db:
   branches=dict(db.execute('SELECT id,branch FROM message'))
   self.assertEqual(branches,{'m1':'main','m2':'main','m3':'alternate'})
   self.assertGreaterEqual(db.execute('SELECT count(*) FROM assertion').fetchone()[0],2)
   self.assertEqual(db.execute("SELECT count(*) FROM verification_queue").fetchone()[0],1)
   self.assertEqual(db.execute("SELECT count(*) FROM requested_work").fetchone()[0],1)
   reqid,reqhash=db.execute("SELECT id,text_sha256 FROM assertion WHERE kind='request'").fetchone()
   statusid,statushash=db.execute("SELECT id,text_sha256 FROM assertion WHERE kind='historical_status_claim'").fetchone()
  with self.assertRaises(SystemExit):
   editorial_review(self.out,statusid,statushash,'user:19','include','This was an implementation assertion')
  with redirect_stdout(io.StringIO()):
   editorial_review(self.out,reqid,reqhash,'user:19','include','Actual user requirement worth preserving')
   editorial_review(self.out,statusid,statushash,'user:19','needs_owner','Require current native owner evidence')
  out=io.StringIO()
  with redirect_stdout(out):wiki_draft(self.out,'notes/worldgraph-review')
  self.assertIn('Actual user requirement worth preserving',out.getvalue())
  self.assertIn('Published: false',out.getvalue())
  self.assertNotIn('Require current native owner evidence',out.getvalue())
  with sqlite3.connect(self.out) as db:
   self.assertEqual(db.execute('SELECT count(*) FROM review_event').fetchone()[0],2)
if __name__=="__main__":unittest.main()
