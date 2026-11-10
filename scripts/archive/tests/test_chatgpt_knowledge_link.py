#!/usr/bin/env python3
"""Private evidence-case regression over a synthetic authoritative ZIP."""
from contextlib import redirect_stdout
import io
import os
from pathlib import Path
import sqlite3
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import test_chatgpt_export_reader as fixture_module
from chatgpt_knowledge_link import case,cite,read_links,exact_part,main
from chatgpt_evidence_graph import source_digest

ROUTE="notes/worldgraph-historical-evidence-20261030"

class KnowledgeCaseTest(unittest.TestCase):
 def setUp(self):
  self.fixture=fixture_module.TestReader()
  self.fixture.setUp()
  self.root=self.fixture.root
  self.addCleanup(self.fixture.tearDown)
 def test_separate_reviewed_source_from_task(self):
  page=case(self.root,1,ROUTE)
  self.assertIn("Source candidate — not reviewed",page)
  self.assertIn("Exact ID mentioned; work owner/state not verified",page)
  self.assertIn("No reviewed task-to-Wiki associations",page)
  self.assertIn("Recovered original message",page)
  self.assertNotIn("<script>",page)
  with redirect_stdout(io.StringIO()):cite(self.root,1,ROUTE,"user:19","cite")
  current=read_links(self.root,source_digest(self.root),"c1")
  self.assertEqual(len(current),1)
  linked=case(self.root,1,ROUTE)
  self.assertIn("Reviewed source citation",linked)
  self.assertIn("reviewed by user:19",linked)
  self.assertIn("No reviewed task-to-Wiki associations",linked)
  with redirect_stdout(io.StringIO()):cite(self.root,1,ROUTE,"user:19","revoke")
  self.assertEqual(read_links(self.root,source_digest(self.root),"c1"),{})
  with sqlite3.connect(self.root/"chatgpt-20261028-knowledge-links.sqlite3") as db:
   self.assertEqual(db.execute("SELECT count(*) FROM source_link_event").fetchone()[0],2)
  self.assertIn("Source candidate — not reviewed",case(self.root,1,ROUTE))
 def test_reject_wrong_source_or_public_route(self):
  for part,route in [(999,ROUTE),(1,"../public"),(1,"/wiki/page"),(1,"notes/../../private")]:
   with self.assertRaises(SystemExit):cite(self.root,part,route,"user:19","cite")
  self.assertFalse((self.root/"chatgpt-20261028-knowledge-links.sqlite3").exists())
 def test_reject_source_zip_drift(self):
  with self.fixture.zpath.open("ab") as f:f.write(b"changed")
  with self.assertRaises(SystemExit):case(self.root,1,ROUTE)
  self.assertFalse((self.root/"chatgpt-20261028-knowledge-links.sqlite3").exists())
 def test_private_html_output_and_no_overwrite(self):
  out=self.root/"case.html"
  with patch.object(sys,"argv",["case","--root",str(self.root),"case","--part","1","--wiki-route",ROUTE,"--out",str(out)]):
   with redirect_stdout(io.StringIO()):main()
  self.assertTrue(out.is_file())
  self.assertIn("Private Evidence Case",out.read_text())
  self.assertEqual(out.stat().st_mode & 0o777,0o600)
  with patch.object(sys,"argv",["case","--root",str(self.root),"case","--part","1","--out",str(out)]):
   with self.assertRaises(SystemExit):
    with redirect_stdout(io.StringIO()):main()

if __name__=="__main__":
 unittest.main()
