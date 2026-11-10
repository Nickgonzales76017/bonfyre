#!/usr/bin/env python3
"""Review-layer tests using the synthetic ChatGPT export, never user data."""
from contextlib import redirect_stdout
import io
from pathlib import Path
import sqlite3
import sys
import unittest

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from chatgpt_evidence_graph import current_reviews,draft,exact_evidence,inspect,review,source_digest
import test_chatgpt_export_reader as fixture_module

GOAL="0123456789abcdef0123456789abcdef"
CONV="c1"
MSG="m1"
ROUTE="notes/world-graph"

class GraphTest(unittest.TestCase):
 def setUp(self):
  self.fixture=fixture_module.TestReader()
  self.fixture.setUp()
  self.root=self.fixture.root
  self.addCleanup(self.fixture.tearDown)
 def test_goal_discovery(self):
  from chatgpt_evidence_graph import main
  from unittest.mock import patch
  out=io.StringIO()
  with patch.object(sys,"argv",["reader","--root",str(self.root),"goals","--limit","1"]):
   with redirect_stdout(out):main()
  self.assertIn("GOAL\tMESSAGES\tMENTIONS",out.getvalue())
  self.assertIn(GOAL,out.getvalue())
 def test_separated_mention_and_review(self):
  out=io.StringIO()
  with redirect_stdout(out):inspect(self.root,GOAL)
  self.assertIn("EXACT_MENTIONS\t1",out.getvalue())
  self.assertIn("REVIEWED_LINKS\t0",out.getvalue())
  self.assertEqual(current_reviews(self.root,source_digest(self.root),GOAL),{})
  with self.assertRaises(SystemExit):draft(self.root,GOAL,CONV,ROUTE)
  with redirect_stdout(io.StringIO()):
   review(self.root,GOAL,CONV,MSG,1,ROUTE,"user:19","review")
  links=current_reviews(self.root,source_digest(self.root),GOAL)
  self.assertEqual(len(links),1)
  out=io.StringIO()
  with redirect_stdout(out):draft(self.root,GOAL,CONV,ROUTE)
  self.assertIn("Published: false",out.getvalue())
  self.assertIn("Allow guest: false",out.getvalue())
  self.assertIn("source part 1",out.getvalue())
  with redirect_stdout(io.StringIO()):
   review(self.root,GOAL,CONV,MSG,1,ROUTE,"user:19","revoke")
  self.assertEqual(current_reviews(self.root,source_digest(self.root),GOAL),{})
  with sqlite3.connect(self.root/"chatgpt-20261028-reviewed-links.sqlite3") as db:
   self.assertEqual(db.execute("SELECT count(*) FROM review_event").fetchone()[0],2)
  with self.assertRaises(SystemExit):draft(self.root,GOAL,CONV,ROUTE)
 def test_wrong_evidence_and_route_refused(self):
  for kwargs in [
   dict(goal="a"*32,conversation=CONV,message=MSG,part_id=1,route=ROUTE,actor="user:19"),
   dict(goal=GOAL,conversation=CONV,message=MSG,part_id=9,route=ROUTE,actor="user:19"),
   dict(goal=GOAL,conversation=CONV,message=MSG,part_id=1,route="../published",actor="user:19"),
   dict(goal=GOAL,conversation=CONV,message=MSG,part_id=1,route=ROUTE,actor="../user")
  ]:
   with self.assertRaises(SystemExit):review(self.root,operation="review",**kwargs)
  self.assertFalse((self.root/"chatgpt-20261028-reviewed-links.sqlite3").exists())
 def test_original_changed_refuses_link(self):
  with self.fixture.zpath.open("ab") as f:f.write(b"drift")
  with self.assertRaises(SystemExit):review(self.root,GOAL,CONV,MSG,1,ROUTE,"user:19","review")
  self.assertFalse((self.root/"chatgpt-20261028-reviewed-links.sqlite3").exists())

if __name__=="__main__":unittest.main()
