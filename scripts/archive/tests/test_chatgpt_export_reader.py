#!/usr/bin/env python3
"""Synthetic regression; never opens the real private export."""
import hashlib,json,sqlite3,subprocess,sys,tempfile,unittest,zipfile
from pathlib import Path
READER=Path(__file__).resolve().parents[1]/"chatgpt_export_reader.py"

class TestReader(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name)
  self.source="conversations-000.json";self.part=chr(96)*3+'python\nprint("hi")\n'+chr(96)*3+'\nTask 0123456789abcdef0123456789abcdef'
  rows=[{"conversation_id":"c1","title":"One","mapping":{"x":{"message":{"id":"m1","content":{"parts":[self.part]}}}}}]
  self.zpath=self.root/"source.zip"
  with zipfile.ZipFile(self.zpath,"w",zipfile.ZIP_DEFLATED) as z:z.writestr(self.source,json.dumps(rows))
  stat=self.zpath.stat();sha=hashlib.sha256(self.zpath.read_bytes()).hexdigest()
  prefix="chatgpt-20261028-"
  with sqlite3.connect(self.root/(prefix+"member-manifest.sqlite3")) as db:
   db.executescript("CREATE TABLE archive(path,size,mtime_ns,sha256);CREATE TABLE member(path,zip_bytes,raw_bytes,sha256,is_directory);")
   db.execute("INSERT INTO archive VALUES(?,?,?,?)",(str(self.zpath),stat.st_size,stat.st_mtime_ns,sha))
   db.execute("INSERT INTO member VALUES(?,?,?,?,?)",(self.source,100,120,'0'*64,0))
  for name in ["search","code-references","goal-references"]:
   with sqlite3.connect(self.root/(prefix+name+".sqlite3")) as db:
    db.execute("CREATE TABLE provenance(archive_sha256)");db.execute("INSERT INTO provenance VALUES(?)",(sha,))
    if name=="search":
     db.executescript("CREATE TABLE conversation(id,source_file,source_position,title);CREATE TABLE part(id,conversation_id,message_id,role,part_index,characters,utf8_bytes);CREATE VIRTUAL TABLE text_index USING fts5(text,content='');")
     db.execute("INSERT INTO conversation VALUES('c1',?,1,'One')",(self.source,))
     db.execute("INSERT INTO part VALUES(1,'c1','m1','assistant',0,?,?)",(len(self.part),len(self.part.encode())))
     db.execute("INSERT INTO text_index(rowid,text) VALUES(1,?)",(self.part,))
    elif name=="code-references":
     db.execute("CREATE TABLE code_block(part_id,block_index,body_start,body_end,sha256,utf8_bytes,language)")
     body=b'print("hi")\n'
     db.execute("INSERT INTO code_block VALUES(1,0,?,?,?,?,?)",(self.part.index("print"),self.part.index("print")+len(body),hashlib.sha256(body).hexdigest(),len(body),"python"))
    else:
     db.execute("CREATE TABLE goal_reference(goal_id,conversation_id,message_id,role,conversation_file,occurrences,goal_state_at_scan)")
     db.execute("INSERT INTO goal_reference VALUES(?,?,?,?,?,?,?)",("0123456789abcdef0123456789abcdef","c1","m1","assistant",self.source,1,"queued"))
 def tearDown(self):self.tmp.cleanup()
 def run_reader(self,*argv):return subprocess.run([sys.executable,str(READER),"--root",str(self.root),*argv],capture_output=True)
 def test_search_status(self):
  a=self.run_reader("search","print");self.assertEqual(a.returncode,0,a.stderr)
  self.assertEqual(json.loads(a.stdout)["conversation_id"],"c1")
  a=self.run_reader("status");self.assertEqual(a.returncode,0,a.stderr)
  self.assertEqual(json.loads(a.stdout)["detected_code_fences"],1)
 def test_code_task(self):
  a=self.run_reader("code","1","0");self.assertEqual(a.returncode,0,a.stderr)
  self.assertEqual(a.stdout,b'print("hi")\n');self.assertIn(b"VERIFIED",a.stderr)
  a=self.run_reader("task","0123456789abcdef0123456789abcdef");self.assertEqual(a.returncode,0,a.stderr)
  self.assertEqual(json.loads(a.stdout)["historical_state"],"queued")
 def test_show_and_scoped_retrieval(self):
  a=self.run_reader("show","1");self.assertEqual(a.returncode,0,a.stderr)
  self.assertEqual(a.stdout,self.part.encode());self.assertIn(b"VERIFIED part=1",a.stderr)
  a=self.run_reader("show","999");self.assertNotEqual(a.returncode,0)
  a=self.run_reader("search","print","--role","assistant","--title","one")
  self.assertEqual(a.returncode,0,a.stderr);self.assertEqual(json.loads(a.stdout)["part_id"],1)
  for flags in [("--role","user"),("--title","not-present"),("--title","%")]:
   a=self.run_reader("search","print",*flags);self.assertEqual(a.returncode,0,a.stderr)
   self.assertEqual(a.stdout,b"")
 def test_changed_zip_refusal(self):
  with self.zpath.open("ab") as f:f.write(b"bad")
  a=self.run_reader("search","print");self.assertNotEqual(a.returncode,0)
  self.assertIn(b"ZIP changed",a.stderr)
if __name__=="__main__":unittest.main()
