#!/usr/bin/env python3
"""End-to-end synthetic ZIP member restoration and low-space refusals."""
import hashlib
import importlib.util
import io
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch
import zipfile

SCRIPT=Path(__file__).resolve().parents[1]/"chatgpt_archive_bundle.py"
spec=importlib.util.spec_from_file_location("chatgpt_archive_bundle",SCRIPT)
bundle=importlib.util.module_from_spec(spec);spec.loader.exec_module(bundle)
SHA=lambda b:hashlib.sha256(b).hexdigest()

class BundleTest(unittest.TestCase):
 def setUp(self):
  self.home=tempfile.TemporaryDirectory();self.addCleanup(self.home.cleanup)
  self.root=Path(self.home.name);self.original=self.root/"export.zip"
  with zipfile.ZipFile(self.original,"w",zipfile.ZIP_DEFLATED) as z:
   z.comment=b"chatgpt-source"
   z.writestr("folder/",b"")
   z.writestr("folder/a.bin",b"\x00\x01\x02hello"*113)
   z.writestr("folder/b.bin",b"\x00\x01\x02hello"*113)
   z.writestr("folder/a.bin",b"X"*(len(b"\x00\x01\x02hello")*113)) # same path and size, different bytes
  stat=self.original.stat()
  sha=bundle.digest_file(self.original)
  with sqlite3.connect(self.root/"chatgpt-20261028-member-manifest.sqlite3") as db:
   db.executescript("CREATE TABLE archive(path,size,mtime_ns,sha256);CREATE TABLE member(path,sha256,raw_bytes,is_directory,zip_bytes);")
   db.execute("INSERT INTO archive VALUES(?,?,?,?)",(str(self.original),stat.st_size,stat.st_mtime_ns,sha))
   with zipfile.ZipFile(self.original) as z:
    for info in z.infolist():
     data=z.read(info)
     db.execute("INSERT INTO member VALUES(?,?,?,?,?)",(info.filename,SHA(data) if not info.is_dir() else "",info.file_size,int(info.is_dir()),info.compress_size))
  self.bundle=self.root/"bundle.zip";self.restored=self.root/"restored.zip"

 def test_plan_rejects_unavailable_space(self):
  report=bundle.plan(self.root,self.bundle,1<<30)
  self.assertEqual(report["members"],4)
  self.assertEqual(report["unique_payloads"],2)
  with patch.object(bundle.shutil,"disk_usage") as m:
   m.return_value=type("Usage",(),{"free":4})()
   with self.assertRaises(SystemExit):
    bundle.space_guard(self.bundle,1024,1024)

 def test_full_restoration(self):
  with patch.object(bundle,"space_guard"):
   bundle.build(self.root,self.bundle,1<<30)
   doc=bundle.verify_bundle(self.bundle)
   self.assertEqual(len(doc["members"]),4)
   with zipfile.ZipFile(self.bundle) as z:
     self.assertEqual(len(z.namelist()),3) # two unique content blobs and the manifest
   bundle.restore(self.bundle,self.restored,1<<30)
  with zipfile.ZipFile(self.original) as a,zipfile.ZipFile(self.restored) as b:
   self.assertEqual(a.comment,b.comment)
   self.assertEqual([i.filename for i in a.infolist()],[i.filename for i in b.infolist()])
   self.assertEqual([a.open(i).read() for i in a.infolist()],
                    [b.open(i).read() for i in b.infolist()])
  self.assertTrue(self.original.exists())

 def test_bounded_streaming_roundtrip(self):
  with patch.object(bundle,"space_guard"):
   bundle.build(self.root,self.bundle,1<<30,bounded=True)
   bundle.restore(self.bundle,self.restored,1<<30)
  with zipfile.ZipFile(self.original) as before,zipfile.ZipFile(self.restored) as after:
   self.assertEqual([before.open(i).read() for i in before.infolist()],
                    [after.open(i).read() for i in after.infolist()])
  self.assertTrue(self.original.exists())

 def test_bounded_streaming_refuses_and_removes_partial(self):
  with patch.object(bundle,"space_guard"):
   with patch.object(bundle.shutil,"disk_usage") as usage:
    usage.return_value=type("Usage",(),{"free":bundle.OVERHEAD+1024})()
    with self.assertRaises(SystemExit):
     bundle.build(self.root,self.bundle,1<<30,bounded=True)
  self.assertFalse(self.bundle.exists())
  self.assertEqual(list(self.root.glob(".*.partial")),[])

 def test_original_drift_refused(self):
  self.original.write_bytes(self.original.read_bytes()+b"altered")
  with self.assertRaises(SystemExit):
   bundle.plan(self.root,self.bundle,1<<30)
  self.assertFalse(self.bundle.exists())

 def test_extra_blob_refused(self):
  with patch.object(bundle,"space_guard"):
   bundle.build(self.root,self.bundle,1<<30)
  with zipfile.ZipFile(self.bundle,"a") as z:z.writestr("blobs/fake",b"attack")
  with self.assertRaises(SystemExit):
   bundle.verify_bundle(self.bundle)

if __name__=="__main__":
 unittest.main()
