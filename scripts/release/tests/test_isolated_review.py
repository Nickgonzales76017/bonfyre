#!/usr/bin/env python3
"""Disposable Git and privacy-contract tests for isolated Bonfyre review."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
CONVERGE = ROOT/"converge.py"
ISOLATE = ROOT/"isolated_review.py"

def sha(data): return hashlib.sha256(data).hexdigest()
def run(*cmd):
    return subprocess.run(cmd, capture_output=True, text=True)

class IsolatedReviewTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name)
        self.repo = self.base/"repo"; self.repo.mkdir()
        assert run("git","-C",str(self.repo),"init","-q","-b","fixture").returncode == 0
        run("git","-C",str(self.repo),"config","user.name","Fixture")
        run("git","-C",str(self.repo),"config","user.email","fixture@example.test")
        (self.repo/"start").write_text("start\n")
        run("git","-C",str(self.repo),"add","start")
        assert run("git","-C",str(self.repo),"commit","-qm","base").returncode == 0
        self.head = run("git","-C",str(self.repo),"rev-parse","--short=12","HEAD").stdout.strip()
        (self.repo/"mcp").mkdir()
        (self.repo/"mcp/README.md").write_text("staged old\n")
        (self.repo/"mcp/bonfyre_mcp_gateway.py").write_text("staged gateway\n")
        run("git","-C",str(self.repo),"add","mcp/README.md","mcp/bonfyre_mcp_gateway.py")
        (self.repo/"mcp/bonfyre_mcp_gateway.py").unlink()
        (self.repo/"mcp/README.md").write_text("current README\n")
        self.name="cmd/BonfyreBridge/src/main.c"
        (self.repo/self.name).parent.mkdir(parents=True)
        (self.repo/self.name).write_text("int main(void){return 0;}\n")
        self.report=self.base/"preflight.json"; self.manifests=self.base/"manifests"
        self.manifests.mkdir()
        self.out=self.base/"private"
        self.snapshot={"path":self.name,"origin":"untracked","class":"CANDIDATE",
                       "inspected":True,"findings":[],"sha256":sha((self.repo/self.name).read_bytes()),
                       "bytes":(self.repo/self.name).stat().st_size}
        body={"schema":"bonfyre.release.privacy_preflight.v1","head":self.head,
              "branch":"fixture","entries":[self.snapshot]+[
                  {"path":f"generated/empty{i}","origin":"untracked"} for i in range(1000)]}
        self.report.write_text(json.dumps(body))
        self.manifests.joinpath("native_bridge.json").write_text(json.dumps({
            "schema":"bonfyre.public_release.candidate.v1","head":self.head,
            "branch":"fixture","source_report_sha256":sha(self.report.read_bytes()),
            "files":[{"path":self.name,"observed_origin":"untracked",
                      "sha256":self.snapshot["sha256"],"bytes":self.snapshot["bytes"]}]}))
    def converge(self):
        return run(sys.executable,str(CONVERGE),"--repo",str(self.repo),"--report",
                   str(self.report),"--manifests",str(self.manifests),
                   "--private-out",str(self.out),"--expected-head",self.head,
                   "--expected-branch","fixture","--apply")
    def isolate(self):
        return run(sys.executable,str(ISOLATE),"--repo",str(self.repo),"--report",
                   str(self.report),"--private-out",str(self.out))
    def test_isolated_git_has_no_remote_or_development_history(self):
        pre=self.converge()
        self.assertEqual(pre.returncode,0,pre.stderr)
        created=self.isolate()
        self.assertEqual(created.returncode,0,created.stderr)
        own=self.out/"isolated-public-git"
        self.assertEqual(run("git","-C",str(own),"remote").stdout,"")
        self.assertEqual(run("git","-C",str(own),"branch","--show-current").stdout.strip(),
                         "bonfyre-public/source-review-20261012")
        commit=run("git","-C",str(own),"rev-parse","HEAD").stdout.strip()
        self.assertEqual(run("git","-C",str(own),"rev-list","--parents","-n","1","HEAD").stdout.strip(),commit)
        self.assertEqual(run("git","-C",str(own),"ls-tree","-r","--name-only","HEAD").stdout.strip(),self.name)
        self.assertTrue((own/".git/hooks/pre-push").exists())
        self.assertEqual(run("git","-C",str(self.repo),"rev-parse","--short=12","HEAD").stdout.strip(),self.head)
        self.assertEqual(run("git","-C",str(self.repo),"branch","--list","bonfyre-public/*").stdout,"")
        self.assertEqual(run("git","-C",str(self.repo),"diff","--cached","--name-only").stdout,"")
    def test_snapshot_tampering_refused_before_git_creation(self):
        self.assertEqual(self.converge().returncode,0)
        sample=self.out/"public-review/native_bridge"/self.name
        sample.write_text("CREDENTIAL='wrong'\n")
        res=self.isolate()
        self.assertEqual(res.returncode,2)
        self.assertFalse((self.out/"isolated-public-git").exists())
    def test_late_worktree_change_refused(self):
        self.assertEqual(self.converge().returncode,0)
        (self.repo/self.name).write_text("int different(void){return 3;}\n")
        res=self.isolate()
        self.assertEqual(res.returncode,2)
        self.assertNotIn("different(void)",res.stderr)
        self.assertEqual(run("git","-C",str(self.repo),"diff","--cached","--name-only").stdout,"")

if __name__=="__main__": unittest.main(verbosity=2)
