#!/usr/bin/env python3
"""Disposable Git regression tests for Bonfyre private release convergence."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

MAIN = Path(__file__).resolve().parents[1] / "converge.py"

def run(*args, check=True):
    p = subprocess.run(args, capture_output=True, text=True)
    if check and p.returncode:
        raise AssertionError("Fixture command failed: " + str(args[:3]) + " " + p.stderr[-300:])
    return p

def sha(data):
    return hashlib.sha256(data).hexdigest()

def git(root, *args):
    return run("git", "-C", str(root), *args)

class ConvergenceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.repo = self.root / "repo"
        self.repo.mkdir()
        git(self.repo, "init", "-q", "-b", "fixture")
        git(self.repo, "config", "user.name", "Fixture")
        git(self.repo, "config", "user.email", "fixture@example.test")
        (self.repo / "README").write_text("baseline\n")
        git(self.repo, "add", "README")
        git(self.repo, "commit", "-qm", "baseline")
        self.head = git(self.repo, "rev-parse", "--short=12", "HEAD").stdout.strip()
        (self.repo / "mcp").mkdir()
        (self.repo / "mcp/README.md").write_text("prior staged README\n")
        (self.repo / "mcp/bonfyre_mcp_gateway.py").write_text("prior Python transport\n")
        git(self.repo, "add", "mcp/README.md", "mcp/bonfyre_mcp_gateway.py")
        (self.repo / "mcp/README.md").write_text("current native MCP README\n")
        (self.repo / "mcp/bonfyre_mcp_gateway.py").unlink()
        self.archive = "scripts/source.py.archive"
        self.current = "scripts/source.mjs"
        (self.repo / "scripts").mkdir()
        (self.repo / self.archive).write_text("original archived Python source\n")
        (self.repo / self.current).write_text("export const ready = true;\n")
        self.published = "cmd/BonfyreBridge/src/main.c"
        (self.repo / self.published).parent.mkdir(parents=True)
        (self.repo / self.published).write_text("int bridge(void){return 1;}\n")
        self.report = self.root / "private.json"
        self.rows = [{"path": "synthetic/unused-" + str(i), "origin": "untracked",
                      "class": "REVIEW"} for i in range(1000)]
        for name in (self.archive, self.current, self.published):
            data = (self.repo / name).read_bytes()
            self.rows.append({"path": name, "origin": "untracked",
                              "class": "CANDIDATE" if name != self.archive else "REVIEW",
                              "inspected": True, "findings": [], "sha256": sha(data),
                              "bytes": len(data)})
        self.refresh()
        self.manifests = self.root / "manifests"
        self.manifests.mkdir()
        self.private = self.root / "private-out"
        self.manifest()
    def refresh(self):
        self.report.write_text(json.dumps({
            "schema": "bonfyre.release.privacy_preflight.v1",
            "head": self.head, "branch": "fixture", "entries": self.rows}))
    def manifest(self):
        data = (self.repo / self.published).read_bytes()
        self.manifests.joinpath("native_bridge.json").write_text(json.dumps({
            "schema": "bonfyre.public_release.candidate.v1",
            "head": self.head, "branch": "fixture",
            "source_report_sha256": sha(self.report.read_bytes()),
            "files": [{"path": self.published, "sha256": sha(data),
                       "bytes": len(data), "observed_origin": "untracked"}]}))
    def invoke(self, *args):
        return run(sys.executable, str(MAIN), "--repo", str(self.repo),
                   "--report", str(self.report), "--manifests", str(self.manifests),
                   "--private-out", str(self.private), "--expected-head", self.head,
                   "--expected-branch", "fixture", *args, check=False)

    def test_dry_run(self):
        p = self.invoke()
        self.assertEqual(p.returncode, 0, p.stderr)
        self.assertIn("1 verified archived replacements", p.stdout)
        self.assertFalse(self.private.exists())
        self.assertEqual(len(git(self.repo, "diff", "--cached", "--name-only").stdout.splitlines()), 2)

    @unittest.skip("In-checkout orphan releases retired; isolated review now owns Git.")
    def test_full_local_convergence_and_orphan_ref(self):
        p = self.invoke("--apply", "--prepare-local-release",
                        "--release-branch", "bonfyre-public/isolated-fixture")
        self.assertEqual(p.returncode, 0, p.stderr)
        self.assertFalse((self.repo / self.archive).exists())
        self.assertEqual((self.private / "retired-archives" / self.archive).read_text(),
                         "original archived Python source\n")
        self.assertTrue((self.repo / self.current).exists())
        self.assertEqual(git(self.repo, "diff", "--cached", "--name-only").stdout, "")
        self.assertEqual(git(self.repo, "rev-parse", "--short=12", "HEAD").stdout.strip(), self.head)
        ref = "bonfyre-public/isolated-fixture"
        commit = git(self.repo, "rev-parse", ref).stdout.strip()
        self.assertEqual(git(self.repo, "rev-list", "--parents", "-n", "1", commit).stdout.strip(), commit)
        self.assertEqual(git(self.repo, "ls-tree", "-r", "--name-only", ref).stdout.strip(), self.published)
        self.assertEqual(self.invoke("--apply").returncode, 2)

    def test_in_checkout_release_ref_is_refused_before_side_effects(self):
        result = self.invoke("--apply", "--prepare-local-release",
                             "--release-branch", "bonfyre-public/unsafe-source-repo")
        self.assertEqual(result.returncode, 2)
        self.assertFalse(self.private.exists())
        self.assertEqual(len(git(self.repo, "diff", "--cached", "--name-only").stdout.splitlines()), 2)

    def test_unknown_staged_work_refused_before_side_effects(self):
        (self.repo / "unrelated").write_text("another agent")
        git(self.repo, "add", "unrelated")
        self.assertEqual(self.invoke("--apply").returncode, 2)
        self.assertFalse(self.private.exists())
        self.assertTrue((self.repo / self.archive).exists())

    def test_git_index_lock_refused_without_creating_output(self):
        lock = self.repo / ".git/index.lock"
        lock.write_text("held by another process")
        self.assertEqual(self.invoke("--apply").returncode, 2)
        self.assertTrue(lock.exists())
        self.assertFalse(self.private.exists())
        self.assertTrue((self.repo / self.archive).exists())

    def test_source_changed_does_not_enter_private_public_review(self):
        (self.repo / self.published).write_text("int bridge(void){return 2;}\n")
        p = self.invoke("--apply")
        self.assertEqual(p.returncode, 0, p.stderr)
        self.assertIn("COHORTS REFUSED: native_bridge", p.stdout)
        self.assertFalse((self.private / "public-review" / "native_bridge").exists())

    def test_credential_pattern_refused_even_with_candidate_label(self):
        data = b'SECRET="fixture-but-sensitive-production-shaped-credential"\n'
        (self.repo / self.published).write_bytes(data)
        next(row for row in self.rows if row["path"] == self.published).update(
            sha256=sha(data), bytes=len(data))
        self.refresh()
        self.manifest()
        p = self.invoke()
        self.assertEqual(p.returncode, 0, p.stderr)
        self.assertIn("COHORTS REFUSED: native_bridge", p.stdout)
        self.assertNotIn("sensitive-production-shaped", p.stdout + p.stderr)

if __name__ == "__main__":
    unittest.main(verbosity=2)
