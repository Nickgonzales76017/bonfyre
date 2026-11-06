#!/usr/bin/env python3
"""No-network, disposable-Git regression of index-only legacy MCP recovery."""
import hashlib
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

HELPER = Path(__file__).resolve().parents[1] / 'resolve_legacy_mcp_index.py'
PATHS = ('mcp/README.md', 'mcp/bonfyre_mcp_gateway.py')

def command(root, *args, check=True):
    r=subprocess.run(['git','-C',str(root),*args],capture_output=True,text=True)
    if check and r.returncode:
        raise AssertionError('Git fixture setup failed: '+r.stderr)
    return r

def fixture(root, extra=False):
    root.mkdir()
    command(root,'init','-q')
    command(root,'config','user.email','fixture@example.test')
    command(root,'config','user.name','Fixture')
    (root/'base.txt').write_text('baseline\n')
    command(root,'add','base.txt')
    command(root,'commit','-qm','baseline')
    (root/'mcp').mkdir()
    (root/PATHS[0]).write_text('old readme in worktree\n')
    (root/PATHS[1]).write_text('print("legacy test")\n')
    command(root,'add',*PATHS)
    (root/PATHS[1]).unlink()
    if extra:
        (root/'unrelated.txt').write_text('another agent\n')
        command(root,'add','unrelated.txt')
    return command(root,'rev-parse','--short=12','HEAD').stdout.strip(), command(root,'branch','--show-current').stdout.strip()

class IndexRecovery(unittest.TestCase):
    def test_resume_existing_private_backup_and_clear_only_index(self):
        with tempfile.TemporaryDirectory() as tmp:
            base=Path(tmp);repo=base/'repo';head,branch=fixture(repo)
            folder=base/'private-backup';folder.mkdir();(folder/'mcp').mkdir()
            manifest=''
            for p in PATHS:
                data=command(repo,'show',':'+p).stdout.encode()
                (folder/p).write_bytes(data)
                manifest += hashlib.sha256(data).hexdigest()+'  '+p+'\n'
            (folder/'BACKUP_SHA256.txt').write_text(manifest)
            cmd=[sys.executable,str(HELPER),'--repo',str(repo),
                 '--expected-head',head,'--expected-branch',branch,
                 '--backup-dir',str(folder)]
            read=subprocess.run(cmd,capture_output=True,text=True)
            self.assertEqual(read.returncode,0,read.stderr)
            run=subprocess.run([*cmd,'--apply'],capture_output=True,text=True)
            self.assertEqual(run.returncode,0,run.stderr)
            self.assertIn('resumed existing verified backup',run.stdout)
            self.assertEqual(command(repo,'diff','--cached','--name-only').stdout,'')
            self.assertEqual((repo/PATHS[0]).read_text(),'old readme in worktree\n')
            self.assertFalse((repo/PATHS[1]).exists())
            self.assertEqual(command(repo,'rev-parse','--short=12','HEAD').stdout.strip(),head)
            self.assertEqual(subprocess.run([*cmd,'--apply'],capture_output=True).returncode,2)

    def test_refuses_unknown_staged_changes_without_mutation(self):
        with tempfile.TemporaryDirectory() as tmp:
            base=Path(tmp);repo=base/'repo';head,branch=fixture(repo,extra=True)
            cmd=[sys.executable,str(HELPER),'--repo',str(repo),
                 '--expected-head',head,'--expected-branch',branch,
                 '--apply','--backup-dir',str(base/'backup')]
            r=subprocess.run(cmd,capture_output=True,text=True)
            self.assertEqual(r.returncode,2)
            self.assertIn('exactly the two',r.stderr)
            self.assertFalse((base/'backup').exists())
            self.assertEqual(len(command(repo,'diff','--cached','--name-only').stdout.splitlines()),3)

    def test_git_index_lock_blocks_and_preserves_backup(self):
        with tempfile.TemporaryDirectory() as tmp:
            base=Path(tmp);repo=base/'repo';head,branch=fixture(repo)
            # Real Git locking: never clear another process's index.lock.
            lock=repo/'.git/index.lock';lock.write_text('held by another process')
            cmd=[sys.executable,str(HELPER),'--repo',str(repo),
                 '--expected-head',head,'--expected-branch',branch,
                 '--apply','--backup-dir',str(base/'backup')]
            r=subprocess.run(cmd,capture_output=True,text=True)
            self.assertEqual(r.returncode,2)
            self.assertIn('Index update refused',r.stderr)
            self.assertTrue(lock.exists())
            self.assertTrue((base/'backup/BACKUP_SHA256.txt').is_file())
            self.assertEqual(len(command(repo,'diff','--cached','--name-only').stdout.splitlines()),2)

    def test_bad_existing_backup_refused_without_index_changes(self):
        with tempfile.TemporaryDirectory() as tmp:
            base=Path(tmp);repo=base/'repo';head,branch=fixture(repo)
            folder=base/'backup';folder.mkdir()
            (folder/'BACKUP_SHA256.txt').write_text('bad')
            cmd=[sys.executable,str(HELPER),'--repo',str(repo),
                 '--expected-head',head,'--expected-branch',branch,
                 '--apply','--backup-dir',str(folder)]
            r=subprocess.run(cmd,capture_output=True,text=True)
            self.assertEqual(r.returncode,2)
            self.assertEqual(len(command(repo,'diff','--cached','--name-only').stdout.splitlines()),2)

if __name__=='__main__':
    unittest.main(verbosity=2)
