#!/usr/bin/env python3
"""Disposable end-to-end Mac Git fixture for the v2 release driver.

Stubs only the scanner and manifest builder. Uses the real convergence, index
repair and isolated-review implementations, against a throwaway repository.
"""
import hashlib
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

OWNER = Path(__file__).resolve().parents[1]
SOURCES = (
    "finish_private_convergence.sh", "finish_private_convergence_v2.sh", "converge.py",
    "isolated_review.py", "resolve_legacy_mcp_index.py"
)

def git(repo, *args):
    r = subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=True)
    if r.returncode:
        raise AssertionError("Fixture Git setup failed: " + r.stderr[-450:])
    return r.stdout.strip()

SCANNER = r'''import argparse,hashlib,json
from pathlib import Path
a=argparse.ArgumentParser()
a.add_argument('--repo');a.add_argument('--out')
opts,_=a.parse_known_args()
repo=Path(opts.repo)
head=__import__('subprocess').check_output(['git','-C',str(repo),'rev-parse','--short=12','HEAD']).decode().strip()
branch=__import__('subprocess').check_output(['git','-C',str(repo),'branch','--show-current']).decode().strip()
entries=[{'path':'irrelevant/entry'+str(i),'origin':'untracked','class':'REVIEW'} for i in range(1000)]
for name in ['cmd/BonfyreCapability/src/main.c','cmd/BonfyreBridge/src/main.c','scripts/source.py.archive']:
    data=(repo/name).read_bytes()
    entries.append({'path':name,'origin':'untracked','class':'CANDIDATE',
                    'inspected':True,'findings':[],'sha256':hashlib.sha256(data).hexdigest(),
                    'bytes':len(data)})
Path(opts.out).write_text(json.dumps({'schema':'bonfyre.release.privacy_preflight.v1',
        'head':head,'branch':branch,'entries':entries}))
print('Fixture privacy preflight completed')
'''
BUILDER = r'''import argparse,hashlib,json
from pathlib import Path
p=argparse.ArgumentParser()
p.add_argument('--repo');p.add_argument('--report');p.add_argument('--outdir')
o=p.parse_args()
raw=Path(o.report).read_bytes();data=json.loads(raw)
base=Path(o.outdir);base.mkdir(parents=True,exist_ok=True)
rows={x['path']:x for x in data['entries']}
for cohort,name in [('native_bridge','cmd/BonfyreBridge/src/main.c'),
                    ('native_capability','cmd/BonfyreCapability/src/main.c')]:
    if cohort=='native_capability' and (Path(o.repo)/'only_bad').exists():
        continue
    item=rows[name]
    m={'schema':'bonfyre.public_release.candidate.v1','head':data['head'],
       'branch':data['branch'],'source_report_sha256':hashlib.sha256(raw).hexdigest(),
       'files':[{'path':name,'sha256':item['sha256'],'bytes':item['bytes'],
                 'observed_origin':item['origin']}]}
    (base/(cohort+'.json')).write_text(json.dumps(m))
print('Fixture manifests written')
'''

class DriverV2Tests(unittest.TestCase):
    def setUp(self):
        self.temporary=tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.base=Path(self.temporary.name)
        self.repo=self.base/"repo"
        self.repo.mkdir()
        git(self.repo,"init","-q","-b","fixture")
        git(self.repo,"config","user.name","Fixture")
        git(self.repo,"config","user.email","fixture@example.test")
        (self.repo/"README.md").write_text("baseline\n")
        git(self.repo,"add","README.md")
        git(self.repo,"commit","-qm","baseline")
        self.head=git(self.repo,"rev-parse","--short=12","HEAD")
        release=self.repo/"scripts/release"
        release.mkdir(parents=True)
        for name in SOURCES: shutil.copy2(OWNER/name,release/name)
        (self.repo/"mcp").mkdir()
        (self.repo/"mcp/README.md").write_text("old staged readme\n")
        (self.repo/"mcp/bonfyre_mcp_gateway.py").write_text("old staged gateway\n")
        git(self.repo,"add","mcp/README.md","mcp/bonfyre_mcp_gateway.py")
        (self.repo/"mcp/bonfyre_mcp_gateway.py").unlink()
        (self.repo/"mcp/README.md").write_text("new unstaged readme\n")
        for file,contents in [
            ("cmd/BonfyreCapability/src/main.c","int capability(void) { return 1; }\n"),
            ("cmd/BonfyreBridge/src/main.c",'const char *secret = "looks-like-sensitive-secret";\n'),
            ("scripts/source.py.archive","archived implementation\n"),
            ("scripts/source.mjs","export const active = true;\n")]:
            target=self.repo/file
            target.parent.mkdir(parents=True,exist_ok=True)
            target.write_text(contents)
        self.tools=self.base/"tools"
        self.tools.mkdir()
        (self.tools/"bonfyre_privacy_preflight_v2.py").write_text(SCANNER)
        (self.tools/"build_manifests.py").write_text(BUILDER)
        self.prep=self.base/"prepared"
        self.private=self.base/"converged"
        self.env=os.environ.copy()
        self.env.update({
            "BONFYRE_RELEASE_REPO":str(self.repo),
            "BONFYRE_RELEASE_TOOLS":str(self.tools),
            "BONFYRE_RELEASE_PREP_ROOT":str(self.prep),
            "BONFYRE_RELEASE_PRIVATE_ROOT":str(self.private),
            "BONFYRE_RELEASE_EXPECTED_HEAD":self.head,
            "BONFYRE_RELEASE_EXPECTED_BRANCH":"fixture",
        })
    def run_driver(self):
        return subprocess.run(["bash",str(self.repo/"scripts/release/finish_private_convergence.sh")],
                              text=True,capture_output=True,env=self.env,timeout=35)
    def test_resume_prior_scan_hold_unsafe_cohort_and_complete_safe_cleanup(self):
        self.prep.mkdir()
        (self.prep/"old-audit-sentinel").write_text("preserve me")
        result=self.run_driver()
        self.assertEqual(result.returncode,0,result.stderr[-1200:])
        self.assertIn("PUBLIC COHORT HOLD",result.stdout)
        self.assertIn("1 screened files can proceed",result.stdout)
        self.assertEqual((self.prep/"old-audit-sentinel").read_text(),"preserve me")
        self.assertTrue((self.base/"prepared-attempt2/privacy-preflight.json").is_file())
        private=self.base/"converged-attempt2"
        self.assertTrue((private/"retired-archives/scripts/source.py.archive").is_file())
        self.assertFalse((self.repo/"scripts/source.py.archive").exists())
        self.assertTrue((self.repo/"scripts/source.mjs").exists())
        self.assertEqual(git(self.repo,"diff","--cached","--name-only"),"")
        self.assertEqual(git(self.repo,"rev-parse","--short=12","HEAD"),self.head)
        self.assertEqual(git(self.repo,"branch","--list","bonfyre-public/*"),"")
        isolated=private/"isolated-public-git"
        self.assertEqual(git(isolated,"remote"),"")
        self.assertEqual(git(isolated,"ls-tree","-r","--name-only","HEAD"),
                         "cmd/BonfyreCapability/src/main.c")
        self.assertTrue((isolated/".git/hooks/pre-push").is_file())
        self.assertFalse((isolated/"cmd/BonfyreBridge").exists())
        self.assertTrue((private/"legacy-mcp-staged-backup/mcp/README.md").is_file())
    def test_unrelated_staged_work_stops_before_scanner_and_quarantine(self):
        (self.repo/"other").write_text("concurrent work")
        git(self.repo,"add","other")
        result=self.run_driver()
        self.assertEqual(result.returncode,2)
        self.assertFalse(self.prep.exists())
        self.assertFalse(self.private.exists())
        self.assertTrue((self.repo/"scripts/source.py.archive").is_file())
        self.assertEqual(len(git(self.repo,"diff","--cached","--name-only").splitlines()),3)
    def test_no_admitted_source_refuses_without_index_change(self):
        (self.repo/"only_bad").write_text("exclude safe fixture")
        result=self.run_driver()
        self.assertEqual(result.returncode,2)
        self.assertIn("All source cohorts are held",result.stderr)
        self.assertFalse(self.private.exists())
        self.assertTrue((self.repo/"scripts/source.py.archive").is_file())
        self.assertEqual(len(git(self.repo,"diff","--cached","--name-only").splitlines()),2)

if __name__=="__main__":
    unittest.main(verbosity=2)
