import {test,afterEach} from 'node:test';
import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import {mkdtempSync,mkdirSync,writeFileSync,symlinkSync,rmSync,readFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {auditRecovery,parseArgs} from '../tools/recovery-audit.mjs';

const roots=[];
afterEach(()=>{for(const r of roots.splice(0))rmSync(r,{recursive:true,force:true});});
function git(dir,...args){
 const p=spawnSync('git',['-C',dir,...args],{encoding:'utf8',timeout:10000});
 assert.equal(p.status,0,p.stderr);
 return p.stdout.trim();
}
function fixture(){
 const root=mkdtempSync(join(tmpdir(),'stitch-recovery-audit-'));roots.push(root);
 const repo=join(root,'project');mkdirSync(repo);
 git(repo,'init','-q');git(repo,'config','user.email','audit@test.invalid');
 git(repo,'config','user.name','Read-only fixture');
 mkdirSync(join(repo,'cmd'));mkdirSync(join(repo,'cmd','BonfyreWebAddress'));
 const tracked=join(repo,'cmd','BonfyreWebAddress','src.c');
 writeFileSync(tracked,'old native\n');
 git(repo,'add','.');git(repo,'commit','-qm','base');
 const candidate=join(root,'candidate');
 git(repo,'worktree','add','--detach','--quiet',candidate,'HEAD');
 const proposed=join(candidate,'cmd','BonfyreWebAddress','src.c');
 writeFileSync(proposed,'new native\n');
 mkdirSync(join(candidate,'cmd','BonfyreWebAddress','src'));
 const newFile=join(candidate,'cmd','BonfyreWebAddress','src','stitch_identity.h');
 writeFileSync(newFile,'/* reviewed candidate */\n');
 return {repo,candidate,tracked,newFile,proposed};
}
test('read-only recovery audit detects tracked and untracked candidate changes',()=>{
 const {repo,candidate,proposed}=fixture();
 const before=readFileSync(proposed,'utf8');
 const report=auditRecovery({candidate,target:repo,includeCleanSource:true});
 assert.equal(report.schema,'bonfyre.stitch.worktree-recovery.v1');
 assert.equal(report.changedFiles,2);
 assert.equal(report.baseIsTargetAncestor,true);
 assert.equal(report.trackedPatch.state,'check_passed');
 assert.deepEqual(report.counts,{tracked_review_candidate:1,new_file_candidate:1});
 assert.equal(report.readOnly,true);assert.equal(report.applied,false);
 assert.equal(report.cleanSource.length,1);
 assert.equal(report.cleanSource[0].content,'/* reviewed candidate */\n');
 assert.equal(readFileSync(proposed,'utf8'),before);
 assert.equal(readFileSync(join(repo,'cmd','BonfyreWebAddress','src.c'),'utf8'),'old native\n');
});
test('reports dirty target collisions and refuses symlink sources',()=>{
 const {repo,candidate,tracked}=fixture();
 writeFileSync(tracked,'someone else owns this\n');
 symlinkSync('src.c',join(candidate,'cmd','BonfyreWebAddress','link.c'));
 const result=auditRecovery({candidate,target:repo});
 assert.equal(result.counts.dirty_target_conflict,1);
 assert.equal(result.counts.unsafe_or_missing,1);
 assert.equal(result.files.find(f=>f.path.endsWith('link.c'))?.source.state,'symlink_refused');
 assert.equal(readFileSync(tracked,'utf8'),'someone else owns this\n');
});
test('recognizes an already delivered candidate without assuming an approved merge',()=>{
 const {repo,candidate,tracked}=fixture();
 writeFileSync(tracked,'new native\n');
 const result=auditRecovery({candidate,target:repo});
 assert.equal(result.counts.already_present,1);
 assert.equal(result.counts.new_file_candidate,1);
 assert.equal(result.applied,false);
});
test('rejects self comparison, missing source and unsupported CLI arguments',()=>{
 const {repo}=fixture();
 assert.throws(()=>auditRecovery({candidate:repo,target:repo}),/itself/);
 assert.throws(()=>auditRecovery({candidate:'/does-not-exist',target:repo}));
 assert.deepEqual(parseArgs(['--candidate','/a','--target','/b']),{candidate:'/a',target:'/b',includeCleanSource:false});
 assert.throws(()=>parseArgs(['--candidate','/a','--wrong','/b']));
 assert.throws(()=>parseArgs(['--candidate','/a','--candidate','/b','--target','/c']));
});
