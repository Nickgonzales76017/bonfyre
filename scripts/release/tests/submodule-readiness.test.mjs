import {test,expect} from 'bun:test';
import {mkdtempSync,mkdirSync,writeFileSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {spawnSync} from 'node:child_process';
import {auditSubmodules,auditText} from '../submodule-readiness.mjs';
const git=(root,...args)=>{
 const r=spawnSync('git',['-C',root,...args],{encoding:'utf8',timeout:10000});
 if(r.status!==0)throw Error(r.stderr);return r.stdout.trim();
};
function setup(fn){
 const root=mkdtempSync(join(tmpdir(),'bonfyre-submodule-audit-'));
 try{
  const lib=join(root,'upstream');mkdirSync(lib);
  git(lib,'init','-q');git(lib,'config','user.email','fixture@example.test');git(lib,'config','user.name','Fixture');
  writeFileSync(join(lib,'README'),'immutable content\n');git(lib,'add','README');git(lib,'commit','-qm','upstream');
  const sha=git(lib,'rev-parse','HEAD'),repo=join(root,'repo');mkdirSync(repo);
  git(repo,'init','-q');git(repo,'config','user.email','fixture@example.test');git(repo,'config','user.name','Fixture');
  writeFileSync(join(repo,'.gitmodules'),'[submodule "fixture"]\n\tpath = vendor/fixture\n\turl = https://fixture.invalid/lib.git\n');
  git(repo,'add','.gitmodules');git(repo,'update-index','--add','--cacheinfo',`160000,${sha},vendor/fixture`);
  git(repo,'commit','-qm','fixture gitlink');
  fn({repo,lib,sha});
 }finally{rmSync(root,{recursive:true,force:true});}
}
test('uninitialized gitlinks are held without changing checkout or index',()=>{
 setup(({repo,sha})=>{
  const view=auditSubmodules(repo);
  expect(view.state).toBe('HELD');expect(view.committed).toEqual([{path:'vendor/fixture',sha}]);
  expect(view.held.join(' ')).toContain('not at a ready checkout');
  expect(auditText(view)).toContain('side_effects none');
 });
});
test('an unregistered local clone is not mistaken for a converged submodule',()=>{
 setup(({repo,lib})=>{
  git(repo,'-c','protocol.file.allow=always','clone','-q',lib,'vendor/fixture');
  git(repo,'submodule','absorbgitdirs','vendor/fixture');
  const view=auditSubmodules(repo);expect(view.state).toBe('HELD');
  expect(auditText(view)).toContain('submodule not at a ready checkout');
  writeFileSync(join(repo,'.gitmodules'),'[submodule "fixture"]\n\tpath = vendor/changed\n\turl = https://fixture.invalid/lib.git\n');
  expect(auditSubmodules(repo).state).toBe('HELD');
 });
});
test('unsafe or duplicated declarations are refused',()=>{
 setup(({repo})=>{
  writeFileSync(join(repo,'.gitmodules'),'[submodule "fixture"]\n\tpath = ../escaped\n\turl = https://fixture.invalid/lib.git\n');
  expect(()=>auditSubmodules(repo)).toThrow('unsafe');
 });
});
