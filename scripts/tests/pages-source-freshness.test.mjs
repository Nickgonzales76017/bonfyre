import {test} from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,mkdirSync,writeFileSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,dirname} from 'node:path';
import {execFileSync} from 'node:child_process';
import {pagesSourceFreshness} from '../pages-source-freshness.mjs';

function fixture(){
 const cwd=mkdtempSync(join(tmpdir(),'pages-source-freshness-'));
 const git=(...args)=>execFileSync('git',args,{cwd,encoding:'utf8'}).trim();
 git('init','-b','main');git('config','user.name','Human Author');git('config','user.email','human@example.test');
 const write=(path,data)=>{mkdirSync(dirname(join(cwd,path)),{recursive:true});writeFileSync(join(cwd,path),data);git('add',path);git('commit','-m','Source update');return git('rev-parse','HEAD');};
 const bot=(path)=>{git('config','user.name','github-actions[bot]');git('config','user.email','41898282+github-actions[bot]@users.noreply.github.com');mkdirSync(dirname(join(cwd,path)),{recursive:true});writeFileSync(join(cwd,path),Date.now().toString());git('add',path);git('commit','-m','release(spaces): retain verified studio and instrument projections');git('config','user.name','Human Author');return git('rev-parse','HEAD');};
 const source=write('site/discover/index.html','original');
 return {cwd,git,write,bot,source,cleanup:()=>rmSync(cwd,{recursive:true,force:true})};
}
test('current checked-out source is fresh',()=>{
 const f=fixture();try{assert.equal(pagesSourceFreshness({cwd:f.cwd,source:f.source,upstream:f.source}).latest,true);}finally{f.cleanup();}
});
test('generated release commits do not make a new authored source obsolete',()=>{
 const f=fixture();try{
  f.bot('site/index.html');const head=f.bot('site/instruments/SOURCE.tsv');
  const actual=pagesSourceFreshness({cwd:f.cwd,source:f.source,upstream:head});
  assert.equal(actual.latest,true);assert.equal(actual.generatedCommits,2);
 }finally{f.cleanup();}
});
test('new editorial or runtime source supersedes an older run, even after bot release',()=>{
 const f=fixture();try{
  f.bot('site/index.html');const head=f.write('site/blog/index.html','new');
  assert.equal(pagesSourceFreshness({cwd:f.cwd,source:f.source,upstream:head}).latest,false);
 }finally{f.cleanup();}
});
test('a bot modifying a source file cannot masquerade as a generated release',()=>{
 const f=fixture();try{
  const head=f.bot('scripts/render-editorial.sh');
  assert.equal(pagesSourceFreshness({cwd:f.cwd,source:f.source,upstream:head}).latest,false);
 }finally{f.cleanup();}
});
test('lost source ancestry refuses silently publishing the wrong history',()=>{
 const f=fixture();try{
  const first=f.source,second=f.write('site/blog/index.html','new');
  assert.throws(()=>pagesSourceFreshness({cwd:f.cwd,source:second,upstream:first}),/no longer contains/);
 }finally{f.cleanup();}
});
