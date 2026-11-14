import {test,expect} from 'bun:test';
import {mkdtempSync,mkdirSync,writeFileSync,readFileSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {createHash} from 'node:crypto';
import {spawnSync} from 'node:child_process';
import {plan,safePath,readReport} from '../bonfyre-lineage-mcp.mjs';
test('source-pinned new-file intake is idempotent and refuses overwritten destinations',()=>{
 const root=mkdtempSync(join(tmpdir(),'lineage-')),a=join(root,'a'),b=join(root,'b');
 try{
  mkdirSync(a);mkdirSync(join(b,'cmd/BonfyreWebAddress/src'),{recursive:true});
  const path='cmd/BonfyreWebAddress/src/affinity_new.h',body='#pragma once\nint a = 4;\n';
  writeFileSync(join(b,path),body);const digest=createHash('sha256').update(body).digest('hex');
  const report={primary:{root:a},secondary:{root:b},difference_count:1,differences:[{path,primary:{state:'absent'},secondary:{state:'file',sha256:digest}}]};
  const candidate=plan(report);expect(candidate.selected.length).toBe(1);
  expect(spawnSync('git',['apply','--check','-'],{cwd:a,input:candidate.patch}).status).toBe(0);
  expect(spawnSync('git',['apply','-'],{cwd:a,input:candidate.patch}).status).toBe(0);
  expect(readFileSync(join(a,path),'utf8')).toBe(body);
  expect(plan(report).summary.already_admitted).toBe(1);
  writeFileSync(join(a,path),'other');
  expect(()=>plan(report)).toThrow('destination was created/changed');
 }finally{rmSync(root,{recursive:true,force:true})}
});
test('overlapping changes and deleted primary files never qualify for automatic intake',()=>{
 const root=mkdtempSync(join(tmpdir(),'lineage-')),a=join(root,'a'),b=join(root,'b');
 try{
  mkdirSync(a);mkdirSync(b);
  const report={primary:{root:a},secondary:{root:b},difference_count:2,differences:[
   {path:'cmd/BonfyreWebAddress/src/a.h',primary:{state:'file',sha256:'a'},secondary:{state:'file',sha256:'b'}},
   {path:'cmd/BonfyreWebAddress/src/b.h',primary:{state:'deleted'},secondary:{state:'file',sha256:'c'}}]};
  expect(plan(report).selected.length).toBe(0);
  expect(plan(report).summary.overlap_requires_review).toBe(2);
 }finally{rmSync(root,{recursive:true,force:true})}
});
test('path policy blocks traversal, secrets and unsafe Git patch paths',()=>{
 for(const path of ['cmd/../bad.h','cmd/.env','cmd/private/pass.json','cmd/site/secret.key','outside.txt','cmd/name space.c'])expect(safePath(path)).toBe(false);
 expect(safePath('cmd/BonfyreWebAddress/src/affinity.c')).toBe(true);
});
test('C source admission refuses unresolved sibling includes before requesting MCP mutation',()=>{
 const root=mkdtempSync(join(tmpdir(),'lineage-dependency-')),a=join(root,'primary'),b=join(root,'secondary');
 try{
  mkdirSync(join(a,'cmd/BonfyreWebAddress/src'),{recursive:true});
  mkdirSync(join(b,'cmd/BonfyreWebAddress/src'),{recursive:true});
  const path='cmd/BonfyreWebAddress/src/article_view.h',body='#include "missing_view.h"\n';
  writeFileSync(join(b,path),body);const digest=createHash('sha256').update(body).digest('hex');
  const report={primary:{root:a},secondary:{root:b},difference_count:1,differences:[{path,primary:{state:'absent'},secondary:{state:'file',sha256:digest}}]};
  const first=plan(report);expect(first.selected).toHaveLength(0);
  expect(first.summary.dependency_blockers[0].missing).toContain('cmd/BonfyreWebAddress/src/missing_view.h');
  writeFileSync(join(a,'cmd/BonfyreWebAddress/src/missing_view.h'),'#pragma once\n');
  expect(plan(report).selected).toHaveLength(1);
 }finally{rmSync(root,{recursive:true,force:true})}
});
test('prioritizes missing includes of already admitted components before unrelated files',()=>{
 const root=mkdtempSync(join(tmpdir(),'lineage-priority-')),a=join(root,'primary'),b=join(root,'secondary');
 try{
  for(const dir of [a,b])mkdirSync(join(dir,'cmd/BonfyreWebAddress/src'),{recursive:true});
  const parent='cmd/BonfyreWebAddress/src/',contents=new Map([['source.h','#include "z_dependency.h"\n'],['a_unrelated.h','#pragma once\n'],['z_dependency.h','#pragma once\n']]);
  const diffs=[];
  for(const [name,body] of contents){
   writeFileSync(join(b,parent+name),body);
   if(name==='source.h')writeFileSync(join(a,parent+name),body);
   diffs.push({path:parent+name,primary:{state:'absent'},secondary:{state:'file',sha256:createHash('sha256').update(body).digest('hex')}});
  }
  const report={primary:{root:a},secondary:{root:b},difference_count:diffs.length,differences:diffs};
  expect(plan(report,1).selected.map(item=>item.path)).toEqual([parent+'z_dependency.h']);
 }finally{rmSync(root,{recursive:true,force:true})}
});
test('accepts actual 40-character Git HEAD IDs without weakening source SHA-256 verification',()=>{
 const root=mkdtempSync(join(tmpdir(),'lineage-git-head-')),a=join(root,'primary'),b=join(root,'secondary');
 const git=(args)=>{const r=spawnSync('git',args,{encoding:'utf8'});if(r.status!==0)throw Error(r.stderr);return r.stdout.trim()};
 try{
  mkdirSync(a);mkdirSync(b);
  for(const dir of [a,b]){
   git(['init','-q',dir]);
   git(['-C',dir,'-c','user.name=Fixture','-c','user.email=fixture@example.invalid','commit','--allow-empty','-qm','fixture']);
  }
  const report={schema:'bonfyre.source-convergence.v1',difference_count:0,differences:[],
   primary:{root:a,head:git(['-C',a,'rev-parse','HEAD'])},
   secondary:{root:b,head:git(['-C',b,'rev-parse','HEAD'])}};
  expect(report.primary.head).toHaveLength(40);
  const path=join(root,'lineage.json');
  writeFileSync(path,JSON.stringify(report));
  expect(readReport(path).primary.head).toBe(report.primary.head);
  report.primary.head='x'.repeat(40);
  writeFileSync(path,JSON.stringify(report));
  expect(()=>readReport(path)).toThrow('invalid source identity');
  report.primary.head='0'.repeat(64);
  writeFileSync(path,JSON.stringify(report));
  expect(()=>readReport(path)).toThrow('HEAD changed since report');
 }finally{rmSync(root,{recursive:true,force:true})}
});
