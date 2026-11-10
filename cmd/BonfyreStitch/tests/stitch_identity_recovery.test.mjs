import {test} from 'node:test';
import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import {createHash} from 'node:crypto';
import {mkdtempSync,readFileSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,resolve} from 'node:path';

const web=resolve(import.meta.dirname,'../../BonfyreWebAddress');
const source=join(web,'tests','stitch_identity_test.c');
const header=join(web,'src','stitch_identity.h');
test('the exact recovered Stitch identity C source and independent test compile and run',()=>{
 const expected=new Map([
  [header,'ad84d65262d1e44c407164d7590550fda4ae156e9979537217baf7b9d6075b72'],
  [source,'a913ef277c1b0004337152ec4e169f90121bcaeac43a5717bd2699ac1219b9bc']
 ]);
 for(const [path,hash] of expected)
  assert.equal(createHash('sha256').update(readFileSync(path)).digest('hex'),hash);
 const temp=mkdtempSync(join(tmpdir(),'stitch-identity-recovery-'));
 try{
  const binary=join(temp,'identity-test');
  const compile=spawnSync('cc',['-std=c11','-Wall','-Wextra','-Werror',
    '-o',binary,source],{cwd:web,encoding:'utf8',timeout:30000,maxBuffer:512*1024});
  assert.equal(compile.status,0,compile.stderr||compile.error?.message);
  const result=spawnSync(binary,[],{cwd:temp,encoding:'utf8',timeout:10000,maxBuffer:512*1024});
  assert.equal(result.status,0,result.stderr||result.error?.message);
  assert.match(result.stdout,/stitch_identity_test: PASS/);
 }finally{rmSync(temp,{recursive:true,force:true});}
});
