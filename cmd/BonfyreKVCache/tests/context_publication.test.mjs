// Exercises the compiled command and real files. The failure bridge is injected
// and counts bytes only to control framing/budget failures; it is not a tokenizer.
import {test, after} from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync, mkdirSync, writeFileSync, readFileSync, readdirSync, lstatSync, existsSync, rmSync, linkSync, symlinkSync} from 'node:fs';
import {join, resolve} from 'node:path';
import {tmpdir} from 'node:os';
import {spawnSync} from 'node:child_process';
const home=mkdtempSync(join(tmpdir(),'bonfyre-context-publication-'));
after(()=>rmSync(home,{recursive:true,force:true}));
const binary=process.env.BIN ?? resolve(import.meta.dirname,'../bonfyre-kvcache');
function fixture({previous=true,mode='normal',witness='keep this',budget='1000'}={}) {
  const dir=mkdtempSync(join(home,'case-')), bodies=join(dir,'bodies'), out=join(dir,'context.txt');
  mkdirSync(bodies);writeFileSync(join(bodies,'a'),'keep this');writeFileSync(join(bodies,'b'),'optional detail');
  const tokenizer=join(dir,'tokenizer'), witnesses=join(dir,'witnesses'), bridge=join(dir,'bridge');
  writeFileSync(tokenizer,'explicit protocol fixture');writeFileSync(witnesses,witness+'\n');
  if(previous)writeFileSync(out,'previous accepted context\n');
  writeFileSync(bridge,`#!/usr/bin/env node
const fs=require('node:fs'), path=require('node:path');
const base=__dirname, countPath=path.join(base,'calls');
const count=fs.existsSync(countPath)?Number(fs.readFileSync(countPath,'utf8'))+1:1;
fs.writeFileSync(countPath,String(count));
const files=process.argv.slice(3), mode=${JSON.stringify(mode)}, output=${JSON.stringify(out)};
if(count>1){
  fs.writeFileSync(path.join(base,'visible'),fs.existsSync(output)?fs.readFileSync(output):Buffer.from('<absent>'));
  if(mode==='fail')process.exit(12);
  if(mode==='mutate')fs.writeFileSync(files[0],'keep this\\nchanged by tokenizer\\n');
  if(mode==='competing')fs.writeFileSync(output,'other writer\\n');
  if(mode==='directory')fs.mkdirSync(output);
  if(mode==='same-size')fs.writeFileSync(output,Buffer.alloc(fs.statSync(output).size,120));
  if(mode==='chmod')fs.chmodSync(output,0o400);
}
const word=n=>{const b=Buffer.alloc(4);b.writeUInt32LE(n);return b;};
const result=[word(files.length)];
for(const file of files){const n=count>1&&mode==='inflate'?100:fs.readFileSync(file).length;
  result.push(word(n),Buffer.alloc(n*4));}
if(mode==='short')process.stdout.write(word(files.length));
else process.stdout.write(Buffer.concat(result));
`,{mode:0o700});
  function run(extra=[]) {return spawnSync(binary,['context-compile','--bodies',bodies,'--tokenizer',tokenizer,'--gigatoken-bin',bridge,'--witnesses',witnesses,'--token-budget',budget,'--out',out,...extra],{encoding:'utf8',timeout:10000});}
  return {dir,bodies,out,tokenizer,witnesses,bridge,run};
}
const noStaging=f=>assert.deepEqual(readdirSync(f.dir).filter(n=>n.startsWith('.bonfyre-context.')),[]);
const old=f=>readFileSync(f.out,'utf8');
for(const previous of [true,false]) {
  test(`missing witness preserves output, existing=${previous}`,()=>{const f=fixture({previous,witness:'not present'}), before=previous?lstatSync(f.out,{bigint:true}):null;
    const r=f.run();assert.equal(r.status,2,r.stderr);assert.equal(JSON.parse(r.stdout).verdict,'FAIL');
    if(previous){assert.equal(old(f),'previous accepted context\n');assert.equal(lstatSync(f.out,{bigint:true}).mtimeNs,before.mtimeNs);}
    else assert.equal(existsSync(f.out),false);noStaging(f);});
  test(`required context over budget preserves output, existing=${previous}`,()=>{const f=fixture({previous,budget:'1'}),r=f.run();assert.equal(r.status,2,r.stderr);
    if(previous)assert.equal(old(f),'previous accepted context\n');else assert.equal(existsSync(f.out),false);noStaging(f);});
  test(`successful output is invisible until validation, existing=${previous}`,()=>{const f=fixture({previous}),r=f.run();assert.equal(r.status,0,r.stderr);
    const report=JSON.parse(r.stdout);assert.equal(report.verdict,'PASS');assert.equal(report.context.path,f.out);
    assert.equal(readFileSync(join(f.dir,'visible'),'utf8'),previous?'previous accepted context\n':'<absent>');
    assert.equal(old(f),'keep this\noptional detail\n');assert.equal(report.context.tokens,old(f).length);noStaging(f);});
}
test('a joined required context exceeding the exact budget preserves the old output',()=>{const f=fixture({budget:'10',mode:'inflate'}),r=f.run();assert.equal(r.status,2,r.stderr);assert.equal(old(f),'previous accepted context\n');noStaging(f);});
test('tokenizer failure after candidate writing preserves the old output',()=>{const f=fixture({mode:'fail'}),r=f.run();assert.notEqual(r.status,0);assert.equal(old(f),'previous accepted context\n');noStaging(f);});
test('a truncated tokenizer frame preserves the old output',()=>{const f=fixture({mode:'short'}),r=f.run();assert.notEqual(r.status,0);assert.equal(old(f),'previous accepted context\n');noStaging(f);});
test('candidate mutation is refused even when every required sentence remains',()=>{const f=fixture({mode:'mutate'}),r=f.run();assert.notEqual(r.status,0);assert.match(r.stderr,/candidate differs/);assert.equal(old(f),'previous accepted context\n');noStaging(f);});
test('a concurrent destination creation is not replaced',()=>{const f=fixture({previous:false,mode:'competing'}),r=f.run();assert.notEqual(r.status,0);assert.equal(old(f),'other writer\n');noStaging(f);});
test('a destination becoming a directory is not removed',()=>{const f=fixture({previous:false,mode:'directory'}),r=f.run();assert.notEqual(r.status,0);assert.ok(lstatSync(f.out).isDirectory());noStaging(f);});
test('token budgets accept only positive uint32 decimal values',()=>{for(const budget of ['0','-1','+3','3x','1.5',' 3','4294967296','99999999999999999999999999999']){const f=fixture({budget}),r=f.run();assert.notEqual(r.status,0,budget);assert.equal(old(f),'previous accepted context\n');assert.equal(existsSync(join(f.dir,'calls')),false);noStaging(f);}});
test('embedded NUL cannot truncate a required-witness list',()=>{const f=fixture();writeFileSync(f.witnesses,Buffer.from('keep this\0\nnot present\n'));const r=f.run();assert.notEqual(r.status,0);assert.equal(old(f),'previous accepted context\n');noStaging(f);});
for(const input of ['body','witnesses','tokenizer','bridge'])test(`destination hard link to ${input} is refused`,()=>{
  const f=fixture({previous:false}),source=input==='body'?join(f.bodies,'a'):f[input],bytes=readFileSync(source);
  linkSync(source,f.out);const r=f.run();assert.notEqual(r.status,0);assert.deepEqual(readFileSync(source),bytes);assert.deepEqual(readFileSync(f.out),bytes);noStaging(f);
});
test('destination symlink is refused without touching its target',()=>{const f=fixture({previous:false});symlinkSync(join(f.bodies,'a'),f.out);const r=f.run();assert.notEqual(r.status,0);assert.equal(readFileSync(join(f.bodies,'a'),'utf8'),'keep this');assert.ok(lstatSync(f.out).isSymbolicLink());noStaging(f);});
test('oldest optional body is dropped only until the counted candidate fits',()=>{const f=fixture({budget:'24'}),r=f.run();assert.equal(r.status,0,r.stderr);assert.equal(old(f),'keep this\n');assert.equal(JSON.parse(r.stdout).context.tokens,10);noStaging(f);});
test('unchanged compilation retains output bytes, inode and nanosecond times',()=>{const f=fixture();assert.equal(f.run().status,0);const before=lstatSync(f.out,{bigint:true}),bytes=readFileSync(f.out);assert.equal(f.run().status,0);const after=lstatSync(f.out,{bigint:true});for(const k of ['ino','mtimeNs','ctimeNs','mode'])assert.equal(after[k],before[k],k);assert.deepEqual(readFileSync(f.out),bytes);noStaging(f);});
test('newly published context is private',()=>{const f=fixture({previous:false}),r=f.run();assert.equal(r.status,0,r.stderr);assert.equal(lstatSync(f.out).mode&0o777,0o600);noStaging(f);});
test('maximum supported uint32 budget is admitted',()=>{const f=fixture({budget:'4294967295'}),r=f.run();assert.equal(r.status,0,r.stderr);assert.equal(JSON.parse(r.stdout).token_budget,4294967295);noStaging(f);});
test('same-length destination edit during counting is not overwritten',()=>{const f=fixture({mode:'same-size'}),r=f.run();assert.notEqual(r.status,0);assert.match(r.stderr,/output changed/);assert.equal(old(f),'x'.repeat('previous accepted context\n'.length));noStaging(f);});
test('destination permission change during counting is not overwritten',()=>{const f=fixture({mode:'chmod'}),r=f.run();assert.notEqual(r.status,0);assert.match(r.stderr,/output changed/);assert.equal(lstatSync(f.out).mode&0o777,0o400);assert.equal(old(f),'previous accepted context\n');noStaging(f);});
