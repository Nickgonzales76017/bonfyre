import {test} from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,writeFileSync,mkdirSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {once} from 'node:events';
import {staticSiteServer} from '../range-static-site.mjs';

async function fixture(fn){
 const dir=mkdtempSync(join(tmpdir(),'bonfyre-range-site-'));
 mkdirSync(join(dir,'audio'));
 writeFileSync(join(dir,'audio','edition.mp3'),Buffer.from('0123456789'));
 writeFileSync(join(dir,'index.html'),'<h1>Reading</h1>');
 const server=staticSiteServer(dir);
 server.listen(0,'127.0.0.1');await once(server,'listening');
 const origin='http://127.0.0.1:'+server.address().port+'/';
 try{await fn(origin);}finally{await new Promise(resolve=>server.close(resolve));rmSync(dir,{recursive:true,force:true});}
}
test('actual media byte-range request returns 206 with exact requested bytes',async()=>{
 await fixture(async origin=>{
  const res=await fetch(origin+'audio/edition.mp3',{headers:{Range:'bytes=3-7'}});
  assert.equal(res.status,206);
  assert.equal(res.headers.get('accept-ranges'),'bytes');
  assert.equal(res.headers.get('content-range'),'bytes 3-7/10');
  assert.equal(await res.text(),'34567');
 });
});
test('full page load and HEAD metadata both remain valid',async()=>{
 await fixture(async origin=>{
  const html=await fetch(origin);
  assert.equal(html.status,200);
  assert.match(html.headers.get('content-type'),/text\/html/);
  assert.equal(await html.text(),'<h1>Reading</h1>');
  const head=await fetch(origin+'audio/edition.mp3',{method:'HEAD'});
  assert.equal(head.status,200);assert.equal(head.headers.get('content-length'),'10');
 });
});
test('invalid ranges and missing content fail instead of pretending media is seekable',async()=>{
 await fixture(async origin=>{
  const invalid=await fetch(origin+'audio/edition.mp3',{headers:{Range:'bytes=50-70'}});
  assert.equal(invalid.status,416);assert.equal(invalid.headers.get('content-range'),'bytes */10');
  assert.equal((await fetch(origin+'missing.mp3')).status,404);
 });
});
