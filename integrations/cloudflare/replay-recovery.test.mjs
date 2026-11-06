import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {nonceStore} from './native-nonce-store.mjs';
import {admission} from './workgraph-admission.mjs';
import {sign} from './workgraph-envelope.mjs';
const secret='native-edge-replay-recovery-test-secret-v1';
test('failed enqueue is retriable and native work accepts exactly once',async()=>{
 const dir=mkdtempSync(join(tmpdir(),'edge-replay-'));const file=join(dir,'nonce.db');
 const now=1700000000000,nonce='nonce_123456789012345';
 const intent={version:1,mission:'source:revision1',family:'source-sync',digest:'b'.repeat(64),issued:now,nonce};
 const wire=JSON.stringify({intent,signature:await sign(intent,secret)});
 let calls=0;let store=nonceStore(file,{clock:()=>now});
 const make=()=>admission({secret,clock:()=>now,replay:store,enqueue:async()=>{calls++;if(calls===1)throw Error('temporary origin down');return {accepted:true,receipt:'queue:source:revision1'};}});
 try{
  await assert.rejects(()=>make()(wire),/temporary/);
  store.close();store=nonceStore(file,{clock:()=>now});
  assert.equal((await make()(wire)).status,'admitted');
  assert.equal((await make()(wire)).status,'duplicate');assert.equal(calls,2);
 }finally{store.close();rmSync(dir,{recursive:true,force:true})}
});
test('in-flight replay leases expire and can be recovered',()=>{
 const dir=mkdtempSync(join(tmpdir(),'edge-lease-'));const file=join(dir,'nonce.db');let now=1700000000000;
 let store=nonceStore(file,{clock:()=>now,leaseMs:100});
 try{const old=store.begin('lease:1','a'.repeat(64),now+300000);assert.equal(old.state,'claimed');store.close();store=nonceStore(file,{clock:()=>now,leaseMs:100});
 assert.equal(store.begin('lease:1','a'.repeat(64),now+300000).state,'busy');now+=101;const next=store.begin('lease:1','a'.repeat(64),1700000300000);assert.equal(next.state,'claimed');
 assert.throws(()=>store.accept('lease:1',old.token,{test:true}),/stale/);assert.equal(store.release('lease:1',old.token),false);
 store.accept('lease:1',next.token,{test:true});now+=101;assert.equal(store.begin('lease:1','a'.repeat(64),1700000300000).state,'accepted');
 }finally{store.close();rmSync(dir,{recursive:true,force:true})}
});
