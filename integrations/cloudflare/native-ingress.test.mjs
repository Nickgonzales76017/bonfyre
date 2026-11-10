import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,rmSync,writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {nonceStore} from './native-nonce-store.mjs';
import {sign} from './workgraph-envelope.mjs';
import {admission} from './workgraph-admission.mjs';
const secret='test-secret-32-bytes-plus-for-native-workgraph';
test('nonce claims survive SQLite restart and duplicate delivery',()=>{
 const dir=mkdtempSync(join(tmpdir(),'bf-nonce-')),db=join(dir,'nonce.sqlite');
 try{let s=nonceStore(db);const claim=s.begin('first','a'.repeat(64),Date.now()+300000);assert.equal(claim.state,'claimed');assert.equal(s.begin('first','a'.repeat(64),Date.now()+300000).state,'busy');s.accept('first',claim.token,{native:'first'});s.close();s=nonceStore(db);assert.equal(s.begin('first','a'.repeat(64),Date.now()+300000).state,'accepted');s.close();}finally{rmSync(dir,{recursive:true,force:true})}
});
test('only source-sync family reaches native owner',async()=>{
 const intent={version:1,mission:'test:mission',family:'system-command',digest:'b'.repeat(64),issued:1700000000000,nonce:'nonce_123456789012'};
 let claims=0;const replay=nonceStore(':memory:',{clock:()=>intent.issued});const receive=admission({secret,clock:()=>intent.issued,replay,enqueue:async()=>{claims++;return {accepted:true,receipt:'test'}}});
 const signature=await sign(intent,secret);await assert.rejects(()=>receive(JSON.stringify({intent,signature})),/unapproved/);assert.equal(claims,0);replay.close();
});
