import {nonceStore} from './native-nonce-store.mjs';
import test from 'node:test';
import assert from 'node:assert/strict';
import {sign} from './workgraph-envelope.mjs';
import {admission} from './workgraph-admission.mjs';
const secret='example-test-secret-with-more-than-32-bytes';
const body={version:1,mission:'sync:source-42',family:'source-sync',digest:'a'.repeat(64),issued:1700000000000,nonce:'n_123456789012345'};
test('verified intent reaches the native admission callback once',async()=>{
 const replay=nonceStore(':memory:',{clock:()=>body.issued}),submitted=[];
 const receive=admission({secret,clock:()=>body.issued,replay,enqueue:async x=>{submitted.push(x);return {accepted:true,receipt:'native:42'}}});
 const raw=JSON.stringify({intent:body,signature:await sign(body,secret)});
 assert.deepEqual(await receive(raw),{status:'admitted',mission:body.mission,receipt:'native:42'});
 assert.deepEqual(await receive(raw),{status:'duplicate',mission:body.mission,receipt:'native:42'});
 assert.equal(submitted.length,1);assert.equal(submitted[0].ifAbsent,true);replay.close();
});
test('unsigned, tampered and oversized envelopes cannot enqueue',async()=>{
 let called=0;const replay=nonceStore(':memory:',{clock:()=>body.issued});const receive=admission({secret,clock:()=>body.issued,replay,enqueue:async()=>{called++;return {accepted:true,receipt:'test'}}});
 await assert.rejects(()=>receive(JSON.stringify({intent:{...body,mission:'altered'},signature:'0'.repeat(64)})));
 await assert.rejects(()=>receive('x'.repeat(4097)));assert.equal(called,0);replay.close();
});
