import test from 'node:test';
import assert from 'node:assert/strict';
import worker from './edge-worker.mjs';
import {verify} from './workgraph-envelope.mjs';
import {nativeMission} from './native-queue-adapter.mjs';

const env={EDGE_SHARED_SECRET:'edge-acknowledgment-test-secret-with-32-bytes',NATIVE_INGRESS_URL:'https://native.example.test/workgraph/intake',SOURCE_MISSION:'sync:fixture',SOURCE_DIGEST:'a'.repeat(64)};
const occurrence={version:1,mission:env.SOURCE_MISSION,family:'source-sync',digest:env.SOURCE_DIGEST,issued:1700000000000,nonce:'scheduled_1700000000000'};

async function delivery(response){
 const original=globalThis.fetch;let ack=0,retry=0,posted;
 globalThis.fetch=async(url,options)=>{posted=JSON.parse(options.body);return response;};
 try{await worker.queue({messages:[{body:occurrence,ack(){ack++;},retry(){retry++;}}]},env);return {ack,retry,posted};}
 finally{globalThis.fetch=original;}
}

test('edge refuses success without a matching native admission receipt',async()=>{
 for(const response of [new Response('<html>Sign in</html>',{status:200}),Response.json({status:'pending',mission:env.SOURCE_MISSION},{status:202}),Response.json({status:'admitted',mission:'different',receipt:{mission:'wrong',node:'job',digest:env.SOURCE_DIGEST}},{status:202})]){
  const result=await delivery(response);assert.equal(result.ack,0);assert.equal(result.retry,1);
 }
});

test('delayed edge delivery refreshes authentication but preserves occurrence identity',async()=>{
 const result=await delivery(new Response('unavailable',{status:503}));
 assert.equal(result.ack,0);assert.equal(result.retry,1);
 assert.equal(result.posted.intent.nonce,occurrence.nonce);
 assert.ok(result.posted.intent.issued>occurrence.issued);
 await verify(result.posted.intent,result.posted.signature,env.EDGE_SHARED_SECRET);
});

test('edge acknowledges both admitted and duplicate native receipts',async()=>{
 for(const [status,code]of [['admitted',202],['duplicate',200]]){
  const result=await delivery(Response.json({status,mission:occurrence.mission,receipt:{mission:nativeMission(occurrence),node:'job',sourceMission:occurrence.mission,digest:occurrence.digest,status:'ready'}},{status:code}));
  assert.equal(result.ack,1);assert.equal(result.retry,0);
 }
});
