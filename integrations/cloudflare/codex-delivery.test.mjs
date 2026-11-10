import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync, rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join, resolve} from 'node:path';
import {Database} from 'bun:sqlite';
import {nonceStore} from './native-nonce-store.mjs';
import {nativeEnqueue, nativeMission} from './native-queue-adapter.mjs';
import {admission} from './workgraph-admission.mjs';
import {serveIngress} from './native-ingress.mjs';
import {sign} from './workgraph-envelope.mjs';
import edgeWorker from './edge-worker.mjs';

const secret='local-delivery-regression-secret-at-least-32-bytes';
const binary=resolve('cmd/BonfyreQueue/bonfyre-queue');
const intent=(now=Date.now())=>({version:1,mission:'sync:fixture',family:'source-sync',digest:'a'.repeat(64),issued:now,nonce:'scheduled_'+now});
const wire=async x=>JSON.stringify({intent:x,signature:await sign(x,secret)});
const directory=()=>mkdtempSync(join(tmpdir(),'edge-codex-'));
const rows=db=>{const read=new Database(db,{readonly:true});try{return read.query('SELECT mission_id,input_uri,family,status FROM workgraph_nodes').all();}finally{read.close();}};

test('overlapping delivery remains pending until native receipt is durable',async()=>{
 const dir=directory(),replay=nonceStore(join(dir,'nonce.db'));
 let entered,finish;
 const started=new Promise(r=>entered=r),blocked=new Promise(r=>finish=r);
 const receipt={mission:'fixture',node:'job'};
 const receive=admission({secret,replay,enqueue:async()=>{entered();await blocked;return {accepted:true,receipt};}});
 const packet=await wire(intent());let first;
 try{
  first=receive(packet);await started;
  assert.equal((await receive(packet)).status,'pending');
  finish();assert.equal((await first).status,'admitted');
  const duplicate=await receive(packet);
  assert.equal(duplicate.status,'duplicate');assert.deepEqual(duplicate.receipt,receipt);
 }finally{finish();await first;replay.close();rmSync(dir,{recursive:true,force:true});}
});

test('stale lease holder cannot accept or release its successor',()=>{
 const dir=directory();let now=1700000000000;
 const replay=nonceStore(join(dir,'nonce.db'),{clock:()=>now,leaseMs:10});
 try{
  const first=replay.begin('fixture','a'.repeat(64),now+10000);
  now+=11;const next=replay.begin('fixture','a'.repeat(64),now+10000);
  assert.notEqual(first.token,next.token);
  assert.throws(()=>replay.accept('fixture',first.token,{mission:'stale'}),/stale/);
  assert.equal(replay.release('fixture',first.token),false);
  assert.equal(replay.begin('fixture','a'.repeat(64),now+10000).state,'busy');
  replay.accept('fixture',next.token,{mission:'current'});
  assert.deepEqual(replay.begin('fixture','a'.repeat(64),now+10000).receipt,{mission:'current'});
  assert.throws(()=>replay.begin('fixture','b'.repeat(64),now+10000),/conflict/);
 }finally{replay.close();rmSync(dir,{recursive:true,force:true});}
});

test('native enqueue followed by lost transport receipt recovers one durable job',async()=>{
 const dir=directory(),db=join(dir,'queue.db'),nonceDb=join(dir,'nonce.db');
 const enqueue=nativeEnqueue({binary,db}),x=intent();
 let replay=nonceStore(nonceDb),calls=0;
 try{
  const receive=admission({secret,replay,enqueue:async i=>{const result=await enqueue(i);calls++;if(calls===1)throw Error('simulated lost reply');return result;}});
  await assert.rejects(()=>wire(x).then(receive),/lost reply/);
  assert.equal(rows(db).length,1);
  replay.close();replay=nonceStore(nonceDb);
  const recover=admission({secret,replay,enqueue});
  const result=await recover(await wire({...x,issued:Date.now()}));
  assert.equal(result.status,'admitted');
  assert.equal(result.receipt.mission,nativeMission(x));
  assert.equal(rows(db).length,1);
  assert.equal((await recover(await wire(x))).status,'duplicate');
  const changed={...x,digest:'b'.repeat(64),nonce:'another_123456789012'};
  await recover(await wire(changed));assert.equal(rows(db).length,2);
 }finally{replay.close();rmSync(dir,{recursive:true,force:true});}
});

test('HTTP ingress admits through the real native owner and survives restart',async()=>{
 const dir=directory(),queueDb=join(dir,'queue.db'),nonceDb=join(dir,'nonce.db');
 let app;
 const start=()=>serveIngress({secret,queueBinary:binary,queueDb,nonceDb,port:0});
 const post=async body=>fetch(`http://127.0.0.1:${app.server.port}/workgraph/intake`,{method:'POST',headers:{'content-type':'application/json'},body});
 try{
  app=start();const x=intent(),packet=await wire(x);
  const first=await post(packet);assert.equal(first.status,202);assert.equal((await first.json()).status,'admitted');
  app.close();app=start();
  const duplicate=await post(packet);assert.equal(duplicate.status,200);assert.equal((await duplicate.json()).status,'duplicate');
  const tampered=await post(await wire({...x,family:'system-command'}));assert.ok(tampered.status>=400);
  assert.equal(rows(queueDb).length,1);
 }finally{app?.close();rmSync(dir,{recursive:true,force:true});}
});

test('edge to HTTP to native queue recovers a lost reply before acknowledging',async()=>{
 const dir=directory(),queueDb=join(dir,'queue.db'),nonceDb=join(dir,'nonce.db');
 const app=serveIngress({secret,queueBinary:binary,queueDb,nonceDb,port:0});
 const original=globalThis.fetch,x=intent();let loseReply=true,ack=0,retry=0;
 const env={EDGE_SHARED_SECRET:secret,NATIVE_INGRESS_URL:'https://native.example.test/workgraph/intake',SOURCE_MISSION:x.mission,SOURCE_DIGEST:x.digest};
 globalThis.fetch=async(url,options)=>{
  assert.equal(String(url),env.NATIVE_INGRESS_URL);
  assert.equal(options.redirect,'error');
  const response=await original(`http://127.0.0.1:${app.server.port}/workgraph/intake`,options);
  if(loseReply){loseReply=false;await response.arrayBuffer();throw Error('simulated connection loss after admission');}
  return response;
 };
 const message={body:x,ack(){ack++;},retry(){retry++;}};
 try{
  await edgeWorker.queue({messages:[message]},env);
  assert.equal(ack,0);assert.equal(retry,1);assert.equal(rows(queueDb).length,1);
  await edgeWorker.queue({messages:[message]},env);
  assert.equal(ack,1);assert.equal(retry,1);assert.equal(rows(queueDb).length,1);
 }finally{globalThis.fetch=original;app.close();rmSync(dir,{recursive:true,force:true});}
});
