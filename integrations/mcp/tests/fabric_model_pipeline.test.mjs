// Real Fabric queue/store + native MoE wire adapter + compiled native Meter.
// The loopback MoE server is a controlled witness, not a model or live account.
import {afterAll,afterEach,beforeAll,test} from 'bun:test';
import assert from 'node:assert/strict';
import {Database} from 'bun:sqlite';
import {spawnSync} from 'node:child_process';
import {mkdtempSync,mkdirSync,rmSync,writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,resolve} from 'node:path';
import {fileURLToPath} from 'node:url';
import {createFabricStore} from '../fabric_store.mjs';
import {createFabricServer} from '../fabric_server.mjs';
import {createNativeModelOwner} from '../native_model.mjs';
import {createNativeMeter} from '../native_meter.mjs';
import {callFabricActivity} from '../fabric_activity_cli.mjs';
import {encode,decode} from '../../browser/workspace-msgpack.mjs';
import {readModelRun} from '../../browser/aurekai-model.mjs';

const source=fileURLToPath(new URL('../../../',import.meta.url));
const alice={actor:'user:1',subject:'institution:studio',scope:'institution'};
const bob={actor:'user:2',subject:'institution:studio',scope:'institution'};
const outsider={actor:'user:1',subject:'institution:another',scope:'institution'};
const cleanups=[];let meterBinary,build;
beforeAll(()=>{
 if(process.env.METER_TEST_BINARY){meterBinary=resolve(process.env.METER_TEST_BINARY);return;}
 build=mkdtempSync(join(tmpdir(),'fabric-model-pipeline-build-'));meterBinary=join(build,'bonfyre-meter');
 const lib=join(source,'lib/libbonfyre'),flags=value=>(value||'').split(/\s+/).filter(Boolean);
 const result=spawnSync(process.env.CC||'cc',['-O1','-g','-std=c11','-D_POSIX_C_SOURCE=200809L','-D_DEFAULT_SOURCE','-DBONFYRE_SHA256_PORTABLE',
  '-I'+join(lib,'include'),'-I'+join(source,'cmd/BonfyreLex/src'),...flags(process.env.METER_TEST_CFLAGS),
  join(source,'cmd/BonfyreMeter/src/main.c'),join(lib,'src/bf_common.c'),join(lib,'src/bf_sha256.c'),join(lib,'src/bf_sqlite.c'),join(lib,'bf_msgpack.c'),join(lib,'bf_csv.c'),
  ...flags(process.env.METER_TEST_LDFLAGS||'-lsqlite3'),'-o',meterBinary],{encoding:'utf8',timeout:60000});
 assert.equal(result.status,0,result.stderr);
});
afterEach(async()=>{for(const done of cleanups.splice(0).reverse())await done();});
afterAll(()=>{if(build)rmSync(build,{recursive:true,force:true});});

async function until(predicate){const deadline=Date.now()+3000;while(!predicate()){if(Date.now()>deadline)throw Error('fixture condition timed out');await new Promise(resolve=>setTimeout(resolve,5));}}
async function fixture({maxQueued=4,hold=false,loseMeterAck=false}={}){
 const root=mkdtempSync(join(tmpdir(),'fabric-model-pipeline-')),projectRoot=join(root,'project'),home=join(root,'fabric'),stop=join(root,'STOP'),db=join(root,'meter.db');mkdirSync(projectRoot);
 const store=createFabricStore({home,projectRoot}),calls=[],releases=[],blocked=new Set(),generation=[];
 let release,gate=hold?new Promise(resolve=>{release=resolve;}):Promise.resolve();
 const moe=Bun.serve({hostname:'127.0.0.1',port:0,async fetch(request){
  const url=new URL(request.url);calls.push(url.pathname+url.search);
  if(url.pathname==='/bonfyre/residency')return Response.json({schema:'bonfyre.moe.residency.v2',resident:true,pid:777,grant_id:91,model:'/fixture/exact.gguf',
   candidates:[{model:'exact.gguf',usable:true,min_bytes:1024}]});
  if(url.pathname==='/bonfyre/lease'&&url.searchParams.has('release')){releases.push(url.searchParams.get('release'));return Response.json({released:true});}
  if(url.pathname==='/bonfyre/lease'){
   assert.equal(request.method,'POST');assert.equal(url.searchParams.get('prefer'),'exact.gguf');
   return Response.json({lease:31,model_path:'/fixture/exact.gguf',preferred:true,context:2048});
  }
  if(url.pathname==='/v1/chat/completions'){
   const body=await request.json();generation.push(body);await gate;
   return Response.json({choices:[{message:{content:'A retained answer for '+body.messages[1].content}}],usage:{prompt_tokens:7,completion_tokens:9,total_tokens:16}});
  }
  return new Response('unavailable',{status:404});
 }});
 const nativeModel=createNativeModelOwner({serverUrl:`http://127.0.0.1:${moe.port}`,catalogBindings:{'exact.gguf':'catalogue-exact'}});
 const nativeMeter=createNativeMeter({binary:meterBinary,db});let lost=false;
 const modelMeter={...nativeMeter,record:async input=>{const receipt=await nativeMeter.record(input);if(loseMeterAck&&!lost){lost=true;throw Error('fixture lost native meter acknowledgement');}return receipt;}};
 const unauthorizedWork=[];
 const node=await createFabricServer({token:'pipeline-service-token',home,projectRoot,store,nativeModel,modelMeter,
  policy:{max_parallel_tasks:1,allow_background_tasks:true},stopPaths:[stop],maxQueued,env:{},
  authorizeAccount:async binding=>!blocked.has(binding.actor),
  work:{execute:async id=>{unauthorizedWork.push(id);throw Error('Model task reached operator executor');},cancel:async id=>store.cancelTask(id),close:async()=>{}},
  estate:{status:()=>({})},pushDelivery:{dispatch:async()=>{throw Error('No device delivery in pipeline fixture');},start(){},close:async()=>{}}});
 const request=(url,options)=>node.handle(new Request(url,options));
 const invoke=(operation,payload,binding=alice,task)=>callFabricActivity({operation,...binding,...payload?{payload}:{},...task?{task}:{}},{token:'pipeline-service-token',request});
 const review=async(id='review-fixture-one',binding=alice)=>{
  const packet=await invoke('model-review',{expectedBinding:binding,requestId:id,modelId:'moe:exact.gguf',prompt:'explain the evidence',maxTokens:32,timeoutMs:10000},binding);
  assert.equal(packet[1],true,packet[2]?.message);return packet[2].review;
 };
 const submit=async(review,requestId='submit-fixture-one',binding=alice)=>invoke('model-submit',{expectedBinding:binding,reviewId:review.id,requestId},binding);
 const frontend=async id=>readModelRun('institution:studio',id,{expectedBinding:alice,request:async url=>{
  const q=new URL(url,'http://fixture').searchParams,reply=await invoke(q.get('operation'),undefined,alice,q.get('task'));
  return new Response(encode(reply),{status:reply[1]?200:503,headers:{'content-type':'application/msgpack'}});
 }});
 const meterRows=()=>{const owner=new Database(db,{readonly:true});try{return owner.query('SELECT * FROM events ORDER BY id').all();}finally{owner.close();}};
 cleanups.push(async()=>{release?.();await node.close();moe.stop(true);store.close();rmSync(root,{recursive:true,force:true});});
 return {node,store,stop,db,calls,releases,generation,blocked,unauthorizedWork,invoke,review,submit,frontend,meterRows,release:()=>release?.()};
}

test('real native pipeline creates one account task, output, usage and recipient inbox, and replays one submission',async()=>{
 const f=await fixture(),review=await f.review();
 assert.deepEqual(review.pricing,{state:'unknown'});
 const submitted=await f.submit(review);assert.equal(submitted[1],true,submitted[2]?.message);const id=submitted[2].taskId;
 await f.node.drain();
 const row=await f.frontend(id);assert.equal(row.phase,'completed');assert.match(row.output.text,/retained answer/);assert.equal(row.usage.state,'recorded');
 assert.equal(Object.hasOwn(row.usage,'cost'),false);assert.deepEqual(row.usage.pricing,{state:'unknown'});
 assert.equal(f.generation.length,1);assert.equal(f.generation[0].max_tokens,32);assert.deepEqual(f.releases,['31']);assert.deepEqual(f.unauthorizedWork,[]);
 const feed=await f.invoke('read');assert.equal(feed[1],true);assert.equal(feed[2].tasks[0].workKind,'model');assert.equal(feed[2].tasks[0].taskId,id);
 assert.ok(feed[2].outputs.some(output=>output.id===row.output.id));assert.ok(feed[2].notifications.length>0);
 const peer=await f.invoke('read',undefined,bob);assert.equal(peer[2].tasks[0].taskId,id);assert.equal(peer[2].notifications.length,0);
 const foreign=await f.invoke('model-read',undefined,outsider,id);assert.equal(foreign[1],false);assert.equal(foreign[2].code,'not_found');
 const repeat=await f.submit(review);assert.equal(repeat[2].taskId,id);assert.equal(repeat[2].replayed,true);assert.equal(f.generation.length,1);assert.equal(f.meterRows().length,1);
 assert.throws(()=>f.node.submitScopedTask({kind:'model',title:'bypass',prompt:'bypass',requestId:'bad-model-bypass'},alice),/exact native execution review/);
});

test('STOP and queue limits refuse new model admission before creating an orphan task',async()=>{
 const f=await fixture({maxQueued:1,hold:true}),firstReview=await f.review(),secondReview=await f.review('review-fixture-two'),thirdReview=await f.review('review-fixture-three');
 const first=await f.submit(firstReview);assert.equal(first[1],true);await until(()=>f.generation.length===1);
 assert.equal((await f.submit(secondReview,'submit-fixture-two'))[1],true);
 const full=await f.submit(thirdReview,'submit-fixture-three');assert.equal(full[1],false);assert.equal(full[2].code,'capacity');assert.equal(f.store.summary().recent_tasks.length,2);
 writeFileSync(f.stop,'fixture pause');
 const paused=await f.submit(thirdReview,'submit-fixture-three');assert.equal(paused[1],false);assert.equal(paused[2].code,'paused');
 f.release();await f.node.drain();assert.equal(f.generation.length,1);assert.equal(f.store.summary().recent_tasks.length,2);
 assert.equal((await f.invoke('model-read',undefined,alice,first[2].taskId))[1],true);
});

test('a queued model rechecks fresh authority before lease and inference',async()=>{
 const f=await fixture({hold:true}),one=await f.review(),two=await f.review('review-fixture-bob',bob);
 const first=await f.submit(one);assert.equal(first[1],true);await until(()=>f.generation.length===1);
 const second=await f.submit(two,'submit-fixture-bob',bob);assert.equal(second[1],true);
 f.blocked.add(bob.actor);f.release();await f.node.drain();assert.equal(f.generation.length,1);
 assert.equal(f.store.task(second[2].taskId).state,'blocked');assert.deepEqual(f.store.taskScope(second[2].taskId),bob);
 const denied=await f.invoke('model-read',undefined,bob,second[2].taskId);assert.equal(denied[1],false);assert.equal(denied[2].code,'admission');
});

test('lost native metering acknowledgement is reconciled without a second inference or meter event',async()=>{
 const f=await fixture({loseMeterAck:true}),review=await f.review(),reply=await f.submit(review);assert.equal(reply[1],true);const id=reply[2].taskId;await f.node.drain();
 const pending=await f.frontend(id);assert.ok(pending.output);assert.equal(pending.usage.state,'pending');assert.equal(pending.reconciliationAllowed,true);assert.equal(f.meterRows().length,1);
 const reconciled=await f.invoke('model-reconcile',{expectedBinding:alice,requestId:'finish-usage-fixture',taskId:id});assert.equal(reconciled[1],true,reconciled[2]?.message);
 const complete=await f.frontend(id);assert.equal(complete.phase,'completed');assert.equal(complete.usage.state,'recorded');assert.equal(complete.output.digest,pending.output.digest);
 assert.equal(f.generation.length,1);assert.equal(f.meterRows().length,1);
});

test('server shutdown aborts the model owner and releases its exact lease instead of invoking the operator canceller',async()=>{
 const f=await fixture({hold:true}),review=await f.review(),reply=await f.submit(review);assert.equal(reply[1],true);await until(()=>f.generation.length===1);
 await f.node.close();assert.deepEqual(f.releases,['31']);assert.deepEqual(f.unauthorizedWork,[]);
 f.release();
});
