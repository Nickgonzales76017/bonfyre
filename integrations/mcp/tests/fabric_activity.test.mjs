import {test,afterEach} from 'bun:test';
import {Database} from 'bun:sqlite';
import assert from 'node:assert/strict';
import {mkdtempSync,mkdirSync,rmSync,writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {fileURLToPath} from 'node:url';
import {createFabricStore} from '../fabric_store.mjs';
import {createFabricServer} from '../fabric_server.mjs';
import {callFabricActivity} from '../fabric_activity_cli.mjs';
import {encode,decode} from '../../browser/workspace-msgpack.mjs';

const cleanup=[];
afterEach(async()=>{for(const release of cleanup.splice(0).reverse())await release();});
const alice={actor:'user:1',subject:'user:1',scope:'self'},bob={actor:'user:2',subject:'user:2',scope:'self'};
const organization=(actor='user:1',subject='institution:one')=>({actor,subject,scope:'institution'});
const input=(requestId='request-one',overrides={})=>({kind:'claude',title:'Review retained source',prompt:'private prompt marker',requestId,...overrides});
const absent=()=>({owned:false,queued:false});
function fixture(){
  const root=mkdtempSync(join(tmpdir(),'fabric-activity-')),projectRoot=join(root,'project'),home=join(root,'owner');mkdirSync(projectRoot);
  const store=createFabricStore({home,projectRoot,now:()=>100});cleanup.push(()=>{store.close();rmSync(root,{recursive:true,force:true});});
  return {root,projectRoot,home,store,read:(binding=alice,options={})=>store.activityRead(binding,{execution:absent,...options})};
}
const rejectCode=(fn,code)=>assert.throws(fn,error=>error.code===code);
async function serverFixture({paused=false,maxQueued=2}={}){
  const f=fixture(),pending=new Map(),executed=[];const stop=join(f.root,'STOP');if(paused)writeFileSync(stop,'retained pause');
  const work={execute:async id=>{f.store.claimTask(id);executed.push(id);await new Promise(resolve=>pending.set(id,resolve));
    if(!f.store.isTaskCancelled(id))f.store.updateTask(id,{state:'completed',completed_at:102});},
    cancel:async id=>{const result=f.store.cancelTask(id);pending.get(id)?.();return result;},close:async()=>{}};
  const node=await createFabricServer({token:'fixture-service-credential',...f,policy:{max_parallel_tasks:1,allow_background_tasks:true},store:f.store,work,estate:{status:async()=>({})},stopPaths:[stop],maxQueued});
  cleanup.push(async()=>{for(const release of pending.values())release();await node.close();});
  const request=(payload,{headers={},method='POST',bytes}={})=>node.handle(new Request('http://127.0.0.1:3053/activity',{method,headers:{authorization:'Bearer fixture-service-credential','content-type':'application/msgpack',...headers},...(method==='POST'?{body:bytes??encode([1,payload])}:{})}));
  return {...f,node,request,pending,executed,stop};
}
const readPacket=binding=>({operation:'read',...binding});
const value=async response=>decode(new Uint8Array(await response.arrayBuffer()));

test('legacy unbound tasks stay private; new native admission is separate and immutable',()=>{
  const f=fixture(),legacy=f.store.createTask({kind:'claude',title:'Operator private',prompt:'estate-only'});
  assert.deepEqual(f.read().tasks,[]);rejectCode(()=>f.read(alice,{taskId:legacy}),'not_found');
  const admitted=f.store.createScopedTask(input(),alice);assert.equal(admitted.replayed,false);
  const projected=f.read();assert.deepEqual(projected.binding,alice);assert.equal(projected.tasks[0].taskId,admitted.task_id);
  assert.equal(projected.tasks[0].state,'queued');assert.equal(projected.tasks[0].reconciliationRequired,true);
  const db=new Database(f.store.dbPath);try{assert.throws(()=>db.query('UPDATE task_scopes SET actor=? WHERE task_id=?').run('user:2',admitted.task_id),/immutable/);}finally{db.close();}
  rejectCode(()=>f.store.createScopedTask({...input('request-extra'),scope:alice},alice),'invalid');
  rejectCode(()=>f.store.createScopedTask(input('request-resume',{kind:'resume-claude'}),alice),'admission');
  rejectCode(()=>f.store.createScopedTask(input('request-wrong'),{...alice,subject:'user:2'}),'invalid');
  assert.equal(f.read().tasks.length,1);
});

test('native admission replays exact requests without duplicate tasks or cross-scope reuse',()=>{
  const f=fixture(),first=f.store.createScopedTask(input(),alice);
  assert.deepEqual(f.store.createScopedTask(input(),alice),{task_id:first.task_id,state:'queued',replayed:true});
  rejectCode(()=>f.store.createScopedTask(input('request-one',{title:'Different effect'}),alice),'conflict');
  const other=f.store.createScopedTask(input(),bob);assert.notEqual(other.task_id,first.task_id);
  const shared=f.store.createScopedTask(input(),organization());
  assert.equal(f.read(organization('user:2')).tasks[0].taskId,shared.task_id);
  assert.deepEqual(f.read(organization('user:2','institution:two')).tasks,[]);
  rejectCode(()=>f.read(bob,{taskId:first.task_id}),'not_found');
  assert.equal(f.read(alice).tasks.length,1);
});

test('existing database rows survive additive schema opening without inferred disclosure',()=>{
  const f=fixture(),id=f.store.createTask({kind:'claude',title:'Historical task',prompt:'private'});f.store.claimTask(id);f.store.close();
  const reopened=createFabricStore({home:f.home,projectRoot:f.projectRoot});cleanup.push(()=>reopened.close());
  assert.equal(reopened.task(id).state,'running');assert.deepEqual(reopened.activityRead(alice,{execution:absent}).tasks,[]);
  assert.equal(reopened.summary().tasks.running,1);
});

test('actual execution witness distinguishes queue, live subagent and historical running',()=>{
  const f=fixture(),id=f.store.createScopedTask(input(),alice).task_id;
  assert.equal(f.read(alice,{execution:()=>({owned:false,queued:true})}).tasks[0].phase,'pending');
  f.store.claimTask(id);const run=f.store.createRun({task_id:id,agent:'claude',role:'review',command:['fixture','private-command-marker'],cwd:f.projectRoot});
  const live=f.read(alice,{execution:()=>({owned:true,queued:false})});assert.equal(live.tasks[0].phase,'active');
  const child=live.tasks.find(row=>row.kind==='subagent');assert.equal(child.id,'run:'+run.run_id);assert.equal(child.parentTaskId,'task:'+id);assert.equal(child.state,'running');
  const historical=f.read();assert.equal(historical.tasks[0].state,'running');assert.equal(historical.tasks[0].phase,'uncertain');assert.equal(historical.tasks[1].reconciliationRequired,true);
  assert.equal(historical.notifications.length,0);assert.equal(historical.unread,0);
  assert.throws(()=>f.store.activityRead(alice),/process must witness/);
});

test('scoped projection excludes prompts, errors, commands, paths and receipt bodies',()=>{
  const f=fixture(),id=f.store.createScopedTask(input(),alice).task_id;f.store.claimTask(id);
  const run=f.store.createRun({task_id:id,agent:'claude',role:'review',command:['fixture','private-command-marker'],cwd:f.projectRoot});
  f.store.finishRun(run.run_id,{state:'passed',final:'private-result-marker',exit_code:0});
  f.store.saveReceipt(id,'source-snapshot',{body:'private-source-marker'});
  const receipt=f.store.saveReceipt(id,'relay-patch','private-receipt-marker','patch');
  f.store.updateTask(id,{state:'completed',completed_at:101,error:'private-error-marker',result_json:{body:'private-task-result-marker'}});
  const projected=f.read(),text=JSON.stringify(projected);
  for(const word of ['private-','stdout_path','stderr_path','worktree','projectRoot','command_json','payload_json'])assert.equal(text.includes(word),false,word);
  assert.equal(projected.outputs.length,1);assert.equal(projected.outputs[0].digest,receipt.sha256);assert.equal(projected.outputs[0].title,'Implementation patch receipt');
  assert.equal(projected.tasks.find(row=>row.kind==='subagent').state,'passed');
  assert.equal(projected.notifications.length,2);assert.equal(projected.unread,2);
  f.store.updateTask(id,{state:'completed'});assert.equal(f.read().notifications.length,2);
});

test('recipient lifecycle is durable, revision guarded and idempotent without cancelling work',()=>{
  const f=fixture(),binding=organization(),id=f.store.createScopedTask(input(),binding).task_id;f.store.claimTask(id);
  f.store.saveReceipt(id,'relay-patch','retained evidence','patch');const n=f.read(binding).notifications[0];
  assert.deepEqual(n.allowedActions,['read','dismiss']);assert.equal(f.read(organization('user:2')).notifications.length,0);
  const read={id:n.id,revision:n.revision,action:'read',requestId:'notice-read-one'};
  rejectCode(()=>f.store.notificationAction(organization('user:2'),read),'not_found');
  rejectCode(()=>f.store.notificationAction(organization('user:1','institution:two'),read),'not_found');
  rejectCode(()=>f.store.notificationAction(binding,{...read,revision:10}),'conflict');
  const receipt=f.store.notificationAction(binding,read);assert.equal(receipt.revision,2);assert.equal(receipt.state,'read');
  assert.equal(f.read(binding).unread,0);assert.equal(f.store.task(id).state,'running');
  const dismissed=f.store.notificationAction(binding,{id:n.id,revision:2,action:'dismiss',requestId:'notice-dismiss-one'});
  assert.equal(dismissed.state,'dismissed');assert.equal(f.read(binding).notifications.length,0);assert.equal(f.store.task(id).state,'running');
  assert.deepEqual(f.store.notificationAction(binding,read),{...receipt,replayed:true});assert.equal(f.read(binding).notifications.length,0);
  rejectCode(()=>f.store.notificationAction(binding,{...read,action:'dismiss'}),'conflict');
  rejectCode(()=>f.store.notificationAction(binding,{...read,requestId:'notice-again'}),'conflict');
  f.store.close();const reopened=createFabricStore({home:f.home,projectRoot:f.projectRoot});cleanup.push(()=>reopened.close());
  assert.deepEqual(reopened.notificationAction(binding,read),{...receipt,replayed:true});assert.equal(reopened.activityRead(binding,{execution:absent}).notifications.length,0);
});

test('bounded latest reads and exact older task continuation retain scope',()=>{
  const f=fixture();let first;
  for(let i=0;i<22;i++){const id=f.store.createScopedTask(input('request-'+String(i).padStart(3,'0')),alice).task_id;first??=id;}
  const latest=f.read();assert.equal(latest.tasks.length,20);assert.equal(latest.more,true);assert.equal(latest.tasks.some(row=>row.taskId===first),false);
  const exact=f.read(alice,{taskId:first});assert.equal(exact.tasks.length,1);assert.equal(exact.tasks[0].taskId,first);assert.equal(exact.more,false);
  rejectCode(()=>f.read(bob,{taskId:first}),'not_found');
});

test('existing Fabric submit/pump admits scoped work once and keeps legacy work unbound',async()=>{
  const f=await serverFixture(),first=f.node.submitScopedTask(input(),alice);
  const replay=f.node.submitScopedTask(input(),alice);assert.equal(replay.task_id,first.task_id);assert.equal(replay.replayed,true);
  await Promise.resolve();await Promise.resolve();assert.deepEqual(f.executed,[first.task_id]);
  const live=await value(await f.request(readPacket(alice)));assert.equal(live[1],true);assert.equal(live[2].tasks[0].reconciliationRequired,false);assert.equal(live[2].tasks[0].phase,'active');
  const legacy=await f.node.call('dispatch_claude',{prompt:'operator work'});
  assert.equal(f.read().tasks.some(row=>row.taskId===legacy.task_id),false);
  f.pending.get(first.task_id)();await new Promise(resolve=>setImmediate(resolve));f.pending.get(legacy.task_id)?.();await f.node.drain();
  assert.equal(f.store.task(first.task_id).state,'completed');assert.equal(f.read().notifications[0].kind,'task_completed');
});

test('STOP and queue limits precede scoped creation; activity carrier has no dispatch operation',async()=>{
  const paused=await serverFixture({paused:true});
  assert.throws(()=>paused.node.submitScopedTask(input(),alice),/stopped/);assert.deepEqual(paused.read().tasks,[]);
  const refused=await paused.request({...readPacket(alice),operation:'submit',payload:input()});assert.equal(refused.status,400);assert.equal(paused.executed.length,0);
  assert.equal((await paused.request(readPacket(alice))).status,200);
  const active=await serverFixture({maxQueued:1});active.node.submitScopedTask(input(),alice);await Promise.resolve();await Promise.resolve();
  active.node.submitScopedTask(input('request-two'),alice);
  assert.throws(()=>active.node.submitScopedTask(input('request-three'),alice),/queue full/);assert.equal(active.read().tasks.length,2);
});

test('MessagePack route enforces service bearer, native origin and exact read coordinates',async()=>{
  const f=await serverFixture({paused:true}),id=f.store.createScopedTask(input(),alice).task_id;
  assert.equal((await f.request(readPacket(alice),{headers:{authorization:'Bearer wrong'}})).status,401);
  for(const headers of [{origin:'https://workspace.test'},{origin:'null'},{cookie:'session=private'},{'sec-fetch-site':'same-origin'}])assert.equal((await f.request(readPacket(alice),{headers})).status,403);
  assert.equal((await f.request(readPacket(alice),{headers:{'content-type':'application/json'}})).status,400);
  assert.equal((await f.request(readPacket(alice),{bytes:new Uint8Array([0xc1])})).status,400);
  assert.equal((await f.request({...readPacket(alice),unknown:'field'})).status,400);
  assert.equal((await f.request({...readPacket(alice),payload:{}})).status,400);
  assert.equal((await f.request({...readPacket(alice),task:id})).status,200);
  const foreign=await f.request({...readPacket(bob),task:id});assert.equal(foreign.status,404);assert.equal((await value(foreign))[2].code,'not_found');
  const response=await f.request(readPacket(alice));assert.equal(response.headers.get('content-type'),'application/msgpack');assert.match(response.headers.get('cache-control'),/no-store/);
  assert.deepEqual((await value(response))[2].binding,alice);
});

test('CLI carrier uses existing process and native credential, with no DB or command dispatch',async()=>{
  const f=await serverFixture({paused:true}),id=f.store.createScopedTask(input(),alice).task_id;
  f.store.updateTask(id,{state:'blocked'});const notice=f.read().notifications[0];const calls=[];
  const options={token:'fixture-service-credential',request:async(url,options)=>{calls.push({url,options});return f.node.handle(new Request(url,options));}};
  const read=await callFabricActivity({...readPacket(alice),task:id},options);assert.equal(read[1],true);assert.equal(read[2].tasks[0].taskId,id);
  const mutation={operation:'notification',...alice,payload:{id:notice.id,revision:1,action:'dismiss',requestId:'native-dismiss-1'}};
  assert.equal((await callFabricActivity(mutation,options))[2].state,'dismissed');assert.equal((await callFabricActivity(mutation,options))[2].replayed,true);
  assert.equal(f.store.task(id).state,'blocked');assert.equal(f.executed.length,0);
  assert.equal(calls[0].url,'http://127.0.0.1:3053/activity');assert.equal(calls[0].options.redirect,'error');assert.equal(calls[0].options.headers.Origin,undefined);
  await assert.rejects(callFabricActivity(readPacket(alice),{...options,host:'outside.test'}));
  await assert.rejects(callFabricActivity(readPacket(alice),{...options,request:async()=>new Response('{}',{headers:{'content-type':'application/json'}})}));
});

test('actual Bun CLI roundtrips loopback MessagePack with empty task argv and durable notification receipt',async()=>{
  const f=await serverFixture({paused:true}),id=f.store.createScopedTask(input(),alice).task_id;f.store.updateTask(id,{state:'blocked'});
  const token=join(f.root,'fixture-token');writeFileSync(token,'fixture-service-credential',{mode:0o600});
  const listener=Bun.serve({hostname:'127.0.0.1',port:0,fetch:f.node.handle});
  const invoke=async(operation,body,task='')=>{
    const child=Bun.spawn([process.execPath,'--no-env-file','--no-install',fileURLToPath(new URL('../fabric_activity_cli.mjs',import.meta.url)),
      '--operation',operation,'--actor',alice.actor,'--subject',alice.subject,'--scope',alice.scope,'--task',task],
      {env:{...process.env,BONFYRE_MCP_TOKEN_FILE:token,BONFYRE_FABRIC_HOST:'127.0.0.1',BONFYRE_FABRIC_PORT:String(listener.port)},stdin:body?encode([1,body]):'ignore',stdout:'pipe',stderr:'pipe',timeout:8000});
    const [status,bytes,stderr]=await Promise.all([child.exited,new Response(child.stdout).arrayBuffer(),new Response(child.stderr).text()]);
    assert.equal(status,0,stderr);return decode(new Uint8Array(bytes));
  };
  try{
    const read=await invoke('read');assert.equal(read[1],true);assert.deepEqual(read[2].binding,alice);
    assert.equal((await invoke('read',null,id))[2].tasks[0].taskId,id);
    const n=read[2].notifications[0],action={id:n.id,revision:n.revision,action:'dismiss',requestId:'real-cli-dismiss'};
    assert.equal((await invoke('notification',action))[2].state,'dismissed');assert.equal((await invoke('notification',action))[2].replayed,true);
    assert.equal(f.store.task(id).state,'blocked');assert.equal(f.executed.length,0);
  }finally{listener.stop(true);}
},10000);
