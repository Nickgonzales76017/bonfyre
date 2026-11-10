import {test,expect} from 'bun:test';
import {execFileSync} from 'node:child_process';
import {mkdtempSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {fileURLToPath} from 'node:url';
import {readTasks,readActivity} from '../aurekai-activity-source.mjs';
import {encode,decode,maxPacket} from '../workspace-msgpack.mjs';

const binding=context=>({actor:'user:7',subject:context==='self'?'user:7':context,scope:context==='self'?'self':context.split(':')[0]});
const time=1750000000000;
const task=(id='account-task',seq=1)=>({id,seq,title:'Review account work',state:'ready',phase:'pending',created:time,updated:time,
 version:'a'.repeat(64),continuation:'/apps?context=self',
 nodes:[{id:'job',parentId:null,operatorId:'queue.fixture',family:'queue',state:'ready',phase:'pending',attempt:0,updated:time,hasOutput:false}],
 workers:[],outputs:[],counts:{nodes:1,workers:0,activeWorkers:0,outputs:0,pending:1,active:0,attention:0,completed:0,cancelled:0},nodesMore:false,workersMore:false,outputsMore:false});
const response=value=>new Response(encode([1,true,{binding:binding('self'),...value}]),{headers:{'content-type':'application/msgpack'}});
const page=tasks=>({tasks,more:false,cursor:0});
const read=(tasks,options={})=>readTasks('self',{request:async()=>response(page(tasks)),...options});

test('task query derives only the requested context and exact mission',async()=>{
 let observed;
 const result=await readTasks('institution:acme',{taskId:'account-org',request:async(url,options)=>{
  observed={url,options};const item=task('account-org');item.continuation='/apps?context=institution:acme';return response({...page([item]),binding:binding('institution:acme')});
 }});
 const url=new URL(observed.url,'https://example.test');
 expect(url.pathname).toBe('/apps/_activity');expect(url.searchParams.get('context')).toBe('institution:acme');
 expect(url.searchParams.get('operation')).toBe('get');expect(url.searchParams.get('mission')).toBe('account-org');
 expect(observed.options).toMatchObject({method:'GET',credentials:'same-origin',cache:'no-store',redirect:'error'});
 expect(result.tasks[0].continuation).toBe('/apps?context=institution:acme');
 expect(result.context).toBe('institution:acme');
});

test('completed root retains pending children and real WorkerRun state',async()=>{
 const value=task();value.state='complete';value.phase='active';
 Object.assign(value.nodes[0],{state:'complete',phase:'completed',hasOutput:true});
 value.nodes.push({id:'child',parentId:'job',operatorId:'queue.child',family:'queue',state:'ready',phase:'pending',attempt:0,updated:time,hasOutput:false});
 value.workers=[{id:'run-1',workerId:'reviewer',state:'running',phase:'active',created:time,updated:time}];
 value.outputs=[{id:'job',nodeId:'job',kind:'declared-output',label:'Root output',reference:'b'.repeat(64),updated:time,href:'javascript:bad()'}];
 Object.assign(value.counts,{nodes:2,workers:1,activeWorkers:1,outputs:1,pending:1,completed:1});
 const result=await read([value]);
 expect(result.tasks[0].phase).toBe('active');expect(result.tasks[0].nodes[1].parentId).toBe('job');
 expect(result.tasks[0].workers[0].workerId).toBe('reviewer');
 expect(result.tasks[0].outputs[0].href).toBeUndefined();expect(result.tasks[0].outputs[0].reference).toBe('b'.repeat(64));
 value.phase='completed';await expect(read([value])).rejects.toMatchObject({code:'invalid_reply'});const blocked=task('account-operator-blocked');Object.assign(blocked,{state:'blocked',phase:'attention'});Object.assign(blocked.nodes[0],{state:'blocked',phase:'attention'});Object.assign(blocked.counts,{pending:0,attention:1});expect((await read([blocked])).tasks[0].phase).toBe('attention');
});

test('continuations cannot cross destination or become external links',async()=>{
 for(const href of ['//example.test','https://example.test','/apps?context=institution:other','/apps/%2e%2e/auth','/apps?context=self&next=//example.test','/apps\\evil','/fleming','/fleming?context=self','/apps/','/apps/example/?context=self','/apps//example?context=self','/sources/web/?context=self']){
  const value=task();value.continuation=href;await expect(read([value])).rejects.toMatchObject({code:'invalid_reply'});
 }
 for(const href of ['/apps','/apps?context=self','/apps/example/next_step-1?context=self','/sources/web?context=self']){
  const value=task();value.continuation=href;expect((await read([value])).tasks[0].continuation).toBe(href);
 }
});

test('account work usage retains measured coverage and refuses invented financial cost',async()=>{
 const value=task();value.usage={runs:2,reportedRuns:1,inputTokens:300,outputTokens:30,costStatus:'unknown'};
 expect((await read([value])).tasks[0].usage).toEqual(value.usage);
 for(const mutation of [u=>u.reportedRuns=3,u=>u.costStatus='paid',u=>u.inputTokens=-1,u=>u.inputTokens=null,u=>u.extra=123]){
  const bad=task();bad.usage={...value.usage};mutation(bad.usage);await expect(read([bad])).rejects.toMatchObject({code:'invalid_reply'});
 }
 const missing=task();missing.usage={runs:1,reportedRuns:0,inputTokens:null,outputTokens:null,costStatus:'unknown'};
 expect((await read([missing])).tasks[0].usage.inputTokens).toBeNull();
 const fabricated=task();fabricated.usage={...missing.usage,inputTokens:0};
 await expect(read([fabricated])).rejects.toMatchObject({code:'invalid_reply'});
});

test('counts, detail limits and parent topology reject incomplete projections',async()=>{
 const wrongCount=task();wrongCount.counts.nodes=2;await expect(read([wrongCount])).rejects.toMatchObject({code:'invalid_reply'});
 const wrongParent=task();wrongParent.nodes[0].parentId='missing';await expect(read([wrongParent])).rejects.toMatchObject({code:'invalid_reply'});
 const cycle=task();cycle.nodes.push({...cycle.nodes[0],id:'child',parentId:'loop'},{...cycle.nodes[0],id:'loop',parentId:'child'});
 cycle.counts.nodes=3;cycle.counts.pending=3;await expect(read([cycle])).rejects.toMatchObject({code:'invalid_reply'});
 const overflow=task();overflow.counts.nodes=4097;overflow.counts.pending=4097;await expect(read([overflow])).rejects.toMatchObject({code:'invalid_reply'});
});

test('bounded details retain exact counts and explicit truncation',async()=>{
 const value=task();value.nodes=Array.from({length:32},(_,i)=>({...value.nodes[0],id:i?'child-'+i:'job',parentId:i?'job':null}));
 value.counts.nodes=36;value.counts.pending=36;value.nodesMore=true;
 const result=await read([value]);expect(result.tasks[0].counts.nodes).toBe(36);expect(result.tasks[0].nodes).toHaveLength(32);expect(result.tasks[0].nodesMore).toBe(true);
 value.nodesMore=false;await expect(read([value])).rejects.toMatchObject({code:'invalid_reply'});
});

test('task pages cannot repeat, reorder or return an unrelated exact identity',async()=>{
 await expect(read([task('account-a',2),task('account-a',1)])).rejects.toMatchObject({code:'invalid_reply'});
 await expect(read([task('account-a',1),task('account-b',2)])).rejects.toMatchObject({code:'invalid_reply'});
 await expect(read([task('account-a',2)],{cursor:2})).rejects.toMatchObject({code:'invalid_reply'});
 await expect(read([task('account-a',1)],{taskId:'account-other'})).rejects.toMatchObject({code:'invalid_reply'});
 const values=Array.from({length:20},(_,i)=>task('account-page-'+i,30-i));
 const result=await readTasks('self',{request:async()=>response({tasks:values,more:true,cursor:11})});
 expect(result.more).toBe(true);expect(result.cursor).toBe(11);
 await expect(readTasks('self',{request:async()=>response({tasks:values,more:true,cursor:12})})).rejects.toMatchObject({code:'invalid_reply'});
});

test('invalid timestamps, control text and unsupported native states are rejected',async()=>{
 for(const change of [t=>t.updated=8640000000000001,t=>t.title='bad\u0001title',t=>t.version='short',t=>t.state='guess',t=>t.nodes[0].hasOutput=true]){
  const value=task();change(value);await expect(read([value])).rejects.toMatchObject({code:'invalid_reply'});
 }
});

test('current access failure invalidates either owner consistently',async()=>{
 for(const status of [401,403])for(const reader of [readTasks,readActivity]){
  await expect(reader('self',{request:async()=>new Response('',{status})})).rejects.toMatchObject({code:'access_required',status});
 }
});

test('cancellation releases a request that ignores its AbortSignal',async()=>{
 const controller=new AbortController();let release;
 const pending=readTasks('self',{signal:controller.signal,request:()=>new Promise(resolve=>{release=resolve;})});
 controller.abort();await expect(pending).rejects.toMatchObject({name:'AbortError'});
 let cancelled=false;release(new Response(new ReadableStream({cancel(){cancelled=true;}}),{headers:{'content-type':'application/msgpack'}}));
 await new Promise(resolve=>setTimeout(resolve,0));expect(cancelled).toBe(true);
});

test('stream and declared lengths are bounded before decoding',async()=>{
 await expect(readTasks('self',{request:async()=>new Response(new Uint8Array(maxPacket+1),{headers:{'content-type':'application/msgpack'}})})).rejects.toMatchObject({code:'invalid_reply'});
 await expect(readTasks('self',{request:async()=>new Response(new Uint8Array(1),{headers:{'content-type':'application/msgpack','content-length':String(maxPacket+1)}})})).rejects.toMatchObject({code:'invalid_reply'});
});

test('native Queue MessagePack passes the browser contract and hides other accounts',async()=>{
 const binary=process.env.BONFYRE_TEST_QUEUE||fileURLToPath(new URL('../../../cmd/BonfyreQueue/bonfyre-queue',import.meta.url));
 const directory=mkdtempSync(join(tmpdir(),'bonfyre-activity-wire-')),db=join(directory,'queue.db');
 const run=(...args)=>execFileSync(binary,[...args,'--db',db],{stdio:['ignore','pipe','pipe']});
 const enqueue=(id,subject)=>run('enqueue','fixture','private-native-input','--mission',id,'--subject',subject,'--scope','self','--title','Scoped task','--continuation','/apps?context=self');
 try{
  enqueue('account-own','user:7');enqueue('account-other','user:8');run('enqueue','fixture','legacy-private','--mission','legacy');
  // The C HTTP owner adds this fresh session binding to the unchanged native Queue packet.
  const request=async()=>{const wire=decode(new Uint8Array(run('activity','--subject','user:7','--scope','self')));return response({...wire[2],binding:binding('self')});};
  const before=await readTasks('self',{request});expect(before.tasks.map(t=>t.id)).toEqual(['account-own']);
  const claimed=JSON.parse(run('claim','account-own','job','fixture-worker').toString());
  run('ack','account-own','job','fixture-worker',claimed.claim_token,'--output-uri','file:///private/result');
  const after=await readTasks('self',{request});expect(after.tasks[0].phase).toBe('completed');
  expect(after.tasks[0].version).not.toBe(before.tasks[0].version);expect(after.tasks[0].outputs).toHaveLength(1);
  expect(JSON.stringify(after)).not.toContain('file:///private');expect(JSON.stringify(after)).not.toContain('private-native-input');
 }finally{rmSync(directory,{recursive:true,force:true});}
});

test('native account-work token coverage survives the WebAddress browser packet without financial invention',async()=>{
 const binary=process.env.BONFYRE_TEST_QUEUE||fileURLToPath(new URL('../../../cmd/BonfyreQueue/bonfyre-queue',import.meta.url));
 const directory=mkdtempSync(join(tmpdir(),'bonfyre-work-usage-')),src=join(directory,'operator.db'),db=join(directory,'index.db');
 const sql=`CREATE TABLE goals(id TEXT,title TEXT,state TEXT,created_at REAL,updated_at REAL);
 CREATE TABLE runs(id TEXT,goal_id TEXT,state TEXT,started_at REAL,completed_at REAL,usage_json TEXT);
 CREATE TABLE actions(seq INTEGER PRIMARY KEY,run_id TEXT,tool TEXT,state TEXT,started_at REAL,duration_ms INTEGER);
 INSERT INTO goals VALUES('g1','Review task usage','queued',1792800000,1792800001);
 INSERT INTO runs VALUES('r1','g1','resolved',1792800000,1792800001,'{"input_tokens":300,"output_tokens":30}');
 INSERT INTO runs VALUES('r2','g1','failed',1792800001,1792800002,'{}');`;
 const run=(...args)=>execFileSync(binary,['account-work',...args,'--db',db],{stdio:['ignore','pipe','pipe']});
 try{
  execFileSync('sqlite3',[src,sql]);
  run('index','--subject','user:7','--scope','self','--owner','user:7','--source','operator='+src);
  const packet=decode(new Uint8Array(run('read','--subject','user:7','--scope','self')));
  expect(packet[2].tasks[0].usage).toEqual({runs:2,reportedRuns:1,inputTokens:300,outputTokens:30,costStatus:'unknown',pricedRuns:0,estimatedMicroUsd:null});
  const result=await readTasks('self',{request:async()=>response({...packet[2],binding:binding('self')})});
  expect(result.tasks[0].usage.reportedRuns).toBe(1);
  expect(result.tasks[0].usage.costStatus).toBe('unknown');
  expect(decode(new Uint8Array(run('read','--subject','user:8','--scope','self')))[2].tasks).toEqual([]);
 }finally{rmSync(directory,{recursive:true,force:true});}
});
