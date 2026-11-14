import {describe,expect,test} from 'bun:test';
import {encode,decode,maxPacket} from '../workspace-msgpack.mjs';
import {readInbox,setInboxState,readInboxReceipt,readAccountNeeds} from '../aurekai-inbox-source.mjs';

const binding={actor:'user:7',subject:'user:7',scope:'self'};
const version='a'.repeat(64),nextVersion='b'.repeat(64);
const row=(extra={})=>({id:'account-task',taskId:'account-task',version,revision:0,title:'Read source material',body:'Work is in progress.',updated:1000,href:'/apps?context=self',read:false,dismissed:false,phase:'active',...extra});
const change=(extra={})=>({expectedBinding:binding,id:'account-task',version,revision:0,action:'read',requestId:'notice-change-0001',...extra});
function reply(payload,okay=true,headers={}){return new Response(encode([1,okay,okay?{binding,...payload}:payload]),{headers:{'Content-Type':'application/msgpack',...headers}});}
const page=notices=>({notices,more:false,cursor:''});
const acknowledgement=(extra={})=>({requestId:'notice-change-0001',replayed:false,appliedVersion:version,appliedRevision:1,notice:row({revision:1,read:true}),...extra});

describe('bounded account Inbox interchange',()=>{
 test('list reads one exact context without browser actor or cached truth',async()=>{
  let called=0;const result=await readInbox('self',{request:(url,options)=>{
   called++;expect(url).toBe('/apps/_activity?context=self&operation=inbox');expect(options.method).toBe('GET');
   expect(options.credentials).toBe('same-origin');expect(options.cache).toBe('no-store');expect(options.redirect).toBe('error');return reply(page([row()]));
  }});expect(result.notices[0].id).toBe('account-task');expect(result.context).toBe('self');expect(called).toBe(1);
 });
 test('notice state write carries one exact idempotency identity and no audience impersonation',async()=>{
  const operation=change();const result=await setInboxState('self',operation,{request:(url,options)=>{
   expect(url).toBe('/apps/_activity?context=self&operation=inbox-set');expect(options.method).toBe('POST');
   expect(options.headers['Content-Type']).toBe('application/msgpack');expect(decode(options.body)).toEqual([1,operation]);return reply(acknowledgement());
  }});expect(result.notice.read).toBe(true);expect(result.appliedRevision).toBe(1);
 });
 test('replayed old action confirms its receipt while exposing newer current state',async()=>{
  const value=await setInboxState('self',change(),{request:()=>reply(acknowledgement({replayed:true,notice:row({version:nextVersion,revision:3})}))});
  expect(value.appliedVersion).toBe(version);expect(value.notice.version).toBe(nextVersion);expect(value.notice.revision).toBe(3);expect(value.notice.read).toBe(false);
 });
 test('wrong task, request, source version or recipient revision cannot confirm a write',async()=>{
  for(const override of [{requestId:'another-request-1'},{appliedVersion:nextVersion},{appliedRevision:2},
   {binding:{...binding,actor:'user:8',subject:'user:8'}},{notice:row({id:'account-other',taskId:'account-other',revision:1})},{notice:row({revision:0})}]){
   await expect(setInboxState('self',change(),{request:()=>reply(acknowledgement(override))})).rejects.toMatchObject({code:'invalid_reply'});
  }
 });
 test('notice links cannot change origin, path or account context',async()=>{
  for(const href of ['https://elsewhere.test','//elsewhere.test','/apps/../secret?context=self','/apps?context=institution:acme',
   '/apps?context=self&context=self','/apps?context=self&request=secret','/apps?context=self#secret','/apps%2Fsecret?context=self',
   '/fleming','/fleming?context=self','/apps/','/apps/example/?context=self','/apps//example?context=self','/sources/web/?context=self','/apps?context=%73elf']){
   await expect(readInbox('self',{request:()=>reply(page([row({href})]))})).rejects.toMatchObject({code:'invalid_reply'});
  }
  for(const href of ['','/apps','/apps?context=self','/apps/example/next_step-1?context=self','/sources/web?context=self'])
   expect((await readInbox('self',{request:()=>reply(page([row({href})]))})).notices[0].href).toBe(href);
 });
 test('duplicate records and malformed timestamps, flags or hashes reject the whole page',async()=>{
  for(const notices of [[row(),row()],[row({updated:8640000000000001})],[row({read:1})],[row({version:'short'})],[row({revision:-1})]]){
   await expect(readInbox('self',{request:()=>reply(page(notices))})).rejects.toMatchObject({code:'invalid_reply'});
  }
 });
 test('native page cursor must be bounded, complete and advance',async()=>{
  const notices=Array.from({length:50},(_,i)=>row({id:'account-'+i,taskId:'account-'+i}));
  const first=await readInbox('self',{request:()=>reply({notices,more:true,cursor:'100'})});expect(first.more).toBe(true);
  await expect(readInbox('self',{cursor:'100',request:()=>reply({notices,more:true,cursor:'100'})})).rejects.toMatchObject({code:'invalid_reply'});
  await expect(readInbox('self',{request:()=>reply({notices:[],more:true,cursor:'1'})})).rejects.toMatchObject({code:'invalid_reply'});
 });
 test('typed refusals stay distinct from unconfirmed transport errors',async()=>{
  await expect(setInboxState('self',change(),{request:()=>reply({code:'version_conflict',message:'The notice changed.'},false)})).rejects.toMatchObject({status:409,code:'version_conflict'});
  await expect(setInboxState('self',change(),{request:()=>new Response('proxy timeout',{status:408})})).rejects.toMatchObject({status:408,code:'unavailable'});
  await expect(readInboxReceipt('self','notice-change-0001',{request:()=>reply({code:'receipt_pending',message:'No committed receipt is visible yet.'},false)})).rejects.toMatchObject({status:503,code:'receipt_pending'});
 });
 test('malformed or oversized transport does not yield a partial page',async()=>{
  await expect(readInbox('self',{request:()=>reply(page([row()]),true,{'Content-Length':String(maxPacket+1)})})).rejects.toMatchObject({code:'invalid_reply'});
  await expect(readInbox('self',{request:()=>reply(page([row()]),true,{'Content-Length':'1'})})).rejects.toMatchObject({code:'invalid_reply'});
  await expect(readInbox('self',{request:()=>new Response(encode([1,true,page([row()])]).slice(0,-1),{headers:{'Content-Type':'application/msgpack'}})})).rejects.toThrow();
 });
 test('rejected media and a late cancelled reader release their bodies',async()=>{
  let closed=0;const body=()=>new ReadableStream({cancel(){closed++;}});
  await expect(readInbox('self',{request:()=>new Response(body(),{headers:{'Content-Type':'text/html'}})})).rejects.toMatchObject({code:'invalid_reply'});
  const controller=new AbortController();let finish;const pending=readInbox('self',{signal:controller.signal,request:()=>new Promise(resolve=>{finish=resolve;})});
  controller.abort(new DOMException('Account switched.','AbortError'));await expect(pending).rejects.toMatchObject({name:'AbortError'});
  finish(new Response(body(),{headers:{'Content-Type':'application/msgpack'}}));await Promise.resolve();await Promise.resolve();expect(closed).toBe(2);
 });
 test('invalid coordinates fail before calling any native owner',async()=>{
  let called=0;const request=()=>{called++;return reply(page([]));};
  await expect(readInbox('user:8',{request})).rejects.toThrow();await expect(readInbox('self',{id:'account-one',cursor:'1',request})).rejects.toThrow();
  await expect(setInboxState('self',change({revision:Number.MAX_SAFE_INTEGER}),{request})).rejects.toThrow();
  await expect(setInboxState('self',change({requestId:'short'}),{request})).rejects.toThrow();expect(called).toBe(0);
 });
 test('the full Inbox action read is scoped, bounded and separate from recipient notices',async()=>{
  const items=[
   {id:'account-operator-approve',kind:'release',title:'Approve the current release',updated:1000},
   {id:'invitation:river-guild',kind:'invitation',title:'Join River Guild',updated:0},
   {id:'dating:introductions',kind:'dating-introduction',title:'Review current dating introductions',updated:0}
  ];
  const snapshot=()=>reply({context:'self',items});
  const read=()=>readAccountNeeds('self',{request:(url,options)=>{
   expect(url).toBe('/inbox/_summary?context=self');
   expect(options.credentials).toBe('same-origin');expect(options.method).toBe('GET');
   return snapshot();
  }});
  expect((await read()).items.map(x=>x.title)).toEqual(['Approve the current release','Join River Guild','Review current dating introductions']);
  for(const wrong of [
   {items:[items[0],items[0]]},
   {items:[{...items[0],kind:'unknown'}]},
   {items:[{...items[0],title:'\nInvalid'}]},
   {binding:{...binding,actor:'user:8'}},
   {context:'institution:other'}
  ])await expect(readAccountNeeds('self',{request:()=>reply({context:'self',items,...wrong})})).rejects.toMatchObject({code:'invalid_reply'});
  let called=0;await expect(readAccountNeeds('user:8',{request:()=>{called++;return snapshot();}})).rejects.toMatchObject({code:'invalid_reply'});
  expect(called).toBe(0);
 });
});
