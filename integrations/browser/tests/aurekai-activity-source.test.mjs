import {test,expect} from 'bun:test';
import {readDestinations,readActivity,readAccountActivity,updateActivityNotification,sameActivityBinding} from '../aurekai-activity-source.mjs';
import {encode,decode,maxPacket} from '../workspace-msgpack.mjs';

const id=n=>Number(n).toString(16).padStart(64,'0');
const hash='a'.repeat(64),observedAt='2026-10-21T18:00:00.000Z';
const binding=(context='self',actor='user:1')=>({actor,subject:context==='self'?actor:context,scope:context==='self'?'self':context.split(':')[0]});
const job=(n,changes={})=>({id:id(n),seq:n,digest:hash,title:'Research '+n,spec:{provider:'exa',operation:'search'},state:'planned',revision:1,archived:false,created:1000,updated:2000,summary:null,binding:binding(),...changes});
const summary=(changes={})=>({items:1,rawSHA256:hash,observedAt,provider:'exa',tool:'web_search_advanced_exa',...changes});
const complete=(n,items=1,changes={})=>job(n,{state:'completed',summary:summary({items}),...changes});
const body=value=>new Response(encode(value),{headers:{'content-type':'application/msgpack'}});
const page=(jobs=[],more=false,cursor=0,context='self')=>[1,true,{jobs,more,cursor,binding:binding(context)}];
const reading=value=>({request:async url=>{const context=new URL(url,'https://aurekai.work').searchParams.get('context');return body(context&&Array.isArray(value)&&value[1]===true&&value[2]?.binding?[value[0],value[1],{...value[2],binding:binding(context)}]:value);}});
const reject=async(value,options={})=>expect(readActivity('self',{...reading(value),...options})).rejects.toThrow();
const tick=()=>new Promise(resolve=>setTimeout(resolve,0));

test('destinations preserve current authority and use only the native same-origin GET',async()=>{
 const calls=[],rows=[['self','Personal',true],['institution:research-team','Research team',false],['household:home','Home',true]];
 const result=await readDestinations({request:async(url,options)=>{calls.push({url,options});return body([1,rows]);}});
 expect(result).toEqual([{context:'self',label:'Personal',canWrite:true},{context:'institution:research-team',label:'Research team',canWrite:false},{context:'household:home',label:'Home',canWrite:true}]);
 expect(calls).toHaveLength(1);expect(calls[0].url).toBe('/apps/_destinations');
 expect(calls[0].options).toMatchObject({method:'GET',credentials:'same-origin',cache:'no-store',redirect:'error',headers:{Accept:'application/msgpack'}});
 expect(calls[0].options.body).toBeUndefined();
});

test('destination rows cannot substitute URL coordinates, duplicate scope, or weak grant types',async()=>{
 const invalid=[[2,[['self','Personal',true]]],[1,[]],[1,[['self','Personal',true],['self','Again',false]]],[1,[['https://foreign.test','Foreign',true]]],[1,[['institution:team/other','Foreign',true]]],[1,[['self','',true]]],[1,[['self','x'.repeat(321),true]]],[1,[['self','Personal',1]]],[1,[['self','Personal',true,'unowned']]]];
 for(const packet of invalid)await expect(readDestinations(reading(packet))).rejects.toThrow('destination');
});

test('native states remain explicit and only positive completed summaries produce outputs',async()=>{
 const rows=[job(7),job(6,{state:'submitting'}),complete(5,2),complete(4,0),job(3,{state:'uncertain'}),job(2,{state:'refused'}),complete(1,1,{archived:true})];
 const result=await readActivity('institution:research-team',reading(page(rows)));
 expect(result.context).toBe('institution:research-team');expect(result.catalogHref).toBe('/s/models/all?context=institution%3Aresearch-team');
 expect(result.activities.map(a=>a.state)).toEqual(['planned','submitting','completed','completed','uncertain','refused','completed']);
 expect(result.activities.map(a=>a.phase)).toEqual(['pending','active','completed','completed','uncertain','refused','archived']);
 expect(result.activities.map(a=>a.items)).toEqual([null,null,2,0,null,null,1]);
 expect(result.outputs).toEqual([5,1].map(n=>({id:id(n),title:'Research '+n,items:n===5?2:1,rawSHA256:hash,observedAt,href:'/sources/web?context=institution%3Aresearch-team&job='+id(n),archived:n===1})));
 expect(result.activities[2]).toMatchObject({id:id(5),title:'Research 5',provider:'exa',created:1000,updated:2000,archived:false});
 for(const field of ['subagents','unread','notifications','push'])expect(Object.hasOwn(result,field)).toBe(false);
});

test('source-work links use validated IDs and the requested destination, never reply-provided hrefs',async()=>{
 const calls=[],result=await readActivity('household:home',{request:async(url,options)=>{calls.push({url,options});return body(page([complete(1,1,{href:'javascript:alert(1)',url:'https://foreign.test',subject:'user:999'})],false,0,'household:home'));}});
 const url=new URL(calls[0].url,'https://aurekai.work');expect(url.pathname).toBe('/sources/_web');expect([...url.searchParams]).toEqual([['context','household:home'],['operation','list']]);
 expect(result.activities[0].href).toBe('/sources/web?context=household%3Ahome&job='+id(1));expect(result.outputs[0].href).toBe(result.activities[0].href);
 expect(calls[0].options.method).toBe('GET');expect(calls[0].options.body).toBeUndefined();
 expect(Object.hasOwn(result,'subject')).toBe(false);
 await reject(page([job(1,{id:'javascript:alert(1)'})]));
});

test('empty and partial final pages preserve emptiness without invented counts or continuation',async()=>{
 expect(await readActivity('self',reading(page()))).toEqual({context:'self',binding:binding(),activities:[],outputs:[],more:false,cursor:0,catalogHref:'/s/models/all?context=self'});
 const result=await readActivity('self',{cursor:10,...reading(page([job(9),job(3)]))});
 expect(result.activities.map(a=>a.id)).toEqual([id(9),id(3)]);expect(result.more).toBe(false);expect(result.cursor).toBe(0);expect(result.outputs).toEqual([]);
});

test('native pagination preserves exact descending sequence and cursor',async()=>{
 const rows=Array.from({length:20},(_,i)=>job(40-i)),calls=[];
 const result=await readActivity('self',{cursor:50,request:async(url)=>{calls.push(url);return body(page(rows,true,21));}});
 expect(result.activities).toHaveLength(20);expect(result.more).toBe(true);expect(result.cursor).toBe(21);expect(new URL(calls[0],'https://aurekai.work').searchParams.get('cursor')).toBe('50');
 for(const invalid of [page(rows,true,22),page(rows.slice(1),true,21),page(rows,false,21),page([...rows,job(20)]),page([job(1),job(2)]),page([job(2),job(1,{id:id(2)})]),page([job(2),job(1,{seq:2})]),[1,true,{jobs:[],more:0,cursor:0}],[1,true,{jobs:[],more:false,cursor:'0'}]])await reject(invalid);
 await reject(page([job(50)]),{cursor:50});
});

test('invalid run metadata fails as one reply rather than becoming a successful partial page',async()=>{
 for(const change of [{digest:'bad'},{seq:0},{revision:0},{title:''},{title:'x'.repeat(501)},{state:'running'},{archived:1},{created:-1},{updated:'2000'},{updated:999},{spec:{provider:'other',operation:'search'}},{spec:{provider:'exa',operation:'execute'}},{summary:summary()}])await reject(page([job(2),job(1,change)]));
});

test('completed summaries require genuine bounded counts, retained hashes and native timestamps',async()=>{
 for(const change of [{items:null},{items:'1'},{items:-1},{items:1.5},{items:26},{rawSHA256:'x'},{observedAt:'2026-02-31T00:00:00.000Z'},{observedAt:'yesterday'},{provider:'firecrawl'},{tool:'firecrawl_search'}])await reject(page([complete(1,1,{summary:summary(change)})]));
 await reject(page([complete(1,1,{summary:null})]));
 const firecrawl=complete(1,0,{spec:{provider:'firecrawl',operation:'read'},summary:summary({items:0,provider:'firecrawl',tool:'firecrawl_scrape'})});
 const result=await readActivity('self',reading(page([firecrawl])));expect(result.activities[0].items).toBe(0);expect(result.outputs).toEqual([]);
});

test('retained millisecond timestamps remain renderable at the Date boundary',async()=>{
 const maximum=8640000000000000;
 for(const value of [maximum+1,Number.MAX_SAFE_INTEGER]){
  expect(Number.isSafeInteger(value)).toBe(true);
  await reject(page([job(1,{updated:value})]));await reject(page([job(1,{created:value,updated:value})]));
 }
 const result=await readActivity('self',reading(page([job(1,{created:0,updated:maximum})])));
 expect(new Date(result.activities[0].created).toISOString()).toBe('1970-01-01T00:00:00.000Z');
 expect(new Date(result.activities[0].updated).toISOString()).toBe('+275760-09-13T00:00:00.000Z');
});

test('observation timestamps match the actual native Date.toISOString serialization',async()=>{
 // The native protocol uses new Date().toISOString(): milliseconds and Z are
 // mandatory, including the extended-year form at valid Date boundaries.
 for(const millis of [-8640000000000000,0,Date.UTC(2026,9,21,18),8640000000000000]){
  const native=new Date(millis).toISOString(),result=await readActivity('self',reading(page([complete(1,1,{summary:summary({observedAt:native})})])));
  expect(result.outputs[0].observedAt).toBe(native);
 }
 for(const value of ['2026-10-21T18:00:00Z','2026-10-21T18:00:00.000+00:00','+275760-09-13T00:00:00.001Z'])await reject(page([complete(1,1,{summary:summary({observedAt:value})})]));
});

test('exact run reads retain identity and summary consistency without copying preview material',async()=>{
 const item={id:id(99),bodySHA256:hash,title:'https://example.test/'+'p'.repeat(700),body:'Preview only',preview:true};
 const selected=complete(1,1,{items:[item]}),calls=[];
 const result=await readActivity('self',{jobId:id(1),request:async url=>{calls.push(url);return body([1,true,selected]);}});
 expect(new URL(calls[0],'https://aurekai.work').searchParams.get('operation')).toBe('get');expect(new URL(calls[0],'https://aurekai.work').searchParams.get('job')).toBe(id(1));
 expect(result.outputs).toHaveLength(1);expect(Object.hasOwn(result.outputs[0],'body')).toBe(false);expect(result.more).toBe(false);
 for(const value of [{...selected,id:id(2)},{...selected,items:[]},{...selected,summary:summary({items:2}),items:[item,item]},{...selected,items:[{...item,bodySHA256:'bad'}]}])await reject([1,true,value],{jobId:id(1)});
 const pending=await readActivity('self',{jobId:id(1),...reading([1,true,job(1,{items:[]})])});expect(pending.outputs).toEqual([]);
});
test('exact provider continuation keeps only the native admitted capability and safe provider identity',async()=>{
 const capability={canContinue:true,canAdminister:false,modes:['provider_tab_sidepanel'],reason:'',provider:{id:'exa',name:'Exa',secret:'owner-only'},credentialInput:null,secret:'owner-only'},selected=job(1,{items:[],providerContinuation:capability});
 const result=await readActivity('self',{jobId:id(1),...reading([1,true,selected])});expect(result.providerContinuation).toEqual({canContinue:true,canAdminister:false,modes:['provider_tab_sidepanel'],reason:'',provider:{id:'exa',name:'Exa'},credentialInput:null});
 for(const change of [{canContinue:'true'},{canAdminister:true},{modes:['iframe']},{provider:null},{credentialInput:'password'}])await reject([1,true,{...selected,providerContinuation:{...capability,...change}}],{jobId:id(1)});
});

test('invalid request coordinates are rejected before any request',async()=>{
 let calls=0;const request=async()=>{calls++;return body(page());};
 for(const context of ['',null,'user:1','institution:Team','self&subject=user:2','//foreign.test'])await expect(readActivity(context,{request})).rejects.toThrow('destination');
 for(const options of [{cursor:-1},{cursor:'20'},{cursor:1.5},{jobId:'x'},{jobId:id(1),cursor:20}])await expect(readActivity('self',{request,...options})).rejects.toThrow();
 expect(calls).toBe(0);
});

test('native refusal remains a refusal, with no retry or empty success projection',async()=>{
 let calls=0;
 await expect(readActivity('self',{request:async()=>{calls++;return body([1,false,{code:'connection_unavailable',message:'The native owner is unavailable.'}]);}})).rejects.toMatchObject({code:'connection_unavailable'});
 expect(calls).toBe(1);
 for(const value of [[1,false,{}],[1,false,{code:'wrong code',message:'No'}],[1,true,null],[2,true,{jobs:[],more:false,cursor:0}]])await reject(value);
});

test('HTTP failures, unsupported format and declared oversize cancel the response body',async()=>{
 for(const fixture of [{status:403,type:'application/msgpack'},{status:503,type:'application/msgpack'},{status:200,type:'application/json'},{status:200,type:'application/msgpack',length:String(maxPacket+1)},{status:200,type:'application/msgpack',length:'invalid'}]){
  let cancelled=false;const headers={'content-type':fixture.type};if(fixture.length)headers['content-length']=fixture.length;
  const request=async()=>new Response(new ReadableStream({cancel(){cancelled=true;}}),{status:fixture.status,headers});
  const pending=readActivity('self',{request});if(fixture.status===403)await expect(pending).rejects.toMatchObject({code:'access_required',status:403});else await expect(pending).rejects.toThrow();
  expect(cancelled).toBe(true);
 }
});

test('streaming replies are bounded even without content length and are released on error',async()=>{
 let cancelled=false;
 const request=async()=>new Response(new ReadableStream({start(c){c.enqueue(new Uint8Array(maxPacket));c.enqueue(new Uint8Array(1));},cancel(){cancelled=true;}}),{headers:{'content-type':'application/msgpack'}});
 await expect(readActivity('self',{request})).rejects.toThrow('limit');expect(cancelled).toBe(true);
 for(const bytes of [new Uint8Array(),new Uint8Array([0xc1]),new Uint8Array([...encode(page()),0])])await expect(readActivity('self',{request:async()=>new Response(bytes,{headers:{'content-type':'application/msgpack'}})})).rejects.toThrow();
 const bytes=encode(page([job(1)]));let offset=0;
 const result=await readActivity('self',{request:async()=>new Response(new ReadableStream({pull(c){if(offset<bytes.length)c.enqueue(bytes.subarray(offset,++offset));else c.close();}}),{headers:{'content-type':'application/msgpack; charset=binary'}})});
 expect(result.activities[0].id).toBe(id(1));
});

test('a pre-aborted destination read never opens a response',async()=>{
 const controller=new AbortController();controller.abort();let calls=0;
 for(const read of [()=>readDestinations({signal:controller.signal,request:async()=>{calls++;return body([1,[['self','Personal',true]]]);}}),()=>readActivity('self',{signal:controller.signal,request:async()=>{calls++;return body(page());}})])await expect(read()).rejects.toMatchObject({name:'AbortError'});
 expect(calls).toBe(0);
});

test('destination cancellation interrupts an injected pending request and cancels its late body',async()=>{
 const controller=new AbortController();let release,requestSignal,cancelled=false;
 const pending=readActivity('self',{signal:controller.signal,request:async(url,{signal})=>{requestSignal=signal;return new Promise(resolve=>release=resolve);}});
 const rejected=pending.catch(error=>error);controller.abort();expect((await rejected).name).toBe('AbortError');
 expect(requestSignal.aborted).toBe(true);
 release(new Response(new ReadableStream({cancel(){cancelled=true;}}),{headers:{'content-type':'application/msgpack'}}));await tick();expect(cancelled).toBe(true);
});

test('destination cancellation interrupts a stalled body reader and releases it',async()=>{
 const controller=new AbortController();let cancelled=false,started;
 const ready=new Promise(resolve=>started=resolve),response=new Response(new ReadableStream({pull(){started();},cancel(){cancelled=true;}}),{headers:{'content-type':'application/msgpack'}});
 const pending=readActivity('self',{signal:controller.signal,request:async()=>response}),rejected=pending.catch(error=>error);
 await ready;await tick();controller.abort();expect((await rejected).name).toBe('AbortError');expect(cancelled).toBe(true);expect(response.body.locked).toBe(false);
});

const taskHref=(context='self',taskId='work-1')=>'/apps?'+new URLSearchParams({context})+'#fabric-task-'+taskId;
const fabric=(context='self')=>({source:'fabric',context,binding:binding(context),revision:'native-1',unread:1,more:false,
 tasks:[{id:'task:work-1',taskId:'work-1',parentTaskId:null,kind:'task',title:'Native work',state:'running',phase:'active',revision:1,created:1000,updated:2000,reconciliationRequired:false,href:taskHref(context)},
 {id:'run:run-1',taskId:'work-1',parentTaskId:'task:work-1',kind:'subagent',title:'Review',state:'running',phase:'active',revision:1,created:1000,updated:2000,reconciliationRequired:false,href:taskHref(context)}],
 outputs:[{id:'receipt-1',taskId:'work-1',title:'Implementation patch receipt',kind:'implementation',digest:hash,created:2000,href:taskHref(context)}],
 notifications:[{id:'notice-1',taskId:'work-1',title:'Review completed',kind:'task',state:'unread',revision:1,created:1000,updated:2000,allowedActions:['read','dismiss'],href:taskHref(context)}]});
test('research and Fabric carry exact server bindings, including self account changes',async()=>{
 const research=await readActivity('self',reading(page([job(1)]))),native=await readAccountActivity('self',reading([1,true,fabric()]));
 expect(sameActivityBinding(research.binding,native.binding)).toBe(true);expect(sameActivityBinding(research.binding,binding('self','user:2'))).toBe(false);
 for(const wrong of [null,{},binding('institution:other'),{actor:'user:2',subject:'user:1',scope:'self'},{...binding(),authority:'invented'}]){
  await expect(readActivity('self',{request:async()=>body([1,true,{jobs:[],more:false,cursor:0,binding:wrong}])})).rejects.toThrow();
  await expect(readAccountActivity('self',{request:async()=>body([1,true,{...fabric(),binding:wrong}])})).rejects.toThrow();
 }
});
test('Fabric projects actual parent-bound subagents, receipt metadata and recipient notifications',async()=>{
 const calls=[],value=fabric('institution:studio');
 const result=await readAccountActivity('institution:studio',{request:async(url,options)=>{calls.push({url,options});return body([1,true,value]);}});
 expect(result.binding).toEqual(binding('institution:studio'));expect(result.tasks[1].parentTaskId).toBe(result.tasks[0].id);expect(result.outputs[0].digest).toBe(hash);expect(result.notifications[0].allowedActions).toEqual(['read','dismiss']);expect(result.unread).toBe(1);
 expect(new URL(calls[0].url,'https://aurekai.work').searchParams.get('operation')).toBe('read');expect(calls[0].options.method).toBe('GET');expect(calls[0].options.body).toBeUndefined();
});
test('Fabric browser state selects safe metadata instead of retaining unrequested owner fields',async()=>{
 const value=fabric(),marker='owner-only-marker';value.ownerSecret=marker;for(const row of [...value.tasks,...value.outputs,...value.notifications]){row.rawBody=marker;row.privatePath=marker;}
 const result=await readAccountActivity('self',reading([1,true,value]));
 expect(Object.hasOwn(result,'ownerSecret')).toBe(false);for(const row of [...result.tasks,...result.outputs,...result.notifications]){expect(Object.hasOwn(row,'rawBody')).toBe(false);expect(Object.hasOwn(row,'privatePath')).toBe(false);}
 expect(result.tasks[0].title).toBe('Native work');expect(result.outputs[0].digest).toBe(hash);expect(result.notifications[0].allowedActions).toEqual(['read','dismiss']);
});
test('malformed Fabric bindings, rows, source identities and unsafe continuations fail the whole read',async()=>{
 const changes=[value=>value.source='research',value=>value.context='institution:other',value=>value.unread=0,value=>value.more='false',value=>value.tasks[0].revision=0,value=>value.tasks[1].parentTaskId='task:other',value=>value.tasks[1].kind='worker',value=>value.outputs[0].digest='bad',value=>value.outputs[0].taskId='other',value=>value.notifications[0].state='completed',value=>value.notifications[0].allowedActions=['cancel'],value=>value.notifications.push({...value.notifications[0]}),value=>value.tasks[0].href='https://foreign.test/',value=>value.outputs[0].href='/apps?context=self#fabric-task-other',value=>value.notifications[0].href='/apps?context=self&actor=user:2#fabric-task-work-1'];
 for(const change of changes){const value=fabric();change(value);await expect(readAccountActivity('self',reading([1,true,value]))).rejects.toThrow();}
});
test('exact task reads retain current recipient notices without substituting a different task',async()=>{
 const value=fabric();value.notifications[0].taskId='older-work';value.notifications[0].href=taskHref('self','older-work');let address;
 const result=await readAccountActivity('self',{taskId:'work-1',request:async url=>{address=url;return body([1,true,value]);}});
 expect(new URL(address,'https://aurekai.work').searchParams.get('task')).toBe('work-1');expect(result.notifications[0].taskId).toBe('older-work');
 await expect(readAccountActivity('self',{taskId:'different',...reading([1,true,fabric()])})).rejects.toThrow();
});
test('notification effects send one exact MessagePack recipient action and require its matching receipt',async()=>{
 const change={id:'notice-1',revision:1,action:'read',requestId:'request-1'},receipt={requestId:'request-1',id:'notice-1',action:'read',revision:2,state:'read',replayed:false},calls=[];
 const result=await updateActivityNotification('self',change,{request:async(url,options)=>{calls.push({url,options});return body([1,true,receipt]);}});
 expect(result).toEqual(receipt);expect(calls).toHaveLength(1);expect(calls[0].url).toBe('/apps/_fabric?context=self&operation=notification');expect(calls[0].options.method).toBe('POST');expect(calls[0].options.headers['Content-Type']).toBe('application/msgpack');expect(decode(calls[0].options.body)).toEqual([1,change]);
 for(const wrong of [{...receipt,id:'other'},{...receipt,action:'dismiss'},{...receipt,state:'dismissed'},{...receipt,revision:0},{...receipt,requestId:'other'}])await expect(updateActivityNotification('self',change,reading([1,true,wrong]))).rejects.toThrow();
});
test('notification ownership coordinates and invalid actions are refused before transport',async()=>{
 let calls=0;const request=async()=>{calls++;throw Error('must not call');};
 for(const change of [{id:'n',revision:0,action:'read',requestId:'r'},{id:'n',revision:1,action:'cancel',requestId:'r'},{id:'n',revision:1,action:'dismiss',requestId:'r',actor:'user:2'}])await expect(updateActivityNotification('self',change,{request})).rejects.toThrow();
 expect(calls).toBe(0);
});
