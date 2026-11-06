// Projections and recipient actions stay in their existing native owners.
// The browser retains no work, notification state or credential store.
import {encode,decode,maxPacket} from './workspace-msgpack.mjs';

const record=value=>value!==null&&typeof value==='object'&&!Array.isArray(value);
const hex=value=>typeof value==='string'&&/^[a-f0-9]{64}$/.test(value);
const integer=(value,min=0)=>Number.isSafeInteger(value)&&value>=min;
// WebWork retains Date.now() milliseconds; every accepted value must also be
// representable by the renderer's Date/toISOString without a RangeError.
const timestamp=value=>integer(value)&&value<=8640000000000000;
const text=(value,max)=>typeof value==='string'&&value.length<=max&&!value.includes('\0');
const contextValid=value=>value==='self'||typeof value==='string'&&/^(?:institution|household):[a-z0-9-]{1,63}$/.test(value);
const states=new Set(['planned','submitting','completed','refused','uncertain']);
const tools={exa:new Set(['web_search_exa','web_search_advanced_exa','web_fetch_exa']),firecrawl:new Set(['firecrawl_search','firecrawl_scrape'])};
const phases={planned:'pending',submitting:'active',completed:'completed',refused:'refused',uncertain:'uncertain'};
const check=(ok,message='The native activity reply is incomplete.')=>{if(!ok)throw Object.assign(Error(message),{code:'invalid_reply'});};
function abortReason(signal){return signal.reason instanceof Error?signal.reason:new DOMException('Activity read cancelled.','AbortError');}
function interrupted(signal){if(signal?.aborted)throw abortReason(signal);}
function cancel(body){try{body?.cancel()?.catch(()=>{});}catch{}}
function waitFor(promise,signal){
 interrupted(signal);
 return new Promise((resolve,reject)=>{
  const abort=()=>{signal.removeEventListener('abort',abort);reject(abortReason(signal));};
  signal.addEventListener('abort',abort,{once:true});
  Promise.resolve(promise).then(value=>{signal.removeEventListener('abort',abort);resolve(value);},error=>{signal.removeEventListener('abort',abort);reject(error);});
 });
}
export async function packet(url,{request=fetch,signal,body,allowErrorPacket=false,encodedBody=false,exactLength=false}={}){
 interrupted(signal);check(typeof request==='function','A native activity reader is required.');
 const controller=new AbortController(),abort=()=>controller.abort(signal.reason);
 const timer=setTimeout(()=>controller.abort(new DOMException('Activity read timed out.','TimeoutError')),15000);
 signal?.addEventListener('abort',abort,{once:true});
 try{
  const pending=Promise.resolve(request(url,{method:body===undefined?'GET':'POST',credentials:'same-origin',cache:'no-store',redirect:'error',headers:{Accept:'application/msgpack',...(body===undefined?{}:{'Content-Type':'application/msgpack'})},body:body===undefined?undefined:encodedBody?body:encode([1,body]),signal:controller.signal}));
  // An injected reader may ignore cancellation. Discard and release any late body.
  pending.then(response=>{if(controller.signal.aborted)cancel(response?.body);},()=>{});
  const response=await waitFor(pending,controller.signal);interrupted(controller.signal);
  if(!response?.ok&&(!allowErrorPacket||[401,403].includes(response?.status))){cancel(response?.body);throw Object.assign(Error([401,403].includes(response?.status)?'Sign in with current access to this destination.':'The native activity owner is unavailable.'),{code:[401,403].includes(response?.status)?'access_required':'unavailable',status:response?.status});}
  const type=response.headers?.get('content-type')?.split(';')[0]?.trim().toLowerCase();
  if(type!=='application/msgpack'){cancel(response.body);check(false,'The native activity reply has an unsupported format.');}
  const declared=response.headers.get('content-length');
  if(declared!==null&&(!/^\d+$/.test(declared)||!integer(Number(declared))||Number(declared)>maxPacket)){cancel(response.body);check(false,'The native activity reply exceeds its limit.');}
  const reader=response.body?.getReader();check(reader,'The native activity reply is missing.');
  const chunks=[];let size=0;
  try{
   for(;;){const part=await waitFor(reader.read(),controller.signal);interrupted(controller.signal);if(part.done)break;
    check(part.value instanceof Uint8Array);size+=part.value.byteLength;check(size<=maxPacket,'The native activity reply exceeds its limit.');chunks.push(part.value);}
  }finally{try{reader.cancel()?.catch(()=>{});}catch{}try{reader.releaseLock();}catch{}}
  interrupted(controller.signal);check(size>0&&(!exactLength||declared===null||Number(declared)===size),'The native activity reply was empty or interrupted.');
  const bytes=new Uint8Array(size);let at=0;for(const chunk of chunks){bytes.set(chunk,at);at+=chunk.byteLength;}
  const decoded=decode(bytes);if(!response.ok)check(Array.isArray(decoded)&&decoded.length===3&&decoded[0]===1&&decoded[1]===false,'The native error response is incomplete.');return decoded;
 }finally{clearTimeout(timer);signal?.removeEventListener('abort',abort);}
}

export async function readDestinations(options={}){
 const value=await packet('/apps/_destinations',options);
 check(Array.isArray(value)&&value.length===2&&value[0]===1&&Array.isArray(value[1])&&value[1].length>0&&value[1].length<=201,'The current destination list is incomplete.');
 const seen=new Set();
 return value[1].map(row=>{
  check(Array.isArray(row)&&row.length===3&&contextValid(row[0])&&text(row[1],320)&&row[1].trim()&&typeof row[2]==='boolean'&&!seen.has(row[0]),'The current destination list is incomplete.');
  seen.add(row[0]);return {context:row[0],label:row[1],canWrite:row[2]};
 });
}
function webValue(value){
 check(Array.isArray(value)&&value.length===3&&value[0]===1&&typeof value[1]==='boolean');
 if(!value[1]){const error=value[2];check(record(error)&&text(error.code,64)&&/^[a-z][a-z0-9_]*$/.test(error.code)&&text(error.message,2000)&&error.message.trim());throw Object.assign(Error(error.message),{code:error.code});}
 check(record(value[2]));return value[2];
}
export function activityBinding(value,context){
 check(record(value)&&Object.keys(value).length===3&&typeof value.actor==='string'&&/^user:[1-9][0-9]{0,18}$/.test(value.actor),'The activity account binding is incomplete.');
 check(value.subject===(context==='self'?value.actor:context)&&value.scope===(context==='self'?'self':context.split(':')[0]),'Activity belongs to a different native destination.');
 return Object.freeze({actor:value.actor,subject:value.subject,scope:value.scope});
}
export const sameActivityBinding=(a,b)=>Boolean(a&&b&&a.actor===b.actor&&a.subject===b.subject&&a.scope===b.scope);
function iso(value){if(!text(value,27)||!/^(?:\d{4}|[+-]\d{6})-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/.test(value))return false;const date=new Date(value);return Number.isFinite(date.valueOf())&&date.toISOString()===value;}
function summary(value,provider){
 check(record(value)&&integer(value.items)&&value.items<=25&&hex(value.rawSHA256)&&iso(value.observedAt)&&value.provider===provider&&typeof value.tool==='string'&&tools[provider].has(value.tool),'The retained output summary is incomplete.');
 return {items:value.items,rawSHA256:value.rawSHA256,observedAt:value.observedAt};
}
function project(job,context,{exact=false}={}){
 check(record(job)&&hex(job.id)&&hex(job.digest)&&integer(job.seq,1)&&integer(job.revision,1)&&text(job.title,500)&&job.title.trim()&&states.has(job.state)&&typeof job.archived==='boolean'&&timestamp(job.created)&&timestamp(job.updated)&&job.updated>=job.created&&record(job.spec)&&Object.hasOwn(tools,job.spec.provider)&&['search','read'].includes(job.spec.operation),'The retained research run is incomplete.');
 const retained=job.state==='completed'?summary(job.summary,job.spec.provider):null;
 if(!retained)check(job.summary===null,'An unfinished run has an invalid output summary.');
 if(exact){
  check(Array.isArray(job.items)&&job.items.length<=25&&job.items.length===(retained?.items??0),'The exact retained result list is incomplete.');
  // Preview titles and material are not projected here; only retained identities
  // corroborate the summary. Full source text belongs to the exact item read.
  const ids=new Set();for(const item of job.items){check(record(item)&&hex(item.id)&&hex(item.bodySHA256)&&!ids.has(item.id),'A retained result identity is incomplete.');ids.add(item.id);}
 }
 const href='/sources/web?'+new URLSearchParams({context,job:job.id});
 const activity={id:job.id,title:job.title,state:job.state,phase:job.archived?'archived':phases[job.state],archived:job.archived,provider:job.spec.provider,revision:job.revision,created:job.created,updated:job.updated,href,items:retained?.items??null};
 const output=retained&&retained.items>0?{id:job.id,title:job.title,...retained,href,archived:job.archived}:null;
 return {activity,output};
}
function providerContinuation(value){
 if(value===undefined||value===null)return null;
 check(record(value)&&typeof value.canContinue==='boolean'&&typeof value.canAdminister==='boolean'&&text(value.reason,2000)&&Array.isArray(value.modes),'The native provider capability is incomplete.');
 check(value.canContinue?value.modes.length===1&&value.modes[0]==='provider_tab_sidepanel':value.modes.length===0&&!value.canAdminister,'The native provider mode is incomplete.');
 check(value.provider===null||record(value.provider)&&Object.hasOwn(tools,value.provider.id)&&text(value.provider.name,160)&&value.provider.name.trim(),'The native provider identity is incomplete.');
 check((!value.canContinue||value.provider!==null)&&value.credentialInput===(value.canContinue&&value.canAdminister?'native_api_key':null),'The native credential capability is incomplete.');
 return {canContinue:value.canContinue,canAdminister:value.canAdminister,modes:Object.freeze([...value.modes]),reason:value.reason,provider:value.provider?{id:value.provider.id,name:value.provider.name}:null,credentialInput:value.credentialInput};
}

// `context` is the requested destination, not a server-echoed identity. The native
// endpoint rechecks its current session/grant on every call. The renderer must
// abort the previous read and check its destination epoch before displaying it.
export async function readActivity(context,{jobId,cursor=0,...options}={}){
 check(contextValid(context),'Choose a current activity destination.');
 check(integer(cursor),'Use the retained activity cursor.');
 check(jobId===undefined||hex(jobId)&&cursor===0,'Choose one exact retained research run.');
 const query=new URLSearchParams({context,operation:jobId===undefined?'list':'get'});
 if(jobId!==undefined)query.set('job',jobId);else if(cursor)query.set('cursor',String(cursor));
 const value=webValue(await packet('/sources/_web?'+query,options)),binding=activityBinding(value.binding,context);let jobs,more=false,next=0;
 if(jobId!==undefined){check(value.id===jobId,'The native owner returned a different retained run.');jobs=[value];}
 else{
  check(Array.isArray(value.jobs)&&value.jobs.length<=20&&typeof value.more==='boolean'&&integer(value.cursor),'The retained activity page is incomplete.');
  jobs=value.jobs;more=value.more;next=value.cursor;
  check(more?jobs.length===20&&next>0:next===0,'The retained activity cursor is incomplete.');
 }
 const activities=[],outputs=[],ids=new Set();let previous=cursor||Infinity;
 for(const job of jobs){
  check(record(job)&&!ids.has(job.id)&&integer(job.seq,1)&&job.seq<previous,'The retained activity page repeated or reordered a run.');ids.add(job.id);previous=job.seq;
  const row=project(job,context,{exact:jobId!==undefined});activities.push(row.activity);if(row.output)outputs.push(row.output);
 }
 if(more)check(next===previous,'The retained activity cursor does not match its page.');
 // The catalogue remains estate-wide; context is a shared-rail selection hint.
 return {context,binding,activities,outputs,more,cursor:next,catalogHref:'/s/models/all?'+new URLSearchParams({context}),...(jobId===undefined?{}:{providerContinuation:providerContinuation(value.providerContinuation)})};
}

const nativeId=value=>typeof value==='string'&&/^[A-Za-z0-9][A-Za-z0-9_.:-]{0,159}$/.test(value);
const taskPhases=new Set(['pending','active','completed','failed','cancelled','blocked','review','uncertain']);
function workLink(value,context,taskId){
 check(text(value,1024)&&value.startsWith('/apps?')&&!value.includes('\\'),'The native work address is incomplete.');
 const url=new URL(value,'https://activity.invalid');
 check(url.origin==='https://activity.invalid'&&url.pathname==='/apps'&&[...url.searchParams].length===1&&url.searchParams.get('context')===context&&url.hash==='#fabric-task-'+encodeURIComponent(taskId),'The native work address belongs to another task.');
 return value;
}
function uniqueRows(value,limit){check(Array.isArray(value)&&value.length<=limit);const seen=new Set();for(const row of value){check(record(row)&&nativeId(row.id)&&!seen.has(row.id));seen.add(row.id);}return value;}
function times(row){check(timestamp(row.created)&&timestamp(row.updated)&&row.updated>=row.created);}
export async function readAccountActivity(context,{taskId,...options}={}){
 check(contextValid(context),'Choose a current activity destination.');check(taskId===undefined||nativeId(taskId),'Choose one exact native task.');
 const query=new URLSearchParams({context,operation:'read'});if(taskId!==undefined)query.set('task',taskId);
 const value=webValue(await packet('/apps/_fabric?'+query,options));
 check(value.source==='fabric'&&value.context===context&&text(value.revision,160)&&value.revision.length>0&&integer(value.unread)&&typeof value.more==='boolean');
 const binding=activityBinding(value.binding,context);
 const tasks=uniqueRows(value.tasks,100).map(row=>{
  check(nativeId(row.taskId)&&['task','subagent'].includes(row.kind)&&text(row.title,500)&&row.title.trim()&&text(row.state,64)&&row.state.length>0&&taskPhases.has(row.phase)&&integer(row.revision,1)&&typeof row.reconciliationRequired==='boolean');times(row);
  check(row.workKind===undefined||['model','operator'].includes(row.workKind),'The native task owner is incomplete.');
  check(row.kind==='task'?row.id==='task:'+row.taskId&&row.parentTaskId===null:row.id.startsWith('run:')&&row.parentTaskId==='task:'+row.taskId);
  check(taskId===undefined||row.taskId===taskId,'The native owner returned a different task.');
  return {id:row.id,taskId:row.taskId,parentTaskId:row.parentTaskId,kind:row.kind,workKind:row.workKind||'operator',title:row.title,state:row.state,phase:row.phase,revision:row.revision,created:row.created,updated:row.updated,reconciliationRequired:row.reconciliationRequired,href:workLink(row.href,context,row.taskId)};
 });
 const roots=new Set(tasks.filter(row=>row.kind==='task').map(row=>row.id));check(tasks.every(row=>row.kind==='task'||roots.has(row.parentTaskId)),'A native subagent is missing its parent task.');
 const outputs=uniqueRows(value.outputs,40).map(row=>{check(nativeId(row.taskId)&&roots.has('task:'+row.taskId)&&text(row.title,160)&&row.title.trim()&&text(row.kind,80)&&hex(row.digest)&&timestamp(row.created));check(taskId===undefined||row.taskId===taskId);return {id:row.id,taskId:row.taskId,title:row.title,kind:row.kind,digest:row.digest,created:row.created,href:workLink(row.href,context,row.taskId)};});
 const notifications=uniqueRows(value.notifications,40).map(row=>{
  check(nativeId(row.taskId)&&text(row.title,500)&&row.title.trim()&&text(row.kind,80)&&['unread','read'].includes(row.state)&&integer(row.revision,1));times(row);
  const allowed=row.state==='unread'?['read','dismiss']:['dismiss'];check(Array.isArray(row.allowedActions)&&row.allowedActions.length===allowed.length&&row.allowedActions.every((action,index)=>action===allowed[index]));
  return {id:row.id,taskId:row.taskId,title:row.title,kind:row.kind,state:row.state,revision:row.revision,created:row.created,updated:row.updated,allowedActions:Object.freeze([...row.allowedActions]),href:workLink(row.href,context,row.taskId)};
 });
 check(value.unread>=notifications.filter(row=>row.state==='unread').length);
 return {source:'fabric',context,binding,revision:value.revision,tasks,outputs,notifications,unread:value.unread,more:value.more};
}
export async function updateActivityNotification(context,change,options={}){
 check(contextValid(context),'Choose a current activity destination.');
 check(record(change)&&Object.keys(change).length===4&&nativeId(change.id)&&integer(change.revision,1)&&['read','dismiss'].includes(change.action)&&nativeId(change.requestId),'Choose a current notification action.');
 const value=webValue(await packet('/apps/_fabric?'+new URLSearchParams({context,operation:'notification'}),{...options,body:change}));
 check(value.requestId===change.requestId&&value.id===change.id&&value.action===change.action&&integer(value.revision,1)&&value.revision>=change.revision&&value.state===(change.action==='read'?'read':'dismissed')&&typeof value.replayed==='boolean','The notification receipt does not match this action.');
 return {requestId:value.requestId,id:value.id,action:value.action,revision:value.revision,state:value.state,replayed:value.replayed};
}

const taskIdValid=value=>typeof value==='string'&&/^[A-Za-z0-9._:-]{1,127}$/.test(value);
const publicText=(value,max,empty=false)=>text(value,max)&&new TextEncoder().encode(value).length<=max&&!/[\u0000-\u001f\u007f]/.test(value)&&(empty||Boolean(value.trim()));
const queueTaskPhases=new Set(['pending','active','attention','completed','cancelled']);
const nodePhase={defined:'pending',blocked:'pending',ready:'pending',retry_wait:'pending',running:'active',complete:'completed',dead_letter:'attention',cancel_requested:'attention',cancelled:'cancelled'};
const workerPhase={running:'active',waiting:'pending',completed:'completed',failed:'attention'};
function continuation(value,context){
 check(publicText(value,512,true),'The task continuation is incomplete.');
 if(!value)return '';
 const [path,query,...rest]=value.split('?');
 check(!rest.length&&/^\/(?:apps(?:\/[a-z0-9_-]+)*|sources\/web)$/.test(path)&&
  (query===undefined?context==='self':query==='context='+context),'The task continuation does not match this destination.');
 return value;
}
function projectTask(task,context){
 check(record(task)&&taskIdValid(task.id)&&integer(task.seq,1)&&publicText(task.title,160)&&
  Object.hasOwn(nodePhase,task.state)&&queueTaskPhases.has(task.phase)&&hex(task.version)&&
  timestamp(task.created)&&timestamp(task.updated)&&task.updated>=task.created&&record(task.counts),
  'The native task is incomplete.');
 const counts={};
 for(const name of ['nodes','workers','activeWorkers','outputs','pending','active','attention','completed','cancelled']){
  const value=task.counts[name];check(integer(value)&&value<=(name==='workers'||name==='activeWorkers'?512:4096),'The native task counts are incomplete.');counts[name]=value;
 }
 check(counts.nodes>0&&counts.outputs<=counts.nodes&&counts.activeWorkers<=counts.workers&&['pending','active','attention','completed','cancelled'].reduce((n,key)=>n+counts[key],0)===counts.nodes,'The native task counts disagree.');
 for(const [key,limit,count] of [['nodes',32,counts.nodes],['workers',16,counts.workers],['outputs',32,counts.outputs]]){
  check(Array.isArray(task[key])&&task[key].length===Math.min(count,limit)&&typeof task[key+'More']==='boolean'&&task[key+'More']===(count>limit),'The native task detail limit is incomplete.');
 }
 const nodeIds=new Set(),nodes=task.nodes.map(node=>{
  check(record(node)&&publicText(node.id,127)&&!nodeIds.has(node.id)&&
   (node.parentId===null||publicText(node.parentId,127)&&node.parentId!==node.id)&&
   publicText(node.operatorId,127)&&publicText(node.family,127,true)&&Object.hasOwn(nodePhase,node.state)&&
   (node.phase===nodePhase[node.state]||node.state==='running'&&node.phase==='attention'||task.id.startsWith('account-operator-')&&node.id==='job'&&node.state==='blocked'&&node.phase==='attention')&&
   integer(node.attempt)&&timestamp(node.updated)&&node.updated<=task.updated&&typeof node.hasOutput==='boolean'&&
   (!node.hasOutput||node.state==='complete'),'The native task step is incomplete.');
  nodeIds.add(node.id);
  return {id:node.id,parentId:node.parentId,operatorId:node.operatorId,family:node.family,state:node.state,phase:node.phase,attempt:node.attempt,updated:node.updated,hasOutput:node.hasOutput};
 });
 check(nodes[0]?.id==='job'&&nodes[0].parentId===null&&nodes[0].state===task.state,'The native task root is incomplete.');
 const byId=new Map(nodes.map(node=>[node.id,node]));
 for(const node of nodes){
  const seen=new Set([node.id]);let parent=node.parentId;
  while(parent!==null){
   check(!seen.has(parent),'The native task hierarchy has a cycle.');seen.add(parent);
   if(!byId.has(parent)){check(task.nodesMore,'The native task parent is missing.');break;}
   parent=byId.get(parent).parentId;
  }
 }
 const workerIds=new Set(),workers=task.workers.map(worker=>{
  check(record(worker)&&publicText(worker.id,127)&&!workerIds.has(worker.id)&&publicText(worker.workerId,127)&&
   Object.hasOwn(workerPhase,worker.state)&&worker.phase===workerPhase[worker.state]&&
   timestamp(worker.created)&&timestamp(worker.updated)&&worker.updated>=worker.created&&worker.updated<=task.updated,
   'The native worker run is incomplete.');
  workerIds.add(worker.id);return {id:worker.id,workerId:worker.workerId,state:worker.state,phase:worker.phase,created:worker.created,updated:worker.updated};
 });
 const outputIds=new Set(),outputs=task.outputs.map(output=>{
  check(record(output)&&publicText(output.id,127)&&output.id===output.nodeId&&!outputIds.has(output.id)&&publicText(output.label,160)&&
   output.kind==='declared-output'&&hex(output.reference)&&timestamp(output.updated)&&output.updated<=task.updated&&
   (byId.has(output.nodeId)?byId.get(output.nodeId).hasOutput:task.nodesMore),'The native output declaration is incomplete.');
  outputIds.add(output.id);
  return {id:output.id,nodeId:output.nodeId,label:output.label,kind:output.kind,reference:output.reference,updated:output.updated};
 });
 const visibleActiveWorkers=workers.filter(worker=>worker.phase==='active').length;
 check(task.workersMore?visibleActiveWorkers<=counts.activeWorkers&&counts.activeWorkers-visibleActiveWorkers<=counts.workers-workers.length:visibleActiveWorkers===counts.activeWorkers,'The native worker counts disagree.');
 // Visible work may establish attention even when another detail array is
 // bounded. Exact totals keep unlisted nodes from disappearing from the phase.
 const anyAttention=counts.attention>0||workers.some(worker=>worker.phase==='attention');
 if(anyAttention)check(task.phase==='attention','The native task phase hides work that needs attention.');
 if(!task.workersMore){
  const combined={...counts};for(const worker of workers)combined[worker.phase]++;
  const phase=combined.attention?'attention':combined.active?'active':combined.pending?'pending':combined.cancelled?'cancelled':'completed';
  check(task.phase===phase,'The native task phase disagrees with its work.');
 }
 for(const phase of queueTaskPhases)check(nodes.filter(node=>node.phase===phase).length<=counts[phase],'The native task step counts disagree.');
 let usage;
 if(Object.hasOwn(task,"usage")){
  const u=task.usage;
  check(record(u)&&[5,7].includes(Object.keys(u).length)&&integer(u.runs)&&integer(u.reportedRuns)&&u.reportedRuns<=u.runs&&((Object.keys(u).length===5&&u.costStatus==="unknown")||(integer(u.pricedRuns)&&u.pricedRuns<=u.reportedRuns&&(u.pricedRuns?(u.costStatus==="estimate"&&integer(u.estimatedMicroUsd)):(u.costStatus==="unknown"&&u.estimatedMicroUsd===null))))&&
   (u.reportedRuns?(integer(u.inputTokens)&&integer(u.outputTokens)):(u.inputTokens===null&&u.outputTokens===null)),
   "The native work usage is incomplete or incorrectly priced.");
  usage={runs:u.runs,reportedRuns:u.reportedRuns,inputTokens:u.inputTokens,outputTokens:u.outputTokens,costStatus:u.costStatus,...(Object.keys(u).length===7?{pricedRuns:u.pricedRuns,estimatedMicroUsd:u.estimatedMicroUsd}:{})};
 }
 return {id:task.id,seq:task.seq,title:task.title,state:task.state,phase:task.phase,created:task.created,updated:task.updated,version:task.version,
  continuation:continuation(task.continuation,context),nodes,workers,outputs,counts,nodesMore:task.nodesMore,workersMore:task.workersMore,outputsMore:task.outputsMore,...(usage===undefined?{}:{usage})};
}
// Presentation-only work themes. Native owners retain task identity, scope and continuations.
const workThemes=[
 ['Connections',['connection studio','oauth','nango','activepieces','airbyte','auth.js','provider','github install','mcp registry','connector','credential','integration']],
 ['Sources',['source','external estate','dataset','library','catalog','archive','publication','discovery','list recovery']],
 ['Identity',['account','identity','people','profile','inbox','household','authorization']],
 ['Worlds',['world','creator','gallery','film','music','podcast','radio','journalism','learning']],
 ['Commerce',['commerce','finance','market','payment','sales','revenue','price','merchant','procurement','opportunit']],
 ['Engineering',['refactor','architecture','runtime','release','build','test','worker','operator','agent','workgraph','native','surface','icon','performance','storage']]];
export const workThemeIcons=Object.freeze({Connections:'link',Sources:'book',Identity:'group',Worlds:'world',Commerce:'coins',Engineering:'tool',Other:'board'});
export function workTheme(task){
 const phrase=(String(task?.title||'')+' '+(Array.isArray(task?.nodes)?task.nodes.map(node=>String(node.family||'')).join(' '):'')).toLowerCase();
 if(task?.kind==='research')return 'Sources';
 for(const [family,terms] of workThemes)if(terms.some(term=>phrase.includes(term)))return family;
 return 'Other';
}
// Repeated numbered ranges are one presentation workstream, not newly admitted parent tasks.
export function workStream(task){
 const title=String(task?.title||'');
 const match=title.match(/^(.*?)\s+\d[\d,]*\s*[–-]\s*\d[\d,]*(?:\s*\/\s*\d[\d,]*)?$/);
 return match&&match[1].trim()||null;
}


// Tasks are a separate native owner from retained research. A caller can keep
// one current section visible when the other owner is unavailable. A 401/403
// still invalidates every section for the requested destination.
export async function readTasks(context,{taskId,cursor=0,outputsOnly=false,...options}={}){
 check(contextValid(context),'Choose a current activity destination.');
 check(integer(cursor),'Use the native task cursor.');
 check(typeof outputsOnly==='boolean'&&(!outputsOnly||taskId===undefined),'Choose a bounded output view.');
 check(taskId===undefined||taskIdValid(taskId)&&cursor===0,'Choose one exact native task.');
 const query=new URLSearchParams({context,operation:outputsOnly?'outputs':taskId===undefined?'list':'get'});
 if(taskId!==undefined)query.set('mission',taskId);else if(cursor)query.set('cursor',String(cursor));
 const value=webValue(await packet('/apps/_activity?'+query,options));
 check(Array.isArray(value.tasks)&&value.tasks.length<=20&&typeof value.more==='boolean'&&integer(value.cursor),'The native task page is incomplete.');
 check(value.more?taskId===undefined&&value.tasks.length===20&&value.cursor>0:value.cursor===0,'The native task cursor is incomplete.');
 if(taskId!==undefined)check(value.tasks.length===1&&value.tasks[0]?.id===taskId,'The native owner returned a different task.');
 let previous=cursor||Infinity;const ids=new Set();
 const tasks=value.tasks.map(task=>{
  check(record(task)&&!ids.has(task.id)&&integer(task.seq,1)&&task.seq<previous,'The native task page repeated or reordered a task.');
  ids.add(task.id);previous=task.seq;return projectTask(task,context);
 });
 if(value.more)check(value.cursor===previous,'The native task cursor does not match its page.');
 return {context,binding:activityBinding(value.binding,context),tasks,more:value.more,cursor:value.cursor};
}
// Agent runs over the same account work index: a missing counter stays null, never zero.
const optionalCount=value=>value===null||integer(value),agentPhases=new Set(['pending','active','attention','completed','cancelled']);
export async function readAgents(context,options={}){check(contextValid(context),'Choose a current activity destination.');const value=webValue(await packet('/apps/_activity?'+new URLSearchParams({context,operation:'agents'}),options)),c=value.counts;check(Array.isArray(value.agents)&&value.agents.length<=20&&typeof value.more==='boolean'&&record(c)&&integer(c.active)&&integer(c.attention)&&integer(c.completed),'The agent run page is incomplete.');
 const agents=value.agents.map(row=>{const u=row?.usage;check(record(row)&&taskIdValid(row.id)&&taskIdValid(row.taskId)&&text(row.title,160)&&text(row.agent,127)&&row.agent&&(row.role===null||taskIdValid(row.role.replace(/ /g,'-')))&&(row.session===null||taskIdValid(row.session))&&agentPhases.has(row.phase)&&text(row.state,31)&&timestamp(row.started)&&timestamp(row.updated)&&row.updated>=row.started&&optionalCount(row.wallMs)&&typeof row.limit==='boolean'&&typeof row.unconfirmed==='boolean'&&(u===null||record(u)&&['inputTokens','outputTokens','cacheReadTokens','cacheWriteTokens','estimatedMicroUsd'].every(key=>optionalCount(u[key]))),'An agent run is incomplete.');return {id:row.id,taskId:row.taskId,title:row.title,agent:row.agent,role:row.role,session:row.session,state:row.state,phase:row.phase,started:row.started,updated:row.updated,wallMs:row.wallMs,limit:row.limit,unconfirmed:row.unconfirmed,usage:u?{...u}:null};});
 return {context,binding:activityBinding(value.binding,context),agents,counts:{active:c.active,attention:c.attention,completed:c.completed},more:value.more};}

// Account-native Meter coverage. This is usage evidence, never a bill, a balance or a token rate.
export async function readCompute(context,options={}) {
 check(context==='self','Compute usage belongs to the signed-in personal account.');
 const value=webValue(await packet('/apps/_activity?'+new URLSearchParams({context,operation:'compute'}),options));
 check(value.source==='account-activity'&&['measured','no_evidence'].includes(value.status)&&Array.isArray(value.providers)&&value.providers.length<=2, 'The native compute record is incomplete.');
 for(const name of ['records','providerSessions','additiveTokens','inheritedSnapshotTokens','cachedInputTokens'])check(integer(value[name]),'The compute counters are incomplete.');
 const seen=new Set();let records=0,sessions=0,tokens=0,snapshots=0,cached=0;
 const providers=value.providers.map(p=>{
  check(record(p)&&['claude','codex'].includes(p.provider)&&!seen.has(p.provider),'The provider identities are incomplete.');seen.add(p.provider);
  for(const field of ['records','sessions','additiveTokens','inheritedSnapshotTokens','cachedInputTokens','uncachedInputTokens','cacheWriteTokens','outputTokens'])check(integer(p[field]),'The provider counters are incomplete.');
  check(p.cachedInputTokens+p.uncachedInputTokens+p.cacheWriteTokens+p.outputTokens===p.additiveTokens,'The provider token dimensions disagree.');
  records+=p.records;sessions+=p.sessions;tokens+=p.additiveTokens;snapshots+=p.inheritedSnapshotTokens;cached+=p.cachedInputTokens;
  return Object.freeze({...p});
 });
 check(records===value.records&&sessions===value.providerSessions&&tokens===value.additiveTokens&&snapshots===value.inheritedSnapshotTokens&&cached===value.cachedInputTokens&&Boolean(records)===(value.status==='measured'), 'The provider totals do not reconcile.');
 return {context,binding:activityBinding(value.binding,context),source:'BonfyreMeter',status:value.status,records,providerSessions:sessions,additiveTokens:tokens,inheritedSnapshotTokens:snapshots,cachedInputTokens:cached,providers,costStatus:'not_valued_no_verified_charge'};
}
