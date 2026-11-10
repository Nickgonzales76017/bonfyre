// Account grants are resolved by the HTTP owner. This module retains no Inbox
// truth or credentials; every state change has one exact native request identity.
import {encode} from './workspace-msgpack.mjs';
import {activityBinding,sameActivityBinding,packet as activityPacket} from './aurekai-activity-source.mjs';

const record=value=>value!==null&&typeof value==='object'&&!Array.isArray(value);
const atom=(value,max=127)=>typeof value==='string'&&value.length>0&&value.length<=max&&/^[A-Za-z0-9._:-]+$/.test(value);
const hex=value=>typeof value==='string'&&/^[a-f0-9]{64}$/.test(value);
const integer=value=>Number.isSafeInteger(value)&&value>=0;
const text=(value,max)=>typeof value==='string'&&value.length<=max&&!/[\x00-\x1f\x7f]/.test(value);
const contextValid=value=>value==='self'||typeof value==='string'&&/^(?:institution|household):[a-z0-9-]{1,63}$/.test(value);
const phases=new Set(['pending','active','attention','completed','cancelled']);
const actions=new Set(['read','unread','dismiss','restore']);
const check=(okay,message='The Inbox reply is incomplete.')=>{if(!okay)throw Object.assign(Error(message),{code:'invalid_reply'});};
async function packet(url,{request=fetch,signal,body}={}){
  const value=await activityPacket(url,{request,signal,body,encodedBody:true,exactLength:true});check(Array.isArray(value)&&value.length===3&&value[0]===1&&typeof value[1]==='boolean'&&record(value[2]));
  if(!value[1]){
   const error=value[2];check(text(error.code,64)&&/^[a-z][a-z0-9_]*$/.test(error.code)&&text(error.message,2000)&&error.message.trim());
   const status={invalid_request:400,not_found:404,version_conflict:409,request_conflict:409}[error.code]??503;
   throw Object.assign(Error(error.message),{code:error.code,status});
  }
  return value[2];
}

function continuation(href,context){
 if(href==='')return true;
 if(!text(href,512)||href.includes('//'))return false;
 const [path,query,...tail]=href.split('?');
 if(tail.length||!/^\/(?:apps(?:\/[a-z0-9_-]+)*|sources\/web)$/.test(path))return false;
 if(query===undefined)return context==='self';
 return query==='context='+context;
}
function notice(row,context){
 check(record(row)&&atom(row.id)&&row.taskId===row.id&&hex(row.version)&&integer(row.revision)&&
  text(row.title,160)&&row.title.trim()&&text(row.body,240)&&row.body.trim()&&integer(row.updated)&&row.updated<=8640000000000000&&
  continuation(row.href,context)&&typeof row.read==='boolean'&&typeof row.dismissed==='boolean'&&phases.has(row.phase),'The current Inbox notice is incomplete.');
 return {id:row.id,taskId:row.taskId,version:row.version,revision:row.revision,title:row.title,body:row.body,
  updated:row.updated,href:row.href,read:row.read,dismissed:row.dismissed,phase:row.phase};
}
function coordinates(context,operation,extra={}){
 check(contextValid(context),'Choose one current account or organization.');
 return '/apps/_activity?'+new URLSearchParams({context,operation,...extra});
}
export async function readInbox(context,{request,signal,id,cursor}={}){
 check(id===undefined||atom(id),'Choose an exact task.');
 check(cursor===undefined||typeof cursor==='string'&&/^[1-9]\d{0,15}$/.test(cursor)&&Number.isSafeInteger(Number(cursor)),'Use a native Inbox page cursor.');
 check(id===undefined||cursor===undefined,'Choose one task or one Inbox page.');
 const value=await packet(coordinates(context,'inbox',{...(id?{mission:id}:{}),...(cursor?{cursor}:{})}),{request,signal});
 check(Array.isArray(value.notices)&&value.notices.length<=50&&typeof value.more==='boolean'&&typeof value.cursor==='string');
 check(value.more?value.notices.length===50&&/^[1-9]\d{0,15}$/.test(value.cursor)&&Number.isSafeInteger(Number(value.cursor)):value.cursor==='');
 if(id)check(!value.more&&value.notices.length===1&&value.notices[0]?.id===id);
 if(cursor&&value.more)check(Number(value.cursor)<Number(cursor),'The Inbox cursor did not advance.');
 const ids=new Set();const notices=value.notices.map(row=>{const next=notice(row,context);check(!ids.has(next.id),'The Inbox contains a duplicate notice.');ids.add(next.id);return next;});
 return {context,binding:activityBinding(value.binding,context),notices,more:value.more,cursor:value.cursor};
}
function ack(value,context,requestId,change){
 const binding=activityBinding(value.binding,context);if(change)check(sameActivityBinding(binding,change.expectedBinding),'The Inbox acknowledgement belongs to another account.');
 check(value.requestId===requestId&&typeof value.replayed==='boolean'&&hex(value.appliedVersion)&&integer(value.appliedRevision)&&value.appliedRevision>=1);
 const current=notice(value.notice,context);
 check(current.revision>=value.appliedRevision,'The Inbox acknowledgement predates its committed change.');
 if(change)check(current.id===change.id&&value.appliedVersion===change.version&&value.appliedRevision===change.revision+1,'The Inbox acknowledgement belongs to another change.');
 return {context,binding,requestId,replayed:value.replayed,appliedVersion:value.appliedVersion,appliedRevision:value.appliedRevision,notice:current};
}
export async function setInboxState(context,change,{request,signal}={}){
 check(record(change)&&atom(change.id)&&hex(change.version)&&integer(change.revision)&&change.revision<Number.MAX_SAFE_INTEGER&&
  actions.has(change.action)&&atom(change.requestId,96)&&change.requestId.length>=8,'Choose a current notice and one exact Inbox action.');
 const expectedBinding=activityBinding(change.expectedBinding,context);
 const body=encode([1,{expectedBinding,id:change.id,version:change.version,revision:change.revision,action:change.action,requestId:change.requestId}],{maxBytes:4096});
 const value=await packet(coordinates(context,'inbox-set'),{request,signal,body});
 return ack(value,context,change.requestId,change);
}
export async function readInboxReceipt(context,requestId,{request,signal}={}){
 check(atom(requestId,96)&&requestId.length>=8,'Use the same Inbox request identity.');
 return ack(await packet(coordinates(context,'inbox-receipt',{request:requestId}),{request,signal}),context,requestId);
}
// The full account Inbox owns approvals, provider steps, invitations and credit
// holds. Read its exact authorized projection rather than guessing from Queue
// recipient notifications, which can legitimately be empty.
export async function readAccountNeeds(context,{request,signal}={}){
 check(contextValid(context),'Choose a current Inbox destination.');
 const value=await packet('/inbox/_summary?'+new URLSearchParams({context}),{request,signal});
 check(value.context===context&&Array.isArray(value.items)&&value.items.length<=58,'Account Inbox actions are incomplete.');
 const binding=activityBinding(value.binding,context),ids=new Set();
 const kinds=new Set(['release','provider','provider-checklist','execution-credit','invitation']);
 const items=value.items.map(row=>{
  check(record(row)&&Object.keys(row).length===4&&atom(row.id,160)&&!ids.has(row.id)&&
   kinds.has(row.kind)&&text(row.title,256)&&row.title.trim()&&integer(row.updated)&&row.updated<=8640000000000000,
   'Account Inbox action is incomplete.');
  ids.add(row.id);return {id:row.id,kind:row.kind,title:row.title,updated:row.updated};
 });
 return {context,binding,items};
}
