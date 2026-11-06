import {packet,activityBinding,sameActivityBinding} from './aurekai-activity-source.mjs';

const hex=(value,n)=>typeof value==='string'&&new RegExp(`^[a-f0-9]{${n}}$`).test(value);
const time=value=>Number.isSafeInteger(value)&&value>=0&&value<=8640000000000000;
const record=value=>value&&typeof value==='object'&&!Array.isArray(value);
const check=(ok,message='The native notification delivery reply is incomplete.')=>{if(!ok)throw Object.assign(new Error(message),{code:'invalid_reply'});};
const contextOf=binding=>binding.scope==='self'?'self':binding.subject;
function interrupted(signal){if(signal?.aborted)throw signal.reason||new DOMException('Notification action cancelled.','AbortError');}
function value(packetValue){
  check(Array.isArray(packetValue)&&packetValue.length===3&&packetValue[0]===1&&typeof packetValue[1]==='boolean'&&record(packetValue[2]));
  if(!packetValue[1]){const error=packetValue[2];check(typeof error.code==='string'&&typeof error.message==='string');throw Object.assign(new Error(error.message),{code:error.code});}
  return packetValue[2];
}
async function action(context,operation,body,options={}) {
  activityBinding(body.expectedBinding,context);
  const result=value(await packet('/apps/_fabric?'+new URLSearchParams({context,operation}),{...options,body,allowErrorPacket:true}));
  check(sameActivityBinding(activityBinding(result.binding,context),body.expectedBinding),'The notification action belongs to a different account.');
  return result;
}
function subscription(row){
  check(record(row)&&hex(row.id,24)&&hex(row.endpointHash,64)&&['active','revoked','expired'].includes(row.state)&&time(row.expiresAt));
  return {id:row.id,endpointHash:row.endpointHash,state:row.state,expiresAt:row.expiresAt};
}
export async function readPushStatus(context,options={}) {
  const result=value(await packet('/apps/_fabric?'+new URLSearchParams({context,operation:'push-status'}),options));
  const binding=activityBinding(result.binding,context);
  check(result.source==='fabric'&&typeof result.configured==='boolean'&&typeof result.paused==='boolean'&&typeof result.reason==='string'&&result.reason.length<=2000&&
    (result.configured?typeof result.publicKey==='string'&&/^[A-Za-z0-9_-]{87}$/.test(result.publicKey)&&hex(result.keyId,64):result.publicKey===null&&result.keyId===null)&&
    Array.isArray(result.subscriptions)&&result.subscriptions.length<=128&&record(result.counts));
  const subscriptions=result.subscriptions.map(subscription);check(new Set(subscriptions.map(row=>row.id)).size===subscriptions.length);
  const counts={};for(const key of ['queued','accepted','shown','opened','uncertain']){check(Number.isSafeInteger(result.counts[key])&&result.counts[key]>=0);counts[key]=result.counts[key];}
  return {source:'fabric',context,binding,configured:result.configured,paused:result.paused,publicKey:result.publicKey,keyId:result.keyId,reason:result.reason,subscriptions,counts};
}
function unbase64(value){return Uint8Array.from(atob(value.replaceAll('-','+').replaceAll('_','/')+'='.repeat((4-value.length%4)%4)),c=>c.charCodeAt(0));}
function base64(bytes){return btoa(String.fromCharCode(...new Uint8Array(bytes))).replaceAll('+','-').replaceAll('/','_').replaceAll('=','');}
async function endpointHash(endpoint,cryptoObject){return [...new Uint8Array(await cryptoObject.subtle.digest('SHA-256',new TextEncoder().encode(endpoint)))].map(x=>x.toString(16).padStart(2,'0')).join('');}
function browser(options){
  const env=options.environment||globalThis;
  if(!env.isSecureContext||!env.navigator?.serviceWorker||!env.PushManager||!env.Notification)
    throw Object.assign(new Error('This browser needs HTTPS and Web Push support. On iPhone or iPad, open the installed Home Screen app.'),{code:'unsupported'});
  return env;
}
function gesture(event,env){
  if(!event?.isTrusted||env.navigator.userActivation&&env.navigator.userActivation.isActive!==true)
    throw Object.assign(new Error('Choose Enable notifications in this browser.'),{code:'gesture_required'});
}
let mutationTail=Promise.resolve();
async function serializePushMutation(env,signal,operation){
  const run=()=>{interrupted(signal);return env.navigator.locks?
    env.navigator.locks.request('aurekai-push-subscription',{mode:'exclusive',...(signal?{signal}:{})},()=>{interrupted(signal);return operation();}):operation();};
  const current=mutationTail.then(run,run);mutationTail=current.catch(()=>{});return current;
}
export async function currentPushDevice(status,options={}) {
  const env=browser(options),registration=await env.navigator.serviceWorker.getRegistration('/apps/');
  if(!registration||registration.scope!==new URL('/apps/',env.location.origin).href)return null;
  const sub=await registration.pushManager.getSubscription();if(!sub)return null;
  const hash=await endpointHash(sub.endpoint,env.crypto);
  return status.subscriptions.find(row=>row.endpointHash===hash&&row.state==='active'&&row.expiresAt>Date.now())||null;
}
export async function enablePush(context,status,options={}) {
  const env=browser(options);gesture(options.event,env);interrupted(options.signal);
  const expectedBinding=activityBinding(status.binding,context);
  check(status.configured&&!status.paused&&typeof status.publicKey==='string','Browser notification delivery is not available for this destination.');
  // Request permission in the click turn, before any asynchronous network read.
  const permission=env.Notification.permission==='granted'?'granted':await env.Notification.requestPermission();
  interrupted(options.signal);
  if(permission!=='granted')throw Object.assign(new Error('Browser notification permission was not granted.'),{code:'permission_required'});
  return serializePushMutation(env,options.signal,async()=>{
  const worker=new URL('/apps/aurekai-push-sw.mjs',env.location.origin).href,scope=new URL('/apps/',env.location.origin).href;
  const old=await env.navigator.serviceWorker.getRegistration('/apps/');interrupted(options.signal);
  if(old?.scope===scope&&[old.active,old.waiting,old.installing].filter(Boolean).some(item=>item.scriptURL!==worker))
    throw Object.assign(new Error('Another application owns this browser worker scope.'),{code:'worker_conflict'});
  const registration=await env.navigator.serviceWorker.register(worker,{type:'module',scope:'/apps/',updateViaCache:'none'});
  interrupted(options.signal);
  // Register() can resolve before activation; subscription needs an active worker.
  if(!registration.active)await new Promise((resolve,reject)=>{
    const worker=registration.installing||registration.waiting;if(!worker){reject(new Error('The notification worker is unavailable.'));return;}
    const timeout=setTimeout(()=>finish(new Error('The notification worker did not become active.')),15000);
    const change=()=>{if(worker.state==='activated')finish();else if(worker.state==='redundant')finish(new Error('The notification worker could not activate.'));};
    const abort=()=>finish(options.signal.reason||new DOMException('Notification action cancelled.','AbortError'));
    function finish(error){clearTimeout(timeout);worker.removeEventListener('statechange',change);options.signal?.removeEventListener('abort',abort);error?reject(error):resolve();}
    worker.addEventListener('statechange',change);options.signal?.addEventListener('abort',abort,{once:true});change();
  });
  interrupted(options.signal);let sub=await registration.pushManager.getSubscription();interrupted(options.signal);
  const key=unbase64(status.publicKey);
  if(sub&&sub.options?.applicationServerKey&&base64(sub.options.applicationServerKey)!==status.publicKey){interrupted(options.signal);await sub.unsubscribe();interrupted(options.signal);sub=null;}
  async function newSubscription(){interrupted(options.signal);return registration.pushManager.subscribe({userVisibleOnly:true,applicationServerKey:key});}
  try {
    if(!sub)sub=await newSubscription();interrupted(options.signal);
    const body=()=>({expectedBinding,endpoint:sub.endpoint,expirationTime:sub.expirationTime??null,keys:{p256dh:base64(sub.getKey('p256dh')),auth:base64(sub.getKey('auth'))}});
    let result;
    try{result=await action(context,'push-subscribe',body(),options);}
    catch(error){
      if(error.code!=='device_account_conflict')throw error;
      // Explicit enable for another account replaces the browser endpoint. A
      // server row is never silently transferred between recipients.
      interrupted(options.signal);await sub.unsubscribe();interrupted(options.signal);sub=await newSubscription();interrupted(options.signal);
      result=await action(context,'push-subscribe',body(),options);
    }
    interrupted(options.signal);return {binding:result.binding,subscription:subscription(result.subscription)};
  } catch(error){
    // An aborted HTTP request might already have admitted this endpoint, and
    // another destination may now share it. Never destructively clean it up.
    throw error;
  }
  });
}
export async function disablePush(context,status,subscriptionId,options={}) {
  check(hex(subscriptionId,24));
  const result=await action(context,'push-unsubscribe',{expectedBinding:status.binding,subscriptionId},options);
  check(result.subscription?.id===subscriptionId&&result.subscription.state==='revoked');
  // The shared PushManager subscription may also serve another opted-in
  // destination for this account. Revoke only this native recipient binding.
  return {binding:result.binding,subscription:subscription(result.subscription)};
}
export async function acknowledgePushDelivery(context,binding,deliveryId,stage,options={}) {
  check(hex(deliveryId,24)&&['shown','opened'].includes(stage));
  const result=await action(context,'push-ack',{expectedBinding:binding,deliveryId,stage},options);
  check(result.deliveryId===deliveryId&&result.stage===stage);return result;
}
export async function resolvePushDelivery(context,binding,deliveryId,options={}) {
  check(hex(deliveryId,24));
  const result=await action(context,'push-resolve',{expectedBinding:binding,deliveryId},options);
  check(result.deliveryId===deliveryId&&hex(result.taskId,24)&&['unread','read','dismissed'].includes(result.notificationState)&&
    result.href==='/apps?'+new URLSearchParams({context})+'#fabric-task-'+result.taskId,'The notification did not resolve to its original work.');
  return result;
}
export async function resumePushFromLocation(location=globalThis.location,options={}) {
  const url=new URL(location.href),deliveryId=url.searchParams.get('aurekai_push');if(!deliveryId)return null;
  const context=url.searchParams.get('context')||'self',actor=url.searchParams.get('aurekai_actor');
  const binding=activityBinding({actor,subject:context==='self'?actor:context,scope:context==='self'?'self':context.split(':')[0]},context);
  try {
    const result=await resolvePushDelivery(context,binding,deliveryId,options);
    await acknowledgePushDelivery(context,binding,deliveryId,'opened',options).catch(()=>{});
    return {state:'ready',...result};
  } catch(error) {
    return {state:['access_required','conflict'].includes(error.code)?'access_required':error.code==='expired'?'expired':'unavailable',
      message:['access_required','conflict'].includes(error.code)?'Sign in to the account for this notification to reopen its work.':error.message,
      deliveryId,context,binding};
  }
}
export function pushEnvelope(value,now=Date.now(),{allowExpired=false}={}) {
  check(Array.isArray(value)&&value.length===2&&value[0]===1&&record(value[1]));const item=value[1];
  check(Object.keys(item).length===4&&hex(item.deliveryId,24)&&time(item.expiresAt)&&(allowExpired||item.expiresAt>now)&&item.expiresAt<=now+86400000);
  const binding=activityBinding(item.binding,item.context);check(item.context===contextOf(binding));
  return {deliveryId:item.deliveryId,binding,context:item.context,expiresAt:item.expiresAt};
}
