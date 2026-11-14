import {expect,test} from 'bun:test';
import {deferred,packet} from './browser-fixture.mjs';
import {createECDH} from 'node:crypto';
import {encode,decode} from '../workspace-msgpack.mjs';
import {readPushStatus,enablePush,disablePush,pushEnvelope,resumePushFromLocation} from '../aurekai-push.mjs';
import {createPushWorker} from '../aurekai-push-sw.mjs';

const binding={actor:'user:1',subject:'institution:studio',scope:'institution'},context=binding.subject;
const key=createECDH('prime256v1');key.generateKeys();const publicKey=key.getPublicKey().toString('base64url');
const status={source:'fabric',binding,context,configured:true,paused:false,publicKey,keyId:'a'.repeat(64),reason:'',subscriptions:[],counts:{queued:0,accepted:0,shown:0,opened:0,uncertain:0}};
function environment(){
  const calls={unsubscribe:0,subscribe:0,permission:0,register:0};
  let sub={endpoint:'https://fcm.googleapis.com/fcm/send/fixture',expirationTime:null,options:{applicationServerKey:key.getPublicKey()},
    getKey:kind=>kind==='p256dh'?key.getPublicKey():new Uint8Array(16),unsubscribe:async()=>{calls.unsubscribe++;return true;}};
  const registration={scope:'https://aurekai.example/apps/',active:{scriptURL:'https://aurekai.example/apps/aurekai-push-sw.mjs'},
    pushManager:{getSubscription:async()=>sub,subscribe:async()=>{calls.subscribe++;return sub;}}};
  const env={isSecureContext:true,location:{origin:'https://aurekai.example'},crypto:globalThis.crypto,PushManager:function(){},Notification:{permission:'granted',requestPermission:async()=>{calls.permission++;return 'granted';}},
    navigator:{userActivation:{isActive:true},serviceWorker:{getRegistration:async()=>registration,register:async()=>{calls.register++;return registration;}}}};
  return {env,calls,registration,sub};
}
const receipt={id:'c'.repeat(24),endpointHash:'d'.repeat(64),state:'active',expiresAt:Date.now()+100000};

test('native push status rejects an echo for another account destination and does not treat unavailable reads as disabled',async()=>{
  expect((await readPushStatus(context,{request:async()=>packet([1,true,status])})).binding).toEqual(binding);
  await expect(readPushStatus('self',{request:async()=>packet([1,true,status])})).rejects.toThrow('different');
  await expect(readPushStatus(context,{request:async()=>new Response('unavailable',{status:503})})).rejects.toMatchObject({code:'unavailable'});
});
test('notification permission requires a trusted user action and performs no work for synthetic events',async()=>{
  const {env,calls}=environment();env.Notification.permission='default';
  await expect(enablePush(context,status,{environment:env,event:{isTrusted:false}})).rejects.toMatchObject({code:'gesture_required'});
  expect(calls).toEqual({unsubscribe:0,subscribe:0,permission:0,register:0});
});
test('account change during a delayed browser subscription read cannot revoke or create an endpoint',async()=>{
  const {env,calls,registration,sub}=environment(),gate=deferred(),entered=deferred(),controller=new AbortController();
  sub.options.applicationServerKey=new Uint8Array(65);
  registration.pushManager.getSubscription=()=>{entered.resolve();return gate.promise;};
  let native=0;const pending=enablePush(context,status,{environment:env,event:{isTrusted:true},signal:controller.signal,request:async()=>{native++;return packet([1,true,{binding,subscription:receipt}]);}});
  await entered.promise;controller.abort();gate.resolve(sub);await expect(pending).rejects.toThrow();
  expect(calls.unsubscribe).toBe(0);expect(calls.subscribe).toBe(0);expect(native).toBe(0);
});
test('aborted native admission never destructively cleans up a subscription another destination may have adopted',async()=>{
  const {env,calls,registration}=environment(),gate=deferred(),entered=deferred(),controller=new AbortController();registration.pushManager.getSubscription=async()=>null;
  const pending=enablePush(context,status,{environment:env,event:{isTrusted:true},signal:controller.signal,request:async()=>{entered.resolve();return gate.promise;}});
  await entered.promise;controller.abort();gate.resolve(packet([1,true,{binding,subscription:receipt}]));await expect(pending).rejects.toThrow();
  expect(calls.subscribe).toBe(1);expect(calls.unsubscribe).toBe(0);
});
test('explicit account replacement renews the browser endpoint and preserves exact expected binding in both native requests',async()=>{
  const {env,calls}=environment();let native=0;
  const result=await enablePush(context,status,{environment:env,event:{isTrusted:true},request:async(url,init)=>{
    const body=decode(init.body)[1];expect(body.expectedBinding).toEqual(binding);native++;
    return native===1?packet([1,false,{code:'device_account_conflict',message:'Renew the browser subscription.'}],409):packet([1,true,{binding,subscription:receipt}]);
  }});
  expect(result.subscription.id).toBe(receipt.id);expect(native).toBe(2);expect(calls.unsubscribe).toBe(1);expect(calls.subscribe).toBe(1);
});
test('scope-only disable calls the recipient owner and never globally unsubscribes the browser endpoint',async()=>{
  const {env,calls}=environment();const result=await disablePush(context,status,receipt.id,{environment:env,request:async(url,init)=>{
    expect(decode(init.body)[1]).toEqual({expectedBinding:binding,subscriptionId:receipt.id});return packet([1,true,{binding,subscription:{...receipt,state:'revoked'}}]);
  }});expect(result.subscription.state).toBe('revoked');expect(calls.unsubscribe).toBe(0);
});
test('worker deduplicates OS display separately from native shown and opened receipts; no task title enters lock-screen copy',async()=>{
  const seen=new Map(),displays=[],acks=[];const envelope={deliveryId:'e'.repeat(24),binding,context,expiresAt:200000};
  const worker=createPushWorker({origin:'https://aurekai.example',now:()=>100000,
    cache:{reserve:async(id,expiry)=>{if(seen.has(id))return false;seen.set(id,{expiresAt:expiry});return true;},shown:async()=>{}},
    registration:{showNotification:async(title,options)=>displays.push({title,options})},acknowledge:async(...args)=>acks.push(args)});
  expect(await worker.receive(encode([1,envelope]))).toEqual({state:'shown'});expect(await worker.receive(encode([1,envelope]))).toEqual({state:'duplicate'});
  expect(displays).toHaveLength(1);expect(displays[0].options.body).toBe('New workspace activity is ready to review.');expect(acks.map(x=>x[3])).toEqual(['shown']);
  await expect(worker.receive(encode([1,{...envelope,deliveryId:'f'.repeat(24),expiresAt:1}]))).rejects.toThrow();expect(displays).toHaveLength(1);
});
test('worker click with changed account opens a sign-in continuation with the original actor and work delivery; expired clicks still open current Inbox',async()=>{
  const opened=[],acks=[];const worker=createPushWorker({origin:'https://aurekai.example',now:()=>100000,
    clients:{matchAll:async()=>[],openWindow:async url=>{opened.push(url);return {}; }},resolve:async()=>{throw Object.assign(new Error('wrong account'),{code:'conflict'});},acknowledge:async(...args)=>acks.push(args)});
  const data={version:1,deliveryId:'e'.repeat(24),binding,context,expiresAt:99999};
  const result=await worker.click(data);expect(result.state).toBe('access_required');const url=new URL(opened[0]);
  expect(url.searchParams.get('aurekai_actor')).toBe(binding.actor);expect(url.searchParams.get('aurekai_push')).toBe(data.deliveryId);expect(acks).toHaveLength(0);
});
test('returning signed-in continuation resolves only its retained work and never marks the Inbox notification read',async()=>{
  const id='e'.repeat(24),task='f'.repeat(24),operations=[];const location={href:'https://aurekai.example/apps?'+new URLSearchParams({context,aurekai_actor:binding.actor,aurekai_push:id})};
  const result=await resumePushFromLocation(location,{request:async(url,init)=>{
    const operation=new URL(url,location.href).searchParams.get('operation'),body=decode(init.body)[1];operations.push(operation);expect(body.expectedBinding).toEqual(binding);
    return packet([1,true,operation==='push-resolve'?{binding,deliveryId:id,taskId:task,notificationState:'unread',href:'/apps?'+new URLSearchParams({context})+'#fabric-task-'+task}:{binding,deliveryId:id,stage:'opened'}]);
  }});
  expect(result.state).toBe('ready');expect(result.taskId).toBe(task);expect(operations).toEqual(['push-resolve','push-ack']);
});
test('push payload refuses foreign scope and an extended unbounded delivery lifetime',()=>{
  const item={deliveryId:'e'.repeat(24),binding,context,expiresAt:200000};expect(pushEnvelope([1,item],100000)).toEqual(item);
  expect(()=>pushEnvelope([1,{...item,context:'self'}],100000)).toThrow();
  expect(()=>pushEnvelope([1,{...item,expiresAt:100000+86400001}],100000)).toThrow();
});
