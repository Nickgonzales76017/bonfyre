import {mkdirSync} from 'node:fs';
import {encode} from '../browser/workspace-msgpack.mjs';
import {FabricActivityError,fabricActivityBinding} from './fabric_store.mjs';
import {createWebPushCarrier,loadPushIdentity,pushOrigin} from './fabric_push_carrier.mjs';

const operations=new Set(['push-status','push-subscribe','push-unsubscribe','push-ack','push-resolve']);
export function createFabricPushService({store,authorize,origin=process.env.BONFYRE_PUSH_ORIGIN,home=store?.home,
  identity:providedIdentity,request=fetch,now=()=>Date.now()/1000,intervalMs=15000,stopped=()=>false}={}) {
  if(!store?.push||typeof authorize!=='function'||typeof stopped!=='function'||!Number.isSafeInteger(intervalMs)||intervalMs<1000)
    throw new TypeError('Use the native recipient store and current account authorizer.');
  let identity=null,carrier=null,initialized=false,reason='',timer=null,active=null,closed=false;
  const controller=new AbortController();
  function initialize() {
    if(initialized)return;
    if(stopped()){reason='Background delivery is paused by the operator.';return;}
    initialized=true;reason='';
    if(!origin){reason='Browser delivery needs the public HTTPS application origin.';return;}
    try {
      pushOrigin(origin);
      if(!providedIdentity)mkdirSync(home,{recursive:true,mode:0o700});
      identity=providedIdentity||loadPushIdentity(home,origin);
      if(identity.origin!==origin)throw new Error('origin mismatch');
      carrier=createWebPushCarrier({identity,request,now});
    } catch {identity=null;reason='The native browser delivery identity is unavailable.';}
  }
  async function permitted(binding) {
    let allowed;try{allowed=await authorize({...binding,permission:'read'});}catch{throw new FabricActivityError('unavailable','The current account authority is unavailable.');}
    if(allowed!==true)throw new FabricActivityError('access_required','Current account access to this destination is required.');
  }
  async function dispatch(authority,operation,payload) {
    if(closed)throw new FabricActivityError('unavailable','The notification delivery owner is stopping.');
    if(!operations.has(operation))throw new FabricActivityError('invalid','Choose a native push action.');
    const binding=fabricActivityBinding(authority);await permitted(binding);initialize();
    if(operation==='push-status'){
      if(payload!==undefined)throw new FabricActivityError('invalid','A push status read has no action body.');
      return {source:'fabric',binding,configured:Boolean(identity),paused:Boolean(stopped()),publicKey:identity?.publicKey||null,keyId:identity?.keyId||null,reason,...store.push.status(binding)};
    }
    if(operation==='push-subscribe'){
      if(stopped())throw new FabricActivityError('paused','Background delivery is paused by the operator.');
      if(!identity)throw new FabricActivityError('unavailable',reason);
      return store.push.subscribe(binding,payload,identity.keyId);
    }
    if(operation==='push-unsubscribe')return store.push.unsubscribe(binding,payload);
    if(operation==='push-ack')return store.push.acknowledge(binding,payload);
    return store.push.resolve(binding,payload);
  }
  async function runPump() {
    const result={planned:0,attempted:0,accepted:0,uncertain:0};
    if(closed||stopped())return result;
    initialize();if(!carrier)return result;
    result.planned=store.push.plan(identity.keyId);
    for(let i=0;i<8&&!closed&&!stopped();i++) {
      const claim=store.push.claim();if(!claim)break;
      const binding=fabricActivityBinding({actor:claim.actor,subject:claim.subject,scope:claim.scope});
      let allowed;
      try{allowed=await authorize({...binding,permission:'read'});}catch{
        store.push.finish(claim,{state:'retry',reason:'authority_unavailable',retryAfter:60});continue;
      }
      if(allowed!==true){store.push.finish(claim,{state:'denied',reason:'access_removed'});continue;}
      if(closed||stopped()){store.push.finish(claim,{state:'retry',reason:'owner_stopping',retryAfter:30});break;}
      const send=store.push.beginSend(claim,identity.keyId);if(!send)continue;
      const ttl=Math.min(86400,Math.floor(send.expires_at-now()));
      if(ttl<=0){store.push.finish(send,{state:'failed',reason:'lease_expired'});continue;}
      const context=binding.scope==='self'?'self':binding.subject;
      const envelope=encode([1,{deliveryId:send.id,binding,context,expiresAt:Math.floor(send.expires_at*1000)}],{maxBytes:1024});
      const receipt=await carrier({endpoint:send.endpoint,expirationTime:Math.round(send.subscription_expires*1000),keys:{p256dh:send.p256dh,auth:send.auth}},envelope,{ttl,topic:send.id,signal:controller.signal});
      store.push.finish(send,receipt);result.attempted++;
      if(receipt.state==='accepted')result.accepted++;
      if(receipt.state==='uncertain')result.uncertain++;
    }
    return result;
  }
  function pump() {
    if(active)return active;
    active=runPump().finally(()=>{active=null;});return active;
  }
  function start() {
    if(closed||timer||!origin)return;
    // Failure remains in the native durable lifecycle. The interval never
    // fabricates a completed delivery or replays a network-uncertain attempt.
    timer=setInterval(()=>{pump().catch(()=>{});},intervalMs);timer.unref?.();
    pump().catch(()=>{});
  }
  async function close() {
    closed=true;clearInterval(timer);timer=null;controller.abort();
    if(active)try{await active;}catch{}
  }
  return Object.freeze({dispatch,pump,start,close});
}
