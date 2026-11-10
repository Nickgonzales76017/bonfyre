import {decode} from './workspace-msgpack.mjs';
import {pushEnvelope,resolvePushDelivery,acknowledgePushDelivery} from './aurekai-push.mjs';

// This bounded cache contains only opaque delivery IDs and expiry. Fabric owns
// inbox state. It survives worker restarts solely to avoid a second OS alert.
export function createPushReceiptCache(indexedDB) {
  async function database(){
    return new Promise((resolve,reject)=>{
      const request=indexedDB.open('aurekai-push-receipts',1);
      request.onupgradeneeded=()=>{const db=request.result;if(!db.objectStoreNames.contains('receipts'))db.createObjectStore('receipts',{keyPath:'id'});};
      request.onsuccess=()=>resolve(request.result);request.onerror=()=>reject(new Error('Notification receipt cache unavailable.'));
      request.onblocked=()=>reject(new Error('Notification receipt cache blocked.'));
    });
  }
  async function reserve(id,expiresAt,now) {
    const db=await database();
    return new Promise((resolve,reject)=>{
      const transaction=db.transaction('receipts','readwrite'),store=transaction.objectStore('receipts');let admitted=false,count=0;
      const cursor=store.openCursor();
      cursor.onsuccess=()=>{const item=cursor.result;if(item){if(item.value.expiresAt<=now)item.delete();else count++;item.continue();}else{
        const old=store.get(id);old.onsuccess=()=>{if(!old.result&&count<512){store.add({id,expiresAt,stage:'claimed'});admitted=true;}};
      }};
      transaction.oncomplete=()=>{db.close();resolve(admitted);};
      transaction.onerror=transaction.onabort=()=>{db.close();reject(new Error('Notification deduplication could not be retained.'));};
    });
  }
  async function shown(id) {
    const db=await database();return new Promise((resolve,reject)=>{
      const transaction=db.transaction('receipts','readwrite'),store=transaction.objectStore('receipts'),old=store.get(id);
      old.onsuccess=()=>{if(old.result)store.put({...old.result,stage:'shown'});};
      transaction.oncomplete=()=>{db.close();resolve();};transaction.onerror=transaction.onabort=()=>{db.close();reject(new Error('Notification display receipt unavailable.'));};
    });
  }
  return {reserve,shown};
}
export function createPushWorker({registration,clients,origin,cache,now=()=>Date.now(),resolve=resolvePushDelivery,acknowledge=acknowledgePushDelivery}={}) {
  async function receive(bytes) {
    const envelope=pushEnvelope(decode(bytes,{maxBytes:1024}),now());
    if(!await cache.reserve(envelope.deliveryId,envelope.expiresAt,now()))return {state:'duplicate'};
    await registration.showNotification('Aurekai activity',{
      body:envelope.binding.scope==='self'?'New personal activity is ready to review.':'New workspace activity is ready to review.',
      tag:'aurekai-'+envelope.deliveryId,renotify:false,requireInteraction:false,
      data:{version:1,...envelope},timestamp:now()
    });
    await cache.shown(envelope.deliveryId);
    // This reports showNotification() resolution, not that a person read it.
    try{await acknowledge(envelope.context,envelope.binding,envelope.deliveryId,'shown');}catch{}
    return {state:'shown'};
  }
  async function click(data) {
    if(!data||data.version!==1)throw new Error('Unknown notification continuation.');
    const envelope=pushEnvelope([1,{deliveryId:data.deliveryId,binding:data.binding,context:data.context,expiresAt:data.expiresAt}],now(),{allowExpired:true});
    let href='/apps?'+new URLSearchParams({context:envelope.context,aurekai_push:envelope.deliveryId,aurekai_actor:envelope.binding.actor});
    let authorized=false;
    try{const result=await resolve(envelope.context,envelope.binding,envelope.deliveryId);href=result.href;authorized=true;}catch{}
    const target=new URL(href,origin);
    if(target.origin!==origin||target.pathname!=='/apps')throw new Error('The work continuation is not a local application address.');
    const windows=await clients.matchAll({type:'window',includeUncontrolled:true});
    const same=windows.find(client=>client.url===target.href);
    const opened=same?await same.focus():await clients.openWindow(target.href);
    if(opened&&authorized)try{await acknowledge(envelope.context,envelope.binding,envelope.deliveryId,'opened');}catch{}
    return {state:authorized?'opened':'access_required',href:target.href};
  }
  return {receive,click};
}

if(typeof ServiceWorkerGlobalScope!=='undefined'&&globalThis instanceof ServiceWorkerGlobalScope){
  const worker=createPushWorker({registration:globalThis.registration,clients:globalThis.clients,origin:globalThis.location.origin,
    cache:createPushReceiptCache(globalThis.indexedDB)});
  addEventListener('install',event=>event.waitUntil(globalThis.skipWaiting()));
  addEventListener('activate',event=>event.waitUntil(globalThis.clients.claim()));
  addEventListener('push',event=>{
    if(!event.data)return;
    event.waitUntil(worker.receive(new Uint8Array(event.data.arrayBuffer())).catch(()=>{}));
  });
  addEventListener('notificationclick',event=>{
    event.notification.close();event.waitUntil(worker.click(event.notification.data).catch(()=>{}));
  });
  addEventListener('pushsubscriptionchange',event=>event.waitUntil((async()=>{
    // A new endpoint does not inherit account consent. The next explicit
    // Enable action binds the current browser subscription to its recipient.
    for(const client of await globalThis.clients.matchAll({type:'window',includeUncontrolled:true}))
      client.postMessage({type:'aurekai-push-subscription-change'});
  })()));
}
