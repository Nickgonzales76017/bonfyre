import {readLocal,saveLocal} from '../../integrations/browser/extension/store.mjs';
export function createWorkbookLifetime(events=globalThis){
 const controller=new AbortController(),callbacks=new Set();let closed=false;
 function close(){if(closed)return;closed=true;controller.abort();for(const fn of callbacks){try{fn();}catch{}}callbacks.clear();}
 events.addEventListener('bonfyre:account-changed',close,{signal:controller.signal});
 events.addEventListener('pagehide',close,{signal:controller.signal});
 return{signal:controller.signal,get closed(){return closed;},assert(){if(closed)throw Object.assign(Error('The workbook session closed. Reload to read current account access and saved work.'),{code:'workbook_closed'});},onClose(fn){if(closed)fn();else callbacks.add(fn);return()=>callbacks.delete(fn);},close};
}
export function clearWorkbookHost(host){
 for(const input of host.querySelectorAll('input,textarea'))input.value='';
 for(const control of host.querySelectorAll('button,input,textarea,select'))control.disabled=true;
 host.replaceChildren();delete host.dataset.ready;host.dataset.closed='true';
 const message=document.createElement('p'),reload=document.createElement('button');
 message.textContent='The workbook session closed. Reload to read current account access and saved work.';message.setAttribute('role','status');
 reload.type='button';reload.textContent='Reload workspace';reload.onclick=()=>location.reload();host.append(message,reload);
}
export function guardWorkbookStore(store,lifetime){
 return Object.fromEntries(['readLocal','saveLocal'].map(name=>[name,async(...args)=>{lifetime.assert();const result=await store[name](...args);lifetime.assert();return result;}]));
}
export async function openWorkbookStore(lifetime,{endpoint=globalThis.document?.querySelector('meta[name="bonfyre-workspace-store"]')?.content,loadNative=()=>import('/apps/workspace-store.mjs'),request=globalThis.fetch?.bind(globalThis),fallback={readLocal,saveLocal}}={}){
 lifetime.assert();if(!endpoint)return guardWorkbookStore(fallback,lifetime);
 const native=await loadNative();lifetime.assert();
 const store=native.openDestinationStore(endpoint,{request:async(url,options)=>{lifetime.assert();const response=await request(url,{...options,signal:lifetime.signal});lifetime.assert();return response;}});
 return guardWorkbookStore(store,lifetime);
}
