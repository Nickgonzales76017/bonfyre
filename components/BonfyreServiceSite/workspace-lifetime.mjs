// One document lifetime. Returning from pagehide requires a fresh current-account read.
export function createWorkspaceLifetime(events=globalThis){
 const controller=new AbortController(),callbacks=new Set();let closed=false;
 function close(){if(closed)return;closed=true;controller.abort();for(const fn of callbacks){try{fn();}catch{}}callbacks.clear();}
 events.addEventListener('bonfyre:account-changed',close,{signal:controller.signal});
 events.addEventListener('pagehide',close,{signal:controller.signal});
 return{signal:controller.signal,get closed(){return closed;},assert(){if(closed)throw Object.assign(Error('This workspace session closed. Reload to read current account access and saved work.'),{code:'workspace_closed'});},onClose(fn){if(closed)fn();else callbacks.add(fn);return()=>callbacks.delete(fn);},close};
}
export function clearWorkspaceHost(host){
 for(const input of host.querySelectorAll('input,textarea'))input.value='';
 for(const select of host.querySelectorAll('select'))select.replaceChildren();
 for(const control of host.querySelectorAll('button,input,textarea,select'))control.disabled=true;
 host.replaceChildren();delete host.dataset.ready;host.dataset.closed='true';host.removeAttribute('aria-busy');host.style.padding='clamp(20px,4vw,48px)';
 const message=document.createElement('p'),reload=document.createElement('button');
 message.textContent='This workspace session closed. Reload to read current account access and saved work.';message.setAttribute('role','status');
 message.style.marginBottom='16px';reload.style.cssText='font:inherit;padding:10px 16px;min-height:44px;border:1px solid currentColor;border-radius:6px';
 reload.type='button';reload.textContent='Reload workspace';reload.onclick=()=>location.reload();host.append(message,reload);
}
export function guardWorkspaceStore(store,lifetime){
 return Object.fromEntries(Object.keys(store).filter(name=>typeof store[name]==='function').map(name=>[name,async(...args)=>{lifetime.assert();const result=await store[name](...args);lifetime.assert();return result;}]));
}
export async function openWorkspaceStore(lifetime,{endpoint=globalThis.document?.querySelector('meta[name="bonfyre-workspace-store"]')?.content,loadNative=endpoint=>import(endpoint.replace(/_records$/,'bridge/workspace-store.mjs')),request=globalThis.fetch?.bind(globalThis),fallback}={}){
 lifetime.assert();if(!endpoint){if(!fallback)throw Error('No workspace storage is available.');return guardWorkspaceStore(fallback,lifetime);}
 if(!/^\/apps\/ws-[a-f0-9]{40}\/_records$/.test(endpoint))throw Error('Invalid workspace storage binding.');
 const native=await loadNative(endpoint);lifetime.assert();
 const store=native.openDestinationStore(endpoint,{signal:lifetime.signal,request:async(url,options)=>{lifetime.assert();const response=await request(url,{...options,signal:lifetime.signal});lifetime.assert();return response;}});
 return guardWorkspaceStore(store,lifetime);
}
