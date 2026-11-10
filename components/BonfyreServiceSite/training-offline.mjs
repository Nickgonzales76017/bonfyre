export async function enableOffline(){
  const status=document.getElementById('offline-state');
  if(!('serviceWorker' in navigator)){status.textContent='Offline storage unavailable';return;}
  const showWaiting=()=>{status.textContent='Update ready · close and reopen training tabs';};
  try{
    const registration=await navigator.serviceWorker.register('/training-sw.js',{scope:'/',updateViaCache:'none'});
    const ready=()=>{status.textContent=navigator.onLine?'Ready offline':'Offline · saved on this device';};
    if(registration.waiting)showWaiting();
    registration.addEventListener('updatefound',()=>{
      const installing=registration.installing;
      installing?.addEventListener('statechange',()=>{
        if(installing.state==='installed'){if(navigator.serviceWorker.controller)showWaiting();else ready();}
        if(installing.state==='redundant'&&!registration.active)status.textContent='Offline setup incomplete · reconnect and reload';
      });
    });
    navigator.serviceWorker.ready.then(()=>{if(!registration.waiting)ready();});
    const connection=()=>{if(registration.waiting)showWaiting();else if(registration.active)ready();};
    window.addEventListener('online',connection);window.addEventListener('offline',connection);
  }catch{
    const registration=await navigator.serviceWorker.getRegistration().catch(()=>null);
    status.textContent=registration?.active?'Offline · saved on this device':'Offline setup unavailable · reconnect and reload';
  }
}

// New versions wait for every old tab to close. No forced reload can discard an edit.
export function workerSource(version,paths){
  return `const VERSION=${JSON.stringify(version)},CACHE='bonfyre-training-'+VERSION,PATHS=${JSON.stringify(paths)};
self.addEventListener('install',event=>event.waitUntil((async()=>{
  try{
    const responses=await Promise.all(PATHS.map(async path=>{
      const response=await fetch(new Request(path,{cache:'reload'}));
      if(!response.ok||response.headers.get('X-Training-Version')!==VERSION)throw Error('App version changed during installation');
      // Consume each body immediately so parallel downloads release their connection slots.
      return [path,new Response(await response.arrayBuffer(),{status:response.status,headers:response.headers})];
    }));
    const cache=await caches.open(CACHE);
    await Promise.all(responses.map(([path,response])=>cache.put(path,response)));
  }catch(error){await caches.delete(CACHE);throw error;}
})()));
self.addEventListener('activate',event=>event.waitUntil((async()=>{
  for(const key of await caches.keys())if(key.startsWith('bonfyre-training-')&&key!==CACHE)await caches.delete(key);
})()));
self.addEventListener('fetch',event=>{
  const url=new URL(event.request.url);
  if(event.request.method!=='GET'||url.origin!==self.location.origin||!PATHS.includes(url.pathname))return;
  event.respondWith((async()=>{
    const cache=await caches.open(CACHE),stored=await cache.match(url.pathname);
    if(stored)return stored;
    // A partially evicted cache must not mix this document with a later release.
    try{const response=await fetch(event.request);if(response.ok&&response.headers.get('X-Training-Version')===VERSION){await cache.put(url.pathname,response.clone());return response;}}catch{}
    return new Response('This app file is unavailable. Reconnect and reopen the app.',{status:503,headers:{'Content-Type':'text/plain'}});
  })());
});`;
}
