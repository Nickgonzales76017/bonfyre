// Browser platform witness on loopback. No vendor subscription is created and
// showNotification is injected, so this test never sends an OS alert to a user.
import {afterAll,beforeAll,expect,test} from 'bun:test';
import {readFile} from 'node:fs/promises';
import {resolve} from 'node:path';
const root=resolve(import.meta.dir,'..');let browser,server;
beforeAll(async()=>{
  const {chromium}=await import(process.env.BONFYRE_PLAYWRIGHT_MODULE||'playwright');
  browser=await chromium.launch({headless:true,...process.env.BONFYRE_CHROMIUM_BINARY?{executablePath:process.env.BONFYRE_CHROMIUM_BINARY}:{}});
  server=Bun.serve({hostname:'127.0.0.1',port:0,async fetch(request){
    const url=new URL(request.url);
    if(url.pathname==='/apps/fixture')return new Response('<!doctype html><html><head><title>Push platform fixture</title></head><body><button id="enable">Enable fixture notifications</button></body></html>',{headers:{'Content-Type':'text/html'}});
    if(/^\/apps\/(?:aurekai-push(?:-sw)?|aurekai-activity-source|workspace-msgpack)\.mjs$/.test(url.pathname))
      return new Response(await readFile(resolve(root,url.pathname.slice(6))),{headers:{'Content-Type':'text/javascript','Cache-Control':'no-store'}});
    return new Response('No native action is available in this fixture',{status:404});
  }});
});
afterAll(async()=>{await browser?.close();server?.stop(true);},30000);

test('real module service worker installs and actual IndexedDB deduplicates concurrent delivery reservations across cache instances',async()=>{
  const context=await browser.newContext(),page=await context.newPage();
  try {
    await page.goto(server.url+'apps/fixture');
    const result=await page.evaluate(async()=>{
      await navigator.serviceWorker.register('/apps/aurekai-push-sw.mjs',{type:'module',scope:'/apps/'});
      const registration=await navigator.serviceWorker.ready;
      const {createPushReceiptCache}=await import('/apps/aurekai-push-sw.mjs');
      const one=createPushReceiptCache(indexedDB),two=createPushReceiptCache(indexedDB),now=Date.now(),id='1'.repeat(24);
      const reservations=await Promise.all([one.reserve(id,now+60000,now),two.reserve(id,now+60000,now)]);
      await one.shown(id);
      const again=await createPushReceiptCache(indexedDB).reserve(id,now+60000,now);
      const rows=await new Promise((resolve,reject)=>{const open=indexedDB.open('aurekai-push-receipts',1);open.onsuccess=()=>{
        const db=open.result,tx=db.transaction('receipts','readonly'),get=tx.objectStore('receipts').getAll();get.onsuccess=()=>resolve(get.result);get.onerror=()=>reject(get.error);tx.oncomplete=()=>db.close();};});
      await registration.unregister();
      return {script:registration.active?.scriptURL,scope:registration.scope,reservations,again,rows};
    });
    expect(result.script).toBe(new URL('/apps/aurekai-push-sw.mjs',server.url).href);expect(result.scope).toBe(new URL('/apps/',server.url).href);
    expect(result.reservations.filter(Boolean)).toHaveLength(1);expect(result.again).toBe(false);expect(result.rows).toHaveLength(1);
    expect(Object.keys(result.rows[0]).sort()).toEqual(['expiresAt','id','stage']);expect(result.rows[0].stage).toBe('shown');
  } finally {await context.close();}
});

test('actual trusted click reaches explicit permission handling while synthetic events cannot; no native or vendor request occurs',async()=>{
  const context=await browser.newContext(),page=await context.newPage();
  try {
    await page.goto(server.url+'apps/fixture');
    await page.evaluate(async()=>{
      const {enablePush}=await import('/apps/aurekai-push.mjs');window.results=[];window.permissionCalls=0;
      const env={isSecureContext,location,crypto,PushManager,Notification:{permission:'default',requestPermission:async()=>{window.permissionCalls++;return 'denied';}},
        navigator:{serviceWorker:navigator.serviceWorker,userActivation:navigator.userActivation,locks:navigator.locks}};
      const status={configured:true,paused:false,publicKey:'B'+'a'.repeat(86),binding:{actor:'user:1',subject:'user:1',scope:'self'}};
      document.querySelector('#enable').onclick=event=>enablePush('self',status,{environment:env,event}).then(()=>window.results.push('unexpected'),error=>window.results.push(error.code));
      document.querySelector('#enable').dispatchEvent(new MouseEvent('click',{bubbles:true}));
    });
    await page.waitForFunction(()=>window.results.length===1);expect(await page.evaluate(()=>({results:window.results,calls:window.permissionCalls}))).toEqual({results:['gesture_required'],calls:0});
    await page.locator('#enable').click();await page.waitForFunction(()=>window.results.length===2);
    expect(await page.evaluate(()=>({results:window.results,calls:window.permissionCalls}))).toEqual({results:['gesture_required','permission_required'],calls:1});
  } finally {await context.close();}
});
