// Installed Chrome extension + real content script, service worker, sidePanel and
// native-messaging transport. Aurekai HTTP authentication and provider pages are
// intercepted fixtures; WebWork SQLite and the native admission validator are real.
// No external provider request, real account, stored secret or live work is used.
import {test,expect} from 'bun:test';
import {readFile,mkdtemp,mkdir,writeFile,rm} from 'node:fs/promises';
import {resolve,join} from 'node:path';
import {tmpdir} from 'node:os';
import {WebWork} from '../../../cmd/BonfyreSourceFrontier/tools/web_work.mjs';
import {encode} from '../workspace-msgpack.mjs';
const root=resolve(import.meta.dir,'..');
const nativePacket=value=>({status:200,contentType:'application/msgpack',headers:{'Cache-Control':'no-store'},body:Buffer.from(encode(value))});
const html=body=>({status:200,contentType:'text/html',body:'<!doctype html><meta charset="utf-8">'+body});
async function providerPanel(context,page,url){
 const cdp=await context.newCDPSession(page),{targetInfos}=await cdp.send('Target.getTargets'),target=targetInfos.find(t=>t.url===url);
 if(!target){await cdp.detach();throw Error('The installed provider panel target is unavailable.');}
 const {sessionId}=await cdp.send('Target.attachToTarget',{targetId:target.targetId,flatten:false});let sequence=0;const pending=new Map();
 cdp.on('Target.receivedMessageFromTarget',event=>{if(event.sessionId!==sessionId)return;const result=JSON.parse(event.message),job=pending.get(result.id);if(!job)return;pending.delete(result.id);clearTimeout(job.timer);result.error?job.reject(Error(result.error.message)):job.resolve(result.result);});
 const send=(method,params={})=>new Promise((resolve,reject)=>{const id=++sequence,timer=setTimeout(()=>{pending.delete(id);reject(Error('The provider panel did not answer '+method));},10000);pending.set(id,{resolve,reject,timer});cdp.send('Target.sendMessageToTarget',{sessionId,message:JSON.stringify({id,method,params})}).catch(error=>{pending.delete(id);clearTimeout(timer);reject(error);});});
 return{async clickReturn(){const point=await send('Runtime.evaluate',{expression:'(()=>{const r=document.getElementById("provider-return").getBoundingClientRect();return{x:r.x+r.width/2,y:r.y+r.height/2};})()',returnByValue:true});const {x,y}=point.result.value;await send('Input.dispatchMouseEvent',{type:'mousePressed',x,y,button:'left',clickCount:1});await send('Input.dispatchMouseEvent',{type:'mouseReleased',x,y,button:'left',clickCount:1});const status=await send('Runtime.evaluate',{expression:'(async()=>{const s=document.getElementById("provider-work-said");for(let i=0;i<80;i++){if(s.textContent&&!s.textContent.startsWith("Returning"))return s.textContent;await new Promise(r=>setTimeout(r,100));}throw Error("Provider return did not finish");})()',awaitPromise:true,returnByValue:true});if(status.exceptionDetails)throw Error(status.exceptionDetails.text);return status.result.value;},async close(){for(const job of pending.values()){clearTimeout(job.timer);job.reject(Error('Panel witness closed.'));}pending.clear();await cdp.detach();}};
}

test('installed browser companion admits exact current work and preserves provider MFA through pane close and reopen',async()=>{
 const chromePath=process.env.BONFYRE_EXTENSION_CHROMIUM_BINARY;
 if(!chromePath)throw Error('Set BONFYRE_EXTENSION_CHROMIUM_BINARY to full Chrome for Testing; headless-shell does not load extensions.');
 const {chromium}=await import(process.env.BONFYRE_PLAYWRIGHT_MODULE||'@playwright/test');
 const directory=await mkdtemp(join(tmpdir(),'provider-installed-')),profile=join(directory,'profile'),calls=[],errors=[];
 let browser,providerAccess=true;const native=new WebWork(':memory:',{subject:'institution:studio',scope:'institution',actor:'user:1'});
 const job=native.plan({requestId:'installed-provider-fixture',title:'Exact installed provider work',mode:'account',spec:{provider:'exa',operation:'search',query:'Reference',objective:'Isolated browser witness',limit:3}});
 try{
  await mkdir(profile,{recursive:true});
  browser=await chromium.launchPersistentContext(profile,{headless:true,executablePath:chromePath,args:['--no-sandbox','--disable-extensions-except='+join(root,'extension'),'--load-extension='+join(root,'extension')],ignoreDefaultArgs:['--disable-extensions']});
  const worker=browser.serviceWorkers()[0]||await browser.waitForEvent('serviceworker',{timeout:15000}),extensionId=new URL(worker.url()).hostname;
  const host=join(directory,'native-provider.mjs');
  // A framing-only test carrier invokes the actual native validator. The full
  // estate host's unrelated retained-page/enrollment binaries are not fixtures.
  await writeFile(host,'#!'+process.execPath+'\n'+
   'import {admitProviderPacket} from '+JSON.stringify(join(root,'host/provider-admission.mjs'))+';\n'+
   'let pending=Buffer.alloc(0);for await(const chunk of Bun.stdin.stream()){pending=Buffer.concat([pending,Buffer.from(chunk)]);while(pending.length>=4){const size=pending.readUInt32LE(0);if(size>262144)process.exit(2);if(pending.length<4+size)break;const message=JSON.parse(pending.subarray(4,4+size));pending=pending.subarray(4+size);let reply;try{if(message.operation!=="provider_admit"||message.confirmed!==true)throw Error("Fixture carries provider_admit only");reply={id:message.id,ok:true,result:admitProviderPacket(message)};}catch(error){reply={id:message.id,ok:false,refused:"owner_refused",reason:error.message};}const body=Buffer.from(JSON.stringify(reply)),head=Buffer.alloc(4);head.writeUInt32LE(body.length);process.stdout.write(Buffer.concat([head,body]));}}\n',{mode:0o700});
  await mkdir(join(profile,'NativeMessagingHosts'),{recursive:true});
  await writeFile(join(profile,'NativeMessagingHosts/com.bonfyre.browser_host.json'),JSON.stringify({name:'com.bonfyre.browser_host',description:'Isolated provider admission witness',path:host,type:'stdio',allowed_origins:['chrome-extension://'+extensionId+'/']}));
  await browser.route('**/*',async route=>{
   const address=new URL(route.request().url());
   if(address.origin==='https://aurekai.work'){
    if(address.pathname==='/fixture'){const selected=address.searchParams.get('job')||job.id;if(!/^[a-f0-9]{64}$/.test(selected))return route.abort();return route.fulfill(html('<main><h1>Native provider fixture</h1><div id="card"></div></main><script type="module">import {mountForeignSignIn} from "/apps/extension/foreign-sign-in.mjs";const job=await fetch("/sources/_web?context=institution%3Astudio&operation=get&job='+selected+'").then(r=>r.arrayBuffer()).then(async bytes=>(await import("/apps/workspace-msgpack.mjs")).decode(new Uint8Array(bytes))[2]);window.card=mountForeignSignIn(document.querySelector("#card"),{context:"institution:studio",jobId:job.id,revision:job.revision,binding:job.binding,capability:job.providerContinuation,onAccessRefused(error){window.accessRefused={status:error.status,code:error.code};window.card.dispose();}});</script>'));}
    if(address.pathname.startsWith('/apps/')){
     const path=address.pathname.slice(6);if(!['extension/foreign-sign-in.mjs','extension/foreign-sign-in.css','workspace-msgpack.mjs','provider-contract.mjs'].includes(path))return route.abort();
     return route.fulfill({status:200,contentType:path.endsWith('.css')?'text/css':'text/javascript',body:await readFile(join(process.env.BONFYRE_TEST_PUBLIC_BROWSER||root,path))});
    }
    if(address.pathname==='/sources/_web'){
     const operation=address.searchParams.get('operation');if(operation==='provider'&&!providerAccess)return route.fulfill({status:403,body:'Fixture access revoked'});calls.push({operation,method:route.request().method(),origin:route.request().headers().origin||null});
     try{const value=operation==='get'?native.get(address.searchParams.get('job')):operation==='status'?native.connectionStatus():operation==='provider'?native.provider({id:address.searchParams.get('job'),revision:Number(address.searchParams.get('revision')),challenge:address.searchParams.get('challenge'),origin:address.origin}):null;if(!value)return route.abort();return route.fulfill(nativePacket([1,true,value]));}
     catch(error){return route.fulfill(nativePacket([1,false,{code:error.code||'refused',message:error.message}]));}
    }
   }
   if(address.origin==='https://dashboard.exa.ai')return route.fulfill(html('<h1>Provider fixture only</h1><a href="https://provider.example.test/mfa?stage=retained">Continue to MFA fixture</a>'));
   if(address.origin==='https://provider.example.test')return route.fulfill(html('<h1>Provider MFA fixture</h1><label>Unsubmitted fixture value<input aria-label="Unsubmitted fixture value"></label>'));
   return route.abort();
  });
  const page=await browser.newPage();page.setDefaultTimeout(10000);page.on('pageerror',error=>errors.push(error.message));
  await page.goto('https://aurekai.work/fixture');const open=page.getByRole('button',{name:/^(Take over to sign in|Return to provider)$/});await page.waitForFunction(()=>document.querySelector('[data-native-provider-continue]')?.disabled===false).catch(async error=>{throw Error(error.message+' '+JSON.stringify({errors,calls,status:await page.locator('.fsi-status').textContent()}));});
  await page.getByRole('button',{name:'Expand connection details',exact:true}).click();expect(await page.locator('input[type=password]').isVisible()).toBe(false);expect(await page.locator('.ak-provider-details').textContent()).toContain('destination administrator');
  await page.evaluate(()=>document.querySelector('[data-native-provider-continue]').click());await page.getByRole('status').filter({hasText:'Use the provider button'}).waitFor();expect(calls.filter(c=>c.operation==='provider')).toHaveLength(0);
  await open.click();await page.getByRole('status').filter({hasText:'Provider tab opened'}).waitFor();
  // Opening Chrome's side panel can emit its own page target first. Select the
  // admitted provider address, never the order of unrelated browser targets.
  let provider;for(let i=0;i<100;i++){provider=browser.pages().find(p=>p.url()==='https://dashboard.exa.ai/api-keys');if(provider)break;await new Promise(r=>setTimeout(r,50));}if(!provider)throw Error('Provider target unavailable: '+JSON.stringify({pages:browser.pages().map(p=>p.url()),status:await page.getByRole('status').textContent(),calls}));
  expect(calls.filter(c=>c.operation==='provider')).toHaveLength(1);
  await provider.getByRole('link',{name:'Continue to MFA fixture'}).click();await provider.waitForURL('https://provider.example.test/mfa?stage=retained');await provider.getByLabel('Unsubmitted fixture value').fill('public fixture text');
  const providerPages=()=>browser.pages().filter(p=>['https://dashboard.exa.ai','https://provider.example.test'].includes(new URL(p.url()).origin));const providerCount=providerPages().length;expect(providerCount).toBe(1);
  const contexts=await worker.evaluate(async()=>{for(let i=0;i<20;i++){const contexts=await chrome.runtime.getContexts({contextTypes:['SIDE_PANEL']});if(contexts.length)return contexts;await new Promise(r=>setTimeout(r,100));}return [];});if(!contexts.some(c=>c.documentUrl.endsWith('panel.html?provider=1')))throw Error('No provider side panel: '+JSON.stringify({contexts,status:await page.getByRole('status').textContent(),options:await worker.evaluate(()=>chrome.sidePanel.getOptions({}))}));
  expect(await worker.evaluate(async()=>{const {id}=await chrome.windows.getCurrent();await chrome.sidePanel.close({windowId:id});return true;})).toBe(true);
  await page.bringToFront();await open.click();await page.getByRole('status').filter({hasText:'Returned to the same provider tab'}).waitFor();expect(providerPages()).toHaveLength(providerCount);expect(provider.url()).toBe('https://provider.example.test/mfa?stage=retained');expect(await provider.getByLabel('Unsubmitted fixture value').inputValue()).toBe('public fixture text');
  expect(native.get(job.id).state).toBe('planned');expect(native.get(job.id).revision).toBe(1);
  await page.evaluate(()=>window.card.dispose());expect(provider.isClosed()).toBe(false);expect(native.get(job.id).revision).toBe(1);
  await page.goto('https://aurekai.work/fixture');await page.waitForFunction(()=>document.querySelector('[data-native-provider-continue]')?.disabled===false);await open.click();await page.getByRole('status').filter({hasText:'Returned to the same provider tab'}).waitFor();expect(providerPages()).toHaveLength(providerCount);expect(await provider.getByLabel('Unsubmitted fixture value').inputValue()).toBe('public fixture text');
  // Open another work in the same window, then return from the older provider
  // through the actual panel UI. A window-wide last-opened marker chooses wrong.
  const other=native.plan({requestId:'installed-second-provider-fixture',title:'Another installed provider work',mode:'account',spec:{provider:'exa',operation:'search',query:'Second',objective:'Exact return witness',limit:3}}),second=await browser.newPage();second.setDefaultTimeout(10000);second.on('pageerror',error=>errors.push(error.message));
  await second.goto('https://aurekai.work/fixture?job='+other.id);await second.waitForFunction(()=>document.querySelector('[data-native-provider-continue]')?.disabled===false);await second.getByRole('button',{name:'Take over to sign in',exact:true}).click();await second.getByRole('status').filter({hasText:'Provider tab opened'}).waitFor();expect(providerPages()).toHaveLength(2);
  const panel=await providerPanel(browser,page,'chrome-extension://'+extensionId+'/panel.html?provider=1');
  try{await provider.bringToFront();expect(await panel.clickReturn()).toBe('Your native work remains available in Aurekai.');}finally{await panel.close();}
  const active=await worker.evaluate(async()=>{const {id}=await chrome.windows.getCurrent();return (await chrome.tabs.query({windowId:id,active:true})).map(tab=>tab.url);});expect(active).toEqual([page.url()]);expect(active).not.toEqual([second.url()]);
  expect(providerPages()).toHaveLength(2);expect(provider.url()).toBe('https://provider.example.test/mfa?stage=retained');expect(await provider.getByLabel('Unsubmitted fixture value').inputValue()).toBe('public fixture text');expect(native.get(other.id).revision).toBe(1);expect(native.get(job.id).revision).toBe(1);
  providerAccess=false;await page.bringToFront();await open.click();await page.waitForFunction(()=>window.accessRefused?.status===403);expect(await page.locator('.ak-provider-card').count()).toBe(0);expect(provider.isClosed()).toBe(false);expect(native.get(job.id).revision).toBe(1);providerAccess=true;await page.goto('https://aurekai.work/fixture');await page.waitForFunction(()=>document.querySelector('[data-native-provider-continue]')?.disabled===false);native.archive({id:job.id,revision:1});await page.bringToFront();await open.click();await page.getByRole('status').filter({hasText:/changed|incomplete|Restore/}).waitFor();expect(providerPages()).toHaveLength(2);expect(native.get(job.id).revision).toBe(2);expect(provider.url()).toBe('https://provider.example.test/mfa?stage=retained');
  expect(errors).toEqual([]);
 }finally{await browser?.close();native.close();await rm(directory,{recursive:true,force:true});}
},60000);
