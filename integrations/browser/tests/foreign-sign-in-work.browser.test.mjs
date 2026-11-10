// Real Chromium rendering against local WebWork and companion protocol fixtures.
// These checks do not open a provider or read an installed account or credential.
import {test,expect,beforeAll,afterAll} from 'bun:test';
import {readFile,mkdir} from 'node:fs/promises';
import {resolve} from 'node:path';
import {encode,decode} from '../workspace-msgpack.mjs';

const root=resolve(import.meta.dir,'../../..');
const job='b'.repeat(64),revision=7;
const binding={actor:'user:19',subject:'institution:studio',scope:'institution'};
const review={context:'institution:studio',jobId:job,revision,binding};
let browser,server,fixture;
const deferred=()=>{let resolve;const promise=new Promise(done=>{resolve=done;});return {promise,resolve};};
const packet=value=>new Response(encode(value),{headers:{'Content-Type':'application/msgpack','Cache-Control':'no-store'}});
const capability=(f=fixture)=>({canContinue:f.mode!=='refused',canAdminister:f.mode!=='read-only',modes:['provider_tab_sidepanel'],reason:'Restore this run before continuing provider work.',provider:{id:'exa',name:'Exa'},credentialInput:f.mode==='read-only'?null:'native_api_key'});
function responseBinding(f=fixture){return f.mode==='changed-binding'?{...binding,actor:'user:20'}:binding;}
function state(f=fixture){return {binding:responseBinding(f),configured:f.mode!=='unconfigured',canAdminister:f.mode!=='read-only',providers:[{id:'exa',name:'Exa',declared:f.mode!=='undeclared',stored:f.stored,fields:[{name:f.mode==='wrong-field'?'other_key':'api_key',required:true,set:f.stored}]}]};}

const document=`<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Provider step fixture</title><style>body{margin:0;background:#f5f6f2;font:14px/1.5 system-ui}main{padding:24px 12px}h1{font:500 14px system-ui;margin:0 auto 24px;max-width:44rem;color:#555}#host{max-width:44rem;margin:0 auto}@media(prefers-color-scheme:dark){body{background:#171a16}h1{color:#ddd}}</style></head><body><main><h1>Provider step</h1><div id="host"></div></main><script type="module">
import {mountForeignSignIn,createProviderContinuation} from '/apps/extension/foreign-sign-in.mjs';
const review=${JSON.stringify(review)};
const capability={canContinue:true,canAdminister:true,modes:['provider_tab_sidepanel'],provider:{id:'exa',name:'Exa'},credentialInput:'native_api_key'};
window.mountFixture=()=>{
 window.fixtureCard?.dispose();
 const adapter=createProviderContinuation({timeoutMs:100});
 window.fixtureCard=mountForeignSignIn(document.querySelector('#host'),{...review,capability,adapter,destinationLabel:'Studio organization',workTitle:'Review project sources',onChange:()=>window.ownerChanges++,onAccessRefused:error=>window.refusals.push(error.code||error.status),onDismiss:value=>window.dismissals.push(value),onReturnToWork:event=>{event.preventDefault();window.returns.push(event.currentTarget.href);}});
};
window.mountFixture();
</script></body></html>`;

beforeAll(async()=>{
 const {chromium}=await import(process.env.BONFYRE_PLAYWRIGHT_MODULE||'@playwright/test');
 browser=await chromium.launch({headless:true,args:['--no-sandbox'],...(process.env.BONFYRE_CHROMIUM_BINARY?{executablePath:process.env.BONFYRE_CHROMIUM_BINARY}:{})});
 server=Bun.serve({hostname:'127.0.0.1',port:0,async fetch(request){
  const url=new URL(request.url);
  if(url.pathname==='/card')return new Response(document,{headers:{'Content-Type':'text/html'}});
  if(url.pathname==='/sources/_web'){
   const f=fixture,operation=url.searchParams.get('operation'),context=url.searchParams.get('context');
   const payload=request.method==='POST'?decode(new Uint8Array(await request.arrayBuffer()))[1]:undefined;
   f.calls.push({operation,context,method:request.method,...payload?{id:payload.id,revision:payload.revision,binding:payload.binding,provider:payload.provider,secretBytes:payload.secret===undefined?0:new TextEncoder().encode(payload.secret).length}:{}});
   if(context!==review.context)return new Response('Fixture scope refused',{status:403});
   if(f.mode==='unavailable')return new Response('Fixture owner unavailable',{status:503});
   if(operation==='get')return packet([1,true,{id:job,revision,binding:responseBinding(f),providerContinuation:capability(f)}]);
   if(operation==='status')return packet([1,true,state(f)]);
   if(['connect','disconnect'].includes(operation)){
    if(JSON.stringify(payload.binding)!==JSON.stringify(binding)||payload.id!==job||payload.revision!==revision||payload.provider!=='exa')return new Response('Fixture review refused',{status:403});
    const waiting=f.mutationGate;if(waiting){waiting.entered.resolve();await waiting.release.promise;}
    f.stored=operation==='connect';
    if(f.loseMutationAck)return packet([1,false,{code:'fixture_lost_ack',message:payload.secret||'Unconfirmed fixture removal'}]);
    return packet([1,true,state(f)]);
   }
   return new Response('Unsupported fixture operation',{status:400});
  }
  const name=url.pathname.startsWith('/apps/')?url.pathname.slice(6):'';
  if(new Set(['extension/foreign-sign-in.mjs','extension/foreign-sign-in.css','workspace-msgpack.mjs','provider-contract.mjs']).has(name))return new Response(await readFile(resolve(process.env.BONFYRE_TEST_PUBLIC_BROWSER||resolve(root,'integrations/browser'),name)),{headers:{'Content-Type':name.endsWith('.css')?'text/css':'text/javascript'}});
  return new Response('Fixture route not found',{status:404});
 }});
});
afterAll(async()=>{fixture?.mutationGate?.release.resolve();server?.stop(true);await browser?.close();});

async function visit({stored=true,mode='ready',companion=true,bridgeMode='ready',...options}={}){
 fixture={stored,mode,calls:[],loseMutationAck:false,mutationGate:null};
 const context=await browser.newContext({viewport:{width:1100,height:900},...options});
 await context.route('**/*',route=>new URL(route.request().url()).origin===server.url.origin?route.continue():route.abort());
 const page=await context.newPage();
 await page.addInitScript(({companion,bridgeMode,binding,job,revision})=>{
  window.ownerChanges=0;window.refusals=[];window.dismissals=[];window.returns=[];window.bridgeCalls=[];window.bridgeMode=bridgeMode;window.companionAvailable=companion;window.trustedClick=false;window.nativeClicks=[];
  // Measure the actual trusted handler's stack. A document capture listener's
  // queued microtask can run before the target listener in Chromium.
  const listen=EventTarget.prototype.addEventListener;
  EventTarget.prototype.addEventListener=function(type,listener,options){
   if(type==='click'&&this instanceof Element&&this.hasAttribute('data-native-provider-continue')&&typeof listener==='function'){
    const callback=listener;listener=function(event){window.trustedClick=event.isTrusted;try{return callback.call(this,event);}finally{window.trustedClick=false;}};
   }
   return listen.call(this,type,listener,options);
  };
  document.addEventListener('click',event=>{
   const button=event.target.closest?.('[data-native-provider-continue]');if(!button)return;
   window.nativeClicks.push({trusted:event.isTrusted,dataset:{...button.dataset}});
  },true);
  window.addEventListener('aurekai:provider:request',event=>{
   const call=event.detail;window.bridgeCalls.push({operation:call.operation,review:call.review,trustedClick:window.trustedClick});
   const reply=result=>window.dispatchEvent(new CustomEvent('aurekai:provider:reply',{detail:{requestId:call.requestId,ok:true,result}}));
   if(call.operation==='capabilities'){queueMicrotask(()=>reply({available:window.companionAvailable}));return;}
   if(call.operation!=='continue'||window.bridgeMode==='timeout')return;
   const attempts=window.bridgeCalls.filter(row=>row.operation==='continue').length;
   queueMicrotask(()=>reply({state:attempts===1?'provider_open':'provider_focused',work:{id:job,revision},binding,companion:'alongside'}));
  });
  const request=window.fetch;
  window.fetch=(url,options)=>{
   if(String(url).includes('operation=connect')){window.pendingMutationSignal=options.signal;window.pendingMutationBody=options.body;}
   return request(url,options);
  };
 },{companion,bridgeMode,binding,job,revision});
 await page.goto(server.url.origin+'/card');await page.waitForFunction(()=>window.fixtureCard&&document.querySelector('.fsi-card')?.getAttribute('aria-busy')==='false');
 return {page,close:async()=>{fixture.mutationGate?.release.resolve();await context.close();}};
}
const expand=page=>page.getByRole('button',{name:'Expand connection details',exact:true}).click();
const mutationCalls=()=>fixture.calls.filter(row=>['connect','disconnect'].includes(row.operation));
async function screenshot(page,name){if(process.env.BONFYRE_TEST_ARTIFACT_DIR){await mkdir(process.env.BONFYRE_TEST_ARTIFACT_DIR,{recursive:true});await page.screenshot({path:resolve(process.env.BONFYRE_TEST_ARTIFACT_DIR,name),fullPage:true});}}

test('saved access card states its provider and organization, keeps credentials inside details and offers the exact work return',async()=>{
 const {page,close}=await visit();try{
  expect(await page.getByRole('heading',{name:'Use this saved connection?',exact:true}).count()).toBe(1);
  expect(await page.locator('.fsi-origin').textContent()).toBe('https://dashboard.exa.ai');
  expect(await page.locator('.fsi-account-label').textContent()).toBe('Studio organization');
  expect(await page.locator('.fsi-access').textContent()).toBe('API key saved for this destination');
  expect(await page.locator('.fsi-work-reference').textContent()).toContain('Review project sources');
  expect(await page.locator('.fsi-card').textContent()).not.toMatch(/matching login|Passwords|saved password/);
  expect(await page.getByLabel('Exa API key',{exact:true}).isVisible()).toBe(false);
  expect(await page.getByRole('button',{name:'Take over to sign in',exact:true}).isEnabled()).toBe(true);
  expect(await page.getByRole('button',{name:'Not now',exact:true}).isEnabled()).toBe(true);
  expect(await page.evaluate(()=>window.bridgeCalls.filter(row=>row.operation==='continue'))).toEqual([]);
  const primary=page.getByRole('link',{name:'Continue work',exact:true});
  expect(await primary.getAttribute('href')).toBe('/sources/web?context=institution%3Astudio&job='+job);
  const colors=await primary.evaluate(node=>({background:getComputedStyle(node).backgroundColor,color:getComputedStyle(node).color}));
  expect(colors).toEqual({background:'rgb(32, 35, 31)',color:'rgb(255, 255, 255)'});
  await primary.click();expect(await page.evaluate(()=>window.returns.length)).toBe(1);expect(mutationCalls()).toHaveLength(0);
  await screenshot(page,'work-saved-connection-desktop.png');
 }finally{await close();}
},30000);

test('takeover carries a trusted synchronous gesture and the same reviewed work, then returns to its retained provider tab',async()=>{
 const {page,close}=await visit();try{
  await page.getByRole('button',{name:'Take over to sign in',exact:true}).click();
  await page.getByRole('button',{name:'Return to provider',exact:true}).waitFor();
  expect(await page.locator('.fsi-status').textContent()).toBe('Provider tab opened. Aurekai stays alongside it.');
  const [first]=await page.evaluate(()=>window.bridgeCalls.filter(row=>row.operation==='continue'));
  expect(first).toEqual({operation:'continue',review,trustedClick:true});
  expect(await page.evaluate(()=>window.nativeClicks[0])).toEqual({trusted:true,dataset:{nativeProviderContinue:'',nativeProviderContext:review.context,nativeProviderJob:job,nativeProviderRevision:String(revision)}});
  await page.getByRole('button',{name:'Return to provider',exact:true}).click();
  await page.getByText('Returned to the same provider tab. Aurekai stays alongside it.',{exact:true}).waitFor();
  const calls=await page.evaluate(()=>window.bridgeCalls.filter(row=>row.operation==='continue'));
  expect(calls).toHaveLength(2);expect(calls.every(row=>JSON.stringify(row.review)===JSON.stringify(review))).toBe(true);
  expect(mutationCalls()).toHaveLength(0);
 }finally{await close();}
},30000);

test('a missing browser acknowledgment blocks another takeover until an explicit connection read',async()=>{
 const {page,close}=await visit({bridgeMode:'timeout'});try{
  await page.getByRole('button',{name:'Take over to sign in',exact:true}).click();
  await page.locator('.fsi-card[data-state="uncertain"]').waitFor();
  expect(await page.getByRole('button',{name:'Take over to sign in',exact:true}).isDisabled()).toBe(true);
  expect(await page.getByRole('link',{name:'Continue work',exact:true}).getAttribute('aria-disabled')).toBe('true');
  await page.evaluate(()=>document.querySelector('[data-native-provider-continue]').click());
  expect(await page.evaluate(()=>window.bridgeCalls.filter(row=>row.operation==='continue').length)).toBe(1);
  await expand(page);await page.getByRole('button',{name:'Check connection',exact:true}).click();
  await page.waitForFunction(()=>document.querySelector('[data-native-provider-continue]').disabled===false);
  expect(await page.evaluate(()=>window.bridgeCalls.filter(row=>row.operation==='continue').length)).toBe(1);
  await page.evaluate(()=>{window.bridgeMode='ready';});await page.getByRole('button',{name:'Take over to sign in',exact:true}).click();
  await page.getByRole('button',{name:'Return to provider',exact:true}).waitFor();
  expect(await page.evaluate(()=>window.bridgeCalls.filter(row=>row.operation==='continue').length)).toBe(2);
 }finally{await close();}
},30000);

test('a lost key acknowledgment clears entry, suppresses echoed secrets and rereads owner state without replay',async()=>{
 const {page,close}=await visit({stored:false});try{
  fixture.loseMutationAck=true;await expand(page);
  await page.getByLabel('Exa API key',{exact:true}).fill('fixture-unconfirmed-key');await page.getByRole('button',{name:'Save API key',exact:true}).click();
  await page.locator('.fsi-card[data-state="uncertain"]').waitFor();
  expect(await page.getByLabel('Exa API key',{exact:true}).inputValue()).toBe('');
  expect(await page.locator('body').textContent()).not.toContain('fixture-unconfirmed-key');
  expect(await page.evaluate(()=>window.pendingMutationBody.every(byte=>byte===0))).toBe(true);
  expect(await page.getByRole('button',{name:'Save API key',exact:true}).isDisabled()).toBe(true);
  expect(await page.getByRole('button',{name:'Take over to sign in',exact:true}).isDisabled()).toBe(true);
  expect(mutationCalls()).toHaveLength(1);expect(await page.evaluate(()=>window.ownerChanges)).toBe(0);
  await page.getByRole('button',{name:'Check connection',exact:true}).click();
  await page.getByRole('heading',{name:'Use this saved connection?',exact:true}).waitFor();
  expect(await page.getByRole('button',{name:'Save API key',exact:true}).isEnabled()).toBe(true);
  expect(mutationCalls()).toHaveLength(1);expect(await page.evaluate(()=>localStorage.length+sessionStorage.length)).toBe(0);
 }finally{await close();}
},30000);

test('Not now aborts pending presentation and clears request bytes while the native work and eventual owner result remain',async()=>{
 const {page,close}=await visit({stored:false});const waiting={entered:deferred(),release:deferred()};try{
  fixture.mutationGate=waiting;await expand(page);await page.getByLabel('Exa API key',{exact:true}).fill('fixture-deferred-key');
  await page.evaluate(()=>{window.previousInput=document.querySelector('.fsi-card input');});
  await page.getByRole('button',{name:'Save API key',exact:true}).click();await waiting.entered.promise;
  await page.getByRole('button',{name:'Not now',exact:true}).click();
  expect(await page.locator('.fsi-card').count()).toBe(0);
  expect(await page.evaluate(()=>window.previousInput.value)).toBe('');
  expect(await page.evaluate(()=>window.pendingMutationSignal.aborted)).toBe(true);
  await page.waitForFunction(()=>window.pendingMutationBody.every(byte=>byte===0));
  expect(await page.evaluate(()=>window.dismissals)).toEqual([{reviewKey:review.context+':'+job+':'+revision,pending:true}]);
  expect(await page.evaluate(()=>window.bridgeCalls.filter(row=>row.operation==='clear').length)).toBe(1);
  waiting.release.resolve();await page.evaluate(()=>new Promise(done=>setTimeout(done,30)));
  expect(await page.evaluate(()=>window.ownerChanges)).toBe(0);expect(mutationCalls()).toHaveLength(1);
  await page.evaluate(()=>window.mountFixture());await page.getByRole('heading',{name:'Use this saved connection?',exact:true}).waitFor();
  expect(await page.getByRole('link',{name:'Continue work',exact:true}).getAttribute('href')).toContain('job='+job);
  expect(fixture.calls.every(call=>!['cancel','archive','complete'].includes(call.operation))).toBe(true);
 }finally{waiting.release.resolve();await close();}
},30000);

test('changed native account binding clears the draft, hides the prior account and disables all continuations',async()=>{
 const {page,close}=await visit();try{
  await expand(page);await page.getByLabel('Exa API key',{exact:true}).fill('fixture-old-account-key');fixture.mode='changed-binding';
  await page.getByRole('button',{name:'Check connection',exact:true}).click();
  await page.getByRole('heading',{name:'Reopen this work to continue',exact:true}).waitFor();
  expect(await page.getByLabel('Exa API key',{exact:true}).inputValue()).toBe('');
  expect(await page.locator('.fsi-account').isVisible()).toBe(false);
  expect(await page.getByRole('button',{name:'Take over to sign in',exact:true}).isDisabled()).toBe(true);
  expect(await page.getByRole('link',{name:'Continue work',exact:true}).getAttribute('aria-disabled')).toBe('true');
  expect(await page.getByRole('link',{name:'Open Exa directly',exact:true}).isVisible()).toBe(false);
  expect(await page.evaluate(()=>window.refusals)).toEqual(['binding_changed']);expect(mutationCalls()).toHaveLength(0);
 }finally{await close();}
},30000);

test('unavailable owner state stays unavailable and a missing companion offers an explicit provider link',async()=>{
 const unavailable=await visit({mode:'unavailable'});try{
  expect(await unavailable.page.locator('.fsi-card').getAttribute('data-state')).toBe('unavailable');
  expect(await unavailable.page.locator('.fsi-access').textContent()).toBe('Saved access unavailable');
  expect(await unavailable.page.getByRole('button',{name:'Take over to sign in',exact:true}).isDisabled()).toBe(true);
  await expand(unavailable.page);expect(await unavailable.page.getByLabel('Exa API key',{exact:true}).isVisible()).toBe(false);
  expect(await unavailable.page.getByRole('link',{name:'Open Exa directly',exact:true}).isVisible()).toBe(false);
 }finally{await unavailable.close();}
 const missing=await visit({companion:false});try{
  expect(await missing.page.getByRole('button',{name:'Take over to sign in',exact:true}).isDisabled()).toBe(true);
  await expand(missing.page);const link=missing.page.getByRole('link',{name:'Open Exa directly',exact:true});
  expect(await link.getAttribute('href')).toBe('https://dashboard.exa.ai/api-keys');expect(await link.getAttribute('rel')).toBe('noopener noreferrer');
  expect(await missing.page.evaluate(()=>window.bridgeCalls.filter(row=>row.operation==='continue').length)).toBe(0);
 }finally{await missing.close();}
},30000);

test('read-only, unconfigured and mismatched native declarations never expose credential writes',async()=>{
 for(const mode of ['read-only','unconfigured','undeclared','wrong-field']){
  const {page,close}=await visit({mode});try{
   await expand(page);expect(await page.getByLabel('Exa API key',{exact:true}).isVisible()).toBe(false);
   expect(await page.getByRole('button',{name:'Save API key',exact:true}).isVisible()).toBe(false);
   expect(await page.getByRole('button',{name:'Remove saved API key',exact:true}).isVisible()).toBe(false);
   expect(await page.getByRole('button',{name:'Take over to sign in',exact:true}).isEnabled()).toBe(true);
   expect(mutationCalls()).toHaveLength(0);
  }finally{await close();}
 }
},30000);

test('compact and expanded cards fit narrow screens in light and dark, with visible keyboard controls and a reversible Escape',async()=>{
 for(const [width,colorScheme]of [[320,'light'],[390,'dark'],[1100,'dark']]){
  const {page,close}=await visit({viewport:{width,height:900},colorScheme});try{
   expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);
   for(const control of [page.getByRole('button',{name:'Not now',exact:true}),page.getByRole('button',{name:'Expand connection details',exact:true}),page.getByRole('link',{name:'Continue work',exact:true})]){
    const box=await control.boundingBox();expect(box.x).toBeGreaterThanOrEqual(0);expect(box.x+box.width).toBeLessThanOrEqual(width);expect(box.height).toBeGreaterThanOrEqual(40);
   }
   const colors=await page.locator('.fsi-card').evaluate(node=>({color:getComputedStyle(node).color,background:getComputedStyle(node).backgroundColor}));
   expect(colors).toEqual(colorScheme==='dark'?{color:'rgb(241, 243, 237)',background:'rgb(32, 35, 31)'}:{color:'rgb(32, 35, 31)',background:'rgb(255, 255, 255)'});
   await screenshot(page,'work-compact-'+width+'-'+colorScheme+'.png');
   const disclosure=page.getByRole('button',{name:'Expand connection details',exact:true});await disclosure.focus();await page.keyboard.press('Enter');
   expect(await page.getByRole('button',{name:'Collapse connection details',exact:true}).getAttribute('aria-expanded')).toBe('true');
   await page.getByLabel('Exa API key',{exact:true}).fill('fixture-collapsed-key');
   expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);
   await screenshot(page,'work-expanded-'+width+'-'+colorScheme+'.png');
   await page.keyboard.press('Escape');expect(await page.getByLabel('Exa API key',{exact:true}).inputValue()).toBe('');
   expect(await disclosure.getAttribute('aria-expanded')).toBe('false');expect(await disclosure.evaluate(node=>document.activeElement===node)).toBe(true);
   await page.keyboard.press('Escape');expect(await page.locator('.fsi-card').count()).toBe(0);
   expect(await page.evaluate(()=>window.dismissals[0].pending)).toBe(false);expect(mutationCalls()).toHaveLength(0);
  }finally{await close();}
 }
},30000);
