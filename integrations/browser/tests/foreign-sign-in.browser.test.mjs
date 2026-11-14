// Real browser UI with protocol fixtures. No provider request, installed Chrome
// extension, native credential database, or successful sign-in is implied.
import {test,expect,beforeAll,afterAll} from 'bun:test';
import {deferred,gate,packet,launchFixtureChromium,loopbackServer} from './browser-fixture.mjs';
import {readFile,mkdir} from 'node:fs/promises';
import {resolve} from 'node:path';
import {encode,decode} from '../workspace-msgpack.mjs';
const root=resolve(import.meta.dir,'../../..');
let browser,server,requests=[],stored=new Map(),statusMode='ready',connectionGate=null,statusGate=null;
const binding=context=>({actor:'user:1',subject:context==='self'?'user:1':context,scope:context==='self'?'self':'institution'});
function state(context){return {binding:binding(context),canAdminister:statusMode!=='read-only',configured:statusMode!=='unavailable',admission:{enabled:false,maxPages:12,reason:'Fixture external reads are off.'},providers:['exa','firecrawl'].map(id=>({id,name:id==='exa'?'Exa':'Firecrawl',declared:statusMode!=='undeclared',stored:stored.get(context+':'+id)===true,verified:false,fields:[{name:statusMode==='wrong-field'?'other_key':'api_key',required:true,set:stored.get(context+':'+id)===true}]}))};}
const head='<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><link rel="stylesheet" href="/apps/web-workbench.css"><link rel="stylesheet" href="/apps/extension/foreign-sign-in.css">';
beforeAll(async()=>{
 browser=await launchFixtureChromium({module:'@playwright/test',noSandbox:true});
 server=loopbackServer({async fetch(request){
  const url=new URL(request.url);
  if(url.pathname==='/card')return new Response('<!doctype html><html lang="en"><head>'+head+'</head><body style="margin:16px;font:16px/1.5 system-ui"><div id="host" class="web-workbench" style="padding:0"></div><script type="module">import {mountForeignSignIn} from "/apps/extension/foreign-sign-in.mjs";window.mountFixture=(more={})=>{window.fixtureCard?.destroy();const details=document.createElement("div");details.textContent="Continue in the provider site or existing side panel.";window.fixtureCard=mountForeignSignIn(document.querySelector("#host"),{origin:"https://mcp.exa.ai",destination:"Your workspace · self",title:"Connect Exa",reviewKey:"self:exa:1",fields:[{name:"api_key",label:"API key",kind:"secret",maxLength:4095}],details,onSubmit:()=>{},onOpen:()=>{window.providerOpens.push("https://exa.ai/docs/get-started/exa-mcp");},...more});};window.mountFixture();</script></body></html>',{headers:{'Content-Type':'text/html'}});
  if(url.pathname==='/workbench')return new Response('<!doctype html><html lang="en"><head>'+head+'</head><body>'+(url.searchParams.has('navigation')?'<span data-account-slot><span class="ak-session" data-session-state="signed-in">Fixture account</span></span>':'')+'<main data-fixture-workbench class="web-workbench"><p data-web-status role="status"></p><div data-web-content></div></main><script type="module">import {mountWebWorkbench} from "/apps/web-workbench.mjs";const root=document.querySelector("[data-fixture-workbench]");root.dataset.webWorkbench="";window.fixtureWorkbench=await mountWebWorkbench(root);</script></body></html>',{headers:{'Content-Type':'text/html'}});
  if(url.pathname==='/apps/_destinations')return packet([1,[['self','Your workspace',true],['institution:fixture','Fixture institution',true]]]);
  if(url.pathname==='/sources/_web'){
   const operation=url.searchParams.get('operation'),context=url.searchParams.get('context');
   if(!['self','institution:fixture'].includes(context))return new Response('Fixture context refused',{status:403});
   const payload=request.method==='POST'?decode(new Uint8Array(await request.arrayBuffer()))[1]:undefined;
   requests.push({operation,context,method:request.method,payload,accept:request.headers.get('accept'),type:request.headers.get('content-type')});
   if(operation==='status'){if(statusGate){const waiting=statusGate;statusGate=null;waiting.entered.resolve();await waiting.release.promise;}return packet([1,true,state(context)]);}
   if(operation==='list')return packet([1,true,{binding:binding(context),jobs:[],cursor:0,more:false}]);
   if(['connect','disconnect'].includes(operation)){
    if(statusMode==='unavailable'||statusMode==='undeclared'||statusMode==='wrong-field')return packet([1,false,{code:'connection_unavailable',message:'The native declaration is unavailable.'}]);
    const waiting=connectionGate;if(waiting){connectionGate=null;waiting.entered.resolve();await waiting.release.promise;}
    stored.set(context+':'+payload.provider,operation==='connect');
    if(waiting?.fail)return packet([1,false,{code:'connection_unavailable',message:payload.secret||'Fixture acknowledgement lost.'}]);
    return packet([1,true,state(context)]);
   }
   return new Response('Unsupported fixture operation',{status:400});
  }
  const paths=new Set(['web-workbench.mjs','web-workbench.css','workspace-msgpack.mjs','workspace-store.mjs','workspace-value.mjs','workspace-continuation.mjs','workspace-destinations.mjs','record-comparison.mjs','interaction-collection.mjs','provider-contract.mjs','extension/foreign-sign-in.mjs','extension/foreign-sign-in.css']);
  const asset=url.pathname.startsWith('/apps/')?url.pathname.slice(6):'';
  if(paths.has(asset))return new Response(await readFile(resolve(process.env.BONFYRE_TEST_PUBLIC_BROWSER||resolve(root,'integrations/browser'),asset)),{headers:{'Content-Type':asset.endsWith('.css')?'text/css':'text/javascript'}});
  return new Response('Fixture route not found',{status:404});
 }});
});
afterAll(async()=>{connectionGate?.release.resolve();statusGate?.release.resolve();server?.stop(true);await browser?.close();});
async function visit(path='/card',options={}){
 const context=await browser.newContext({viewport:{width:1024,height:900},...options});
 await context.route('**/*',route=>new URL(route.request().url()).origin===server.url.origin?route.continue():route.abort());
 const page=await context.newPage();
 await page.addInitScript(()=>{window.providerOpens=[];window.observedEvents=[];window.fetchOptions=[];window.open=(url,target,features)=>{window.providerOpens.push({url,target,features});return null;};for(const type of ['bonfyre:work-changed','bonfyre:workspace-changed'])window.addEventListener(type,event=>window.observedEvents.push({type,detail:event.detail}));const request=window.fetch;window.fetch=(url,options)=>{window.fetchOptions.push({url:String(url),credentials:options?.credentials,redirect:options?.redirect,cache:options?.cache,headers:options?.headers});return request(url,options);};});
 await page.goto(server.url.origin+path);await page.waitForFunction(()=>window.fixtureCard||window.fixtureWorkbench,null,{timeout:8000});
 return {page,close:()=>context.close()};
}
async function connections(page){await page.getByText('Provider connections',{exact:true}).click();await page.locator('.fsi-card').waitFor();}
async function screenshot(page,name){if(process.env.BONFYRE_TEST_ARTIFACT_DIR){await mkdir(process.env.BONFYRE_TEST_ARTIFACT_DIR,{recursive:true});await page.screenshot({path:resolve(process.env.BONFYRE_TEST_ARTIFACT_DIR,name),fullPage:true});}}
test('compact card exposes its exact origin and destination, and only an explicit action opens the provider',async()=>{
 const {page,close}=await visit();try{
  expect(await page.locator('.fsi-origin').textContent()).toBe('https://mcp.exa.ai');expect(await page.locator('.fsi-destination').textContent()).toBe('Your workspace · self');expect(await page.evaluate(()=>window.providerOpens)).toEqual([]);
  await page.getByRole('button',{name:'Expand setup details',exact:true}).click();expect(await page.getByRole('button',{name:'Collapse setup details',exact:true}).getAttribute('aria-expanded')).toBe('true');expect(await page.locator('.fsi-details').isVisible()).toBe(true);
  await page.getByRole('button',{name:'Continue on provider site',exact:true}).click();expect(await page.evaluate(()=>window.providerOpens)).toEqual(['https://exa.ai/docs/get-started/exa-mcp']);expect(await page.locator('.fsi-status').textContent()).toBe('Continue sign-in on the provider page. Return here when it is finished.');expect(await page.locator('.fsi-card').textContent()).not.toContain('verified');await screenshot(page,'compact-light.png');
 }finally{await close();}
},30000);
test('submission clears visible secrets immediately; dismiss aborts and ignores a late completion',async()=>{
 const {page,close}=await visit();try{
  await page.evaluate(()=>{window.mountFixture({onSubmit:(values,{signal})=>{window.pendingValues=values;window.pendingSignal=signal;return new Promise(resolve=>{window.finish=resolve;});}});});
  await page.getByLabel('API key',{exact:true}).fill('fixture-secret-canary');await page.getByRole('button',{name:'Show API key',exact:true}).click();await page.getByRole('button',{name:'Store credential',exact:true}).click();
  expect(await page.getByLabel('API key',{exact:true}).inputValue()).toBe('');expect(await page.getByLabel('API key',{exact:true}).getAttribute('type')).toBe('password');expect(await page.locator('.fsi-card').textContent()).not.toContain('fixture-secret-canary');
  await page.getByRole('button',{name:'Not now',exact:true}).click();expect(await page.evaluate(()=>window.pendingSignal.aborted)).toBe(true);expect(await page.evaluate(()=>window.pendingValues.api_key)).toBe('');await page.evaluate(()=>window.finish());expect(await page.locator('.fsi-card').getAttribute('data-state')).toBe('paused');expect(await page.locator('.fsi-status').textContent()).toContain('may still finish');expect(await page.getByRole('button',{name:'Expand setup details',exact:true}).getAttribute('aria-expanded')).toBe('false');
 }finally{await close();}
},30000);
test('disposal wipes pending values, removes the old card and prevents late success in its replacement',async()=>{
 const {page,close}=await visit();try{
  await page.evaluate(()=>window.mountFixture({onSubmit:(values,{signal})=>{window.pendingValues=values;window.pendingSignal=signal;return new Promise(resolve=>{window.finish=resolve;});}}));await page.getByLabel('API key',{exact:true}).fill('fixture-disposal-canary');await page.getByRole('button',{name:'Store credential',exact:true}).click();
  await page.evaluate(()=>{window.previousInput=document.querySelector('input');window.mountFixture({title:'Connect Firecrawl',origin:'https://mcp.firecrawl.dev',reviewKey:'self:firecrawl:2'});});expect(await page.evaluate(()=>window.previousInput.value)).toBe('');expect(await page.evaluate(()=>window.pendingValues.api_key)).toBe('');expect(await page.evaluate(()=>window.pendingSignal.aborted)).toBe(true);await page.evaluate(()=>window.finish());expect(await page.getByRole('heading',{name:'Connect Firecrawl',exact:true}).count()).toBe(1);expect(await page.locator('.fsi-card').getAttribute('data-state')).toBe('ready');expect(await page.locator('.fsi-status').textContent()).toBe('');
 }finally{await close();}
},30000);
test('adapter errors never echo secret content and require a fresh owner read',async()=>{
 const {page,close}=await visit();try{
  await page.evaluate(()=>window.mountFixture({onSubmit:values=>{throw Error('Do not echo '+values.api_key);}}));await page.getByLabel('API key',{exact:true}).fill('fixture-error-canary');await page.getByRole('button',{name:'Store credential',exact:true}).click();expect(await page.locator('.fsi-card').getAttribute('data-state')).toBe('uncertain');expect(await page.locator('.fsi-status').textContent()).toContain('Read the current owner status');expect(await page.locator('body').textContent()).not.toContain('fixture-error-canary');expect(await page.getByRole('button',{name:'Store credential',exact:true}).isDisabled()).toBe(true);expect(await page.evaluate(()=>localStorage.length+sessionStorage.length)).toBe(0);
 }finally{await close();}
},30000);
test('the compact card keeps paired contrast and fits the real workbench host at a narrow width',async()=>{
 const {page,close}=await visit('/card',{viewport:{width:360,height:800},colorScheme:'dark'});try{
  expect(await page.evaluate(()=>document.documentElement.scrollWidth<=window.innerWidth)).toBe(true);
  const colors=await page.locator('.fsi-card').evaluate(node=>({text:getComputedStyle(node).color,background:getComputedStyle(node).backgroundColor}));expect(colors).toEqual({text:'rgb(241, 243, 237)',background:'rgb(32, 35, 31)'});await screenshot(page,'compact-dark-mobile.png');
 }finally{await close();}
},30000);
test('generic native adapters retain declared text and secret fields and the reviewed callback identity',async()=>{
 const {page,close}=await visit();try{
  await page.evaluate(()=>window.mountFixture({origin:'https://provider.example',title:'Existing native enrollment',reviewKey:'native-enrollment:work:7',fields:[{name:'handle',label:'Account handle',kind:'text',required:true,maxLength:64},{name:'value',label:'Native declared credential',kind:'secret',required:true,maxLength:128}],onSubmit:(values,{reviewKey})=>{window.reviewedSubmission={values:{...values},reviewKey};}}));
  expect(await page.locator('.fsi-origin').textContent()).toBe('https://provider.example');expect(await page.getByLabel('Account handle',{exact:true}).getAttribute('type')).toBe('text');expect(await page.getByLabel('Native declared credential',{exact:true}).getAttribute('type')).toBe('password');
  await page.getByLabel('Account handle',{exact:true}).fill('fixture-account');await page.getByLabel('Native declared credential',{exact:true}).fill('fixture-declared-value');await page.getByRole('button',{name:'Store credential',exact:true}).click();
  expect(await page.evaluate(()=>window.reviewedSubmission)).toEqual({values:{handle:'fixture-account',value:'fixture-declared-value'},reviewKey:'native-enrollment:work:7'});expect(await page.getByLabel('Account handle',{exact:true}).inputValue()).toBe('');expect(await page.getByLabel('Native declared credential',{exact:true}).inputValue()).toBe('');expect(await page.locator('.fsi-card').getAttribute('data-state')).toBe('stored');expect(await page.locator('.fsi-status').textContent()).toContain('Provider access still needs verification.');
 }finally{await close();}
},30000);
test('workbench exposes only its native API-key declaration and does not open a provider automatically',async()=>{
 requests=[];stored.clear();statusMode='unavailable';const {page,close}=await visit('/workbench');try{
  await connections(page);expect(await page.getByLabel('API key',{exact:true}).count()).toBe(0);expect(await page.evaluate(()=>window.providerOpens)).toEqual([]);
  statusMode='wrong-field';await page.getByRole('button',{name:'Read current connection status',exact:true}).click();await page.getByText('Exa: provider declaration unavailable in the native credential owner',{exact:false}).waitFor();expect(await page.getByLabel('API key',{exact:true}).count()).toBe(0);
  statusMode='ready';await page.getByRole('button',{name:'Read current connection status',exact:true}).click();await page.getByLabel('API key',{exact:true}).waitFor();expect(await page.locator('.fsi-origin').textContent()).toBe('https://mcp.exa.ai');expect(await page.locator('.fsi-destination').textContent()).toBe('Your workspace · self');
  await page.getByRole('button',{name:'Open setup at exa.ai',exact:true}).click();expect(await page.evaluate(()=>window.providerOpens)).toEqual([{url:'https://exa.ai/docs/get-started/exa-mcp',target:'_blank',features:'noopener,noreferrer'}]);expect(await page.locator('.fsi-status').textContent()).toBe('Setup instructions were requested in a new tab. Provider access has not been tested.');expect(requests.filter(row=>row.operation==='connect')).toHaveLength(0);
 }finally{statusMode='ready';await close();}
},30000);
test('a read-only native connection declaration never presents a key input',async()=>{
 requests=[];stored.clear();statusMode='read-only';const {page,close}=await visit('/workbench');try{
  await connections(page);expect(await page.getByLabel('API key',{exact:true}).count()).toBe(0);expect(await page.getByRole('button',{name:'Store key in native credential owner',exact:true}).count()).toBe(0);expect(requests.filter(row=>['connect','disconnect'].includes(row.operation))).toHaveLength(0);
 }finally{statusMode='ready';await close();}
},30000);
test('workbench clears on provider and destination changes, and emits only the confirmed destination context',async()=>{
 requests=[];stored.clear();const {page,close}=await visit('/workbench');try{
  await connections(page);await page.getByLabel('API key',{exact:true}).fill('fixture-provider-canary');await page.evaluate(()=>{window.oldInput=document.querySelector('.fsi-card input');});await page.getByLabel('Connection provider',{exact:true}).selectOption('firecrawl');expect(await page.evaluate(()=>window.oldInput.value)).toBe('');expect(await page.locator('.fsi-origin').textContent()).toBe('https://mcp.firecrawl.dev');expect(await page.getByLabel('API key',{exact:true}).inputValue()).toBe('');
  await page.getByLabel('API key',{exact:true}).fill('fixture-destination-canary');await page.evaluate(()=>{window.oldInput=document.querySelector('.fsi-card input');});await page.getByLabel('Research destination',{exact:true}).selectOption('institution:fixture');await page.getByText('Research destination changed.',{exact:true}).waitFor();expect(await page.evaluate(()=>window.oldInput.value)).toBe('');expect(await page.getByLabel('API key',{exact:true}).inputValue()).toBe('');expect(await page.locator('.fsi-destination').textContent()).toBe('Fixture institution · institution:fixture');expect(await page.evaluate(()=>window.observedEvents)).toEqual([{type:'bonfyre:workspace-changed',detail:{context:'institution:fixture'}}]);
  await page.getByLabel('API key',{exact:true}).fill('fixture-native-key');await page.getByRole('button',{name:'Store key in native credential owner',exact:true}).click();await page.getByText('Native credential owner confirmed the stored field. No provider request was made.',{exact:true}).waitFor();const calls=requests.filter(row=>row.operation==='connect');expect(calls).toHaveLength(1);expect(calls[0]).toEqual({operation:'connect',context:'institution:fixture',method:'POST',payload:{provider:'firecrawl',secret:'fixture-native-key',binding:binding('institution:fixture')},accept:'application/msgpack',type:'application/msgpack'});expect(await page.getByLabel('API key',{exact:true}).inputValue()).toBe('');expect(await page.locator('.fsi-status').textContent()).toContain('Provider access has not been verified');expect(await page.evaluate(()=>window.observedEvents)).toEqual([{type:'bonfyre:workspace-changed',detail:{context:'institution:fixture'}},{type:'bonfyre:work-changed',detail:{context:'institution:fixture'}}]);
  const options=await page.evaluate(()=>window.fetchOptions.find(row=>row.url.includes('operation=connect')));expect(options.credentials).toBe('same-origin');expect(options.redirect).toBe('error');expect(options.cache).toBe('no-store');await screenshot(page,'workbench-connected.png');
 }finally{await close();}
},30000);
test('unconfirmed connection replies stay uncertain without echo or replay; an explicit status read recovers',async()=>{
 requests=[];stored.clear();const {page,close}=await visit('/workbench');const pending=gate();pending.fail=true;try{
  await connections(page);connectionGate=pending;await page.getByLabel('API key',{exact:true}).fill('fixture-lost-ack-canary');await page.getByRole('button',{name:'Store key in native credential owner',exact:true}).click();await pending.entered.promise;expect(await page.getByLabel('API key',{exact:true}).inputValue()).toBe('');pending.release.resolve();await page.locator('.fsi-card[data-state="uncertain"]').waitFor();expect(await page.locator('body').textContent()).not.toContain('fixture-lost-ack-canary');expect(await page.getByRole('button',{name:'Store key in native credential owner',exact:true}).isDisabled()).toBe(true);expect(requests.filter(row=>row.operation==='connect')).toHaveLength(1);expect(await page.evaluate(()=>window.observedEvents)).toEqual([]);
  const before=requests.filter(row=>row.operation==='status').length;await page.getByLabel('Connection provider',{exact:true}).selectOption('firecrawl');await page.getByLabel('Connection provider',{exact:true}).selectOption('exa');await page.getByLabel('Provider',{exact:true}).selectOption('firecrawl');expect(await page.getByLabel('API key',{exact:true}).count()).toBe(0);expect(await page.getByRole('button',{name:'Store key in native credential owner',exact:true}).count()).toBe(0);expect(requests.filter(row=>row.operation==='status')).toHaveLength(before);expect(await page.getByText('The previous credential outcome is not confirmed.',{exact:false}).count()).toBe(1);
  await page.getByRole('button',{name:'Read current connection status',exact:true}).click();await page.getByText('A key is stored for this destination. Provider access has not been verified.',{exact:true}).waitFor();expect(requests.filter(row=>row.operation==='connect')).toHaveLength(1);expect(await page.getByLabel('API key',{exact:true}).inputValue()).toBe('');
 }finally{connectionGate=null;pending.release.resolve();await close();}
},30000);
test('native account replacement clears drafts and waits for current access before creating a new entry',async()=>{
 requests=[];stored.clear();const {page,close}=await visit('/workbench?navigation');try{
  await connections(page);await page.getByLabel('API key',{exact:true}).fill('fixture-account-canary');await page.getByLabel('Purpose',{exact:true}).fill('Private fixture draft');const before=requests.filter(row=>row.operation==='status').length;
  await page.evaluate(()=>{window.oldInput=document.querySelector('.fsi-card input');window.oldPurpose=document.querySelector('textarea');const node=document.createElement('span');node.className='ak-session';node.dataset.sessionState='checking';document.querySelector('[data-account-slot]').replaceChildren(node);});
  await page.getByText('Read current account access before reopening research.',{exact:true}).waitFor();expect(await page.evaluate(()=>window.oldInput.value)).toBe('');expect(await page.evaluate(()=>window.oldPurpose.value)).toBe('');expect(await page.getByLabel('API key',{exact:true}).count()).toBe(0);expect(requests.filter(row=>row.operation==='status')).toHaveLength(before);expect(requests.filter(row=>row.operation==='connect')).toHaveLength(0);
  await page.evaluate(()=>{const node=document.createElement('span');node.className='ak-session';node.dataset.sessionState='signed-in';document.querySelector('[data-account-slot]').replaceChildren(node);});await page.getByText('Choose a provider and save a research plan.',{exact:true}).waitFor();await connections(page);expect(requests.filter(row=>row.operation==='status').length).toBeGreaterThan(before);expect(await page.getByLabel('API key',{exact:true}).inputValue()).toBe('');expect(await page.getByLabel('Purpose',{exact:true}).inputValue()).toBe('');expect(await page.evaluate(()=>window.providerOpens)).toEqual([]);
 }finally{await close();}
},30000);
test('an unconfirmed clear remains unavailable across provider rerenders until native status is reread',async()=>{
 requests=[];stored.clear();stored.set('self:exa',true);const {page,close}=await visit('/workbench'),pending=gate();pending.fail=true;try{
  await connections(page);await page.getByRole('button',{name:'Expand setup details',exact:true}).click();connectionGate=pending;await page.getByRole('button',{name:'Clear stored provider key',exact:true}).click();await pending.entered.promise;pending.release.resolve();await page.getByText('Fixture acknowledgement lost.',{exact:true}).waitFor();expect(await page.getByRole('button',{name:'Store key in native credential owner',exact:true}).isDisabled()).toBe(true);
  await page.getByLabel('Connection provider',{exact:true}).selectOption('firecrawl');await page.getByLabel('Connection provider',{exact:true}).selectOption('exa');expect(await page.getByLabel('API key',{exact:true}).count()).toBe(0);expect(requests.filter(row=>row.operation==='disconnect')).toHaveLength(1);
  await page.getByRole('button',{name:'Read current connection status',exact:true}).click();await page.getByText('No key is stored for this provider in this destination.',{exact:true}).waitFor();expect(await page.getByLabel('API key',{exact:true}).inputValue()).toBe('');expect(requests.filter(row=>row.operation==='disconnect')).toHaveLength(1);
 }finally{connectionGate=null;pending.release.resolve();await close();}
},30000);
test('pagehide clears credential entry and bfcache restoration must reread the current native destination',async()=>{
 requests=[];stored.clear();const {page,close}=await visit('/workbench');try{
  await connections(page);await page.getByLabel('API key',{exact:true}).fill('fixture-bfcache-canary');const before=requests.filter(row=>row.operation==='status').length;await page.evaluate(()=>{window.oldInput=document.querySelector('.fsi-card input');window.dispatchEvent(new PageTransitionEvent('pagehide',{persisted:true}));});expect(await page.evaluate(()=>window.oldInput.value)).toBe('');expect(await page.getByLabel('API key',{exact:true}).count()).toBe(0);
  await page.evaluate(()=>window.dispatchEvent(new PageTransitionEvent('pageshow',{persisted:true})));await page.getByText('Choose a provider and save a research plan.',{exact:true}).waitFor();await connections(page);expect(requests.filter(row=>row.operation==='status').length).toBeGreaterThan(before);expect(await page.getByLabel('API key',{exact:true}).inputValue()).toBe('');expect(requests.filter(row=>row.operation==='connect')).toHaveLength(0);expect(await page.evaluate(()=>window.observedEvents)).toEqual([]);
 }finally{await close();}
},30000);
test('disposing the workbench clears a pending credential and suppresses its late receipt and event',async()=>{
 requests=[];stored.clear();const {page,close}=await visit('/workbench'),pending=gate();try{
  await connections(page);connectionGate=pending;await page.getByLabel('API key',{exact:true}).fill('fixture-pending-canary');await page.getByRole('button',{name:'Store key in native credential owner',exact:true}).click();await pending.entered.promise;await page.evaluate(()=>{window.oldInput=document.querySelector('.fsi-card input');window.fixtureWorkbench.destroy();});expect(await page.evaluate(()=>window.oldInput.value)).toBe('');pending.release.resolve();await page.evaluate(()=>new Promise(resolve=>setTimeout(resolve,80)));expect(await page.locator('[data-web-content]').evaluate(node=>node.childElementCount)).toBe(0);expect(await page.evaluate(()=>window.observedEvents)).toEqual([]);expect(requests.filter(row=>row.operation==='connect')).toHaveLength(1);
 }finally{connectionGate=null;pending.release.resolve();await close();}
},30000);
