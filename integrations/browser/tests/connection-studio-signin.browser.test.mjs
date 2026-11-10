// Real Connection Studio and shared card in Chromium with bounded native-protocol
// fixtures. This suite does not use a real account, provider, extension or key.
import {test,expect,beforeAll,afterAll,beforeEach} from 'bun:test';
import {readFile,mkdir} from 'node:fs/promises';
import {resolve} from 'node:path';
import {encode,decode} from '../workspace-msgpack.mjs';

const repository=resolve(import.meta.dir,'../../..');
const binding={actor:'user:1',subject:'user:1',scope:'self'};
const accounts={gmail:'fixture@gmail.example',proton:'fixture@proton.example'};
const allStages=['existing_access_checked','account_bound','scope_confirmed','credential_sealed','credential_verified','consumer_bound'];
const entry={id:'201',name:'Fixture Provider',url:'https://provider.example/setup',category:'Research',gate:'portal',note:'Continue this saved provider setup.',handoff:'Open provider setup',recovery:false,recoveryScopes:[]};
const deferred=()=>{let resolve;const promise=new Promise(done=>resolve=done);return {promise,resolve};};
const gate=()=>({entered:deferred(),release:deferred()});
let browser,server,requests,lanes,progress,readGate,changeGate,refuseReads;
const packet=value=>new Response(encode(value),{headers:{'Content-Type':'application/msgpack','Cache-Control':'no-store'}});
function setup(identity){
 const state=progress[identity],workId='201-'+identity;
 return {binding,identity,entry,item:{work_id:workId,identity,account:accounts[identity],signup_url:entry.url,platform:entry.name},revision:state.revision,
  review:{owner:'BonfyreEnrollment',context:'self',identity,workId,jobId:(identity==='gmail'?'a':'b').repeat(64),revision:state.revision+1,binding},
  // AccountConnections returns an inspection even after continuation. Running
  // describes the queue item; an inspect response never conveys a held lease.
  status:state.status,read_only:true,resumable:['ready','running'].includes(state.status),providerReady:true,identityBrowser:identity==='proton'?'incognito':'regular',
  credential_fields:[{field:'api_key',label:'API key',sensitive:true,required:true,help:'Fixture declared API credential.'}],credential_fields_available:true,
  stages:state.sealed?{credential_sealed:{owner:'BonfyreCredentials',evidence_kind:'native_seal'}}:{},missing:allStages.filter(stage=>stage!=='credential_sealed'||!state.sealed),notes:[]};
}
function plan(identity){return {binding,identity,identities:lanes.map(id=>({id,label:id==='gmail'?'Gmail · regular window':'Proton · private window',account:accounts[id]})),catalog:[entry],items:[{id:entry.id,workId:entry.id+'-'+identity,revision:progress[identity].revision,status:progress[identity].status}],sourceLinks:[],prepared:1,totalTargets:1,missing:[]};}
beforeEach(()=>{requests=[];lanes=['gmail','proton'];progress={gmail:{revision:0,status:'ready',sealed:false},proton:{revision:0,status:'ready',sealed:false}};readGate=null;changeGate=null;refuseReads=false;});
beforeAll(async()=>{
 const {chromium}=await import(process.env.BONFYRE_PLAYWRIGHT_MODULE||'@playwright/test');
 browser=await chromium.launch({headless:true,args:['--no-sandbox'],...(process.env.BONFYRE_CHROMIUM_BINARY?{executablePath:process.env.BONFYRE_CHROMIUM_BINARY}:{})});
 const assets=new Set(['source-desk.mjs','source-publication.mjs','workspace-destinations.mjs','authentication-capability-surface.mjs','provider-auth-capability.mjs','connection-studio.mjs','connection-studio.css','connection-source.mjs','connection-lab.mjs','interaction-collection.mjs','workspace-store.mjs','workspace-value.mjs','workspace-continuation.mjs','workspace-msgpack.mjs','publication-picker.mjs','publication-research.mjs','provider-contract.mjs','extension/foreign-sign-in.mjs','extension/foreign-sign-in.css','extension/panel-enrollment.mjs','extension/enrollment-integrity.mjs']);
 server=Bun.serve({hostname:'127.0.0.1',port:0,async fetch(request){
  const url=new URL(request.url);
  if(url.pathname==='/studio')return new Response('<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><link rel="stylesheet" href="/apps/connection-studio.css"></head><body><main id="fixture-studio" class="cs-workspace"></main><script type="module">import {mountConnectionStudio} from "/apps/connection-studio.mjs";const root=document.getElementById("fixture-studio");root.dataset.connectionStudio="";window.fixtureStudio=mountConnectionStudio(root);</script></body></html>',{headers:{'Content-Type':'text/html'}});
  if(url.pathname==='/panel')return new Response('<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head><body><main id="enroll"></main><p id="enroll-said" role="status"></p><script type="module" src="/apps/extension/panel-enrollment.mjs"></script></body></html>',{headers:{'Content-Type':'text/html'}});
  if(url.pathname==='/apps/_connections'){
   const operation=url.searchParams.get('operation'),identity=url.searchParams.get('identity');
   const body=request.method==='POST'?decode(new Uint8Array(await request.arrayBuffer()))[1]:null;
   requests.push({operation,identity,body,method:request.method});
   if(refuseReads||url.searchParams.get('context')!=='self'||!lanes.includes(identity))return new Response('Fixture access refused',{status:403});
   if(readGate&&request.method==='GET'){const wait=readGate;readGate=null;wait.entered.resolve();await wait.release.promise;}
   if(operation==='list')return packet([1,true,plan(identity)]);
   if(operation==='get')return packet([1,true,setup(identity)]);
   if(operation==='change'){
    if(JSON.stringify(body.review)!==JSON.stringify(setup(identity).review))return packet([1,false,{code:'version_conflict',message:'The reviewed fixture setup changed.'}]);
    if(changeGate){const wait=changeGate;changeGate=null;wait.entered.resolve();await wait.release.promise;if(wait.fail)return packet([1,false,{code:'unconfirmed',message:'The credential owner did not confirm this change. Read the setup again.'}]);}
    if(body.action==='continue')progress[identity].status='running';
    else if(body.action==='seal'){progress[identity].sealed=true;progress[identity].revision++;}
    else return packet([1,false,{code:'invalid_request',message:'Fixture action unsupported.'}]);
    return packet([1,true,setup(identity)]);
   }
   return new Response('Unsupported fixture operation',{status:400});
  }
  const asset=url.pathname.startsWith('/apps/')?url.pathname.slice(6):'';
  if(assets.has(asset))return new Response(await readFile(resolve(process.env.BONFYRE_TEST_PUBLIC_BROWSER||resolve(repository,'integrations/browser'),asset)),{headers:{'Content-Type':asset.endsWith('.css')?'text/css':'text/javascript'}});
  return new Response('Fixture route not found',{status:404});
 }});
});
afterAll(async()=>{readGate?.release.resolve();changeGate?.release.resolve();server?.stop(true);await browser?.close();});
async function visit({companion=true,browserContext='regular',presentationScope='adapter',deferCapabilities=false,clipboard='allow',viewport={width:1280,height:900}}={}){
 const context=await browser.newContext({viewport});
 await context.route('**/*',route=>new URL(route.request().url()).origin===server.url.origin?route.continue():route.abort());
 const page=await context.newPage(),errors=[];page.setDefaultTimeout(7000);page.on('pageerror',error=>errors.push(error.message));
 await page.addInitScript(({companion,browserContext,presentationScope,deferCapabilities,clipboard})=>{
  window.providerContinuations=[];window.nativeClicks=[];window.capabilityReplies=[];window.copiedProviderLinks=[];
  Object.defineProperty(navigator,'clipboard',{configurable:true,value:{async writeText(value){if(clipboard==='deny')throw Error('Fixture clipboard refusal');window.copiedProviderLinks.push(value);}}});
  document.addEventListener('click',event=>{const button=event.target.closest?.('[data-native-provider-continue]');if(button)window.nativeClicks.push({trusted:event.isTrusted,context:button.dataset.nativeProviderContext,jobId:button.dataset.nativeProviderJob,revision:button.dataset.nativeProviderRevision});},true);
  window.addEventListener('aurekai:provider:request',event=>{
   const {requestId,operation,review}=event.detail;
   const reply=result=>window.dispatchEvent(new CustomEvent('aurekai:provider:reply',{detail:{requestId,ok:true,result}}));
   if(operation==='capabilities'){const result={available:companion,modes:companion?['provider_tab_sidepanel']:[],...(browserContext===null?{}:{browserContext}),...(presentationScope===null?{}:{presentationScope})};if(deferCapabilities)window.capabilityReplies.push(()=>reply(result));else queueMicrotask(()=>reply(result));}
   else if(operation==='continue'){window.providerContinuations.push({review,active:navigator.userActivation.isActive});if(window.deferProviderContinuation)return;if(window.declineProviderContinuation){queueMicrotask(()=>window.dispatchEvent(new CustomEvent('aurekai:provider:reply',{detail:{requestId,ok:false,reason:'The fixture companion could not confirm this operation.'}})));return;}queueMicrotask(()=>reply({state:'provider_focused',presentation:'provider_tab_sidepanel',companion:'open',binding:review.binding,work:{id:review.jobId,revision:review.revision},provider:{id:'enrollment-201',name:'Fixture Provider'}}));}
   else if(operation==='clear')reply({cleared:true,workUnchanged:true});
  });
 },{companion,browserContext,presentationScope,deferCapabilities,clipboard});
 await page.goto(server.url.origin+'/studio');await page.waitForFunction(()=>window.fixtureStudio?.model.snapshot.plan);
 return {page,errors,close:()=>context.close()};
}
async function inspect(page){await page.locator('.ac-title').click();await page.locator('.fsi-card').waitFor();await page.waitForFunction(()=>!window.fixtureStudio.model.snapshot.loading);}
async function expand(page){await page.locator('.fsi-card').getByRole('button',{name:'Expand setup details',exact:true}).click();}
async function snapshot(page,name){if(process.env.BONFYRE_TEST_ARTIFACT_DIR){await mkdir(process.env.BONFYRE_TEST_ARTIFACT_DIR,{recursive:true});await page.screenshot({path:resolve(process.env.BONFYRE_TEST_ARTIFACT_DIR,name),fullPage:true});const card=page.locator('.fsi-card');if(await card.isVisible())await card.screenshot({path:resolve(process.env.BONFYRE_TEST_ARTIFACT_DIR,name.replace('.png','-card.jpg')),type:'jpeg',quality:45});}}
async function visitPanel(){
 const context=await browser.newContext({viewport:{width:420,height:900}});await context.route('**/*',route=>new URL(route.request().url()).origin===server.url.origin?route.continue():route.abort());
 const page=await context.newPage(),errors=[];page.setDefaultTimeout(7000);page.on('pageerror',error=>errors.push(error.message));
 const view=setup('gmail');view.item.credential_provider='fixture-provider';
 await page.addInitScript(({view})=>{
  const stored={'enroll-view-gmail':{work_id:view.item.work_id},'enroll-tab-gmail':{id:10,work_id:view.item.work_id,window_id:1}};
  const tab={id:10,windowId:1,incognito:false,url:'https://provider.example/consent/unfinished'};window.panelNativeCalls=[];window.panelTabChanges=[];
  // Full Chrome exposes a non-configurable, writable property. Preserve its flags.
  Object.defineProperty(window,'chrome',{value:{extension:{inIncognitoContext:false},windows:{getCurrent:async()=>({id:1,type:'normal',incognito:false})},storage:{session:{get:async key=>({[key]:stored[key]}),set:async values=>Object.assign(stored,values),remove:async key=>delete stored[key]}},tabs:{get:async id=>{if(id!==tab.id)throw Error('Fixture tab missing.');return {...tab};},update:async(id,values)=>{window.panelTabChanges.push({id,...values});return Object.assign(tab,values);},create:async()=>{throw Error('The retained fixture tab must be reused.');},remove:async()=>{},onReplaced:{addListener(){}}}}});
  window.aurekaiAsk=async(operation,more)=>{
   window.panelNativeCalls.push({operation,...more});
   if(operation==='enroll_inspect')return {ok:true,result:{...structuredClone(view),read_only:true}};
   if(operation==='enroll_continue'&&more.identity===view.identity&&more.expected?.work_id===view.item.work_id&&more.expected.revision===view.revision&&more.confirmed===true)return {ok:true,result:{...structuredClone(view),read_only:false,status:'running',how:'continued'}};
   if(operation==='enroll_seal')return {ok:false,reason:'The fixture native enrollment hold expired.'};
   throw Error('The fixture admits only inspection, reviewed continuation and an expired-hold refusal.');
  };
 },{view});
 try{await page.goto(server.url.origin+'/panel');await page.getByRole('button',{name:'Continue native enrollment',exact:true}).click();await page.locator('.fsi-card').waitFor();return {page,errors,close:()=>context.close()};}
 catch(error){const detail=errors.slice(0,4).map(value=>value.slice(0,300)).join('; ');await context.close();throw Error('Companion fixture setup failed: '+String(error.message).slice(0,500)+(detail?' Page errors: '+detail:''));}
}

test('account choices use admitted identities and primary continues the exact saved setup',async()=>{
 lanes=['gmail'];const {page,errors,close}=await visit();try{
  expect(await page.getByLabel('Provider identity',{exact:true}).locator('option').allTextContents()).toEqual(['Gmail · regular window · fixture@gmail.example']);
  expect(await page.getByLabel('Provider identity',{exact:true}).isDisabled()).toBe(true);await inspect(page);
  const card=page.locator('.fsi-card');expect(await card.locator('.fsi-account-label').textContent()).toBe(accounts.gmail);expect(await card.textContent()).toContain('Account for this setup');expect(await card.textContent()).not.toContain('matching login');
  expect(await page.getByLabel('API key',{exact:true}).count()).toBe(0);expect(requests.filter(r=>r.method==='POST')).toHaveLength(0);
  const before=setup('gmail').review;await card.getByRole('button',{name:'Use saved account',exact:true}).click();await page.waitForFunction(()=>window.fixtureStudio.model.snapshot.detail.status==='running');
  expect(requests.filter(r=>r.method==='POST').map(r=>r.body)).toEqual([{action:'continue',review:before}]);expect(await page.evaluate(()=>window.providerContinuations)).toEqual([]);
  expect(await page.getByLabel('API key',{exact:true}).isVisible()).toBe(false);await expand(page);expect(await page.getByLabel('API key',{exact:true}).isVisible()).toBe(true);expect(errors).toEqual([]);
 }finally{await close();}
},30000);

test('takeover carries native button coordinates in the same real click and keeps the reviewed work',async()=>{
 const {page,errors,close}=await visit();try{
  await inspect(page);const review=setup('gmail').review,takeover=page.getByRole('button',{name:'Take over to sign in',exact:true});await page.waitForFunction(()=>{const button=document.querySelector('.fsi-takeover');return button&&!button.disabled;});
  expect(await takeover.getAttribute('data-native-provider-job')).toBe(review.jobId);await takeover.click();
  expect(await page.evaluate(()=>window.providerContinuations)).toEqual([{review,active:true}]);expect(await page.evaluate(()=>window.nativeClicks)).toEqual([{trusted:true,context:'self',jobId:review.jobId,revision:String(review.revision)}]);
  expect(requests.filter(r=>r.method==='POST')).toHaveLength(0);expect(await page.locator('.fsi-status').textContent()).toContain('Continue sign-in on the provider page');expect(await page.getByRole('button',{name:'Open with browser companion',exact:true}).count()).toBe(0);expect(errors).toEqual([]);await snapshot(page,'connection-studio-account-card.png');
 }finally{await close();}
},30000);

test('without the companion, copying the reviewed provider link preserves manual window choice and only rereads saved work',async()=>{
 const {page,errors,close}=await visit({companion:false,viewport:{width:390,height:844}});try{
  await inspect(page);await page.getByText('The browser companion is unavailable. Copy the provider link and open it in a regular window.',{exact:true}).waitFor();const url=page.getByRole('textbox',{name:'Reviewed provider link',exact:true});expect(await url.inputValue()).toBe(entry.url);expect(await url.getAttribute('readonly')).not.toBeNull();expect(await page.getByRole('button',{name:'Take over to sign in',exact:true}).isVisible()).toBe(false);expect(await page.locator('.fsi-card a[target="_blank"]').count()).toBe(0);const bounds=await page.locator('.fsi-card').boundingBox();expect(bounds.x).toBeGreaterThanOrEqual(0);expect(bounds.x+bounds.width).toBeLessThanOrEqual(390);expect(bounds.y).toBeGreaterThanOrEqual(0);expect(bounds.y+bounds.height).toBeLessThanOrEqual(844);await snapshot(page,'connection-studio-card-mobile.png');
  const pages=page.context().pages().length;await page.getByRole('button',{name:'Copy provider link',exact:true}).click();await page.getByRole('heading',{name:'Continue in a regular window',exact:true}).waitFor();expect(await page.locator('.fsi-card').getAttribute('data-state')).toBe('manual');expect(page.context().pages().length).toBe(pages);expect(await page.evaluate(()=>window.copiedProviderLinks)).toEqual([entry.url]);
  expect(await page.evaluate(()=>window.providerContinuations)).toEqual([]);expect(requests.filter(r=>r.method==='POST')).toHaveLength(0);const before=requests.filter(r=>r.operation==='get').length;
  await page.getByRole('button',{name:'Check setup',exact:true}).click();await page.getByText('Saved setup refreshed. This does not confirm a provider login.',{exact:true}).waitFor();expect(requests.filter(r=>r.operation==='get')).toHaveLength(before+1);expect(progress.gmail.status).toBe('ready');expect(requests.filter(r=>r.method==='POST')).toHaveLength(0);expect(errors).toEqual([]);
 }finally{await close();}
},30000);

test('companion takeover requires the assigned regular or private browser context',async()=>{
 for(const fixture of [{browserContext:'regular',identity:'gmail',matching:true},{browserContext:'regular',identity:'proton',matching:false},{browserContext:'incognito',identity:'proton',matching:true},{browserContext:'incognito',identity:'gmail',matching:false}]){
  const {page,errors,close}=await visit({browserContext:fixture.browserContext});try{
   await inspect(page);if(fixture.identity==='proton'){await page.getByLabel('Provider identity',{exact:true}).selectOption('proton');await page.locator('.fsi-account-label').filter({hasText:accounts.proton}).waitFor();}
   const takeover=page.getByRole('button',{name:'Take over to sign in',exact:true});
   if(fixture.matching){await page.waitForFunction(()=>{const button=document.querySelector('.fsi-takeover');return button&&!button.disabled&&!button.hidden;});expect(await page.getByRole('region',{name:'Manual provider sign-in',exact:true}).isVisible()).toBe(false);await takeover.click();expect(await page.evaluate(()=>window.providerContinuations.map(row=>row.review.identity))).toEqual([fixture.identity]);}
   else{const windowName=fixture.identity==='proton'?'private window':'regular window';await page.waitForFunction(()=>{const text=document.querySelector('.fsi-status')?.textContent;return text&&!text.startsWith('Checking the browser companion');});expect(await takeover.isVisible()).toBe(false);await page.getByText('This setup needs a '+windowName+'. Open Aurekai there with the companion enabled, or copy the provider link below.',{exact:true}).waitFor();expect(await page.getByRole('button',{name:'Copy provider link',exact:true}).isEnabled()).toBe(true);expect(await page.getByRole('region',{name:'Manual provider sign-in',exact:true}).textContent()).toContain('Open a '+windowName);expect(await page.evaluate(()=>window.providerContinuations)).toEqual([]);expect(await page.evaluate(()=>window.nativeClicks)).toEqual([]);}
   expect(requests.filter(row=>row.method==='POST')).toHaveLength(0);expect(errors).toEqual([]);
  }finally{await close();}
 }
},30000);

test('missing or older companion context offers explicit manual window guidance for each account',async()=>{
 for(const fixture of [{companion:false,identity:'proton'},{companion:true,identity:'gmail'},{companion:true,identity:'proton'}]){
  const {page,errors,close}=await visit({companion:fixture.companion,browserContext:null});try{
   await inspect(page);if(fixture.identity==='proton'){await page.getByLabel('Provider identity',{exact:true}).selectOption('proton');await page.locator('.fsi-account-label').filter({hasText:accounts.proton}).waitFor();}
   const windowName=fixture.identity==='proton'?'private window':'regular window',prefix=fixture.companion?'The browser companion has not confirmed this window.':'The browser companion is unavailable.';await page.getByText(prefix+' Copy the provider link and open it in a '+windowName+'.',{exact:true}).waitFor();
   expect(await page.getByRole('button',{name:'Take over to sign in',exact:true}).isVisible()).toBe(false);expect(await page.getByRole('textbox',{name:'Reviewed provider link',exact:true}).inputValue()).toBe(entry.url);await page.getByRole('button',{name:'Copy provider link',exact:true}).click();await page.getByRole('heading',{name:'Continue in a '+windowName,exact:true}).waitFor();expect(await page.getByRole('button',{name:'Check setup',exact:true}).isEnabled()).toBe(true);expect(await page.evaluate(()=>window.copiedProviderLinks)).toEqual([entry.url]);expect(await page.evaluate(()=>window.providerContinuations)).toEqual([]);expect(requests.filter(row=>row.method==='POST')).toHaveLength(0);expect(errors).toEqual([]);
  }finally{await close();}
 }
},30000);

test('an older companion with the correct window still needs its current presentation protocol',async()=>{
 const {page,errors,close}=await visit({browserContext:'regular',presentationScope:null});try{
  await inspect(page);await page.getByText('Reload the Aurekai browser companion to use takeover. You can also copy the provider link for a regular window.',{exact:true}).waitFor();expect(await page.getByRole('button',{name:'Take over to sign in',exact:true}).isVisible()).toBe(false);expect(await page.getByRole('button',{name:'Copy provider link',exact:true}).isEnabled()).toBe(true);await page.getByRole('button',{name:'Copy provider link',exact:true}).click();await page.getByRole('button',{name:'Check setup',exact:true}).waitFor();expect(await page.evaluate(()=>window.copiedProviderLinks)).toEqual([entry.url]);expect(await page.evaluate(()=>window.providerContinuations)).toEqual([]);expect(await page.evaluate(()=>window.nativeClicks)).toEqual([]);expect(requests.filter(row=>row.method==='POST')).toHaveLength(0);expect(errors).toEqual([]);
 }finally{await close();}
},30000);

test('a refused clipboard write leaves a selectable reviewed link without mutation uncertainty',async()=>{
 const {page,errors,close}=await visit({companion:false,clipboard:'deny'});try{
  await inspect(page);await page.getByRole('button',{name:'Copy provider link',exact:true}).click();await page.getByText('Copy was unavailable. The provider link is selected so you can copy it yourself.',{exact:true}).waitFor();const field=page.getByRole('textbox',{name:'Reviewed provider link',exact:true});expect(await field.evaluate(input=>({start:input.selectionStart,end:input.selectionEnd,value:input.value}))).toEqual({start:0,end:entry.url.length,value:entry.url});
  expect(await page.locator('.fsi-card').getAttribute('data-state')).toBe('manual');expect(await page.evaluate(()=>window.fixtureStudio.model.snapshot.uncertain)).toBe(false);expect(await page.getByRole('button',{name:'Check setup',exact:true}).isEnabled()).toBe(true);expect(await page.getByRole('button',{name:'Choose account or sign-in option',exact:true}).isEnabled()).toBe(true);await page.getByRole('button',{name:'Select link',exact:true}).click();expect(await field.evaluate(input=>input===document.activeElement&&input.selectionStart===0&&input.selectionEnd===input.value.length)).toBe(true);expect(await page.evaluate(()=>window.copiedProviderLinks)).toEqual([]);expect(await page.evaluate(()=>window.providerContinuations)).toEqual([]);expect(requests.filter(row=>row.method==='POST')).toHaveLength(0);expect(errors).toEqual([]);
 }finally{await close();}
},30000);

test('account switching clears manual links and ignores a late capability for the old account',async()=>{
 const {page,errors,close}=await visit({browserContext:'regular',deferCapabilities:true});const pending=gate();try{
  await inspect(page);expect(await page.getByRole('textbox',{name:'Reviewed provider link',exact:true}).inputValue()).toBe(entry.url);readGate=pending;await page.getByLabel('Provider identity',{exact:true}).selectOption('proton');await pending.entered.promise;expect(await page.locator('.fsi-card').count()).toBe(0);expect(await page.getByRole('textbox',{name:'Reviewed provider link',exact:true}).count()).toBe(0);pending.release.resolve();await page.locator('.fsi-account-label').filter({hasText:accounts.proton}).waitFor();await page.waitForFunction(()=>window.capabilityReplies.length===2);await page.evaluate(()=>window.capabilityReplies[1]());await page.getByText('This setup needs a private window. Open Aurekai there with the companion enabled, or copy the provider link below.',{exact:true}).waitFor();
  await page.evaluate(()=>window.capabilityReplies[0]());await page.evaluate(()=>new Promise(done=>requestAnimationFrame(()=>requestAnimationFrame(done))));expect(await page.getByRole('button',{name:'Take over to sign in',exact:true}).isVisible()).toBe(false);expect(await page.getByRole('region',{name:'Manual provider sign-in',exact:true}).textContent()).toContain('Open a private window');await page.getByRole('button',{name:'Copy provider link',exact:true}).click();await page.getByRole('heading',{name:'Continue in a private window',exact:true}).waitFor();expect(await page.evaluate(()=>window.providerContinuations)).toEqual([]);expect(requests.filter(row=>row.method==='POST')).toHaveLength(0);expect(errors).toEqual([]);
 }finally{pending.release.resolve();await close();}
},30000);

test('identity switching clears private entry and waits for the newly admitted provider review',async()=>{
 progress.gmail.status='running';const {page,errors,close}=await visit(),pending=gate();try{
  await inspect(page);await expand(page);await page.getByLabel('API key',{exact:true}).fill('fixture-identity-secret');await page.evaluate(()=>{window.oldCredential=document.querySelector('.fsi-card .fsi-form input');});
  readGate=pending;await page.getByLabel('Provider identity',{exact:true}).selectOption('proton');await pending.entered.promise;
  expect(await page.evaluate(()=>window.oldCredential.value)).toBe('');expect(await page.locator('.fsi-card').count()).toBe(0);expect(await page.getByLabel('Provider identity',{exact:true}).locator('option').allTextContents()).toEqual(['Reading assigned accounts…']);expect(await page.getByLabel('Provider identity',{exact:true}).isDisabled()).toBe(true);
  pending.release.resolve();await page.locator('.fsi-account-label').filter({hasText:accounts.proton}).waitFor();expect(await page.getByRole('button',{name:'Take over to sign in',exact:true,includeHidden:true}).getAttribute('data-native-provider-job')).toBe('b'.repeat(64));expect(await page.locator('body').textContent()).not.toContain('fixture-identity-secret');expect(requests.filter(r=>r.method==='POST')).toHaveLength(0);expect(errors).toEqual([]);
 }finally{readGate=null;pending.release.resolve();await close();}
},30000);

test('Not now retains work and reopens through a fresh owner read without claiming or canceling it',async()=>{
 progress.gmail.status='running';const {page,errors,close}=await visit();try{
  await inspect(page);await expand(page);await page.getByLabel('API key',{exact:true}).fill('fixture-dismiss-secret');await page.evaluate(()=>{window.oldCredential=document.querySelector('.fsi-card .fsi-form input');});
  const before=requests.filter(r=>r.operation==='get').length;await page.getByRole('button',{name:'Not now',exact:true}).click();expect(await page.evaluate(()=>window.oldCredential.value)).toBe('');expect(await page.locator('.fsi-card').isVisible()).toBe(false);expect(await page.getByRole('button',{name:'Reopen sign-in card',exact:true}).isVisible()).toBe(true);
  await page.getByRole('button',{name:'Reopen sign-in card',exact:true}).click();await page.locator('.fsi-card').waitFor({state:'visible'});expect(requests.filter(r=>r.operation==='get')).toHaveLength(before+1);expect(requests.filter(r=>r.method==='POST')).toHaveLength(0);expect(await page.getByLabel('API key',{exact:true}).inputValue()).toBe('');expect(await page.locator('.fsi-account-label').textContent()).toBe(accounts.gmail);expect(errors).toEqual([]);
 }finally{await close();}
},30000);

test('a pending credential is cleared immediately and an uncertain result cannot replay through the card',async()=>{
 progress.gmail.status='running';const {page,errors,close}=await visit(),pending=gate();pending.fail=true;try{
  await inspect(page);await expand(page);changeGate=pending;await page.getByLabel('API key',{exact:true}).fill('fixture-seal-secret');await page.getByRole('button',{name:'Store declared credential',exact:true}).click();await pending.entered.promise;
  expect(await page.getByLabel('API key',{exact:true}).inputValue()).toBe('');pending.release.resolve();await page.waitForFunction(()=>window.fixtureStudio.model.snapshot.uncertain&&document.querySelector('.fsi-card')?.getAttribute('aria-busy')==='false');expect(await page.getByRole('button',{name:'Store declared credential',exact:true}).isDisabled()).toBe(true);expect(await page.getByRole('button',{name:'Check setup',exact:true}).isEnabled()).toBe(true);expect(await page.locator('body').textContent()).not.toContain('fixture-seal-secret');
  const writes=requests.filter(r=>r.method==='POST');expect(writes).toHaveLength(1);expect(writes[0].body.review).toEqual(setup('gmail').review);expect(writes[0].body.field).toBe('api_key');
  await page.getByRole('button',{name:'Not now',exact:true}).click();await page.getByRole('button',{name:'Reopen sign-in card',exact:true}).click();await page.waitForFunction(()=>!window.fixtureStudio.model.snapshot.uncertain);expect(requests.filter(r=>r.method==='POST')).toHaveLength(1);expect(await page.getByLabel('API key',{exact:true}).inputValue()).toBe('');expect(errors).toEqual([]);
 }finally{changeGate=null;pending.release.resolve();await close();}
},30000);

test('fresh access refusal clears the account chooser and the provider card',async()=>{
 progress.gmail.status='running';const {page,errors,close}=await visit();try{
  await inspect(page);await expand(page);await page.getByLabel('API key',{exact:true}).fill('fixture-refusal-secret');await page.evaluate(()=>{window.oldCredential=document.querySelector('.fsi-card .fsi-form input');});refuseReads=true;
  await page.getByRole('button',{name:'Refresh native state',exact:true}).click();await page.waitForFunction(()=>window.fixtureStudio.model.snapshot.locked);expect(await page.evaluate(()=>window.oldCredential.value)).toBe('');expect(await page.locator('.fsi-card').count()).toBe(0);expect(await page.getByLabel('Provider identity',{exact:true}).locator('option').allTextContents()).toEqual(['No account plan available']);expect(await page.getByLabel('Provider identity',{exact:true}).isDisabled()).toBe(true);expect(requests.filter(r=>r.method==='POST')).toHaveLength(0);expect(errors).toEqual([]);
 }finally{refuseReads=false;await close();}
},30000);

test('the companion shares the assigned account card and focuses its retained provider form',async()=>{
 progress.gmail.status='running';const {page,errors,close}=await visitPanel();try{
  expect(await page.locator('.fsi-account-label').textContent()).toBe(accounts.gmail);expect(await page.locator('.fsi-account-note').textContent()).toContain('Assigned identity');expect(await page.getByLabel('API key',{exact:true}).count()).toBe(0);expect(await page.evaluate(()=>window.panelTabChanges)).toEqual([]);
  await page.getByRole('button',{name:'Take over to sign in',exact:true}).click();await page.waitForFunction(()=>window.panelTabChanges.length===1);expect(await page.evaluate(()=>window.panelTabChanges)).toEqual([{id:10,active:true}]);expect(await page.evaluate(()=>window.panelNativeCalls.map(call=>call.operation))).toEqual(['enroll_inspect','enroll_inspect']);
  await page.getByRole('button',{name:'Use this account',exact:true}).click();await page.waitForFunction(()=>document.querySelector('.fsi-form input')!==null);
  await expand(page);await page.getByLabel('API key',{exact:true}).fill('fixture-panel-secret');await page.evaluate(()=>{window.oldPanelInput=document.querySelector('.fsi-card .fsi-form input');});await page.getByRole('button',{name:'Not now',exact:true}).click();expect(await page.evaluate(()=>window.oldPanelInput.value)).toBe('');expect(await page.locator('.fsi-card').getAttribute('data-state')).toBe('paused');
  await page.getByRole('button',{name:'Reload native item',exact:true}).click();await page.waitForFunction(()=>document.querySelector('.fsi-card')?.dataset.state==='ready');expect(await page.getByLabel('API key',{exact:true}).count()).toBe(0);expect(await page.evaluate(()=>window.panelNativeCalls.map(call=>call.operation))).toEqual(['enroll_inspect','enroll_inspect','enroll_continue','enroll_inspect']);expect(errors).toEqual([]);
 }finally{await close();}
},30000);

test('only a successful fresh owner read unlocks an uncertain credential at the same checkpoint',async()=>{
 progress.gmail.status='running';const {page,errors,close}=await visit(),pending=gate(),reread=gate();pending.fail=true;
 page.setDefaultTimeout(7000);
 const failedRead=route=>new URL(route.request().url()).searchParams.get('operation')==='get'?route.fulfill({status:503,body:'Fixture owner read unavailable'}):route.continue();
 try{
  await inspect(page);await expand(page);changeGate=pending;
  await page.getByLabel('API key',{exact:true}).fill('fixture-reconcile-secret');await page.getByRole('button',{name:'Store declared credential',exact:true}).click();await pending.entered.promise;pending.release.resolve();
  await page.waitForFunction(()=>window.fixtureStudio.model.snapshot.uncertain&&document.querySelector('.fsi-card')?.dataset.state==='uncertain');
  const before=setup('gmail').review,reads=requests.filter(r=>r.operation==='get').length;
  expect(await page.locator('.fsi-card').getAttribute('data-expanded')).toBe('true');expect(await page.getByRole('button',{name:'Check setup',exact:true}).isEnabled()).toBe(true);await snapshot(page,'connection-studio-uncertain-expanded.png');
  await page.route('**/apps/_connections?*',failedRead);await page.getByRole('button',{name:'Check setup',exact:true}).click();
  await page.waitForFunction(()=>!window.fixtureStudio.model.snapshot.loading&&window.fixtureStudio.model.snapshot.error);
  expect(await page.locator('.fsi-card').getAttribute('data-state')).toBe('uncertain');expect(await page.getByRole('button',{name:'Store declared credential',exact:true}).isDisabled()).toBe(true);expect(await page.getByLabel('API key',{exact:true}).inputValue()).toBe('');
  await page.unroute('**/apps/_connections?*',failedRead);readGate=reread;await page.getByRole('button',{name:'Check setup',exact:true}).click();await reread.entered.promise;
  expect(await page.getByRole('button',{name:'Check setup',exact:true}).isDisabled()).toBe(true);expect(await page.getByRole('button',{name:'Store declared credential',exact:true}).isDisabled()).toBe(true);reread.release.resolve();
  await page.waitForFunction(()=>!window.fixtureStudio.model.snapshot.loading&&!window.fixtureStudio.model.snapshot.uncertain&&document.querySelector('.fsi-card')?.dataset.state==='ready');
  expect(await page.evaluate(()=>window.fixtureStudio.model.snapshot.detail.review)).toEqual(before);expect(requests.filter(r=>r.operation==='get')).toHaveLength(reads+1);expect(requests.filter(r=>r.method==='POST')).toHaveLength(1);
  await expand(page);expect(await page.getByRole('button',{name:'Store declared credential',exact:true}).isDisabled()).toBe(false);expect(await page.getByLabel('API key',{exact:true}).inputValue()).toBe('');expect(errors).toEqual([]);
 }finally{changeGate=null;readGate=null;pending.release.resolve();reread.release.resolve();await close();}
},30000);

test('Check setup recovers a failed takeover at the same checkpoint without replaying it',async()=>{
 const {page,errors,close}=await visit();try{
  page.setDefaultTimeout(7000);
  await inspect(page);await page.waitForFunction(()=>!document.querySelector('.fsi-takeover').disabled);
  await page.evaluate(()=>{window.declineProviderContinuation=true;});await page.getByRole('button',{name:'Take over to sign in',exact:true}).click();
  await page.waitForFunction(()=>document.querySelector('.fsi-card')?.dataset.state==='uncertain');
  const before=setup('gmail').review,reads=requests.filter(r=>r.operation==='get').length;
  expect(await page.getByRole('button',{name:'Take over to sign in',exact:true}).isDisabled()).toBe(true);expect(await page.evaluate(()=>window.providerContinuations)).toHaveLength(1);
  await page.getByRole('button',{name:'Check setup',exact:true}).click();
  await page.waitForFunction(()=>!window.fixtureStudio.model.snapshot.loading&&document.querySelector('.fsi-card')?.dataset.state==='ready'&&!document.querySelector('.fsi-takeover').disabled);
  expect(await page.evaluate(()=>window.fixtureStudio.model.snapshot.detail.review)).toEqual(before);expect(requests.filter(r=>r.operation==='get')).toHaveLength(reads+1);expect(requests.filter(r=>r.method==='POST')).toHaveLength(0);expect(await page.evaluate(()=>window.providerContinuations)).toHaveLength(1);
  await page.evaluate(()=>{window.declineProviderContinuation=false;});await page.getByRole('button',{name:'Take over to sign in',exact:true}).click();
  await page.waitForFunction(()=>document.querySelector('.fsi-card')?.dataset.state==='provider-page');expect(await page.evaluate(()=>window.providerContinuations)).toHaveLength(2);expect(errors).toEqual([]);
 }finally{await close();}
},30000);

test('an expired enrollment hold clears the secret and requires inspection before another explicit continuation',async()=>{
 progress.gmail.status='running';const {page,errors,close}=await visitPanel();try{
  await page.getByRole('button',{name:'Use this account',exact:true}).click();await page.waitForFunction(()=>document.querySelector('.fsi-form input')!==null);await expand(page);
  await page.getByLabel('API key',{exact:true}).fill('fixture-expired-hold-secret');await page.evaluate(()=>{window.expiredHoldInput=document.querySelector('.fsi-card .fsi-form input');});
  await page.getByRole('button',{name:'Seal this credential',exact:true}).click();
  await page.getByText('The last result is stale or uncertain. Reload reads this exact setup; it does not replay the change.',{exact:true}).waitFor();
  expect(await page.evaluate(()=>window.expiredHoldInput.value)).toBe('');expect(await page.getByRole('button',{name:'Seal this credential',exact:true}).isDisabled()).toBe(true);
  expect(await page.evaluate(()=>window.panelNativeCalls.filter(call=>call.operation==='enroll_seal').length)).toBe(1);
  await page.getByRole('button',{name:'Reload native item',exact:true}).click();await page.waitForFunction(()=>document.querySelector('.fsi-card')?.dataset.state==='ready'&&document.querySelector('.fsi-form input')===null);
  expect(await page.getByRole('button',{name:'Use this account',exact:true}).isDisabled()).toBe(false);expect(await page.evaluate(()=>window.panelNativeCalls.filter(call=>call.operation==='enroll_seal').length)).toBe(1);
  await page.getByRole('button',{name:'Use this account',exact:true}).click();await page.waitForFunction(()=>document.querySelector('.fsi-form input')!==null);
  expect(await page.getByLabel('API key',{exact:true}).isVisible()).toBe(true);expect(await page.getByLabel('API key',{exact:true}).inputValue()).toBe('');expect(await page.getByRole('button',{name:'Seal this credential',exact:true}).isDisabled()).toBe(false);expect(await page.evaluate(()=>window.panelNativeCalls.map(call=>call.operation))).toEqual(['enroll_inspect','enroll_continue','enroll_seal','enroll_inspect','enroll_continue']);expect(errors).toEqual([]);
 }finally{await close();}
},30000);

for(const side of [false,true])test('Not now closes a pending credential card in the '+(side?'side view':'full view')+' without replaying or cancelling its write',async()=>{
 progress.gmail.status='running';const {page,errors,close}=await visit(),pending=gate();
 try{
  page.setDefaultTimeout(7000);await inspect(page);if(side)await page.getByRole('button',{name:'Open side view',exact:true}).click();await expand(page);changeGate=pending;
  await page.getByLabel('API key',{exact:true}).fill('fixture-pending-dismiss-secret');await page.evaluate(()=>{window.pendingCredentialInput=document.querySelector('.fsi-card .fsi-form input');});
  await page.getByRole('button',{name:'Store declared credential',exact:true}).click();await pending.entered.promise;
  expect(await page.evaluate(()=>document.querySelector('.fsi-dismiss').closest('[inert]')===null)).toBe(true);expect(await page.evaluate(()=>window.pendingCredentialInput.value)).toBe('');
  expect(await page.getByRole('button',{name:'Store declared credential',exact:true}).isDisabled()).toBe(true);
  await page.getByRole('button',{name:'Not now',exact:true}).click();expect(await page.locator('.fsi-card').isVisible()).toBe(false);expect(await page.evaluate(()=>window.fixtureStudio.model.snapshot.busy)).toBe(true);expect(requests.filter(r=>r.method==='POST')).toHaveLength(1);
  pending.release.resolve();await page.waitForFunction(()=>!window.fixtureStudio.model.snapshot.busy&&window.fixtureStudio.model.snapshot.detail.revision===1);
  expect(await page.locator('.fsi-card').isVisible()).toBe(false);expect(await page.evaluate(()=>window.providerContinuations)).toEqual([]);expect(requests.filter(r=>r.method==='POST')).toHaveLength(1);
  const reads=requests.filter(r=>r.operation==='get').length;await page.getByRole('button',{name:'Reopen sign-in card',exact:true}).click();await page.locator('.fsi-card').waitFor({state:'visible'});await expand(page);
  expect(requests.filter(r=>r.operation==='get')).toHaveLength(reads+1);expect(await page.getByLabel('API key',{exact:true}).inputValue()).toBe('');expect(await page.evaluate(()=>window.fixtureStudio.model.snapshot.detail.missing.includes('credential_sealed'))).toBe(false);expect(requests.filter(r=>r.method==='POST')).toHaveLength(1);expect(errors).toEqual([]);
 }finally{changeGate=null;pending.release.resolve();await close();}
},30000);

test('leaving and returning to Connections cannot clear a rejected takeover without an owner read',async()=>{
 const {page,errors,close}=await visit();try{
  page.setDefaultTimeout(7000);await inspect(page);await page.waitForFunction(()=>!document.querySelector('.fsi-takeover').disabled);
  await page.evaluate(()=>{window.declineProviderContinuation=true;});await page.getByRole('button',{name:'Take over to sign in',exact:true}).click();await page.waitForFunction(()=>document.querySelector('.fsi-card')?.dataset.state==='uncertain');
  const reads=requests.filter(r=>r.operation==='get').length;
  expect(await page.getByRole('button',{name:'Open side view',exact:true}).isDisabled()).toBe(true);await page.getByRole('tab',{name:'Explore sources',exact:true}).click();expect(await page.locator('.fsi-card').count()).toBe(0);await page.getByRole('tab',{name:'Connections',exact:true}).click();
  expect(await page.getByRole('button',{name:'Take over to sign in',exact:true}).isDisabled()).toBe(true);expect(await page.getByRole('button',{name:'Check setup',exact:true}).isEnabled()).toBe(true);expect(await page.locator('.fsi-status').textContent()).toBe('The previous result was not confirmed. Check setup before trying again.');
  expect(requests.filter(r=>r.operation==='get')).toHaveLength(reads);expect(await page.evaluate(()=>window.providerContinuations)).toHaveLength(1);
  await page.getByRole('button',{name:'Check setup',exact:true}).click();await page.waitForFunction(()=>!window.fixtureStudio.model.snapshot.loading&&!document.querySelector('.fsi-takeover').disabled);
  expect(requests.filter(r=>r.operation==='get')).toHaveLength(reads+1);expect(await page.evaluate(()=>window.providerContinuations)).toHaveLength(1);expect(requests.filter(r=>r.method==='POST')).toHaveLength(0);expect(errors).toEqual([]);
 }finally{await close();}
},30000);

test('bottom account menu switches only the assigned lane after rereading it',async()=>{
 const {page,errors,close}=await visit();try{await inspect(page);const dock=page.locator('.cs-signin-dock');expect(await dock.isVisible()).toBe(true);const box=await dock.boundingBox();expect(box.y+box.height).toBeLessThanOrEqual(900);expect(box.y+box.height).toBeGreaterThan(850);
  const trigger=page.getByRole('button',{name:'Choose account or sign-in option',exact:true});await trigger.click();const choices=page.getByRole('menuitemradio');expect(await choices.count()).toBe(2);expect(await choices.nth(0).getAttribute('aria-checked')).toBe('true');
  await page.keyboard.press('ArrowDown');expect(await choices.nth(1).evaluate(n=>n===document.activeElement)).toBe(true);await page.keyboard.press('Escape');expect(await trigger.evaluate(n=>n===document.activeElement)).toBe(true);expect(await page.locator('.fsi-account-menu').isVisible()).toBe(false);
  await trigger.click();await page.locator('[data-account-choice="proton"]').click();await page.locator('.fsi-account-label').filter({hasText:accounts.proton}).waitFor();expect(requests.filter(r=>r.method==='POST')).toHaveLength(0);expect(requests.some(r=>r.operation==='get'&&r.identity==='proton')).toBe(true);expect(await page.getByRole('button',{name:'Use saved account',exact:true}).isEnabled()).toBe(true);expect(errors).toEqual([]);await snapshot(page,'saved-account-menu-studio.png');
 }finally{await close();}
},30000);

test('return from provider rereads the same setup without claiming success or changing work',async()=>{
 const {page,errors,close}=await visit();try{await inspect(page);await page.waitForFunction(()=>!document.querySelector('.fsi-takeover').disabled);await page.getByRole('button',{name:'Take over to sign in',exact:true}).click();await page.getByRole('heading',{name:'Finished signing in?',exact:true}).waitFor();
  const before=requests.filter(r=>r.operation==='get').length;await page.getByRole('button',{name:'Check setup',exact:true}).click();await page.getByText('Saved setup refreshed. This does not confirm a provider login.',{exact:true}).waitFor();expect(requests.filter(r=>r.operation==='get')).toHaveLength(before+1);expect(requests.filter(r=>r.method==='POST')).toHaveLength(0);expect(progress.gmail.status).toBe('ready');expect(errors).toEqual([]);
 }finally{await close();}
},30000);

test('phone dock and its account menu remain inside the viewport and dismissal clears the menu',async()=>{
 const {page,errors,close}=await visit({viewport:{width:390,height:844}});try{await inspect(page);const dock=page.locator('.cs-signin-dock'),b=await dock.boundingBox();expect(b.x).toBeGreaterThanOrEqual(0);expect(b.x+b.width).toBeLessThanOrEqual(390);expect(b.y).toBeGreaterThanOrEqual(0);expect(b.y+b.height).toBeLessThanOrEqual(844);
  await page.getByRole('button',{name:'Choose account or sign-in option',exact:true}).click();const menu=page.getByRole('menu',{name:'Saved setup accounts'}),m=await menu.boundingBox();expect(m.x).toBeGreaterThanOrEqual(0);expect(m.x+m.width).toBeLessThanOrEqual(390);expect(m.y).toBeGreaterThanOrEqual(0);expect(m.y+m.height).toBeLessThanOrEqual(844);await snapshot(page,'saved-account-menu-phone.png');
  await page.keyboard.press('Escape');await page.getByRole('button',{name:'Not now',exact:true}).click();expect(await dock.isVisible()).toBe(false);expect(await menu.isVisible()).toBe(false);expect(await page.getByRole('button',{name:'Reopen sign-in card',exact:true}).isVisible()).toBe(true);expect(requests.filter(r=>r.method==='POST')).toHaveLength(0);expect(errors).toEqual([]);
 }finally{await close();}
},30000);

test('leaving and returning to Connections during a pending takeover requires a fresh read before retry',async()=>{
 const {page,errors,close}=await visit();try{
  page.setDefaultTimeout(7000);await inspect(page);await page.waitForFunction(()=>!document.querySelector('.fsi-takeover').disabled);
  await page.evaluate(()=>{window.deferProviderContinuation=true;});await page.getByRole('button',{name:'Take over to sign in',exact:true}).click();await page.waitForFunction(()=>document.querySelector('.fsi-card')?.getAttribute('aria-busy')==='true');
  const reads=requests.filter(r=>r.operation==='get').length;expect(await page.getByRole('button',{name:'Open side view',exact:true}).isDisabled()).toBe(true);await page.getByRole('tab',{name:'Explore sources',exact:true}).click();expect(await page.locator('.fsi-card').count()).toBe(0);await page.getByRole('tab',{name:'Connections',exact:true}).click();
  expect(await page.getByRole('button',{name:'Take over to sign in',exact:true}).isDisabled()).toBe(true);expect(await page.getByRole('button',{name:'Choose account or sign-in option',exact:true}).isDisabled()).toBe(true);expect(await page.getByRole('button',{name:'Check setup',exact:true}).isEnabled()).toBe(true);expect(await page.locator('.fsi-status').textContent()).toBe('The previous result was not confirmed. Check setup before trying again.');expect(requests.filter(r=>r.operation==='get')).toHaveLength(reads);expect(await page.evaluate(()=>window.providerContinuations)).toHaveLength(1);
  await page.getByRole('button',{name:'Check setup',exact:true}).click();await page.waitForFunction(()=>!window.fixtureStudio.model.snapshot.loading&&!document.querySelector('.fsi-takeover').disabled);
  expect(requests.filter(r=>r.operation==='get')).toHaveLength(reads+1);expect(await page.evaluate(()=>window.providerContinuations)).toHaveLength(1);expect(requests.filter(r=>r.method==='POST')).toHaveLength(0);expect(errors).toEqual([]);
 }finally{await close();}
},30000);

test('side view account chooser is interactive and Not now remains available during pending storage',async()=>{
 progress.gmail.status='running';const {page,errors,close}=await visit(),pending=gate();try{await inspect(page);await page.getByRole('button',{name:'Open side view',exact:true}).click();await page.locator('.cs-drawer[open]').waitFor();expect(await page.locator('.cs-signin-dock').isVisible()).toBe(false);
  await page.getByRole('button',{name:'Choose account or sign-in option',exact:true}).click();expect(await page.getByRole('menuitemradio').count()).toBe(2);await page.keyboard.press('Escape');await expand(page);await page.getByLabel('API key',{exact:true}).fill('side-fixture-secret');changeGate=pending;await page.getByRole('button',{name:'Store declared credential',exact:true}).click();await pending.entered.promise;
  expect(await page.getByRole('button',{name:'Not now',exact:true}).isEnabled()).toBe(true);await page.getByRole('button',{name:'Not now',exact:true}).click();expect(await page.locator('.fsi-card').isVisible()).toBe(false);pending.release.resolve();await page.waitForFunction(()=>!window.fixtureStudio.model.snapshot.busy);expect(requests.filter(r=>r.method==='POST')).toHaveLength(1);expect(await page.locator('body').textContent()).not.toContain('side-fixture-secret');expect(errors).toEqual([]);
 }finally{changeGate=null;pending.release.resolve();await close();}
},30000);

test('account change removes the portaled menu and clears its old secret entry',async()=>{
 progress.gmail.status='running';const {page,errors,close}=await visit();try{await inspect(page);await expand(page);await page.getByLabel('API key',{exact:true}).fill('menu-account-change-secret');await page.evaluate(()=>window.secretBeforeAccountChange=document.querySelector('.fsi-card .fsi-form input'));await page.getByRole('button',{name:'Choose account or sign-in option',exact:true}).click();
  await page.evaluate(()=>window.dispatchEvent(new Event('bonfyre:account-changed')));expect(await page.locator('.fsi-account-menu,.fsi-card').count()).toBe(0);expect(await page.locator('.cs-signin-dock').isVisible()).toBe(false);expect(await page.evaluate(()=>window.secretBeforeAccountChange.value)).toBe('');expect(requests.filter(r=>r.method==='POST')).toHaveLength(0);expect(errors).toEqual([]);
 }finally{await close();}
},30000);

test('an unavailable private companion never falls back into the regular provider session',async()=>{
 const {page,errors,close}=await visit({companion:false});try{await inspect(page);await page.getByRole('button',{name:'Choose account or sign-in option',exact:true}).click();await page.locator('[data-account-choice="proton"]').click();await page.locator('.fsi-account-label').filter({hasText:accounts.proton}).waitFor();await page.getByText('The browser companion is unavailable. Copy the provider link and open it in a private window.',{exact:true}).waitFor();
  expect(await page.getByRole('link',{name:'Take over to sign in',exact:true}).count()).toBe(0);expect(await page.getByRole('button',{name:'Take over to sign in',exact:true,includeHidden:true}).isDisabled()).toBe(true);expect(await page.getByRole('button',{name:'Copy provider link',exact:true}).isVisible()).toBe(true);expect(await page.getByRole('textbox',{name:'Reviewed provider link',exact:true}).inputValue()).toBe(entry.url);expect(await page.locator('.fsi-card a[target="_blank"]').count()).toBe(0);expect(requests.filter(r=>r.method==='POST')).toHaveLength(0);expect(await page.evaluate(()=>window.providerContinuations)).toEqual([]);expect(errors).toEqual([]);
 }finally{await close();}
},30000);

test('manual sign-in at 320px keeps its provider link and recovery footer reachable',async()=>{
 const {page,errors,close}=await visit({companion:false,viewport:{width:320,height:640}});try{
  await inspect(page);expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);await page.getByRole('button',{name:'Copy provider link',exact:true}).click();const check=page.getByRole('button',{name:'Check setup',exact:true}),bounds=await check.boundingBox();expect(bounds.y).toBeGreaterThanOrEqual(0);expect(bounds.y+bounds.height).toBeLessThanOrEqual(640);const reads=requests.filter(row=>row.operation==='get').length;await check.click();await page.getByText('Saved setup refreshed. This does not confirm a provider login.',{exact:true}).waitFor();expect(requests.filter(row=>row.operation==='get')).toHaveLength(reads+1);await page.getByRole('button',{name:'Not now',exact:true}).click();expect(await page.locator('.cs-signin-dock').isVisible()).toBe(false);expect(errors).toEqual([]);
 }finally{await close();}
},30000);

test('an expanded bottom card at 320px keeps both its fields and footer reachable',async()=>{
 progress.gmail.status='running';const {page,errors,close}=await visit({viewport:{width:320,height:640}});try{await inspect(page);await expand(page);await page.getByLabel('API key',{exact:true}).fill('narrow-fixture-key');expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);await page.getByRole('button',{name:'Not now',exact:true}).click();expect(await page.locator('.cs-signin-dock').isVisible()).toBe(false);expect(errors).toEqual([]);
 }finally{await close();}
},30000);

test('a dismissed pending takeover stays hidden through navigation and failed rereads before a fresh owner read reopens it',async()=>{
 const {page,errors,close}=await visit(),reread=gate();
 const failedRead=route=>new URL(route.request().url()).searchParams.get('operation')==='get'?route.fulfill({status:503,body:'Fixture owner unavailable'}):route.continue();
 try{
  await inspect(page);await page.waitForFunction(()=>!document.querySelector('button.fsi-takeover').disabled);
  await page.evaluate(()=>{window.deferProviderContinuation=true;window.dismissedReview=window.fixtureStudio.model.snapshot.detail;});await page.getByRole('button',{name:'Take over to sign in',exact:true}).click();await page.waitForFunction(()=>document.querySelector('.fsi-card')?.getAttribute('aria-busy')==='true');
  const reads=requests.filter(r=>r.operation==='get').length;await page.getByRole('button',{name:'Not now',exact:true}).click();expect(await page.locator('.fsi-card').isVisible()).toBe(false);
  await page.getByRole('tab',{name:'Explore sources',exact:true}).click();await page.getByRole('tab',{name:'Connections',exact:true}).click();
  expect(await page.locator('.fsi-card').isVisible()).toBe(false);expect(await page.locator('button.fsi-takeover').isDisabled()).toBe(true);expect(requests.filter(r=>r.operation==='get')).toHaveLength(reads);expect(await page.evaluate(()=>window.providerContinuations)).toHaveLength(1);
  await page.route('**/apps/_connections?*',failedRead);await page.getByRole('button',{name:'Reopen sign-in card',exact:true}).click();await page.waitForFunction(()=>!window.fixtureStudio.model.snapshot.loading&&window.fixtureStudio.model.snapshot.error);
  expect(await page.locator('.fsi-card').isVisible()).toBe(false);expect(await page.locator('button.fsi-takeover').isDisabled()).toBe(true);expect(await page.getByRole('button',{name:'Reopen sign-in card',exact:true}).isVisible()).toBe(true);expect(await page.evaluate(()=>window.fixtureStudio.model.snapshot.detail===window.dismissedReview)).toBe(true);expect(await page.evaluate(()=>window.providerContinuations)).toHaveLength(1);expect(requests.filter(r=>r.method==='POST')).toHaveLength(0);
  await page.unroute('**/apps/_connections?*',failedRead);readGate=reread;await page.getByRole('button',{name:'Reopen sign-in card',exact:true}).click();await reread.entered.promise;expect(await page.locator('.fsi-card').isVisible()).toBe(false);reread.release.resolve();
  await page.locator('.fsi-card').waitFor({state:'visible'});await page.waitForFunction(()=>!document.querySelector('button.fsi-takeover').disabled);
  expect(await page.evaluate(()=>window.fixtureStudio.model.snapshot.detail!==window.dismissedReview)).toBe(true);expect(await page.evaluate(()=>window.fixtureStudio.model.snapshot.detail.review)).toEqual(setup('gmail').review);expect(requests.filter(r=>r.operation==='get')).toHaveLength(reads+1);expect(await page.evaluate(()=>window.providerContinuations)).toHaveLength(1);expect(requests.filter(r=>r.method==='POST')).toHaveLength(0);expect(errors).toEqual([]);
 }finally{readGate=null;reread.release.resolve();await close();}
},30000);

test('Stitch native relay cards show actual plan standing and open existing provider inspector',async()=>{
 const {page,errors,close}=await visit();try{
  const cards=page.locator('.cs-relay-card');
  expect(await cards.count()).toBe(1);
  expect(await cards.locator('h3').textContent()).toBe('Fixture Provider');
  expect(await cards.locator('.cs-relay-state').textContent()).toBe('Not started');
  expect(await cards.locator('.cs-relay-lane').textContent()).toContain('gmail');
  expect(await cards.textContent()).not.toContain('Connected');
  expect(await cards.evaluate(el=>getComputedStyle(el).backgroundColor)).toBe('rgb(23, 29, 25)');
  await page.waitForFunction(()=>{const b=document.querySelector('.cs-relay-card button');return b&&!b.disabled;});
  await cards.getByRole('button',{name:'Inspect setup',exact:true}).click();
  await page.locator('.fsi-card').waitFor();
  expect(requests.some(r=>r.operation==='get'&&r.identity==='gmail')).toBe(true);
  expect(requests.filter(r=>r.method==='POST')).toHaveLength(0);
  expect(errors).toEqual([]);
  await snapshot(page,'stitch-connection-native-plan.png');
 }finally{await close();}
},30000);
