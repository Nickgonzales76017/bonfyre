import {test,expect,beforeAll,afterAll} from 'bun:test';
import {join} from 'node:path';
import {createRequire} from 'node:module';
import {mkdirSync} from 'node:fs';
import {gunzipSync} from 'node:zlib';
import {workspaceFixture,root} from '../../cmd/BonfyreWebAddress/tests/workspace_fixture.mjs';
import {materializeCatalogueAccountRepair,catalogueAccountRepairFiles,catalogueAccountBase} from '../materialize_catalogue_account_repair.mjs';
import {loadWorkspaceFiles} from '../workspace_material.mjs';
import {baseline,makeDraft,draftKey} from '../../components/BonfyreServiceSite/catalogue-model.mjs';
const packageRoot=process.env.BONFYRE_ROOM_BASE_ROOT,retained=packageRoot?test:test.skip;
let f,binding,browser,pack,files,config,sourceKey;
const prior=Object.fromEntries(['BONFYRE_WORKSPACE_RELEASE','BONFYRE_WORKSPACE_PACKAGES'].map(k=>[k,process.env[k]]));
beforeAll(async()=>{
 if(!packageRoot)return;
 pack=await materializeCatalogueAccountRepair({root:packageRoot});files=await loadWorkspaceFiles(packageRoot,pack.id);
 process.env.BONFYRE_WORKSPACE_PACKAGES=packageRoot;process.env.BONFYRE_WORKSPACE_RELEASE=pack.id;
 config=JSON.parse(files.get('catalogue/index.html').toString().match(/<script[^>]*data-catalogue-config[^>]*>([\s\S]*?)<\/script>/)[1]);
 const entry=config.entries.find(p=>p.store==='wayfair-reference'&&p.id==='W002337865');
 const product=JSON.parse(gunzipSync(files.get(pack.projection.prefix+entry.chunk))).find(p=>p.store===entry.store&&p.id===entry.id);
 const original=baseline(product),edit=structuredClone(original);edit.title='Private catalogue draft';edit.variants[0].price='79.99';sourceKey=draftKey(product);
 f=await workspaceFixture();binding=await f.install('catalogue','institution:workspace-team');await f.store(binding).saveLocal(sourceKey,makeDraft(product,original,edit),0);
 browser=await createRequire(join(root,'tools/creator-ingest/package.json'))('puppeteer').launch({headless:true,executablePath:'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',args:['--disable-background-networking']});
},60000);
afterAll(async()=>{await browser?.close();await f?.close();for(const[k,v]of Object.entries(prior))if(v===undefined)delete process.env[k];else process.env[k]=v;});
async function open(query,{actor=f.owner,width=1440,hold=null}={}){
 const context=await browser.createBrowserContext(),page=await context.newPage(),errors=[];page.setDefaultTimeout(20000);await page.setViewport({width,height:1000});page.on('pageerror',e=>errors.push(e.message));
 const[name,...v]=actor.cookie.split('=');await page.setCookie({name,value:v.join('='),url:f.base,httpOnly:true,sameSite:'Lax'});
 await page.setRequestInterception(true);page.on('request',r=>{if(new URL(r.url()).origin!==f.base)r.respond({status:204}).catch(()=>{});else r.continue().catch(()=>{});});
 await page.evaluateOnNewDocument(hold=>{
  const fetch=window.fetch.bind(window);window.writeCalls=0;let held=false;
  window.fetch=async(url,options)=>{
   const u=new URL(url,location.href),record=u.pathname.endsWith('/_records'),operation=u.searchParams.get('operation');
   if(record&&operation==='write')window.writeCalls++;
   const response=await fetch(url,options);
   if(!held&&record&&response.ok&&(hold==='write'&&operation==='write'||hold==='read'&&operation==='read'&&u.searchParams.get('prefix')==='draft:catalogue:')){
    held=true;window.heldResponse=true;await new Promise(resolve=>window.releaseHeld=resolve);
   }
   return response;
  };
 },hold);
 await page.goto(f.base+binding.path+'?'+query,{waitUntil:hold==='read'?'domcontentloaded':'networkidle0'});return{context,page,errors};
}
async function stage(page,title){await page.waitForSelector('[data-release=grid] .ag-row');await page.click('[data-release=grid] .ag-row .ag-selection-checkbox');await page.type('[data-release=name]',title);await page.click('[data-release=stage]');}
const notice=(page,text)=>page.waitForFunction(text=>document.querySelector('[data-release=notice]')?.textContent.includes(text),{},text);
async function proof(page,name){if(process.env.CATALOGUE_ACCOUNT_PROOF){mkdirSync(process.env.CATALOGUE_ACCOUNT_PROOF,{recursive:true});await page.screenshot({path:join(process.env.CATALOGUE_ACCOUNT_PROOF,name+'.png'),fullPage:true});}}
retained('lifetime projection keeps historical modules, native composition and every unrelated package byte',async()=>{
 const before=await loadWorkspaceFiles(packageRoot,catalogueAccountBase),expected=catalogueAccountRepairFiles(before);
 expect(pack.projection.files).toBe(44);expect(files.size-before.size).toBe(44);
 for(const[n,b]of before)if(n!=='catalogue/index.html')expect(files.get(n).equals(b)).toBe(true);
 expect(files.get(pack.projection.prefix+'catalogue-client.mjs').toString()).toContain('createCatalogueRoomDialog');
 expect(files.get(pack.projection.prefix+'catalogue-grid.mjs').toString()).toContain('composeCollectionWorkspace');
 expect(expected.projection).toEqual(pack.projection);
});
retained('product editing, native save and release review work before closure; private controls and continuation disappear on account change',async()=>{
 const{context,page,errors}=await open('store=wayfair-reference&workspace=products&product=W002337865');
 try{
  await page.waitForFunction(()=>document.querySelector('[data-ui=title]')?.value==='Private catalogue draft');
  await page.$eval('[data-ui=title]',n=>{n.value='Private revised catalogue draft';n.dispatchEvent(new Event('input',{bubbles:true}));});await page.click('[data-ui=save]');
  await page.waitForFunction(()=>document.querySelector('[data-ui=message]').textContent.startsWith('Draft saved.'));
  expect((await f.store(binding).readLocal(sourceKey))[0].revision).toBe(2);
  await page.click('[data-workspace-view=releases]');await stage(page,'Private release title');await notice(page,'Release staged.');
  await page.type('[data-release=note]','Private unsaved review note');
  await page.waitForSelector('[data-related-work]:not([hidden])');
  const key=new URL(page.url()).searchParams.get('release');expect(key).toMatch(/^release:catalogue:/);
  await page.evaluate(()=>{window.heldNote=document.querySelector('[data-release=note]');window.heldReview=document.querySelector('[data-release=review]');window.writesBefore=writeCalls;dispatchEvent(new Event('bonfyre:account-changed'));});
  await page.waitForSelector('[data-catalogue-workspace][data-closed=true]');
  expect(await page.evaluate(()=>({note:heldNote.value,disabled:heldReview.disabled,private:document.body.textContent.includes('Private'),related:!!document.querySelector('[data-related-work]')}))).toEqual({note:'',disabled:true,private:false,related:false});
  await page.evaluate(()=>{heldReview.click();dispatchEvent(new PopStateEvent('popstate'));});await page.waitForNetworkIdle({idleTime:100});
  expect(await page.evaluate(()=>writeCalls)).toBe(await page.evaluate(()=>writesBefore));expect((await f.store(binding).readLocal(key))[0].revision).toBe(1);
  await proof(page,'closed-1440');await page.setViewport({width:390,height:844});expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1)).toBe(true);await proof(page,'closed-390');expect(errors).toEqual([]);
 }finally{await context.close();}
},60000);
retained('native inventory and its separate room dialog close together without retaining private room choices',async()=>{
 const catalogue=config.fixture_catalogues.find(c=>c.id==='wayfair-reference');
 const plan={schema:'service-site.fixture-plan.v1',catalogue:catalogue.digest,id:'account-private-room',...structuredClone(catalogue.defaults),title:'Private retained room',items:[{key:'private-lamp',product:catalogue.products[0].id,x:1,y:1,rotation:0}]};
 await f.store(binding).saveLocal('fixture-plan:planner-wayfair-reference:'+plan.id,{plan,receipt:null},0);
 const{context,page,errors}=await open('store=wayfair-reference&workspace=inventory');
 try{
  await page.waitForSelector('[data-grid=table] .ag-row[row-id="product:W002337865"] [col-id=title]');await page.click('[data-grid=table] .ag-row[row-id="product:W002337865"] [col-id=title]');
  await page.waitForSelector('[data-grid-action=inspect-3d]',{visible:true}).catch(async error=>{await proof(page,'inventory-diagnostic');throw Error(error.message+'; errors='+JSON.stringify(errors)+'; inspector='+await page.$eval('[data-grid=inspector]',n=>n.outerHTML));});await page.click('[data-grid-action=inspect-3d]');
  await page.waitForFunction(()=>Array.from(document.querySelectorAll('.cr-dialog select option')).some(n=>n.textContent==='Private retained room')).catch(async error=>{throw Error(error.message+'; errors='+JSON.stringify(errors)+'; dialog='+await page.$eval('.cr-dialog',n=>n.textContent));});
  await page.evaluate(()=>{window.heldRoom=document.querySelector('.cr-dialog select[aria-label="Destination room"]');window.heldPlace=Array.from(document.querySelectorAll('.cr-dialog button')).find(n=>n.textContent==='Place in a room');window.writesBefore=writeCalls;dispatchEvent(new Event('bonfyre:account-changed'));});
  await page.waitForNetworkIdle({idleTime:200});
  expect(await page.$('.cr-dialog')).toBeNull();expect(await page.$('[data-grid=table]')).toBeNull();
  expect(await page.evaluate(()=>({text:heldRoom.textContent,disabled:heldPlace.disabled,private:document.body.textContent.includes('Private')}))).toEqual({text:'',disabled:true,private:false});
  await page.evaluate(()=>heldPlace.click());expect(await page.evaluate(()=>writeCalls)).toBe(await page.evaluate(()=>writesBefore));expect(errors).toEqual([]);
 }finally{await context.close();}
},40000);
retained('delayed initial private read cannot repopulate a closed editor or continuation',async()=>{
 const{context,page,errors}=await open('store=wayfair-reference&workspace=products&product=W002337865',{hold:'read'});
 try{
  await page.waitForFunction(()=>window.heldResponse);await page.evaluate(()=>{dispatchEvent(new Event('bonfyre:account-changed'));releaseHeld();});
  await page.waitForNetworkIdle({idleTime:200});
  expect(await page.$eval('[data-catalogue-workspace]',n=>n.dataset.closed)).toBe('true');expect(await page.evaluate(()=>document.body.textContent.includes('Private'))).toBe(false);
  expect(await page.$('[data-related-work]')).toBeNull();expect(await page.evaluate(()=>writeCalls)).toBe(0);expect(errors).toEqual([]);
 }finally{await context.close();}
},40000);
retained('a stage committed before account closure remains exactly once and its delayed reply cannot offer retry',async()=>{
 const{context,page,errors}=await open('store=wayfair-reference&workspace=releases',{hold:'write'});
 try{
  await stage(page,'Private committed before closure');await page.waitForFunction(()=>window.heldResponse);
  const before=(await f.store(binding).readLocal('release:catalogue:')).filter(r=>r.value.title==='Private committed before closure');expect(before).toHaveLength(1);expect(before[0].revision).toBe(1);
  await page.evaluate(()=>{window.heldRetry=document.querySelector('[data-release=retry]');dispatchEvent(new Event('bonfyre:account-changed'));releaseHeld();});
  await page.waitForNetworkIdle({idleTime:200});await page.evaluate(()=>heldRetry.click());
  expect(await page.evaluate(()=>writeCalls)).toBe(1);expect(await page.evaluate(()=>document.body.textContent.includes('Private'))).toBe(false);expect(await page.$('[data-release=retry]')).toBeNull();
  expect((await f.store(binding).readLocal(before[0].key))[0]).toEqual(before[0]);expect(errors).toEqual([]);
 }finally{await context.close();}
},40000);
retained('pagehide and persisted pageshow stay closed; reload evaluates current membership',async()=>{
 const{context,page,errors}=await open('store=wayfair-reference&workspace=products&product=W002337865',{width:390});
 try{
  await page.waitForFunction(()=>document.querySelector('[data-ui=title]')?.value==='Private revised catalogue draft');
  await page.evaluate(()=>{dispatchEvent(new PageTransitionEvent('pagehide',{persisted:true}));dispatchEvent(new PageTransitionEvent('pageshow',{persisted:true}));});
  expect(await page.$eval('[data-catalogue-workspace]',n=>n.dataset.closed)).toBe('true');expect(await page.evaluate(()=>document.body.textContent.includes('Private'))).toBe(false);
  const[name,...v]=f.other.cookie.split('=');await page.setCookie({name,value:v.join('='),url:f.base,httpOnly:true,sameSite:'Lax'});
  const response=await page.reload({waitUntil:'networkidle0'});expect(response.status()).toBe(403);expect(await page.evaluate(()=>document.body.textContent.includes('Private'))).toBe(false);expect(errors).toEqual([]);
 }finally{await context.close();}
},40000);
