import {test,expect,beforeAll,afterAll} from 'bun:test';
import {join} from 'node:path';
import {createRequire} from 'node:module';
import {mkdirSync} from 'node:fs';
import {gunzipSync} from 'node:zlib';
import {workspaceFixture,root} from '../../cmd/BonfyreWebAddress/tests/workspace_fixture.mjs';
import {materializeCatalogueReleaseRepair,catalogueReleaseRepairFiles,catalogueReleaseBase} from '../materialize_catalogue_release_repair.mjs';
import {loadWorkspaceFiles} from '../workspace_material.mjs';
import {baseline,makeDraft,draftKey} from '../../components/BonfyreServiceSite/catalogue-model.mjs';
const packageRoot=process.env.BONFYRE_ROOM_BASE_ROOT,retained=packageRoot?test:test.skip;
let f,binding,browser,pack,files,config,plan,roomKey,releaseKey,sourceKey;
const prior=Object.fromEntries(['BONFYRE_WORKSPACE_RELEASE','BONFYRE_WORKSPACE_PACKAGES'].map(k=>[k,process.env[k]]));
beforeAll(async()=>{
 if(!packageRoot)return;
 pack=await materializeCatalogueReleaseRepair({root:packageRoot});files=await loadWorkspaceFiles(packageRoot,pack.id);
 process.env.BONFYRE_WORKSPACE_PACKAGES=packageRoot;process.env.BONFYRE_WORKSPACE_RELEASE=pack.id;
 config=JSON.parse(files.get('catalogue/index.html').toString().match(/<script[^>]*data-catalogue-config[^>]*>([\s\S]*?)<\/script>/)[1]);
 const catalogue=config.fixture_catalogues.find(c=>c.id==='wayfair-reference'),p=catalogue.products[0];
 plan={schema:'service-site.fixture-plan.v1',catalogue:catalogue.digest,id:'release-retained-room',...structuredClone(catalogue.defaults),title:'Lighting for forty positions',width:60,depth:60,budget:300000,items:Array.from({length:40},(_,i)=>({key:'lamp-'+i,product:p.id,x:i%8*7,y:Math.floor(i/8)*10,rotation:0}))};
 roomKey='fixture-plan:planner-wayfair-reference:'+plan.id;sourceKey='draft:catalogue:wayfair-reference:W002337865';
 f=await workspaceFixture();binding=await f.install('catalogue','institution:workspace-team');await f.store(binding).saveLocal(roomKey,{plan,receipt:null},0);
 browser=await createRequire(join(root,'tools/creator-ingest/package.json'))('puppeteer').launch({headless:true,executablePath:'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',args:['--disable-background-networking']});
},60000);
afterAll(async()=>{await browser?.close();await f?.close();for(const[k,v]of Object.entries(prior))if(v===undefined)delete process.env[k];else process.env[k]=v;});
async function open(query,actor=f.owner,width=1440,lose=false){
 const context=await browser.createBrowserContext(),page=await context.newPage(),errors=[];page.setDefaultTimeout(20000);await page.setViewport({width,height:1000});page.on('pageerror',e=>errors.push(e.message));
 const[name,...v]=actor.cookie.split('=');await page.setCookie({name,value:v.join('='),url:f.base,httpOnly:true,sameSite:'Lax'});
 await page.setRequestInterception(true);page.on('request',r=>{if(new URL(r.url()).origin!==f.base)r.respond({status:204}).catch(()=>{});else r.continue().catch(()=>{});});
 if(lose)await page.evaluateOnNewDocument(()=>{const fetch=window.fetch.bind(window);let lost=false;window.fetch=async(url,options)=>{const reply=await fetch(url,options),u=new URL(url,location.href);if(!lost&&u.pathname.endsWith('/_records')&&u.searchParams.get('operation')==='write'&&reply.ok){lost=true;await reply.arrayBuffer();throw new TypeError('Simulated lost reply after native commit');}return reply;};});
 await page.goto(f.base+binding.path+'?'+query,{waitUntil:'networkidle0'});return{context,page,errors};
}
const notice=(p,text)=>p.waitForFunction(text=>document.querySelector('[data-release=notice]').textContent.includes(text),{},text);
async function stage(page,title){await page.waitForSelector('[data-release=grid] .ag-row');await page.click('[data-release=grid] .ag-row .ag-selection-checkbox');await page.type('[data-release=name]',title);await page.click('[data-release=stage]');}
async function proof(page,name){if(process.env.CATALOGUE_RELEASE_PROOF){mkdirSync(process.env.CATALOGUE_RELEASE_PROOF,{recursive:true});await page.screenshot({path:join(process.env.CATALOGUE_RELEASE_PROOF,name+'.png'),fullPage:true});}}
retained('new catalogue release closure preserves existing source material, scenes, storage and history',async()=>{
 const before=await loadWorkspaceFiles(packageRoot,catalogueReleaseBase),expected=catalogueReleaseRepairFiles(before);
 expect(pack.projection.files).toBe(39);expect(files.size-before.size).toBe(39);
 for(const[n,b]of before)if(!['catalogue/index.html','CONTINUATIONS.tsv'].includes(n))expect(files.get(n).equals(b)).toBe(true);
 const changed=[...before].filter(([n,b])=>n.startsWith(pack.projection.prior)&&!b.equals(files.get(pack.projection.prefix+n.slice(pack.projection.prior.length)))).map(([n])=>n.slice(pack.projection.prior.length)).sort();
 expect(changed).toEqual(['catalogue-client.mjs','catalogue-desk.mjs','catalogue-grid.mjs','planner-model.mjs']);
 expect(files.get(pack.projection.prefix+'catalogue-client.mjs').toString()).toContain('createCatalogueRoomDialog');
 expect(files.get(pack.projection.prefix+'catalogue-grid.mjs').toString()).toContain('composeCollectionWorkspace');
 expect(files.get('CONTINUATIONS.tsv').toString()).toContain('release=$key&workspace=@releases');
 expect(expected.projection).toEqual(pack.projection);
});
retained('actual installed merchant IDs support price edit, forty-position impact, shared native review and exact continuation',async()=>{
 const{context,page,errors}=await open('store=wayfair-reference&workspace=products&product=W002337865&panel=options');
 try{
  await page.waitForSelector('[data-field="price:W002337865"]',{visible:true});
  expect(await page.$eval('[data-ui=store]',n=>n.value)).toBe('wayfair-reference');expect(await page.$eval('[data-ui=store]',n=>n.selectedOptions[0].textContent)).toBe('Wayfair model reference');
  await page.$eval('[data-field="price:W002337865"]',n=>{n.value='79.99';n.dispatchEvent(new Event('input',{bubbles:true}));});await page.click('[data-ui=save]');
  await page.waitForFunction(()=>document.querySelector('[data-ui=message]').textContent.startsWith('Draft saved.'));
  await page.click('[data-workspace-view=releases]');await stage(page,'Forty lamp price review');await notice(page,'Release staged.');
  releaseKey=new URL(page.url()).searchParams.get('release');expect(releaseKey).toMatch(/^release:catalogue:/);
  expect(await page.$$eval('[data-release=impact] li',n=>n.length)).toBe(40);expect(await page.$eval('.cr-placements',n=>n.open)).toBe(false);await page.click('.cr-placements summary');expect(await page.$eval('.cr-placements',n=>n.open)).toBe(true);await page.click('.cr-placements summary');
  const impact=await page.$eval('[data-release=impact]',n=>n.textContent);
  expect(impact).toContain('$2,799.60');expect(impact).toContain('$3,199.60');expect(impact).toContain('$400.00');expect(impact).toContain('$199.60 over');
  const row=(await f.store(binding).readLocal(releaseKey))[0];expect(row.revision).toBe(1);expect(row.value.impact.unresolved).toEqual([]);
  const continued=await f.raw('/apps/'+binding.id+'/_views?'+new URLSearchParams({key:releaseKey,revision:'1',release:pack.id}));
  expect(continued.status).toBe(200);const document=await continued.text(),target=document.match(/data-compatible-view href="([^"]+)"/)?.[1].replaceAll('&amp;','&');expect(target).toBeDefined();
  const opened=await f.raw(target);expect(opened.status).toBe(303);const route=new URL(opened.headers.get('location'),f.base);expect(route.searchParams.get('release')).toBe(releaseKey);expect(route.searchParams.get('workspace')).toBe('releases');
  expect(errors).toEqual([]);
 }finally{await context.close();}
 const shared=await open('store=wayfair-reference&workspace=releases&release='+encodeURIComponent(releaseKey),f.writer,390);
 try{
  await shared.page.waitForSelector('[data-release=detail]:not([hidden])');await shared.page.type('[data-release=note]','Checked forty placements against the retained listing. Depth still needs confirmation.');
  await shared.page.click('[data-release=review]');await notice(shared.page,'Review saved.');
  const row=(await f.store(binding,f.reader).readLocal(releaseKey))[0];expect(row.revision).toBe(2);expect(row.value.state).toBe('reviewed');expect(row.value.publication).toBeNull();
  expect((await f.store(binding).readLocal(roomKey))[0].revision).toBe(1);expect((await f.store(binding).readLocal(sourceKey))[0].revision).toBe(1);
  expect(await shared.page.$eval('.cr-new',n=>n.open)).toBe(false);expect(await shared.page.$eval('[data-release=impact]',n=>n.textContent)).not.toContain('price:W002337865');
  expect(await shared.page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1)).toBe(true);await proof(shared.page,'release-review-390');
  await shared.page.setViewport({width:1440,height:1000});await proof(shared.page,'release-review-1440');expect(shared.errors).toEqual([]);
 }finally{await shared.context.close();}
},60000);
retained('concurrent room correction refuses old review and refresh clears its annotation',async()=>{
 const{context,page}=await open('store=wayfair-reference&workspace=releases&release='+encodeURIComponent(releaseKey));
 try{
  await page.waitForSelector('[data-release=detail]:not([hidden])');const store=f.store(binding),before=(await store.readLocal(releaseKey))[0],room=(await store.readLocal(roomKey))[0];
  await store.saveLocal(roomKey,{...room.value,plan:{...room.value.plan,notes:'A colleague corrected the room specification.'}},room.revision);
  await page.click('[data-release=review]');await notice(page,'changed after staging');expect((await store.readLocal(releaseKey))[0]).toEqual(before);
  await page.click('[data-release=reload]');await page.waitForFunction(()=>document.querySelector('[data-release=state]').textContent.includes('refresh required'));expect(await page.$eval('[data-release=review]',n=>n.disabled)).toBe(true);
  await page.click('[data-release=refresh]');await notice(page,'Sources refreshed');const refreshed=(await store.readLocal(releaseKey))[0];expect(refreshed.value.state).toBe('staged');expect(refreshed.value.review).toBeNull();
 }finally{await context.close();}
},40000);
retained('lost native stage response retries the same release once without duplicate effects',async()=>{
 const{context,page,errors}=await open('store=wayfair-reference&workspace=releases',f.owner,1440,true);
 try{
  await stage(page,'Uncertain save recovery');await notice(page,'Save status is unknown');
  const rows=await f.store(binding).readLocal('release:catalogue:'),saved=rows.find(r=>r.value.title==='Uncertain save recovery');
  expect(saved?.revision).toBe(1);await page.click('[data-release=retry]');await notice(page,'Release save confirmed');
  const after=await f.store(binding).readLocal('release:catalogue:');expect(after).toHaveLength(rows.length);expect(after.find(r=>r.key===saved.key)).toEqual(saved);expect(errors).toEqual([]);
 }finally{await context.close();}
},40000);
retained('retained source-observation mismatch stays blocked in the actual catalogue',async()=>{
 const catalogue=config.fixture_catalogues.find(c=>c.id!=='wayfair-reference'),product=JSON.parse(gunzipSync(files.get(pack.projection.prefix+'products-7.json.gz'))).find(p=>p.store==='red3-display'&&p.id==='7631'),original=baseline(product),edit=structuredClone(original);edit.variants[0].price='25.00';
 const room={schema:'service-site.fixture-plan.v1',catalogue:catalogue.digest,id:'release-easel-gap',...structuredClone(catalogue.defaults),items:[{key:'easel-one',product:'easel',x:1,y:1,rotation:0}]};
 await f.store(binding).saveLocalBatch([{key:draftKey(product),value:makeDraft(product,original,edit),expectedRevision:0},{key:'fixture-plan:planner-red3-fixtures:'+room.id,value:{plan:room,receipt:null},expectedRevision:0}]);
 const{context,page}=await open('store=red3-display&workspace=releases');
 try{await stage(page,'Source check required');await notice(page,'Release staged.');expect(await page.$eval('[data-release=impact]',n=>n.textContent)).toContain('different source observations');expect(await page.$eval('[data-release=review]',n=>n.disabled)).toBe(true);expect(await page.$eval('[data-release=impact]',n=>n.textContent)).toContain('Needs source check');}
 finally{await context.close();}
},40000);
