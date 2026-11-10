import {test,expect,beforeAll,afterAll} from 'bun:test';
import {join} from 'node:path';
import {createRequire} from 'node:module';
import {mkdirSync} from 'node:fs';
import {workspaceFixture,root} from '../../cmd/BonfyreWebAddress/tests/workspace_fixture.mjs';
import {materializePlannerAccountRepair,plannerAccountBase} from '../materialize_planner_account_repair.mjs';
import {loadWorkspaceFiles} from '../workspace_material.mjs';
const packageRoot=process.env.BONFYRE_ROOM_BASE_ROOT,retained=packageRoot?test:test.skip;
let f,binding,browser,pack,files,configs;
const prior=Object.fromEntries(['BONFYRE_WORKSPACE_RELEASE','BONFYRE_WORKSPACE_PACKAGES'].map(k=>[k,process.env[k]]));
beforeAll(async()=>{
 if(!packageRoot)return;pack=await materializePlannerAccountRepair({root:packageRoot});files=await loadWorkspaceFiles(packageRoot,pack.id);
 process.env.BONFYRE_WORKSPACE_PACKAGES=packageRoot;process.env.BONFYRE_WORKSPACE_RELEASE=pack.id;
 configs=['product-room','red3-planner'].map(entry=>({entry,...JSON.parse(files.get(entry+'/index.html').toString().match(/<script[^>]*data-planner-config[^>]*>([\s\S]*?)<\/script>/)[1])}));
 f=await workspaceFixture();binding=await f.install('catalogue','institution:workspace-team');
 browser=await createRequire(join(root,'tools/creator-ingest/package.json'))('puppeteer').launch({headless:true,executablePath:'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',args:['--disable-background-networking']});
},60000);
afterAll(async()=>{await browser?.close();await f?.close();for(const[k,v]of Object.entries(prior))if(v===undefined)delete process.env[k];else process.env[k]=v;});
async function seed(c,id){
 const plan={schema:'service-site.fixture-plan.v1',catalogue:c.catalogue.digest,id,...structuredClone(c.catalogue.defaults),title:'Private planner '+id,notes:'Private saved note '+id,items:[{key:'private-fixture',product:c.catalogue.products[0].id,x:1,y:1,rotation:0}]};
 const key='fixture-plan:'+c.form+':'+id;await f.store(binding).saveLocal(key,{plan,receipt:null},0);return{plan,key};
}
async function open(c,id,{width=1440,hold=null}={}){
 const context=await browser.createBrowserContext(),page=await context.newPage(),errors=[];page.setDefaultTimeout(20000);await page.setViewport({width,height:1000});page.on('pageerror',e=>errors.push(e.message));
 const[name,...v]=f.owner.cookie.split('=');await page.setCookie({name,value:v.join('='),url:f.base,httpOnly:true,sameSite:'Lax'});
 await page.setRequestInterception(true);page.on('request',r=>{if(new URL(r.url()).origin!==f.base)r.respond({status:204}).catch(()=>{});else r.continue().catch(()=>{});});
 await page.evaluateOnNewDocument(hold=>{
  const fetch=window.fetch.bind(window);let held=false,reads=0;window.writeCalls=0;
  window.fetch=async(url,options)=>{
   const u=new URL(url,location.href),record=u.pathname.endsWith('/_records'),op=u.searchParams.get('operation'),prefix=u.searchParams.get('prefix')||'';
   if(record&&op==='write')window.writeCalls++;
   if(record&&op==='read'&&prefix.startsWith('fixture-plan:'))reads++;
   const response=await fetch(url,options);
   if(!held&&record&&response.ok&&(hold==='write'&&op==='write'||hold==='read'&&op==='read'&&prefix.startsWith('fixture-plan:')&&reads===1||hold==='projects'&&op==='read'&&prefix.startsWith('fixture-plan:')&&reads===2||hold==='model'&&op==='read'&&prefix.startsWith('product-model:'))){held=true;window.heldResponse=true;await new Promise(resolve=>window.releaseHeld=resolve);}
   return response;
  };
 },hold);
 await page.goto(f.base+'/apps/'+binding.id+'/'+c.entry+'/?project='+id,{waitUntil:hold==='read'||hold==='projects'?'domcontentloaded':'networkidle0'});
 return{context,page,errors};
}
async function proof(page,name){if(process.env.PLANNER_ACCOUNT_PROOF){mkdirSync(process.env.PLANNER_ACCOUNT_PROOF,{recursive:true});await page.screenshot({path:join(process.env.PLANNER_ACCOUNT_PROOF,name+'.png'),fullPage:true});}}
async function closed(page){await page.waitForSelector('[data-planner][data-closed=true]');await page.waitForNetworkIdle({idleTime:150});expect(await page.evaluate(()=>document.body.textContent.includes('Private'))).toBe(false);expect(await page.$('[data-related-work]')).toBeNull();}
retained('both room closures preserve every historical module and unrelated package byte',async()=>{
 const before=await loadWorkspaceFiles(packageRoot,plannerAccountBase);expect(files.size-before.size).toBe(68);
 for(const[n,b]of before)if(!['product-room/index.html','red3-planner/index.html'].includes(n))expect(files.get(n).equals(b)).toBe(true);
 const parser=new Bun.Transpiler({loader:'js'});
 for(const p of pack.projections)for(const n of ['planner-client.mjs','planner-project-client.mjs','planner-spatial.mjs','planner-product-scene.mjs','planner-workspace.mjs'])expect(()=>parser.transformSync(files.get(p.prefix+n).toString())).not.toThrow();
});
for(const i of [0,1])retained('saved room '+i+' edits and reopens before account closure clears held notes, modal, scenes and history',async()=>{
 const c=configs[i],{key,plan}=await seed(c,'planner-lifetime-'+i),{context,page,errors}=await open(c,plan.id,{width:i?390:1440});
 try{
  await page.waitForSelector('[data-planner][data-ready=true]');await page.$eval('[data-ui=notes]',n=>{n.value='Private revised room note';n.dispatchEvent(new Event('input',{bubbles:true}));});await page.click('[data-ui=save]');
  await page.waitForFunction(()=>document.querySelector('[data-ui=save-state]').textContent.startsWith('Saved'));expect((await f.store(binding).readLocal(key))[0].revision).toBe(2);
  await page.reload({waitUntil:'networkidle0'});await page.waitForSelector('[data-planner][data-ready=true]');expect(await page.$eval('[data-ui=notes]',n=>n.value)).toBe('Private revised room note');
  await page.evaluate(()=>document.querySelector('[data-ui=review]').click());await page.waitForSelector('[data-ui=review-dialog][open]');
  await page.evaluate(()=>{window.heldNotes=document.querySelector('[data-ui=notes]');window.heldSave=document.querySelector('[data-ui=save]');window.writesBefore=writeCalls;dispatchEvent(new Event('bonfyre:account-changed'));});
  await closed(page);expect(await page.evaluate(()=>({value:heldNotes.value,disabled:heldSave.disabled,stages:globalThis.Konva?.stages.length??0}))).toEqual({value:'',disabled:true,stages:0});
  await page.evaluate(()=>{heldSave.click();heldNotes.dispatchEvent(new Event('input',{bubbles:true}));dispatchEvent(new PopStateEvent('popstate'));dispatchEvent(new KeyboardEvent('keydown',{key:'z',metaKey:true,bubbles:true}));});await page.waitForNetworkIdle({idleTime:100});
  expect(await page.evaluate(()=>writeCalls)).toBe(await page.evaluate(()=>writesBefore));expect((await f.store(binding).readLocal(key))[0].revision).toBe(2);
  expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1)).toBe(true);await proof(page,'closed-'+c.entry);expect(errors).toEqual([]);
 }finally{await context.close();}
},60000);
for(const hold of ['read','projects'])retained('a delayed '+hold+' read cannot repopulate a closed room',async()=>{
 const c=configs[0],{plan}=await seed(c,'planner-delayed-'+hold),{context,page,errors}=await open(c,plan.id,{hold});
 try{await page.waitForFunction(()=>window.heldResponse);await page.evaluate(()=>{dispatchEvent(new Event('bonfyre:account-changed'));releaseHeld();});await closed(page);expect(await page.evaluate(()=>writeCalls)).toBe(0);expect(errors).toEqual([]);}
 finally{await context.close();}
},40000);
retained('a committed save with a delayed response stays exactly once and cannot trigger a latest shortcut after closure',async()=>{
 const c=configs[0],{plan,key}=await seed(c,'planner-delayed-write'),{context,page,errors}=await open(c,plan.id,{hold:'write'});
 try{
  await page.waitForSelector('[data-planner][data-ready=true]');await page.$eval('[data-ui=notes]',n=>{n.value='Private committed note';n.dispatchEvent(new Event('input',{bubbles:true}));});await page.click('[data-ui=save]');await page.waitForFunction(()=>window.heldResponse);
  const row=(await f.store(binding).readLocal(key))[0];expect(row.revision).toBe(2);expect(row.value.plan.notes).toBe('Private committed note');
  await page.evaluate(()=>{window.heldSave=document.querySelector('[data-ui=save]');dispatchEvent(new Event('bonfyre:account-changed'));releaseHeld();});await closed(page);await page.evaluate(()=>heldSave.click());
  expect(await page.evaluate(()=>writeCalls)).toBe(1);expect((await f.store(binding).readLocal(key))[0]).toEqual(row);expect(errors).toEqual([]);
 }finally{await context.close();}
},40000);
retained('pagehide and persisted pageshow remain closed, then reload checks the new account',async()=>{
 const c=configs[1],{plan}=await seed(c,'planner-pagehide-test'),{context,page,errors}=await open(c,plan.id,{width:390});
 try{
  await page.waitForSelector('[data-planner][data-ready=true]');await page.evaluate(()=>{dispatchEvent(new PageTransitionEvent('pagehide',{persisted:true}));dispatchEvent(new PageTransitionEvent('pageshow',{persisted:true}));});await closed(page);
  const[name,...v]=f.other.cookie.split('=');await page.setCookie({name,value:v.join('='),url:f.base,httpOnly:true,sameSite:'Lax'});const response=await page.reload({waitUntil:'networkidle0'});expect(response.status()).toBe(403);expect(await page.evaluate(()=>document.body.textContent.includes('Private'))).toBe(false);expect(errors).toEqual([]);
 }finally{await context.close();}
},40000);

retained('a delayed private model read cannot recreate a closed 3D room',async()=>{
 const c=configs[0],{plan}=await seed(c,'planner-delayed-model'),{context,page,errors}=await open(c,plan.id,{hold:'model'});
 try{
  await page.waitForSelector('[data-planner][data-ready=true]');await page.click('[data-ui=product-scene-toggle]');await page.waitForFunction(()=>window.heldResponse);
  await page.evaluate(()=>{dispatchEvent(new Event('bonfyre:account-changed'));releaseHeld();});await closed(page);
  expect(await page.$('[data-ui=product-scene]')).toBeNull();expect(await page.evaluate(()=>globalThis.Konva?.stages.length??0)).toBe(0);expect(await page.evaluate(()=>writeCalls)).toBe(0);expect(errors).toEqual([]);
 }finally{await context.close();}
},40000);
