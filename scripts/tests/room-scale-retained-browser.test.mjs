import {test,expect,beforeAll,afterAll} from 'bun:test';
import {join} from 'node:path';
import {createRequire} from 'node:module';
import {mkdirSync} from 'node:fs';
import {workspaceFixture,root} from '../../cmd/BonfyreWebAddress/tests/workspace_fixture.mjs';
import {materializeRoomScaleRepair} from '../materialize_room_scale_repair.mjs';
import {loadWorkspaceFiles} from '../workspace_material.mjs';
const baseRoot=process.env.BONFYRE_ROOM_BASE_ROOT,retained=baseRoot?test:test.skip;
let f,binding,browser,pack,plan,key;
const prior=Object.fromEntries(['BONFYRE_WORKSPACE_RELEASE','BONFYRE_WORKSPACE_PACKAGES'].map(k=>[k,process.env[k]]));
beforeAll(async()=>{
 if(!baseRoot)return;
 pack=await materializeRoomScaleRepair({root:baseRoot});process.env.BONFYRE_WORKSPACE_PACKAGES=baseRoot;process.env.BONFYRE_WORKSPACE_RELEASE=pack.id;
 const files=await loadWorkspaceFiles(baseRoot,pack.id),html=files.get('product-room/index.html').toString();
 const config=JSON.parse(html.match(/<script[^>]*data-planner-config[^>]*>([\s\S]*?)<\/script>/)[1]),catalogue=config.catalogue;
 const p=catalogue.products[0]; // Retain the actual listing, including its unresolved depth.
 plan={schema:'service-site.fixture-plan.v1',catalogue:catalogue.digest,id:'retained-scale-001',...structuredClone(catalogue.defaults),title:'Forty fixture test',width:60,depth:60,budget:100000000,items:Array.from({length:40},(_,i)=>({key:'scale-'+i,product:p.id,x:i%8*7,y:Math.floor(i/8)*10,rotation:0}))};
 key='fixture-plan:'+config.form+':'+plan.id;
 f=await workspaceFixture();binding=await f.install('catalogue','institution:workspace-team');await f.store(binding).saveLocal(key,{plan,receipt:null},0);
 browser=await createRequire(join(root,'tools/creator-ingest/package.json'))('puppeteer').launch({headless:true,executablePath:'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',args:['--disable-background-networking']});
},60000);
afterAll(async()=>{await browser?.close();await f?.close();for(const[k,v]of Object.entries(prior))if(v===undefined)delete process.env[k];else process.env[k]=v;});
retained('exact retained package adds beyond twelve, edits forty fixtures and reopens native saves at both widths',async()=>{
 const context=await browser.createBrowserContext(),page=await context.newPage(),errors=[];page.setDefaultTimeout(20000);page.on('pageerror',e=>errors.push(e.message));
 const[name,...v]=f.owner.cookie.split('=');await page.setCookie({name,value:v.join('='),url:f.base,httpOnly:true,sameSite:'Lax'});
 await page.setRequestInterception(true);page.on('request',r=>{if(new URL(r.url()).origin!==f.base)r.respond({status:204}).catch(()=>{});else r.continue().catch(()=>{});});
 try{
  for(const width of [1440,390]){
   await page.setViewport({width,height:1000});await page.goto(f.base+'/apps/'+binding.id+'/product-room/?project='+plan.id,{waitUntil:'networkidle0'});await page.waitForSelector('[data-planner][data-ready=true]');
   expect(await page.$$eval('[data-ui=selection] [data-select]',n=>n.length)).toBe(40);
   await page.evaluate(()=>document.querySelector('[data-add]').click());expect(await page.$$eval('[data-ui=selection] [data-select]',n=>n.length)).toBe(41);
   await page.click('[data-ui=remove]');expect(await page.$$eval('[data-ui=selection] [data-select]',n=>n.length)).toBe(40);
   await page.evaluate(()=>document.querySelector('[data-select="scale-39"]').click());
   await page.$eval('[data-ui=x]',(n,v)=>{n.value=v;n.dispatchEvent(new Event('input',{bubbles:true}));},width===1440?'49.25':'49.5');
   await page.click('[data-ui=save]');await page.waitForFunction(()=>document.querySelector('[data-ui=save-state]').textContent.startsWith('Saved'));
   const row=(await f.store(binding).readLocal(key))[0];expect(row.value.plan.items).toHaveLength(40);expect(row.value.plan.items[39].x).toBe(width===1440?49.25:49.5);
   await page.reload({waitUntil:'networkidle0'});await page.waitForSelector('[data-planner][data-ready=true]');expect(await page.$$eval('[data-ui=selection] [data-select]',n=>n.length)).toBe(40);
   const labels=await page.evaluate(()=>{const stage=Konva.stages.find(s=>s.container()===document.querySelector('[data-ui=spatial]'));return stage.find('.fixture-number').map(n=>({width:n.width()*n.getAbsoluteScale().x,wrap:n.wrap()}));});expect(labels).toHaveLength(40);expect(labels.every(n=>n.width>=23.9&&n.wrap==='none')).toBe(true);
   expect(await page.$eval('[data-ui=selection]',n=>n.clientHeight)).toBeLessThanOrEqual(366);expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1)).toBe(true);
   if(process.env.ROOM_SCALE_PROOF){mkdirSync(process.env.ROOM_SCALE_PROOF,{recursive:true});await page.screenshot({path:join(process.env.ROOM_SCALE_PROOF,'retained-planner-'+width+'.png'),fullPage:true});}
  }
  expect(errors).toEqual([]);
 }finally{await context.close();}
},60000);
retained('retained catalogue still opens its native product editor from the new immutable closure',async()=>{
 const context=await browser.createBrowserContext(),page=await context.newPage(),errors=[];page.setDefaultTimeout(20000);page.on('pageerror',e=>errors.push(e.message));
 const[name,...v]=f.owner.cookie.split('=');await page.setCookie({name,value:v.join('='),url:f.base,httpOnly:true,sameSite:'Lax'});
 await page.setRequestInterception(true);page.on('request',r=>{if(new URL(r.url()).origin!==f.base)r.respond({status:204}).catch(()=>{});else r.continue().catch(()=>{});});
 try{const response=await page.goto(f.base+binding.path,{waitUntil:'networkidle0'});expect(response.status()).toBe(200);await page.waitForSelector('.ag-row');expect(await page.$$eval('.ag-row',n=>n.length)).toBeGreaterThan(0);expect(errors).toEqual([]);}
 finally{await context.close();}
},40000);
