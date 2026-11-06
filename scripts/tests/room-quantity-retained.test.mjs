import {test,expect,beforeAll,afterAll} from 'bun:test';
import {join} from 'node:path';
import {createRequire} from 'node:module';
import {mkdirSync} from 'node:fs';
import {workspaceFixture,root} from '../../cmd/BonfyreWebAddress/tests/workspace_fixture.mjs';
import {materializeRoomQuantityRepair,roomQuantityBase} from '../materialize_room_quantity_repair.mjs';
import {loadWorkspaceFiles} from '../workspace_material.mjs';
import {addRoomUnits,newRoomPlan,changeRoomQuantity,resolveRoomOption} from '../../components/BonfyreServiceSite/catalogue-room-model.mjs';
import {fixtureCatalogue} from '../../components/BonfyreServiceSite/tests/catalogue-release-fixture.mjs';
const baseRoot=process.env.BONFYRE_ROOM_BASE_ROOT,retained=baseRoot?test:test.skip;
test('catalogue quantity changes respect the shared whole-room bound and keep input immutable',()=>{
 const p=newRoomPlan(fixtureCatalogue,'table',40,'quantity-unit-001'),snapshot=structuredClone(p);
 expect(addRoomUnits(p,fixtureCatalogue,'table',1).items).toHaveLength(41);
 const full=addRoomUnits(p,fixtureCatalogue,'table',24);expect(full.items).toHaveLength(64);
 expect(changeRoomQuantity(p,fixtureCatalogue,'table',64).items).toHaveLength(64);
 expect(changeRoomQuantity(full,fixtureCatalogue,'table',13).items).toHaveLength(13);
 expect(()=>addRoomUnits(full,fixtureCatalogue,'table',1)).toThrow('64 fixtures');
 expect(()=>newRoomPlan(fixtureCatalogue,'table',65)).toThrow();
 for(const n of [-1,0,1.5,NaN,Infinity,65])expect(()=>addRoomUnits(p,fixtureCatalogue,'table',n)).toThrow();
 expect(()=>changeRoomQuantity(p,fixtureCatalogue,'table',0)).toThrow();expect(p).toEqual(snapshot);
});
let f,binding,browser,pack,files,config,route,fixture;
const prior=Object.fromEntries(['BONFYRE_WORKSPACE_RELEASE','BONFYRE_WORKSPACE_PACKAGES'].map(k=>[k,process.env[k]]));
beforeAll(async()=>{
 if(!baseRoot)return;pack=await materializeRoomQuantityRepair({root:baseRoot});files=await loadWorkspaceFiles(baseRoot,pack.id);
 process.env.BONFYRE_WORKSPACE_PACKAGES=baseRoot;process.env.BONFYRE_WORKSPACE_RELEASE=pack.id;
 config=JSON.parse(files.get('catalogue/index.html').toString().match(/<script[^>]*data-catalogue-config[^>]*>([\s\S]*?)<\/script>/)[1]);
 route=config.roomRoutes.find(r=>r.bindings.some(b=>b.productId==='W002337865'));fixture=route.catalogue.products.find(p=>p.product_id==='W002337865');
 f=await workspaceFixture();binding=await f.install('catalogue','institution:workspace-team');route.path=route.path.replace('__WORKSPACE__',binding.id);
 browser=await createRequire(join(root,'tools/creator-ingest/package.json'))('puppeteer').launch({headless:true,executablePath:'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',args:['--disable-background-networking']});
},60000);
afterAll(async()=>{await browser?.close();await f?.close();for(const[k,v]of Object.entries(prior))if(v===undefined)delete process.env[k];else process.env[k]=v;});
retained('immutable closures preserve all historical members and exact option source joins',async()=>{
 const before=await loadWorkspaceFiles(baseRoot,roomQuantityBase);expect(files.size-before.size).toBe(73);
 for(const[n,b]of before)if(!['catalogue/index.html','product-room/index.html'].includes(n))expect(files.get(n).equals(b)).toBe(true);
 const b=route.bindings.find(b=>b.productId==='W002337865'),product={id:b.productId,store:b.store,source_sha256:b.sourceDigest,source_url:fixture.source_url,variants:[{id:b.variantId,price:(fixture.price_cents/100).toFixed(2)}]};
 expect(resolveRoomOption(product,b.variantId,[route]).fixture.id).toBe(fixture.id);
 expect(resolveRoomOption({...product,source_sha256:'0'.repeat(64)},b.variantId,[route])).toBeNull();
 expect(()=>resolveRoomOption({...product,source_url:'https://example.test/wrong'},b.variantId,[route])).toThrow('source disagree');
 expect(()=>resolveRoomOption(product,b.variantId,[route,route])).toThrow('more than one');
});
async function open(path,width){
 const context=await browser.createBrowserContext(),page=await context.newPage(),errors=[];page.setDefaultTimeout(20000);await page.setViewport({width,height:1000});page.on('pageerror',e=>errors.push(e.message));
 const[name,...v]=f.owner.cookie.split('=');await page.setCookie({name,value:v.join('='),url:f.base,httpOnly:true,sameSite:'Lax'});
 await page.setRequestInterception(true);page.on('request',r=>{if(new URL(r.url()).origin!==f.base)r.respond({status:204}).catch(()=>{});else r.continue().catch(()=>{});});
 await page.goto(f.base+path,{waitUntil:'networkidle0'});return{context,page,errors};
}
retained('catalogue places twenty-four units into a forty-item saved room and refuses a sixty-fifth',async()=>{
 const plan=newRoomPlan(route.catalogue,fixture.id,40,'catalogue-quantity-001');plan.title='Forty item quantity room';
 const key='fixture-plan:'+route.form+':'+plan.id;await f.store(binding).saveLocal(key,{plan,receipt:null},0);
 const{context,page,errors}=await open(binding.path+'?store=wayfair-reference&workspace=inventory',1440);
 async function dialog(){
  await page.waitForSelector('[data-grid=table] .ag-row[row-id="product:W002337865"] [col-id=title]');await page.click('[data-grid=table] .ag-row[row-id="product:W002337865"] [col-id=title]');
  await page.waitForSelector('[data-grid-action=inspect-3d]',{visible:true});await page.click('[data-grid-action=inspect-3d]');
  await page.waitForSelector('.cr-dialog[data-ready=true]');await page.select('[aria-label="Destination room"]',plan.id);
 }
 try{
  await dialog();expect(await page.$eval('[aria-label="Quantity to place"]',n=>n.max)).toBe('64');
  await page.$eval('[aria-label="Quantity to place"]',n=>{n.value='24';n.dispatchEvent(new Event('input',{bubbles:true}));});
  await Promise.all([page.waitForNavigation({waitUntil:'networkidle0'}),page.evaluate(()=>[...document.querySelectorAll('.cr-dialog button')].find(n=>n.textContent==='Place in a room').click())]);
  let row=(await f.store(binding).readLocal(key))[0];expect(row.revision).toBe(2);expect(row.value.plan.items).toHaveLength(64);expect(new URL(page.url()).searchParams.get('project')).toBe(plan.id);
  await page.goto(f.base+binding.path+'?store=wayfair-reference&workspace=inventory',{waitUntil:'networkidle0'});await dialog();
  await page.evaluate(()=>[...document.querySelectorAll('.cr-dialog button')].find(n=>n.textContent==='Place in a room').click());
  await page.waitForFunction(()=>document.querySelector('.cr-status').textContent.includes('64 fixtures'));
  expect((await f.store(binding).readLocal(key))[0]).toEqual(row);
  await page.setViewport({width:390,height:844});expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1)).toBe(true);
  expect(await page.$$eval('.cr-controls input,.cr-controls select',nodes=>nodes.every(n=>n.getBoundingClientRect().width<=n.parentElement.getBoundingClientRect().width+1))).toBe(true);
  if(process.env.ROOM_QUANTITY_PROOF){mkdirSync(process.env.ROOM_QUANTITY_PROOF,{recursive:true});await page.screenshot({path:join(process.env.ROOM_QUANTITY_PROOF,'catalogue-limit-390.png'),fullPage:true});}
  await page.evaluate(()=>dispatchEvent(new Event('bonfyre:account-changed')));expect(await page.$('.cr-dialog')).toBeNull();expect(errors).toEqual([]);
 }finally{await context.close();}
},60000);
retained('room quantity control edits forty to sixty-four, saves and reopens at both widths',async()=>{
 const html=files.get('product-room/index.html').toString(),c=JSON.parse(html.match(/<script[^>]*data-planner-config[^>]*>([\s\S]*?)<\/script>/)[1]);
 for(const width of [1440,390]){
  const p=newRoomPlan(c.catalogue,c.catalogue.products[0].id,40,'room-quantity-'+width),key='fixture-plan:'+c.form+':'+p.id;
  await f.store(binding).saveLocal(key,{plan:p,receipt:null},0);
  const{context,page,errors}=await open('/apps/'+binding.id+'/product-room/?project='+p.id,width);
  try{
   await page.waitForSelector('[data-planner][data-ready=true]');const selector='[data-ui=material-quantities] input';
   expect(await page.$eval(selector,n=>({value:n.value,max:n.max}))).toEqual({value:'40',max:'64'});
   await page.$eval(selector,n=>{n.value='64';n.dispatchEvent(new Event('change',{bubbles:true}));});
   await page.click('[data-ui=save]');await page.waitForFunction(()=>document.querySelector('[data-ui=save-state]').textContent.startsWith('Saved'));
   let row=(await f.store(binding).readLocal(key))[0];expect(row.value.plan.items).toHaveLength(64);expect(row.revision).toBe(2);
   await page.reload({waitUntil:'networkidle0'});await page.waitForSelector('[data-planner][data-ready=true]');expect(await page.$eval(selector,n=>n.value)).toBe('64');
   await page.$eval(selector,n=>{n.value='65';n.dispatchEvent(new Event('change',{bubbles:true}));});expect(await page.$eval(selector,n=>n.value)).toBe('64');
   expect((await f.store(binding).readLocal(key))[0]).toEqual(row);
   if(process.env.ROOM_QUANTITY_PROOF){mkdirSync(process.env.ROOM_QUANTITY_PROOF,{recursive:true});await page.screenshot({path:join(process.env.ROOM_QUANTITY_PROOF,'room-quantity-'+width+'.png'),fullPage:true});}
   expect(errors).toEqual([]);
  }finally{await context.close();}
 }
},60000);
