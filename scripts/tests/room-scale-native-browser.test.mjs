import { test, expect, beforeAll, afterAll } from 'bun:test';
import { mkdtempSync, mkdirSync, readFileSync, writeFileSync, rmSync, readdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { build } from '../../components/BonfyreServiceSite/build.mjs';
import { materializeLocalWorkspacePackage } from '../materialize_workspace_package.mjs';
import { workspaceFixture, root } from '../../cmd/BonfyreWebAddress/tests/workspace_fixture.mjs';
import { openDestinationStore } from '../../integrations/browser/workspace-store.mjs';
import { prepareCatalogueRelease, saveCatalogueRelease, reviewCatalogueRelease, releasePrefix, verifyCatalogueRelease } from '../../components/BonfyreServiceSite/catalogue-release.mjs';
import { releaseFixture, fixtureCatalogue, product } from '../../components/BonfyreServiceSite/tests/catalogue-release-fixture.mjs';

import {createRequire} from 'node:module';
let temp, f, binding, pack, store, browser;
const source = releaseFixture(), savedEnv = Object.fromEntries(['BONFYRE_WORKSPACE_RELEASE','BONFYRE_WORKSPACE_PACKAGES'].map(key => [key,process.env[key]]));
const prepare = async (id, rows) => prepareCatalogueRelease({ ...source, records: rows ?? await store.readLocal(), id, title: 'Fixture price release', createdAt: '2026-10-17T12:00:00Z' });
const plan=source.records[1].value.plan;Object.assign(plan,{width:40,depth:20,budget:1400000,items:Array.from({length:40},(_,i)=>({key:'placement-'+i,product:'table',x:1+i%10*3.25,y:1+Math.floor(i/10)*2.25,rotation:0}))});
beforeAll(async () => {
  temp = mkdtempSync(join(tmpdir(),'catalogue-release-')); const site = join(temp,'site'), out = join(temp,'public'); mkdirSync(site);
  const content = JSON.parse(readFileSync(new URL('../../components/BonfyreServiceSite/tests/fixtures/sample-lawn/content.json',import.meta.url)));
  content.leads = 'server';
  content.pages = [
    {path:'catalogue',title:'Catalogue',menu:'Catalogue',layout:'workspace',sections:[{kind:'catalogue',id:'products',title:'Fixture catalogue',material:'products'}]},
    {path:'product-room',title:'Rooms',menu:'Rooms',layout:'workspace',sections:[{kind:'planner',id:'rooms',title:'Fixture rooms',material:'rooms',presentation:{brand:'Fixture store',intro:'Plan your showroom.',descriptor:'Fixtures',accent:'',planning_url:'https://fixture.example/planning',presets:{compact:'Compact'}}}]}
  ];
  writeFileSync(join(site,'content.json'),JSON.stringify(content));
  writeFileSync(join(site,'material.mjs'),`import {material,fixtureInput} from ${JSON.stringify(new URL('../../components/BonfyreServiceSite/tests/catalogue-release-fixture.mjs',import.meta.url).href)};\nimport {plannerFiles} from ${JSON.stringify(new URL('../../components/BonfyreServiceSite/planner-material.mjs',import.meta.url).href)};\nexport const products=()=>material;export const rooms=()=>plannerFiles(fixtureInput);\n`);
  await build(site,out);
  const compiled=await Bun.build({entrypoints:[join(out,'data/products/catalogue-client.mjs')],target:'browser',external:['/apps/workspace-store.mjs']});
  if(!compiled.success)throw new AggregateError(compiled.logs,'Catalogue modules did not build.');
  const files = new Map();
  function gather(dir, prefix='') { for (const entry of readdirSync(dir,{withFileTypes:true})) {
    const path=prefix+entry.name; if(entry.isDirectory()){gather(join(dir,entry.name),path+'/');continue;}
    if (!/\.(?:html|mjs|js|css|gz|txt)$/.test(path)) continue;
    let bytes=readFileSync(join(dir,entry.name));
    if (/\.(?:html|mjs|js|css)$/.test(path)) bytes=Buffer.from(bytes.toString().replace(/(["'])\/(data|assets|catalogue|product-room)\//g,'$1/bonfyre/spaces/$2/').replaceAll('href="/#','href="/bonfyre/spaces/#').replaceAll('href="/"','href="/bonfyre/spaces/"'));
    files.set(path,bytes);
  } }
  gather(out);
  process.env.BONFYRE_WORKSPACE_PACKAGES=join(temp,'packages');
  pack=await materializeLocalWorkspacePackage(files,{root:process.env.BONFYRE_WORKSPACE_PACKAGES});process.env.BONFYRE_WORKSPACE_RELEASE=pack.id;
  f=await workspaceFixture();binding=await f.install('catalogue','institution:workspace-team');store=f.store(binding);
  await store.saveLocalBatch(source.records.map(row=>({key:row.key,value:row.value,expectedRevision:0})));
 browser=await createRequire(join(root,'tools/creator-ingest/package.json'))('puppeteer').launch({headless:true,executablePath:'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',args:['--disable-background-networking']});
},60000);

afterAll(async()=>{await browser?.close();await f?.close();if(temp)rmSync(temp,{recursive:true,force:true});for(const[k,v]of Object.entries(savedEnv))if(v===undefined)delete process.env[k];else process.env[k]=v;});
async function open(path,width=1440){
 const context=await browser.createBrowserContext(),page=await context.newPage();page.setDefaultTimeout(20000);await page.setViewport({width,height:1000});
 const[name,...v]=f.owner.cookie.split('=');await page.setCookie({name,value:v.join('='),url:f.base,httpOnly:true,sameSite:'Lax'});
 const errors=[];page.on('pageerror',e=>errors.push(e.message));
 await page.setRequestInterception(true);page.on('request',r=>{if(new URL(r.url()).hostname==='fixture.example')r.respond({status:200,contentType:'image/svg+xml',body:'<svg xmlns="http://www.w3.org/2000/svg" width="60" height="40"><path fill="#70583a" d="M5 8h50v8H5zM10 16h4v20h-4zM46 16h4v20h-4z"/></svg>'}).catch(()=>{});else r.continue().catch(()=>{});});
 await page.goto(f.base+path,{waitUntil:'networkidle0'});return{context,page,errors};
}
async function proof(page,name){if(process.env.ROOM_SCALE_PROOF){mkdirSync(process.env.ROOM_SCALE_PROOF,{recursive:true});await page.screenshot({path:join(process.env.ROOM_SCALE_PROOF,name+'.png'),fullPage:true});}}
test('native planner edits, saves and reopens all forty placements on desktop and narrow screens',async()=>{
 for(const width of [1440,390]){
  const{context,page,errors}=await open('/apps/'+binding.id+'/product-room/?project='+plan.id,width);
  try{
   await page.waitForSelector('[data-planner][data-ready=true]');
   expect(await page.$$eval('[data-ui=selection] [data-select]',n=>n.length)).toBe(40);
   expect(await page.$eval('[data-ui=subtotal]',n=>n.textContent)).toBe('$12,000.00');
   await page.evaluate(()=>document.querySelector('[data-select="placement-39"]').click());
   await page.$eval('[data-ui=x]',(n,v)=>{n.value=v;n.dispatchEvent(new Event('input',{bubbles:true}));},width===1440?'30.5':'30.75');
   await page.click('[data-ui=save]');
   await page.waitForFunction(()=>document.querySelector('[data-ui=save-state]').textContent.startsWith('Saved'));
   const saved=(await store.readLocal(source.records[1].key))[0];
   expect(saved.value.plan.items).toHaveLength(40);expect(saved.value.plan.items[39].x).toBe(width===1440?30.5:30.75);
   await page.reload({waitUntil:'networkidle0'});await page.waitForSelector('[data-planner][data-ready=true]');
   expect(await page.$$eval('[data-ui=selection] [data-select]',n=>n.length)).toBe(40);
   expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1)).toBe(true);
   expect(await page.$eval('[data-ui=selection]',n=>n.clientHeight)).toBeLessThanOrEqual(366);
   await proof(page,'planner-'+width);expect(errors).toEqual([]);
  }finally{await context.close();}
 }
},60000);
test('browser stages forty-placement impact, retains shared review and refuses changed-room review',async()=>{
 const{context,page,errors}=await open(binding.path+'?workspace=releases');
 try{
  await page.waitForSelector('[data-release=grid] .ag-row');
  await page.click('[data-release=grid] .ag-row .ag-selection-checkbox');
  await page.type('[data-release=name]','Forty fixture price review');await page.click('[data-release=stage]');
  await page.waitForFunction(()=>document.querySelector('[data-release=notice]').textContent==='Release staged.');
  expect(await page.$$eval('[data-release=impact] li',n=>n.length)).toBe(40);
  const totals=await page.$eval('[data-release=impact]',n=>n.textContent);
  expect(totals).toContain('$12,000.00');expect(totals).toContain('$16,000.00');expect(totals).toContain('$4,000.00');expect(totals).toContain('$2,000.00 over');
  await page.type('[data-release=note]','Checked all forty fixtures.');await page.click('[data-release=review]');
  await page.waitForFunction(()=>document.querySelector('[data-release=notice]').textContent==='Review saved.');
  let row=(await store.readLocal(releasePrefix))[0];expect(row.value.state).toBe('reviewed');expect(row.value.publication).toBeNull();expect(row.value.impact.impacted[0].items).toHaveLength(40);
  const retained=structuredClone(row),room=(await store.readLocal(source.records[1].key))[0];
  await store.saveLocal(room.key,{...room.value,plan:{...room.value.plan,notes:'Concurrent room correction'}},room.revision);
  await page.click('[data-release=review]');await page.waitForFunction(()=>document.querySelector('[data-release=notice]').textContent.includes('changed after staging'));
  expect((await store.readLocal(row.key))[0]).toEqual(retained);
  await page.click('[data-release=reload]');await page.waitForFunction(()=>document.querySelector('[data-release=state]').textContent.includes('refresh required'));
  expect(await page.$eval('[data-release=review]',n=>n.disabled)).toBe(true);
  await page.click('[data-release=refresh]');await page.waitForFunction(()=>document.querySelector('[data-release=notice]').textContent.startsWith('Sources refreshed'));
  row=(await store.readLocal(row.key))[0];expect(row.value.state).toBe('staged');expect(row.value.review).toBeNull();
  await proof(page,'release-1440');await page.setViewport({width:390,height:844});await proof(page,'release-390');
  expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1)).toBe(true);expect(errors).toEqual([]);
 }finally{await context.close();}
},60000);
