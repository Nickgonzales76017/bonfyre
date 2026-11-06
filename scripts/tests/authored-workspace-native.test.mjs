import {beforeAll,afterAll,test,expect} from 'bun:test';
import {mkdtempSync,rmSync,mkdirSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {createRequire} from 'node:module';
import {materializeAuthoredWorkspace,authoredWorkspaceSetup} from '../materialize_authored_workspace.mjs';
import {publishSetups} from '../publish_experience_setups.mjs';
import {workspaceFixture,root} from '../../cmd/BonfyreWebAddress/tests/workspace_fixture.mjs';
import {createAuthoredWorkspace,addInstrument,connectInstruments,prepareAuthoredBatch,reuseWorkspaceDesign,validateAuthoredWorkspace,productionPrefix} from '../../cmd/BonfyreSurfaceScene/runtime/authored-workspace.mjs';
let temp,pack,f,binding,browser;
const previousWorkspaceEnvironment={BONFYRE_WORKSPACE_RELEASE:process.env.BONFYRE_WORKSPACE_RELEASE,BONFYRE_WORKSPACE_PACKAGES:process.env.BONFYRE_WORKSPACE_PACKAGES};
beforeAll(async()=>{
  temp=mkdtempSync(join(tmpdir(),'authored-workspace-native-'));
  const fixed=process.env.BONFYRE_AUTHORED_RELEASE;
  if(fixed&&!/^[a-f0-9]{64}$/.test(fixed))throw Error('Invalid immutable authoring release.');
  pack=fixed?{id:fixed}:await materializeAuthoredWorkspace({root:join(temp,'packages')});
  process.env.BONFYRE_WORKSPACE_RELEASE=pack.id;if(!fixed)process.env.BONFYRE_WORKSPACE_PACKAGES=join(temp,'packages');
  f=await workspaceFixture();publishSetups({records:authoredWorkspaceSetup(pack.id),binary:f.cms,db:f.db,schemas:join(f.home,'schemas'),apply:true});
  binding=await f.install('experience.connected-instruments.workspace','institution:workspace-team');
  const puppeteer=createRequire(join(root,'tools/creator-ingest/package.json'))('puppeteer');
  browser=await puppeteer.launch({headless:true,executablePath:'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',args:['--no-sandbox','--disable-dev-shm-usage']});
},60000);
afterAll(async()=>{try{await browser?.close();await f?.close();if(temp)rmSync(temp,{recursive:true,force:true});}finally{for(const [key,value]of Object.entries(previousWorkspaceEnvironment)){if(value===undefined)delete process.env[key];else process.env[key]=value;}}});
async function pageFor(user=f.owner){const context=await browser.createBrowserContext(),page=await context.newPage(),errors=[];page.on('pageerror',e=>errors.push(e.message));page.on('dialog',d=>d.accept());const [name,...value]=user.cookie.split('=');await page.setCookie({name,value:value.join('='),url:f.base,httpOnly:true,sameSite:'Lax'});await page.setViewport({width:1600,height:1050});return {context,page,errors};}
async function clickText(page,text,scope='body'){const selector=await page.evaluate((scope,text)=>{const all=[...document.querySelector(scope).querySelectorAll('button')],n=all.find(x=>x.textContent.trim()===text);if(!n)throw Error('No button '+text);n.dataset.testClick='current';return '[data-test-click=current]';},scope,text);await page.click(selector);await page.evaluate(()=>document.querySelector('[data-test-click]')?.removeAttribute('data-test-click'));}
test('design reuse resets record identities without copying source data or authority',()=>{
  let p=addInstrument(createAuthoredWorkspace('Unfamiliar composition'),'research',{role:'source'});p=addInstrument(p,'product',{role:'output'});p=connectInstruments(p,p.panels[0].id,p.panels[1].id);
  const next=reuseWorkspaceDesign(p);expect(next.id).not.toBe(p.id);expect(next.panels.every((v,i)=>v.recordKey!==p.panels[i].recordKey&&v.revision===0)).toBe(true);expect(next.connections[0].from).toBe(next.panels[0].id);expect(Object.keys(next)).not.toContain('credentials');
  const tooMany=structuredClone(p);tooMany.panels.push(...p.panels,...p.panels);expect(()=>validateAuthoredWorkspace(tooMany)).toThrow();
});
test('reviewed saves snapshot changes and check unchanged dependencies without writing them',()=>{
  let p=addInstrument(createAuthoredWorkspace('Reviewed production'),'research');p=addInstrument(p,'product');
  const records=p.panels.map((x,i)=>({id:x.id,kind:x.kind,key:x.recordKey,revision:i+1})),change={key:records[0].key,value:{kind:'research',title:'Reviewed'},expectedRevision:1};
  const batch=prepareAuthoredBatch(p,4,records,[change]);change.value.title='Later edit';p.title='Later title';
  expect(batch.changes[0].value.title).toBe('Reviewed');expect(batch.production.title).toBe('Reviewed production');expect(batch.changes[1]).toEqual({key:records[1].key,expectedRevision:2,checkOnly:true});expect(batch.production.panels.map(x=>x.revision)).toEqual([2,2]);
});
test('a new author composes real blank editors, saves to CMS and reopens exact sources',async()=>{
  const {context,page,errors}=await pageFor();
  try{
    await page.goto(f.base+'/apps/'+binding.id+'/instruments/production/',{waitUntil:'networkidle0'});
    await page.waitForSelector('#production-title',{timeout:8000}).catch(async e=>{throw Error(e.message+'; '+await page.$eval('body',n=>n.innerText.slice(-2500))+'; '+errors.join('; '));});
    await page.$eval('#production-title',n=>{n.value='Community workshop planning';n.dispatchEvent(new Event('input',{bubbles:true}));});
    await page.click('.production-empty button');await page.waitForSelector('[data-instrument-kind=research]');
    await page.select('.production-add select','product');await page.click('.production-add button');await page.waitForSelector('[data-instrument-kind=product]');
    await page.click('#save-production');
    await page.waitForFunction(()=>document.querySelector('.production-status')?.textContent.includes('Saved production'),{timeout:8000}).catch(async e=>{throw Error(e.message+'; '+await page.$eval('.production-status',n=>n.textContent)+'; '+errors.join('; '));});
    const rows=await f.store(binding).readLocal('draft:instruments:'),p=rows.find(r=>r.key.startsWith(productionPrefix));
    expect(p.value.title).toBe('Community workshop planning');expect(p.value.panels.length).toBe(2);expect(rows.filter(r=>['research','product'].includes(r.value.kind)).length).toBe(2);
    const url=page.url();await page.reload({waitUntil:'networkidle0'});await page.waitForSelector('[data-instrument-kind=product]');expect(page.url()).toBe(url);expect(await page.$$eval('.production-pane',n=>n.length)).toBe(2);expect(errors).toEqual([]);
    expect(await page.evaluate(()=>localStorage.length)).toBe(0);expect(await page.evaluate(()=>indexedDB.databases())).toEqual([]);
    if(process.env.BONFYRE_AUTHORING_SCREENSHOTS){mkdirSync(process.env.BONFYRE_AUTHORING_SCREENSHOTS,{recursive:true});await page.screenshot({path:join(process.env.BONFYRE_AUTHORING_SCREENSHOTS,'production-desktop.png'),fullPage:true});}
  }finally{await context.close();}
},60000);
export {clickText};

test('an unfamiliar source becomes a reviewed opportunity in the authored receiving map; layout changes retain edits',async()=>{
  const {context,page,errors}=await pageFor();
  const saved=(await f.store(binding).readLocal(productionPrefix))[0];
  try{
    await page.goto(f.base+'/apps/'+binding.id+'/instruments/production/?record='+encodeURIComponent(saved.key)+'&revision='+saved.revision,{waitUntil:'networkidle0'});
    await page.waitForSelector('[data-instrument-kind=research]');
    const research='[data-instrument-kind=research]',product='[data-instrument-kind=product]';
    await clickText(page,'Add source',research);await page.waitForSelector('dialog[open]');
    await page.type('dialog[open] input','Workshop interviews, October');
    const original='Interview A: Volunteers cannot identify which repairs are safe to begin. They asked for a shared inspection checklist before the Saturday workshop.';
    await page.type('dialog[open] textarea',original);await clickText(page,'Retain source','dialog[open]');
    await page.waitForSelector('#original-source');
    await page.$eval('#original-source',n=>{const range=document.createRange();range.selectNodeContents(n);getSelection().removeAllRanges();getSelection().addRange(range);n.dispatchEvent(new MouseEvent('mouseup',{bubbles:true}));});
    await clickText(page,'Capture selection',research);
    await page.waitForSelector('#finding-statement');await page.type('#finding-statement','Prepare a shared inspection checklist before volunteer repairs begin.');await page.type('#finding-theme','Repair safety');
    await clickText(page,'Create finding',research);
    await page.$eval('[data-instrument-kind=research] .production-domain-inspector select',n=>{n.value='include';n.dispatchEvent(new Event('change',{bubbles:true}));});
    await clickText(page,'Inspect',research);await clickText(page,'Behavior','.production-inspector');
    const target=await page.$eval(product,n=>n.dataset.productionPane);
    await page.select('.production-inspector select[aria-label="Sourced findings go to"]',target);
    await clickText(page,'Offer as product opportunity',research);
    await page.waitForSelector('.production-incoming',{timeout:12000}).catch(async e=>{throw Error(e.message+'; '+await page.$eval('.production-status',n=>n.textContent));});
    expect(await page.$eval('.production-incoming blockquote',n=>n.textContent)).toBe(original);
    await clickText(page,'Add prepared opportunity','.production-incoming');
    await page.waitForFunction(()=>document.querySelector('[data-instrument-kind=product] .production-domain-inspector textarea')?.value.includes('inspection checklist'));
    await clickText(page,'Appearance','.production-inspector');await page.select('[aria-label=Arrangement]','stack');
    await page.select('[aria-label=Canvas]','night');
    expect(await page.$eval('[data-instrument-kind=product] .production-domain-inspector textarea',n=>n.value)).toContain('inspection checklist');
    await page.click('#save-production');await page.waitForFunction(()=>document.querySelector('.production-status')?.textContent.startsWith('Saved production'));
    const rows=await f.store(binding).readLocal('draft:instruments:'),composition=rows.find(r=>r.key===saved.key),plan=rows.find(r=>r.key===composition.value.panels.find(p=>p.kind==='product').recordKey);
    expect(composition.value.layout).toBe('stack');expect(composition.value.appearance).toBe('night');expect(composition.value.connections.length).toBe(1);
    expect(plan.value.stories.length).toBe(1);expect(plan.value.stories[0].evidence[0].quote).toBe(original);expect(plan.value.stories[0].origin.record).toStartWith('draft:instruments:research:');
    const before=composition.value.panels.map(p=>p.revision);await page.click('#save-production');await page.waitForFunction(rev=>location.search.includes('revision='+rev),{},composition.revision+1);
    const after=(await f.store(binding).readLocal(saved.key)).find(r=>r.key===saved.key);expect(after.value.panels.map(p=>p.revision)).toEqual(before);
    await page.setViewport({width:390,height:950});expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);
    await page.emulateMediaFeatures([{name:'prefers-reduced-motion',value:'reduce'}]);expect(await page.$eval('.production-pane',n=>getComputedStyle(n).animationName)).toBe('none');
    expect(errors).toEqual([]);expect(await page.evaluate(()=>localStorage.length)).toBe(0);
    if(process.env.BONFYRE_AUTHORING_SCREENSHOTS){await page.setViewport({width:1600,height:1050});await page.screenshot({path:join(process.env.BONFYRE_AUTHORING_SCREENSHOTS,'production-source-to-output.png'),fullPage:true});}
  }finally{await context.close();}
},60000);

test('current native destination authority and source revisions guard the entire production',async()=>{
  const saved=(await f.store(binding).readLocal(productionPrefix))[0];
  await expect(f.store(binding,f.other).readLocal(productionPrefix)).rejects.toThrow();
  await expect(f.store(binding,f.reader).saveLocal(saved.key,{...saved.value,title:'Reader overwrite'},saved.revision)).rejects.toThrow();
  const refs=saved.value.panels,source=refs.find(x=>x.kind==='research'),store=f.store(binding),row=(await store.readLocal(source.recordKey))[0];
  await store.saveLocal(row.key,{...row.value,title:'Reviewed separately by another editor'},row.revision);
  const attempt=prepareAuthoredBatch(saved.value,saved.revision,refs.map(x=>({id:x.id,kind:x.kind,key:x.recordKey,revision:x.revision})),[]);
  await expect(store.saveLocalBatch(attempt.changes)).rejects.toThrow();expect((await store.readLocal(saved.key))[0].revision).toBe(saved.revision);
});

test('a different composition runs SQLite and imported process data, parks editors and enters through the native CMS',async()=>{
  const {context,page,errors}=await pageFor();
  try{
    const route=await f.request('/cms/productions?context=institution:workspace-team',{cookie:f.owner.cookie});
    expect(route.status).toBe(303);expect(route.headers.get('location')).toBe('/apps/'+binding.id+'/instruments/production/');
    expect((await f.request('/cms/productions?context=institution:workspace-team',{cookie:f.other.cookie})).status).toBe(403);
    await page.goto(f.base+route.headers.get('location'),{waitUntil:'networkidle0'});await page.waitForSelector('#production-title');
    await page.$eval('#production-title',n=>{n.value='Workshop operations rehearsal';n.dispatchEvent(new Event('input',{bubbles:true}));});
    await page.select('.production-add select','schema');await page.click('.production-add button');await page.waitForSelector('[data-instrument-kind=schema] input[placeholder="New name"]');
    await page.type('[data-instrument-kind=schema] input[placeholder="New name"]','review_status');await clickText(page,'Run on isolated working copy','[data-instrument-kind=schema]');
    await page.waitForFunction(()=>[...document.querySelectorAll('[data-instrument-kind=schema] button')].some(n=>n.textContent==='Use this tested database'));
    await clickText(page,'Use this tested database','[data-instrument-kind=schema]');
    await page.select('.production-add select','traces');await page.click('.production-add button');await page.waitForSelector('[data-instrument-kind=traces]');await clickText(page,'Import event log','[data-instrument-kind=traces]');
    const csv='case_id,activity,start,end,resource,source_url\nrepair-17,Inspect,2026-10-19T09:00:00Z,2026-10-19T09:08:00Z,A,\nrepair-17,Test,2026-10-19T09:10:00Z,2026-10-19T09:15:00Z,A,\nrepair-22,Inspect,2026-10-19T09:02:00Z,2026-10-19T09:06:00Z,B,\nheld-23,Inspect,not-a-date,,B,\n';
    await page.$eval('dialog[open] textarea',(n,text)=>{n.value=text;n.dispatchEvent(new Event('input',{bubbles:true}));},csv);await clickText(page,'Validate and preview','dialog[open]');
    await page.waitForFunction(()=>document.querySelector('dialog[open]')?.innerText.includes('3 valid events · 1 held · 2 cases'));
    await clickText(page,'Use valid records','dialog[open]');
    for(const kind of ['research','product']){await page.select('.production-add select',kind);await page.click('.production-add button');await page.waitForSelector('[data-instrument-kind='+kind+']');expect(await page.$$eval('.production-pane',nodes=>nodes.length)).toBeLessThanOrEqual(2);}
    await page.click('#save-production');await page.waitForFunction(()=>document.querySelector('.production-status')?.textContent.startsWith('Saved production'));
    const key=new URL(page.url()).searchParams.get('record'),p=(await f.store(binding).readLocal(key))[0];expect(p.value.panels.length).toBe(4);
    const schema=(await f.store(binding).readLocal(p.value.panels.find(x=>x.kind==='schema').recordKey))[0];expect(schema.value.bytes.byteLength).toBeGreaterThan(1000);expect(schema.value.acceptedSQL.join(' ')).toContain('review_status');
    const trace=(await f.store(binding).readLocal(p.value.panels.find(x=>x.kind==='traces').recordKey))[0];expect(trace.value.text).toBe(csv);
    await page.click('.production-outline-row button');await page.waitForSelector('[data-instrument-kind=schema]');expect(await page.$eval('[data-instrument-kind=schema]',n=>n.innerText)).toContain('review_status');
    await page.setViewport({width:390,height:950});expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);expect(errors).toEqual([]);
    if(process.env.BONFYRE_AUTHORING_SCREENSHOTS)await page.screenshot({path:join(process.env.BONFYRE_AUTHORING_SCREENSHOTS,'production-mobile.png'),fullPage:true});
  }finally{await context.close();}
},60000);
