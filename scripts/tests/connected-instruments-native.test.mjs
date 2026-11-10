// Exact public implementations over disposable native Auth/CMS/WebAddress.
// Browser controls, not injected application state, exercise the receiving flow.
import {test,expect,beforeAll,afterAll} from 'bun:test';
import {mkdtempSync,rmSync} from 'node:fs';
import {join} from 'node:path';
import {homedir,tmpdir} from 'node:os';
import {createRequire} from 'node:module';
import {loadWorkspaceFiles} from '../workspace_material.mjs';
import {projectConnectedInstrumentMaterial,connectedInstrumentViews} from '../materialize_workspace_package.mjs';
import {projectConnectedInstrumentsSetup,publishSetups} from '../publish_experience_setups.mjs';
import {workspaceFixture,root} from '../../cmd/BonfyreWebAddress/tests/workspace_fixture.mjs';
const sourceId='fad60f319bc25583dce678605babb7c4946c52d343ae8c005458a0c163b7acfb';
const commit='396bcaf02f24bd96c1789aee5410d111edbf0818',sourceTree='e27b1a7640d51bbace199793471a271674513f43';
const packName='experience.connected-instruments.workspace',prefix='draft:instruments:';
const names=['BONFYRE_WORKSPACE_RELEASE','BONFYRE_WORKSPACE_PACKAGES'];
const prior=Object.fromEntries(names.map(k=>[k,process.env[k]]));
let temp,f,pack,binding,personal,browser,connectedURL,researchRow,productRow;
const errors=[],external=[];
beforeAll(async()=>{
  temp=mkdtempSync(join(tmpdir(),'connected-instruments-'));
  const source=await loadWorkspaceFiles(join(homedir(),'.bonfyre/workspace-packages'),sourceId);
  pack=await projectConnectedInstrumentMaterial(source,{commit,sourceTree,root:join(temp,'packages')});
  process.env.BONFYRE_WORKSPACE_RELEASE=pack.id;process.env.BONFYRE_WORKSPACE_PACKAGES=join(temp,'packages');
  f=await workspaceFixture();const setup={records:projectConnectedInstrumentsSetup(pack.id),binary:f.cms,db:f.db,schemas:join(f.home,'schemas'),apply:true};
  expect(publishSetups(setup).created).toBe(9);expect(publishSetups(setup).created).toBe(0);
  binding=await f.install(packName,'institution:workspace-team');personal=await f.install(packName,'self');
  const puppeteer=createRequire(join(root,'tools/creator-ingest/package.json'))('puppeteer');
  browser=await puppeteer.launch({executablePath:'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',headless:true,userDataDir:join(temp,'chrome'),args:['--disable-background-networking']});
},60000);
afterAll(async()=>{await browser?.close();await f?.close();if(temp)rmSync(temp,{recursive:true,force:true});for(const k of names)if(prior[k]===undefined)delete process.env[k];else process.env[k]=prior[k];});
async function pageFor(user=f.owner){
  const context=await browser.createBrowserContext(),page=await context.newPage();page.setDefaultTimeout(12000);
  const [name,...parts]=user.cookie.split('=');await page.setCookie({name,value:parts.join('='),url:f.base,httpOnly:true,sameSite:'Lax'});
  await page.setViewport({width:1440,height:1000});page.on('pageerror',e=>errors.push(e.message));
  await page.setRequestInterception(true);page.on('request',req=>{if(req.url().startsWith(f.base)||req.url().startsWith('blob:')||req.url().startsWith('data:'))req.continue();else{external.push(req.url());req.abort();}});
  page.on('dialog',d=>d.dismiss());return {page,context};
}
const click=(page,text,scope='body')=>page.evaluate(({text,scope})=>{const b=[...document.querySelector(scope).querySelectorAll('button')].find(b=>b.textContent.trim()===text&&!b.disabled&&b.getClientRects().length);if(!b)throw Error('Button unavailable: '+text);b.click();},{text,scope});
const field=(page,label,value,scope='body')=>page.evaluate(({label,value,scope})=>{const l=[...document.querySelector(scope).querySelectorAll('label')].find(l=>l.textContent.startsWith(label));const n=l?.querySelector('input,select,textarea');if(!n)throw Error('Field unavailable: '+label);n.value=value;n.dispatchEvent(new Event('input',{bubbles:true}));n.dispatchEvent(new Event('change',{bubbles:true}));},{label,value,scope});
const ready=page=>page.waitForFunction(()=>document.querySelector('[data-working-pane="primary"]')&&document.querySelector('#title')?.textContent!=='Opening your workspace…'&&document.querySelector('#main-pane')?.children.length>0&&!document.querySelector('[data-open-failure]')&&!document.querySelector('#save').disabled);
const noLocal=page=>page.evaluate(async()=>({keys:Object.keys(localStorage),databases:(await indexedDB.databases()).map(x=>x.name)}));
test('four actual editor views share the declared native family and preserve exact source material',async()=>{
  const response=await f.raw(binding.path),html=await response.text();expect(response.status).toBe(200);expect(html).toContain(binding.endpoint);expect(html).not.toContain('__WORKSPACE__');
  const files=await loadWorkspaceFiles(join(temp,'packages'),pack.id),source=await loadWorkspaceFiles(join(homedir(),'.bonfyre/workspace-packages'),sourceId);
  for(const name of ['research.mjs','product.mjs','schema.mjs','traces.mjs','research-view.mjs','product-view.mjs','schema-view.mjs','trace-view.mjs','runtime/workspace-composition.mjs','vendor/sql-wasm.wasm','UPSTREAM-LICENSE.txt'])expect(files.get('instruments/'+name)).toEqual(source.get(name));
  expect(connectedInstrumentViews.map(v=>v.view)).toEqual(['traces','research','schema','product']);
  expect(files.get('instruments/runtime/store.mjs').toString()).toContain("import('../../bridge/workspace-store.mjs')");
});
test('unfamiliar research becomes a prepared product opportunity and both edits survive a joint native save',async()=>{
  const {page,context}=await pageFor();try{
    await page.goto(f.base+binding.path+'?tool=research');await ready(page);await click(page,'Add source');
    await field(page,'Title','Repair workshop field notes','dialog');await field(page,'Original text','Operators need exact battery counts before scheduling collections. Shared pickups must preserve each school’s accepted allocation.','dialog');
    await click(page,'Retain source','dialog');await page.waitForFunction(()=>document.querySelector('#original-source')?.textContent.startsWith('Operators need'));
    await page.evaluate(()=>{const n=document.querySelector('#original-source').firstChild,r=document.createRange();r.setStart(n,0);r.setEnd(n,n.textContent.indexOf('.')+1);getSelection().removeAllRanges();getSelection().addRange(r);});
    await click(page,'Capture selection');await field(page,'Your finding','Resolve received quantities before assigning pickup work');await field(page,'Theme','Receiving and logistics');await click(page,'Create finding');
    await field(page,'Synthesis decision','include');await click(page,'Offer as product opportunity');await page.waitForSelector('[data-incoming]');
    expect(await page.$eval('[data-incoming] blockquote',n=>n.textContent)).toBe('Operators need exact battery counts before scheduling collections.');
    await click(page,'Add prepared opportunity','[data-working-pane="companion"]');await page.waitForFunction(()=>[...document.querySelectorAll('[data-working-pane="companion"] textarea')].some(n=>n.value.includes('Resolve received quantities')));
    await field(page,'Estimated reach','12','[data-working-pane="companion"]');await field(page,'Impact estimate','2','[data-working-pane="companion"]');await field(page,'Confidence %','60','[data-working-pane="companion"]');await field(page,'Effort units','2','[data-working-pane="companion"]');
    await click(page,'Save connected work');await page.waitForFunction(()=>new URL(location.href).searchParams.has('withRecord'));
    connectedURL=page.url();const rows=await f.store(binding).readLocal(prefix);researchRow=rows.find(r=>r.value.kind==='research');productRow=rows.find(r=>r.value.kind==='product');
    const incoming=rows.find(r=>r.key.startsWith(prefix+'incoming:')),story=productRow.value.stories.find(s=>s.origin?.incoming===incoming.key);
    expect(researchRow.revision).toBe(2);expect(productRow.revision).toBe(1);expect(incoming.revision).toBe(1);expect(story.reach).toBe(12);expect(story.confidence).toBe(60);expect(story.evidence[0].quote).toBe(researchRow.value.findings[0].evidence[0].quote);expect(story.origin.record).toBe(researchRow.key);
    expect(await noLocal(page)).toEqual({keys:[],databases:[]});
    await page.reload();await ready(page);await page.waitForFunction(()=>document.querySelector('[data-working-pane="companion"] .linked-status')?.textContent.startsWith('Opened exact saved revision'));
    expect(await page.$eval('#findings',n=>n.textContent)).toContain('Resolve received quantities');
    expect(await page.$eval('[data-working-pane="companion"]',n=>n.textContent)).toContain('Resolve received quantities');
    await page.setViewport({width:390,height:844});expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);
  }finally{await context.close();}
},60000);
test('another authorized browser reopens connected work; its revision cannot be overwritten by stale review',async()=>{
  const {page,context}=await pageFor(f.writer);try{
    await page.goto(connectedURL);await ready(page);await page.waitForFunction(()=>document.querySelector('[data-working-pane="companion"] .linked-status')?.textContent.startsWith('Opened exact saved revision'));
    expect(await page.$eval('#findings',n=>n.textContent)).toContain(researchRow.value.findings[0].statement);
    const store=f.store(binding,f.writer);await store.saveLocal(researchRow.key,{...researchRow.value,title:'Writer retained the same research'},researchRow.revision);
    const prior=productRow.revision;
    await expect(f.store(binding).saveLocalBatch([{key:researchRow.key,value:researchRow.value,expectedRevision:researchRow.revision},{key:productRow.key,value:productRow.value,expectedRevision:prior}])).rejects.toMatchObject({status:409});
    expect((await store.readVersion(productRow.key)).revision).toBe(prior);expect(await noLocal(page)).toEqual({keys:[],databases:[]});
  }finally{await context.close();}
},30000);
test('native access and unrelated destinations remain isolated for the full connected record family',async()=>{
  const store=f.store(binding);expect((await f.store(personal).readLocal()).length).toBe(0);
  await expect(f.store(binding,f.reader).saveLocal(productRow.key,productRow.value,1)).rejects.toMatchObject({status:403});
  await expect(f.store(binding,f.other).readLocal()).rejects.toMatchObject({status:403});
  await expect(store.saveLocal('draft:catalogue:unrelated',{title:'Wrong family'},0)).rejects.toThrow();
  expect(f.sql.query("SELECT COUNT(*) n FROM _workspace_record WHERE record_key='draft:catalogue:unrelated'").get().n).toBe(0);
  const views=await f.raw('/apps/'+binding.id+'/_views?'+new URLSearchParams({key:productRow.key,revision:'1',release:pack.id}));expect(views.status).toBe(200);
  const link=(await views.text()).match(/data-compatible-view href="([^"]+)"/)[1].replaceAll('&amp;','&'),opened=await f.raw(link);expect(opened.status).toBe(303);expect(new URL(opened.headers.get('location'),f.base).searchParams.get('record')).toBe(productRow.key);
});
test('an unfamiliar observed event log is validated, inspected and saved through its native owner',async()=>{
  const {page,context}=await pageFor();try{
    await page.goto(f.base+binding.path+'?tool=traces');await ready(page);await click(page,'Import event log');
    await page.$eval('textarea[aria-label="Event log CSV"]',n=>{n.value='case_id,activity,start,end,resource,source_url\nlot-A,receive,2026-10-19T10:00:00Z,2026-10-19T10:02:00Z,operator,\nlot-A,inspect,2026-10-19T10:03:00Z,2026-10-19T10:06:00Z,technician,\nlot-B,receive,2026-10-19T11:00:00Z,2026-10-19T11:02:00Z,operator,\n';n.dispatchEvent(new Event('input',{bubbles:true}));});
    await click(page,'Validate and preview','dialog');await page.waitForFunction(()=>[...document.querySelectorAll('dialog button')].some(b=>b.textContent==='Use valid records'&&!b.disabled));await click(page,'Use valid records','dialog');await click(page,'Save');await page.waitForFunction(()=>new URL(location.href).searchParams.has('record'));
    const key=new URL(page.url()).searchParams.get('record'),row=await f.store(binding).readVersion(key);expect(row.value.text).toContain('lot-B');expect(await page.$eval('#trace-case-list',n=>n.textContent)).toContain('lot-A');expect(await noLocal(page)).toEqual({keys:[],databases:[]});
  }finally{await context.close();}
},30000);
test('real SQLite migration executes and the accepted bytes reopen from the native record',async()=>{
  const {page,context}=await pageFor();try{
    await page.goto(f.base+binding.path+'?tool=schema');await ready(page);await field(page,'New column, table or index name','verification_note');await click(page,'Run on isolated working copy');
    await page.waitForFunction(()=>[...document.querySelectorAll('button')].some(b=>b.textContent==='Use this tested database'));await click(page,'Use this tested database');await ready(page);await click(page,'Save');await page.waitForFunction(()=>new URL(location.href).searchParams.has('record'));
    const key=new URL(page.url()).searchParams.get('record'),row=await f.store(binding).readVersion(key);expect(row.value.bytes.byteLength).toBeGreaterThan(1000);expect(row.value.acceptedSQL.join('\n')).toContain('verification_note');
    await page.reload();await ready(page);expect(await page.$eval('.schema-inner',n=>n.textContent)).toContain('verification_note');expect(await noLocal(page)).toEqual({keys:[],databases:[]});
  }finally{await context.close();}
},30000);
test('the isolated browser journeys had no application exceptions or external requests',()=>{expect(errors).toEqual([]);expect(external).toEqual([]);});
