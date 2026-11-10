import {homedir} from 'node:os';
import {loadWorkspaceFiles,retainWorkspaceMaterial} from '../workspace_material.mjs';
import {encode} from '../../integrations/browser/workspace-msgpack.mjs';
import {beforeAll,afterAll,test,expect} from 'bun:test';
import {mkdtempSync,rmSync,mkdirSync,copyFileSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {createRequire} from 'node:module';
import {materializeAuthoredWorkspace,authoredWorkspaceSetup} from '../materialize_authored_workspace.mjs';
import {publishSetups} from '../publish_experience_setups.mjs';
import {workspaceFixture,root} from '../../cmd/BonfyreWebAddress/tests/workspace_fixture.mjs';
import {createAuthoredWorkspace,addInstrument,connectInstruments,prepareAuthoredBatch,reuseWorkspaceDesign,validateAuthoredWorkspace,productionPrefix} from '../../cmd/BonfyreSurfaceScene/runtime/authored-workspace.mjs';
let temp,pack,f,binding,browser;
const prior=Object.fromEntries(['BONFYRE_WORKSPACE_RELEASE','BONFYRE_WORKSPACE_PACKAGES'].map(key=>[key,process.env[key]]));
beforeAll(async()=>{
  temp=mkdtempSync(join(tmpdir(),'authored-workspace-native-'));
  const fixed=process.env.BONFYRE_TEST_WORKSPACE_RELEASE;
  if(fixed){if(!/^[a-f0-9]{64}$/.test(fixed))throw Error('Choose an exact test package.');pack=await retainWorkspaceMaterial(await loadWorkspaceFiles(join(homedir(),'.bonfyre/workspace-packages'),fixed),{root:join(temp,'packages')});expect(pack.id).toBe(fixed);}
  else pack=await materializeAuthoredWorkspace({root:join(temp,'packages')});
  process.env.BONFYRE_WORKSPACE_RELEASE=pack.id;process.env.BONFYRE_WORKSPACE_PACKAGES=join(temp,'packages');
  f=await workspaceFixture();publishSetups({records:authoredWorkspaceSetup(pack.id),binary:f.cms,db:f.db,schemas:join(f.home,'schemas'),apply:true});
  for(const type of ['publication_occurrence','internet_source'])copyFileSync(join(root,'cmd/BonfyreCMS/content-types',type+'.json'),join(f.home,'schemas',type+'.json'));
  f.run(f.cms,['schema','migrate','--db',f.db,'--schemas',join(f.home,'schemas')]);
  f.add('internet_source',{source:'fixture-feed',family:'rss',label:'Fixture feed',says:'Fixture publisher',state:'observed',url:'https://example.test/feed',status:'published'});
  for(const [id,body,extra]of [['0','Operators need exact battery counts before scheduling collections. Shared pickups preserve accepted allocations.',{}],['1','Staff review quantities before agreeing on collection dates.',{}],['empty','',{}],['private','Private source',{visibility:'private'}]])f.add('publication_occurrence',{occurrence_id:'fixture-'+id,source_twin:'twin:fixture',source_ref:'fixture-feed',canonical_href:'https://example.test/'+id,title:'Collection report '+id,summary:body,observed_at:'2026-10-20',receipt_ref:'receipt:'+id,rights_basis:'Fictional excerpt for isolated testing',retention:'excerpt',visibility:'public',assertion_state:'OBSERVED',cannot_infer:'No publication permission.',status:'published',...extra});
  binding=await f.install('experience.connected-instruments.workspace','institution:workspace-team');
  const puppeteer=createRequire(join(root,'tools/creator-ingest/package.json'))('puppeteer');
  browser=await puppeteer.launch({headless:true,executablePath:'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',args:['--disable-background-networking']});
},60000);
afterAll(async()=>{try{await browser?.close();await f?.close();if(temp)rmSync(temp,{recursive:true,force:true});}finally{for(const[key,value]of Object.entries(prior))if(value===undefined)delete process.env[key];else process.env[key]=value;}});
async function pageFor(user=f.owner){const context=await browser.createBrowserContext(),page=await context.newPage(),errors=[];page.on('pageerror',e=>errors.push(e.message));page.on('dialog',d=>d.accept());const [name,...value]=user.cookie.split('=');await page.setCookie({name,value:value.join('='),url:f.base,httpOnly:true,sameSite:'Lax'});await page.setViewport({width:1600,height:1050});return {context,page,errors};}
async function clickText(page,text,scope='body'){const selector=await page.evaluate((scope,text)=>{const all=[...document.querySelector(scope).querySelectorAll('button')],n=all.find(x=>x.textContent.trim()===text);if(!n)throw Error('No button '+text);n.dataset.testClick='current';return '[data-test-click=current]';},scope,text);await page.click(selector);await page.evaluate(()=>document.querySelector('[data-test-click]')?.removeAttribute('data-test-click'));}

const field=(page,label,value,scope='body')=>page.evaluate(({label,value,scope})=>{const l=[...document.querySelector(scope).querySelectorAll('label')].find(l=>l.textContent.startsWith(label));const n=l?.querySelector('input,select,textarea');if(!n)throw Error('Field unavailable: '+label);n.value=value;n.dispatchEvent(new Event('input',{bubbles:true}));n.dispatchEvent(new Event('change',{bubbles:true}));},{label,value,scope});
const research='[data-instrument-kind=research]',product='[data-instrument-kind=product]';
async function settle(page){await page.waitForFunction(()=>!document.querySelector('#production-authoring').inert);}
test('an author selects retained sources, reviews a finding, prepares a real product map and reopens the production',async()=>{
  const {context,page,errors}=await pageFor();page.setDefaultTimeout(12000);
  try{
    await page.goto(f.base+'/apps/'+binding.id+'/instruments/production/',{waitUntil:'networkidle0'});
    await page.waitForSelector('#production-title');
    await page.$eval('#production-title',n=>{n.value='Collection operations';n.dispatchEvent(new Event('input',{bubbles:true}));});
    await page.click('.production-empty button');await page.waitForSelector(research);await settle(page);
    await clickText(page,'Choose retained sources');await page.waitForSelector('input[aria-label="Select Collection report 0"]:enabled');
    expect(await page.$$eval('[data-publication-picker-results] article',n=>n.length)).toBe(3);
    expect(await page.$eval('input[aria-label="Select Collection report empty"]',n=>n.disabled)).toBe(true);
    await page.click('input[aria-label="Select Collection report 0"]');await clickText(page,'Add selected excerpts','dialog[open]');
    await page.waitForSelector('#original-source');expect((await f.store(binding).readLocal('draft:instruments:research:')).length).toBe(0);
    await page.select('.production-add select','product');await page.click('.production-add button');await page.waitForSelector(product);await settle(page);
    const target=await page.$eval(product,n=>n.dataset.productionPane);await clickText(page,'Inspect',research);await clickText(page,'Behavior','.production-inspector');await page.select('[aria-label="Sourced findings go to"]',target);await settle(page);
    await page.evaluate(()=>{const n=document.querySelector('#original-source [data-original-start="0"]').firstChild,r=document.createRange();r.setStart(n,0);r.setEnd(n,n.textContent.indexOf('.')+1);getSelection().removeAllRanges();getSelection().addRange(r);});
    await clickText(page,'Capture selection',research);await field(page,'Your finding','Resolve received quantities before assigning pickup work',research);await field(page,'Theme','Receiving and logistics',research);await clickText(page,'Create finding',research);await field(page,'Synthesis decision','include',research);
    await clickText(page,'Appearance','.production-inspector');await page.select('[aria-label=Arrangement]','stack');await settle(page);
    expect(await page.$eval('#findings',n=>n.textContent)).toContain('Resolve received quantities');
    await clickText(page,'Offer as product opportunity',research);await page.waitForSelector('[data-incoming]');
    expect(await page.$eval('[data-incoming] blockquote',n=>n.textContent)).toBe('Operators need exact battery counts before scheduling collections.');
    await clickText(page,'Add prepared opportunity',product);await settle(page);
    await field(page,'Estimated reach','12',product);await field(page,'Impact estimate','2',product);await field(page,'Confidence %','60',product);await field(page,'Effort units','2',product);
    let held;await page.setRequestInterception(true);page.on('request',r=>{if(r.method()==='POST'&&new URL(r.url()).searchParams.get('operation')==='write'&&!held)held=r;else void r.continue();});
    await page.click('#save-production');await page.waitForFunction(()=>document.querySelector('#production-title').disabled);
    expect(await page.$eval('#production-description',n=>n.disabled)).toBe(true);
    for(let i=0;i<100&&!held;i++)await new Promise(r=>setTimeout(r,10));expect(Boolean(held)).toBe(true);await held.continue();
    await settle(page);await page.setRequestInterception(false);page.removeAllListeners('request');
    expect(await page.$eval('.production-status',n=>n.textContent)).toContain('Saved production');
    const rows=await f.store(binding).readLocal('draft:instruments:'),saved=rows.find(r=>r.value.kind==='composition'),review=rows.find(r=>r.value.kind==='research'),map=rows.find(r=>r.value.kind==='product');
    expect(saved.value.layout).toBe('stack');expect(review.value.sources[0].publication.receipt).toBe('receipt:0');expect(map.value.stories[0].reach).toBe(12);expect(map.value.stories[0].evidence[0].quote).toBe(review.value.findings[0].evidence[0].quote);
    await page.reload({waitUntil:'networkidle0'});await page.waitForFunction(()=>document.querySelector('.production-status')?.textContent.startsWith('Opened production'));expect(await page.$eval('#findings',n=>n.textContent)).toContain('Resolve received quantities');
    await clickText(page,'Inspect',research);await clickText(page,'Choose retained sources');await page.waitForSelector('input[aria-label="Select Collection report 1"]:enabled');await page.click('input[aria-label="Select Collection report 1"]');await clickText(page,'Add selected excerpts','dialog[open]');await page.waitForFunction(()=>!document.querySelector('dialog[open]'));
    expect(await page.$eval('#findings',n=>n.textContent)).toContain('Resolve received quantities');
    await page.click('#save-production');await settle(page);expect((await f.store(binding).readVersion(review.key)).value.sources.length).toBe(2);
    expect(await page.evaluate(()=>localStorage.length)).toBe(0);expect(errors).toEqual([]);
    mkdirSync('/private/tmp/bonfyre-production-source/proof',{recursive:true});await page.screenshot({path:'/private/tmp/bonfyre-production-source/proof/production-populated.png',fullPage:true});
  }catch(e){throw Error(e.message+'; '+await page.$eval('body',n=>n.innerText.slice(-7000))+'; '+errors.join('; '));}finally{await context.close();}
},60000);

test('a failed production replacement preserves the current dirty canvas; a read-only member cannot save it',async()=>{
 const {context,page,errors}=await pageFor();page.setDefaultTimeout(10000);
 try{
  const store=f.store(binding),existing=(await store.readLocal(productionPrefix)).find(r=>r.value.title==='Collection operations');
  let broken=addInstrument(createAuthoredWorkspace('Unavailable source editor'),'research');broken.panels[0].revision=1;
  await store.saveLocal(broken.panels[0].recordKey,{kind:'research',title:'Broken fixture',sources:[null],findings:[]},0);await store.saveLocal(productionPrefix+broken.id,broken,0);
  await page.goto(f.base+'/apps/'+binding.id+'/instruments/production/?record='+encodeURIComponent(existing.key),{waitUntil:'networkidle0'});await page.waitForFunction(()=>document.querySelector('.production-status')?.textContent.startsWith('Opened production'));
  await page.$eval('#production-description',n=>{n.value='Unsaved operational context';n.dispatchEvent(new Event('input',{bubbles:true}));});
  await clickText(page,'Open saved');await page.waitForSelector('dialog[open]');await clickText(page,'Unavailable source editor · revision 1','dialog[open]');await settle(page);
  expect(await page.$eval('.production-status',n=>n.dataset.error)).toBe('true');expect(await page.$eval('#production-description',n=>n.value)).toBe('Unsaved operational context');expect(await page.$$eval('.production-pane',n=>n.length)).toBe(2);expect(await page.$eval('#findings',n=>n.textContent)).toContain('Resolve received quantities');expect(await page.$eval('#production-authoring',n=>n.dataset.dirty)).toBe('true');
  await page.click('#save-production');await settle(page);expect((await store.readVersion(existing.key)).value.description).toBe('Unsaved operational context');
  await page.setViewport({width:390,height:844});expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);await page.screenshot({path:'/private/tmp/bonfyre-production-source/proof/production-mobile.png',fullPage:true});expect(errors).toEqual([]);
  const reader=await pageFor(f.reader);try{await reader.page.goto(f.base+'/apps/'+binding.id+'/instruments/production/?record='+encodeURIComponent(existing.key),{waitUntil:'networkidle0'});await reader.page.waitForFunction(()=>document.querySelector('.production-status')?.textContent.startsWith('Opened production'));await reader.page.$eval('#production-title',n=>{n.value='Unauthorized change';n.dispatchEvent(new Event('input',{bubbles:true}));});const before=await store.readVersion(existing.key);await reader.page.click('#save-production');await settle(reader.page);expect(await reader.page.$eval('.production-status',n=>n.dataset.error)).toBe('true');expect((await store.readVersion(existing.key)).revision).toBe(before.revision);expect(await reader.page.$eval('#production-title',n=>n.value)).toBe('Unauthorized change');}finally{await reader.context.close();}
 }finally{await context.close();}
},60000);

test('refreshed excerpts require a fresh selection and blank production metadata requires a leave decision',async()=>{
 const {context,page}=await pageFor();page.setDefaultTimeout(10000);
 try{
  await page.goto(f.base+'/apps/'+binding.id+'/instruments/production/',{waitUntil:'networkidle0'});await page.waitForSelector('#production-title');
  page.removeAllListeners('dialog');let confirmed=0;page.on('dialog',d=>{confirmed++;void d.dismiss();});await page.$eval('#production-description',n=>{n.value='Unsaved brief';n.dispatchEvent(new Event('input',{bubbles:true}));});await clickText(page,'New');await settle(page);expect(confirmed).toBe(1);expect(await page.$eval('#production-description',n=>n.value)).toBe('Unsaved brief');
  await page.click('.production-empty button');await page.waitForSelector(research);await settle(page);await clickText(page,'Behavior','.production-inspector');expect(await page.$eval('.production-inspector a',n=>new URL(n.href).searchParams.get('context'))).toBe('institution:workspace-team');await clickText(page,'Source','.production-inspector');
  await clickText(page,'Choose retained sources');await page.waitForSelector('input[aria-label="Select Collection report 0"]:enabled');await page.click('input[aria-label="Select Collection report 0"]');
  const corrected=encode([1,[['fixture-0','Collection report 0','Corrected retained excerpt.','https://example.test/0','fixture-feed','receipt:0-corrected','Fictional excerpt for isolated testing','excerpt','']],0]);
  await page.setRequestInterception(true);page.on('request',r=>{if(new URL(r.url()).pathname.endsWith('/_publication_inputs'))void r.respond({status:200,contentType:'application/msgpack',body:Buffer.from(corrected)});else void r.continue();});
  await clickText(page,'Search','dialog[open]');await page.waitForFunction(()=>document.querySelector('dialog[open] [role=status]')?.textContent.includes('changed'));
  expect(await page.$eval('input[aria-label="Select Collection report 0"]',n=>n.checked)).toBe(false);expect(await page.$eval('[data-publication-picker-selection]',n=>n.children.length)).toBe(0);
  await page.click('input[aria-label="Select Collection report 0"]');await clickText(page,'Add selected excerpts','dialog[open]');await page.waitForSelector('#original-source');expect(await page.$eval('#original-source',n=>n.textContent)).toBe('Corrected retained excerpt.');
  await page.setRequestInterception(false);page.removeAllListeners('request');
 }finally{await context.close();}
},60000);

for(const change of ['correction','withdrawal'])test('Production Studio refuses '+change+' after selection and keeps its unsaved draft',async()=>{
 const {context,page,errors}=await pageFor();page.setDefaultTimeout(12000);const row=f.sql.query("SELECT id,summary,status FROM publication_occurrence WHERE occurrence_id='fixture-0'").get();
 try{
  await page.goto(f.base+'/apps/'+binding.id+'/instruments/production/',{waitUntil:'networkidle0'});await page.waitForSelector('#production-title');
  await page.$eval('#production-title',n=>{n.value='Retain this draft';n.dispatchEvent(new Event('input',{bubbles:true}));});
  await page.click('.production-empty button');await page.waitForSelector(research);await settle(page);
  const before=await f.store(binding).readLocal('draft:instruments:');
  await clickText(page,'Choose retained sources');await page.waitForSelector('input[aria-label="Select Collection report 0"]:enabled');await page.click('input[aria-label="Select Collection report 0"]');
  f.run(f.cms,['entry','update','publication_occurrence',String(row.id),JSON.stringify(change==='correction'?{summary:'Corrected source after selection.'}:{status:'withdrawn'}),'--db',f.db,'--schemas',join(f.home,'schemas')]);
  await clickText(page,'Add selected excerpts','dialog[open]');await page.waitForFunction(()=>document.querySelector('dialog[open] [role=status]')?.textContent.includes('changed or is no longer available'));
  expect(await page.$eval('[data-publication-picker-selection]',n=>n.children.length)).toBe(1);expect(await page.$eval('#production-title',n=>n.value)).toBe('Retain this draft');expect(await f.store(binding).readLocal('draft:instruments:')).toEqual(before);
  expect(await page.$eval('#production-authoring',n=>n.dataset.dirty)).toBe('true');expect(errors).toEqual([]);
 }finally{f.run(f.cms,['entry','update','publication_occurrence',String(row.id),JSON.stringify({summary:row.summary,status:row.status}),'--db',f.db,'--schemas',join(f.home,'schemas')]);await context.close();}
},45000);
