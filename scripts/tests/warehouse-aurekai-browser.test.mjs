import {test,beforeAll,afterAll,expect} from 'bun:test';
import {mkdtempSync,rmSync,writeFileSync,readFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {createRequire} from 'node:module';
import {createHash} from 'node:crypto';
import {workspaceFixture,root} from '../../cmd/BonfyreWebAddress/tests/workspace_fixture.mjs';
import {materializeKnownSourceLibrary} from '../release/source_library_known_upgrade.mjs';
import {sourceLibrarySetup,sourceLibraryPack} from '../materialize_source_library.mjs';
import {publishSetups} from '../publish_experience_setups.mjs';
import {materializeAuthoredWorkspace,authoredWorkspaceSetup} from '../materialize_authored_workspace.mjs';
import {prepareRepositoryFPQReview} from '../../integrations/browser/repository-fpq-review.mjs';

const env={release:process.env.BONFYRE_WORKSPACE_RELEASE,packages:process.env.BONFYRE_WORKSPACE_PACKAGES};
let temp,fixture,binding,research,browser,portfolio,repositoryCut,fpqReceipt,fpqTampered;
const page=(rows)=>({schema:'bonfyre.warehouse.portfolio.v1',state:'READ_ONLY',
 total:rows.length,offset:0,limit:100,rows,next_offset:null,
 read_only:true,execution_authority:false,
 physical_bytes_verified:false,tenant_access_verified:false});
beforeAll(async()=>{
 temp=mkdtempSync(join(tmpdir(),'aurekai-warehouse-browser-'));
 const packages=join(temp,'packages'),release=await materializeKnownSourceLibrary({root:packages});
 const researchRelease=await materializeAuthoredWorkspace({root:packages});
 process.env.BONFYRE_WORKSPACE_RELEASE=release.id;
 process.env.BONFYRE_WORKSPACE_PACKAGES=packages;
 fixture=await workspaceFixture();
 publishSetups({records:[...sourceLibrarySetup(release.id),...authoredWorkspaceSetup(researchRelease.id)],binary:fixture.cms,db:fixture.db,
   schemas:join(fixture.home,'schemas'),apply:true});
 binding=await fixture.install(sourceLibraryPack);
 research=await fixture.install('experience.connected-instruments.workspace');
 const row={semantic_kind:'civic-2026',state:'PINNED_METADATA',
   representation_kind:'parquet',generation:7,representation_id:'parquet-civic-7',
   digest:'a'.repeat(64),realization_count:1,max_realization_bytes:42000};
 portfolio=join(temp,'warehouse.json');
 writeFileSync(portfolio,JSON.stringify(page([row])));
 const commit='a'.repeat(40),tree='b'.repeat(40),cutDigest=createHash('sha256').update([
  'bonfyre.repository-cut.v1','forgejo','aurekai','source-datasets','HEAD',
  ':/datasets',commit,tree,''].join('\n')).digest('hex');
 const sourceCut={
  schema:'bonfyre.repository-cut.v1',
  source_provider:'forgejo',source_org:'aurekai',
  source_repository:'source-datasets',requested_ref:'HEAD',
  josh_filter:':/datasets',virtual_commit:commit,virtual_tree:tree,
  cut_digest:cutDigest,canonical_mapping:'unverified',pack_root:'uncompiled',
  install_authority:'none',publication_authority:'none'
 };
 repositoryCut=join(temp,'source-cut.yaff');
 writeFileSync(repositoryCut,Object.entries(sourceCut).map(([key,value])=>key+': '+value+'\n').join(''));
 const fpqRequest=await prepareRepositoryFPQReview(readFileSync(repositoryCut,'utf8'));
 const fpqOutput='REVIEW ONLY: Check upstream canonical mapping, license, and rights before using this repository cut.';
 const fingerprint=createHash('sha256').update(JSON.stringify({
  route:'quic-fpq',host:'127.0.0.1',port:9443,model:fpqRequest.model,
  prompt:fpqRequest.prompt,maxTokens:128,temperature:0
 })).digest('hex');
 const syntheticFPQ={
  schema_version:'bonfyre.quic.fpq.generation.receipt.v1',
  receipt_id:'quic-fpq-'+'c'.repeat(32),status:'passed',event:'quic.fpq.generate',
  endpoint:'quic://127.0.0.1:9443',backend:'ngtcp2-quic-fpq-metal',
  model:fpqRequest.model,request:{max_tokens:128,temperature:0},
  prompt_sha256:fpqRequest.prompt_sha256,output:fpqOutput,
  output_sha256:createHash('sha256').update(fpqOutput).digest('hex'),
  request_fingerprint:fingerprint
 };
 fpqReceipt=join(temp,'fpq-synthetic-fixture.json');
 fpqTampered=join(temp,'fpq-tampered-fixture.json');
 writeFileSync(fpqReceipt,JSON.stringify(syntheticFPQ));
 writeFileSync(fpqTampered,JSON.stringify({...syntheticFPQ,output:'fabricated'}));
 browser=await createRequire(join(root,'tools/creator-ingest/package.json'))('puppeteer').launch({
  headless:true,executablePath:'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
  args:['--disable-background-networking','--no-sandbox']
 });
},60000);
afterAll(async()=>{
 await browser?.close();await fixture?.close();
 if(temp)rmSync(temp,{recursive:true,force:true});
 for(const [field,key]of [['release','BONFYRE_WORKSPACE_RELEASE'],['packages','BONFYRE_WORKSPACE_PACKAGES']]){
   if(env[field]===undefined)delete process.env[key];else process.env[key]=env[field];
 }
},20000);
test('signed-in Warehouse Intelligence imports one pinned source without writing or leaking bytes',async()=>{
 const ctx=await browser.createBrowserContext(),p=await ctx.newPage(),errors=[],requests=[];
 try{
  p.setDefaultTimeout(16000);await p.setViewport({width:1440,height:900});
  p.on('pageerror',e=>errors.push(e.message));
  p.on('request',r=>requests.push({method:r.method(),url:r.url()}));
  const [name,...rest]=fixture.owner.cookie.split('=');
  await p.setCookie({name,value:rest.join('='),url:fixture.base,httpOnly:true,sameSite:'Lax'});
  await p.goto(fixture.base+binding.path+'warehouse/',{waitUntil:'domcontentloaded'});
  await p.waitForSelector('[data-warehouse-studio] input[type=file]');
  expect(await p.$eval('[data-warehouse-studio] h1',el=>el.textContent))
    .toBe('Warehouse intelligence');
  const file=await p.$('input[type=file]');await file.uploadFile(portfolio);
  await p.evaluate(()=>[...document.querySelectorAll('button')]
    .find(b=>b.textContent==='Inspect portfolio').click());
  await p.waitForSelector('.wp-source');
  expect(await p.$eval('.wp-source',el=>el.textContent)).toContain('Generation 7');
  await p.click('.wp-source');
  await p.waitForFunction(()=>document.querySelector('.wp-detail')?.textContent.includes('parquet-civic-7'));
  await p.evaluate(()=>[...document.querySelectorAll('button')]
    .find(b=>b.textContent==='Find current Aurekai tools').click());
  await p.waitForSelector('.wp-detail a[href*="/apps/ws-"]');
  expect(await p.$eval('.wp-detail a[href*="/apps/ws-"]',n=>n.getAttribute('href')))
    .toContain(binding.id);
  expect(await p.$eval('.wp-detail',n=>n.textContent)).toContain('No snapshot bytes');
  expect(requests.filter(r=>r.method!=='GET')).toHaveLength(0);
  expect(requests.some(r=>r.url.includes('warehouse.json'))).toBe(false);
  expect((await fixture.store(binding).readLocal('source-material:'))).toHaveLength(0);
  expect(errors).toEqual([]);
  await p.setViewport({width:390,height:850});
  expect(await p.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);
 }finally{await ctx.close();}
},70000);
test('the current Source Library entrance retains Known Sources and the new Warehouse link',async()=>{
 const ctx=await browser.createBrowserContext(),p=await ctx.newPage(),errors=[];
 try{
  p.on('pageerror',e=>errors.push(e.message));
  const [name,...rest]=fixture.owner.cookie.split('=');
  await p.setCookie({name,value:rest.join('='),url:fixture.base,httpOnly:true,sameSite:'Lax'});
  await p.goto(fixture.base+binding.path,{waitUntil:'domcontentloaded'});
  await p.waitForSelector('[data-source-library][data-ready=true]');
  await p.waitForSelector('.known-source-lane');
  expect(await p.$eval('.warehouse-entry a',x=>x.getAttribute('href'))).toBe('./warehouse/');
  expect(errors).toEqual([]);
 }finally{await ctx.close();}
},60000);

test('JOSH subtree source cut opens exact installed owner without source reads, writes or a forged upstream mapping',async()=>{
 const ctx=await browser.createBrowserContext(),p=await ctx.newPage(),errors=[],requests=[];
 try{
  p.setDefaultTimeout(16000);await p.setViewport({width:1440,height:900});
  p.on('pageerror',error=>errors.push(error.message));
  p.on('request',request=>requests.push({method:request.method(),url:request.url()}));
  const [name,...rest]=fixture.owner.cookie.split('=');
  await p.setCookie({name,value:rest.join('='),url:fixture.base,httpOnly:true,sameSite:'Lax'});
  await p.goto(fixture.base+binding.path+'warehouse/',{waitUntil:'domcontentloaded'});
  await p.waitForSelector('[data-repository-cut-studio] input[type=file]');
  await (await p.$('[data-repository-cut-studio] input[type=file]')).uploadFile(repositoryCut);
  await p.evaluate(()=>[...document.querySelectorAll('[data-repository-cut-studio] button')]
    .find(x=>x.textContent==='Verify repository cuts').click());
  await p.waitForSelector('[data-repository-cut-studio] .wp-source');
  await p.click('[data-repository-cut-studio] .wp-source');
  await p.waitForFunction(()=>document.querySelector('[data-repository-cut-studio] .wp-detail')
    ?.textContent.includes(':/datasets'));
  const text=await p.$eval('[data-repository-cut-studio] .wp-detail',x=>x.textContent);
  expect(text).toContain('unverified');
  expect(text.toLowerCase()).toContain('upstream');
  expect(text).toContain('a'.repeat(40));
  expect(text).toContain('b'.repeat(40));
  await p.evaluate(()=>[...document.querySelectorAll('[data-repository-cut-studio] button')]
    .find(x=>x.textContent==='Find review owners').click());
  await p.waitForSelector('[data-repository-cut-studio] .wp-detail a[href*="/apps/ws-"]');
  expect(await p.$eval('[data-repository-cut-studio] .wp-detail a[href*="/apps/ws-"]',
    node=>node.getAttribute('href'))).toContain(binding.id);
  expect(requests.filter(r=>r.method!=='GET')).toHaveLength(0);
  expect(requests.some(r=>r.url.includes('source-datasets'))).toBe(false);
  expect((await fixture.store(binding).readLocal('source-material:'))).toHaveLength(0);
  expect(errors).toEqual([]);
  await p.setViewport({width:390,height:850});
  expect(await p.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);
 }finally{await ctx.close();}
},70000);


test('reviewed JOSH receipt is saved once as exact Source Library original and reopened by current record identity',async()=>{
 const ctx=await browser.createBrowserContext(),p=await ctx.newPage(),errors=[],requests=[];
 try{
  p.setDefaultTimeout(18000);p.on('pageerror',error=>errors.push(error.message));
  p.on('request',request=>requests.push({method:request.method(),url:request.url()}));
  const [name,...rest]=fixture.owner.cookie.split('=');
  await p.setCookie({name,value:rest.join('='),url:fixture.base,httpOnly:true,sameSite:'Lax'});
  await p.goto(fixture.base+binding.path+'warehouse/',{waitUntil:'domcontentloaded'});
  await p.waitForSelector('[data-repository-cut-studio] input[type=file]');
  await (await p.$('[data-repository-cut-studio] input[type=file]')).uploadFile(repositoryCut);
  await p.evaluate(()=>[...document.querySelectorAll('[data-repository-cut-studio] button')]
    .find(x=>x.textContent==='Verify repository cuts').click());
  await p.waitForSelector('[data-repository-cut-studio] .wp-source');
  await p.click('[data-repository-cut-studio] .wp-source');
  await p.evaluate(()=>[...document.querySelectorAll('[data-repository-cut-studio] button')]
    .find(x=>x.textContent==='Review retention in Source Library').click());
  await p.waitForSelector('dialog[open] button');
  expect(await p.$eval('dialog[open]',n=>n.textContent)).toContain('No source has been written');
  expect(requests.filter(r=>r.method!=='GET')).toHaveLength(0);
  expect(await fixture.store(binding).readLocal('source-material:')).toHaveLength(0);
  await p.evaluate(()=>[...document.querySelectorAll('dialog[open] button')]
    .find(x=>x.textContent==='Save reviewed source cut').click());
  await p.waitForFunction(()=>document.querySelector('dialog[open]')?.textContent
    .includes('Source Library retained the exact original'));
  const saved=await fixture.store(binding).readLocal('source-material:');
  expect(saved).toHaveLength(1);
  expect(saved[0].revision).toBe(1);
  expect(saved[0].value).toMatchObject({kind:'file',state:'active',
    textLike:true,filename:'source-cut.yaff',collection:'Repository cuts',
    tags:['git','repository-cut','josh']});
  const original=await saved[0].value.original.text();
  expect(original).toBe((await import('node:fs')).readFileSync(repositoryCut,'utf8'));
  expect(original).toContain('josh_filter: :/datasets');
  expect(original).toContain('canonical_mapping: unverified');
  expect(requests.filter(r=>r.method==='POST'&&r.url.includes('operation=write'))).toHaveLength(1);
  const href=await p.$eval('dialog[open] a',node=>node.getAttribute('href'));
  expect(href).toContain('record=source-material');
  await p.evaluate(()=>document.querySelector('dialog[open]').close());
  await p.evaluate(()=>[...document.querySelectorAll('[data-repository-cut-studio] button')]
    .find(x=>x.textContent==='Review retention in Source Library').click());
  await p.waitForFunction(()=>document.querySelector('dialog[open]')?.textContent
    .includes('already retained'));
  expect(await p.$eval('dialog[open] button',n=>n.disabled)).toBe(true);
  expect(requests.filter(r=>r.method==='POST'&&r.url.includes('operation=write'))).toHaveLength(1);
  expect(errors).toEqual([]);
 }finally{await ctx.close();}
},75000);

test('retained JOSH source cut flows into a real research review with exact file lineage and no generated findings',async()=>{
 const records=await fixture.store(binding).readLocal('source-material:');
 expect(records).toHaveLength(1);
 const source=records[0];
 const original=await source.value.original.text();
 const ctx=await browser.createBrowserContext(),p=await ctx.newPage(),errors=[],requests=[];
 try{
  p.setDefaultTimeout(20000);p.on('pageerror',error=>errors.push(error.message));
  p.on('request',req=>requests.push({method:req.method(),url:req.url()}));
  const [name,...rest]=fixture.owner.cookie.split('=');
  await p.setCookie({name,value:rest.join('='),url:fixture.base,httpOnly:true,sameSite:'Lax'});
  await p.goto(fixture.base+binding.path+'?'+new URLSearchParams({record:source.key}),
    {waitUntil:'domcontentloaded'});
  await p.waitForSelector('[data-source-library][data-ready=true]');
  await p.waitForSelector('.ac-check input');
  await p.click('.ac-check input');
  await p.evaluate(()=>[...document.querySelectorAll('[data-source-library] button')]
   .find(button=>button.textContent==='Prepare research').click());
  await p.waitForSelector('select[aria-label="Research destination"] option');
  await p.waitForFunction(()=>document.querySelector('dialog')?.dataset.busy==='false');
  expect(await p.$eval('select[aria-label="Research destination"]',select=>select.value))
   .toBe(research.id);
  await p.evaluate(()=>[...document.querySelectorAll('dialog button')]
    .find(x=>x.textContent==='Review research addition').click());
  await p.waitForFunction(()=>[...document.querySelectorAll('dialog button')]
    .some(button=>button.textContent==='Save reviewed research'&&!button.disabled));
  expect(requests.filter(r=>r.method==='POST'&&r.url.includes(research.id))).toHaveLength(0);
  await p.evaluate(()=>[...document.querySelectorAll('dialog button')]
    .find(x=>x.textContent==='Save reviewed research').click());
  await p.waitForSelector('dialog[data-state=saved]');
  const saved=await fixture.store(research).readLocal('draft:instruments:research:');
  expect(saved).toHaveLength(1);
  expect(saved[0].value.findings).toEqual([]);
  expect(saved[0].value.sources).toHaveLength(1);
  expect(saved[0].value.sources[0].body).toBe(original);
  expect(saved[0].value.sources[0].material).toMatchObject({
    binding:binding.id,key:source.key,revision:source.revision,sha256:source.value.contentSHA256,
    filename:'source-cut.yaff'
  });
  expect(requests.filter(r=>r.method==='POST'&&r.url.includes(research.id)&&
    r.url.includes('operation=write'))).toHaveLength(1);
  expect(errors).toEqual([]);
 }finally{await ctx.close();}
},75000);


test('signed-in JOSH cut prepares native PackGraph source demand without publishing or changing installed packs',async()=>{
 const ctx=await browser.createBrowserContext(),p=await ctx.newPage(),requests=[],errors=[];
 try{
  p.setDefaultTimeout(16000);p.on('pageerror',e=>errors.push(e.message));
  p.on('request',r=>requests.push({method:r.method(),url:r.url()}));
  const [name,...rest]=fixture.owner.cookie.split('=');
  await p.setCookie({name,value:rest.join('='),url:fixture.base,httpOnly:true,sameSite:'Lax'});
  await p.goto(fixture.base+binding.path+'warehouse/',{waitUntil:'domcontentloaded'});
  await (await p.$('[data-repository-cut-studio] input[type=file]')).uploadFile(repositoryCut);
  await p.evaluate(()=>[...document.querySelectorAll('[data-repository-cut-studio] button')]
    .find(x=>x.textContent==='Verify repository cuts').click());
  await p.waitForSelector('[data-repository-cut-studio] .wp-source');
  await p.click('[data-repository-cut-studio] .wp-source');
  await p.type('input[aria-label="Existing canonical PackGraph pack identifier"]','source-campaign');
  await p.evaluate(()=>[...document.querySelectorAll('[data-repository-cut-studio] button')]
    .find(x=>x.textContent==='Prepare PackGraph demand draft').click());
  const content=await p.$eval('[aria-label="PackGraph source demand draft"]',n=>n.textContent);
  const receipt=(await import('node:fs')).readFileSync(repositoryCut,'utf8');
  const digest=/cut_digest: ([a-f0-9]{64})/.exec(receipt)[1];
  expect(content).toContain('BonfyreCMS/PackGraph');
  expect(content).toContain('source-campaign');
  expect(content).toContain('repository-cut:'+digest);
  expect(content).toContain('Preferred sources');
  expect(content).toContain('draft');
  expect(content).toContain('No CMS write was sent');
  expect(content).toContain('NOT been verified');
  expect(requests.filter(r=>r.method!=='GET')).toHaveLength(0);
  await p.$eval('input[aria-label="Existing canonical PackGraph pack identifier"]',node=>{
    node.value='../forged';node.dispatchEvent(new Event('input',{bubbles:true}));
  });
  await p.evaluate(()=>[...document.querySelectorAll('[data-repository-cut-studio] button')]
    .find(x=>x.textContent==='Prepare PackGraph demand draft').click());
  expect(await p.$eval('[aria-label="PackGraph source demand draft"]',n=>n.textContent))
    .toContain('Draft refused');
  expect(requests.filter(r=>r.method!=='GET')).toHaveLength(0);
  expect(errors).toEqual([]);
 }finally{await ctx.close();}
},70000);

test('FPQ source-cut review binds its exact prompt and refuses tampered generations without source writes',async()=>{
 const ctx=await browser.createBrowserContext(),p=await ctx.newPage(),errors=[],requests=[];
 try{
  p.setDefaultTimeout(18000);
  p.on('pageerror',error=>errors.push(error.message));
  p.on('request',request=>requests.push({method:request.method(),url:request.url()}));
  const [name,...rest]=fixture.owner.cookie.split('=');
  await p.setCookie({name,value:rest.join('='),url:fixture.base,httpOnly:true,sameSite:'Lax'});
  const before=await fixture.store(binding).readLocal('source-material:');
  await p.goto(fixture.base+binding.path+'warehouse/',{waitUntil:'domcontentloaded'});
  await (await p.$('[data-repository-cut-studio] input[aria-label="Choose native JOSH repository-cut receipts"]'))
    .uploadFile(repositoryCut);
  await p.evaluate(()=>[...document.querySelectorAll('[data-repository-cut-studio] button')]
    .find(button=>button.textContent==='Verify repository cuts').click());
  await p.waitForSelector('[data-repository-cut-studio] .wp-source');
  await p.click('[data-repository-cut-studio] .wp-source');
  await p.evaluate(()=>[...document.querySelectorAll('[data-repository-cut-studio] button')]
    .find(button=>button.textContent==='Prepare local FPQ review').click());
  await p.waitForFunction(()=>document.querySelector('textarea[aria-label="Prepared native FPQ review prompt"]')
    ?.value.includes('JOSH filter: :/datasets'));
  const prompt=await p.$eval('textarea[aria-label="Prepared native FPQ review prompt"]',
    element=>element.value);
  expect(prompt).toContain('Source-cut digest: ');
  expect(prompt).toContain('Upstream canonical mapping: unverified');
  expect(prompt).not.toContain('BEGIN PRIVATE KEY');
  const receiptInput='input[aria-label="Native FPQ generation receipt"]';
  await (await p.$(receiptInput)).uploadFile(fpqTampered);
  await p.evaluate(()=>[...document.querySelectorAll('[data-repository-cut-studio] button')]
    .find(button=>button.textContent==='Verify FPQ generation receipt').click());
  await p.waitForFunction(()=>[...document.querySelectorAll('[data-repository-cut-studio] p')]
    .some(element=>element.textContent.includes('FPQ receipt refused')));
  expect(await p.$eval('[aria-label="FPQ generation review"]',element=>element.textContent))
    .toBe('');
  await (await p.$(receiptInput)).uploadFile(fpqReceipt);
  await p.evaluate(()=>[...document.querySelectorAll('[data-repository-cut-studio] button')]
    .find(button=>button.textContent==='Verify FPQ generation receipt').click());
  await p.waitForSelector('[aria-label="FPQ generation review"] pre');
  expect(await p.$eval('[aria-label="FPQ generation review"]',element=>element.textContent))
    .toContain('human review required');
  expect(await p.$eval('[aria-label="FPQ generation review"] pre',element=>element.textContent))
    .toContain('Check upstream canonical mapping');
  expect(await p.$eval('.repository-fpq-review [role=status]',element=>element.textContent))
    .toContain('unverified model-generated candidate');
  expect(requests.filter(request=>request.method!=='GET')).toHaveLength(0);
  expect(requests.some(request=>request.url.includes('9443'))).toBe(false);
  expect(await fixture.store(binding).readLocal('source-material:')).toEqual(before);
  expect(errors).toEqual([]);
  await p.setViewport({width:390,height:850});
  expect(await p.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);
 }finally{await ctx.close();}
},75000);


test('Bombadil-style signed-in repository-cut mutation exploration refuses forged provenance without effects',async()=>{
 const ctx=await browser.createBrowserContext(),p=await ctx.newPage(),errors=[],requests=[];
 const original=readFileSync(repositoryCut,'utf8');
 const mutations=[
  s=>s.replace('cut_digest: ','cut_digest: 00'),
  s=>s.replace('josh_filter: :/datasets','josh_filter: :/../../private'),
  s=>s.replace('source_repository: source-datasets','source_repository: ../../secrets'),
  s=>s.replace('publication_authority: none','publication_authority: approved'),
  s=>s.replace('install_authority: none','install_authority: approved'),
  s=>s.replace('schema: bonfyre.repository-cut.v1','schema: unexpected'),
  s=>s.replace('virtual_commit: '+'a'.repeat(40),'virtual_commit: forged'),
  s=>s.replace('virtual_tree: '+'b'.repeat(40),'virtual_tree: forgery'),
  s=>s.replace('canonical_mapping: unverified','canonical_mapping: verified'),
 ];
 try{
  p.setDefaultTimeout(18000);await p.setViewport({width:390,height:820});
  p.on('pageerror',error=>errors.push(error.message));
  p.on('request',r=>requests.push({method:r.method(),url:r.url()}));
  const [name,...rest]=fixture.owner.cookie.split('=');
  await p.setCookie({name,value:rest.join('='),url:fixture.base,httpOnly:true,sameSite:'Lax'});
  await p.goto(fixture.base+binding.path+'warehouse/',{waitUntil:'domcontentloaded'});
  const selector='[data-repository-cut-studio]';
  const input=await p.$(selector+' input[aria-label="Choose native JOSH repository-cut receipts"]');
  expect(input).not.toBeNull();
  for(let i=0;i<mutations.length;i++){
   const path=join(temp,'fuzzed-'+i+'.yaff');
   const modified=mutations[i](original);
   expect(modified).not.toBe(original);
   writeFileSync(path,modified);
   await input.uploadFile(path);
   await p.evaluate(()=>[...document.querySelectorAll('[data-repository-cut-studio] button')]
      .find(b=>b.textContent==='Verify repository cuts').click());
   await p.waitForFunction(()=>document.querySelector('[data-repository-cut-studio] [role=status]')
     ?.textContent.includes('Verification refused'));
   expect(await p.$eval(selector+' .wp-list',el=>el.textContent)).not.toContain('source-datasets');
   expect(await p.$(selector+' .wp-detail dl')).toBeNull();
   expect(await p.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);
  }
  // The next valid selection must recover without mixing rejected input.
  await input.uploadFile(repositoryCut);
  await p.evaluate(()=>[...document.querySelectorAll('[data-repository-cut-studio] button')]
   .find(b=>b.textContent==='Verify repository cuts').click());
  await p.waitForSelector(selector+' .wp-source');
  expect(await p.$eval(selector+' .wp-source',el=>el.textContent)).toContain('source-datasets');
  expect(requests.filter(r=>r.method!=='GET')).toHaveLength(0);
  expect(errors).toEqual([]);
 }finally{await ctx.close();}
},90000);


test('Antithesis reliability lab retains one exact triage original after explicit signed-in review',async()=>{
 const sourceSHA='b'.repeat(64),commit='a'.repeat(40),repo='antithesishq/glitch-grid';
 const report={schema_version:'0.1',status:'complete',summary:'Untrusted candidate',
  selection:{applied:'fixture only',runs_considered:1},grouping:{applied:'same source',inferred:false},
  runs:[{run_id:'run-001',created_at:'2026-11-06T00:00:00Z',group:'fixture',status:'completed',
    commit,triage_report_url:null,properties_failed:1,properties_passed:0,instrumentation:'ok',notes:''}],
  skipped_runs:[],failures:[{property:'No duplicate effect after retry',run_group:'fixture',
    classification:'new',failing_runs:['run-001'],baseline_runs:[],rate:'1/1',
    known_issue:null,evidence:[{run_id:'run-001',input_hash:'a',vtime:'0',container:'fixture',
     excerpt:'PRIVATE NEVER_LEAK_FROM_REPORT'}],root_cause:'untrusted',
    confidence:'low',category:'unknown',source_refs:[],suggested_fix:'no action',
    patch_file:'patches/ignored.patch',recommended_debug_session:null,
    reproduce_commands:['snouty runs exec NEVER_LEAK_FROM_REPORT']}],
  known_issues_not_seen:[],ignored:[],not_investigated:[],run_health:[],errors:[]};
 const original=JSON.stringify(report);
 const filename='antithesis-findings.json',path=join(temp,filename);
 writeFileSync(path,original);
 const invalid=join(temp,'antithesis-failure.json');
 writeFileSync(invalid,JSON.stringify({...report,runs:[{...report.runs[0],commit:'c'.repeat(40)}]}));
 const ctx=await browser.createBrowserContext(),p=await ctx.newPage(),requests=[],errors=[];
 try{
  p.setDefaultTimeout(20000);await p.setViewport({width:390,height:850});
  p.on('request',request=>requests.push({method:request.method(),url:request.url()}));
  p.on('pageerror',error=>errors.push(error.message));
  const [name,...rest]=fixture.owner.cookie.split('=');
  await p.setCookie({name,value:rest.join('='),url:fixture.base,httpOnly:true,sameSite:'Lax'});
  await p.goto(fixture.base+binding.path+'warehouse/',{waitUntil:'domcontentloaded'});
  const root='[data-antithesis-reliability]',control=root+' input[aria-label="Antithesis triage findings JSON"]';
  await p.waitForSelector(control);
  await p.type(root+' input[aria-label="Antithesis source repository"]',repo);
  await p.type(root+' input[aria-label="Antithesis tested Git commit"]',commit);
  await p.type(root+' input[aria-label="Antithesis source digest"]',sourceSHA);
  const inspect=async()=>p.evaluate(()=>[...document.querySelectorAll('[data-antithesis-reliability] button')]
   .find(b=>b.textContent==='Inspect triage report').click());
  await (await p.$(control)).uploadFile(invalid);await inspect();
  await p.waitForFunction(()=>document.querySelector('[data-antithesis-reliability] [role=status]')
   ?.textContent.includes('refused'));
  expect(await p.$eval(root+' [aria-label="Antithesis reviewed findings"]',x=>x.textContent)).toBe('');
  expect(requests.filter(x=>x.method!=='GET')).toHaveLength(0);
  await (await p.$(control)).uploadFile(path);await inspect();
  await p.waitForFunction(()=>document.querySelector('[data-antithesis-reliability] [role=status]')
   ?.textContent.includes('structure and digest checked'));
  const findings=await p.$eval(root+' [aria-label="Antithesis reviewed findings"]',x=>x.textContent);
  expect(findings).toContain('1 unverified failure candidates');
  expect(findings).not.toContain('NEVER_LEAK_FROM_REPORT');
  expect(findings).not.toContain('snouty runs exec');
  expect(requests.filter(x=>x.method!=='GET')).toHaveLength(0);
  await p.evaluate(()=>[...document.querySelectorAll('[data-antithesis-reliability] button')]
   .find(b=>b.textContent==='Review original retention').click());
  await p.waitForSelector('dialog[open]');
  expect(await p.$eval('dialog[open]',n=>n.textContent)).toContain('does not authenticate Antithesis');
  await p.evaluate(()=>[...document.querySelectorAll('dialog[open] button')]
   .find(b=>b.textContent==='Save original as reviewed source').click());
  await p.waitForFunction(()=>document.querySelector('dialog[open] [role=status]')
   ?.textContent.includes('Retained at revision'));
  const saved=(await fixture.store(binding).readLocal('source-material:'))
   .filter(row=>row.value?.filename===filename);
  expect(saved).toHaveLength(1);
  expect(saved[0].revision).toBe(1);
  expect(saved[0].value).toMatchObject({kind:'file',textLike:true,
   filename,collection:'Antithesis Reliability Lab',
   tags:['antithesis','reliability','triage','unverified']});
  expect(await saved[0].value.original.text()).toBe(original);
  expect(requests.filter(x=>x.method==='POST'&&x.url.includes('operation=write'))).toHaveLength(1);
  await p.evaluate(()=>document.querySelector('dialog[open]').close());
  await p.waitForFunction(()=>document.querySelector('dialog')===null);
  await p.evaluate(()=>[...document.querySelectorAll('[data-antithesis-reliability] button')]
   .find(b=>b.textContent==='Review original retention').click());
  try{
   await p.waitForFunction(()=>document.querySelector('dialog[open]')?.textContent
    .includes('already exists'),{timeout:5500});
  }catch(error){
   const debug=await p.evaluate(()=>({
    open:document.querySelector('dialog[open]')?.textContent||null,
    allDialogs:document.querySelectorAll('dialog').length,
    activeStatus:document.querySelector('[data-antithesis-reliability] [role=status]')?.textContent,
    enabled:[...document.querySelectorAll('[data-antithesis-reliability] button')].map(x=>[x.textContent,x.disabled])
   }));
   throw Error('Second review state: '+JSON.stringify(debug)+' / '+error.message);
  }
  expect(await p.$eval('dialog[open] button',x=>x.disabled)).toBe(true);
  expect(requests.filter(x=>x.method==='POST'&&x.url.includes('operation=write'))).toHaveLength(1);
  expect(await p.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);
  expect(errors).toEqual([]);
 }finally{await ctx.close();}
},90000);
