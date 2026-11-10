import {test,beforeAll,afterAll,expect} from 'bun:test';
import {mkdtempSync,rmSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {createRequire} from 'node:module';
import {workspaceFixture,root} from '../../../cmd/BonfyreWebAddress/tests/workspace_fixture.mjs';
import {materializeKnownSourceLibrary} from '../source_library_known_upgrade.mjs';
import {sourceLibrarySetup,sourceLibraryPack} from '../../materialize_source_library.mjs';
import {publishSetups} from '../../publish_experience_setups.mjs';
const prior={release:process.env.BONFYRE_WORKSPACE_RELEASE,packages:process.env.BONFYRE_WORKSPACE_PACKAGES};
let temp,fixture,installed,browser;
const packet={schema:'bonfyre.foreign.twins.v2',state:'current',catalogue_generation:'foreign-v1-review',total:1,next_offset:null,
 twins:[{url:'https://publisher.example/observed-feed',release:{source_contract:{source:'feed-publisher-example',family:'news',label:'Original publisher'},stages:{archive:{items:17}}}}]};
beforeAll(async()=>{
 temp=mkdtempSync(join(tmpdir(),'bonfyre-known-source-browser-'));const packages=join(temp,'packages');
 const packageRelease=await materializeKnownSourceLibrary({root:packages});
 process.env.BONFYRE_WORKSPACE_RELEASE=packageRelease.id;process.env.BONFYRE_WORKSPACE_PACKAGES=packages;
 fixture=await workspaceFixture();
 publishSetups({records:sourceLibrarySetup(packageRelease.id),binary:fixture.cms,db:fixture.db,schemas:join(fixture.home,'schemas'),apply:true});
 installed=await fixture.install(sourceLibraryPack);
 const puppeteer=createRequire(join(root,'tools/creator-ingest/package.json'))('puppeteer');
 browser=await puppeteer.launch({headless:true,executablePath:'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',args:['--no-sandbox','--disable-dev-shm-usage']});
},60000);
afterAll(async()=>{await browser?.close();await fixture?.close();if(temp)rmSync(temp,{recursive:true,force:true});
 for(const[k,v]of [['BONFYRE_WORKSPACE_RELEASE',prior.release],['BONFYRE_WORKSPACE_PACKAGES',prior.packages]])if(v===undefined)delete process.env[k];else process.env[k]=v;
},20000);
test('known source is reviewed, written once in native store and reopened without fetching publisher',async()=>{
 const context=await browser.createBrowserContext(),page=await context.newPage(),errors=[],requests=[];
 page.setDefaultTimeout(15000);page.on('pageerror',e=>errors.push(e.message));
 await page.setRequestInterception(true);
 page.on('request',req=>{
  requests.push({method:req.method(),url:req.url()});
  if(req.url().includes('/foreign/twins?'))void req.respond({status:200,contentType:'application/json',body:JSON.stringify(packet)});
  else void req.continue();
 });
 const [name,...rest]=fixture.owner.cookie.split('=');
 await page.setCookie({name,value:rest.join('='),url:fixture.base,httpOnly:true,sameSite:'Lax'});
 try{
  await page.goto(fixture.base+installed.path,{waitUntil:'domcontentloaded'});
  await page.waitForSelector('[data-source-library][data-ready=true]');
  await page.waitForSelector('.known-source-lane');
  expect(await fixture.store(installed).readLocal('source-material:')).toHaveLength(0);
  await page.evaluate(()=>[...document.querySelectorAll('.known-source-lane button')].find(x=>x.textContent==='Browse known sources').click());
  await page.waitForSelector('.known-source-results article');
  expect(await page.$eval('.known-source-lane [role=status]',x=>x.textContent)).toContain('1 known sources');
  expect(await fixture.store(installed).readLocal('source-material:')).toHaveLength(0);
  await page.evaluate(()=>[...document.querySelectorAll('.known-source-results button')].find(x=>x.textContent==='Review bookmark').click());
  await page.waitForSelector('dialog.known-source-review[open]');
  expect(await page.$eval('dialog.known-source-review',x=>x.textContent)).toContain('feed-publisher-example');
  await page.evaluate(()=>[...document.querySelectorAll('dialog.known-source-review button')].find(x=>x.textContent==='Save reviewed bookmark').click());
  await page.waitForSelector('dialog.known-source-review a[href*="record="]');
  const saved=await fixture.store(installed).readLocal('source-material:');
  expect(saved).toHaveLength(1);expect(saved[0].revision).toBe(1);
  expect(saved[0].value).toMatchObject({kind:'link',state:'active',title:'Original publisher',
   url:'https://publisher.example/observed-feed',provenance:{sourceRef:'feed-publisher-example',catalogueGeneration:'foreign-v1-review'}});
  expect(requests.some(r=>r.url.startsWith('https://publisher.example/'))).toBe(false);
  expect(requests.filter(r=>r.method==='POST'&&r.url.includes('operation=write'))).toHaveLength(1);
   // A repeated explicit review resolves the same saved identity rather than
   // silently dead-ending or issuing a second write.
   await page.evaluate(()=>document.querySelector('dialog.known-source-review').close());
   await page.evaluate(()=>[...document.querySelectorAll('.known-source-results button')].find(x=>x.textContent==='Review bookmark').click());
   await page.waitForSelector('dialog.known-source-review[open]');
   await page.evaluate(()=>[...document.querySelectorAll('dialog.known-source-review button')].find(x=>x.textContent==='Save reviewed bookmark').click());
   // The saved link persists across modal reopening. Wait for this second
   // review's verdict, not the link already present from the first save.
   await page.waitForFunction(()=>document.querySelector('dialog.known-source-review [role=status]')?.textContent?.includes('already saved'));
   expect(await page.$eval('dialog.known-source-review [role=status]',x=>x.textContent)).toContain('already saved');
   expect(requests.filter(r=>r.method==='POST'&&r.url.includes('operation=write'))).toHaveLength(1);
  await page.evaluate(()=>document.querySelector('dialog.known-source-review a').click());
  await page.waitForSelector('[data-source-library][data-ready=true]');
  expect((await fixture.store(installed).readLocal('source-material:'))[0].revision).toBe(1);
  expect(errors).toEqual([]);
 }finally{await context.close();}
},60000);

test('real retained recipe search saves an exact public reference into Source Library, never a body or order',async()=>{
 const key='ffts-lg33961.31000kz-0700-f61d99d1df3da63f';
 const packet={jsonrpc:'2.0',id:'source-library-recipe',result:{skill:'recipe-search',execution:'free',data:{query:'chicken',count:1,hits:[{key,title:'Retained chicken preparation'}]}}};
 const context=await browser.createBrowserContext(),page=await context.newPage(),requests=[],errors=[];
 page.setDefaultTimeout(15000);page.on('pageerror',e=>errors.push(e.message));
 await page.setRequestInterception(true);
 page.on('request',request=>{
  requests.push({url:request.url(),method:request.method()});
  if(request.url().endsWith('/a2a')&&request.method()==='POST')void request.respond({status:200,contentType:'application/a2a+json',body:JSON.stringify(packet)});
  else void request.continue();
 });
 const [name,...token]=fixture.owner.cookie.split('=');
 await page.setCookie({name,value:token.join('='),url:fixture.base,httpOnly:true,sameSite:'Lax'});
 try{
  await page.goto(fixture.base+installed.path,{waitUntil:'domcontentloaded'});
  await page.waitForSelector('[data-source-library][data-ready=true]');
  await page.waitForSelector('[data-recipe-corpus] form');
  const before=await fixture.store(installed).readLocal('source-material:');
  await page.type('[data-recipe-corpus] input[type=search]','chicken');
  await page.click('[data-recipe-corpus] button[type=submit]');
  await page.waitForSelector('[data-recipe-corpus] article');
  expect(await page.$eval('[data-recipe-corpus] article a',x=>x.getAttribute('href'))).toBe('/recipes?id='+key);
  expect(await fixture.store(installed).readLocal('source-material:')).toHaveLength(before.length);
  await page.evaluate(()=>document.querySelector('[data-recipe-corpus] article button').click());
  await page.waitForSelector('dialog[data-recipe-review][open]');
  await page.evaluate(()=>[...document.querySelectorAll('dialog[data-recipe-review] button')].find(x=>x.textContent==='Save reviewed recipe reference').click());
  await page.waitForSelector('dialog[data-recipe-review] a[href*="record="]');
  const rows=await fixture.store(installed).readLocal('source-material:');
  expect(rows).toHaveLength(before.length+1);
  const row=rows.find(x=>x.value.provenance?.schema==='bonfyre.recipe-material-ref.v1');
  expect(row?.revision).toBe(1);
  expect(row?.value).toMatchObject({kind:'link',url:'https://aurekai.life/recipes?id='+key,
    provenance:{recordKey:key,source:'native-recipe-material'}});
  expect(row.value.original).toBeUndefined();
  expect(requests.filter(x=>x.url.endsWith('/a2a')&&x.method==='POST')).toHaveLength(1);
  expect(requests.some(x=>x.url.startsWith('https://aurekai.life/recipes?id='))).toBe(false);
  expect(errors).toEqual([]);
 }finally{await context.close();}
},60000);
