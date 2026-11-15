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
test('a pasted address is followed once into the native store and reopened without fetching the publisher',async()=>{
 const context=await browser.createBrowserContext(),page=await context.newPage(),errors=[],requests=[];
 page.setDefaultTimeout(15000);page.on('pageerror',e=>errors.push(e.message));page.on('request',r=>requests.push({method:r.method(),url:r.url()}));
 const [name,...rest]=fixture.owner.cookie.split('=');
 await page.setCookie({name,value:rest.join('='),url:fixture.base,httpOnly:true,sameSite:'Lax'});
 const lookup=fixture.base+installed.path+'?mode=link&url='+encodeURIComponent('https://publisher.example/observed-feed');
 try{
  await page.goto(lookup,{waitUntil:'domcontentloaded'});
  expect(await fixture.store(installed).readLocal('source-material:')).toHaveLength(0);
  expect(await page.$eval('.ak-src-found h2',x=>x.textContent)).toBe('publisher.example');
  await Promise.all([page.waitForNavigation({waitUntil:'domcontentloaded'}),page.click('[data-src-follow] button[type=submit]')]);
  expect(page.url()).toContain('followed=');
  const saved=await fixture.store(installed).readLocal('source-material:');
  expect(saved).toHaveLength(1);expect(saved[0].revision).toBe(1);
  expect(saved[0].value).toMatchObject({kind:'link',state:'active',title:'publisher.example',url:'https://publisher.example/observed-feed'});
  expect(await page.$eval('.ak-src-library li[data-fresh] strong',x=>x.textContent)).toBe('publisher.example');
  // Looking the same address up again says it is already kept and offers no second write.
  await page.goto(lookup,{waitUntil:'domcontentloaded'});
  expect(await page.$eval('.ak-src-done',x=>x.textContent)).toContain('Already in your library');
  expect(await page.$('[data-src-follow]')).toBeNull();
  expect((await fixture.store(installed).readLocal('source-material:'))[0].revision).toBe(1);
  expect(requests.some(r=>r.url.startsWith('https://publisher.example/'))).toBe(false);
  expect(requests.filter(r=>r.method==='POST')).toHaveLength(1);
  expect(errors).toEqual([]);
 }finally{await context.close();}
},60000);

test('a follow posted from another origin is refused and writes nothing',async()=>{
 const res=await fetch(fixture.base+installed.path.replace(/sources\/$/,'_follow'),{method:'POST',redirect:'manual',
  headers:{cookie:fixture.owner.cookie,origin:'https://elsewhere.example','content-type':'application/x-www-form-urlencoded'},body:'url='+encodeURIComponent('https://other.example/feed')});
 expect(res.status).toBe(403);
 expect((await fixture.store(installed).readLocal('source-material:')).some(r=>r.value.url==='https://other.example/feed')).toBe(false);
});
