// Auth navigation and product presentation over real isolated native owners.
// These checks do not sign in to a customer account or publish customer work.
import {test,expect,beforeAll,afterAll} from 'bun:test';
import {readFileSync,mkdirSync} from 'node:fs';
import {join} from 'node:path';
import {createRequire} from 'node:module';
import {workspaceFixture,root} from './workspace_fixture.mjs';
import {retainWorkspaceFiles} from '../../../scripts/materialize_workspace_package.mjs';
import {portfolio,projectSetups,projectWorkspaceSetups,publishSetups} from '../../../scripts/publish_experience_setups.mjs';
let f,owner,second,binding,preview,browser;
const failures=[],external=[],directory=join(root,'cmd/BonfyreExperienceStage/verification/navigation-20261020');
const pack='experience.multi-merchant-catalogue.workspace';
const release='a5b379ab187e523c3d1eb090a33d014287491c4d37594520bbb7b4c151183e5a';
const header=body=>body.match(/<header class="ak-bar">[\s\S]*?<\/header>/)?.[0]??'';
const previous={release:process.env.BONFYRE_WORKSPACE_RELEASE,packages:process.env.BONFYRE_WORKSPACE_PACKAGES};
beforeAll(async()=>{
 process.env.BONFYRE_WORKSPACE_RELEASE=release;
 process.env.BONFYRE_WORKSPACE_PACKAGES='/Users/nickgonzales/.bonfyre/workspace-packages';
 f=await workspaceFixture();owner=f.owner;second=f.writer;
 f.authsql.query('UPDATE users SET name=? WHERE id=?').run('Avery Example',owner.id);
 f.authsql.query('UPDATE users SET name=? WHERE id=?').run('Jordan Example',second.id);
 const material=await retainWorkspaceFiles(new Map([['assets/catalogue.png',readFileSync(join(root,'packs/aurekai-work-samples/site/assets/shots/catalogue-grid.png'))]]));
 preview='/apps/material/'+material.id+'/assets/catalogue.png';
 const rows=portfolio(readFileSync(join(root,'cmd/BonfyreExperienceStage/experience-maturities.yaff'),'utf8'));
 publishSetups({records:[...projectSetups(rows,[1,2]),...projectWorkspaceSetups(rows,release)],binary:f.cms,db:f.db,schemas:join(f.home,'schemas'),apply:true});
 f.add('pack_demand',{pack,demand_kind:'preview-media',needs:preview,preferred_sources:'Published catalogue editor preview',fields:'image',optional:'true',status:'published'});
 f.add('pack_demand',{pack,demand_kind:'preview-media',needs:'https://example.invalid/private-tracker.png',preferred_sources:'Refused remote media',fields:'image',optional:'true',status:'published'});
 binding=await f.install(pack,'institution:workspace-team');
 mkdirSync(directory,{recursive:true});
},30000);
afterAll(async()=>{
 await browser?.close();await f?.close();
 if(previous.release===undefined)delete process.env.BONFYRE_WORKSPACE_RELEASE;else process.env.BONFYRE_WORKSPACE_RELEASE=previous.release;
 if(previous.packages===undefined)delete process.env.BONFYRE_WORKSPACE_PACKAGES;else process.env.BONFYRE_WORKSPACE_PACKAGES=previous.packages;
});
test('native setup header resolves the current session without browser state',async()=>{
 const signed=await f.request('/packs/setup',{cookie:owner.cookie}),nav=header(signed.body);
 expect(signed.status).toBe(200);expect(signed.headers.get('cache-control')).toContain('no-store');
 expect(nav).toContain('data-session-state="signed-in"');expect(nav).toContain('Avery Example');
 expect(nav).toContain('href="/account#presence"');expect(nav).toContain('data-your-work');
 expect(nav).not.toMatch(/href="\/(signup|login)(?:\?|"|\/)/);
 expect(nav).not.toContain('workspace-owner@example.test');expect(nav).not.toContain(owner.cookie);
});
test('alternating account and anonymous requests never inherit another identity',async()=>{
 for(const [cookie,name]of [[owner.cookie,'Avery Example'],[second.cookie,'Jordan Example'],['',null],[owner.cookie,'Avery Example']]){
  const r=await f.request('/packs/setup',{cookie}),nav=header(r.body);
  if(name){expect(nav).toContain(name);expect(nav).not.toContain(name==='Avery Example'?'Jordan Example':'Avery Example');}
  else{expect(nav).toContain('data-session-state="anonymous"');expect(nav).toContain('/session/continue?next=%2Fpacks%2Fsetup');expect(nav).not.toContain('Example');}
 }
});
test('private navigation fragment rejects actor hints and returns no secret-bearing identity fields',async()=>{
 const r=await f.request('/account/navigation',{cookie:owner.cookie});
 expect(r.status).toBe(200);expect(r.headers.get('cache-control')).toContain('private, no-store');expect(r.headers.get('vary')).toContain('Cookie');
 expect(r.body).toContain('Avery Example');expect(r.body).not.toContain(owner.cookie);expect(r.body).not.toContain('@example.test');
 expect((await f.request('/account/navigation?user=999',{cookie:owner.cookie})).status).toBe(400);
 expect((await f.request('/account/navigation',{cookie:owner.cookie,form:{}})).status).toBe(400);
});
test('revoked sessions render anonymous navigation and cannot enter installed work',async()=>{
 const u=await f.login('revoked-nav@example.test');await f.request('/logout',{cookie:u.cookie,form:{}});
 expect(header((await f.request('/packs/setup',{cookie:u.cookie})).body)).toContain('data-session-state="anonymous"');
 expect((await f.request('/apps',{cookie:u.cookie})).status).toBe(303);
});
test('published preview material and direct installed-editor links share the same native declarations',async()=>{
 const response=await f.request('/packs/setup?context=institution%3Aworkspace-team',{cookie:owner.cookie});
 expect(response.status).toBe(200);expect(response.body).toContain('class="pack-panel pack-tool"');
 expect(response.body).toContain('src="'+preview+'"');expect(response.body).not.toContain('private-tracker.png');
 expect(response.body).toContain('data-pack-editor href="'+binding.path+'"');expect(response.body).toContain('data-pack-filter');
 const apps=await f.request('/apps?context=institution%3Aworkspace-team',{cookie:owner.cookie});
 expect(apps.status).toBe(200);expect(apps.body).toContain(preview);expect(apps.body).toContain('data-work-editor href="'+binding.path+'"');
 expect(header(apps.body)).not.toContain('>Sign in<');
 expect((await f.request('/apps?context=institution%3Aworkspace-team',{cookie:f.other.cookie})).status).toBe(403);
});
test('private native navigation remains correct without scripts',async()=>{
 const r=await f.request('/packs/setup',{cookie:owner.cookie});
 const noScript=r.body.replace(/<script\b[^>]*>[\s\S]*?<\/script>/g,'');
 expect(header(noScript)).toContain('Avery Example');expect(header(noScript)).toContain('/account#presence');
 expect(header(noScript)).not.toContain('>Sign in<');
});
test('real Chrome renders, filters and navigates the same product cards at desktop and phone widths',async()=>{
 const puppeteer=createRequire(join(root,'tools/creator-ingest/package.json'))('puppeteer');
 browser=await puppeteer.launch({executablePath:'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',headless:true,args:['--no-sandbox','--disable-dev-shm-usage']});
 const context=await browser.createBrowserContext(),page=await context.newPage();page.setDefaultTimeout(20000);
 const [name,...value]=owner.cookie.split('=');await page.setCookie({name,value:value.join('='),url:f.base,httpOnly:true,sameSite:'Lax'});
 page.on('pageerror',e=>failures.push(e.message));
 await page.setRequestInterception(true);page.on('request',r=>{if(r.url().startsWith(f.base)||r.url().startsWith('data:'))r.continue();else{external.push(r.url());r.abort();}});
 try{
   const own=await f.install(pack,'self');
  for(const width of [1440,390]){
   await page.setViewport({width,height:1000});await page.goto(f.base+'/packs/setup?context=institution%3Aworkspace-team',{waitUntil:'networkidle0'});
   await page.waitForSelector('.pack-tool img');await page.$eval('.pack-tool img',n=>n.scrollIntoView());
   await page.waitForFunction(()=>[...document.querySelectorAll('.pack-tool img')].every(n=>n.complete&&n.naturalWidth>0));
   expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);
   expect(await page.$eval('.ak-session',n=>n.dataset.sessionState)).toBe('signed-in');
   await page.$eval('.ak-user summary',n=>n.scrollIntoView());await page.click('.ak-user summary');
   expect(await page.$eval('.ak-user',n=>n.open)).toBe(true);await page.keyboard.press('Escape');
   expect(await page.$eval('.ak-user',n=>n.open)).toBe(false);
   await page.type('[data-pack-filter]','Catalogue');expect(await page.$$eval('.pack-tool:not([hidden])',n=>n.length)).toBeGreaterThan(0);
   expect(await page.$$eval('.pack-tool:not([hidden])',nodes=>nodes.every(n=>n.textContent.toLocaleLowerCase().includes('catalogue')))).toBe(true);
   await page.$eval('[data-pack-filter]',n=>{n.value='';n.dispatchEvent(new Event('input',{bubbles:true}));});
   await page.evaluate(()=>scrollTo(0,0));await page.screenshot({path:join(directory,'directory-'+width+'.png'),fullPage:true});
   await page.goto(f.base+'/apps?context=institution%3Aworkspace-team',{waitUntil:'networkidle0'});await page.waitForSelector('[data-work-editor]');
   expect(await page.$eval('[data-work-editor]',n=>n.getAttribute('href'))).toBe(binding.path);
   expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);
   await page.$eval('.pack-tool',n=>n.scrollIntoView({block:'center'}));
   await page.mouse.move(0,0);await new Promise(r=>setTimeout(r,700));
   const resting=await page.$eval('.pack-tool',n=>getComputedStyle(n).transform);
   await page.hover('.pack-tool');await new Promise(r=>setTimeout(r,700));
   expect(await page.$eval('.pack-tool',n=>getComputedStyle(n).transform)).not.toBe(resting);
   // Keyboard focus reaches the same presentation, after the entrance finishes.
   await page.mouse.move(0,0);await page.focus('[data-work-editor]');
   await new Promise(r=>setTimeout(r,350));
   expect(await page.$eval('.pack-tool',n=>getComputedStyle(n).transform)).not.toBe(resting);
   await page.emulateMediaFeatures([{name:'prefers-reduced-motion',value:'reduce'}]);
   expect(await page.$eval('.pack-tool',n=>getComputedStyle(n).transitionDuration)).toBe('0s');
   expect(await page.evaluate(()=>performance.getEntriesByType('resource').every(r=>!/(sql-wasm|three\.module|creator.*wasm)/.test(r.name)))).toBe(true);
   await page.screenshot({path:join(directory,'installed-'+width+'.png'),fullPage:true});
    // The account shelf is inserted after DOMContentLoaded. Its native cards
    // must use the same real preview, responsive layout and live search.
    await page.emulateMediaFeatures([{name:'prefers-reduced-motion',value:'no-preference'}]);
    await page.goto(f.base+'/account',{waitUntil:'networkidle0'});
    await page.waitForSelector('[data-account-tools] .pack-tool');
    expect(await page.$eval('.account-tool-shelf .pack-cards',n=>getComputedStyle(n).display)).toBe('grid');
    expect(await page.$eval('.account-tool-shelf .pack-tool',n=>getComputedStyle(n).animationName)).toContain('pack-rise');
    expect(await page.$eval('.account-tool-shelf [data-work-editor]',n=>n.getAttribute('href'))).toBe(own.path);
    await page.waitForFunction(()=>[...document.querySelectorAll('.account-tool-shelf .pack-preview')].every(n=>n.complete&&n.naturalWidth>0));
    expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);
    await page.type('.account-tool-shelf [data-pack-filter]','not-a-real-product');
    expect(await page.$$eval('.account-tool-shelf .pack-tool:not([hidden])',n=>n.length)).toBe(0);
    await page.$eval('.account-tool-shelf [data-pack-filter]',n=>{n.value='Catalogue';n.dispatchEvent(new Event('input',{bubbles:true}));});
    expect(await page.$$eval('.account-tool-shelf .pack-tool:not([hidden])',n=>n.length)).toBe(1);
    expect(await page.$eval('.account-tool-shelf [data-pack-count]',n=>n.textContent)).toContain('1 of 1');
    await page.click('.account-tool-shelf [data-pack-inspect]');
    await page.waitForFunction(()=>document.querySelector('dialog[open] .bf-preview-viewport img')?.naturalWidth>0);
    expect(await page.$eval('.bf-preview-actions a',n=>new URL(n.href).pathname)).toBe(own.path);
    expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);
    await page.keyboard.press('Escape');
    expect(await page.$('dialog[open]')).toBeNull();
    expect(await page.$eval('.account-tool-shelf [data-pack-inspect]',n=>n===document.activeElement)).toBe(true);
    await page.screenshot({path:join(directory,'account-'+width+'.png'),fullPage:true});
    // The public apex keeps editorial content, but adds authorized native work
    // only after resolving the current browser session from the existing owner.
    const workHome=await f.request('/',{host:'aurekai.work',cookie:owner.cookie});expect(workHome.status).toBe(200);await page.setExtraHTTPHeaders({});await page.goto(f.base+'/',{waitUntil:'domcontentloaded'});await page.setContent(workHome.body,{waitUntil:'networkidle0'});
    await page.waitForSelector('.ak-home-continuation .pack-tool',{timeout:5500}).catch(async()=>{throw Error('HOME_DIAG '+JSON.stringify(await page.evaluate(()=>({url:location.href,state:document.querySelector('.ak-session')?.dataset.sessionState,bar:!!document.querySelector('.ak-bar'),home:!!document.querySelector('.ak-home-continuation'),main:!!document.querySelector('main'),script:[...document.scripts].some(s=>s.textContent.includes('ak-home-continuation')),hrefs:[...document.querySelectorAll('.ak-session a')].map(a=>a.getAttribute('href'))})))+' errors='+JSON.stringify(failures));});
    expect(await page.$eval('.ak-home-continuation [data-work-editor]',n=>n.getAttribute('href'))).toBe(own.path);
    expect(await page.$eval('.ak-home-continuation .pack-cards',n=>getComputedStyle(n).display)).toBe('grid');
    expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);
    expect(await page.evaluate(()=>performance.getEntriesByType('resource').every(r=>!/(sql-wasm|three\.module|creator.*wasm)/.test(r.name)))).toBe(true);
  }
  expect(failures).toEqual([]);expect(external.every(u=>u.startsWith('https://fonts.'))).toBe(true);
 }finally{await context.close();}
},60000);

test('explicit actor and current Matrix public LIFE presence appear in the signed-in menu, not another account',async()=>{
 const matrix=join(f.home,'matrix.db'),actor='person:avery',ev='fixture:public-profile';
 f.run(f.auth,['--db',f.authdb,'actor-bind','--user-id',String(owner.id),'--actor',actor]);
 f.run(f.web,['subject','--db',matrix,'--id',actor,'--kind','Person','--digest','a'.repeat(64),'--evidence',ev]);
 f.run(f.web,['institution','--db',matrix,'--id','inst:person-avery','--kind','Person','--world','live','--evidence',ev]);
 f.run(f.web,['domain','--db',matrix,'--host','avery.aurekai.life','--institution','inst:person-avery','--territory','life','--scope','public','--world','live','--repr','HTML','--evidence',ev]);
 f.run(f.web,['presence','--db',matrix,'--id','presence:avery:life','--subject',actor,'--territory','life','--scope','public','--world','live','--repr','HTML','--evidence',ev]);
 const nav=await f.request('/account/navigation',{cookie:owner.cookie}),unbound=await f.request('/account/navigation',{cookie:second.cookie});
 expect(nav.body).toContain('data-public-profile-menu href="https://avery.aurekai.life/"');
 expect(unbound.body).not.toContain('data-public-profile-menu');
 const context=await browser.createBrowserContext(),page=await context.newPage();
 try{
  const [name,...value]=owner.cookie.split('=');await page.setCookie({name,value:value.join('='),url:f.base,httpOnly:true,sameSite:'Lax'});
  await page.setViewport({width:390,height:900});await page.goto(f.base+'/account',{waitUntil:'networkidle0'});
  await page.waitForSelector('[data-public-profile-menu]');
  expect(await page.$eval('[data-public-profile]',n=>getComputedStyle(n).display)).toBe('none');
  await page.click('.ak-user summary');
  expect(await page.$eval('[data-public-profile-menu]',n=>({href:n.href,visible:n.getBoundingClientRect().height>0}))).toEqual({href:'https://avery.aurekai.life/',visible:true});
  expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);
 }finally{await context.close();}
},30000);

test('declared external account links become responsive, filterable identity cards without becoming verified credentials',async()=>{
 const claim=await f.request('/account/connections',{cookie:owner.cookie,form:{provider:'github',remote_account:'fixture-handle',profile_url:'https://github.com/fixture-handle',display_name:'Fixture creator'}});
 expect(claim.status).toBe(303);
 const secondLinks=await f.request('/account/connections',{cookie:second.cookie});
 expect(secondLinks.body).not.toContain('fixture-handle');
 const puppeteer=createRequire(join(root,'tools/creator-ingest/package.json'))('puppeteer');
 const local=await puppeteer.launch({executablePath:'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',headless:true,args:['--no-sandbox','--disable-dev-shm-usage']});
 try{const context=await local.createBrowserContext(),page=await context.newPage();
  const [name,...value]=owner.cookie.split('=');await page.setCookie({name,value:value.join('='),url:f.base,httpOnly:true,sameSite:'Lax'});
  for(const width of [1440,390]){
   await page.setViewport({width,height:900});await page.goto(f.base+'/account',{waitUntil:'networkidle0'});
   await page.waitForSelector('.external-item[data-enhanced]');
   expect(await page.$eval('.connection-provider',e=>e.textContent)).toBe('github');
   expect(await page.$eval('.connection-proof',e=>e.textContent)).toContain('not verified');
   expect(await page.$eval('.external-item',e=>e.dataset.connectionProof)).toBe('declared');
   expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);
   await page.type('.connection-filters input','no-such-account');
   expect(await page.$eval('.external-item',e=>e.hidden)).toBe(true);
   await page.$eval('.connection-filters input',e=>{e.value='';e.dispatchEvent(new Event('input',{bubbles:true}));});
   expect(await page.$eval('.external-item',e=>e.hidden)).toBe(false);
   await page.select('.connection-filters select','verified');
   expect(await page.$eval('.external-item',e=>e.hidden)).toBe(true);await page.evaluate(()=>{const n=document.createElement('article');n.className='external-item';n.dataset.fixture='unsafe';n.innerHTML='<span><b>Legacy link</b><br><small>legacy · old</small></span><p><span class="status">unverified</span></p>';document.querySelector('#connections').append(n);});await page.waitForFunction(()=>document.querySelector('[data-fixture=unsafe]')?.dataset.connectionProof==='declared');expect(await page.$eval('[data-fixture=unsafe]',e=>e.hidden)).toBe(true);await page.select('.connection-filters select','declared');expect(await page.$eval('[data-fixture=unsafe]',e=>({hidden:e.hidden,text:e.querySelector('.connection-body b')?.textContent,link:!!e.querySelector('a')}))).toEqual({hidden:false,text:'Legacy link',link:false});
  }await context.close();
 }finally{await local.close();}
},45000);
