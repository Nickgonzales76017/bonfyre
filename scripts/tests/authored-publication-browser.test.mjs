import {beforeAll,afterAll,test,expect} from 'bun:test';
import {mkdtempSync,rmSync,mkdirSync,copyFileSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {createRequire} from 'node:module';
import {materializeAuthoredWorkspace,authoredWorkspaceSetup} from '../materialize_authored_workspace.mjs';
import {publishSetups} from '../publish_experience_setups.mjs';
import {workspaceFixture,root} from '../../cmd/BonfyreWebAddress/tests/workspace_fixture.mjs';
import {instantiatePublicDesign} from '../../cmd/BonfyreSurfaceScene/runtime/authored-publication.mjs';
import {blankInstrument,prepareAuthoredBatch} from '../../cmd/BonfyreSurfaceScene/runtime/authored-workspace.mjs';
let f,pack,browser,temp,binding,recipientBinding,author,productionRow;
const prior={release:process.env.BONFYRE_WORKSPACE_RELEASE,packages:process.env.BONFYRE_WORKSPACE_PACKAGES};
const design={schema:'bonfyre.public-production-design.v1',title:'Workshop research and delivery',description:'Turn reviewed repair findings into planned work.',layout:'stack',appearance:'paper',panels:[{kind:'research',label:'Workshop evidence',role:'source'},{kind:'product',label:'Repair delivery',role:'output'}],connections:[{from:0,to:1,operation:'research-opportunity'}]};
beforeAll(async()=>{
 temp=mkdtempSync(join(tmpdir(),'publication-browser-'));
 const reviewed=process.env.BONFYRE_REVIEWED_RELEASE,packageRoot=reviewed?process.env.BONFYRE_REVIEWED_PACKAGES:join(temp,'packages');
 if(reviewed&&(!/^[a-f0-9]{64}$/.test(reviewed)||!packageRoot))throw Error('Supply the exact reviewed release and package root.');
 pack=reviewed?{id:reviewed}:await materializeAuthoredWorkspace({root:packageRoot});
 process.env.BONFYRE_WORKSPACE_RELEASE=pack.id;process.env.BONFYRE_WORKSPACE_PACKAGES=packageRoot;
 f=await workspaceFixture();for(const type of ['site_page','site_fragment'])copyFileSync(join(root,'cmd/BonfyreCMS/content-types',type+'.json'),join(f.home,'schemas',type+'.json'));f.run(f.cms,['schema','migrate','--db',f.db,'--schemas',join(f.home,'schemas')]);
 publishSetups({records:authoredWorkspaceSetup(pack.id),binary:f.cms,db:f.db,schemas:join(f.home,'schemas'),apply:true});
 f.run(f.auth,['update','--id',String(f.owner.id),'--tier','pro','--db',f.authdb]);author=await f.login('workspace-owner@example.test',{signup:false});
 binding=await f.install('experience.connected-instruments.workspace','self',author);recipientBinding=await f.install('experience.connected-instruments.workspace','self',f.other);
 const production=instantiatePublicDesign(design),changes=production.panels.map(p=>({key:p.recordKey,expectedRevision:0,value:{...blankInstrument(p.kind),title:'PRIVATE ORIGINAL - NOT PUBLISHED'}}));
 const batch=prepareAuthoredBatch(production,0,production.panels.map(p=>({id:p.id,kind:p.kind,key:p.recordKey,revision:0})),changes);productionRow=(await f.store(binding,author).saveLocalBatch(batch.changes)).find(r=>r.value.kind==='composition');
 const pp=createRequire(join(root,'tools/creator-ingest/package.json'))('puppeteer');browser=await pp.launch({headless:true,executablePath:'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',args:['--no-sandbox','--disable-dev-shm-usage']});
},60000);
afterAll(async()=>{await browser?.close();await f?.close();if(temp)rmSync(temp,{recursive:true,force:true});if(prior.release===undefined)delete process.env.BONFYRE_WORKSPACE_RELEASE;else process.env.BONFYRE_WORKSPACE_RELEASE=prior.release;if(prior.packages===undefined)delete process.env.BONFYRE_WORKSPACE_PACKAGES;else process.env.BONFYRE_WORKSPACE_PACKAGES=prior.packages;});
const click=(p,text,scope='body')=>p.evaluate(({text,scope})=>{const n=[...document.querySelector(scope).querySelectorAll('button')].find(n=>n.textContent.trim()===text&&!n.disabled&&n.getClientRects().length);if(!n)throw Error('Missing usable action: '+text);n.click();},{text,scope});
async function pageFor(user){const context=await browser.createBrowserContext(),page=await context.newPage();page.setDefaultTimeout(25000);await page.setViewport({width:1440,height:1000});if(user){const[name,...v]=user.cookie.split('=');await page.setCookie({name,value:v.join('='),url:f.base,httpOnly:true,sameSite:'Lax'});}return {page,context};}
test('actual editor recovers post-commit loss, publishes anonymously, reuses independently and withdraws',async()=>{
 const {page,context}=await pageFor(author),errors=[];page.on('pageerror',e=>errors.push(e.message));const sourceStore=f.store(binding,author),before=await sourceStore.readLocal('draft:instruments:');let writes=0,lose=true;
 const cdp=await page.createCDPSession();await cdp.send('Fetch.enable',{patterns:[{urlPattern:'*/cms/compose/page',requestStage:'Response'}]});
 cdp.on('Fetch.requestPaused',async e=>{try{if(e.request.method==='POST'){writes++;if(lose&&e.responseStatusCode===200){lose=false;await cdp.send('Fetch.failRequest',{requestId:e.requestId,errorReason:'ConnectionClosed'});return;}}await cdp.send('Fetch.continueResponse',{requestId:e.requestId});}catch{}});
 try{
  await page.goto(f.base+'/apps/'+binding.id+'/instruments/production/?record='+encodeURIComponent(productionRow.key),{waitUntil:'networkidle0'});await click(page,'Publish design');await page.waitForSelector('dialog[open]');await click(page,'Publish reviewed design','dialog[open]');
  await page.waitForFunction(()=>document.querySelector('dialog[open] [data-publication-state]')?.dataset.publicationState==='committed');expect(writes).toBe(1);expect(await page.$eval('dialog[open]',n=>n.textContent)).toContain('no second write was sent');
  const href=await page.$eval('dialog[open] a[href^="/productions/"]',n=>n.getAttribute('href'));const anonymous=await f.raw(href,null);expect(anonymous.status).toBe(200);const html=await anonymous.text();expect(html).toContain(design.title);expect(html).not.toContain('PRIVATE ORIGINAL');
  await click(page,'Publish reviewed design','dialog[open]');expect(writes).toBe(1);
  const reader=await pageFor(f.other);try{
   await reader.page.goto(f.base+href,{waitUntil:'networkidle0'});await Promise.all([reader.page.waitForNavigation({waitUntil:'networkidle0'}),reader.page.click('a[href^="/cms/productions?"]')]);await reader.page.waitForSelector('.production-heading');
   expect(new URL(reader.page.url()).pathname).toBe('/apps/'+recipientBinding.id+'/instruments/production/');await click(reader.page,'Save production');await reader.page.waitForFunction(()=>document.body.textContent.includes('Saved'));
   const rows=await f.store(recipientBinding,f.other).readLocal('draft:instruments:'),saved=rows.find(r=>r.value.kind==='composition');expect(saved).toBeDefined();expect(saved.key).not.toBe(productionRow.key);expect(saved.value.panels.map(p=>p.kind)).toEqual(['research','product']);expect(saved.value.connections).toHaveLength(1);expect(JSON.stringify(rows)).not.toContain('PRIVATE ORIGINAL');
   for(const width of [1440,390]){await reader.page.setViewport({width,height:1000});expect(await reader.page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);}
  }finally{await reader.context.close();}
  lose=true;await click(page,'Withdraw public design','dialog[open]');await page.waitForFunction(()=>document.querySelector('dialog[open] [data-publication-state]')?.dataset.publicationState==='committed'&&document.querySelector('dialog[open]')?.textContent.includes('withdrawn'));expect(writes).toBe(2);expect((await f.raw(href,null)).status).toBe(404);
  expect(await sourceStore.readLocal('draft:instruments:')).toEqual(before);expect(errors).toEqual([]);
  const out=process.env.BONFYRE_PUBLICATION_PROOF;if(out){mkdirSync(out,{recursive:true});await page.screenshot({path:join(out,'native-publication-recovered.png'),fullPage:true});}
 }finally{await cdp.detach();await context.close();}
},90000);
test('unreadable post-commit state exposes read-only recovery and never sends an implicit second write',async()=>{
 const {page,context}=await pageFor(author);let afterWrite=false,readsBlocked=true,writes=0;await page.setRequestInterception(true);
 page.on('request',r=>{const url=new URL(r.url());if(url.pathname==='/cms/compose/page'&&r.method()==='GET'&&afterWrite&&readsBlocked)return void r.abort();r.continue().catch(()=>{});});
 const cdp=await page.createCDPSession();await cdp.send('Fetch.enable',{patterns:[{urlPattern:'*/cms/compose/page',requestStage:'Response'}]});cdp.on('Fetch.requestPaused',async e=>{try{if(e.request.method==='POST'){writes++;afterWrite=true;await cdp.send('Fetch.failRequest',{requestId:e.requestId,errorReason:'ConnectionClosed'});}else await cdp.send('Fetch.continueResponse',{requestId:e.requestId});}catch{}});
 try{await page.goto(f.base+'/apps/'+binding.id+'/instruments/production/?record='+encodeURIComponent(productionRow.key),{waitUntil:'networkidle0'});await click(page,'Publish design');await page.waitForSelector('dialog[open]');await click(page,'Publish reviewed design','dialog[open]');await page.waitForFunction(()=>document.querySelector('[data-publication-state]')?.dataset.publicationState==='uncertain');expect(writes).toBe(1);
  expect(await page.$$eval('dialog[open] button',ns=>ns.filter(n=>['Publish reviewed design','Withdraw public design'].includes(n.textContent)).every(n=>n.disabled))).toBe(true);
  readsBlocked=false;await click(page,'Check saved publication','dialog[open]');await page.waitForFunction(()=>document.querySelector('[data-publication-state]')?.dataset.publicationState==='committed');expect(writes).toBe(1);
 }finally{await cdp.detach();await context.close();}
},90000);


test('Production Studio shares pane resize, retained inspector tabs and commands without replacing source records',async()=>{
 const {page,context}=await pageFor(author),errors=[];page.on('pageerror',e=>errors.push(e.message));const store=f.store(binding,author),before=await store.readVersion(productionRow.key);
 try{await page.setViewport({width:1600,height:1050});await page.goto(f.base+'/apps/'+binding.id+'/instruments/production/?record='+encodeURIComponent(productionRow.key),{waitUntil:'networkidle0'});await page.waitForFunction(()=>document.querySelector('.production-status')?.textContent.startsWith('Opened production'));
 const tabs='.production-tabs [role=tab]';expect(await page.$eval('.production-tabs',n=>n.getAttribute('aria-label'))).toBe('Production configuration');await page.focus(tabs);await page.keyboard.press('End');expect(await page.evaluate(()=>document.activeElement.textContent)).toBe('Appearance');await page.keyboard.press('Enter');expect(await page.$eval('.production-tabs [aria-selected=true]',n=>n.textContent)).toBe('Appearance');
 await page.evaluate(()=>window.sameArrangement=document.querySelector('select[aria-label="Arrangement"]'));await page.focus('.production-tabs [aria-selected=true]');await page.keyboard.press('Home');await page.keyboard.press('Enter');await page.keyboard.press('End');await page.keyboard.press('Enter');expect(await page.evaluate(()=>window.sameArrangement===document.querySelector('select[aria-label="Arrangement"]'))).toBe(true);
 const handle='.production-workarea [role=separator]';await page.waitForSelector('.ak-work-rail[data-docked=true]');expect(await page.$eval('.production-workarea',n=>n.getBoundingClientRect().width)).toBeLessThan(980);await page.click('[aria-label="Close Aurekai activity"]');await page.waitForFunction(()=>document.querySelector('.production-workarea').dataset.splitMode==='split');await page.focus(handle);await page.keyboard.press('Home');expect(await page.$eval(handle,n=>n.getAttribute('aria-valuenow'))).toBe('45');await page.keyboard.press('End');expect(await page.$eval(handle,n=>n.getAttribute('aria-valuenow'))).toBe('82');await page.click('.ak-rail-launch');await page.waitForFunction(()=>document.querySelector('.production-workarea').dataset.splitMode==='stacked');expect(await page.$eval('.production-tabs [aria-selected=true]',n=>n.textContent)).toBe('Appearance');await page.click('[aria-label="Close Aurekai activity"]');await page.waitForFunction(()=>document.querySelector('.production-workarea').dataset.splitMode==='split');expect(await page.$eval(handle,n=>n.getAttribute('aria-valuenow'))).toBe('82');
 await page.click('[data-production-commands]');await page.type('.ak-command-dialog[open] input','Configure behavior');await page.keyboard.press('Enter');await page.waitForFunction(()=>document.querySelector('.production-tabs [aria-selected=true]')?.textContent==='Behavior');expect(await page.$('.ak-command-dialog[open]')).toBeNull();
 expect((await store.readVersion(productionRow.key)).revision).toBe(before.revision);for(const width of [1600,390]){await page.setViewport({width,height:1000});await page.waitForFunction(()=>document.documentElement.scrollWidth<=innerWidth);expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);}await page.waitForFunction(()=>document.querySelector('.production-workarea')?.dataset.splitMode==='stacked');expect(await page.$eval(handle,n=>n.hidden)).toBe(true);
 await page.setViewport({width:1600,height:1050});await page.$eval('#production-description',n=>{n.value='Prepared in the same native production.';n.dispatchEvent(new Event('input',{bubbles:true}));});await page.click('[data-production-commands]');await page.type('.ak-command-dialog[open] input','Save production');await page.keyboard.press('Enter');await page.waitForFunction(()=>document.querySelector('.production-status')?.textContent.startsWith('Saved production'));const saved=await store.readVersion(productionRow.key);expect(saved.revision).toBe(before.revision+1);expect(saved.value.panels).toEqual(before.value.panels);expect(saved.value.description).toBe('Prepared in the same native production.');
 await page.reload({waitUntil:'networkidle0'});await page.waitForFunction(()=>document.querySelector('.production-status')?.textContent.startsWith('Opened production'));expect(await page.$eval('#production-description',n=>n.value)).toBe('Prepared in the same native production.');expect(errors).toEqual([]);
 }finally{await context.close();}
},60000);
