import {test,expect,beforeAll,afterAll} from 'bun:test';
import {copyFileSync,mkdirSync} from 'node:fs';
import {join} from 'node:path';
import {createRequire} from 'node:module';
import {packSetupFixture,root,password} from './pack_setup_fixture.mjs';
let f,writer,other,free,browser;
const json=r=>JSON.parse(r.body);
const page={route:'/editor-proof',title:'A working page',heading:'Our shared material',status:'draft'};
const section=(coordinate,heading)=>({coordinate,page:page.route,slot:'body',audience:'anonymous',kind:'copy',position:0,heading,body:'First paragraph.\n\nSecond paragraph.',status:'draft',source_ref:'source:fixture',source_digest:'a'.repeat(64),evidence_ref:'receipt:fixture'});
async function read(route=page.route,cookie=writer.cookie){return f.request('/cms/compose/page?route='+encodeURIComponent(route),{cookie});}
async function save(value,cookie=writer.cookie,headers={}){return f.request('/cms/compose/page',{cookie,form:JSON.stringify(value),headers:{'Content-Type':'application/json',...headers}});}
function payload(snapshot,patch={}){const clean=x=>Object.fromEntries(Object.entries(x).filter(([k])=>!['id','namespace','created_at','updated_at','published_at'].includes(k)));return {operation:snapshot.id?'replace':'create',route:snapshot.snapshot.route,expected_id:snapshot.id,expected_digest:snapshot.digest,page:snapshot.id?clean(snapshot.snapshot.page):page,fragments:snapshot.snapshot.fragments.map(clean),...patch};}
beforeAll(async()=>{
 f=await packSetupFixture();const schemas=join(f.home,'schemas');copyFileSync(join(root,'cmd/BonfyreCMS/content-types/site_fragment.json'),join(schemas,'site_fragment.json'));f.run(f.cms,['schema','migrate','--db',f.db,'--schemas',schemas]);
 for(const name of ['composer','other-composer'])f.run(f.auth,['--db',f.authdb,'signup','--email',name+'@example.test','--name',name,'--password',password,'--tier','pro']);
 writer=await f.login('composer@example.test',{signup:false});other=await f.login('other-composer@example.test',{signup:false});free=await f.login('free-composer@example.test');
 const puppeteer=createRequire(join(root,'tools/creator-ingest/package.json'))('puppeteer');browser=await puppeteer.launch({executablePath:'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',headless:true,args:['--no-sandbox']});
},30000);
afterAll(async()=>{await browser?.close();await f?.close();},30000);
test('composition rechecks grants, same origin and actor namespace',async()=>{
 expect((await f.request('/cms/compose')).status).toBe(403);
 expect((await f.request('/cms/compose',{cookie:free.cookie})).status).toBe(403);
 const initial=json(await read());expect(initial.id).toBe(0);
 const input=payload(initial,{fragments:[section('proof:first','First section')]});
 expect((await save(input,writer.cookie,{Origin:'https://other.test'})).status).toBe(403);
 expect((await save(input,writer.cookie,{Origin:''})).status).toBe(403);
 expect((await save(input)).status).toBe(200);
 const retained=json(await read());expect(retained.snapshot.namespace).toBe('actor-'+writer.id);expect(retained.snapshot.fragments[0].source_ref).toBe('source:fixture');
 expect(json(await read(page.route,other.cookie)).id).toBe(0);
 expect(json(await f.request('/cms/compose/pages',{cookie:other.cookie})).pages).toEqual([]);
 expect(json(await f.request('/cms/compose/pages',{cookie:writer.cookie})).pages[0].route).toBe(page.route);
});
test('whole-page writes reject stale snapshots and retain the saved aggregate',async()=>{
 const opened=json(await read());const first=payload(opened);first.page.heading='Updated elsewhere';
 expect((await save(first)).status).toBe(200);
 const stale=payload(opened);stale.page.heading='Overwritten';expect((await save(stale)).status).toBe(409);
 expect(json(await read()).snapshot.page.heading).toBe('Updated elsewhere');
 const current=json(await read()),invalid=payload(current);invalid.fragments[0].page='/different-page';expect((await save(invalid)).status).toBe(409);expect(json(await read()).digest).toBe(current.digest);
});
test('browser creates, rearranges, saves and reopens a composition with provenance',async()=>{
 const context=await browser.createBrowserContext(),tab=await context.newPage(),errors=[];tab.on('pageerror',e=>errors.push(e.message));
 const [name,...rest]=writer.cookie.split('=');await tab.setCookie({name,value:rest.join('='),url:f.base,httpOnly:true});
 try{
 await tab.goto(f.base+'/cms/compose?route=/browser-composition',{waitUntil:'networkidle0'});await tab.waitForSelector('#field-title');
 const fill=async(id,value)=>{await tab.$eval('#'+id,(n,value)=>{n.focus();n.value=value;n.dispatchEvent(new Event('input',{bubbles:true}));n.dispatchEvent(new Event('change',{bubbles:true}));},value);};
 await fill('field-title','Field notes');await fill('field-heading','A useful composition');await fill('field-lede','From a blank page to a retained composition.');
 await tab.click('#add');await fill('field-heading','First section');await fill('field-body','<script>window.injected=true</script>\nRetained text.');await fill('field-source_ref','source:browser-proof');await fill('field-evidence_ref','receipt:browser-proof');
 await tab.click('#add');await fill('field-heading','Second section');await tab.click('#up');
 expect(await tab.$$eval('#canvas h3',x=>x.map(n=>n.textContent))).toEqual(['Second section','First section']);
 await tab.click('#undo');expect(await tab.$$eval('#canvas h3',x=>x.map(n=>n.textContent))).toEqual(['First section','Second section']);await tab.click('#redo');
 await tab.click('#save');await tab.waitForFunction(()=>document.querySelector('#notice').textContent.includes('saved and reopened'));
 expect(await tab.$eval('#dirty',x=>x.textContent)).toBe('Saved version');expect(await tab.evaluate(()=>window.injected)).toBeUndefined();
 await tab.reload({waitUntil:'networkidle0'});await tab.waitForSelector('#field-title');expect(await tab.$$eval('#canvas h3',x=>x.map(n=>n.textContent))).toEqual(['Second section','First section']);
 let dropped=0;await tab.setRequestInterception(true);
 const intercept=request=>{void(async()=>{if(request.method()==='POST'&&new URL(request.url()).pathname==='/cms/compose/page'&&dropped===0){dropped++;const result=await save(JSON.parse(request.postData()));if(result.status!==200)throw Error('Native lost-reply setup failed');await request.abort();}else await request.continue();})().catch(()=>request.abort().catch(()=>{}));};
 tab.on('request',intercept);await fill('field-lede','Saved despite an interrupted reply');await tab.click('#save');await tab.waitForFunction(()=>document.querySelector('#notice').textContent.includes('confirms your complete composition is saved'));
 expect(dropped).toBe(1);expect(await tab.$eval('#dirty',n=>n.textContent)).toBe('Saved version');tab.off('request',intercept);await tab.setRequestInterception(false);
 const stored=json(await read('/browser-composition'));expect(stored.snapshot.fragments[1].source_ref).toBe('source:browser-proof');expect(stored.snapshot.fragments[1].evidence_ref).toBe('receipt:browser-proof');
 await fill('field-heading','Local edit survives conflict');const external=payload(stored);external.page.heading='Remote edit';expect((await save(external)).status).toBe(200);
 await tab.click('#save');await tab.waitForFunction(()=>document.querySelector('#notice').textContent.includes('draft is still here'));expect(await tab.$eval('#field-heading',n=>n.value)).toBe('Local edit survives conflict');expect(await tab.$eval('#save',n=>n.disabled)).toBe(true);
 await tab.setViewport({width:390,height:844});expect(await tab.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);
 const proof='/private/tmp/bonfyre-page-authoring/composer-proof';mkdirSync(proof,{recursive:true});await tab.screenshot({path:join(proof,'mobile.png'),fullPage:true});await tab.setViewport({width:1440,height:1000});await tab.screenshot({path:join(proof,'desktop.png'),fullPage:true});expect(errors).toEqual([]);
 }finally{await context.close();}
},30000);
