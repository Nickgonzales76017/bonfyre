import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import {mkdtempSync,mkdirSync,writeFileSync,readFileSync,rmSync,existsSync} from 'node:fs';
import {join,resolve} from 'node:path';
import {tmpdir} from 'node:os';
const repo=resolve(import.meta.dir,'../../..'),extension=process.env.WORKBENCH_TEST_ROOT??join(repo,'integrations/browser/extension');
const publicOrigin=process.env.WORKBENCH_TEST_ORIGIN;
if(publicOrigin&&publicOrigin!=='https://browser-workbench.aurekai-work-samples.pages.dev')throw Error('Unapproved public verification origin.');
const memory=Bun.spawnSync(['/usr/bin/memory_pressure','-Q'],{stdout:'pipe'}).stdout.toString();
if(Number(memory.match(/free percentage: (\d+)%/)?.[1])<18)throw Error('Browser check waits for 18% free memory.');
const puppeteer=createRequire(join(repo,'tools/creator-ingest/package.json'))('puppeteer');
const root=mkdtempSync(join(tmpdir(),'bonfyre-program-browser-')),out=process.env.WORKBENCH_TEST_PROOF??join(repo,'output/verification/workbench-program-editor');mkdirSync(out,{recursive:true});
const errors=[],checks=[],check=(name,value)=>{assert.ok(value,name);checks.push(name);console.log('PASS '+name);};
const server=publicOrigin?null:Bun.serve({hostname:'127.0.0.1',port:0,fetch(request){const path=new URL(request.url).pathname.slice(1);if(!/^[a-z0-9.-]+$/.test(path)||!existsSync(join(extension,path)))return new Response('Not found',{status:404});const file=Bun.file(join(extension,path));return new Response(file,{headers:{'content-type':path.endsWith('.mjs')?'text/javascript':path.endsWith('.css')?'text/css':path.endsWith('.json')?'application/json':'text/html'}});}});
const origin=publicOrigin??('http://127.0.0.1:'+server.port),options={executablePath:'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',headless:true,userDataDir:join(root,'profile'),args:['--no-sandbox','--disable-gpu','--disable-dev-shm-usage']};
const blocked=[];
async function guard(page){
  page.on('pageerror',e=>errors.push(e.message));await page.setRequestInterception(true);
  page.on('request',request=>{const url=request.url();if((url.startsWith(origin+'/')&&request.method()==='GET')||url.startsWith('data:')||url.startsWith('blob:'))request.continue();else{blocked.push({method:request.method(),url});request.abort();}});
}
let browser;
try {
  browser=await puppeteer.launch(options);const page=await browser.newPage();await guard(page);page.setDefaultTimeout(12000);await page.setViewport({width:1440,height:1000});
  await page.goto(origin+'/workbench.html',{waitUntil:'networkidle0'});await page.waitForSelector('#program-name');await page.click('[data-view="schedule"]');
  check('human fields are the primary editor',await page.$eval('#program-name',e=>e.checkVisibility())&&await page.$eval('#input',e=>!e.checkVisibility()&&e.type==='hidden'));
  check('web mode disables native actions and offers the public samples entry point',await page.$eval('#capture',e=>e.disabled)&&await page.$eval('#open-collected',e=>e.disabled)&&await page.$eval('#web-workspace-notice',e=>e.textContent.includes('this browser')));
  const edit=async(selector,value)=>{await page.$eval(selector,e=>{e.value='';});await page.type(selector,value);};
  await page.click('[data-view="sources"]');
  for(let n=1;n<=2;n++) {
    const file=join(root,'source-'+n+'.json');writeFileSync(file,JSON.stringify({url:'https://fixture.invalid/box-'+n,title:'Fixture source '+n,captured_at:'2026-10-15T00:00:00Z',fixture:true,selected_text:'Comparison terms '+n,term_snippets:['Cancel before the next cycle.'],structured:[{'@context':'https://schema.org','@type':'Product',name:'Fixture box '+n,offers:{'@type':'Offer',price:'45.00',priceCurrency:'USD'}}]}));
    await(await page.$('#import')).uploadFile(file);await page.waitForSelector('#import-dialog[open]');await page.click('#import-commit');await page.waitForFunction(()=>!document.querySelector('#import-dialog').open);await page.waitForFunction(n=>document.querySelectorAll('[data-source-id]').length===n,{},n);
  }
  check('two selected sources remain separate',await page.$eval('#selected-sources',e=>e.textContent.startsWith('2 ')));
  await page.click('[data-view="schedule"]');
  await edit('#program-name','Monthly source comparison');await edit('#program-price','12.35');await edit('#program-shipping','2.10');await page.click('#compile');
  await page.waitForFunction(()=>document.querySelector('.cycle-preview article'));
  await page.click('#save-draft');await page.waitForSelector('[data-draft-id]');
  const storedDraft=()=>page.evaluate(async()=>{const {readLocal}=await import('./store.mjs');return (await readLocal('draft:')).find(r=>r.value.name==='Monthly source comparison').value;});
  let draft=await storedDraft();check('human money reaches the unchanged compiler as exact cents',draft.terms.price_cents===1235&&draft.terms.shipping_cents===210);
  check('draft carries two source digests without copying source bodies',draft.browser_evidence.length===2&&!JSON.stringify(draft.browser_evidence).includes('Comparison terms'));
  check('stages and dates remain previews instead of invented completions',draft.stage_plan.every(s=>s.state==='not_executed')&&draft.cycle_preview.every(s=>s.effect==='none'));
  await page.click('#save-draft');await page.waitForSelector('[data-draft-id]');const id=draft.program_id;
  check('budget displays dollar months rather than machine output',await page.$eval('#basket',e=>e.textContent.includes('$14.45')&&!e.textContent.includes('modeled_cents')));
  await page.reload({waitUntil:'networkidle0'});await page.click('[data-draft-id]');await page.waitForFunction(()=>document.querySelector('#program-name').value==='Monthly source comparison');
  check('reopened draft restores both exact source selections',await page.$$eval('[data-source-id]',rows=>rows.every(e=>e.checked)&&rows.length===2));
  const other=await browser.newPage();await guard(other);await other.goto(origin+'/workbench.html',{waitUntil:'networkidle0'});await other.click('[data-draft-id]');await other.waitForFunction(()=>document.querySelector('#program-name').value==='Monthly source comparison');
  await page.bringToFront();await page.click('[data-view="schedule"]');await edit('#program-price','16.75');await page.click('#compile');await page.click('#save-draft');await page.waitForFunction(()=>document.querySelector('#draft-summary').textContent.startsWith('Draft kept'));
  await other.bringToFront();await other.$eval('#program-price',e=>{e.value='19.50';e.dispatchEvent(new Event('input',{bubbles:true}));});await other.click('#compile');await other.click('#save-draft');await other.waitForFunction(()=>document.querySelector('#error').textContent.includes('another tab'));
  check('stale editing tab cannot overwrite a newer saved draft',await other.$eval('#error',e=>e.textContent.includes('another tab')));await other.close();
  await page.bringToFront();await page.click('[data-view="sources"]');await page.click('[data-source-id]');check('changing selected evidence requires a new review',await page.$eval('#save-draft',e=>e.disabled));await page.click('#compile');await page.click('#save-draft');await page.waitForFunction(()=>document.querySelector('#draft-summary').textContent.startsWith('Draft kept'));
  draft=await storedDraft();check('deselection changes only the draft references',draft.browser_evidence.length===1&&await page.$$eval('[data-source-id]',rows=>rows.length===2));
  await page.screenshot({path:join(out,'desktop.png'),fullPage:true});
  const templates=await page.evaluate(async()=> (await(await fetch('./catalog.json')).json()).templates);
  for(const t of templates){await page.click('[data-view="schedule"]');await page.select('#template',t.id);await page.click('#compile');const error=await page.$eval('#error',e=>e.textContent);assert.equal(error,'',t.id+': '+error);const text=await page.$eval('#draft-output',e=>e.textContent);assert.ok(text.startsWith(t.name+'\n'),t.id+': readable proposal belongs to selected pattern');}
  check('all 36 existing templates compile through the human editor',templates.length===36);
  await page.click('[data-view="schedule"]');await edit('#program-price','12.345');await page.click('#compile');check('excess precision is refused without silently rounding',await page.$eval('#error',e=>e.textContent.includes('decimal')));
  await page.click('[data-draft-id]');await page.waitForFunction(()=>document.querySelector('#program-name').value==='Monthly source comparison').catch(async error=>{console.log(JSON.stringify(await page.evaluate(()=>({name:document.querySelector('#program-name').value,error:document.querySelector('#error').textContent,drafts:document.querySelector('#drafts').textContent}))));throw error;});await page.setViewport({width:390,height:844});
  check('source selection and program editor fit mobile width',await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth));await page.screenshot({path:join(out,'mobile.png'),fullPage:true});
  await browser.close();browser=await puppeteer.launch(options);const reopened=await browser.newPage();await guard(reopened);await reopened.goto(origin+'/workbench.html',{waitUntil:'networkidle0'});await reopened.click('[data-draft-id]');await reopened.waitForFunction(()=>document.querySelector('#program-name').value==='Monthly source comparison');
  check('saved terms survive a full browser restart',await reopened.$eval('#program-price',e=>e.value==='16.75')&&await reopened.$$eval('[data-source-id]',rows=>rows.filter(e=>e.checked).length===1));
  const studio=await browser.newPage();await guard(studio);await studio.goto(origin+'/studio.html',{waitUntil:'networkidle0'});await studio.click('[data-tab="rhythm"]');
  check('the existing full studio reads the saved workbench program without re-entry',await studio.$eval('#rhythm-list',e=>e.textContent.includes('Monthly source comparison')));await studio.close();
  check('browser reports no uncaught runtime errors',errors.length===0);
  check('browser made no third-party requests or remote write attempts',blocked.length===0);
  writeFileSync(join(out,'readback.json'),JSON.stringify({at:new Date().toISOString(),checks,errors,sourceFixtures:2,liveNativeEffects:'not exercised by this browser test',publicDeployment:!!publicOrigin,origin,blockedRequests:blocked},null,2)+'\n');
} finally {await browser?.close();server?.stop(true);rmSync(root,{recursive:true,force:true});}
