import assert from 'node:assert/strict';
import {createServer} from 'node:http';
import {readFile,writeFile,mkdir} from 'node:fs/promises';
import {resolve,extname} from 'node:path';
import {setTimeout as pause} from 'node:timers/promises';
import {runReuseChecks} from './component-reuse-checks.mjs';
const {chromium}=await import(process.env.PLAYWRIGHT_MODULE||'playwright');
const output=resolve('component-evidence/replacement');await mkdir(output,{recursive:true});
const checks=[],errors=[],posts=[],records={};
const check=(name,value)=>{assert.ok(value,name);checks.push(name);console.log('PASS '+name);};
const browser=await chromium.launch({executablePath:process.env.CHROME_PATH||'/usr/bin/google-chrome',headless:true,args:['--no-sandbox','--use-gl=angle','--use-angle=swiftshader','--enable-unsafe-swiftshader']});
let activePage;
async function serve(mode,{mutate=false}={}){const catalogue=resolve(mode==='before'?'component-donors/catalogue-before':'site/spaces'),itad=resolve(mode==='before'?'component-donors/itad-before':'component-donors/itad/site');const server=createServer(async(req,res)=>{try{if(req.method!=='GET')throw Error('Read-only test server');let path=decodeURIComponent(new URL(req.url,'http://localhost').pathname);const prefix='/bonfyre/spaces/',base=path.startsWith(prefix)?catalogue:itad;path=path.startsWith(prefix)?path.slice(prefix.length):path.slice(1);const file=resolve(base,path+(path.endsWith('/')?'index.html':''));if(!file.startsWith(base+'/'))throw Error('Outside source');const bytes=mutate&&path.includes('/interaction-')&&path.endsWith('/record-grid.mjs')?Buffer.from("export function createRecordGrid(){throw Error('Shared renderer deliberately withheld');}"):await readFile(file);res.writeHead(200,{'Content-Type':({'.mjs':'text/javascript','.js':'text/javascript','.css':'text/css','.html':'text/html','.svg':'image/svg+xml','.png':'image/png','.glb':'model/gltf-binary','.wasm':'application/wasm'})[extname(file)]||'application/octet-stream'});res.end(bytes);}catch{res.writeHead(404);res.end('Not found');}});await new Promise(r=>server.listen(0,'127.0.0.1',r));return {server,origin:'http://127.0.0.1:'+server.address().port};}
async function settledText(locator,expected){let actual='';for(let n=0;n<100;n++){actual=(await locator.innerText()).trim();if(actual===expected)return true;await pause(50);}assert.equal(actual,expected,'Grid display must converge to the exact requested value');}
async function cellEdit(page,locator,value){await locator.dblclick();const input=page.locator('.ag-cell-inline-editing input').first();await input.fill(value);await input.press('Enter');const close=page.locator('[data-grid-action="close-inspector"]');if(await close.isVisible())await close.click();}
async function catalogueFlow(page,origin,mode){
 await page.goto(origin+'/bonfyre/spaces/catalogue/?store=wayfair-reference&workspace=inventory');await page.waitForSelector('[data-ui="inventory"][data-ready="true"]');
 const host=page.locator('[data-ui="inventory"]'),row=()=>host.locator('.ag-row').first(),price=()=>row().locator('[col-id="price"]');
 await host.locator('[data-grid-action="options"]').click();await price().waitFor();await settledText(price(),'69.99');
 const before=(await price().innerText()).trim();
 await cellEdit(page,price(),'79.99');await page.waitForFunction(()=>document.querySelector('[data-grid="changes"]')?.textContent==='1');
 check(mode+' catalogue edits retained option through the original cents validator',await settledText(price(),'79.99'));
 await host.locator('[data-grid-action="undo"]').click();check(mode+' catalogue undo restores the observed value',await settledText(price(),before));
 await host.locator('[data-grid-action="redo"]').click();check(mode+' catalogue redo restores the edit',await settledText(price(),'79.99'));
 await host.locator('.ag-row .ag-selection-checkbox input').first().check();
 await host.locator('[data-grid="bulk-action"]').selectOption('price');await host.locator('[data-grid="bulk-value"]').fill('89.99');await host.locator('[data-grid-action="preview"]').click();
 await host.locator('dialog[open]').waitFor();check(mode+' catalogue reviews before applying',await host.locator('[data-grid="dialog-body"]').innerText().then(s=>s.includes('79.99')&&s.includes('89.99')));
 await host.locator('[data-grid="dialog-confirm"]').click();check(mode+' catalogue selection survives applying a bulk edit',(await host.locator('[data-grid="selection"]').innerText()).includes('1 selected'));
 await host.locator('[data-grid-action="review"]').click();await host.locator('dialog[open]').waitFor();await host.locator('[data-grid="dialog-confirm"]').click();await page.waitForFunction(()=>document.querySelector('[data-grid="notice"]')?.textContent.includes('drafts saved'));
 const script=await page.locator('script[src*="catalogue-client.mjs"]').getAttribute('src'),module=new URL('store.mjs',origin+script).href;
 const saved=await page.evaluate(async module=>{const {readLocal}=await import(module);return readLocal('draft:catalogue:');},module);
 check(mode+' catalogue commits through its existing browser store',saved.length>0);
 await page.reload();await page.waitForSelector('[data-ui="inventory"][data-ready="true"]');await host.locator('[data-grid-action="options"]').click();
 check(mode+' catalogue reload restores shared-component edits',await settledText(price(),'89.99'));
 const model=await page.evaluate(async url=>{const{readLocal}=await import(url);return readLocal('draft:catalogue:');},module);
 await page.screenshot({path:output+'/'+mode+'-catalogue.png',fullPage:true});
 return model.map(r=>({key:r.key,value:{...r.value,saved_at:undefined}}));
}
async function itadState(page){return page.evaluate(async()=>{const {openDraft}=await import('/journey-kit/room.mjs');return (await (await openDraft('itad-exchange.v1')).read()).value;});}
async function itadFlow(page,origin,mode){
 await page.goto(origin+'/itad-exchange/?lot=houston35#manifest');await page.waitForSelector('#manifestGrid[data-ready="true"]');
 await page.locator('#sampleValues').click();await page.locator('#applySample').click();await page.waitForFunction(()=>document.querySelector('#lineCount')?.textContent==='2');
 const qty=()=>page.locator('#manifestGrid .ag-row [col-id="qty"]').first();
 await cellEdit(page,qty(),'21');await page.waitForFunction(()=>document.querySelector('#manifestResult').textContent.includes('36 modeled'));
 check(mode+' ITAD quantity edit changes valuation inputs and mismatch',true);
 await page.locator('#undo').click();await page.waitForFunction(()=>document.querySelector('#manifestResult').textContent.includes('35 modeled'));check(mode+' ITAD undo restores original manifest totals',true);
 await page.locator('#redo').click();await page.waitForFunction(()=>document.querySelector('#manifestResult').textContent.includes('36 modeled'));check(mode+' ITAD redo restores changed manifest totals',true);
 await page.locator('#manifestGrid .ag-row .ag-selection-checkbox input').first().check();await page.locator('#bulkField').selectOption('grade');await page.locator('#bulkGrade').selectOption('parts');await page.locator('#previewBulk').click();
 await page.locator('#bulkDialog[open]').waitFor();check(mode+' ITAD reviews the selected manifest change',(await page.locator('#bulkDiff').innerText()).includes('parts'));await page.locator('#applyBulk').click();
 check(mode+' ITAD applied bulk edit retains row selection',(await page.locator('#selectionCount').innerText()).includes('1'));
 await page.locator('#saveDraft').click();await page.waitForFunction(()=>document.querySelector('#savedState').textContent.startsWith('Saved locally'));
 const saved=await itadState(page);check(mode+' ITAD retains exact row identity and cent estimates',saved.records.houston35.rows[0].qty===21&&saved.records.houston35.rows[0].grade==='parts'&&Number.isInteger(saved.records.houston35.rows[0].recovery));
 await page.reload();await page.waitForSelector('#manifestGrid[data-ready="true"]');check(mode+' ITAD reload uses the original draft record',(await itadState(page)).records.houston35.rows[0].qty===21);
 await page.locator('#manifestGrid .manifest-open-line').first().click();await page.locator('#lineInspector:not([hidden])').waitFor();check(mode+' ITAD line inspector still continues the selection',await page.locator('#lineForm input[name=model]').inputValue().then(Boolean));await page.locator('#closeInspector').click();
 await page.locator('[data-view="economics"]').click();check(mode+' ITAD valuation and sensitivity remain available',await page.locator('#sensitivity').innerText().then(s=>s.length>0));
 await page.locator('[data-view="buyer"]').click();check(mode+' ITAD buyer preparation retains its source questions',await page.locator('#buyerPaper').innerText().then(s=>s.length>100));
 await page.locator('[data-view="manifest"]').click();await page.setViewportSize({width:390,height:844});check(mode+' ITAD stays within mobile viewport',await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth));await page.screenshot({path:output+'/'+mode+'-itad-mobile.png',fullPage:true});
 return {record:saved.records.houston35,selected:saved.selected,recipient:saved.recipient,budget:saved.budget};
}
try{
 for(const mode of ['before','after']){const {server,origin}=await serve(mode),context=await browser.newContext({viewport:{width:1440,height:1050}});try{const page=activePage=await context.newPage();page.setDefaultTimeout(20000);page.on('pageerror',e=>errors.push(mode+': '+e.message));page.on('request',r=>{if(r.method()==='POST')posts.push(r.url());});records[mode]={catalogue:await catalogueFlow(page,origin,mode)};await page.setViewportSize({width:1440,height:1050});records[mode].itad=await itadFlow(page,origin,mode);}catch(error){await writeFile(output+'/'+mode+'-failure.html',await activePage.content());await activePage.screenshot({path:output+'/'+mode+'-failure.png',fullPage:true});throw error;}finally{await context.close();server.close();}}
 assert.deepEqual(records.before.catalogue,records.after.catalogue);check('generalized catalogue components produce identical source-linked drafts',true);
 assert.deepEqual(records.before.itad,records.after.itad);check('generalized ITAD components produce exactly the original domain record',true);
 check('neither donor workflow sends an external effect',posts.length===0);check('both original and replacement flows run without script errors',errors.length===0);
 await runReuseChecks({serve,browser,check,edit:cellEdit});
 await writeFile(output+'/RESULTS.txt',checks.map(x=>'PASS '+x).join('\n')+'\n');
}catch(error){await writeFile(output+'/FAILURE.txt',error.stack+'\n'+errors.join('\n'));throw error;}finally{await browser.close();}
