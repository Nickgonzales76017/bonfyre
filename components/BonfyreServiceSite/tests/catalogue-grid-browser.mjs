import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import {mkdirSync,mkdtempSync,rmSync,writeFileSync} from 'node:fs';
import {resolve,join,sep} from 'node:path';import{tmpdir}from'node:os';
const live=process.argv.includes('--live'),root=resolve(process.env.CATALOGUE_BUILD??'/tmp/aurekai-frontend-upgrade');
const out=resolve(process.env.CATALOGUE_GRID_PROOF??`packs/no-simulation/product-maturity-20261015/frontend/${live?'live-grid':'local-grid'}`);mkdirSync(out,{recursive:true});
const puppeteer=createRequire(resolve('tools/creator-ingest/package.json'))('puppeteer'),profile=mkdtempSync(join(tmpdir(),'catalogue-grid-'));
const server=live?null:Bun.serve({hostname:'127.0.0.1',port:0,async fetch(r){if(!['GET','HEAD'].includes(r.method))return new Response('No writes',{status:405});const u=new URL(r.url);let f=resolve(root,'.'+decodeURIComponent(u.pathname));if(!f.startsWith(root+sep))return new Response('Not found',{status:404});if(u.pathname.endsWith('/'))f=join(f,'index.html');return await Bun.file(f).exists()?new Response(Bun.file(f)):new Response('Not found',{status:404});}});
const origin=live?'https://aurekai-work-samples.pages.dev':`http://127.0.0.1:${server.port}`,report={origin,checks:[],errors:[],writes:[],status:'running'};
const check=(name,value)=>{assert(value,name);report.checks.push(name);console.log('PASS '+name);};let browser;
const $=name=>`[data-grid="${name}"]`,action=name=>`[data-grid-action="${name}"]`,cell=(id,col)=>`.ag-row[row-id="${id}"] [col-id="${col}"]`;
const waitReady=page=>page.waitForSelector('[data-ui="inventory"][data-ready="true"]');
const text=async(page,selector)=>page.$eval(selector,n=>n.textContent);
async function edit(page,id,col,value){await page.bringToFront();await page.click(cell(id,col),{clickCount:2});await page.waitForSelector(cell(id,col)+' input');const input=await page.$(cell(id,col)+' input');await input.click({clickCount:3});await input.type(value);await page.keyboard.press('Enter');}
async function replace(page,selector,value){await page.click(selector,{clickCount:3});await page.keyboard.type(value);}
try{
 browser=await puppeteer.launch({headless:true,executablePath:'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',userDataDir:profile});
 const page=await browser.newPage();page.setDefaultTimeout(15000);page.on('pageerror',e=>report.errors.push(e.message));page.on('console',m=>{if(m.type()==='error')report.errors.push(m.text());});page.on('request',r=>{if(!['GET','HEAD'].includes(r.method()))report.writes.push(r.url());});
 await page.setViewport({width:1440,height:1000});await page.goto(origin+'/catalogue/',{waitUntil:'networkidle2'});await waitReady(page);
 check('default route opens a populated product table',Number(await text(page,$('products')))===223&&await text(page,$('visible'))==='223 products shown');
 check('thousands of options are virtualized',await page.$$eval('.ag-row[row-id]',r=>r.length>0&&r.length<60));
 const ids=await page.$$eval('.ag-row[row-id]',ns=>ns.slice(0,2).map(n=>n.getAttribute('row-id')));
 await page.click('[data-grid-filter="missing"]');await page.waitForFunction(()=>document.querySelector('[data-grid="visible"]').textContent!=='223 products shown');check('missing identifiers filter actual products',await text(page,$('visible'))!=='223 products shown');await page.click('.cw-grid-tools [data-grid-filter="all"]');
 await page.type($('search'),'DP4-BK-AL-4.5');await page.waitForFunction(()=>document.querySelector('[data-grid="visible"]').textContent==='1 products shown');check('quick search narrows to one product',await text(page,$('visible'))==='1 products shown');await replace(page,$('search'),'');await page.keyboard.press('Backspace');
 await edit(page,ids[0],'title','Edited walnut selection');await page.waitForFunction(()=>document.querySelector('[data-grid="changes"]').textContent==='1');check('inline title edits reach product state',await text(page,cell(ids[0],'title'))==='Edited walnut selection');
 await page.click(action('undo'));check('undo restores source title',!(await text(page,cell(ids[0],'title'))).includes('Edited walnut'));await page.click(action('redo'));check('redo restores the edit',await text(page,cell(ids[0],'title'))==='Edited walnut selection');
 for(const id of ids)await page.click(`.ag-row[row-id="${id}"] .ag-selection-checkbox`);
 check('multi-product selection exposes bulk actions',await text(page,$('selection'))==='2 selected');await page.type($('bulk-value'),'10');await page.click(action('preview'));await page.waitForSelector('dialog[open]');
 check('bulk preview expands selected products to exact options',await page.$$eval('.cw-bulk-diff',ns=>ns.length===31));check('preview includes exact new price',await page.$$eval('.cw-bulk-diff ins',ns=>ns.some(n=>n.textContent==='218.90')));
 await page.click(action('apply'));check('applying updates two products without saving',await text(page,$('changes'))==='2');await page.click(action('review'));await page.click(action('save'));await page.waitForFunction(()=>!document.querySelector('dialog').open);check('batch save clears unsaved state',await text(page,$('changes'))==='0');
 const stored=await page.evaluate(async()=>{const cfg=JSON.parse(document.querySelector('[data-catalogue-config]').textContent);const m=await import(new URL(cfg.data+'store.mjs',location.origin));return m.readLocal('draft:catalogue:');});check('both products saved under the existing draft contract',stored.length===2&&stored.every(r=>r.revision===1&&r.value.schema==='service-site.product-draft.v1'));
 await page.reload({waitUntil:'networkidle2'});await waitReady(page);check('saved edits survive full reload',await text(page,cell(ids[0],'title'))==='Edited walnut selection');
 await page.click(action('options'));check('option layout exposes every original option',await text(page,$('visible'))==='3644 options shown');const optionId=await page.$eval('.ag-row[row-id]',n=>n.getAttribute('row-id'));
 await edit(page,optionId,'price','bad');check('invalid inline price is refused visibly',(await text(page,$('notice'))).includes('price of zero'));await edit(page,optionId,'price','221.23');check('valid option price edits immediately',await text(page,cell(optionId,'price'))==='221.23');
 await page.click(action('review'));check('save review compares against the last saved price',await page.$$eval('.cw-bulk-diff del',ns=>ns.some(n=>n.textContent==='218.90')));await page.click(action('save'));await page.waitForFunction(()=>!document.querySelector('dialog').open);
 await page.click(action('products'));await page.click(cell(ids[0],'option'));check('row inspector displays current product',await text(page,$('inspector')).then(t=>t.includes('Edited walnut selection')));await page.locator(action('close-inspector')).click();
 for(const width of [1440,390,320]){await page.setViewport({width,height:1000});await new Promise(r=>setTimeout(r,400));check(`workspace fits ${width}px`,await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth));await page.screenshot({path:join(out,`inventory-${width}.png`),fullPage:true});}
 await page.setViewport({width:1440,height:1000});await new Promise(r=>setTimeout(r,400));
 // Two browser tabs share the owner; one changed revision must abort every row.
 const stale=await browser.newPage();stale.setDefaultTimeout(15000);await stale.setViewport({width:1440,height:1000});await stale.goto(origin+'/catalogue/',{waitUntil:'networkidle2'});await waitReady(stale);
 await edit(stale,ids[0],'title','Stale first');await edit(stale,ids[1],'title','Stale second');
 await edit(page,ids[0],'title','Latest first');await page.click(action('review'));await page.click(action('save'));await page.waitForFunction(()=>!document.querySelector('dialog').open);
 await stale.bringToFront();await stale.click(action('review'));await stale.click(action('save'));await stale.waitForFunction(()=>document.querySelector('[data-grid="dialog-status"]').textContent.includes('another tab'));
 check('stale batch refuses the entire save',await text(stale,$('dialog-status')).then(t=>t.includes('None')));
 const after=await stale.evaluate(async()=>{const cfg=JSON.parse(document.querySelector('[data-catalogue-config]').textContent);return (await import(new URL(cfg.data+'store.mjs',location.origin))).readLocal('draft:catalogue:');});
 check('failed batch preserves other products as well as the conflicting draft',after.find(r=>r.key===stored[1].key).revision===1&&after.find(r=>r.key===stored[0].key).value.changes.some(c=>c.after==='Latest first'));
 await stale.close();await page.bringToFront();await page.select('[data-ui="store"]','aj-collectives');await waitReady(page);check('store switch clears selection and draft context',await text(page,$('changes'))==='0'&&await page.$eval($('bulk'),n=>n.hidden));
 await page.select('[data-ui="store"]','redwood-rings');await waitReady(page);await page.click('.cw-workspace-nav [data-workspace-view="drafts"]');await page.waitForSelector('.cw-desk-row');check('grid drafts continue into the existing review workflow',await page.$$eval('.cw-desk-row',ns=>ns.length===2));
 await page.click('[data-desk-product]');await page.waitForSelector('[data-ui="review-view"]:not([hidden])');check('existing native intake action remains available',await page.$eval('[data-ui="submit"]',n=>!n.disabled));
 check('no browser exceptions or library errors',report.errors.length===0);check('no remote writes during verification',report.writes.length===0);report.status='passed';
}catch(error){try{const pages=await browser.pages();await pages.at(-1).screenshot({path:join(out,'failure.png'),fullPage:true});}catch{}report.status='failed';report.error=error.stack;process.exitCode=1;}
finally{await browser?.close();server?.stop(true);rmSync(profile,{recursive:true,force:true});writeFileSync(join(out,'verification.json'),JSON.stringify(report,null,2)+'\n');console.log(JSON.stringify(report));}
