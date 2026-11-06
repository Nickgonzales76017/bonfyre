import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import {readFileSync,writeFileSync,mkdirSync,mkdtempSync,rmSync} from 'node:fs';
import {resolve,join,sep} from 'node:path';
import {tmpdir} from 'node:os';
const live=process.argv.includes('--live'),root=resolve(process.env.CATALOGUE_BUILD??'packs/aurekai-work-samples/site/dist');
const out=resolve(process.env.CATALOGUE_DESK_PROOF??`packs/no-simulation/product-maturity-20261015/catalogue/${live?'live-desk':'local-desk'}`);mkdirSync(out,{recursive:true});
const profile=mkdtempSync(join(tmpdir(),'catalogue-desk-'));
const puppeteer=createRequire(resolve('tools/creator-ingest/package.json'))('puppeteer');
let browser;
const server=live?null:Bun.serve({hostname:'127.0.0.1',port:0,async fetch(request){
 if(!['GET','HEAD'].includes(request.method))return new Response('No writes',{status:405});
 const url=new URL(request.url);let file=resolve(root,'.'+decodeURIComponent(url.pathname));
 if(file!==root&&!file.startsWith(root+sep))return new Response('Not found',{status:404});
 if(url.pathname.endsWith('/'))file=join(file,'index.html');
 const content=Bun.file(file);return await content.exists()?new Response(content,{headers:{'Cache-Control':'no-store'}}):new Response('Not found',{status:404});
}});
const origin=live?'https://aurekai-work-samples.pages.dev':`http://127.0.0.1:${server.port}`;
const report={origin,checks:[],errors:[],writes:[],status:'running'},check=(name,value)=>{assert(value,name);report.checks.push(name);console.log('PASS '+name);};
const $=key=>`[data-ui="${key}"]`;
try{
 browser=await puppeteer.launch({headless:true,executablePath:'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',userDataDir:profile});
 const page=await browser.newPage();page.setDefaultTimeout(12000);page.on('pageerror',e=>report.errors.push(e.message));page.on('request',r=>{if(!['GET','HEAD'].includes(r.method()))report.writes.push({url:r.url(),method:r.method()});});
 await page.setViewport({width:1440,height:960});await page.goto(origin+'/catalogue/?workspace=products',{waitUntil:'networkidle2'});await page.waitForSelector($('editor')+':not([hidden])');
 check('details open without the long options and preparation forms',await page.$eval($('options-view'),n=>n.hidden)&&await page.$eval($('preparation-view'),n=>n.hidden));
 await page.click($('options-tab'));check('options have a separate task view and URL',await page.$eval($('options-view'),n=>!n.hidden)&&new URL(page.url()).searchParams.get('panel')==='options');
 const price=await page.$('[data-field^="price:"]');await price.click({clickCount:3});await page.keyboard.type('188.75');await page.click($('save'));await page.waitForFunction(()=>document.querySelector('[data-ui="draft-state"]').textContent==='Draft saved');
 await page.click($('preparation-tab'));check('search preparation opens independently',await page.$eval($('preparation-view'),n=>!n.hidden)&&await page.$eval($('options-view'),n=>n.hidden));
 await page.type($('prep:seo_title'),'A catalogue draft ready for review');await page.goBack();await page.waitForSelector($('options-view')+':not([hidden])');check('Back between task panels retains unsaved edits',await page.$eval($('prep:seo_title'),n=>n.value==='A catalogue draft ready for review'));await page.click($('preparation-tab'));await page.click($('save'));await page.waitForFunction(()=>document.querySelector('[data-ui="draft-state"]').textContent==='Draft saved');
 await page.click('.cw-product:nth-child(2)');await page.waitForFunction(()=>document.querySelector('[data-ui="draft-state"]').textContent==='No changes yet');await page.type($('title'),' — second draft');await page.click($('save'));await page.waitForFunction(()=>document.querySelector('[data-ui="draft-state"]').textContent==='Draft saved');
 await page.click('.cw-workspace-nav [data-workspace-view="drafts"]');await page.waitForSelector('.cw-desk-row');
 check('draft desk shows both saved products',await page.$$eval('.cw-desk-row',rows=>rows.length===2));
 check('desk has its own reloadable route',new URL(page.url()).searchParams.get('workspace')==='drafts');await page.reload({waitUntil:'networkidle2'});await page.waitForSelector('.cw-desk-row');check('draft desk survives reload',await page.$$eval('.cw-desk-row',rows=>rows.length===2));
 for(const width of [1440,390,320]){await page.setViewport({width,height:960});check(`draft desk fits ${width}px`,await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth));if(width!==320)await page.screenshot({path:join(out,`draft-desk-${width}.png`),fullPage:true});}
 await page.type('[data-desk-search] input','second draft');await page.click('[data-desk-search] button');check('search narrows the saved work',await page.$$eval('.cw-desk-row',rows=>rows.length===1));
 await page.click('[data-desk-product]');await page.waitForSelector($('review-view')+':not([hidden])');check('opening a saved row goes directly to its change review',await page.$eval($('changes'),n=>n.textContent.includes('second draft')));
 await page.goBack();await page.waitForSelector('.cw-desk-row');check('Back returns from review to the draft desk',new URL(page.url()).searchParams.get('workspace')==='drafts');
 await page.select($('store'),'aj-collectives');await page.waitForFunction(()=>document.querySelector('[data-ui="desk"] h2')?.textContent==='No saved drafts');check('another store does not inherit these drafts',await page.$$eval('.cw-desk-row',rows=>rows.length===0));
 await page.select($('store'),'redwood-rings');await page.waitForSelector('.cw-desk-row');await page.click('[data-desk-product]');await page.waitForSelector($('review-view')+':not([hidden])');await page.click($('edit-tab'));await page.type($('title'),' unsaved');
 page.once('dialog',dialog=>dialog.dismiss());await page.click('.cw-workspace-nav [data-workspace-view="drafts"]');check('canceling navigation preserves unsaved work',await page.$eval($('title'),n=>n.value.endsWith('unsaved'))&&await page.$eval($('desk'),n=>n.hidden));
 page.once('dialog',dialog=>dialog.accept());await page.click('.cw-workspace-nav [data-workspace-view="sent"]');await page.waitForSelector('[data-ui="desk"]:not([hidden])');check('unsent drafts never appear as received reviews',await page.$eval($('desk'),n=>n.textContent.includes('No reviews sent')));
 check('no browser exceptions',report.errors.length===0);check('no remote writes or submissions',report.writes.length===0);report.status='passed';
}catch(error){report.status='failed';report.error=error.stack;process.exitCode=1;}
finally{await browser?.close();server?.stop(true);rmSync(profile,{recursive:true,force:true});writeFileSync(join(out,'verification.json'),JSON.stringify(report,null,2)+'\n');console.log(JSON.stringify(report));}
