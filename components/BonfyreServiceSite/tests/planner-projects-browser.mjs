import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import {readFileSync,writeFileSync,mkdirSync,mkdtempSync,rmSync} from 'node:fs';
import {resolve,join,sep} from 'node:path';
import {tmpdir} from 'node:os';
const live=process.argv.includes('--live'),root=resolve(process.env.PLANNER_BUILD??'packs/aurekai-work-samples/site/dist');
const out=resolve(process.env.PLANNER_PROJECT_PROOF??`packs/no-simulation/product-maturity-20261015/planner/${live?'live-desk':'local-desk'}`);mkdirSync(out,{recursive:true});
const profile=mkdtempSync(join(tmpdir(),'planner-projects-'));
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
 const tab=async view=>{await page.click(`[data-planner-view="${view}"]`);await page.waitForSelector($(''+view)+':not([hidden])');};
 const input=async(name,value)=>{await page.click($(name),{clickCount:3});await page.keyboard.press('Backspace');await page.type($(name),value);};
 const save=async()=>{await page.click($('save'));await page.waitForFunction(()=>document.querySelector('[data-ui="save-state"]').textContent==='Saved in this browser'&&!document.querySelector('[data-ui="save"]').disabled);};
 await page.setViewport({width:1440,height:960});await page.goto(origin+'/red3-planner/',{waitUntil:'networkidle2'});await page.waitForSelector('[data-planner][data-ready=true]');
 check('layout opens separately from the fixture catalogue',await page.$eval($('fixtures'),n=>n.hidden));
 await input('title','Downtown pop-up');await input('budget','1200');await save();const first=new URL(page.url()).searchParams.get('project');
 await page.click($('duplicate-project'));await page.waitForFunction(()=>document.querySelector('[data-ui="title"]').value.endsWith('(copy)'));
 const second=new URL(page.url()).searchParams.get('project');check('another option has its own project identity',second!==first&&await page.$eval($('save-state'),n=>n.textContent)==='Changes not saved');
 await input('title','Downtown with extra rack');await tab('fixtures');await page.click('[data-ui=fixtures] [data-add="rack"]');await tab('layout');await save();
 await tab('projects');check('both arrangements are saved as independent projects',await page.$$('[data-ui=project-list] .fp-project-card').then(v=>v.length===2));
 await page.click(`[data-project-compare="${first}"]`);await page.waitForSelector($('compare')+':not([hidden])');
 check('comparison computes the extra rack cost',await page.$eval('.fp-comparison-delta',n=>n.textContent.includes('$83.00')));
 check('comparison carries the revised budget problem',await page.$eval($('comparison'),n=>n.textContent.includes('$51.50 over fixture budget')));
 check('comparison keeps source listings and unresolved supplier questions',await page.$$eval('[data-ui="comparison"] a',v=>v.length>0)&&await page.$eval($('comparison'),n=>n.textContent.includes('supplier questions')));
 const downloads=join(out,'downloads');mkdirSync(downloads,{recursive:true});const cdp=await page.createCDPSession();await cdp.send('Page.setDownloadBehavior',{behavior:'allow',downloadPath:downloads});await page.click($('download-comparison'));
 await page.waitForFunction(()=>true);let exported='';for(let i=0;i<30;i++){try{exported=readFileSync(join(downloads,'fixture-plan-comparison.txt'),'utf8');break;}catch{await new Promise(r=>setTimeout(r,100));}}
 check('download includes both exact plans and sources',exported.includes('Downtown pop-up')&&exported.includes('Downtown with extra rack')&&exported.includes('PLAN A')&&exported.includes('Source listings'));
 await page.reload({waitUntil:'networkidle2'});await page.waitForSelector('[data-planner][data-ready=true]');
 check('comparison and selected plans survive reload',await page.$eval($('compare-left'),n=>n.value)===first&&await page.$eval($('compare-right'),n=>n.value)===second&&await page.$eval($('compare'),n=>!n.hidden));
 for(const width of [1440,390,320]){await page.setViewport({width,height:960});check(`comparison fits ${width}px`,await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth));if(width!==320)await page.screenshot({path:join(out,`comparison-${width}.png`),fullPage:true});}
 await tab('projects');await page.type('[name=query]','extra rack');await page.click('[data-ui=project-search] button');check('project search narrows saved alternatives',await page.$$('[data-ui=project-list] .fp-project-card').then(v=>v.length===1));
 await page.click($('new-project'));await page.waitForSelector($('layout')+':not([hidden])');check('a new project keeps previous plans and clears prior receipt',await page.$eval($('title'),n=>!n.value.includes('Downtown'))&&await page.$eval($('receipt'),n=>!n.textContent.includes('Reference:')));
 await input('title','Uptown second location');await input('width','24');await save();const third=new URL(page.url()).searchParams.get('project');
 await tab('projects');await page.click('[name=query]',{clickCount:3});await page.keyboard.press('Backspace');await page.click('[data-ui=project-search] button');check('multiple spaces remain available',await page.$$('[data-ui=project-list] .fp-project-card').then(v=>v.length===3));
 await page.click(`[data-project-open="${first}"]`);await page.waitForFunction(()=>document.querySelector('[data-ui="title"]').value==='Downtown pop-up');check('opening an earlier plan restores its original arrangement',await page.$eval($('subtotal'),n=>n.textContent)==='$1,168.50');
 await input('notes','Unsaved detail');await tab('projects');page.once('dialog',dialog=>dialog.dismiss());await page.click(`[data-project-open="${third}"]`);check('canceling a project switch preserves unsaved work',await page.$eval($('notes'),n=>n.value==='Unsaved detail')&&new URL(page.url()).searchParams.get('project')===first);
 page.once('dialog',dialog=>dialog.accept());await page.click(`[data-project-open="${third}"]`);await page.waitForFunction(()=>document.querySelector('[data-ui="title"]').value==='Uptown second location');
 await page.goBack();await page.waitForSelector($('projects')+':not([hidden])');check('Back restores the preceding project and view',new URL(page.url()).searchParams.get('project')===first);
 // An obsolete saved record must remain stored without entering current comparisons.
 await page.evaluate(async()=>{const root=document.querySelector('[data-planner]'),cfg=JSON.parse(root.querySelector('[data-planner-config]').textContent);const base=new URL(document.querySelector('script[src$="planner-client.mjs"]').src);const {readLocal,saveLocal}=await import(new URL('store.mjs',base));const prefix='fixture-plan:'+cfg.form+':';const row=(await readLocal(prefix)).find(r=>r.value?.plan);const plan=structuredClone(row.value.plan);plan.id='obsolete-project';plan.catalogue='old-source';await saveLocal(prefix+plan.id,{plan},0);});
 await tab('layout');await tab('projects');check('obsolete source plans are identified and excluded without deletion',await page.$eval($('unavailable-projects'),n=>n.textContent.includes('1 saved plan'))&&await page.$$('[data-ui=project-list] .fp-project-card').then(v=>v.length===3));
 await page.reload({waitUntil:'networkidle2'});await page.waitForSelector('[data-planner][data-ready=true]');check('the project library survives a fresh load',await page.$$('[data-ui=project-list] .fp-project-card').then(v=>v.length===3));
 check('no browser exceptions',report.errors.length===0);check('no remote writes or submissions',report.writes.length===0);report.status='passed';
}catch(error){report.status='failed';report.error=error.stack;process.exitCode=1;}
finally{await browser?.close();server?.stop(true);rmSync(profile,{recursive:true,force:true});writeFileSync(join(out,'verification.json'),JSON.stringify(report,null,2)+'\n');console.log(JSON.stringify(report));}
