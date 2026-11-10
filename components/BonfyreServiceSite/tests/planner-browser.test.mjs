// Fresh-browser verification of the generated site and its real intake worker.
import {createRequire} from 'node:module';
import {createServer} from 'node:http';
import {readFileSync,existsSync,statSync,mkdtempSync,mkdirSync,rmSync,writeFileSync} from 'node:fs';
import {resolve,join,extname,sep} from 'node:path';
import {tmpdir} from 'node:os';
import assert from 'node:assert/strict';
const repo=resolve(import.meta.dir,'../../..');
const build=process.env.PLANNER_BUILD||'/tmp/aurekai-fixture-planner-build';
const live=process.env.PLANNER_TEST_ORIGIN;
if(live&&live!=='https://aurekai-work-samples.pages.dev')throw Error('Unexpected live verification origin.');
const out=process.env.PLANNER_PROOF||join(repo,'packs/aurekai-work-samples/site/design/verification-planner-'+(live?'live':'local'));
mkdirSync(out,{recursive:true});
const puppeteer=createRequire(join(repo,'tools/creator-ingest/package.json'))('puppeteer');
const worker=(await import(join(build,'_worker.js'))).default,held=new Map(),errors=[],checks=[],posts=[];
const env={LEADS:{get:async k=>held.get(k)??null,put:async(k,v)=>held.set(k,v)}};
let failNext=false;
const server=createServer(async(req,res)=>{try{
  if(req.url.startsWith('/api/lead/')){posts.push(req.url);if(failNext){failNext=false;res.writeHead(503,{'content-type':'application/json'}).end(JSON.stringify({ok:false,why:'Test intake outage'}));return;}const parts=[];for await(const p of req)parts.push(p);const result=await worker.fetch(new Request('http://'+req.headers.host+req.url,{method:req.method,headers:req.headers,body:Buffer.concat(parts)}),env);res.writeHead(result.status,Object.fromEntries(result.headers));res.end(Buffer.from(await result.arrayBuffer()));return;}
  let path=resolve(build,'.'+decodeURIComponent(new URL(req.url,'http://local.test').pathname));if(path!==build&&!path.startsWith(build+sep)){res.writeHead(403).end();return;}if(existsSync(path)&&statSync(path).isDirectory())path+='/index.html';if(!existsSync(path)){res.writeHead(404).end();return;}
  res.setHeader('content-type',({'.html':'text/html;charset=utf-8','.mjs':'text/javascript','.js':'text/javascript','.css':'text/css','.gz':'application/gzip'})[extname(path)]||'application/octet-stream');res.end(readFileSync(path));
}catch(e){res.writeHead(500).end(e.message);}});
await new Promise(r=>server.listen(0,'127.0.0.1',r));
const origin=live||'http://127.0.0.1:'+server.address().port,url=origin+'/red3-planner/';
const dir=mkdtempSync(join(tmpdir(),'planner-browser-'));
const check=(name,value)=>{assert.ok(value,name);checks.push(name);console.log('PASS '+name);};
const $=name=>`[data-ui="${name}"]`;
let browser;
const launch=()=>puppeteer.launch({headless:true,executablePath:'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',userDataDir:dir});
const open=async(browser,address=url,width=1440)=>{const p=await browser.newPage();p.setDefaultTimeout(10000);p.on('pageerror',e=>errors.push(e.message));if(live)p.on('request',req=>{if(req.method()==='POST')posts.push(req.url());});await p.setViewport({width,height:width<650?844:1100});await p.goto(address,{waitUntil:'networkidle2'});await p.waitForSelector('[data-planner][data-ready=true]');return p;};
const input=async(p,name,value)=>{await p.bringToFront();await p.click($(name),{clickCount:3});await p.keyboard.press('Backspace');await p.type($(name),String(value));};
const save=async p=>{await p.bringToFront();await p.click($('save'));await p.waitForFunction(()=>document.querySelector('[data-ui=save-state]').textContent==='Saved in this browser');};
try {
  browser=await launch();let p=await open(browser);
  check('A prepared four-fixture layout opens',await p.$$eval('[data-select]',x=>x.length)===4);
  check('Exact selected options produce the expected subtotal',await p.$eval($('subtotal'),n=>n.textContent)==='$1,168.50');
  await p.click('[data-planner-view=fixtures]');await p.waitForSelector('[data-ui=fixtures]:not([hidden])');
  check('Supplier missing dimensions are visible',await p.$eval('[data-ui=products]',n=>n.innerText.includes('depth to confirm')));
  await p.click('[data-planner-view=layout]');await p.waitForSelector('[data-ui=layout]:not([hidden])');
  await p.screenshot({path:join(out,'desktop.png'),fullPage:true});
  for(const width of [1440,1024,768,390,320]){await p.setViewport({width,height:width<650?844:1100});check('No horizontal overflow at '+width,await p.evaluate(()=>document.documentElement.scrollWidth<=innerWidth));if(width===390)await p.screenshot({path:join(out,'mobile.png'),fullPage:true});}
  await p.setViewport({width:1440,height:1100});
  await p.click('[data-ui=source-review]');
  await p.waitForSelector('dialog[data-source-review],dialog[aria-label="Review retained fixture sources"]');
  check('Source review shows retained snapshot and not claimed supplier page text',await p.$eval('[data-source-reader]',n=>n.value.includes('Listing title:')&&n.value.includes('Source URL:')));
  await p.$eval('[data-source-reader]',n=>{n.focus();n.setSelectionRange(0,35);});
  await p.click('[data-source-stage]');
  check('Exact source passage staged with provenance',await p.$eval('[data-source-preview]',n=>n.textContent.includes('Source digest:')));
  await p.click('[data-source-adopt]');
  check('User-confirmed source selection enters editable plan notes',await p.$eval('[data-ui=notes]',n=>n.value.includes('[Retained catalogue:')));
  await input(p,'width','0');check('Invalid room dimensions block saving',await p.$eval($('error'),n=>!n.hidden&&n.innerText.includes('6 to 60')));await p.click($('save'));check('Invalid room was not saved',await p.$eval($('save-state'),n=>n.textContent)!=='Saved in this browser');await input(p,'width','16');
  await input(p,'budget','500');check('Budget overage is visible',await p.$eval($('issues'),n=>n.innerText.includes('exceeds')));await input(p,'budget','1500');
  await p.click('[data-select="counter-1"]');await input(p,'x','15');check('Out-of-room position is identified',await p.$eval($('issues'),n=>n.innerText.includes('beyond')));await input(p,'x','10');
  await p.click('[data-planner-view=fixtures]');await p.waitForSelector('[data-ui=fixtures]:not([hidden])');await p.click('[data-ui=fixtures] [data-add="rack"]');await p.click('[data-planner-view=layout]');await p.waitForSelector('[data-ui=layout]:not([hidden])');check('Adding a fixture updates both layout and subtotal',await p.$$eval('[data-select]',x=>x.length)===5&&await p.$eval($('subtotal'),n=>n.textContent)==='$1,251.50');
  await p.click($('remove'));check('Removing a fixture updates the proposal',await p.$$eval('[data-select]',x=>x.length)===4);
  await input(p,'title','My autumn pop-up');await save(p);await p.reload({waitUntil:'networkidle2'});await p.waitForSelector('[data-ready=true]');check('Saved project survives reload',await p.$eval($('title'),n=>n.value)==='My autumn pop-up');
  console.log('STEP open concurrent tab');const other=await open(browser);console.log('STEP save first tab');await input(p,'notes','First tab update');await save(p);console.log('STEP save stale tab');await other.bringToFront();await input(other,'notes','Stale update');await other.click($('save'));await other.waitForFunction(()=>document.querySelector('[data-ui=error]').textContent.includes('another tab'));check('Stale tab cannot overwrite the project',await other.$eval($('save'),n=>n.disabled));await other.close();await p.bringToFront();
  await p.click($('share'));const shared=await p.$eval($('share-url'),n=>n.value);check('Share link contains a plan without contact details',shared.includes('#plan=')&&!shared.includes('example.invalid'));
  const context=await browser.createBrowserContext(),sharedPage=await context.newPage();await sharedPage.goto(shared,{waitUntil:'networkidle2'});await sharedPage.waitForSelector('[data-ready=true]');check('A fresh visitor opens the shared layout and notes',await sharedPage.$eval($('title'),n=>n.value)==='My autumn pop-up'&&await sharedPage.$eval($('notes'),n=>n.value)==='First tab update');check('Shared copy is not falsely marked saved',await sharedPage.$eval($('save-state'),n=>n.textContent)==='Changes not saved');await context.close();
  await p.click($('review'));check('Brief carries exact source and missing dimensions',await p.$eval($('brief'),n=>n.innerText.includes('option 10749')&&n.innerText.includes('depth is missing')));
  const downloads=join(out,'downloads');mkdirSync(downloads,{recursive:true});
  const cdp=await p.createCDPSession();await cdp.send('Page.setDownloadBehavior',{behavior:'allow',downloadPath:downloads});
  await p.click($('download'));
  const downloaded=join(downloads,'fixture-project-brief.txt');
  for(let i=0;i<40&&!existsSync(downloaded);i++)await new Promise(r=>setTimeout(r,50));
  check('Downloaded brief matches the reviewed plan',existsSync(downloaded)&&readFileSync(downloaded,'utf8')===await p.$eval($('brief'),n=>n.textContent));
  await p.emulateMediaType('print');
  check('Print shows the complete brief and hides interactive controls',await p.evaluate(()=>{
    const brief=document.querySelector('[data-ui=brief]'),form=document.querySelector('[data-ui=submit-form]');
    return brief.getBoundingClientRect().height>0&&brief.scrollHeight<=brief.clientHeight+1&&getComputedStyle(form).display==='none'&&getComputedStyle(document.querySelector('main')).display!=='none';
  }));
  await p.screenshot({path:join(out,'print.png'),fullPage:true});
  await p.emulateMediaType('screen');
  let receipt=null;
  if(!live){
  await p.type($('name'),'Aurekai verification');await p.type($('email'),'verification@example.invalid');failNext=true;await p.click($('submit'));await p.waitForFunction(()=>document.querySelector('[data-ui=receipt]').textContent.includes('Test intake outage'));check('Failed send is retryable without a receipt',!await p.$eval($('submit'),n=>n.disabled));
  await p.click($('submit'));await p.waitForFunction(()=>document.querySelector('[data-ui=receipt]').textContent.includes('Received by Aurekai'));receipt=await p.$eval($('receipt'),n=>n.textContent);check('Generated worker retains the real saved plan',held.size>0&&[...held.keys()].filter(k=>k.startsWith('lead:')).length===1);
  }
  await p.screenshot({path:join(out,'review-desktop.png'),fullPage:true});await p.setViewport({width:390,height:844});await p.screenshot({path:join(out,'review-mobile.png'),fullPage:true});check('Review fits a phone',await p.evaluate(()=>document.documentElement.scrollWidth<=innerWidth));
  await browser.close();browser=await launch();p=await open(browser);await p.click($('review'));
  if(live){check('Published plan survives browser restart',await p.$eval($('title'),n=>n.value)==='My autumn pop-up');check('Live verification sends no intake requests',posts.length===0);}
  else {check('Browser restart restores the receipt without resending',await p.$eval($('receipt'),n=>n.textContent)===receipt&&posts.length===2);await p.click($('close-review'));await input(p,'notes','Changed after receipt');await save(p);await p.click($('review'));check('A revised plan cannot inherit the previous receipt',!await p.$eval($('submit'),n=>n.disabled)&&!(await p.$eval($('receipt'),n=>n.textContent)).includes('Reference:'));}
  check('No browser exceptions',errors.length===0);
  writeFileSync(join(out,'browser-verification.json'),JSON.stringify({at:new Date().toISOString(),origin,checks,errors,posts,receipt,external_writes:0},null,2)+'\n');
}catch(e){console.error('BROWSER_TEST_FAILURE '+e.stack);throw e;}finally{if(browser){const closed=await Promise.race([browser.close().then(()=>true),new Promise(r=>setTimeout(()=>r(false),5000))]);if(!closed)browser.process()?.kill('SIGKILL');}server.closeAllConnections();await new Promise(r=>server.close(r));rmSync(dir,{recursive:true,force:true});}
