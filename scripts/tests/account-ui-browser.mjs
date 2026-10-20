/** Explicit isolated Auth-owner fixture plus a read-only public gallery mode. Not native-account acceptance. */
import {createServer} from 'node:http';import {readFile,mkdir,writeFile} from 'node:fs/promises';import path from 'node:path';import {pathToFileURL} from 'node:url';import assert from 'node:assert/strict';
const root=process.cwd(),evidence=path.resolve(process.env.ACCOUNT_UI_EVIDENCE||'account-ui-evidence');await mkdir(evidence,{recursive:true});
const {chromium}=await import(pathToFileURL(process.env.PLAYWRIGHT_MODULE||path.join(root,'.spaces-tools/node_modules/playwright/index.mjs')).href);
const checks=[],errors=[];const check=(label,truth)=>{assert(truth,label);checks.push(label);console.log('PASS '+label);};
const fixture={mode:'authenticated',name:'Local fixture reader',logout:'failure',posts:0,reads:0};
const pageHTML=`<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><link rel="stylesheet" href="/assets/account-navigation.css"><link rel="stylesheet" href="/assets/application-previews.css"><style>body{font:15px system-ui;margin:0;background:#f2efe7;color:#263a2e}header{padding:18px;background:#244338;color:white;display:flex;justify-content:space-between;gap:15px}main{max-width:1100px;margin:auto;padding:22px}.fixture-note{font-size:12px;padding:12px;background:#fff0ce}.donor-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:15px}.donor-banner{background:linear-gradient(45deg,#654b36,#ddac75);height:100px;border-radius:15px}.donor-grid>div{padding:15px;border-radius:12px;box-shadow:0 5px 12px #0002;background:#fff8e8}[hidden]{display:none!important}</style></head><body><div class="fixture-note">ISOLATED AUTH-OWNER FIXTURE. This is not a real user, account, installation or live native page.</div><header><b>AUREKAI</b><a id="registration" data-ak-anonymous href="/signup">Sign up</a><div id="account"></div></header><main><h1>Current workspace</h1><div class="donor-banner"></div><div class="donor-grid"><div>Original grid</div><div>Original shadow</div><div>Original geometry</div></div><section id="catalogue"></section></main><script src="/assets/showcase.js"></script><script type="module">
import {createNativeAccountSession,bindAccountLifecycle} from '/assets/account-session.mjs';import {mountAccountNavigation} from '/assets/account-navigation.mjs';
const session=createNativeAccountSession({origin:location.origin,maxAge:0});const lifecycle=bindAccountLifecycle(session,window);
window.testSession=session;window.testLifecycle=lifecycle;window.nav=mountAccountNavigation({root:document.querySelector('#account'),session,origin:location.origin,links:{userId:17,profile:'/s/person/fixture-profile?edition=2#work'},anonymousNodes:[document.querySelector('#registration')]});await session.refresh();
const preview={identity:'preview:sqlite',revision:'fixture-v1',image:'https://captures.example.test/schema.png',alt:'Actual SQLite application capture',summary:'Inspect and rehearse changes',kind:'Data'};
const installed=AurekaiApplicationPreviews.joinPublishedPreview({previewIdentity:preview.identity,previewRevision:preview.revision,title:'Schema migration studio',href:'https://native.example.test/apps/ws-fixture/editor?context=self',state:'installed',contextLabel:'Isolated fixture workspace',action:'Open installed editor'},preview);
window.collection=AurekaiApplicationPreviews.mountCollection(document.querySelector('#catalogue'),[installed,{title:'Audio arrangement',href:'https://native.example.test/packs/setup?pack=audio',kind:'Media',summary:'Arrange permitted recordings',state:'setup',action:'Review setup'},{title:'Acquisition workflow',href:'https://native.example.test/packs/setup?pack=itad',kind:'Operations',state:'workflow',summary:'Configure recovery and allocation',action:'Review workflow'}]);
</script></body></html>`;
const server=createServer(async(req,res)=>{
 try{
  if(req.url==='/account'){
   fixture.reads++;res.setHeader('Cache-Control','no-store');
   if(fixture.mode==='anonymous'){res.writeHead(401,{'Content-Type':'application/json'}).end('{"valid":false}');return;}
   if(fixture.mode==='unavailable'){res.writeHead(502).end('Unavailable');return;}
   res.writeHead(200,{'Content-Type':'application/json'}).end(JSON.stringify({valid:true,user:{id:17,name:fixture.name}}));return;
  }
  if(req.url==='/logout'&&req.method==='POST'){fixture.posts++;if(fixture.logout==='lost'){fixture.mode='anonymous';req.socket.destroy();return;}if(fixture.logout==='success'){fixture.mode='anonymous';res.writeHead(204).end();return;}res.writeHead(503).end('Not confirmed');return;}
  if(req.url==='/away'){res.writeHead(200,{'Content-Type':'text/html'}).end('<a href="/">Back to fixture</a>');return;}
  if(req.url==='/'){res.writeHead(200,{'Content-Type':'text/html'}).end(pageHTML);return;}
  if(req.url?.startsWith('/assets/')){const name=req.url.slice(8);if(!/^[a-z.-]+$/.test(name))throw Error('Invalid path');const body=await readFile(path.join(root,'site/assets',name));res.writeHead(200,{'Content-Type':name.endsWith('.css')?'text/css':'text/javascript'}).end(body);return;}
  res.writeHead(404).end();
 }catch{res.writeHead(404).end();}
});
await new Promise(r=>server.listen(0,'127.0.0.1',r));
let browser,page;
try{
 browser=await chromium.launch({executablePath:process.env.CHROME_PATH||undefined,headless:true,args:['--no-sandbox']});const context=await browser.newContext({viewport:{width:1440,height:1000}});page=await context.newPage();page.on('pageerror',e=>errors.push(e.message));
 if(process.env.ACCOUNT_UI_ORIGIN){
  const origin=process.env.ACCOUNT_UI_ORIGIN;await page.goto(origin,{waitUntil:'domcontentloaded'});await page.waitForFunction(()=>document.querySelector('[data-demo-list]')?.dataset.previewEnhanced==='true');
  const records=await page.evaluate(()=>AurekaiApplicationPreviews.readDirectory(document.querySelector('[data-demo-list]'),document.baseURI));
  check('public full list retains exact authored membership',await page.locator('.ak-application-card').count()===records.length);
  const targets=await page.locator('.ak-application-card').evaluateAll(nodes=>nodes.map(n=>n.href));check('public full-list destinations remain exact',records.every((r,i)=>r.href===targets[i]));
  check('featured and full lists share their preview metadata',await page.locator('#demo-preview-track .showcase-card').count()===records.length);
  await page.getByRole('searchbox',{name:'Find an application'}).fill('Schema migration');check('public search identifies the real schema application',await page.locator('.ak-application-cell:visible').count()===1);await page.getByRole('searchbox').fill('');
  await page.getByRole('button',{name:'Compact',exact:true}).click();check('public compact layout works',await page.locator('.ak-application-grid').getAttribute('data-layout')==='list');
  await page.getByRole('button',{name:'Visual',exact:true}).click();await page.locator('.ak-application-collection').screenshot({path:path.join(evidence,'public-application-list.png')});
  await page.setViewportSize({width:390,height:844});check('public full list does not widen the document',await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth));
  await page.emulateMedia({reducedMotion:'reduce'});check('public preview motion respects reduced motion',await page.locator('.ak-application-card').first().evaluate(n=>getComputedStyle(n).transitionDuration==='0s'));
  check('public gallery has no uncaught exceptions',errors.length===0);
  await writeFile(path.join(evidence,'public-results.json'),JSON.stringify({origin,checks,errors,scope:'Read-only public gallery. No authenticated native journey.'},null,2));
 }else{
  await page.route('https://captures.example.test/schema.png',async route=>{const body=await readFile(path.join(root,'site/instruments/shots/schema-desktop.png'));await route.fulfill({status:200,contentType:'image/png',body});});
  await page.goto('http://127.0.0.1:'+server.address().port+'/',{waitUntil:'networkidle'});await page.waitForFunction(()=>testSession.state.kind==='authenticated');
  check('verified identity replaces the registration prompt',await page.locator('#registration').isHidden());
  await page.locator('summary').click();check('verified profile address reaches the menu without guessing a slug',(await page.getByRole('link',{name:'My profile'}).getAttribute('href'))==='/s/person/fixture-profile?edition=2#work');
  check('installed work remains in personal navigation',await page.getByRole('link',{name:'Your work'}).getAttribute('href')==='/apps?context=self');
  await page.keyboard.press('Escape');check('Escape closes the disclosure and restores keyboard focus',await page.locator('details').getAttribute('open')===null&&await page.locator('summary').evaluate(n=>n===document.activeElement));
  await page.locator('summary').click();await page.getByRole('button',{name:'Sign out',exact:true}).click();await page.waitForFunction(()=>testSession.state.kind==='authenticated'&&!testSession.busy);check('failed logout is not claimed as revoked',fixture.posts===1&&(await page.locator('#account').innerText()).includes('not confirmed'));
  fixture.mode='unavailable';await page.evaluate(()=>testSession.refresh({force:true}));check('unavailable owner is not a sign-up prompt',await page.locator('#registration').isHidden()&&await page.getByRole('button',{name:'Check account',exact:true}).count()===1);check('unavailable owner removes private identity',!(await page.locator('#account').innerText()).includes('Local fixture reader'));
  fixture.mode='anonymous';await page.getByRole('button',{name:'Check account',exact:true}).click();await page.waitForFunction(()=>testSession.state.kind==='anonymous');check('confirmed anonymity restores ordinary sign-in',await page.getByRole('link',{name:'Sign in',exact:true}).count()===1&&await page.locator('#registration').isVisible());
  fixture.mode='authenticated';fixture.name='<img src=x onerror=alert(1)>';await page.evaluate(()=>testSession.refresh({force:true}));check('display name remains text',await page.locator('#account img').count()===0&&(await page.locator('summary').innerText()).includes('<img'));
  fixture.name='Reader after refresh';await page.evaluate(()=>testSession.refresh({force:true}));await page.goto('http://127.0.0.1:'+server.address().port+'/away');fixture.mode='anonymous';await page.goBack();await page.waitForFunction(()=>testSession.state.kind==='anonymous');check('Back navigation rechecks the owner instead of restoring identity',!(await page.locator('#account').innerText()).includes('Reader after refresh'));
  fixture.mode='authenticated';await page.evaluate(()=>testSession.refresh({force:true}));fixture.logout='lost';await page.locator('summary').click();await page.getByRole('button',{name:'Sign out',exact:true}).click();await page.waitForFunction(()=>testSession.state.kind==='anonymous');check('lost logout response is reconciled without duplicate mutation',fixture.posts===2);
  check('same renderer accepts installed and setup records',await page.locator('.ak-application-card').count()===3);
  check('preview does not redirect installed work to the demo',(await page.locator('.ak-application-card').first().getAttribute('href'))==='https://native.example.test/apps/ws-fixture/editor?context=self');
  check('workflow configuration is not presented as installed',(await page.locator('.ak-application-card').nth(2).innerText()).includes('Workflow configuration'));
  check('only supplied genuine capture is rendered',await page.locator('.ak-application-card img').count()===1&&await page.locator('.ak-application-card img').evaluate(n=>n.naturalWidth>0));
  await page.getByRole('searchbox').fill('unmatched');check('no-result state remains useful',await page.locator('.ak-application-empty').isVisible());await page.getByRole('searchbox').fill('schema');check('search filters without dropping the source records',await page.locator('.ak-application-cell:visible').count()===1&&await page.locator('.ak-application-cell').count()===3);await page.getByRole('searchbox').fill('');
  await page.getByRole('button',{name:'Compact',exact:true}).click();check('compact view retains exact editor destinations',await page.locator('.ak-application-grid').getAttribute('data-layout')==='list');await page.getByRole('button',{name:'Visual',exact:true}).click();
  check('shared component does not flatten another donor grid',await page.locator('.donor-grid').evaluate(n=>getComputedStyle(n).display==='grid'&&getComputedStyle(n).gridTemplateColumns.split(' ').length===3));
  check('shared component preserves another donor banner',await page.locator('.donor-banner').evaluate(n=>getComputedStyle(n).backgroundImage.includes('gradient')));
  await page.screenshot({path:path.join(evidence,'isolated-account-and-list-desktop.png'),fullPage:true});await page.setViewportSize({width:390,height:844});check('mobile fixture has no whole-page overflow',await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth));await page.screenshot({path:path.join(evidence,'isolated-account-and-list-mobile.png'),fullPage:true});
  await page.emulateMedia({reducedMotion:'reduce'});check('reduced-motion preview has no transition',await page.locator('.ak-application-card').first().evaluate(n=>getComputedStyle(n).transitionDuration==='0s'));
  check('fixture has no uncaught exceptions',errors.length===0);await writeFile(path.join(evidence,'isolated-results.json'),JSON.stringify({checks,errors,account:'explicit local test fixture',nativeAccountVerified:false},null,2));
 }
 console.log('ACCOUNT_UI_BROWSER_PASS '+checks.length);
}catch(error){if(page)await page.screenshot({path:path.join(evidence,'failure.png'),fullPage:true}).catch(()=>{});await writeFile(path.join(evidence,'failure.json'),JSON.stringify({error:error.stack,checks,errors},null,2));throw error;}
finally{await browser?.close();server.closeAllConnections();await new Promise(r=>server.close(r));}
