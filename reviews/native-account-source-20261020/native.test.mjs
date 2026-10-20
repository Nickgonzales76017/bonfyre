import {test,beforeAll,afterAll,expect} from 'bun:test';
import {readFileSync,mkdirSync,writeFileSync} from 'node:fs';
import {join} from 'node:path';
import {pathToFileURL} from 'node:url';
import {createRequire} from 'node:module';
import {buildCandidate,directory,once,digest} from './build-candidate.mjs';
const root=process.env.BONFYRE_PROJECT_ROOT||'/Users/nickgonzales/Developer/Bonfyre';
const {packSetupFixture}=await import(pathToFileURL(join(root,'cmd/BonfyreWebAddress/tests/pack_setup_fixture.mjs')));
let f,browser,candidate;const results=[],screenshots=[];
const record=(name,detail)=>results.push({name,detail});
const pause=ms=>new Promise(resolve=>setTimeout(resolve,ms));
const client=readFileSync(join(directory,'account-client.js'),'utf8');
function macro(text){return [...text.split('#define BF_ACCOUNT_REFRESH')[1].matchAll(/^"(?:[^"\\]|\\.)*"/gm)].map(match=>JSON.parse(match[0])).join('');}
beforeAll(async()=>{
 candidate=buildCandidate(root);f=await packSetupFixture();
 f.run(f.web,['domain','--db',join(f.home,'matrix.db'),'--host','127.0.0.1','--territory','work','--scope','public','--world','CANDIDATE','--repr','HTML','--evidence','fixture:account-source-browser']);
 const puppeteer=createRequire(join(root,'tools/creator-ingest/package.json'))('puppeteer');
 browser=await puppeteer.launch({executablePath:'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',headless:true,args:['--no-sandbox','--disable-dev-shm-usage']});
 mkdirSync(join(candidate.output,'screenshots'),{recursive:true});
},30000);
afterAll(async()=>{
 await browser?.close();await f?.close();
 if(candidate){writeFileSync(join(candidate.output,'verification.txt'),JSON.stringify({scope:'candidate frontend, real isolated native Auth/CMS/WebAddress; source-list failure/race inputs explicitly injected; no public account or deployment',native_executable_sha256:f?.fingerprint,results,screenshots},null,2));console.log('REVIEW_OUTPUT '+candidate.output);}
});
function patchedAccount(body){
 const original=candidate.originals.get('site/matrix/surfaces/account-native.html');
 const legacy=original.match(/const out=document\.querySelector\('#out'\)[\s\S]*?(?=<\/script><script>document\.querySelector\('#institution-open'\))/)[0];
 body=once(body,legacy,client);
 body=once(body,'<script defer src="/assets/matrix-htmx.js"></script>','<script src="/assets/bonfyre-kit.js"></script><script defer src="/assets/matrix-htmx.js"></script>');
 body=once(body,'</head>','<style>#signed[hidden],#out[hidden],.account-tools-host[hidden]{display:none!important}</style></head>');
 const before=macro(candidate.originals.get('cmd/BonfyreWebAddress/src/account_navigation_markup.h'));
 const after=macro(candidate.changes.get('cmd/BonfyreWebAddress/src/account_navigation_markup.h'));
 return once(body,before,after,'compiled current navigation script');
}
async function pageFor({baseline=false,width=1440}={}){
 const actor=await f.login('candidate-'+crypto.randomUUID()+'@example.test');
 f.authsql.query('UPDATE users SET name=? WHERE id=?').run('Avery + Research & Design',actor.id);
 const context=await browser.createBrowserContext(),page=await context.newPage();page.setDefaultTimeout(10000);
 const [name,...value]=actor.cookie.split('=');await page.setCookie({name,value:value.join('='),url:f.base,httpOnly:true,sameSite:'Lax'});
 await page.setViewport({width,height:950});
 const mode={linksFail:false,identityFail:false,logoutFail:false,profileFail:false,sources:null,posts:[],receipts:0,errors:[],requests:[]};
 page.on('pageerror',error=>mode.errors.push(error.message));
 await page.setRequestInterception(true);
 page.on('request',async request=>{
  try{
   const url=new URL(request.url()),path=url.pathname,method=request.method();
   if(!request.url().startsWith(f.base)&&!request.url().startsWith('data:'))return request.abort();
   mode.requests.push(method+' '+path);
   if(method==='POST')mode.posts.push({path,body:request.postData(),type:request.headers()['content-type']});
   if(path==='/foreign/receipts')mode.receipts++;
   if(path==='/account'&&request.resourceType()==='document'&&!baseline){const response=await fetch(request.url(),{headers:request.headers()});return request.respond({status:response.status,contentType:'text/html',body:patchedAccount(await response.text())});}
   if(path==='/assets/bonfyre-kit.js'&&!baseline)return request.respond({status:200,contentType:'application/javascript',body:candidate.changes.get('site/matrix/bonfyre-kit.js')});
   if(path==='/assets/aurekai-bar.css'&&!baseline)return request.respond({status:200,contentType:'text/css',body:candidate.changes.get('site/matrix/aurekai-bar.css')});
   if(path==='/account'&&method==='GET'&&mode.identityFail)return request.respond({status:503,contentType:'application/json',body:'{"error":"explicit test outage"}'});
   if(path==='/account/connections'&&method==='GET'&&mode.linksFail)return request.respond({status:503,contentType:'application/json',body:'{"error":"explicit test outage"}'});
   if(path==='/studio/v1/profile'&&mode.profileFail)return request.respond({status:503,contentType:'text/html',body:'Explicit profile test outage'});
   if(path==='/logout'&&mode.logoutFail)return request.respond({status:503,contentType:'text/plain',body:'Explicit logout test failure'});
   if(path==='/foreign/workbench'&&request.resourceType()==='document')return request.respond({status:200,contentType:'text/html',body:candidate.changes.get('site/matrix/surfaces/link.html')});
   if(path.startsWith('/foreign/twins')&&mode.sources){const response=await mode.sources(url);return request.respond({status:response.status??200,contentType:'application/json',body:JSON.stringify(response.body)});}
   if(path==='/foreign/edges')return request.respond({status:200,contentType:'application/json',body:'{"edges":[]}'});
   if(path==='/foreign/receipts')return request.respond({status:200,contentType:'application/json',body:'{"receipts":[]}'});
   return request.continue();
  }catch(error){mode.errors.push('interception: '+error.message);if(!request.isInterceptResolutionHandled())await request.abort();}
 });
 return {actor,context,page,mode,close:async()=>{console.log('FIXTURE_DIAG '+JSON.stringify({errors:mode.errors,state:await page.evaluate(()=>({account:document.documentElement.dataset.accountState,out:document.querySelector('#out')?.textContent,source:document.querySelector('#list')?.textContent,search:document.querySelector('#search')?.value})).catch(()=>null),requests:mode.requests.slice(-12)}));return context.close()}};
}
async function signed(p){await p.page.goto(f.base+'/account',{waitUntil:'networkidle0'});await p.page.waitForFunction(()=>document.documentElement.dataset.accountState==='authenticated');}
async function shot(p,name){const path=join(candidate.output,'screenshots',name+'.png');await p.page.screenshot({path,fullPage:true});screenshots.push({name,path});}

test('reproduces the released legacy false-sign-out on linked-account failure',async()=>{
 const p=await pageFor({baseline:true});try{p.mode.linksFail=true;await p.page.goto(f.base+'/account',{waitUntil:'networkidle0'});await p.page.waitForFunction(()=>document.querySelector('#out').textContent.includes('could not be reached'));expect(await p.page.$eval('#out',node=>!!node.querySelector('a[href="/signup"]'))).toBe(true);record('baseline false sign-out','Real isolated identity succeeds; only linked-account response is replaced by 503.');}finally{await p.close();}
},30000);
test('linked-account and profile failures stay separate from verified account and navigation',async()=>{
 const p=await pageFor();try{p.mode.linksFail=true;p.mode.profileFail=true;await signed(p);expect(await p.page.$eval('#out',node=>node.hidden&&node.childNodes.length===0)).toBe(true);expect(await p.page.$eval('#signed',node=>getComputedStyle(node).display)).toBe('grid');expect(await p.page.$eval('#identity',node=>node.textContent)).toContain('Avery + Research & Design');expect(await p.page.$eval('#connections',node=>node.textContent)).toContain('could not be loaded');expect(await p.page.$eval('#presence',node=>node.textContent)).toContain('could not be loaded');expect(await p.page.$eval('.ak-session',node=>node.dataset.sessionState)).toBe('signed-in');p.mode.linksFail=false;await p.page.click('[data-links-retry]');await p.page.waitForFunction(()=>document.querySelector('#connections').textContent.includes('No outside'));expect(p.mode.posts).toHaveLength(0);await shot(p,'account-independent-failures');record('independent failures','Identity, profile and connection sections have separate outcomes; a connection retry does not mutate anything.');}finally{await p.close();}
},30000);
test('real native connection write and revoke use URL-encoded fields without losing plus and ampersand',async()=>{
 const p=await pageFor();try{await signed(p);await p.page.type('#connect [name=provider]','github');await p.page.type('#connect [name=remote_account]','candidate-handle');await p.page.type('#connect [name=profile_url]','https://example.test/profile?a=1&b=2');await p.page.type('#connect [name=display_name]','Research + Design & Studio');await p.page.click('#connect button[type=submit]');await p.page.waitForSelector('#connections .external-item');const post=p.mode.posts.find(x=>x.path==='/account/connections');expect(post.type).toContain('application/x-www-form-urlencoded');const fields=new URLSearchParams(post.body);expect(fields.get('display_name')).toBe('Research + Design & Studio');expect(fields.get('profile_url')).toBe('https://example.test/profile?a=1&b=2');const owner=await f.request('/account/connections',{cookie:p.actor.cookie,headers:{Accept:'application/json'}});expect(owner.body).toContain('Research + Design & Studio');await p.page.click('#connections .remove');await p.page.waitForFunction(()=>!document.querySelector('#connections .external-item'));expect(p.mode.posts.filter(x=>x.path==='/account/connections/revoke')).toHaveLength(1);record('native add and revoke','Two actual operations in disposable Auth; POST encoding and owner readback verified.');}finally{await p.close();}
},30000);
test('failed logout is reconciled; successful logout clears private sections and updates native navigation',async()=>{
 const p=await pageFor();try{await signed(p);p.mode.logoutFail=true;await p.page.click('#logout button');await p.page.waitForFunction(()=>document.querySelector('#logout').textContent.includes('still active'));expect(await p.page.$eval('#signed',node=>node.hidden)).toBe(false);p.mode.logoutFail=false;await p.page.click('#logout button');await p.page.waitForFunction(()=>document.documentElement.dataset.accountState==='anonymous');await p.page.waitForFunction(()=>document.querySelector('.ak-session')?.dataset.sessionState==='anonymous');expect(await p.page.$eval('#signed',node=>getComputedStyle(node).display)).toBe('none');expect(await p.page.$eval('#identity',node=>node.childNodes.length)).toBe(0);expect(await p.page.$eval('#presence',node=>node.childNodes.length)).toBe(0);expect(await p.page.$eval('#connections',node=>node.childNodes.length)).toBe(0);expect(p.mode.posts.filter(x=>x.path==='/logout')).toHaveLength(2);record('logout reconciliation','A 503 does not claim success; subsequent native logout clears private DOM and refreshes shared navigation.');}finally{await p.close();}
},30000);
test('history suspension restores a same-account unsaved form but never exposes it to another account',async()=>{
 const p=await pageFor();try{await signed(p);await p.page.type('#connect [name=display_name]','Unsaved source identity');await p.page.evaluate(()=>window.dispatchEvent(new PageTransitionEvent('pagehide')));expect(await p.page.$eval('#signed',node=>getComputedStyle(node).display)).toBe('none');await p.page.evaluate(()=>window.dispatchEvent(new PageTransitionEvent('pageshow',{persisted:true})));await p.page.waitForFunction(()=>document.documentElement.dataset.accountState==='authenticated');expect(await p.page.$eval('#connect [name=display_name]',node=>node.value)).toBe('Unsaved source identity');const other=await f.login('candidate-other-'+crypto.randomUUID()+'@example.test');await p.page.evaluate(()=>window.dispatchEvent(new PageTransitionEvent('pagehide')));const [name,...value]=other.cookie.split('=');await p.page.setCookie({name,value:value.join('='),url:f.base,httpOnly:true,sameSite:'Lax'});await p.page.evaluate(()=>window.dispatchEvent(new PageTransitionEvent('pageshow',{persisted:true})));await p.page.waitForFunction(id=>document.querySelector('#identity').textContent.includes('#'+id),{},other.id);expect(await p.page.$eval('#connect [name=display_name]',node=>node.value)).toBe('');record('history and identity','Same-account draft retained; different-account draft unavailable. Events are explicitly synthetic, not a browser-managed BFCache traversal.');}finally{await p.close();}
},30000);
test('identity outage does not invent logout and the shared kit renders an unsafe name as text',async()=>{
 const p=await pageFor();try{p.mode.identityFail=true;await p.page.goto(f.base+'/account',{waitUntil:'networkidle0'});await p.page.waitForFunction(()=>document.documentElement.dataset.accountState==='unavailable');expect(await p.page.$eval('#out',node=>!!node.querySelector('a[href^="/signup"]'))).toBe(false);p.mode.identityFail=false;f.authsql.query('UPDATE users SET name=? WHERE id=?').run('<img src=x onerror=window.BAD=true>',p.actor.id);await p.page.click('[data-account-retry]');await p.page.waitForFunction(()=>document.documentElement.dataset.accountState==='authenticated');await p.page.evaluate(()=>document.body.append(K.note({kind:'refused',status:403,path:'/fixture'},'Fixture section')));await p.page.waitForFunction(()=>document.querySelector('.k-session-note').textContent.includes('<img'));expect(await p.page.$eval('.k-session-note',node=>node.children.length)).toBe(0);expect(await p.page.evaluate(()=>window.BAD)).toBeUndefined();record('identity unavailable and safe label','A failed check stays unknown; account names cannot become markup in the shared refusal note.');}finally{await p.close();}
},30000);
const sourceRow=id=>({subject_id:'foreign_twin:'+id,url:'https://'+id+'.example.test/feed',claims:0,promoted:false,release:{source_contract:{source:'feed-'+id,family:'research',body_regime:'REMOTE',tier:'WARM',cadence_seconds:3600},stages:{frontier:{state:'current',channels:1,admitted:1,eligible:0},archive:{state:'current',items:3},public_program:{state:'unbound',bindings:0,programs:[],delivery_state:'not_verified'}},facets:{},readiness:{}}});
const sourceBody=rows=>({schema:'bonfyre.foreign.twins.v2',state:'current',twins:rows,offset:0,total:rows.length,estate_total:rows.length,next_offset:null,catalogue_generation:'browser-v1',release_summary:{source_contracts:rows.length,frontier_current:rows.length,archive_current:rows.length,readiness:{}}});
test('source search, compare, native-route entry and destination controls work on desktop and phone without eager receipt scans',async()=>{
 for(const width of [1440,390]){const p=await pageFor({width});try{p.mode.sources=async url=>({body:sourceBody([sourceRow(url.searchParams.get('q')==='beta'?'beta':'alpha')])});await p.page.goto(f.base+'/foreign/workbench',{waitUntil:'networkidle0'});await p.page.waitForSelector('.source-row');await p.page.click('.source-row>input');await p.page.click('.source-row .row');expect(await p.page.$eval('#detail',node=>node.textContent)).toContain('alpha.example.test');await p.page.type('#search','beta');await p.page.waitForFunction(()=>document.querySelector('.source-row .row')?.textContent.includes('beta'));await p.page.click('.source-row>input');expect(await p.page.$$eval('.source-compare-table tbody tr',nodes=>nodes.length)).toBe(2);expect(await p.page.$eval('.plan',node=>getComputedStyle(node).display)).not.toBe('none');await p.page.$eval('#source-destinations',node=>node.scrollIntoView());expect(await p.page.$eval('#source-destinations',node=>node.getBoundingClientRect().height>0)).toBe(true);expect(await p.page.$eval('a[href="/s/sets/all"]',node=>node.textContent)).toBe('Discovery sets');expect(await p.page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);expect(p.mode.receipts).toBe(0);expect(await p.page.evaluate(()=>localStorage.getItem('bonfyre.foreign.distribution-plan.v2'))).toBeNull();expect(p.mode.posts).toHaveLength(0);await shot(p,'source-workbench-'+width);expect(p.mode.errors).toEqual([]);record('source browser '+width,'Explicit synthetic sources, cross-query comparison, real native route links, reachable plan, no eager receipt reads or writes.');}finally{await p.close();}}
},45000);
test('shared navigation CSS no longer destroys donor grids, imagery and local typography',async()=>{
 const p=await pageFor({width:390});try{await p.page.goto(f.base+'/healthz');await p.page.setContent('<html><head><link href="/assets/donor-pinry.css" rel="stylesheet"><link href="/assets/donor-nextcommerce.css" rel="stylesheet"><link href="/assets/donor-mobilizon.css" rel="stylesheet"><style>#pn-grid,#nx-grid{display:grid;grid-template-columns:1fr 1fr}.pin-card{background:rgb(21,32,43);box-shadow:0 5px 10px black}.pin-card h2{font-family:monospace}#mz-banner{background-image:linear-gradient(red,blue);height:100px}</style></head><body><header class="ak-bar">Native navigation</header><div id="pn-grid"><article class="pin-card"><h2>A</h2></article><article>B</article></div><div id="nx-grid"><div>A</div><div>B</div></div><div id="mz-banner"></div></body></html>');await p.page.addStyleTag({content:candidate.changes.get('site/matrix/aurekai-bar.css')});expect(await p.page.$eval('#pn-grid',node=>getComputedStyle(node).display)).toBe('grid');expect(await p.page.$eval('#nx-grid',node=>getComputedStyle(node).display)).toBe('grid');expect(await p.page.$eval('#mz-banner',node=>getComputedStyle(node).backgroundImage)).toContain('linear-gradient');expect(await p.page.$eval('.pin-card h2',node=>getComputedStyle(node).fontFamily)).toBe('monospace');await p.page.emulateMediaFeatures([{name:'prefers-reduced-motion',value:'reduce'}]);record('donor anatomy','Synthetic minimal CSS fixture; proves destructive overrides removed, not full live-gallery visual parity.');}finally{await p.close();}
},30000);

test('the existing public-profile owner stays connected to the signed-in menu',async()=>{
 const p=await pageFor({width:390});try{const matrix=join(f.home,'matrix.db'),actor='person:review-avery',evidence='fixture:profile-navigation';
 f.run(f.auth,['--db',f.authdb,'actor-bind','--user-id',String(p.actor.id),'--actor',actor]);
 f.run(f.web,['subject','--db',matrix,'--id',actor,'--kind','Person','--digest','a'.repeat(64),'--evidence',evidence]);
 f.run(f.web,['institution','--db',matrix,'--id','inst:person-review-avery','--kind','Person','--world','live','--evidence',evidence]);
 f.run(f.web,['domain','--db',matrix,'--host','review-avery.aurekai.life','--institution','inst:person-review-avery','--territory','life','--scope','public','--world','live','--repr','HTML','--evidence',evidence]);
 f.run(f.web,['presence','--db',matrix,'--id','presence:review-avery:life','--subject',actor,'--territory','life','--scope','public','--world','live','--repr','HTML','--evidence',evidence]);
 await signed(p);await p.page.waitForSelector('[data-public-profile-menu]');await p.page.click('.ak-user summary');
 expect(await p.page.$eval('[data-public-profile-menu]',n=>n.getAttribute('href'))).toBe('https://review-avery.aurekai.life/');
 expect(await p.page.$eval('[data-public-profile-menu]',n=>n.getBoundingClientRect().height>0)).toBe(true);
 expect(await p.page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);
 expect(await p.page.$$eval('.ak-session a',nodes=>nodes.some(n=>new URL(n.href).pathname==='/signup'))).toBe(false);
 record('existing profile resolution','Disposable explicit Auth actor and Matrix LIFE presence; the exact public profile is in the working mobile menu, with no signup link.');
 }finally{await p.close()}
},30000);
