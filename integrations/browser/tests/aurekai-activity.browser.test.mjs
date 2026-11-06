// Real WebWork retention and shared MessagePack adapter, served only on loopback.
// Provider execution is injected; no credential store or external service is used.
import {test,expect,beforeAll,beforeEach,afterAll} from 'bun:test';
import {readFile} from 'node:fs/promises';
import {mkdtempSync,mkdirSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {resolve,join} from 'node:path';
import {WebWork} from '../../../cmd/BonfyreSourceFrontier/tools/web_work.mjs';
import {webResults,webSpec,WebRefusal} from '../../../cmd/BonfyreSourceFrontier/tools/web_protocol.mjs';
import {createFabricStore} from '../../mcp/fabric_store.mjs';
import {encode,decode} from '../workspace-msgpack.mjs';
const root=process.env.BONFYRE_TEST_PUBLIC_BROWSER||resolve(import.meta.dir,'..'),deferred=()=>{let resolve;const promise=new Promise(done=>resolve=done);return {promise,resolve};};
const packet=value=>new Response(encode(value),{headers:{'Content-Type':'application/msgpack','Cache-Control':'no-store'}});
let browser,server,owners={},jobs={},calls=[],readGate=null,access=true,finishActive=null,activeTask=null,clock=Date.UTC(2026,9,21,18);
let fabric,fabricRoot,nativeUnavailable=false,researchUnavailable=false,nativeRefused=false,loseActionReply=false,nativeActor='user:1';
const binding=(context='self',actor='user:1')=>({actor,subject:context==='self'?actor:context,scope:context==='self'?'self':context.split(':')[0]});
function retainedNative(context='self'){
 const id=fabric.createScopedTask({kind:'claude',title:'Retained native review',prompt:'Fixture only',requestId:'native-'+context},binding(context)).task_id;fabric.claimTask(id);
 const run=fabric.createRun({task_id:id,agent:'claude',role:'review',command:['fixture'],cwd:join(fabricRoot,'project')});fabric.finishRun(run.run_id,{state:'passed',final:'Private retained result',exit_code:0});fabric.saveReceipt(id,'relay-patch','Private fixture patch','patch');fabric.updateTask(id,{state:'completed',completed_at:++clock/1000});return id;
}
const fixtureTitle='Exact sources <img src=x onerror="window.injected=true">';
function makeOwner(subject,scope,actor='user:1'){return new WebWork(':memory:',{subject,scope,actor,clock:()=>++clock,config:{admissionEnv:{BONFYRE_EXTERNAL_READS_ENABLED:'1'}},execute:async spec=>{
 if(spec.query==='Wait')await finishActive.promise;
 if(spec.query==='Uncertain')throw new WebRefusal('Fixture retained outcome is uncertain.','transport',true);
 return {...webResults(webSpec(spec),{structuredContent:{results:spec.query==='Empty'?[]:[{url:'https://example.org/source',title:'Exact source',text:'Retained source text.'}]}}),raw:'fixture provider reply',rawSHA256:'a'.repeat(64),observedAt:new Date(clock).toISOString(),provider:spec.provider,tool:'web_search_advanced_exa'};
}});}
function plan(owner,title,query='Guide',mode='public'){return owner.plan({requestId:'fixture-'+title.replace(/[^A-Za-z0-9_.:-]/g,'-'),title,mode,spec:{provider:'exa',operation:'search',query,objective:'Local activity witness',limit:3}});}
const start=(owner,row)=>owner.start({id:row.id,digest:row.digest,revision:row.revision});
async function finish(){finishActive?.resolve();await activeTask;for(const owner of Object.values(owners))owner.close();owners={};activeTask=null;fabric?.close();fabric=null;if(fabricRoot)rmSync(fabricRoot,{recursive:true,force:true});fabricRoot=null;}
beforeEach(async()=>{
 await finish();calls=[];readGate=null;access=true;nativeUnavailable=researchUnavailable=nativeRefused=loseActionReply=false;nativeActor='user:1';clock=Date.UTC(2026,9,21,18);finishActive=deferred();
 fabricRoot=mkdtempSync(join(tmpdir(),'activity-browser-'));mkdirSync(join(fabricRoot,'project'));fabric=createFabricStore({home:join(fabricRoot,'owner'),projectRoot:join(fabricRoot,'project'),now:()=>++clock/1000});
 owners={self:makeOwner('user:1','self'),'institution:studio':makeOwner('institution:studio','institution')};
 jobs.complete=await start(owners.self,plan(owners.self,'Exact sources','Guide'));
 owners.self.db.query('UPDATE web_work_job SET title=?1 WHERE id=?2').run(fixtureTitle,jobs.complete.id);
 jobs.empty=await start(owners.self,plan(owners.self,'No matching sources','Empty'));
 const archived=await start(owners.self,plan(owners.self,'Archived private output','Archive'));owners.self.archive({id:archived.id,revision:archived.revision});
 jobs.uncertain=await start(owners.self,plan(owners.self,'Interrupted research','Uncertain'));
 jobs.plan=plan(owners.self,'Ready for review','Plan');jobs.active=plan(owners.self,'Research in progress','Wait');activeTask=start(owners.self,jobs.active);
 jobs.org=await start(owners['institution:studio'],plan(owners['institution:studio'],'Studio retained sources','Studio'));
});
beforeAll(async()=>{
 const {chromium}=await import(process.env.BONFYRE_PLAYWRIGHT_MODULE||'@playwright/test');browser=await chromium.launch({headless:true,...process.env.BONFYRE_CHROMIUM_BINARY?{executablePath:process.env.BONFYRE_CHROMIUM_BINARY}:{}});
 server=Bun.serve({hostname:'127.0.0.1',port:0,async fetch(request){const url=new URL(request.url);calls.push({path:url.pathname,search:url.search,method:request.method});
  if(url.pathname==='/fixture'){const nav=url.searchParams.has('native')?'<nav data-fixture-native-workspace data-activity-context="institution%3Astudio">Studio workspace</nav>':'<header class="ak-bar"><span data-fixture-account-slot><span class="ak-session" data-session-state="'+(url.searchParams.has('anonymous')?'anonymous':'signed-in')+'">Account</span></span></header>';return new Response('<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><link rel="stylesheet" href="/apps/aurekai-activity.css"></head><body>'+nav+'<main><h1>Source work</h1><button id="outside">Page action</button><dialog id="other"><form method="dialog"><button id="dialog-action">Dialog action</button></form></dialog></main><script type="module">import {mountAurekaiActivity} from "/apps/aurekai-activity.mjs";import {readActivity} from "/apps/aurekai-activity-source.mjs";const slot=document.querySelector("[data-fixture-account-slot]");if(slot)slot.dataset.accountSlot="";else document.querySelector("[data-fixture-native-workspace]").dataset.nativeWorkspace="";window.fixtureNavigation=state=>{const node=document.createElement("span");node.className="ak-session";node.dataset.sessionState=state;node.textContent="Account";slot.replaceChildren(node);};window.fixtureRail=mountAurekaiActivity({intervalMs:3600000,tasks:null,inbox:null,needs:null,setNotice:null,providerView:async()=>({mountForeignSignIn:(node,review)=>{window.fixtureProviderMounts=(window.fixtureProviderMounts||0)+1;window.fixtureProviderReview=review;const control=document.createElement("button");control.textContent="Provider-owned disabled control";control.disabled=true;node.append(control);return {dispose(){window.fixtureProviderDisposals=(window.fixtureProviderDisposals||0)+1;}};}}),activity:(context,options)=>readActivity(context,{...options,signal:window.fixtureIgnoreAbort?undefined:options.signal})});</script></body></html>',{headers:{'Content-Type':'text/html'}});}
  if(url.pathname==='/apps/_destinations')return access?packet([1,[['self','Your account',true],['institution:studio','Studio',false]]]):new Response('Current access refused',{status:403});
  if(url.pathname==='/sources/_web'){
   if(researchUnavailable)return new Response('Research unavailable',{status:503});
   if(!access)return new Response('Current access refused',{status:403});const owner=owners[url.searchParams.get('context')];if(!owner)return new Response('Scope refused',{status:403});
   if(request.method!=='GET'||!['list','get'].includes(url.searchParams.get('operation')))return new Response('Read-only fixture',{status:405});
   const reply=url.searchParams.get('operation')==='get'?owner.get(url.searchParams.get('job')):owner.list(),gate=readGate;if(gate){readGate=null;gate.entered.resolve();await gate.release.promise;}return packet([1,true,reply]);
  }
  if(url.pathname==='/apps/_fabric'){
   if(!access||nativeRefused)return new Response('Access refused',{status:403});if(nativeUnavailable)return new Response('Native owner unavailable',{status:503});const context=url.searchParams.get('context');if(!['self','institution:studio'].includes(context))return new Response('Scope refused',{status:403});
   try{const authority=binding(context,nativeActor);if(request.method==='GET'&&url.searchParams.get('operation')==='read')return packet([1,true,fabric.activityRead(authority,{execution:()=>({owned:false,queued:false}),taskId:url.searchParams.get('task')||undefined})]);
    if(request.method==='POST'&&url.searchParams.get('operation')==='notification'){const value=decode(new Uint8Array(await request.arrayBuffer())),receipt=fabric.notificationAction(authority,value[1]);if(loseActionReply){loseActionReply=false;return new Response('Acknowledgment unavailable',{status:503});}return packet([1,true,receipt]);}return new Response('Method refused',{status:405});
   }catch{return new Response('Native review refused',{status:409});}
  }
  if(/^\/apps\/(?:aurekai-activity(?:-source)?\.mjs|aurekai-push\.mjs|aurekai-inbox-source\.mjs|aurekai-activity\.css|workspace-msgpack\.mjs|extension\/workbench-shell\.mjs)$/.test(url.pathname))return new Response(await readFile(resolve(root,url.pathname.slice(6))),{headers:{'Content-Type':url.pathname.endsWith('.css')?'text/css':'text/javascript'}});
  return new Response('Not found',{status:404});
 }});
});
afterAll(async()=>{await browser?.close();readGate?.release.resolve();await finish();server?.stop(true);});
const gate=()=>({entered:deferred(),release:deferred()});
async function visit(query=''){const page=await browser.newPage();page.setDefaultTimeout(7000);await page.goto(server.url+'fixture'+query);await page.locator('.ak-rail-status').filter({hasText:'Current research activity'}).waitFor({state:'attached'});if(await page.locator('.ak-rail-launch').getAttribute('aria-expanded')==='false')await page.locator('.ak-rail-launch').click();return page;}

test('actual retained runs drive research state and outputs while recipient inbox stays separate',async()=>{
 const page=await visit();try{
  expect(await page.locator('.ak-rail-badge').textContent()).toBe('1');expect(await page.locator('.ak-rail-runs').textContent()).toContain('Running');
  expect(await page.locator('.ak-rail-outputs a').count()).toBe(1);const link=await page.locator('.ak-rail-outputs a').getAttribute('href');const address=new URL(link,server.url);expect(address.pathname).toBe('/sources/web');expect(address.searchParams.get('context')).toBe('self');expect(address.searchParams.get('job')).toBe(jobs.complete.id);
  expect(await page.locator('.ak-rail-outputs').textContent()).toContain(fixtureTitle);expect(await page.locator('.ak-rail-outputs img').count()).toBe(0);expect(await page.evaluate(()=>window.injected)).toBeUndefined();
  expect(await page.locator('.ak-rail-research-updates').textContent()).toContain('Check the saved outcome');expect(await page.locator('.ak-rail-notices').textContent()).toContain('No recent notifications');expect((await page.locator('.ak-rail-card').allTextContents()).join(' ')).not.toContain('Archived private output');expect(await page.locator('.ak-rail-outputs').textContent()).not.toContain('No matching sources');
  expect(await page.getByRole('link',{name:'Model Commons',exact:true}).getAttribute('href')).toBe('/s/models/all?context=self');
  expect(await page.getByRole('link',{name:'Programs',exact:true}).getAttribute('href')).toBe('/programs?context=self');
  finishActive.resolve();await activeTask;await page.evaluate(()=>window.fixtureRail.refresh());expect(await page.locator('.ak-rail-badge').isVisible()).toBe(false);expect(await page.locator('.ak-rail-outputs a').count()).toBe(2);
  expect(calls.every(call=>call.method==='GET')).toBe(true);expect(await page.evaluate(()=>localStorage.length)).toBe(0);
 }finally{await page.close();}
},15000);

test('a late uncooperative read cannot cross destination selection',async()=>{
 const page=await visit(),waiting=gate();try{
  readGate=waiting;await page.evaluate(()=>{window.fixtureIgnoreAbort=true;window.fixturePending=window.fixtureRail.refresh();});await waiting.entered.promise;
  await page.getByLabel('Activity account or organization').selectOption('institution:studio');await page.locator('.ak-rail-outputs').filter({hasText:'Studio retained sources'}).waitFor();
  expect(await page.locator('.ak-rail-cards').textContent()).not.toContain(fixtureTitle);expect(await page.locator('.ak-rail-inbox>p').first().textContent()).toBe('Studio');
  waiting.release.resolve();await page.evaluate(()=>window.fixturePending);expect(await page.getByLabel('Activity account or organization').inputValue()).toBe('institution:studio');expect(await page.locator('.ak-rail-outputs').textContent()).toContain('Studio retained sources');expect(await page.locator('.ak-rail-outputs').textContent()).not.toContain('Exact sources');
  for(const href of await page.locator('.ak-rail-outputs a,.ak-rail-runs a').evaluateAll(nodes=>nodes.map(node=>node.href)))expect(new URL(href).searchParams.get('context')).toBe('institution:studio');
  expect(await page.getByRole('link',{name:'Model Commons',exact:true}).getAttribute('href')).toBe('/s/models/all?context=institution%3Astudio');
  expect(await page.getByRole('link',{name:'Programs',exact:true}).getAttribute('href')).toBe('/programs?context=institution%3Astudio');
 }finally{waiting.release.resolve();await page.close();}
},15000);

test('a signed-out visitor on desktop keeps the page width and opens activity only on request',async()=>{
 const page=await browser.newPage();page.setDefaultTimeout(7000);try{
  await page.setViewportSize({width:1440,height:900});await page.goto(server.url+'fixture?anonymous');await page.locator('.ak-rail-status').filter({hasText:'Sign in to see your activity.'}).waitFor({state:'attached'});
  expect(await page.locator('.ak-rail-launch').getAttribute('aria-expanded')).toBe('false');expect(await page.locator('[data-aurekai-activity]').getAttribute('data-docked')).not.toBe('true');expect(await page.evaluate(()=>document.body.style.paddingInlineEnd)).toBe('');
  await page.locator('.ak-rail-launch').click();await page.getByRole('link',{name:'Sign in',exact:true}).waitFor(); // signed out: no account picker, inbox or outputs, only sign-in and public research links; docked, --ak-dock leaves the page its width
  expect(await page.locator('[data-aurekai-activity]').getAttribute('data-visitor')).toBe('anonymous');for(const hidden of ['.ak-rail-scope','.ak-rail-inbox','.ak-rail-outputs'])expect(await page.locator(hidden).isVisible()).toBe(false);await page.getByRole('link',{name:'Programs'}).waitFor();expect(await page.evaluate(()=>document.documentElement.style.getPropertyValue('--ak-dock'))).toBe(await page.locator('[data-aurekai-activity]').getAttribute('data-docked')==='true'?'390px':'0px');
 }finally{await page.close();}
});
test('navigation invalidation clears private rows and re-enables retry after an aborted refresh',async()=>{
 const page=await visit(),waiting=gate();try{
  await page.getByLabel('Activity account or organization').selectOption('institution:studio');await page.locator('.ak-rail-outputs').filter({hasText:'Studio retained sources'}).waitFor();
  readGate=waiting;await page.evaluate(()=>{window.fixturePending=window.fixtureRail.refresh();});await waiting.entered.promise;
  await page.evaluate(()=>window.fixtureNavigation('unavailable'));await page.locator('.ak-rail-status').filter({hasText:'could not be checked'}).waitFor();expect(await page.locator('.ak-rail-cards').textContent()).not.toContain('Studio retained sources');expect(await page.getByRole('button',{name:'Refresh',exact:true}).isDisabled()).toBe(false);expect(await page.locator('[data-aurekai-activity]').getAttribute('aria-busy')).toBeNull();
  waiting.release.resolve();await page.evaluate(()=>window.fixturePending);await page.evaluate(()=>window.fixtureNavigation('anonymous'));await page.getByRole('link',{name:'Sign in',exact:true}).waitFor();expect(await page.getByRole('button',{name:'Refresh',exact:true}).isDisabled()).toBe(false);
  const href=await page.getByRole('link',{name:'Sign in',exact:true}).getAttribute('href');expect(new URL(href,server.url).searchParams.get('next')).toBe('/fixture');
  await page.evaluate(()=>window.fixtureNavigation('signed-in'));await page.locator('.ak-rail-outputs').filter({hasText:'Exact sources'}).waitFor();expect(await page.getByLabel('Activity account or organization').inputValue()).toBe('self');expect(await page.locator('.ak-rail-cards').textContent()).not.toContain('Studio retained sources');
 }finally{waiting.release.resolve();await page.close();}
},15000);

test('revoked current access removes destination labels, outputs and update cards',async()=>{
 const page=await visit('?context=institution%3Astudio');try{expect(await page.locator('.ak-rail-outputs').textContent()).toContain('Studio retained sources');access=false;await page.evaluate(()=>window.fixtureRail.refresh());expect(await page.locator('.ak-rail-status').textContent()).toContain('Current access');expect(await page.locator('.ak-rail-cards').textContent()).not.toContain('Studio retained sources');expect(await page.getByLabel('Activity account or organization').locator('option').count()).toBe(0);expect(await page.locator('.ak-rail-launch').textContent()).not.toContain('Studio');expect(await page.getByRole('button',{name:'Refresh',exact:true}).isDisabled()).toBe(false);
 }finally{await page.close();}
},15000);

test('either owner authority refusal immediately clears private work even before a late successful read settles',async()=>{
 retainedNative();const page=await visit(),waiting=gate();try{
  readGate=waiting;nativeRefused=true;await page.evaluate(()=>{window.fixtureIgnoreAbort=true;window.fixturePending=window.fixtureRail.refresh();});await waiting.entered.promise;
  await page.locator('.ak-rail-status').filter({hasText:'Current access'}).waitFor({state:'attached'});
  expect(await page.locator('.ak-rail-cards').textContent()).not.toContain('Exact sources');expect(await page.locator('.ak-rail-cards').textContent()).not.toContain('Retained native review');expect(await page.locator('.ak-rail-runs a,.ak-rail-outputs a,.ak-rail-notices button').count()).toBe(0);expect(await page.getByLabel('Activity account or organization').locator('option').count()).toBe(0);
  waiting.release.resolve();await page.evaluate(()=>window.fixturePending);expect(await page.locator('.ak-rail-runs a,.ak-rail-outputs a,.ak-rail-notices button').count()).toBe(0);expect(await page.locator('.ak-rail-status').textContent()).toContain('Current access');
 }finally{waiting.release.resolve();await page.close();}
},15000);

test('Escape owned by another dialog is not consumed or moved to the activity launch',async()=>{
 const page=await visit();try{
  await page.evaluate(()=>document.querySelector('#other').showModal());await page.locator('#dialog-action').focus();await page.keyboard.press('Escape');expect(await page.locator('#other').evaluate(node=>node.open)).toBe(false);expect(await page.locator('.ak-rail-launch').getAttribute('aria-expanded')).toBe('true');
  const prevented=await page.evaluate(()=>{const event=new KeyboardEvent('keydown',{key:'Escape',bubbles:true,cancelable:true});event.preventDefault();document.querySelector('#outside').dispatchEvent(event);return document.querySelector('.ak-rail-launch').getAttribute('aria-expanded');});expect(prevented).toBe('true');
  await page.locator('.ak-rail-actions button').first().focus();await page.keyboard.press('Escape');expect(await page.locator('.ak-rail-launch').getAttribute('aria-expanded')).toBe('false');expect(await page.locator('.ak-rail-launch').evaluate(node=>node===document.activeElement)).toBe(true);
 }finally{await page.close();}
},15000);

test('page suspension and disposal suppress late work without leaving a stale account rail',async()=>{
 const page=await visit(),waiting=gate();try{
  readGate=waiting;await page.evaluate(()=>{window.fixtureIgnoreAbort=true;window.fixturePending=window.fixtureRail.refresh();});await waiting.entered.promise;
  await page.evaluate(()=>window.dispatchEvent(new PageTransitionEvent('pagehide',{persisted:true})));expect(await page.locator('.ak-rail-cards').textContent()).not.toContain('Exact sources');expect(await page.locator('.ak-rail-launch').getAttribute('aria-expanded')).toBe('false');waiting.release.resolve();await page.evaluate(()=>window.fixturePending);expect(await page.locator('.ak-rail-outputs a').count()).toBe(0);
  await page.evaluate(()=>window.dispatchEvent(new PageTransitionEvent('pageshow',{persisted:true})));await page.locator('.ak-rail-outputs').filter({hasText:'Exact sources'}).waitFor({state:'attached'});
  await page.evaluate(()=>{window.fixtureRail.destroy();window.fixtureRail.destroy();window.dispatchEvent(new Event('bonfyre:account-changed'));window.dispatchEvent(new PageTransitionEvent('pageshow',{persisted:true}));});expect(await page.locator('[data-aurekai-activity]').count()).toBe(0);expect(await page.evaluate(()=>{const event=new KeyboardEvent('keydown',{key:'Escape',bubbles:true,cancelable:true});document.dispatchEvent(event);return event.defaultPrevented;})).toBe(false);
 }finally{waiting.release.resolve();await page.close();}
},15000);

test('installed editor destination is a selection hint and still requires current native access',async()=>{
 const page=await visit('?native=1&context=self');try{
  expect(await page.locator('[data-account-slot]').count()).toBe(0);expect(await page.getByLabel('Activity account or organization').inputValue()).toBe('institution:studio');expect(await page.locator('.ak-rail-outputs').textContent()).toContain('Studio retained sources');
  await page.evaluate(()=>{document.querySelector('[data-native-workspace]').dataset.activityContext='self';});await page.locator('.ak-rail-outputs').filter({hasText:'Exact sources'}).waitFor();expect(await page.getByLabel('Activity account or organization').inputValue()).toBe('self');
  access=false;await page.evaluate(()=>window.fixtureRail.refresh());expect(await page.locator('.ak-rail-outputs a').count()).toBe(0);expect(await page.getByRole('link',{name:'Sign in',exact:true}).isVisible()).toBe(true);expect(await page.locator('.ak-rail-status').textContent()).toContain('Current access');
 }finally{await page.close();}
},15000);

test('workbench events follow the current destination and reread actual retained outcomes',async()=>{
 const page=await visit();try{
  await page.evaluate(()=>{const url=new URL(location.href);url.searchParams.set('context','institution:studio');history.replaceState(null,'',url);window.dispatchEvent(new CustomEvent('bonfyre:workspace-changed',{detail:{context:'institution:studio'}}));});await page.locator('.ak-rail-outputs').filter({hasText:'Studio retained sources'}).waitFor();
  const later=await start(owners['institution:studio'],plan(owners['institution:studio'],'New studio output','New'));
  await page.evaluate(()=>window.dispatchEvent(new CustomEvent('bonfyre:work-changed',{detail:{context:'institution:studio',state:'invented-completed'}})));await page.locator('.ak-rail-outputs').filter({hasText:'New studio output'}).waitFor();
  const href=await page.locator('.ak-rail-outputs a').filter({hasText:'New studio output'}).getAttribute('href');expect(new URL(href,server.url).searchParams.get('job')).toBe(later.id);expect(await page.locator('.ak-rail-cards').textContent()).not.toContain('invented-completed');
  expect(await page.getByLabel('Activity account or organization').inputValue()).toBe('institution:studio');expect(calls.every(call=>call.method==='GET')).toBe(true);
 }finally{await page.close();}
},15000);

test('real Fabric receipts and recipient actions stay distinct from derived research updates',async()=>{
 const task=retainedNative(),page=await visit();try{
  expect(await page.locator('.ak-rail-tasks').textContent()).toContain('Retained native review');expect(await page.locator('.ak-rail-tasks [data-kind="subagent"]').count()).toBe(1);
  expect(await page.locator('.ak-rail-outputs').textContent()).toContain('Native receipt evidence');expect(await page.locator('.ak-rail-inbox header').textContent()).toContain('2 unread');
  await page.getByRole('button',{name:'Mark read',exact:true}).first().click();await page.locator('.ak-rail-inbox header').filter({hasText:'1 unread'}).waitFor();expect(fabric.activityRead(binding(),{execution:()=>({owned:false,queued:false})}).unread).toBe(1);
  await page.getByRole('button',{name:'Dismiss',exact:true}).first().click();await page.waitForFunction(()=>document.querySelectorAll('.ak-rail-notices>li').length===1);expect(fabric.activityRead(binding(),{execution:()=>({owned:false,queued:false})}).notifications).toHaveLength(1);expect(fabric.task(task).state).toBe('completed');
  expect(await page.locator('.ak-rail-research-updates').textContent()).toContain('Check the saved outcome');expect(calls.filter(call=>call.method==='POST').every(call=>call.path==='/apps/_fabric')).toBe(true);
  await page.getByLabel('Activity account or organization').selectOption('institution:studio');await page.locator('.ak-rail-outputs').filter({hasText:'Studio retained sources'}).waitFor();expect(await page.locator('.ak-rail-cards').textContent()).not.toContain('Retained native review');expect(await page.locator('.ak-rail-inbox header').textContent()).toContain('0 unread');
 }finally{await page.close();}
},15000);

test('an unconfirmed recipient action stays frozen until a fresh owner read reconciles its actual result',async()=>{
 retainedNative();const page=await visit();try{
  loseActionReply=true;await page.getByRole('button',{name:'Mark read',exact:true}).first().click();await page.locator('.ak-rail-inbox-note').filter({hasText:'could not be confirmed'}).waitFor();
  const failed=page.locator('.ak-rail-notices>li').first();expect(await failed.locator('button').evaluateAll(nodes=>nodes.every(node=>node.disabled))).toBe(true);expect(await page.locator('.ak-rail-inbox header').textContent()).toContain('2 unread');expect(fabric.activityRead(binding(),{execution:()=>({owned:false,queued:false})}).unread).toBe(1);
  await failed.locator('button').first().dispatchEvent('click');expect(calls.filter(call=>call.method==='POST')).toHaveLength(1);
  await page.evaluate(()=>window.fixtureRail.refresh());expect(await page.locator('.ak-rail-inbox header').textContent()).toContain('1 unread');expect(await page.getByRole('button',{name:'Mark read',exact:true}).count()).toBe(1);expect(await failed.getByRole('button',{name:'Dismiss',exact:true}).isEnabled()).toBe(true);expect(calls.filter(call=>call.method==='POST')).toHaveLength(1);
 }finally{await page.close();}
},15000);

test('independent owner outages preserve healthy work and disagreeing account bindings never merge',async()=>{
 retainedNative();const page=await visit();try{
  nativeUnavailable=true;await page.evaluate(()=>window.fixtureRail.refresh());expect(await page.locator('.ak-rail-outputs').textContent()).toContain('Exact sources');expect(await page.locator('.ak-rail-notices').textContent()).toContain('could not be checked');expect(await page.locator('.ak-rail-tasks').textContent()).not.toContain('Retained native review');
  nativeUnavailable=false;researchUnavailable=true;await page.evaluate(()=>window.fixtureRail.refresh());expect(await page.locator('.ak-rail-tasks').textContent()).toContain('Retained native review');expect(await page.locator('.ak-rail-outputs').textContent()).not.toContain('Exact sources');
  researchUnavailable=false;nativeActor='user:2';await page.evaluate(()=>window.fixtureRail.refresh());await page.locator('.ak-rail-status').filter({hasText:'current account could not be confirmed'}).waitFor();expect(await page.locator('.ak-rail-cards').textContent()).not.toContain('Exact sources');expect(await page.locator('.ak-rail-cards').textContent()).not.toContain('Retained native review');expect(await page.locator('.ak-rail-inbox header').textContent()).not.toContain('unread');
 }finally{await page.close();}
},15000);

test('receipt continuations resolve exact native detail and provider presentation survives same-scope polling',async()=>{
 const task=retainedNative(),account=plan(owners.self,'Account provider review','Plan','account'),page=await visit();try{
  await page.locator('.ak-rail-outputs a').filter({hasText:'Implementation patch receipt'}).click();await page.locator('.ak-rail-detail h3').filter({hasText:'Retained native review'}).waitFor();expect(await page.locator('.ak-rail-detail').textContent()).toContain('Receipt evidence');expect(await page.locator('.ak-rail-detail').textContent()).not.toContain('Private fixture patch');expect(calls.some(call=>call.path==='/apps/_fabric'&&new URLSearchParams(call.search).get('task')===task)).toBe(true);
  await page.getByRole('button',{name:'Close details',exact:true}).click();await page.getByRole('button',{name:'Details for Research in progress',exact:true}).click();await page.locator('.ak-rail-detail').filter({hasText:'This run uses public provider access.'}).waitFor();expect(await page.locator('.ak-rail-detail h3').textContent()).toBe('Research in progress');expect(await page.locator('.ak-rail-provider').count()).toBe(0);
  await page.getByRole('button',{name:'Close details',exact:true}).click();await page.getByRole('button',{name:'Details for Account provider review',exact:true}).click();await page.getByRole('button',{name:'Provider-owned disabled control'}).waitFor();const before=await page.evaluate(()=>({count:window.fixtureProviderMounts,review:window.fixtureProviderReview}));expect(before.review.binding).toEqual(binding());expect(before.review.jobId).toBe(account.id);expect(before.review.capability.canContinue).toBe(true);
  await page.evaluate(()=>window.fixtureRail.refresh());expect(await page.evaluate(()=>window.fixtureProviderMounts)).toBe(before.count);expect(await page.getByRole('button',{name:'Provider-owned disabled control'}).isDisabled()).toBe(true);
  await page.getByRole('button',{name:'Close Aurekai activity',exact:true}).click();expect(await page.locator('.ak-rail-detail').isVisible()).toBe(false);await page.locator('.ak-rail-launch').click();expect(await page.getByRole('button',{name:'Provider-owned disabled control'}).isVisible()).toBe(true);expect(await page.evaluate(()=>window.fixtureProviderMounts)).toBe(before.count);
  await page.getByLabel('Activity account or organization').selectOption('institution:studio');await page.locator('.ak-rail-outputs').filter({hasText:'Studio retained sources'}).waitFor();expect(await page.locator('.ak-rail-provider').count()).toBe(0);expect(await page.evaluate(()=>window.fixtureProviderDisposals)).toBe(1);
 }finally{await page.close();}
},15000);

test('provider-only authority refusal clears the rail immediately while obsolete cards cannot clear a newer account',async()=>{
 retainedNative();plan(owners.self,'Account provider review','Plan','account');const page=await visit();try{
  await page.getByRole('button',{name:'Details for Account provider review',exact:true}).click();await page.getByRole('button',{name:'Provider-owned disabled control'}).waitFor();
  await page.evaluate(()=>{window.fixtureOldProviderRefusal=window.fixtureProviderReview.onAccessRefused;window.fixtureOldProviderRefusal(Object.assign(Error('Provider access refused'),{status:403}));});
  expect(await page.locator('.ak-rail-status').textContent()).toContain('Current access');expect(await page.locator('.ak-rail-runs a,.ak-rail-outputs a,.ak-rail-notices button,.ak-rail-provider').count()).toBe(0);expect(await page.locator('.ak-rail-cards').textContent()).not.toContain('Retained native review');expect(await page.getByRole('link',{name:'Model Commons',exact:true}).getAttribute('href')).toBe('/s/models/all');
  expect(await page.getByRole('link',{name:'Programs',exact:true}).getAttribute('href')).toBe('/programs');
  finishActive.resolve();await activeTask;owners.self.close();owners.self=makeOwner('user:2','self','user:2');nativeActor='user:2';await start(owners.self,plan(owners.self,'Second account retained sources'));plan(owners.self,'Second account provider review','Plan','account');
  await page.evaluate(()=>window.fixtureNavigation('signed-in'));await page.locator('.ak-rail-outputs').filter({hasText:'Second account retained sources'}).waitFor();
  await page.evaluate(()=>{window.fixtureOldProviderRefusal(Object.assign(Error('Late old refusal'),{status:403}));window.fixtureOldProviderRefusal(Object.assign(Error('Late old binding change'),{code:'binding_changed'}));});
  expect(await page.locator('.ak-rail-outputs').textContent()).toContain('Second account retained sources');expect(await page.getByLabel('Activity account or organization').inputValue()).toBe('self');expect(await page.getByRole('link',{name:'Model Commons',exact:true}).getAttribute('href')).toBe('/s/models/all?context=self');
  expect(await page.getByRole('link',{name:'Programs',exact:true}).getAttribute('href')).toBe('/programs?context=self');
  await page.getByRole('button',{name:'Details for Second account provider review',exact:true}).click();await page.getByRole('button',{name:'Provider-owned disabled control'}).waitFor();expect(await page.evaluate(()=>window.fixtureProviderReview.binding.actor)).toBe('user:2');
  await page.evaluate(()=>window.fixtureProviderReview.onAccessRefused(Object.assign(Error('Current binding changed'),{code:'binding_changed'})));expect(await page.locator('.ak-rail-status').textContent()).toContain('Current access');expect(await page.locator('.ak-rail-runs a,.ak-rail-outputs a,.ak-rail-notices button,.ak-rail-provider').count()).toBe(0);expect(await page.evaluate(()=>window.fixtureProviderDisposals)).toBe(2);
 }finally{await page.close();}
},15000);

test('body-replacing editor retains the same shared rail, native destination and controller',async()=>{
 const page=await visit('?native=1');try{
  await page.evaluate(async()=>{window.fixtureOriginalRail=document.querySelector('[data-aurekai-activity]');const {mountWorkbenchShell}=await import('/apps/extension/workbench-shell.mjs');mountWorkbenchShell();});
  expect(await page.locator('[data-native-workspace]').count()).toBe(1);expect(await page.locator('[data-aurekai-activity]').count()).toBe(1);expect(await page.evaluate(()=>window.fixtureOriginalRail===document.querySelector('[data-aurekai-activity]'))).toBe(true);await page.evaluate(()=>window.fixtureRail.refresh());expect(await page.getByLabel('Activity account or organization').inputValue()).toBe('institution:studio');expect(await page.locator('.ak-rail-outputs').textContent()).toContain('Studio retained sources');
 }finally{await page.close();}
},15000);

test('desktop reserves working space and 1024, 390 and 320 layouts retain reachable keyboard controls',async()=>{
 retainedNative();const page=await visit(),screens=process.env.BONFYRE_ACTIVITY_SCREENSHOTS;try{
  for(const name of ['Activity','Outputs','Inbox']){const heading=page.getByRole('heading',{name,exact:true}),bounds=await heading.boundingBox();expect(await heading.isVisible()).toBe(true);expect(bounds.y).toBeGreaterThanOrEqual(0);expect(bounds.y+bounds.height).toBeLessThanOrEqual(720);}
  expect(await page.locator('.ak-rail-subagents').evaluateAll(nodes=>nodes.every(node=>!node.open))).toBe(true);
  if(screens){mkdirSync(screens,{recursive:true});await page.screenshot({path:join(screens,'activity-desktop.png'),fullPage:true});}
  expect(await page.evaluate(()=>parseFloat(getComputedStyle(document.body).paddingInlineEnd))).toBeGreaterThanOrEqual(390);
  for(const width of [1024,390,320]){
   await page.setViewportSize({width,height:780});await page.evaluate(()=>new Promise(requestAnimationFrame));if(await page.locator('.ak-rail-launch').getAttribute('aria-expanded')==='false')await page.locator('.ak-rail-launch').click();
   await page.locator('.ak-rail-cards').waitFor({state:'visible'});if(screens)await page.screenshot({path:join(screens,'activity-'+width+'.png'),fullPage:true});const bounds=await page.locator('.ak-rail-cards').boundingBox();expect(bounds.x).toBeGreaterThanOrEqual(0);expect(bounds.x+bounds.width).toBeLessThanOrEqual(width);expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);
   const close=page.getByRole('button',{name:'Close Aurekai activity',exact:true});expect((await close.boundingBox()).height).toBeGreaterThanOrEqual(44);await close.focus();await page.keyboard.press('Escape');expect(await page.locator('.ak-rail-launch').getAttribute('aria-expanded')).toBe('false');expect(await page.locator('.ak-rail-launch').evaluate(node=>node===document.activeElement)).toBe(true);
  }
  if(screens){
   // Separate owner-created review state. The escaped-injection assertions and
   // proof screenshots above retain their original title and evidence.
   owners.self.archive({id:jobs.complete.id,revision:owners.self.get(jobs.complete.id).revision});await start(owners.self,plan(owners.self,'Shared activity integration review','Review'));
   await page.evaluate(()=>window.fixtureRail.refresh());
   // Remove transient capture focus without clicking page content covered by the expanded mobile rail.
   for(const [width,height,name]of [[1280,720,'desktop'],[390,780,'390']]){await page.setViewportSize({width,height});await page.evaluate(()=>new Promise(requestAnimationFrame));if(await page.locator('.ak-rail-launch').getAttribute('aria-expanded')==='false')await page.locator('.ak-rail-launch').click();await page.locator('.ak-rail-outputs').filter({hasText:'Shared activity integration review'}).waitFor();await page.evaluate(()=>document.activeElement?.blur());await page.mouse.move(0,0);await page.screenshot({path:join(screens,'activity-review-'+name+'.png'),fullPage:true});}
  }
 }finally{await page.close();}
},15000);
