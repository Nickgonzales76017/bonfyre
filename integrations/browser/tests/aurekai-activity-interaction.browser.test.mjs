// Browser presentation tests with the production task and Inbox MessagePack adapters.
// Recipient mutations use a loopback receipt fixture; this is not a live account,
// provider, installed Queue database or operating-system notification witness.
import {test,expect,beforeAll,beforeEach,afterAll} from 'bun:test';
import {readFile,writeFile} from 'node:fs/promises';
import {resolve} from 'node:path';
import {encode,decode} from '../workspace-msgpack.mjs';
const root=process.env.BONFYRE_TEST_PUBLIC_BROWSER||resolve(import.meta.dir,'..'),clone=value=>structuredClone(value),deferred=()=>{let resolve;const promise=new Promise(done=>resolve=done);return {promise,resolve};};
const packet=value=>new Response(encode(value),{headers:{'Content-Type':'application/msgpack','Cache-Control':'no-store'}});
const baseTime=Date.UTC(2026,9,21,19),version='a'.repeat(64),otherVersion='b'.repeat(64);
let browser,server,events=[],state={},recipient='first',receipts=new Map(),writes=0,loseNext=false,holdWrite=null,holdInbox=null,holdResearch=null,allow=true,taskUnavailable=false,readsUnavailable=false,queueRefused=false,inboxActor=null;
const binding=context=>({actor:recipient==='first'?'user:7':'user:8',subject:context==='self'?(recipient==='first'?'user:7':'user:8'):context,scope:context==='self'?'self':context.split(':')[0]});
function fixtures(context){const own=context==='self',prefix=own?'Personal':'Studio';return {
 research:{context,binding:binding(context),activities:[{id:'c'.repeat(64),revision:1,kind:'research',title:prefix+' retained sources',state:'completed',phase:'completed',provider:'exa',created:baseTime-2000,updated:baseTime,archived:false,href:'/sources/web?'+new URLSearchParams({context,job:'c'.repeat(64)}),items:2}],outputs:[{id:'c'.repeat(64),title:prefix+' retained sources',items:2,rawSHA256:'d'.repeat(64),href:'/sources/web?'+new URLSearchParams({context,job:'c'.repeat(64)})}],more:false,cursor:0},
 tasks:{context,binding:binding(context),tasks:[{id:prefix+'-work',seq:2,title:prefix+' document watch',state:'running',phase:'active',created:baseTime-3000,updated:baseTime+3000,version,continuation:'/apps?context='+context,nodes:[{id:'job',parentId:null,operatorId:'Observe page',family:'watch',state:'running',phase:'active',attempt:1,updated:baseTime+3000,hasOutput:false},{id:'child',parentId:'job',operatorId:'Compare revisions',family:'comparison',state:'complete',phase:'completed',attempt:1,updated:baseTime+2000,hasOutput:true}],workers:[{id:'worker-run-1',workerId:'worker-alpha',state:'running',phase:'active',created:baseTime,updated:baseTime+3000}],outputs:[{id:'child',nodeId:'child',label:'Revision comparison',kind:'declared-output',reference:'e'.repeat(64),updated:baseTime+2000}],counts:{nodes:2,workers:1,activeWorkers:1,outputs:1,pending:0,active:1,attention:0,completed:1,cancelled:0},nodesMore:false,workersMore:false,outputsMore:false},{id:prefix+'-attention',seq:1,title:prefix+' needs review',state:'cancel_requested',phase:'attention',created:baseTime-1000,updated:baseTime+1000,version,continuation:'/apps?context='+context,nodes:[{id:'job',parentId:null,operatorId:'Review change',family:'review',state:'cancel_requested',phase:'attention',attempt:1,updated:baseTime+1000,hasOutput:false}],workers:[],outputs:[],counts:{nodes:1,workers:0,activeWorkers:0,outputs:0,pending:0,active:0,attention:1,completed:0,cancelled:0},nodesMore:false,workersMore:false,outputsMore:false}],more:false,cursor:0},
 needs:[],notices:[{id:prefix+'-work',taskId:prefix+'-work',phase:'active',version,revision:0,title:'Watch updated',body:prefix+' document watch <img src=x onerror="window.injected=true">',updated:baseTime+3000,href:'/apps?context='+context,read:false,dismissed:false},{id:prefix+'-attention',taskId:prefix+'-attention',phase:'attention',version,revision:0,title:'Review needed',body:prefix+' needs review',updated:baseTime+1000,href:'/apps?context='+context,read:false,dismissed:false}]
};}
// Agent runs as the account work index serves them: absent counters stay null.
const agentRuns=context=>context==='self'?{agents:[{id:'run-codex',taskId:'account-fabric-duel',title:'Personal duel review',agent:'codex',role:'peer-critique',session:'duel',state:'running',phase:'active',started:Date.now()-125000,updated:Date.now()-1000,wallMs:null,limit:false,unconfirmed:false,usage:null},{id:'run-claude',taskId:'account-fabric-duel',title:'Personal duel review',agent:'claude',role:'convergence-arbiter',session:'duel',state:'failed',phase:'attention',started:baseTime,updated:baseTime+7733,wallMs:7733,limit:true,unconfirmed:false,usage:{inputTokens:1200,outputTokens:300,cacheReadTokens:900,cacheWriteTokens:null,estimatedMicroUsd:null}}],counts:{active:1,attention:1,completed:0},more:false}:{agents:[],counts:{active:0,attention:0,completed:0},more:false};
function current(context){const key=recipient+':'+context;if(!state[key])state[key]=fixtures(context);return state[key];}
beforeEach(()=>{events=[];state={};recipient='first';receipts=new Map();writes=0;loseNext=false;holdWrite=null;holdInbox=null;holdResearch=null;allow=true;taskUnavailable=false;readsUnavailable=false;queueRefused=false;inboxActor=null;});
beforeAll(async()=>{
 const {chromium}=await import(process.env.BONFYRE_PLAYWRIGHT_MODULE||'@playwright/test');browser=await chromium.launch({headless:true,...process.env.BONFYRE_CHROMIUM_BINARY?{executablePath:process.env.BONFYRE_CHROMIUM_BINARY}:{}});
 server=Bun.serve({hostname:'127.0.0.1',port:0,async fetch(request){const url=new URL(request.url);events.push({method:request.method,path:url.pathname,search:url.search});
  if(url.pathname==='/fixture')return new Response(`<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><link rel="stylesheet" href="/apps/aurekai-activity.css"></head><body><header><span data-fixture-account-slot><span class="ak-session" data-session-state="signed-in">Account</span></span></header><main><h1>Workspace</h1><button id="outside">Page action</button></main><script type="module">
import {mountAurekaiActivity} from '/apps/aurekai-activity.mjs';import {decode} from '/apps/workspace-msgpack.mjs';import {readInbox,setInboxState} from '/apps/aurekai-inbox-source.mjs';import {readTasks} from '/apps/aurekai-activity-source.mjs';
const slot=document.querySelector('[data-fixture-account-slot]');slot.dataset.accountSlot='';
const read=async(url,options={})=>{const response=await fetch(url,{cache:'no-store',...options,signal:window.fixtureIgnoreAbort?undefined:options.signal});if(!response.ok)throw Object.assign(Error('Fixture refusal'),{status:response.status});return decode(new Uint8Array(await response.arrayBuffer()));};
// The injected card tests rail placement and continuation admission. Its owner
// controls are deliberately local; shared-card and provider tests cover those adapters.
const providerView=async()=>({mountForeignSignIn(host,review){
 window.fixtureProviderMounts=(window.fixtureProviderMounts||0)+1;window.fixtureProviderReview=review;
 const card=document.createElement('section');card.className='fixture-provider-card';card.style.cssText='background:white;padding:24px;min-height:300px;border:1px solid #d8dfd4;border-radius:26px';
 const heading=document.createElement('h3');heading.textContent=review.workTitle;const account=document.createElement('p');account.textContent=review.destinationLabel;const details=document.createElement('div');details.style.cssText='height:350px';details.textContent='Provider setup';details.hidden=!review.expanded;
 const expand=document.createElement('button');expand.textContent='Expand sign-in';expand.style.minHeight='44px';expand.setAttribute('aria-expanded',String(!details.hidden));const toggle=()=>{details.hidden=!details.hidden;expand.setAttribute('aria-expanded',String(!details.hidden));review.onExpandedChange(!details.hidden);};expand.onclick=toggle;
 const takeover=document.createElement('button');takeover.textContent='Take over to sign in';takeover.style.minHeight='44px';takeover.onclick=()=>{window.fixtureProviderOpens=(window.fixtureProviderOpens||0)+1;window.fixtureVisibility(true);};
 const dismiss=document.createElement('button');dismiss.textContent='Not now';dismiss.style.minHeight='44px';dismiss.onclick=()=>review.onDismiss();const back=document.createElement('a');back.textContent='Return to work';back.href=review.returnHref;
 card.append(heading,account,expand,takeover,dismiss,back,details);card.addEventListener('keydown',event=>{if(event.key==='Escape'&&!details.hidden){toggle();event.preventDefault();}});host.append(card);
 return {dispose(){window.fixtureProviderDisposals=(window.fixtureProviderDisposals||0)+1;card.remove();}};
}});
window.fixtureRail=mountAurekaiActivity({intervalMs:3600000,providerView,accountActivity:new URL(location.href).searchParams.has('combined')?undefined:null,destinations:options=>read('/fixture/destinations',options),activity:(context,options={})=>read('/fixture/research?'+new URLSearchParams({context,...options.jobId?{job:options.jobId}:{}}),options),tasks:async(context,options)=>{const result=readTasks(context,{...options,signal:window.fixtureIgnoreAbort?undefined:options.signal}),gate=window.fixtureTaskGates?.[options.outputsOnly?'outputs':'list'];if(gate){await result.then(()=>gate.entered(),()=>gate.entered());await gate.release;}return result;},inbox:(context,options)=>readInbox(context,{...options,signal:window.fixtureIgnoreAbort?undefined:options.signal}),needs:(context,options)=>read('/fixture/needs?'+new URLSearchParams({context}),options),setNotice:(context,input,options)=>{const result=setInboxState(context,input,{...options,signal:window.fixtureIgnoreAbort?undefined:options.signal});(window.fixtureWrites??=[]).push(result.then(()=>undefined,()=>undefined));return result;}});
window.fixtureAccount=()=>{const node=document.createElement('span');node.className='ak-session';node.dataset.sessionState='signed-in';node.textContent='Other account';slot.replaceChildren(node);};
window.fixtureVisibility=hidden=>{Object.defineProperty(document,'hidden',{configurable:true,value:hidden});Object.defineProperty(document,'visibilityState',{configurable:true,value:hidden?'hidden':'visible'});document.dispatchEvent(new Event('visibilitychange'));};
</script></body></html>`,{headers:{'Content-Type':'text/html'}});
  if(/^\/apps\/(?:aurekai-activity(?:-source)?\.mjs|aurekai-inbox-source\.mjs|aurekai-push\.mjs|aurekai-activity\.css|workspace-msgpack\.mjs)$/.test(url.pathname))return new Response(await readFile(resolve(root,url.pathname.slice(6))),{headers:{'Content-Type':url.pathname.endsWith('.css')?'text/css':'text/javascript'}});
  if(!url.pathname.startsWith('/fixture/')&&url.pathname!=='/apps/_activity'&&url.pathname!=='/apps/_fabric')return new Response('Not found',{status:404});
  if(!allow)return new Response('Current access refused',{status:403});
  if(readsUnavailable&&request.method==='GET')return new Response('Read unavailable',{status:503});
  if(url.pathname==='/fixture/destinations')return packet([{context:'self',label:'Your account',canWrite:true},{context:'institution:studio',label:'Studio',canWrite:false}]);
  const context=url.searchParams.get('context');if(!['self','institution:studio'].includes(context))return new Response('Scope refused',{status:403});const data=current(context);
  if(url.pathname==='/fixture/needs')return packet({context,binding:binding(context),items:clone(data.needs)});
  if(url.pathname==='/fixture/research'){const snapshot=clone(data.research),gate=holdResearch;if(gate){holdResearch=null;gate.entered.resolve();await gate.release.promise;}return packet(snapshot);}
  if(url.pathname==='/apps/_fabric'){
   if(url.searchParams.get('operation')!=='read')return new Response('No push provider in this fixture',{status:503});
   const href='/apps?'+new URLSearchParams({context})+'#fabric-task-fabric-work';
   return packet([1,true,{source:'fabric',context,binding:binding(context),revision:'fabric-1',unread:1,more:false,
    tasks:[{id:'task:fabric-work',taskId:'fabric-work',parentTaskId:null,kind:'task',title:'Fabric model work',state:'running',phase:'active',revision:1,created:baseTime,updated:baseTime+4000,reconciliationRequired:false,workKind:'model',href}],
    outputs:[{id:'fabric-output',taskId:'fabric-work',title:'Fabric result evidence',kind:'model-output',digest:version,created:baseTime+4000,href}],
    notifications:[{id:'fabric-notice',taskId:'fabric-work',title:'Fabric recipient notice',kind:'task',state:'unread',revision:1,created:baseTime,updated:baseTime+4000,allowedActions:['read','dismiss'],href}]
   }]);
  }
  if(url.pathname==='/apps/_activity'){
   if(url.searchParams.get('operation')==='agents')return packet([1,true,{...agentRuns(context),binding:binding(context),source:'account-activity'}]);
    if(url.searchParams.get('operation')==='compute') {const providers=[{provider:'claude',records:3,sessions:2,additiveTokens:900,inheritedSnapshotTokens:0,cachedInputTokens:600,uncachedInputTokens:200,cacheWriteTokens:50,outputTokens:50},{provider:'codex',records:2,sessions:1,additiveTokens:600,inheritedSnapshotTokens:300,cachedInputTokens:400,uncachedInputTokens:150,cacheWriteTokens:0,outputTokens:50}];return context==='self'?packet([1,true,{source:'account-activity',binding:binding(context),status:'measured',records:5,providerSessions:3,additiveTokens:1500,inheritedSnapshotTokens:300,cachedInputTokens:1000,providers}]):new Response('Compute belongs to the personal account',{status:403});}
   if(['list','get','outputs'].includes(url.searchParams.get('operation')))return queueRefused?new Response('Queue account refused',{status:403}):taskUnavailable&&url.searchParams.get('operation')!=='outputs'?new Response('Not configured',{status:503}):packet([1,true,url.searchParams.get('operation')==='outputs'?(data.outputTasks||data.tasks):data.tasks]);
   if(request.method==='GET'){const offset=url.searchParams.has('cursor')?Number(url.searchParams.get('cursor')):0,noticePage=offset?data.notices.slice(50):data.notices.slice(0,50),hasMore=!offset&&data.notices.length>50;const expected=binding(context),snapshot={context,binding:inboxActor?{...expected,actor:inboxActor,subject:context==='self'?inboxActor:expected.subject}:expected,notices:clone(noticePage),more:hasMore,cursor:hasMore?'50':''},gate=holdInbox;if(gate){holdInbox=null;gate.entered.resolve();await gate.release.promise;}return packet([1,true,snapshot]);}
   if(request.method!=='POST')return new Response('Method refused',{status:405});const wire=decode(new Uint8Array(await request.arrayBuffer()));if(!Array.isArray(wire)||wire[0]!==1||wire.length!==2)return new Response('Bad native mutation',{status:400});const input=wire[1];if(JSON.stringify(input.expectedBinding)!==JSON.stringify(binding(context)))return new Response('Current binding refused',{status:403});events.push({mutation:clone(input),context,recipient});const key=recipient+':'+context+':'+input.requestId,payload=JSON.stringify(input),previous=receipts.get(key);
   if(previous)return previous.payload===payload?packet([1,true,{...previous.result,replayed:true,notice:clone(data.notices.find(row=>row.id===input.id))}]):packet([1,false,{code:'request_conflict',message:'Request identity conflict.'}]);
   const row=data.notices.find(row=>row.id===input.id);if(!row||row.version!==input.version||row.revision!==input.revision)return packet([1,false,{code:'version_conflict',message:'Stale event or recipient revision.'}]);
   if(!['read','unread','dismiss','restore'].includes(input.action))return new Response('Invalid action',{status:400});
   if(input.action==='read')row.read=true;if(input.action==='unread')row.read=false;if(input.action==='dismiss')row.dismissed=true;if(input.action==='restore')row.dismissed=false;row.revision++;writes++;
   const result={context,binding:binding(context),requestId:input.requestId,replayed:false,appliedVersion:input.version,appliedRevision:input.revision+1,notice:clone(row)};receipts.set(key,{payload,result});const gate=holdWrite;if(gate){holdWrite=null;gate.entered.resolve();await gate.release.promise;}
   if(loseNext){loseNext=false;return new Response('Fixture dropped acknowledgement',{status:503});}return packet([1,true,result]);
  }
  return new Response('Not found',{status:404});
 }});
});
afterAll(async()=>{holdWrite?.release.resolve();holdInbox?.release.resolve();holdResearch?.release.resolve();await browser?.close();server?.stop(true);});
const gate=()=>({entered:deferred(),release:deferred()});
async function visit(options={},query=''){const page=await browser.newPage(options);page.setDefaultTimeout(7000);await page.goto(server.url+'fixture'+query);await page.locator('.ak-rail-status').filter({hasText:'Current account activity'}).waitFor({state:'attached'});if(await page.locator('.ak-rail-launch').getAttribute('aria-expanded')==='false')await page.locator('.ak-rail-launch').click();await page.locator('.ak-rail-notices li[data-notice-id]').first().waitFor();return page;}


async function noticeGeometry(page,label){
 const geometry=await page.evaluate(()=>{
  const cards=document.querySelector('.ak-rail-cards'),sections=document.querySelector('.ak-rail-sections'),list=document.querySelector('.ak-rail-notices'),row=list.querySelector('[data-notice-id]');
  const clipped=node=>{const original=node.getBoundingClientRect();let top=Math.max(0,original.top),bottom=Math.min(innerHeight,original.bottom);
   for(let parent=node.parentElement;parent;parent=parent.parentElement){const style=getComputedStyle(parent);if(/^(auto|scroll|hidden|clip)$/.test(style.overflowY)){const box=parent.getBoundingClientRect();top=Math.max(top,box.top);bottom=Math.min(bottom,box.bottom);}}
   const nav=sections.getBoundingClientRect(),outer=cards.getBoundingClientRect();if(nav.bottom>outer.top&&nav.top<outer.bottom)top=Math.max(top,nav.bottom);
   return {height:Math.max(0,bottom-top),full:original.height};};
  const rowBox=clipped(row),bodyBox=clipped(row.querySelector('.ak-notice-body'));
  const inbox=document.querySelector('.ak-rail-inbox').getBoundingClientRect(),body=document.querySelector('.ak-rail-inbox-body').getBoundingClientRect(),push=document.querySelector('.ak-rail-push').getBoundingClientRect();
  return {inboxContainsContent:body.bottom<=inbox.bottom&&push.bottom<=inbox.bottom,viewport:{width:innerWidth,height:innerHeight},noticeViewport:clipped(list).height,rowVisible:rowBox.height,rowHeight:rowBox.full,bodyVisible:bodyBox.height,controlsVisible:[...row.querySelectorAll('button')].every(node=>{const box=clipped(node);return box.height>=box.full-.5;})};
 });
 if(process.env.BONFYRE_RAIL_SCREENSHOT_DIR)await writeFile(resolve(process.env.BONFYRE_RAIL_SCREENSHOT_DIR,'rail-geometry-'+label+'.json'),JSON.stringify(geometry,null,2)+'\n');
 return geometry;
}

 test('task disclosure keeps real steps, workers and output references together without inventing artifact links',async()=>{
 const page=await visit();try{
  if(process.env.BONFYRE_RAIL_SCREENSHOT_DIR)await page.screenshot({path:resolve(process.env.BONFYRE_RAIL_SCREENSHOT_DIR,'rail-desktop.png')});
  const detail=page.locator('details[data-work-key="task:Personal-work"]');await detail.locator('summary').click();expect(await detail.getByRole('heading',{name:'Steps',exact:true}).count()).toBe(1);expect(await detail.getByRole('heading',{name:'Workers',exact:true}).count()).toBe(1);expect(await detail.locator('.ak-work-children').first().locator('li').count()).toBe(2);expect(await detail.textContent()).toContain('worker-alpha');expect(await detail.textContent()).toContain('Revision comparison');expect(await detail.textContent()).toContain('Recorded output reference');
  if(process.env.BONFYRE_RAIL_SCREENSHOT_DIR)await page.screenshot({path:resolve(process.env.BONFYRE_RAIL_SCREENSHOT_DIR,'rail-details.png')});
  expect(await detail.getByRole('link',{name:'Open workspace',exact:true}).getAttribute('href')).toBe('/apps?context=self');expect(await page.locator('a[href^="file:"],a[href^="s3:"],a[href*="output-1"]').count()).toBe(0);expect(await page.locator('.ak-rail-notices img').count()).toBe(0);expect(await page.evaluate(()=>window.injected)).toBeUndefined();
  expect(await page.locator('.ak-rail-badge').getAttribute('aria-label')).toBe('1 running item');
 }finally{await page.close();}
 },15000);

 test('account work usage is visible and finance navigation keeps the current account',async()=>{
  current('self').tasks.tasks[0].usage={runs:2,reportedRuns:1,inputTokens:300,outputTokens:30,costStatus:'estimate',pricedRuns:1,estimatedMicroUsd:1250000};
  const page=await visit();try{
   const detail=page.locator('details[data-work-key="task:Personal-work"]');expect(await detail.locator('summary').textContent()).toContain('Estimated $1.25 · 1 of 2 runs');await detail.locator('summary').click();
   expect(await detail.textContent()).toContain('Token reports: 1 of 2 runs');
   expect(await detail.textContent()).toContain('300 input');
   expect(await detail.textContent()).toContain('Estimated cost: $1.25 for 1 of 2 runs (not an invoice)');
   expect(await detail.getByRole('link',{name:'Open financial tools'}).getAttribute('href')).toBe('/apps?context=self');
  }finally{await page.close();}
 },15000);

 test('attention work remains available without crowding Inbox controls',async()=>{
  const page=await visit();try{const section=page.locator('.ak-rail-attention');expect(await section.getAttribute('open')).toBeNull();await section.locator('summary').click();const link=section.getByRole('link',{name:'Personal needs review',exact:true});expect(await link.getAttribute('href')).toContain('/work-board?context=self');await link.focus();await page.evaluate(()=>window.fixtureRail.refresh());expect(await link.evaluate(node=>node===document.activeElement)).toBe(true);expect(writes).toBe(0);}finally{await page.close();}
 },15000);

 test('empty recipient notices still expose genuine attention work without a fake unread badge',async()=>{
  const page=await visit();current('self').notices=[];await page.evaluate(()=>window.fixtureRail.refresh());try{
   const section=page.locator('.ak-rail-attention');await section.locator('summary').filter({hasText:'Work needing attention'}).waitFor();
   expect(await page.locator('.ak-rail-notices').textContent()).toContain('No recent notifications. Open the full Inbox for approvals, invitations and account steps.');
   expect(await page.locator('.ak-rail-inbox>header').textContent()).toContain('0 unread');
   expect(await section.isVisible()).toBe(true);await section.locator('summary').click();
   const own=section.getByRole('link',{name:'Personal needs review'});expect(await own.getAttribute('href')).toBe('/work-board?context=self&task=Personal-attention');
   expect(await section.getByRole('button',{name:'Mark read'}).count()).toBe(0);
   await page.getByLabel('Activity account or organization').selectOption('institution:studio');
   await page.waitForFunction(()=>document.querySelector('[data-inbox-page]')?.getAttribute('href')==='/inbox?context=institution%3Astudio');await section.locator('summary').click();await section.getByRole('link',{name:'Studio needs review'}).waitFor();expect(await section.textContent()).not.toContain('Personal needs review');
   allow=false;await page.evaluate(()=>window.fixtureRail.refresh());
   expect(await section.isVisible()).toBe(false);expect(await section.textContent()).not.toContain('Studio needs review');
  }finally{await page.close();}
 },15000);
test('account actions populate the Inbox rail even with zero recipient notifications',async()=>{
 const self=current('self');self.needs=[
  {id:'account-operator-g-release',kind:'release',title:'Approve current release',updated:baseTime+6000},
  {id:'account-operator-g-provider',kind:'provider',title:'Connect Twilio customer accounts',updated:baseTime+5000},
  {id:'invitation:river-guild',kind:'invitation',title:'Join River Guild',updated:0},
  {id:'account-operator-g-credit',kind:'execution-credit',title:'Execution needs API credit',updated:baseTime+4000}
 ];const page=await visit();try{
  self.notices=[];await page.evaluate(()=>window.fixtureRail.refresh());
  const list=page.locator('.ak-rail-notices'),account=list.locator('[data-notice-owner="account"]');
  expect(await account.count()).toBe(4);
  for(const title of self.needs.map(item=>item.title))expect(await list.textContent()).toContain(title);
  expect(await account.getByRole('link',{name:/Approve current release/}).getAttribute('href')).toBe('/inbox?context=self#account-operator-g-release');
  expect(await account.getByRole('link',{name:/Join River Guild/}).getAttribute('href')).toBe('/inbox?context=self#sub-accounts');
  expect(await list.textContent()).not.toContain('No recent notifications');expect(await account.getByRole('button',{name:'Mark read'}).count()).toBe(0);
  await page.locator('[data-inbox-filter=unread]').click();expect(await account.count()).toBe(4);
  await page.locator('[data-inbox-filter=dismissed]').click();expect(await account.count()).toBe(0);
  await page.locator('[data-inbox-filter=all]').click();expect(await account.count()).toBe(4);
  expect(await page.locator('.ak-rail-inbox>header').textContent()).toContain('4 account actions');
  await page.getByLabel('Activity account or organization').selectOption('institution:studio');
  await page.waitForFunction(()=>document.querySelector('[data-inbox-page]')?.getAttribute('href')==='/inbox?context=institution%3Astudio');
  const section=page.locator('.ak-rail-attention');await section.locator('summary').click();await section.getByRole('link',{name:'Studio needs review'}).waitFor();
  expect(await list.textContent()).not.toContain('Approve current release');expect(await account.count()).toBe(0);
  expect(await page.locator('[data-inbox-page]').getAttribute('href')).toBe('/inbox?context=institution%3Astudio');
 }finally{await page.close();}
},15000);

 test('polling preserves open work, keyboard focus and filter selection while rendering changed children',async()=>{
 const page=await visit();try{
  await page.locator('[data-filter=running]').click();const detail=page.locator('details[data-work-key="task:Personal-work"]');await detail.locator('summary').click();await detail.getByRole('link',{name:'Open workspace',exact:true}).focus();current('self').tasks.tasks[0].workers[0].state='completed';current('self').tasks.tasks[0].workers[0].phase='completed';current('self').tasks.tasks[0].counts.activeWorkers=0;current('self').tasks.tasks[0].workers[0].updated++;current('self').tasks.tasks[0].updated++;
  await page.evaluate(()=>window.fixtureRail.refresh());expect(await detail.evaluate(node=>node.open)).toBe(true);expect(await page.locator('[data-filter=running]').getAttribute('aria-pressed')).toBe('true');expect(await page.evaluate(()=>document.activeElement.dataset.focusKey)).toBe('continue:task:Personal-work');expect(await detail.locator('.ak-work-workers').textContent()).toContain('Completed');expect(await page.locator('.ak-rail-runs>li').count()).toBe(1);
  await page.locator('[data-filter=attention]').click();expect(await page.locator('.ak-rail-runs').textContent()).toContain('Personal needs review');expect(await page.locator('.ak-rail-runs').textContent()).not.toContain('document watch');
 }finally{await page.close();}
 },15000);

 test('a worker beyond the bounded detail list still appears in Running from the native count',async()=>{
 const task=current('self').tasks.tasks[0];task.state='complete';task.phase='attention';task.nodes[0].state='complete';task.nodes[0].phase='completed';task.counts.active=0;task.counts.completed=2;task.counts.workers=17;task.counts.activeWorkers=1;task.workersMore=true;task.workers=Array.from({length:16},(_,index)=>({id:'worker-'+index,workerId:'worker-'+index,state:index?'waiting':'failed',phase:index?'pending':'attention',created:baseTime,updated:baseTime+3000}));const page=await visit();try{
  expect(await page.locator('.ak-rail-badge').getAttribute('aria-label')).toBe('1 running item');await page.locator('[data-filter=running]').click();const detail=page.locator('details[data-work-key="task:Personal-work"]');await detail.locator('summary').click();expect(await detail.textContent()).toContain('Showing 16 of 17 workers');expect(await detail.locator('.ak-work-workers li').count()).toBe(16);expect(await page.locator('.ak-rail-runs>li').count()).toBe(1);
 }finally{await page.close();}
 },15000);

 test('recipient read/dismiss survives reload, restores explicitly and never changes work state',async()=>{
 const page=await visit();try{
  const notice=page.locator('[data-notice-id="Personal-work"]');await notice.getByRole('button',{name:'Mark read',exact:true}).click();await notice.getByRole('button',{name:'Mark unread',exact:true}).waitFor();expect(await page.locator('.ak-rail-inbox>header').textContent()).toContain('1 unread update');expect(await page.evaluate(()=>document.activeElement.dataset.focusKey)).toBe('notice-read:Personal-work');
  await notice.getByRole('button',{name:'Dismiss',exact:true}).click();await page.getByRole('button',{name:'Undo',exact:true}).waitFor();expect(await page.locator('[data-notice-id="Personal-work"]').count()).toBe(0);expect(current('self').tasks.tasks[0].state).toBe('running');expect(await page.evaluate(()=>document.activeElement.dataset.focusKey)).toBe('notice-undo');
  await page.reload();await page.locator('.ak-rail-status').filter({hasText:'Current account activity'}).waitFor({state:'attached'});if(await page.locator('.ak-rail-launch').getAttribute('aria-expanded')==='false')await page.locator('.ak-rail-launch').click();expect(await page.locator('[data-notice-id="Personal-work"]').count()).toBe(0);await page.locator('[data-inbox-filter=dismissed]').click();await page.locator('[data-notice-id="Personal-work"]').getByRole('button',{name:'Restore',exact:true}).click();await page.locator('.ak-inbox-receipt').waitFor({state:'hidden'});await page.locator('[data-inbox-filter=all]').click();expect(await page.locator('[data-notice-id="Personal-work"]').count()).toBe(1);expect(writes).toBe(3);expect(current('self').tasks.tasks[0].state).toBe('running');expect(await page.evaluate(()=>localStorage.length)).toBe(0);
 }finally{await page.close();}
 },15000);

 test('lost acknowledgement freezes fresh actions and replays only the exact recipient request',async()=>{
 const page=await visit();try{
  loseNext=true;await page.locator('[data-notice-id="Personal-work"]').getByRole('button',{name:'Dismiss',exact:true}).click();await page.getByRole('button',{name:'Check status',exact:true}).waitFor();expect(writes).toBe(1);expect(await page.locator('.ak-notice-actions button:enabled').count()).toBe(0);await page.evaluate(()=>window.fixtureRail.refresh());expect(await page.getByRole('button',{name:'Check status',exact:true}).count()).toBe(1);expect(await page.locator('.ak-notice-actions button:enabled').count()).toBe(0);expect(await page.evaluate(()=>document.activeElement.dataset.focusKey)).toBe('notice-check');
  await page.getByRole('button',{name:'Check status',exact:true}).click();await page.getByRole('button',{name:'Undo',exact:true}).waitFor();const requests=events.filter(event=>event.mutation);expect(requests).toHaveLength(2);expect(requests[0].mutation).toEqual(requests[1].mutation);expect(writes).toBe(1);expect(await page.locator('[data-notice-id="Personal-work"]').count()).toBe(0);
 }finally{await page.close();}
 },15000);

 for(const lifecycle of ['visibility','cached-page'])test(lifecycle+' suspension preserves only the unresolved request and recovers its exact identity',async()=>{
 const page=await visit(),waiting=gate();try{
  holdWrite=waiting;await page.evaluate(()=>{window.fixtureIgnoreAbort=true;});await page.locator('[data-notice-id="Personal-work"]').getByRole('button',{name:'Dismiss',exact:true}).click();await waiting.entered.promise;const first=clone(events.find(event=>event.mutation).mutation);
  await page.evaluate(kind=>{window.fixtureVisibility(true);if(kind==='cached-page')window.dispatchEvent(new PageTransitionEvent('pagehide',{persisted:true}));},lifecycle);expect(await page.locator('.ak-rail-cards').textContent()).not.toContain('Personal');expect(await page.locator('[data-notice-id]').count()).toBe(0);expect(await page.getByRole('button',{name:'Check status',exact:true}).count()).toBe(0);
  await page.evaluate(kind=>{window.fixtureVisibility(false);if(kind==='cached-page')window.dispatchEvent(new PageTransitionEvent('pageshow',{persisted:true}));},lifecycle);await page.locator('.ak-rail-status').filter({hasText:'Current account activity'}).waitFor({state:'attached'});if(await page.locator('.ak-rail-launch').getAttribute('aria-expanded')==='false')if(await page.locator('.ak-rail-launch').getAttribute('aria-expanded')==='false')await page.locator('.ak-rail-launch').click();await page.getByRole('button',{name:'Check status',exact:true}).waitFor();expect(await page.locator('.ak-inbox-receipt').textContent()).not.toContain('Saving');expect(await page.locator('.ak-notice-actions button:enabled').count()).toBe(0);
  waiting.release.resolve();await page.evaluate(()=>window.fixtureWrites[0]);expect(await page.getByRole('button',{name:'Check status',exact:true}).count()).toBe(1);expect(events.filter(event=>event.mutation)).toHaveLength(1);await page.getByRole('button',{name:'Check status',exact:true}).click();await page.getByRole('button',{name:'Undo',exact:true}).waitFor();const sent=events.filter(event=>event.mutation);expect(sent).toHaveLength(2);expect(sent[1].mutation).toEqual(first);expect(writes).toBe(1);expect(await page.locator('[data-notice-id="Personal-work"]').count()).toBe(0);
 }finally{waiting.release.resolve();await page.close();}
 },15000);

 test('scope and account changes during suspension discard the previous recipient request identity',async()=>{
 const page=await visit();try{
  loseNext=true;await page.locator('[data-notice-id="Personal-work"]').getByRole('button',{name:'Dismiss',exact:true}).click();await page.getByRole('button',{name:'Check status',exact:true}).waitFor();
  await page.evaluate(()=>{window.fixtureVisibility(true);const url=new URL(location.href);url.searchParams.set('context','institution:studio');history.replaceState(null,'',url);window.dispatchEvent(new Event('bonfyre:workspace-changed'));window.fixtureVisibility(false);});await page.locator('.ak-rail-notices').filter({hasText:'Studio document watch'}).waitFor();expect(await page.getByRole('button',{name:'Check status',exact:true}).count()).toBe(0);expect(await page.locator('.ak-notice-actions button:enabled').count()).toBeGreaterThan(0);
  loseNext=true;await page.locator('[data-notice-id="Studio-work"]').getByRole('button',{name:'Mark read',exact:true}).click();await page.getByRole('button',{name:'Check status',exact:true}).waitFor();await page.evaluate(()=>window.fixtureVisibility(true));recipient='second';current('institution:studio').notices[0].body='Second recipient Studio notice';await page.evaluate(()=>window.fixtureAccount());await page.evaluate(()=>window.fixtureVisibility(false));await page.locator('.ak-rail-notices').filter({hasText:'Second recipient Studio notice'}).waitFor();expect(await page.getByRole('button',{name:'Check status',exact:true}).count()).toBe(0);
  await page.locator('[data-notice-id="Studio-work"]').getByRole('button',{name:'Dismiss',exact:true}).click();await page.getByRole('button',{name:'Undo',exact:true}).waitFor();const sent=events.filter(event=>event.mutation);expect(sent).toHaveLength(3);expect(new Set(sent.map(event=>event.mutation.requestId)).size).toBe(3);expect(sent.map(event=>[event.context,event.recipient])).toEqual([['self','first'],['institution:studio','first'],['institution:studio','second']]);expect(writes).toBe(3);
 }finally{await page.close();}
 },15000);

 test('a changed native event is shown after conflict without replaying a fresh action',async()=>{
 const page=await visit();try{
  const row=current('self').notices[0];row.version=otherVersion;row.body='New event awaiting review';row.revision=0;
  await page.locator('[data-notice-id="Personal-work"]').getByRole('button',{name:'Dismiss',exact:true}).click();await page.locator('.ak-rail-notices').filter({hasText:'New event awaiting review'}).waitFor();expect(writes).toBe(0);expect(events.filter(event=>event.mutation)).toHaveLength(1);expect(await page.locator('.ak-inbox-receipt').textContent()).toContain('This update changed');expect(await page.locator('.ak-notice-actions button:enabled').count()).toBeGreaterThan(0);
  await page.locator('[data-notice-id="Personal-work"]').getByRole('button',{name:'Dismiss',exact:true}).click();await page.getByRole('button',{name:'Undo',exact:true}).waitFor();expect(writes).toBe(1);const requests=events.filter(event=>event.mutation);expect(requests[1].mutation.version).toBe(otherVersion);expect(requests[1].mutation.requestId).not.toBe(requests[0].mutation.requestId);
 }finally{await page.close();}
 },15000);

 test('scope change drops a pending response and preserves the organization work and recipient state',async()=>{
 const page=await visit(),waiting=gate();try{
  holdWrite=waiting;await page.evaluate(()=>{window.fixtureIgnoreAbort=true;});await page.locator('[data-notice-id="Personal-work"]').getByRole('button',{name:'Dismiss',exact:true}).click();await waiting.entered.promise;await page.getByLabel('Activity account or organization').selectOption('institution:studio');await page.locator('.ak-rail-notices').filter({hasText:'Studio document watch'}).waitFor();waiting.release.resolve();await page.evaluate(()=>window.fixtureRail.refresh());expect(await page.locator('.ak-rail-cards').textContent()).not.toContain('Personal');expect(await page.getByRole('button',{name:'Undo',exact:true}).count()).toBe(0);expect(current('institution:studio').notices[0].dismissed).toBe(false);expect(await page.getByLabel('Activity account or organization').inputValue()).toBe('institution:studio');
 }finally{waiting.release.resolve();await page.close();}
 },15000);

 test('account replacement and revoked access discard private work, expansions and notices',async()=>{
 const page=await visit(),waiting=gate();try{
  await page.locator('details[data-work-key="task:Personal-work"] summary').click();holdInbox=waiting;await page.evaluate(()=>{window.fixtureIgnoreAbort=true;window.fixturePending=window.fixtureRail.refresh();});await waiting.entered.promise;recipient='second';current('self').tasks.tasks[0].title='Second account task';current('self').notices[0].body='Second account notice';await page.evaluate(()=>window.fixtureAccount());await page.locator('.ak-rail-notices').filter({hasText:'Second account notice'}).waitFor();waiting.release.resolve();await page.evaluate(()=>window.fixturePending);expect(await page.locator('.ak-rail-cards').textContent()).not.toContain('Personal document watch');expect(await page.locator('details[open]').count()).toBe(0);
  allow=false;await page.evaluate(()=>window.fixtureRail.refresh());expect(await page.locator('.ak-rail-cards').textContent()).not.toContain('Second account');expect(await page.locator('.ak-rail-notices [data-notice-id]').count()).toBe(0);expect(await page.getByRole('link',{name:'Sign in',exact:true}).count()).toBe(1);
 }finally{waiting.release.resolve();await page.close();}
 },15000);

 test('one unavailable owner leaves the other authenticated work available and labels the gap',async()=>{
 const page=await visit();try{taskUnavailable=true;await page.evaluate(()=>window.fixtureRail.refresh());expect(await page.locator('.ak-rail-status').textContent()).toContain('Background work could not be checked');expect(await page.locator('.ak-rail-outputs').textContent()).toContain('Personal retained sources');expect(await page.locator('.ak-rail-notices').textContent()).toContain('Personal document watch');expect(await page.locator('details[data-work-key^="task:"]').count()).toBe(0);}finally{await page.close();}
 },15000);

 test('Queue task, output and agent reads wait for each preceding response',async()=>{
 const page=await visit();try{
  events=[];await page.evaluate(()=>{
   const gate=()=>{let entered,release;return {arrived:new Promise(done=>entered=done),entered:()=>entered(),release:new Promise(done=>release=done),resume:()=>release()};};
   window.fixtureTaskGates={list:gate(),outputs:gate()};window.fixturePending=window.fixtureRail.refresh();
  });
  const operations=()=>events.filter(event=>event.path==='/apps/_activity').map(event=>new URLSearchParams(event.search).get('operation')).filter(op=>['list','outputs','agents'].includes(op));
  await page.evaluate(()=>window.fixtureTaskGates.list.arrived);expect(operations()).toEqual(['list']);
  await page.evaluate(()=>window.fixtureTaskGates.list.resume());await page.evaluate(()=>window.fixtureTaskGates.outputs.arrived);expect(operations()).toEqual(['list','outputs']);
  await page.evaluate(()=>window.fixtureTaskGates.outputs.resume());await page.evaluate(()=>window.fixturePending);expect(operations()).toEqual(['list','outputs','agents']);
 }finally{await page.evaluate(()=>{window.fixtureTaskGates?.list.resume();window.fixtureTaskGates?.outputs.resume();});await page.close();}
 },15000);

 test('available task outputs survive a failed task list',async()=>{
 const page=await visit();try{
  taskUnavailable=true;events=[];await page.evaluate(()=>window.fixtureRail.refresh());
  expect(events.some(event=>event.path==='/apps/_activity'&&new URLSearchParams(event.search).get('operation')==='outputs')).toBe(true);
  expect(await page.locator('.ak-rail-status').textContent()).toContain('Background work could not be checked');
  expect(await page.locator('.ak-rail-outputs').textContent()).toContain('Revision comparison');
  expect(await page.locator('.ak-rail-notices').textContent()).toContain('Personal document watch');
 }finally{await page.close();}
 },15000);

 test('a replayed older receipt does not dismiss or offer Undo for a newer event',async()=>{
 const page=await visit();try{
  loseNext=true;await page.locator('[data-notice-id="Personal-work"]').getByRole('button',{name:'Dismiss',exact:true}).click();await page.getByRole('button',{name:'Check status',exact:true}).waitFor();const row=current('self').notices[0];row.version=otherVersion;row.body='A newer work event';row.read=false;row.dismissed=false;
  await page.getByRole('button',{name:'Check status',exact:true}).click();await page.locator('.ak-rail-notices').filter({hasText:'A newer work event'}).waitFor();expect(await page.getByRole('button',{name:'Undo',exact:true}).count()).toBe(0);expect(await page.getByRole('button',{name:'Check status',exact:true}).count()).toBe(0);expect(await page.locator('[data-notice-id="Personal-work"]').getAttribute('data-dismissed')).toBe('false');expect(writes).toBe(1);const requests=events.filter(event=>event.mutation);expect(requests).toHaveLength(2);expect(requests[0].mutation).toEqual(requests[1].mutation);
 }finally{await page.close();}
 },15000);

 test('unconfirmed recipient action remains recoverable when all subsequent reads fail',async()=>{
 const page=await visit();try{
  loseNext=true;await page.locator('[data-notice-id="Personal-work"]').getByRole('button',{name:'Dismiss',exact:true}).click();await page.getByRole('button',{name:'Check status',exact:true}).waitFor();readsUnavailable=true;await page.evaluate(()=>window.fixtureRail.refresh());expect(await page.getByRole('button',{name:'Check status',exact:true}).count()).toBe(1);expect(await page.locator('.ak-rail-status').textContent()).toContain('Activity could not be refreshed');
  await page.getByRole('button',{name:'Check status',exact:true}).click();await page.getByRole('button',{name:'Check status',exact:true}).waitFor({state:'hidden'});readsUnavailable=false;await page.evaluate(()=>window.fixtureRail.refresh());expect(writes).toBe(1);expect(await page.locator('[data-notice-id="Personal-work"]').count()).toBe(0);expect(current('self').notices[0].dismissed).toBe(true);
 }finally{readsUnavailable=false;await page.close();}
 },15000);

 test('older Inbox pages use the native cursor, refresh in place and reset on destination change',async()=>{
 const data=current('self'),template=clone(data.notices[0]);for(let index=0;index<51;index++)data.notices.push({...template,id:'older-'+index,taskId:'older-'+index,body:'Older update '+index});const page=await visit();try{
  expect(await page.locator('.ak-rail-inbox>header').textContent()).toContain('50 unread updates in these recent pages');await page.getByRole('button',{name:'Earlier updates',exact:true}).click();await page.locator('.ak-rail-notices').filter({hasText:'Older update 50'}).waitFor();expect(events.some(event=>event.path==='/apps/_activity'&&new URLSearchParams(event.search).get('cursor')==='50')).toBe(true);expect(await page.locator('.ak-rail-inbox>header').textContent()).toContain('3 unread updates in these recent pages');
  await page.evaluate(()=>window.fixtureRail.refresh());expect(await page.locator('.ak-rail-notices').textContent()).toContain('Older update 50');await page.getByRole('button',{name:'Recent updates',exact:true}).click();await page.locator('.ak-rail-notices').filter({hasText:'Personal document watch'}).waitFor();await page.getByRole('button',{name:'Earlier updates',exact:true}).click();await page.locator('.ak-rail-notices').filter({hasText:'Older update 50'}).waitFor();await page.getByLabel('Activity account or organization').selectOption('institution:studio');await page.locator('.ak-rail-notices').filter({hasText:'Studio document watch'}).waitFor();expect(await page.locator('[data-inbox-page]').getAttribute('href')).toBe('/inbox?context=institution%3Astudio');expect(await page.getByRole('button',{name:'Recent updates',exact:true}).isVisible()).toBe(false);expect(events.filter(event=>event.path==='/apps/_activity'&&new URLSearchParams(event.search).get('context')==='institution:studio').every(event=>!new URLSearchParams(event.search).has('cursor'))).toBe(true);
 }finally{await page.close();}
 },15000);

 test('sticky section controls make Inbox reachable and expose its unread count on a short viewport',async()=>{
 const page=await visit({viewport:{width:1280,height:720}});try{
  const initial=await noticeGeometry(page,'desktop-default');expect(initial.inboxContainsContent).toBe(true);expect(initial.controlsVisible).toBe(true);expect(initial.noticeViewport).toBeGreaterThanOrEqual(100);expect(initial.rowVisible).toBeGreaterThanOrEqual(100);expect(initial.bodyVisible).toBeGreaterThanOrEqual(48);
  expect(await page.locator('.ak-rail-launch small').textContent()).toContain('2 unread');expect(await page.locator('[data-inbox-page]').getAttribute('href')).toBe('/inbox?context=self');await page.getByRole('navigation',{name:'Activity sections'}).getByRole('button',{name:'Inbox, 2 unread updates',exact:true}).click();const heading=await page.locator('#ak-inbox-heading').boundingBox(),cards=await page.locator('.ak-rail-cards').boundingBox();expect(heading.y).toBeGreaterThanOrEqual(cards.y);expect(heading.y+heading.height).toBeLessThanOrEqual(cards.y+cards.height);expect(await page.locator('#ak-inbox-heading').evaluate(node=>node===document.activeElement)).toBe(true);const focused=await noticeGeometry(page,'desktop-inbox');expect(focused.rowVisible).toBeGreaterThanOrEqual(focused.rowHeight-.5);expect(focused.controlsVisible).toBe(true);if(process.env.BONFYRE_RAIL_SCREENSHOT_DIR)await page.screenshot({path:resolve(process.env.BONFYRE_RAIL_SCREENSHOT_DIR,'rail-inbox.png')});
  await page.getByRole('navigation',{name:'Activity sections'}).getByRole('button',{name:'Activity',exact:true}).click();expect(await page.locator('.ak-rail-cards').evaluate(node=>node.scrollTop)).toBeLessThan(40);
 }finally{await page.close();}
 },15000);

 test('mobile disclosure and Inbox controls stay within the viewport and remain keyboard operable',async()=>{
 const page=await visit({viewport:{width:390,height:844},isMobile:true,hasTouch:true});try{
  const initial=await noticeGeometry(page,'mobile-default');expect(initial.inboxContainsContent).toBe(true);expect(initial.controlsVisible).toBe(true);expect(initial.noticeViewport).toBeGreaterThanOrEqual(100);expect(initial.rowVisible).toBeGreaterThanOrEqual(100);expect(initial.bodyVisible).toBeGreaterThanOrEqual(48);
  if(process.env.BONFYRE_RAIL_SCREENSHOT_DIR)await page.screenshot({path:resolve(process.env.BONFYRE_RAIL_SCREENSHOT_DIR,'rail-mobile.png')});await page.locator('details[data-work-key="task:Personal-work"] summary').tap();expect(await page.locator('details[data-work-key="task:Personal-work"]').evaluate(node=>node.open)).toBe(true);expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);const bounds=await page.locator('.ak-rail-cards').boundingBox();expect(bounds.x).toBeGreaterThanOrEqual(0);expect(bounds.x+bounds.width).toBeLessThanOrEqual(390);await page.locator('[data-inbox-filter=unread]').scrollIntoViewIfNeeded();await page.locator('[data-inbox-filter=unread]').tap();expect(await page.locator('[data-inbox-filter=unread]').getAttribute('aria-pressed')).toBe('true');await page.locator('[data-inbox-filter=unread]').focus();await page.keyboard.press('Escape');expect(await page.locator('.ak-rail-launch').getAttribute('aria-expanded')).toBe('false');expect(await page.locator('.ak-rail-launch').evaluate(node=>node===document.activeElement)).toBe(true);
 }finally{await page.close();}
 },15000);


test('all four native feeds share one actor binding while task and notification owners remain separate',async()=>{
 const page=await visit({},'?combined=1');try{
  expect(await page.locator('.ak-rail-tasks').textContent()).toContain('Fabric model work');
  expect(await page.locator('.ak-rail-runs').textContent()).toContain('Personal document watch');expect(await page.locator('.ak-rail-runs').textContent()).toContain('Personal retained sources');
  expect(await page.locator('.ak-rail-outputs').textContent()).toContain('Fabric result evidence');expect(await page.locator('.ak-rail-outputs').textContent()).toContain('Revision comparison');
  expect(await page.locator('[data-notice-owner=queue]').count()).toBe(2);expect(await page.locator('[data-notice-owner=fabric]').count()).toBe(1);expect(await page.locator('.ak-rail-inbox>header').textContent()).toContain('3 unread');
  expect(events.some(event=>event.path==='/apps/_activity'&&new URLSearchParams(event.search).get('operation')==='list')).toBe(true);expect(events.some(event=>event.path==='/apps/_fabric'&&new URLSearchParams(event.search).get('operation')==='read')).toBe(true);
 }finally{await page.close();}
},15000);

test('Queue authority refusal clears all four feeds before an uncooperative Inbox read settles',async()=>{
 const page=await visit({},'?combined=1'),waiting=gate();try{
  holdInbox=waiting;queueRefused=true;await page.evaluate(()=>{window.fixtureIgnoreAbort=true;window.fixturePending=window.fixtureRail.refresh();});await waiting.entered.promise;
  await page.locator('.ak-rail-status').filter({hasText:'Current access'}).waitFor({state:'attached'});
  expect(await page.locator('.ak-rail-runs details,.ak-rail-tasks a,.ak-rail-outputs a,[data-notice-id]').count()).toBe(0);expect(await page.getByLabel('Activity account or organization').locator('option').count()).toBe(0);
  waiting.release.resolve();await page.evaluate(()=>window.fixturePending);expect(await page.locator('.ak-rail-runs details,.ak-rail-tasks a,.ak-rail-outputs a,[data-notice-id]').count()).toBe(0);
 }finally{waiting.release.resolve();await page.close();}
},15000);

test('a same-organization Inbox actor mismatch cannot mix Queue, research, Fabric or notification rows',async()=>{
 const page=await visit({},'?combined=1');try{
  await page.getByLabel('Activity account or organization').selectOption('institution:studio');await page.locator('.ak-rail-runs').filter({hasText:'Studio document watch'}).waitFor();
  inboxActor='user:8';await page.evaluate(()=>window.fixtureRail.refresh());await page.locator('.ak-rail-status').filter({hasText:'current account could not be confirmed'}).waitFor({state:'attached'});
  expect(await page.locator('.ak-rail-runs details,.ak-rail-tasks a,.ak-rail-outputs a,[data-notice-id]').count()).toBe(0);expect(await page.locator('.ak-rail-cards').textContent()).not.toContain('Studio document watch');expect(await page.locator('.ak-rail-cards').textContent()).not.toContain('Fabric model work');
 }finally{await page.close();}
},15000);


test('unchanged owner polling preserves a pressed notification control and its Undo receipt',async()=>{
 const page=await visit({},'?combined=1');
 const heldRefresh=async control=>{
  await control.scrollIntoViewIfNeeded();await control.hover();await control.evaluate(node=>{window.fixturePressedControl=node;});await page.mouse.down();
  try{
   await page.evaluate(()=>window.fixtureRail.refresh());
   expect(await page.evaluate(()=>window.fixturePressedControl.isConnected)).toBe(true);
   expect(await control.evaluate(node=>node===window.fixturePressedControl)).toBe(true);
   expect(await page.evaluate(()=>document.activeElement===window.fixturePressedControl)).toBe(true);
  }finally{await page.mouse.up();}
 };
 try{
  const notice=page.locator('[data-notice-owner="queue"][data-notice-id="Personal-work"]');
  await heldRefresh(notice.getByRole('button',{name:'Mark read',exact:true}));
  await notice.getByRole('button',{name:'Mark unread',exact:true}).waitFor();expect(writes).toBe(1);expect(current('self').notices[0].read).toBe(true);
  await notice.getByRole('button',{name:'Dismiss',exact:true}).click();await page.getByRole('button',{name:'Undo',exact:true}).waitFor();expect(writes).toBe(2);
  await heldRefresh(page.getByRole('button',{name:'Undo',exact:true}));
  await page.getByRole('button',{name:'Undo',exact:true}).waitFor({state:'hidden'});await notice.waitFor();expect(writes).toBe(3);expect(current('self').notices[0].dismissed).toBe(false);
  expect(await page.locator('[data-notice-owner="fabric"]').count()).toBe(1);expect(events.filter(event=>event.mutation).map(event=>event.mutation.action)).toEqual(['read','dismiss','restore']);
 }finally{await page.mouse.up();await page.close();}
},15000);

test('an identical notice under a new organization actor replaces the pressed old-account control',async()=>{
 const page=await visit({},'?context=institution%3Astudio&combined=1');try{
  const control=page.locator('[data-notice-owner="queue"][data-notice-id="Studio-work"]').getByRole('button',{name:'Mark read',exact:true});
  await control.scrollIntoViewIfNeeded();await control.hover();await control.evaluate(node=>{window.fixturePressedControl=node;});await page.mouse.down();
  recipient='second';await page.evaluate(()=>window.fixtureRail.refresh());
  expect(await page.evaluate(()=>window.fixturePressedControl.isConnected)).toBe(false);expect(await control.evaluate(node=>node===window.fixturePressedControl)).toBe(false);
  await page.mouse.up();expect(writes).toBe(0);expect(events.filter(event=>event.mutation)).toHaveLength(0);expect(current('institution:studio').notices[0].read).toBe(false);
 }finally{await page.mouse.up();await page.close();}
},15000);

function providerFixture(context='self'){
 const research=current(context).research;research.providerContinuation={canContinue:true,provider:{id:'exa',name:'Exa'}};return clone(research.activities[0]);
}
async function openProvider(page,title='Personal retained sources'){
 await page.getByRole('button',{name:'Details for '+title,exact:true}).click();await page.locator('.ak-rail-connection').waitFor();
}

test('the provider decision docks below the workspace with reachable controls at desktop and mobile widths',async()=>{
 const work=providerFixture(),page=await visit({viewport:{width:1440,height:900}});try{
  await openProvider(page);const review=await page.evaluate(()=>({destination:window.fixtureProviderReview.destinationLabel,title:window.fixtureProviderReview.workTitle,href:window.fixtureProviderReview.returnHref,id:window.fixtureProviderReview.jobId}));
  expect(review).toEqual({destination:'Your account',title:work.title,href:work.href,id:work.id});
  const card=page.locator('.ak-rail-connection'),before=await card.boundingBox(),rail=await page.locator('.ak-rail-cards').boundingBox();
  expect(before.width).toBe(680);expect(before.x+before.width/2).toBe((1440-390)/2);expect(before.y+before.height).toBe(876);expect(before.x+before.width).toBeLessThan(rail.x);expect(await page.locator('.ak-rail-cards').isVisible()).toBe(true);
  const expand=page.getByRole('button',{name:'Expand sign-in',exact:true});await expand.click();await page.evaluate(()=>window.fixtureRail.refresh());expect(await page.evaluate(()=>window.fixtureProviderMounts)).toBe(1);expect(await expand.getAttribute('aria-expanded')).toBe('true');expect(await expand.evaluate(node=>node===document.activeElement)).toBe(true);expect(await card.getAttribute('data-expanded')).toBe('true');
  for(const [width,height]of [[1024,780],[390,844],[320,640]]){
   await page.setViewportSize({width,height});const bounds=await card.boundingBox();expect(bounds.x).toBeGreaterThanOrEqual(0);expect(bounds.x+bounds.width).toBeLessThanOrEqual(width);expect(bounds.y).toBeGreaterThanOrEqual(0);expect(bounds.y+bounds.height).toBeLessThanOrEqual(height);expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);
   expect(await page.locator('.ak-rail-cards').isVisible()).toBe(false);expect(await page.locator('.ak-rail-launch').isVisible()).toBe(true);const dismiss=page.getByRole('button',{name:'Not now',exact:true});await dismiss.scrollIntoViewIfNeeded();expect((await dismiss.boundingBox()).height).toBeGreaterThanOrEqual(44);
  }
  await page.getByRole('button',{name:'Not now',exact:true}).click();expect(await card.isVisible()).toBe(false);expect(await page.locator('.ak-rail-cards').isVisible()).toBe(true);expect(current('self').research.activities[0]).toEqual(work);expect(writes).toBe(0);
 }finally{await page.close();}
},15000);

test('returning from provider takeover rereads the same work and current binding before restoring its card',async()=>{
 const work=providerFixture(),page=await visit(),waiting=gate();try{
  await openProvider(page);await page.getByRole('button',{name:'Expand sign-in',exact:true}).click();await page.getByRole('button',{name:'Take over to sign in',exact:true}).click();expect(await page.locator('.ak-rail-provider').count()).toBe(0);expect(await page.locator('.ak-work-rail').textContent()).not.toContain(work.title);
  holdResearch=waiting;await page.evaluate(()=>window.fixtureVisibility(false));await waiting.entered.promise;expect(await page.locator('.ak-rail-provider').count()).toBe(0);expect(await page.evaluate(()=>window.fixtureProviderMounts)).toBe(1);
  waiting.release.resolve();await page.waitForFunction(()=>window.fixtureProviderMounts===2);expect(await page.locator('.ak-rail-connection').isVisible()).toBe(true);expect(await page.getByRole('button',{name:'Expand sign-in',exact:true}).getAttribute('aria-expanded')).toBe('true');
  expect(await page.evaluate(()=>window.fixtureProviderOpens)).toBe(1);expect(await page.evaluate(()=>window.fixtureProviderReview.binding)).toEqual(binding('self'));expect(await page.evaluate(()=>window.fixtureProviderReview.jobId)).toBe(work.id);
  expect(events.filter(event=>event.path==='/fixture/research'&&new URLSearchParams(event.search).get('job')===work.id)).toHaveLength(2);expect(writes).toBe(0);
 }finally{waiting.release.resolve();await page.close();}
},15000);

test('a new actor in the same organization cannot inherit a suspended provider card',async()=>{
 providerFixture('institution:studio');const page=await visit({},'?context=institution%3Astudio'),waiting=gate();try{
  await openProvider(page,'Studio retained sources');await page.getByRole('button',{name:'Take over to sign in',exact:true}).click();recipient='second';providerFixture('institution:studio');holdResearch=waiting;
  await page.evaluate(()=>window.fixtureVisibility(false));await waiting.entered.promise;expect(await page.locator('.ak-rail-provider').count()).toBe(0);
  waiting.release.resolve();await page.locator('.ak-rail-status').filter({hasText:'Current account activity'}).waitFor({state:'attached'});await page.evaluate(()=>window.fixtureRail.refresh());expect(await page.evaluate(()=>window.fixtureProviderMounts)).toBe(1);expect(await page.locator('.ak-rail-provider').count()).toBe(0);
  await openProvider(page,'Studio retained sources');expect(await page.evaluate(()=>window.fixtureProviderReview.binding.actor)).toBe('user:8');expect(await page.evaluate(()=>window.fixtureProviderMounts)).toBe(2);expect(await page.evaluate(()=>window.fixtureProviderOpens)).toBe(1);
 }finally{waiting.release.resolve();await page.close();}
},15000);

test('changing scope while a suspended card is being read discards its late result',async()=>{
 providerFixture();const page=await visit(),waiting=gate();try{
  await openProvider(page);await page.getByRole('button',{name:'Take over to sign in',exact:true}).click();holdResearch=waiting;await page.evaluate(()=>{window.fixtureIgnoreAbort=true;window.fixtureVisibility(false);});await waiting.entered.promise;
  await page.getByLabel('Activity account or organization').selectOption('institution:studio');await page.locator('.ak-rail-runs').filter({hasText:'Studio document watch'}).waitFor();waiting.release.resolve();await page.evaluate(()=>window.fixtureRail.refresh());expect(await page.locator('.ak-rail-provider').count()).toBe(0);expect(await page.evaluate(()=>window.fixtureProviderMounts)).toBe(1);expect(await page.locator('.ak-work-rail').textContent()).not.toContain('Personal retained sources');expect(await page.getByLabel('Activity account or organization').inputValue()).toBe('institution:studio');
 }finally{waiting.release.resolve();await page.close();}
},15000);

test('Escape closes expansion, then the card, then the rail; dismissal never cancels or restores work',async()=>{
 const work=providerFixture(),page=await visit();try{
  await openProvider(page);const expand=page.getByRole('button',{name:'Expand sign-in',exact:true});await expand.click();await page.keyboard.press('Escape');expect(await expand.getAttribute('aria-expanded')).toBe('false');expect(await page.locator('.ak-rail-connection').isVisible()).toBe(true);expect(await page.locator('.ak-rail-launch').getAttribute('aria-expanded')).toBe('true');
  current('self').tasks.tasks[0].updated++;await page.evaluate(()=>window.fixtureRail.refresh());await page.keyboard.press('Escape');expect(await page.locator('.ak-rail-provider').count()).toBe(0);expect(await page.evaluate(()=>document.activeElement.dataset.focusKey)).toBe('inspect:research:'+work.id);expect(await page.locator('.ak-rail-launch').getAttribute('aria-expanded')).toBe('true');
  await page.keyboard.press('Escape');expect(await page.locator('.ak-rail-launch').getAttribute('aria-expanded')).toBe('false');await page.locator('.ak-rail-launch').click();await openProvider(page);await page.getByRole('button',{name:'Not now',exact:true}).click();expect(await page.locator('.ak-rail-provider').count()).toBe(0);
  await page.evaluate(()=>{window.fixtureVisibility(true);window.fixtureVisibility(false);});await page.evaluate(()=>window.fixtureRail.refresh());expect(await page.evaluate(()=>window.fixtureProviderMounts)).toBe(2);expect(await page.locator('.ak-rail-provider').count()).toBe(0);expect(current('self').research.activities[0]).toEqual(work);expect(writes).toBe(0);
 }finally{await page.close();}
},15000);
test('the Agents lane shows each recorded run, its limit flag and only reported usage, per destination',async()=>{const page=await visit();try{
 const lane=page.locator('.ak-rail-agents');expect(await page.locator('.ak-rail-agent-heading+p').textContent()).toBe('1 running · 1 needs you');expect(await lane.locator('li').nth(0).textContent()).toContain('Codex · Peer Critique · Duel · Running 2 min');expect(await lane.locator('li').nth(0).textContent()).not.toContain('input');const failed=await lane.locator('li').nth(1).textContent();expect(failed).toContain('Claude · Convergence Arbiter · Duel · Needs attention 8 s');expect(failed).toContain('1,200 input · 300 output · 900 cached');expect(failed).not.toContain('$');expect(failed).toContain('Provider limit reached');await page.getByRole('button',{name:'Agents',exact:true}).click();expect(await page.evaluate(()=>document.activeElement.textContent)).toBe('Agents');expect(await lane.locator('a').nth(1).getAttribute('href')).toBe('/work-board?context=self&task=account-fabric-duel');await page.locator('.ak-rail-scope select').selectOption('institution:studio');await lane.filter({hasText:'No agent runs in this destination.'}).waitFor();expect(await lane.textContent()).not.toContain('Personal duel review');
}finally{await page.close();}});

test('Compute renders measured usage without pricing and clears it on organization switch',async()=>{
 const page=await visit();try{
  const usage=page.locator('.ak-rail-compute'),note=page.locator('.ak-rail-compute-note');
  await usage.getByText('Claude',{exact:true}).waitFor();
  expect(await note.textContent()).toContain('1,500 additive tokens');
  expect(await note.textContent()).toContain('5 records');
  expect(await usage.locator('li').count()).toBe(4);
  expect(await usage.textContent()).toContain('1 provider-limited run to review');
  expect(await usage.getByRole('link',{name:'Personal duel review · Claude'}).getAttribute('href')).toBe('/work-board?context=self&task=account-fabric-duel');
  expect(await note.textContent()).toContain('1,000 cached input');
  expect(await usage.textContent()).toContain('300 inherited snapshot tokens excluded');
  expect(await usage.textContent()).toContain('Not a bill');
  expect(await usage.textContent()).not.toContain('$');
  await page.getByRole('button',{name:'Compute',exact:true}).click();
  await page.getByLabel('Activity account or organization').selectOption('institution:studio');
  expect(await usage.isVisible()).toBe(false);
  expect(await page.locator('.ak-rail-compute-heading').isVisible()).toBe(false);
  await page.getByLabel('Activity account or organization').selectOption('self');
  await usage.getByText('Claude',{exact:true}).waitFor();
  expect(await note.textContent()).toContain('1,500 additive tokens');
 }finally{await page.close();}
},15000);
