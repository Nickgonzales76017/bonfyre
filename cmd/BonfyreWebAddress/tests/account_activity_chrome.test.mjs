// Real installed Chrome and native Queue, in throwaway browser/SQLite stores.
// Identity and workspace grants are fixture-controlled, not a public Auth witness.
import {test,expect} from 'bun:test';
import {mkdtempSync,readFileSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,resolve} from 'node:path';
import {withChrome} from '../../../integrations/browser/capture/chrome-session.mjs';
import {encode,decode} from '../../../integrations/browser/workspace-msgpack.mjs';
const root=resolve(import.meta.dir,'../../..');
const binary=process.env.BONFYRE_TEST_QUEUE||join(root,'cmd/BonfyreQueue/bonfyre-queue');
const deferred=()=>{let resolve;const promise=new Promise(done=>resolve=done);return {promise,resolve};};
const files=new Set(['aurekai-activity.mjs','aurekai-activity-source.mjs','aurekai-inbox-source.mjs','aurekai-activity.css','workspace-msgpack.mjs','aurekai-push.mjs']);
async function exercise(work,{mobile=false}={}){
 const directory=mkdtempSync(join(tmpdir(),'bonfyre-rail-chrome-')),db=join(directory,'work.db');
 const state={actor:'user:7',allowed:true,lose:false,gate:null,beforeSet:null,mutations:[]};let server;
 const native=(args,body)=>{
  const result=Bun.spawnSync([binary,...args,'--db',db],{stdin:body??new Uint8Array(),stdout:'pipe',stderr:'pipe'});
  if(result.exitCode)throw Error('Native Queue refused fixture: '+result.stderr.toString());return new Uint8Array(result.stdout);
 };
 const enqueue=(id,subject,scope,title)=>native(['enqueue','fixture','private-input-never-display','--mission',id,'--subject',subject,'--scope',scope,'--title',title,'--continuation','/apps?context='+(scope==='self'?'self':subject)]);
 const complete=id=>{
  const claim=JSON.parse(new TextDecoder().decode(native(['claim',id,'job','chrome-fixture'])));
  native(['ack',id,'job','chrome-fixture',claim.claim_token,'--output-uri','file:///private/never-a-browser-link']);
 };
 try{
  enqueue('account-own','user:7','self','Personal document review');enqueue('account-later','user:7','self','Personal follow-up');
  enqueue('account-other','user:8','self','Other account secret');enqueue('account-org','institution:acme','institution','Acme document review');
  const initialClaim=JSON.parse(new TextDecoder().decode(native(['claim','account-own','job','chrome-fixture'])));
  const completeOwn=()=>native(['ack','account-own','job','chrome-fixture',initialClaim.claim_token,'--output-uri','file:///private/never-a-browser-link']);
  const wire=bytes=>new Response(bytes,{headers:{'Content-Type':'application/msgpack','Cache-Control':'no-store'}});
  server=Bun.serve({hostname:'127.0.0.1',port:0,async fetch(request){
   const url=new URL(request.url);
   if(url.pathname==='/fixture')return new Response(`<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><link rel="stylesheet" href="/apps/aurekai-activity.css"></head><body><header><span data-fixture-account-slot><span class="ak-session" data-session-state="signed-in">Account</span></span></header><main><h1>Native activity fixture</h1></main><script type="module">
import {mountAurekaiActivity} from '/apps/aurekai-activity.mjs';
const slot=document.querySelector('[data-fixture-account-slot]');slot.dataset.accountSlot='';window.rail=mountAurekaiActivity({activity:null,intervalMs:3600000});
window.accountChanged=()=>{const node=document.createElement('span');node.className='ak-session';node.dataset.sessionState='signed-in';node.textContent='Changed account';slot.replaceChildren(node);};
window.visibility=hidden=>{Object.defineProperty(document,'hidden',{configurable:true,value:hidden});document.dispatchEvent(new Event('visibilitychange'));};
</script></body></html>`,{headers:{'Content-Type':'text/html'}});
   if(url.pathname.startsWith('/apps/')&&files.has(url.pathname.slice(6))){const name=url.pathname.slice(6);return new Response(readFileSync(join(process.env.BONFYRE_TEST_PUBLIC_BROWSER||join(root,'integrations/browser'),name)),{headers:{'Content-Type':name.endsWith('.css')?'text/css':'text/javascript'}});}
   if(!state.allowed)return new Response('Fixture grant revoked',{status:403});
   if(url.pathname==='/apps/_destinations')return wire(encode([1,[['self','Your account',true],['institution:acme','Acme',true]]]));
   if(url.pathname!=='/apps/_activity')return new Response('Not found',{status:404});
   const context=url.searchParams.get('context'),operation=url.searchParams.get('operation');
   if(context!=='self'&&context!=='institution:acme')return new Response('Fixture scope refused',{status:403});
   const actor=state.actor,scope=context==='self'?'self':'institution',subject=context==='self'?actor:context;
   const binding={actor,subject,scope};
   // Match account_activity_web.h: the HTTP owner adds the fresh session binding.
   const bound=bytes=>{
    const packet=decode(bytes);
    if(!Array.isArray(packet)||packet.length!==3||packet[0]!==1||typeof packet[1]!=='boolean'||!packet[2]||Array.isArray(packet[2])||typeof packet[2]!=='object')return new Response('Invalid native fixture envelope',{status:503});
    if(!packet[1])return wire(bytes);
    if(Object.hasOwn(packet[2],'binding')||Object.hasOwn(packet[2],'source'))return new Response('Colliding native fixture envelope',{status:503});
    return wire(encode([1,true,{...packet[2],binding,source:'account-activity'}]));
   };
   const audience=['--subject',subject,'--scope',scope];
   if(operation==='list'&&request.method==='GET')return bound(native(['activity',...audience]));
   if(operation==='inbox'&&request.method==='GET')return bound(native(['account-inbox','list',...audience,'--actor',actor]));
   if(operation==='inbox-set'&&request.method==='POST'){
    if(request.headers.get('origin')!==server.url.origin||request.headers.get('content-type')!=='application/msgpack')return new Response('Invalid fixture origin',{status:403});
    const bytes=new Uint8Array(await request.arrayBuffer());
    const exactKeys=(value,keys)=>value&&typeof value==='object'&&!Array.isArray(value)&&Object.keys(value).length===keys.length&&keys.every(key=>Object.hasOwn(value,key));
    let packet;try{packet=decode(bytes,{maxBytes:4096});}catch{return new Response('Invalid fixture change packet',{status:400});}
    if(!Array.isArray(packet)||packet.length!==2||packet[0]!==1||!exactKeys(packet[1],['id','version','revision','action','requestId','expectedBinding']))return new Response('Invalid fixture change packet',{status:400});
    const input=packet[1],expected=input.expectedBinding,atom=(value,min,max)=>typeof value==='string'&&value.length>=min&&value.length<=max&&/^[A-Za-z0-9._:-]+$/.test(value);
    if(!atom(input.id,1,127)||typeof input.version!=='string'||!/^[0-9a-f]{64}$/.test(input.version)||!Number.isSafeInteger(input.revision)||input.revision<0||input.revision>=Number.MAX_SAFE_INTEGER||!['read','unread','dismiss','restore'].includes(input.action)||!atom(input.requestId,8,96)||!exactKeys(expected,['actor','subject','scope'])||Object.values(expected).some(value=>typeof value!=='string'||!value.length||value.length>=160||value.includes('\0')))return new Response('Invalid fixture change packet',{status:400});
    if(Object.keys(binding).some(key=>expected[key]!==binding[key]))return new Response('The fixture account changed',{status:409});
    state.mutations.push({context,actor,input});
    const beforeSet=state.beforeSet;state.beforeSet=null;if(beforeSet)beforeSet();
    const {expectedBinding,...change}=input;
    const reply=native(['account-inbox','set',...audience,'--actor',actor],encode([1,change])),gate=state.gate;
    state.mutations.at(-1).result=decode(reply);
    if(gate){state.gate=null;gate.entered.resolve();await gate.release.promise;}
    if(state.lose){state.lose=false;return new Response('Lost native acknowledgement',{status:503});}return bound(reply);
   }
   return new Response('Unsupported fixture operation',{status:405});
  }});
  await withChrome(async({open})=>{
   const tab=await open();
   if(mobile)await tab.page('Emulation.setDeviceMetricsOverride',{width:390,height:844,deviceScaleFactor:1,mobile:true});
   const run=async expression=>{const result=await tab.page('Runtime.evaluate',{expression,awaitPromise:true,returnByValue:true});if(result.exceptionDetails)throw Error(result.exceptionDetails.exception?.description||result.exceptionDetails.text);return result.result.value;};
   // The host owns the deadline, rather than a throttled page timer.
   const wait=async expression=>{
    const deadline=Date.now()+8000;
    while(Date.now()<deadline){if(await run(`(()=>{try{return Boolean(${expression});}catch(error){if(error.name!=='TypeError')throw error;return false;}})()`))return true;await Bun.sleep(20);}
    throw Error('Activity condition not reached: '+expression+'; mutations='+state.mutations.length);
   };
   const click=async selector=>{
    const point=await run(`(()=>{const el=document.querySelector(${JSON.stringify(selector)});if(!el)throw Error('Missing control');el.scrollIntoView({block:'center'});const r=el.getBoundingClientRect();return {x:r.x+r.width/2,y:r.y+r.height/2};})()`);
    await tab.page('Input.dispatchMouseEvent',{type:'mouseMoved',...point});
    await tab.page('Input.dispatchMouseEvent',{type:'mousePressed',...point,button:'left',clickCount:1});await tab.page('Input.dispatchMouseEvent',{type:'mouseReleased',...point,button:'left',clickCount:1});
   };
   const ready=async()=>{await wait(`window.rail&&document.querySelector('.ak-rail-status').textContent==='Current account activity'`);if(await run(`document.querySelector('.ak-rail-launch').getAttribute('aria-expanded')==='false'`))await click('.ak-rail-launch');await wait(`document.querySelector('[data-notice-id]')&&!document.querySelector('[data-aurekai-activity]').hasAttribute('aria-busy')`);};
   const refresh=()=>run('window.rail.refresh()');
   await tab.go(server.url+'fixture');await ready();await work({tab,run,wait,click,refresh,ready,state,native,complete,completeOwn,url:server.url+'fixture'});
  },{timeoutMs:60000,window:mobile?'390,844':'1280,720'});
 }finally{state.gate?.release.resolve();server?.stop(true);rmSync(directory,{recursive:true,force:true});}
}
const notice='[data-notice-id="account-own"]';
const summary='details[data-work-key="task:account-own"] summary';
const readButton=notice+' [data-focus-key="notice-read:account-own"]';
const dismissButton=notice+' [data-focus-key="notice-dismiss:account-own"]';
const controlText=(selector)=>`document.querySelector(${JSON.stringify(selector)})?.textContent`;

test('native Chrome: scoped tasks, real completion/output and focus survive refresh',()=>exercise(async({run,wait,click,refresh,completeOwn})=>{
 expect(await run(`document.querySelector('.ak-rail-cards').textContent`)).not.toContain('Other account secret');
 expect(await run(`getComputedStyle(document.querySelector('.ak-rail-output-heading')).display`)).not.toBe('none');
 expect(await run(`getComputedStyle(document.querySelector('.ak-rail-outputs')).display`)).not.toBe('none');
 expect(await run(`document.querySelector('.ak-rail-outputs').textContent`)).toMatch(/No retained outputs|Outputs could not be checked/);
 await click(summary);expect(await run(`document.querySelector('details[open] .ak-work-children').textContent`)).toContain('Queue Fixture');
 await run(`document.querySelector('[data-focus-key="continue:task:account-own"]').focus()`);await refresh();
 expect(await run(`document.activeElement.dataset.focusKey`)).toBe('continue:task:account-own');
 completeOwn();await refresh();await wait(`document.querySelector('details[data-work-key="task:account-own"]').textContent.includes('Completed')`);
 expect(await run(`document.querySelector('details[data-work-key="task:account-own"]').open`)).toBe(true);
 const text=await run(`document.querySelector('.ak-rail-cards').textContent`);expect(text).toContain('Recorded output reference');expect(text).not.toContain('private-input');
 expect(await run(`document.querySelectorAll('a[href^="file:"]').length`)).toBe(0);
}),70000);

test('native Chrome: recipient marks persist after document reload and restore without completing work',()=>exercise(async({tab,url,ready,run,wait,click,native})=>{
 await click(readButton);await wait(`${controlText(readButton)}==='Mark unread'`);await click(dismissButton);await wait(`document.querySelector('[data-focus-key="notice-undo"]')`);
 expect(await run(`document.querySelector(${JSON.stringify(notice)})===null`)).toBe(true);
 await tab.go(url);await ready();expect(await run(`document.querySelector(${JSON.stringify(notice)})===null`)).toBe(true);
 await click('[data-inbox-filter="dismissed"]');await click(dismissButton);await wait(`document.querySelector('.ak-inbox-receipt').hidden`);await click('[data-inbox-filter="all"]');
 expect(await run(controlText(readButton))).toBe('Mark unread');
 expect(decode(native(['activity','--subject','user:7','--scope','self']))[2].tasks.find(row=>row.id==='account-own').state).toBe('running');
 expect(await run('localStorage.length')).toBe(0);
}),70000);

test('native Chrome: a dropped acknowledgement retries only its committed native request',()=>exercise(async({run,wait,click,refresh,state})=>{
 state.lose=true;await click(dismissButton);await wait(`document.querySelector('[data-focus-key="notice-check"]')`);
 expect(await run(`document.querySelectorAll('.ak-notice-actions button:enabled').length`)).toBe(0);await refresh();
 await click('[data-focus-key="notice-check"]');await wait(`document.querySelector('[data-focus-key="notice-undo"]')`);
 expect(state.mutations.length).toBe(2);expect(state.mutations[1].input).toEqual(state.mutations[0].input);
 expect(await run(`document.querySelector(${JSON.stringify(notice)})===null`)).toBe(true);
}),70000);

test('native Chrome: a new task version refuses the stale action and exposes the current notice',()=>exercise(async({run,wait,click,state,completeOwn})=>{
 // Advance the real task after the browser submits its reviewed version.
 // Polling cannot turn the intended stale action into a fresh mutation.
 state.beforeSet=completeOwn;await click(dismissButton);await wait(`document.querySelector('.ak-inbox-receipt').textContent.includes('This update changed')&&!document.querySelector('[data-aurekai-activity]').hasAttribute('aria-busy')`);
 expect(state.mutations.length).toBe(1);expect(await run(`document.querySelector(${JSON.stringify(notice)}).dataset.dismissed`)).toBe('false');
 expect(state.mutations[0].result[1]).toBe(false);expect(state.mutations[0].result[2].code).toBe('version_conflict');
 await click(dismissButton);await wait(`document.querySelector('[data-focus-key="notice-undo"]')`);expect(state.mutations.length).toBe(2);
 expect(state.mutations[1].input.version).not.toBe(state.mutations[0].input.version);expect(state.mutations[1].input.requestId).not.toBe(state.mutations[0].input.requestId);
}),70000);

test('native Chrome: hidden-page recovery retains only the unresolved operation and ignores a late reply',()=>exercise(async({run,wait,click,state,ready})=>{
 const gate={entered:deferred(),release:deferred()};state.gate=gate;
 try{
  await click(dismissButton);await gate.entered.promise;const first=structuredClone(state.mutations[0].input);await run('window.visibility(true)');
  expect(await run(`document.querySelector('.ak-rail-cards').textContent`)).not.toContain('Personal');await run('window.visibility(false)');await ready();
  await wait(`document.querySelector('[data-focus-key="notice-check"]')`);gate.release.resolve();await click('[data-focus-key="notice-check"]');
  await wait(`document.querySelector('[data-focus-key="notice-undo"]')`);expect(state.mutations.length).toBe(2);expect(state.mutations[1].input).toEqual(first);
 }finally{gate.release.resolve();}
}),70000);

test('native Chrome: scope/account changes and revocation clear private work and stale recovery',()=>exercise(async({run,wait,click,refresh,state})=>{
 state.lose=true;await click(dismissButton);await wait(`document.querySelector('[data-focus-key="notice-check"]')`);
 await run(`(()=>{const select=document.querySelector('.ak-rail-scope select');select.value='institution:acme';select.dispatchEvent(new Event('change'));})()`);
 await wait(`document.querySelector('.ak-rail-runs').textContent.includes('Acme document review')`);
 expect(await run(`document.querySelector('.ak-rail-cards').textContent`)).not.toContain('Personal');expect(await run(`document.querySelector('[data-focus-key="notice-check"]')===null`)).toBe(true);
 state.actor='user:8';await run('window.accountChanged()');await wait(`document.querySelector('.ak-rail-runs').textContent.includes('Other account secret')`);
 expect(await run(`document.querySelector('.ak-rail-cards').textContent`)).not.toContain('Personal');state.allowed=false;await refresh();
 expect(await run(`document.querySelector('.ak-rail-cards').textContent`)).not.toContain('Other account secret');expect(await run(`document.querySelectorAll('[data-notice-id]').length`)).toBe(0);
}),70000);

test('native Chrome: mobile drawer, filters, section navigation and Escape remain usable',()=>exercise(async({tab,run,click})=>{
 await click(summary);await click('[data-filter="running"]');expect(await run(`document.querySelectorAll('.ak-rail-runs>li').length`)).toBe(1);
 await click('.ak-rail-sections button:last-child');expect(await run(`document.activeElement.id`)).toBe('ak-inbox-heading');
 expect(await run(`(()=>{const r=document.querySelector('.ak-rail-cards').getBoundingClientRect();return r.x>=0&&r.right<=innerWidth&&document.documentElement.scrollWidth<=innerWidth;})()`)).toBe(true);
 await run(`document.querySelector('[data-inbox-filter="unread"]').focus()`);await tab.page('Input.dispatchKeyEvent',{type:'keyDown',key:'Escape',code:'Escape',windowsVirtualKeyCode:27});
 expect(await run(`document.querySelector('.ak-rail-launch').getAttribute('aria-expanded')`)).toBe('false');expect(await run(`document.activeElement.className`)).toBe('ak-rail-launch');
},{mobile:true}),70000);

test('native Chrome: unchanged polling preserves pressed Inbox and recovery controls',()=>exercise(async({tab,run,wait,click,refresh,state})=>{
 await run(`(()=>{const node=document.querySelector(${JSON.stringify(readButton)});node.scrollIntoView({block:'center'});window.pressedNotice=node;})()`);
 const point=await run(`(()=>{const r=window.pressedNotice.getBoundingClientRect();return {x:r.x+r.width/2,y:r.y+r.height/2};})()`);
 await tab.page('Input.dispatchMouseEvent',{type:'mouseMoved',...point});
 await tab.page('Input.dispatchMouseEvent',{type:'mousePressed',...point,button:'left',clickCount:1});
 await refresh();
 expect(await run(`window.pressedNotice===document.querySelector(${JSON.stringify(readButton)})&&window.pressedNotice.isConnected`)).toBe(true);
 await tab.page('Input.dispatchMouseEvent',{type:'mouseReleased',...point,button:'left',clickCount:1});
 await wait(`${controlText(readButton)}==='Mark unread'`);
 state.lose=true;await click(dismissButton);await wait(`document.querySelector('[data-focus-key="notice-check"]')`);
 await run(`window.savedRecovery=document.querySelector('[data-focus-key="notice-check"]')`);
 await refresh();
 expect(await run(`window.savedRecovery===document.querySelector('[data-focus-key="notice-check"]')&&window.savedRecovery.isConnected`)).toBe(true);
 await click('[data-focus-key="notice-check"]');await wait(`document.querySelector('[data-focus-key="notice-undo"]')`);
 expect(state.mutations).toHaveLength(3);
 expect(state.mutations[1].input).toEqual(state.mutations[2].input);
}),70000);
