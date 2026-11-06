#!/usr/bin/env bun
// Runs only an isolated loopback site/profile. No extension installation or everyday profile use.
import {existsSync,mkdtempSync,readFileSync,writeFileSync,copyFileSync,mkdirSync,rmSync} from 'node:fs';
import {tmpdir,homedir} from 'node:os';
import {join,resolve,extname} from 'node:path';
import {createHash} from 'node:crypto';
import {spawnSync} from 'node:child_process';
const root=resolve(process.argv[2]||join(import.meta.dir,'..','..','extension'));
const exe=process.env.CHROME_BINARY||'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome';
if(!existsSync(exe))throw Error('Chrome executable required');
const profile=mkdtempSync(join(tmpdir(),'aurekai-commerce-browser-'));
const mime={'.html':'text/html','.mjs':'text/javascript','.js':'text/javascript','.json':'application/json','.css':'text/css'};
const fixture=revision=>`<!doctype html><html data-fixture="true"><meta charset="utf-8"><title>Fixture Pantry</title><h1>Fixture only</h1><p>Monthly shipment; shipping quoted separately.</p><p>Cancel before ${revision===1?'Tuesday':'Thursday'}.</p><form><input value="NEVER_CAPTURE_SECRET"><input type="password" value="NEVER_CAPTURE_PASSWORD"></form><script type="application/ld+json">${JSON.stringify({'@context':'https://schema.org','@type':'Product',name:'Fixture Pantry',sku:'TEST',offers:{'@type':'Offer',price:revision===1?'45.00':'55.00',priceCurrency:'USD'}})}</script></html>`;
const server=Bun.serve({hostname:'127.0.0.1',port:0,async fetch(req){const u=new URL(req.url);if(u.pathname==='/fixture')return new Response(fixture(Number(u.searchParams.get('revision')||1)),{headers:{'content-type':'text/html'}});const p=resolve(root,'.'+decodeURIComponent(u.pathname==='/'?'/workbench.html':u.pathname));if(!p.startsWith(root+'/'))return new Response('missing',{status:404});const f=Bun.file(p);return await f.exists()?new Response(f,{headers:{'content-type':mime[extname(p)]||'application/octet-stream','cache-control':'no-store'}}):new Response('missing',{status:404})}});
const base=`http://127.0.0.1:${server.port}`;
let proc=null,ws=null,seq=0;const pending=new Map(),errors=[],checks=[];const delay=ms=>new Promise(r=>setTimeout(r,ms));
function check(name,ok,detail){if(!ok)throw Error(name+' failed: '+JSON.stringify(detail));checks.push({name,passed:true,...(detail?{detail}:{})});}
function rpc(method,params={},sessionId){return new Promise((resolve,reject)=>{const id=++seq,timer=setTimeout(()=>{pending.delete(id);reject(Error(method+' timeout'))},12000);pending.set(id,{resolve,reject,timer});ws.send(JSON.stringify({id,method,params,...(sessionId?{sessionId}:{})}));});}
async function start(){const pf=join(profile,'DevToolsActivePort');rmSync(pf,{force:true});proc=Bun.spawn([exe,'--headless=new','--no-first-run','--no-default-browser-check','--disable-background-networking','--disable-component-update','--disable-sync','--remote-debugging-address=127.0.0.1','--remote-debugging-port=0','--user-data-dir='+profile,'about:blank'],{stdout:'ignore',stderr:'ignore'});
 const deadline=Date.now()+15000;while(!existsSync(pf)&&Date.now()<deadline){if(proc.exitCode!==null)throw Error('Chrome exited');await delay(50)}if(!existsSync(pf))throw Error('No DevTools endpoint');
 const [port,path]=readFileSync(pf,'utf8').trim().split('\n');ws=new WebSocket('ws://127.0.0.1:'+port+path);
 await new Promise((yes,no)=>{const t=setTimeout(()=>no(Error('socket timeout')),5000);ws.onopen=()=>{clearTimeout(t);yes()};ws.onerror=()=>{clearTimeout(t);no(Error('socket failed'))}});
 ws.onmessage=e=>{const r=JSON.parse(e.data);if(r.method==='Runtime.exceptionThrown')errors.push(r.params.exceptionDetails);const p=pending.get(r.id);if(!p)return;pending.delete(r.id);clearTimeout(p.timer);r.error?p.reject(Error(JSON.stringify(r.error))):p.resolve(r.result)};
 return await rpc('Browser.getVersion');}
async function stop(){if(ws?.readyState===WebSocket.OPEN){try{await rpc('Browser.close')}catch{}ws.close()}if(proc){const ended=await Promise.race([proc.exited.then(()=>true),delay(3000).then(()=>false)]);if(!ended){proc.kill();await Promise.race([proc.exited,delay(1000)])}}ws=null;proc=null;for(const p of pending.values()){clearTimeout(p.timer);p.reject(Error('closed'))}pending.clear();}
async function page(path){const t=await rpc('Target.createTarget',{url:base+path});const {sessionId:s}=await rpc('Target.attachToTarget',{targetId:t.targetId,flatten:true});await rpc('Runtime.enable',{},s);await wait(s,'document.readyState==="complete"');return s;}
async function run(s,expression){const r=await rpc('Runtime.evaluate',{expression,awaitPromise:true,returnByValue:true},s);if(r.exceptionDetails)throw Error(JSON.stringify(r.exceptionDetails));return r.result.value;}
async function wait(s,expression){const until=Date.now()+12000;while(Date.now()<until){try{if(await run(s,expression))return}catch{}await delay(60)}throw Error('Wait failed: '+expression);}
try{
 const version=await start();const s=await page('/workbench.html');await wait(s,'document.querySelector("#template").options.length===36');check('workbench loads all 36 existing templates',true);
 await run(s,'document.querySelector("#fixture").click()');await wait(s,'!document.querySelector("#save-observation").disabled');check('fixture is labeled, price rendered',await run(s,'document.querySelector("#capture-result").innerText.includes("FIXTURE") && document.querySelector("#capture-result").innerText.includes("45.00")'));
 await run(s,'document.querySelector("#save-observation").click()');await wait(s,'document.querySelectorAll("#observations .row").length===1');
 await run(s,'document.querySelector("#compile").click()');await wait(s,'!document.querySelector("#save-draft").disabled');
 const compiled=await run(s,'JSON.parse(document.querySelector("#draft-output").textContent)');check('UI uses existing compiled stages with browser evidence',compiled.stage_plan.length>0&&compiled.browser_evidence.length===1&&compiled.status==='draft');
 await run(s,'document.querySelector("#save-draft").click()');await wait(s,'document.querySelectorAll("#drafts .row").length===1');check('routine budget uses saved draft',await run(s,'JSON.parse(document.querySelector("#basket").textContent).months[0].modeled_cents===5000'));
 const f1=await page('/fixture?revision=1');const observed1=await run(f1,"(async()=>{const {capturePage}=await import('/capture.mjs');const {observe}=await import('/observation.mjs');const c=await(await fetch('/catalog.json')).json();return observe(capturePage(),c)})()");
 check('rendered-page capture excludes form values',!JSON.stringify(observed1).includes('NEVER_CAPTURE'));check('rendered JSONLD yields exact USD cents',observed1.offers[0].price.usd_cents===4500);
 const f2=await page('/fixture?revision=2');const observed2=await run(f2,"(async()=>{const {capturePage}=await import('/capture.mjs');const {observe}=await import('/observation.mjs');const c=await(await fetch('/catalog.json')).json();return observe(capturePage(),c)})()");
 const comparison=await run(s,`(async()=>{const m=await import('./observation.mjs');return m.compareObservations(${JSON.stringify(observed1)},${JSON.stringify(observed2)})})()`);check('price and cancellation excerpt differences both detected',comparison.changes.length===2);
 const cas=await run(s,"(async()=>{const m=await import('./store.mjs');return (await Promise.allSettled([m.saveLocal('test:cas',{n:1},0),m.saveLocal('test:cas',{n:2},0)])).map(r=>r.status)})()");check('concurrent stale write refused atomically',cas.filter(x=>x==='fulfilled').length===1&&cas.filter(x=>x==='rejected').length===1);
 const studio=await page('/studio.html');await wait(studio,'document.querySelector("#template").options.length===36');await run(studio,'document.querySelector("[data-tab=rhythm]").click()');await wait(studio,'document.querySelectorAll("#rhythm-list tbody tr").length===1');check('original studio reads same draft store',true);
 const all=await run(s,"(async()=>{const {compileDraft}=await import('./recurring-commerce.mjs');const c=await(await fetch('./catalog.json')).json();const v=JSON.parse(document.querySelector('#input').value);return c.templates.map(t=>{const x=structuredClone(v);x.template_id=t.id;x.program_id='draft:'+t.id;x.terms.cadence={unit:t.pattern==='replenishment'?'usage':'month',every:1};x.terms.auto_renew=t.pattern!=='finite_gift';x.terms.gift_cycles=3;x.terms.style_fee_cents=2000;return compileDraft(c,x).status})})()");check('all 36 patterns/templates compile inside Chromium',all.length===36&&all.every(x=>x==='draft'));
 const metrics=await run(s,"({documentWidth:document.documentElement.scrollWidth,viewport:innerWidth,webmcp:JSON.parse(document.querySelector('#capabilities').textContent).webmcp})");
 check('no horizontal document overflow',metrics.documentWidth<=metrics.viewport);
 await stop();await start();const reopened=await page('/workbench.html');await wait(reopened,'document.querySelectorAll("#drafts .row").length===1 && document.querySelectorAll("#observations .row").length===1');check('draft and observation survive browser process restart',true);
 await rpc('Network.enable',{},reopened);await rpc('Network.emulateNetworkConditions',{offline:true,latency:0,downloadThroughput:0,uploadThroughput:0},reopened);
 await run(reopened,'document.querySelector("#compile").click()');await wait(reopened,'!document.querySelector("#save-draft").disabled');check('already-loaded workbench compiles without network',true);
 check('no uncaught page errors',errors.length===0,errors);
 const native=await run(reopened,`(async()=>{const m=await import('./observation.mjs');return m.cmsRecord(${JSON.stringify(observed1)})})()`);
 let nativeResult={executed:false};const repo=process.env.BONFYRE_REPO||join(homedir(),'Developer/Bonfyre'),cms=join(homedir(),'.bonfyre/bin/bonfyre-cms');
 if(existsSync(cms)&&existsSync(join(repo,'cmd/BonfyreCMS/content-types/service_offer.json'))){
  const dir=join(profile,'native-fixture');mkdirSync(dir);const schemas=join(dir,'schemas');mkdirSync(schemas);copyFileSync(join(repo,'cmd/BonfyreCMS/content-types/service_offer.json'),join(schemas,'service_offer.json'));
  const db=join(dir,'cms.db'),call=args=>{const r=spawnSync(cms,[...args,'--db',db,'--schemas',schemas],{encoding:'utf8',timeout:10000});if(r.status!==0)throw Error(r.stderr);const value=JSON.parse(r.stdout);if(value.error)throw Error(JSON.stringify(value));return value;};
  {const r=spawnSync(cms,['schema','migrate','--db',db,'--schemas',schemas],{encoding:'utf8',timeout:10000});if(r.status!==0)throw Error(r.stderr);}const created=call(['entry','create','service_offer','--ns','browser-fixture',JSON.stringify(native)]);const rows=call(['entry','list','service_offer','--ns','browser-fixture','--limit','5']).data;
  check('browser capture consumed by existing native CMS',rows.length===1&&rows[0].attributes.offer_id===native.offer_id&&rows[0].attributes.status==='draft');
  nativeResult={executed:true,temporary_database:true,created_id:created.data?.id??created.id,readback:rows[0],no_live_estate_write:true};
 }
 console.log(JSON.stringify({schema:'aurekai.chromium-commerce-witness.v1',browser:version,checks_passed:checks.length,checks,webmcp:metrics.webmcp,native:nativeResult,observed_fixture:observed1,
  limits:['No extension installed; scripting, side-panel and native-messaging permissions were not browser-tested.','Real Chrome HTML/module/storage behavior tested.','Offline means calculation in an already loaded page; no standalone-web offline reload claim.','No merchant, checkout, subscription, or publication accessed.']},null,2));
}finally{await stop();server.stop(true);rmSync(profile,{recursive:true,force:true,maxRetries:5,retryDelay:200});}
