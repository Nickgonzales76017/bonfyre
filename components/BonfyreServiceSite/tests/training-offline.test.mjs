import {execFileSync} from "node:child_process";
import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import {mkdtempSync,rmSync,mkdirSync,writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,resolve} from 'node:path';
import {trainingHandler,trainingMaterial} from '../training-workspace.mjs';
const free=Number(execFileSync("memory_pressure",["-Q"],{encoding:"utf8"}).match(/free percentage: (\d+)%/)?.[1]);
if(!(free>=18))throw Error("Defer browser verification until at least 18% memory is available.");
const puppeteer=createRequire(resolve('tools/creator-ingest/package.json'))('puppeteer');
const profile=mkdtempSync(join(tmpdir(),'bonfyre-training-offline-'));
const out=resolve('packs/no-simulation/fitness-studio-20261011');mkdirSync(out,{recursive:true});
const material=trainingMaterial('packs/aurekai-fitness-studio');
const requests=[];
let handler,server=Bun.serve({hostname:'127.0.0.1',port:0,fetch:r=>{requests.push(new URL(r.url).pathname);return handler(r);}});
const port=server.port,origin=`http://127.0.0.1:${port}`;handler=trainingHandler(material,port);
const launch=()=>puppeteer.launch({executablePath:'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',headless:true,userDataDir:profile,args:['--no-sandbox','--disable-gpu','--disable-dev-shm-usage']});
let browser;const checks=[];
const check=(name,ok)=>{assert.ok(ok,name);checks.push(name);console.log('PASS '+name);};
try{
  browser=await launch();let page=await browser.newPage();page.setDefaultTimeout(15000);
  await page.goto(origin,{waitUntil:'networkidle0'});await page.waitForFunction(()=>document.querySelector('#offline-state').textContent==='Ready offline');
  check('complete app bundle installs',await page.evaluate(async()=>{const keys=await caches.keys();return keys.filter(k=>k.startsWith('bonfyre-training-')).length===1;}));
  await page.click('[data-action="adopt"]');await page.waitForSelector('[data-action="start"]');await page.click('[data-action="start"]');await page.waitForSelector('.set-row');
  await page.click('.set-done');await page.waitForSelector('.complete');await page.click('[data-action="pause"]');await page.waitForSelector('[data-action="resume"]');
  const saved=await page.evaluate(async()=>{const {readLocal}=await import('/store.mjs');return (await readLocal('training:member:primary'))[0].value;});
  server.stop(true);server=null;await browser.close();browser=await launch();page=await browser.newPage();page.setDefaultTimeout(15000);await page.setOfflineMode(true);
  await page.goto(origin+'/#train',{waitUntil:'networkidle0'});await page.waitForSelector('[data-action="resume"]');
  check('app and paused workout reopen after browser restart with server stopped',await page.$$eval('.complete',rows=>rows.length===1));
  const restored=await page.evaluate(async()=>{const {readLocal}=await import('/store.mjs');return (await readLocal('training:member:primary'))[0].value;});
  check('offline restart preserves member, session and rest deadline',saved.member===restored.member&&saved.active.id===restored.active.id&&saved.active.restUntil===restored.active.restUntil);
  await page.click('[data-action="resume"]');await page.waitForSelector('[data-action="pause"]');await page.click('[data-action="finish"]');await page.waitForSelector('.history-row');
  await page.reload({waitUntil:'networkidle0'});await page.waitForSelector('.history-row');
  check('offline workout completion survives reload',await page.$$eval('.history-row',rows=>rows.length===1));
  check('private training data never enters the app asset cache',await page.evaluate(async member=>{
    for(const key of await caches.keys()){if(!key.startsWith('bonfyre-training-'))continue;const cache=await caches.open(key);for(const request of await cache.keys()){if((await (await cache.match(request)).text()).includes(member))return false;}}
    return true;
  },saved.member));
  await page.setViewport({width:390,height:844});await page.screenshot({path:out+'/offline-history-390.png',fullPage:true});
  const initialVersion=await page.evaluate(async()=> (await caches.keys()).find(k=>k.startsWith('bonfyre-training-')));
  const changed=structuredClone(material);changed.creators[0].settings.brand.name='Forge Updated';handler=trainingHandler(changed,port);
  server=Bun.serve({hostname:'127.0.0.1',port,fetch:r=>handler(r)});await page.setOfflineMode(false);
  await page.evaluate(async()=>{const registration=await navigator.serviceWorker.getRegistration();await registration.update();});
  await page.waitForFunction(async()=>!!(await navigator.serviceWorker.getRegistration()).waiting);
  check('new app version waits while the old tab stays open',await page.$eval('#brand-name',e=>e.textContent==='Forge Studio'));
  // Simulate a server changing mid-install: every candidate response has the wrong release header.
  const later=structuredClone(changed);later.creators[0].settings.brand.name='Incomplete Update';const rejected=trainingHandler(later,port);
  const rejectedVersion=rejected(new Request(origin+'/',{headers:{host:`127.0.0.1:${port}`}})).headers.get('X-Training-Version');
  handler=request=>{const response=rejected(request);if(new URL(request.url).pathname==='/training-model.mjs')response.headers.set('X-Training-Version','wrong-version');return response;};
  await page.evaluate(async()=>{const registration=await navigator.serviceWorker.getRegistration();await registration.update();});
  await page.waitForFunction(async()=>!(await navigator.serviceWorker.getRegistration()).installing);
  check('mismatched update cannot replace the installed app',await page.$eval('#brand-name',e=>e.textContent==='Forge Studio'));
  check('failed update leaves no partial candidate cache',await page.evaluate(async version=>!(await caches.keys()).includes('bonfyre-training-'+version),rejectedVersion));
  handler=trainingHandler(changed,port);
  await browser.close();browser=await launch();page=await browser.newPage();page.setDefaultTimeout(15000);
  await page.goto(origin,{waitUntil:'networkidle0'});await page.waitForSelector('[data-action="start"]');
  // A failed candidate may discard the earlier waiting candidate; request the valid bundle again.
  await page.evaluate(async()=>{const registration=await navigator.serviceWorker.getRegistration();await registration.update();});
  const activeName=await page.$eval('#brand-name',e=>e.textContent);
  if(activeName!=='Forge Updated'){
    await page.waitForFunction(async()=>!!(await navigator.serviceWorker.getRegistration()).waiting);
    await browser.close();browser=await launch();page=await browser.newPage();await page.goto(origin,{waitUntil:'networkidle0'});
  }
  await page.waitForFunction(()=>document.querySelector('#brand-name')?.textContent==='Forge Updated');
  check('complete update takes over after old tabs close',true);
  check('update preserves training history and clears obsolete app cache',await page.evaluate(async previous=>{
    const {readLocal}=await import('/store.mjs'),state=(await readLocal('training:member:primary'))[0].value,keys=(await caches.keys()).filter(k=>k.startsWith('bonfyre-training-'));
    return state.history.length===1&&keys.length===1&&keys[0]!==previous;
  },initialVersion));
  writeFileSync(out+'/offline-verification.json',JSON.stringify({date:'2026-10-11',checks,serverStopped:true,browserRestarted:true},null,2)+'\n');
}catch(error){
  console.error({requests});
  if(browser)for(const page of await browser.pages())if(page.url().startsWith(origin))console.error(await page.evaluate(async()=>({notice:document.querySelector('#notice')?.textContent,offline:document.querySelector('#offline-state')?.textContent,registrations:(await navigator.serviceWorker.getRegistrations()).map(r=>({active:r.active?.state,waiting:r.waiting?.state,installing:r.installing?.state})),cache:await caches.keys()})));
  throw error;
}finally{if(browser)await browser.close();server?.stop(true);rmSync(profile,{recursive:true,force:true});}
