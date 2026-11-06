import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import {execFileSync} from 'node:child_process';
import {mkdtempSync,rmSync,writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {resolve,join} from 'node:path';
import {trainingMaterial,trainingHandler} from '../training-workspace.mjs';
import {requestStore} from '../training-requests.mjs';
import {creatorAuth,creatorInstitution} from '../training-creator.mjs';
const free=Number(execFileSync('memory_pressure',['-Q'],{encoding:'utf8'}).match(/free percentage: (\d+)%/)?.[1]);
if(!(free>=18))throw Error('Defer browser verification until at least 18% memory is available.');
const puppeteer=createRequire(resolve('tools/creator-ingest/package.json'))('puppeteer'),home=mkdtempSync(join(tmpdir(),'training-draft-browser-')),db=join(home,'auth.db');
const material=trainingMaterial('packs/aurekai-fitness-studio'),requests=requestStore(join(home,'requests'),material),auth=creatorAuth(db);
const native=(command,args=[],input='')=>JSON.parse(execFileSync('cmd/BonfyreAuth/bonfyre-auth',['--db',db,command,...args],{input,encoding:'utf8',stdio:'pipe'}));
let handler,browser;const server=Bun.serve({hostname:'127.0.0.1',port:0,fetch:r=>handler(r)});handler=trainingHandler(material,server.port,{requests,auth});
const origin=`http://127.0.0.1:${server.port}`,checks=[],errors=[],check=(name,value)=>{assert.ok(value,name);checks.push(name);console.log('PASS '+name);};
const launch=()=>puppeteer.launch({executablePath:'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',userDataDir:join(home,'browser'),headless:true,args:['--no-sandbox','--disable-gpu','--disable-dev-shm-usage']});
async function page(){const p=await browser.newPage();p.setDefaultTimeout(12000);p.on('pageerror',e=>errors.push(e.message));await p.setViewport({width:390,height:844});await p.goto(origin+'/creator',{waitUntil:'networkidle0'});return p;}
async function fill(p,selector,value){await p.click(selector,{clickCount:3});await p.keyboard.press('Backspace');if(value)await p.type(selector,value);}
const saved=p=>p.waitForFunction(()=>document.querySelector('#draft-state')?.textContent.startsWith('Saved '));
try{
  native('http-signup',[],JSON.stringify({email:'coach@example.test',password:'draft-test-password',name:'Coach'}));native('actor-bind',['--user-id','1','--actor','actor:draft-browser']);native('institution-grant',['--user-id','1','--institution',creatorInstitution('forge'),'--role','write']);
  const request=requests.submit({id:crypto.randomUUID(),creatorId:'forge',programId:material.creators[0].routines[0].id,title:'Travel strength',details:'A short strength routine for travel.'});
  browser=await launch();let a=await page();await a.waitForSelector('#creator-login');await a.type('[name="email"]','coach@example.test');await a.type('[name="password"]','draft-test-password');await a.click('#creator-login button');await a.waitForSelector('[data-action="accept"]');await a.click('[data-action="accept"]');await a.waitForSelector('[data-action="build"]');await a.click('[data-action="build"]');await a.waitForSelector('[data-action="draft.source"]');await a.click('[data-action="draft.source"]');await saved(a);
  check('coach starts from the requested program with all exercises and sets',await a.$$eval('fieldset[data-exercise]',els=>els.length)===material.creators[0].routines[0].exercises.length);
  await fill(a,'[name="routine-name"]','');await saved(a);await a.reload({waitUntil:'networkidle0'});await a.waitForSelector('[data-action="build"]');await a.click('[data-action="build"]');await a.waitForSelector('#routine-editor');check('unfinished draft names survive reload',await a.$eval('[name="routine-name"]',e=>e.value)==='');
  await fill(a,'[name="routine-name"]','Saved travel plan');await saved(a);await browser.close();browser=null;browser=await launch();a=await page();await a.waitForSelector('[data-action="build"]');await a.click('[data-action="build"]');await a.waitForSelector('#routine-editor');
  check('draft survives a complete browser restart',await a.$eval('[name="routine-name"]',e=>e.value)==='Saved travel plan');
  check('saving a draft does not deliver it to the member',requests.read(request.id,request.accessToken).delivery===null);
  const worker=requests.creatorRead(request.id,'forge').worker,revision=requests.draftRead(request.id,worker).revision;
  await a.select('[name="effort"]','rir');await a.waitForFunction(()=>document.querySelector('#notice')?.textContent.includes('numeric target'));
  check('an unfinished numeric target is retained on screen without silently clearing the saved effort',requests.draftRead(request.id,worker).revision===revision&&await a.$eval('[name="effort"]',e=>e.value)==='rir');
  await fill(a,'[name="effort-value"]','2');await saved(a);
  const b=await page();await b.waitForSelector('[data-action="build"]');await b.click('[data-action="build"]');await b.waitForSelector('#routine-editor');
  await a.bringToFront();await fill(a,'[name="routine-name"]','First tab saved');await saved(a);
  await b.bringToFront();await fill(b,'[name="routine-name"]','Second tab edits');await b.waitForSelector('#draft-conflict:not([hidden])');
  check('stale autosave preserves current edits and offers recovery',await b.$eval('[name="routine-name"]',e=>e.value)==='Second tab edits'&&await b.$('[data-action="draft.download"]')!==null);
  await b.screenshot({path:'packs/no-simulation/fitness-studio-20261011/creator-draft-conflict-390.png',fullPage:true});
  await b.click('[data-action="draft.reload"]');await b.waitForFunction(()=>document.querySelector('[name="routine-name"]')?.value==='First tab saved');check('loading the saved draft recovers the winning revision',true);
  await b.click('#routine-editor button[type="submit"]');await b.waitForSelector('[data-action="publish"]');
  await a.bringToFront();await fill(a,'[name="routine-name"]','Final approved travel plan');await saved(a);
  await b.bringToFront();await b.click('[data-action="publish"]');await b.waitForSelector('#draft-conflict:not([hidden])');
  check('stale review cannot complete native work',requests.read(request.id,request.accessToken).state==='running');
  await b.click('[data-action="draft.reload"]');await b.waitForFunction(()=>document.querySelector('[name="routine-name"]')?.value==='Final approved travel plan');await b.click('#routine-editor button[type="submit"]');await b.waitForSelector('[data-action="publish"]');await b.click('[data-action="publish"]');await b.waitForFunction(()=>document.querySelector('.creator-request .status')?.textContent==='Delivered');
  check('reviewing the latest draft delivers that exact routine',requests.read(request.id,request.accessToken).delivery.routine.name==='Final approved travel plan');
  check('no browser runtime errors',errors.length===0);
  writeFileSync('packs/no-simulation/fitness-studio-20261011/draft-browser-verification.json',JSON.stringify({checkedAt:new Date().toISOString(),browser:await browser.version(),memoryFreePercentBefore:free,checks,errors},null,2)+'\n');
}finally{await browser?.close();server.stop(true);rmSync(home,{recursive:true,force:true});}
