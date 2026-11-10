// Fresh test profile; no logged-in browser state or external service is used.
import { createRequire } from 'node:module';
import { mkdtempSync, mkdirSync, copyFileSync, readFileSync, writeFileSync, rmSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { tmpdir } from 'node:os';
import assert from 'node:assert/strict';
const repo=resolve(import.meta.dir,'../../..');
const puppeteer=createRequire(join(repo,'tools/creator-ingest/package.json'))('puppeteer');
const retained=join(repo,'.bonfyre/revenue/platform-products-20261008/recording-notebook');
const dir=mkdtempSync(join(tmpdir(),'recording-browser-')),profile=join(dir,'chrome');
const proof=join(repo,'packs/aurekai-work-samples/site/design/verification-recording-local');
mkdirSync(proof,{recursive:true});mkdirSync(join(dir,'media'));
copyFileSync(join(retained,'documents.json'),join(dir,'documents.json'));
const data=JSON.parse(readFileSync(join(dir,'documents.json'))),media=data.documents[0].media;
copyFileSync(join(retained,media.file),join(dir,media.file));
const reserve=Bun.serve({hostname:'127.0.0.1',port:0,fetch:()=>new Response('')});
const port=reserve.port,origin='http://127.0.0.1:'+port;reserve.stop(true);
let server,browser;
const checks=[],errors=[],outside=[];
const check=(name,value)=>{assert.ok(value,name);checks.push(name);console.log('PASS '+name);};
async function start(){
 server=Bun.spawn(['bun',join(repo,'components/BonfyreServiceSite/site.mjs'),'documents',dir,'--port',String(port)],{stdout:'pipe',stderr:'pipe'});
 const reader=server.stdout.getReader();let text='';const timer=setTimeout(()=>server.kill(),60000);
 try{while(!text.includes(origin)){const {value,done}=await reader.read();if(done)throw Error(await new Response(server.stderr).text());text+=new TextDecoder().decode(value);}}
 finally{clearTimeout(timer);reader.releaseLock();}
}
async function stop(){if(server){server.kill();await server.exited;server=null;}}
const launch=()=>puppeteer.launch({headless:true,executablePath:'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',userDataDir:profile});
async function open(){
 const p=await browser.newPage();p.setDefaultTimeout(15000);
 p.on('pageerror',e=>errors.push(e.message));p.on('request',r=>{if(/^https?:/.test(r.url())&&!r.url().startsWith(origin+'/'))outside.push(r.url());});
 await p.setViewport({width:1440,height:1080});await p.goto(origin,{waitUntil:'networkidle0'});await p.waitForSelector('body[data-ready=true]');return p;
}
const idle=p=>p.waitForFunction(()=>!document.body.hasAttribute('aria-busy'));
try{
 await start();browser=await launch();let page=await open();
 await page.type('#query','How long is the lecture?');await page.click('#search');await idle(page);
 check('Native search returns timed transcript passages',await page.$eval('#results',n=>n.innerText.includes('0:06')&&n.innerText.includes('lecture')));
 await page.click('[data-document=recording]');await idle(page);
 check('Recording duration and passage count are visible',await page.$eval('#transcript-status',n=>!n.hidden&&n.textContent.includes('0:10')&&n.textContent.includes('2 transcript passages')));
 await page.waitForFunction(()=>document.querySelector('#recording-player').readyState>=2);
 check('Actual recording decodes locally',await page.$eval('#recording-player',n=>n.duration>10&&n.duration<11&&!n.error&&n.paused));
 await page.click('[data-recording-paragraph="2"]');await idle(page);
 await page.waitForFunction(()=>document.querySelector('#recording-player').currentTime>6.64);
 check('Play action seeks to the second retained segment',await page.$eval('#recording-player',n=>n.currentTime>=6.64&&n.currentTime<10.4&&!n.paused));
 await page.$eval('#recording-player',n=>n.pause());
 check('Citation changes to the played segment',await page.$eval('#citation',n=>n.textContent.includes('paragraph 2')&&n.textContent.includes('0:06–0:10')));
 check('Playback highlights the current segment',await page.$eval('[data-paragraph="2"]',n=>n.classList.contains('playing-passage')));
 await page.screenshot({path:join(proof,'desktop.png'),fullPage:true});
 for(const width of [1440,1024,768,390,320]){
  await page.setViewport({width,height:width<650?844:1080});
  check('Recording view fits width '+width,await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth));
  if(width===390)await page.screenshot({path:join(proof,'mobile.png'),fullPage:true});
 }
 await page.setViewport({width:1440,height:1080});
 await page.type('#note','The speaker says the first lecture will be brief.');await page.click('#save-note');await idle(page);
 check('Saved note retains recording time',await page.$eval('#notes',n=>n.innerText.includes('0:06–0:10')&&n.innerText.includes('lecture will be brief')));
 await page.reload({waitUntil:'networkidle0'});await page.waitForSelector('body[data-ready=true]');
 await page.click('#notes button');await idle(page);
 check('Reload opens the saved timed citation',await page.$eval('#citation',n=>n.textContent.includes('paragraph 2')&&n.textContent.includes('0:06–0:10')));
 // Corrupt the copied test recording; a cached player must not authorize a new citation.
 const original=readFileSync(join(dir,media.file));writeFileSync(join(dir,media.file),Buffer.from('changed recording'));
 await page.click('#save-note');await idle(page);
 check('Changed recording prevents a new save',await page.$eval('#error',n=>n.textContent.includes('recording changed')));
 writeFileSync(join(dir,media.file),original);
 await page.click('[data-recording-paragraph="1"]');await idle(page);
 await page.$eval('#recording-player',n=>n.pause());
 await page.$eval('#note',n=>n.value='');await page.type('#note','Opening of the lecture.');await page.click('#save-note');await idle(page);
 check('Separate moments keep separate notes',await page.$$eval('#notes .saved-note',nodes=>nodes.length===2));
 await page.click('#source summary');await page.click('#remove');await idle(page);
 check('Removal unloads the local player',await page.$eval('#recording-player',n=>n.hidden&&!n.getAttribute('src')));
 check('Saved note remains after source removal',await page.$eval('#notes',n=>n.innerText.includes('Source removed from search')&&n.innerText.includes('0:06–0:10')));
 await browser.close();browser=null;await stop();await start();browser=await launch();page=await open();
 check('Recording removal survives process and browser restart',await page.$eval('[data-document=recording]',n=>n.disabled));
 check('Timed note survives full browser restart',await page.$eval('#notes',n=>n.innerText.includes('lecture will be brief')&&n.innerText.includes('0:06–0:10')));
 check('No browser exceptions',errors.length===0);check('No external page or recording requests: '+outside.join(', '),outside.length===0);
 writeFileSync(join(proof,'browser-verification.json'),JSON.stringify({at:new Date().toISOString(),checks,errors,outside,recording_sha256:media.sha256,transcript_sha256:media.transcript_sha256,review:'unreviewed',human_listening_verified:false,playback_verified:true,process_restart:true,browser_restart:true},null,2)+'\n');
} finally {if(browser)await browser.close();await stop();rmSync(dir,{recursive:true,force:true});}

