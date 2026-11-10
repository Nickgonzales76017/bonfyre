// Dedicated Chrome profile and synthetic collection; no user's browser state is read.
import { createRequire } from 'node:module';
import { mkdtempSync, mkdirSync, copyFileSync, writeFileSync, rmSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { tmpdir } from 'node:os';
import assert from 'node:assert/strict';

const repo = resolve(import.meta.dir,'../../..');
const puppeteer = createRequire(join(repo,'tools/creator-ingest/package.json'))('puppeteer');
const dir = mkdtempSync(join(tmpdir(),'documents-browser-')), profile = join(dir,'chrome');
const proof = join(repo,'packs/aurekai-work-samples/site/design/verification-documents-local');
mkdirSync(proof,{recursive:true});
copyFileSync(join(repo,'packs/aurekai-work-samples/household/documents.json'),join(dir,'documents.json'));
const reserved = Bun.serve({hostname:'127.0.0.1',port:0,fetch:()=>new Response('')});
const port = reserved.port, origin = `http://127.0.0.1:${port}`; reserved.stop(true);
let server, browser;
const checks = [], errors = [], outside = [];
const check = (name,good) => {assert.ok(good,name);checks.push(name);console.log('PASS '+name);};
async function start() {
  server = Bun.spawn(['bun',join(repo,'components/BonfyreServiceSite/site.mjs'),'documents',dir,'--port',String(port)],{stdout:'pipe',stderr:'pipe'});
  const reader = server.stdout.getReader(); let output = '';
  const timeout = setTimeout(()=>server.kill(),60000);
  try {
    while(!output.includes(origin)) { const {value,done} = await reader.read(); if(done) throw Error('Preview failed: '+await new Response(server.stderr).text());output += new TextDecoder().decode(value); }
  } finally {clearTimeout(timeout);reader.releaseLock();}
}
async function stop() {if(server){server.kill();await server.exited;server=null;}}
const launch = () => puppeteer.launch({headless:true,executablePath:'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',userDataDir:profile});
async function open() {
  const page = await browser.newPage(); page.setDefaultTimeout(15000);
  page.on('pageerror',e=>errors.push(e.message));
  page.on('request',r=>{if(!r.url().startsWith(origin+'/')) outside.push(r.url());});
  await page.setViewport({width:1440,height:1080}); await page.goto(origin,{waitUntil:'networkidle0'});
  await page.waitForSelector('body[data-ready=true]'); return page;
}
async function input(page,id,value) {await page.bringToFront();await page.click('#'+id,{clickCount:3});await page.keyboard.press('Backspace');await page.type('#'+id,value);}
async function waitIdle(page) {await page.waitForFunction(()=>!document.body.hasAttribute('aria-busy'));}
try {
  await start(); browser = await launch(); let page = await open();
  check('Eight fictional documents open',await page.$$eval('#documents button',n=>n.length)===8);
  await page.click('#search'); await waitIdle(page);
  check('Blank search has a useful error',await page.$eval('#error',n=>!n.hidden&&n.textContent.includes('1 to 500')));
  await input(page,'query','Where is the dishwasher receipt?');await page.click('#search');
  await page.waitForSelector('.result');await waitIdle(page);
  check('Native semantic search finds the appliance source first',(await page.$eval('.result',n=>n.innerText)).includes('Appliance receipts'));
  await page.click('.result button');await waitIdle(page);
  check('Original document and paragraph are visible',await page.$eval('#citation',n=>n.textContent.includes('paragraph 2')&&n.textContent.includes('Appliance receipts')));
  check('Exact source paragraph is highlighted',await page.$eval('.selected-passage',n=>n.textContent.includes('blue folder in the kitchen drawer')));
  await page.screenshot({path:join(proof,'desktop.png'),fullPage:true});
  for(const width of [1440,1024,768,390,320]) {
    await page.setViewport({width,height:width<650?844:1080});
    check('No horizontal overflow at '+width,await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth));
    if(width===390)await page.screenshot({path:join(proof,'mobile.png'),fullPage:true});
  }
  await page.setViewport({width:1440,height:1080});
  await input(page,'note','Check the blue folder for the dishwasher receipt.');await page.click('#save-note');await waitIdle(page);
  check('Cited note saves',await page.$eval('#save-state',n=>n.textContent)==='Saved in this browser');
  await page.reload({waitUntil:'networkidle0'});await page.waitForSelector('body[data-ready=true]');
  check('Note and its paragraph survive reload',await page.$eval('#notes',n=>n.innerText.includes('Check the blue folder')&&n.innerText.includes('paragraph 2')));
  await page.click('#notes button');await waitIdle(page);
  const stale = await open();await stale.click('#notes button');await waitIdle(stale);
  await input(page,'note','Updated: check the blue kitchen folder.');await page.click('#save-note');await waitIdle(page);
  await input(stale,'note','Stale overwrite');await stale.click('#save-note');await waitIdle(stale);
  check('A stale tab cannot overwrite the saved note',await stale.$eval('#error',n=>n.textContent.includes('another tab')));
  await stale.close();await page.bringToFront();
  // Actual network failure in the browser, then a successful retry.
  await page.setRequestInterception(true);
  const failRemove = r => r.url().endsWith('/api/remove') ? r.abort('failed') : r.continue();
  page.on('request',failRemove);await page.click('#source summary');await page.click('#remove');await waitIdle(page);
  check('Failed removal leaves the original available',await page.$eval('#source',n=>!n.hidden)&&await page.$eval('#error',n=>!n.hidden));
  page.off('request',failRemove);await page.setRequestInterception(false);
  await page.click('#remove');await waitIdle(page);
  check('Removal updates the list and preserves the saved note',await page.$eval('[data-document=appliance-receipts]',n=>n.disabled)&&await page.$eval('#notes',n=>n.innerText.includes('Updated:')&&n.innerText.includes('Source removed from search')));
  await input(page,'query','dishwasher');await page.select('#mode','phrase');await page.click('#search');await waitIdle(page);
  check('Removed passages cannot match exact search',await page.$$eval('.result',n=>n.length)===0);
  await input(page,'query','submarine insurance policy');await page.click('#search');await waitIdle(page);
  check('Unsupported phrase gives an empty result without an answer',await page.$eval('#results-title',n=>n.textContent)==='No matching passages');
  await browser.close();browser=null;await stop();await start();browser=await launch();page=await open();
  check('Removal survives process and browser restart',await page.$eval('[data-document=appliance-receipts]',n=>n.disabled));
  check('Saved note survives full browser restart',await page.$eval('#notes',n=>n.innerText.includes('Updated: check the blue kitchen folder.')));
  await input(page,'query','Where is the dishwasher receipt?');await page.click('#search');await waitIdle(page);
  check('Semantic search after restart excludes the removed document',!(await page.$eval('#results',n=>n.textContent)).includes('Appliance receipts'));
  await page.screenshot({path:join(proof,'after-restart.png'),fullPage:true});
  check('No page exceptions',errors.length===0);check('The workspace made no remote page requests',outside.length===0);
  writeFileSync(join(proof,'browser-verification.json'),JSON.stringify({at:new Date().toISOString(),checks,errors,outside,synthetic:true,backend:'installed native ONNX embedding and vector search',process_restart:true,browser_restart:true},null,2)+'\n');
} finally {if(browser)await browser.close();await stop();rmSync(dir,{recursive:true,force:true});}
