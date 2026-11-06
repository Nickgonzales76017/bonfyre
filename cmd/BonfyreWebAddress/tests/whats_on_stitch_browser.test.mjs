// Chromium witness against actual WebAddress /whats-on output from page_events_test.sh.
// The input is a locally retained, generated calendar page; no real account or source changes.
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {createRequire} from 'node:module';
import {resolve,join} from 'node:path';
import {fileURLToPath} from 'node:url';

const file=process.argv[2];
if(!file)throw Error('Provide a rendered native agenda HTML file.');
const html=readFileSync(file,'utf8');
assert(html.includes('data-agenda-controls')&&html.includes('data-agenda-weekend'),'Not a native filtered event page.');
const root=resolve(fileURLToPath(new URL('../../..',import.meta.url)));
const puppeteer=createRequire(join(root,'tools/creator-ingest/package.json'))('puppeteer');
const browser=await puppeteer.launch({executablePath:'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',headless:true,args:['--no-sandbox','--disable-dev-shm-usage','--disable-background-networking']});
try{
 const context=await browser.createBrowserContext(),page=await context.newPage(),requests=[];
 await page.setRequestInterception(true);
 page.on('request',r=>{requests.push({method:r.method(),url:r.url()});if(['data:','blob:'].includes(new URL(r.url()).protocol))void r.continue();else void r.abort();});
 await page.setViewport({width:1360,height:920});
 await page.setContent(html,{waitUntil:'domcontentloaded'});
 await page.waitForSelector('[data-agenda-controls]');
 const agendas=await page.$$('ol.agenda'),agenda=agendas[0],other=agendas[1];
 const count=await agenda.$$eval(':scope > li.agenda-item',items=>items.length);
 assert(count>=3,'Expected real source calendar events, not placeholders.');
 assert.equal(await agenda.$$eval(':scope > li.agenda-item[hidden]',items=>items.length),0);
 const textBefore=await page.$eval('[data-agenda-count]',n=>n.textContent);
 assert.equal(textBefore,'Showing '+count+' of '+count+' published events on this page.');
 await page.type('[data-agenda-search]','Doors open');
 assert.deepEqual(await agenda.$$eval(':scope > li.agenda-item:not([hidden])',items=>items.map(n=>n.textContent.includes('Doors open'))),[true]);
 assert.equal(await agenda.$$eval(':scope > li.agenda-day:not([hidden])',items=>items.length),1);
 await page.$eval('[data-agenda-search]',n=>{n.value='';n.dispatchEvent(new Event('input',{bubbles:true}));});
 const weekendCount=await agenda.$$eval(':scope > li.agenda-item',items=>items.filter(item=>item.querySelector('[data-agenda-weekend]')?.dataset.agendaWeekend==='1').length);
 await page.click('[data-agenda-filter=weekend]');
 assert.equal(await agenda.$$eval(':scope > li.agenda-item:not([hidden])',items=>items.length),weekendCount);
 assert.equal(await page.$eval('[data-agenda-filter=weekend]',n=>n.getAttribute('aria-pressed')),'true');
 assert.equal(await page.$eval('[data-agenda-count]',n=>n.textContent),'Showing '+weekendCount+' of '+count+' published events on this page.');
 await page.click('[data-agenda-filter=all]');
 assert.equal(await agenda.$$eval(':scope > li.agenda-item:not([hidden])',items=>items.length),count);
 if(other)assert.equal(await other.$$eval(':scope > li.agenda-item[hidden]',items=>items.length),0,'The second agenda must remain unchanged');
 await page.setViewport({width:390,height:840});
 const geometry=await page.$eval('[data-agenda-controls]',n=>({width:n.getBoundingClientRect().width,viewport:innerWidth}));
 assert(geometry.width<=geometry.viewport+1,'The agenda controls overflow the phone viewport.');
 assert.equal(requests.filter(r=>r.method==='POST').length,0);
 console.log('Stitch What’s on: PASS exact calendar event filtering, local weekend, day grouping, keyboard-size mobile and no writes');
 await context.close();
}finally{await browser.close();}
