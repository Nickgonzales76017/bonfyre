// Runs against an already served recording workspace in a fresh browser profile.
import {createRequire} from 'node:module';
import {mkdirSync,writeFileSync,mkdtempSync,rmSync} from 'node:fs';
import {resolve,join} from 'node:path';
import {tmpdir} from 'node:os';
import assert from 'node:assert/strict';
const [origin,output]=process.argv.slice(2);
if(!origin||!output)throw Error('Pass the local workspace origin and an evidence directory.');
const repo=resolve(import.meta.dir,'../../..');
const puppeteer=createRequire(join(repo,'tools/creator-ingest/package.json'))('puppeteer');
const profile=mkdtempSync(join(tmpdir(),'webb-browser-'));mkdirSync(output,{recursive:true});
const checks=[],errors=[],external=[];let browser,page;
const check=(name,value)=>{assert.ok(value,name);checks.push(name);console.log('PASS '+name);};
const idle=p=>p.waitForFunction(()=>!document.body.hasAttribute('aria-busy'));
try {
 browser=await puppeteer.launch({headless:true,executablePath:'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',userDataDir:profile});
 page=await browser.newPage();page.setDefaultTimeout(30000);await page.setViewport({width:1440,height:1080});
 page.on('pageerror',e=>errors.push(e.message));page.on('request',r=>{if(/^https?:/.test(r.url())&&!r.url().startsWith(origin+'/'))external.push(r.url());});
 await page.goto(origin,{waitUntil:'networkidle0'});await page.waitForSelector('body[data-ready=true]');
 const queries=[
  ['Why do the raw telescope images look almost black?',/appear black/i,125,145],
  ['How are colors assigned to infrared wavelengths?',/chromatic order/i,300,320],
  ['How many filters does the near infrared camera have?',/29 filters/i,290,310]
 ];
 const retrieval=[];
 for(const [query,pattern,start,end] of queries){
  await page.$eval('#query',n=>n.value='');await page.type('#query',query);await page.click('#search');await idle(page);
  const first=await page.$eval('#results .result',n=>n.innerText);check('First result answers: '+query,pattern.test(first));
  await page.click('#results [data-play-result]');await idle(page);
  await page.waitForFunction(()=>document.querySelector('#recording-player').currentTime>0&&!document.querySelector('#recording-player').paused);
  const position=await page.$eval('#recording-player',n=>{n.pause();return {time:n.currentTime,duration:n.duration,width:n.videoWidth,height:n.videoHeight,error:n.error?.message};});
  check('Result plays its passage after minute '+Math.floor(start/60),position.time>=start&&position.time<=end&&!position.error);
  check('Full six-minute video decodes',position.duration>360&&position.duration<361&&position.width===960);
  retrieval.push({query,first_result:first,playback:position});
 }
 check('Publisher source link is present',await page.$eval('#source-link',n=>!n.hidden&&n.href==='https://science.nasa.gov/asset/webb/composing-webb-telescope-images/'));
 // Revisit the colour explanation for desktop and mobile screenshots.
 await page.$eval('#query',n=>n.value='');await page.type('#query',queries[1][0]);await page.click('#search');await idle(page);await page.click('#results [data-play-result]');await idle(page);
 await page.$eval('#recording-player',n=>n.pause());
 check('Current transcript follows playback',await page.$eval('#source-text [aria-current=true]',n=>n.innerText.includes('chromatic')));
 check('Playing passage is visible in the transcript',await page.$eval('#source-text [aria-current=true]',n=>{const a=n.getBoundingClientRect(),b=n.parentElement.getBoundingClientRect();return a.top>=b.top-2&&a.top<b.bottom;}));
 const note='Colors follow wavelength order: blue, green, then red.';
 await page.type('#note',note);await page.click('#save-note');await idle(page);
 check('Note keeps its timed citation',await page.$eval('#notes',n=>n.innerText.includes('5:')&&n.innerText.includes('wavelength order')));
 await page.evaluate(()=>window.scrollTo(0,0));
 await page.screenshot({path:join(output,'desktop.png'),fullPage:true});
 for(const width of [1024,768,390,320]){
  await page.setViewport({width,height:900});
  check('No horizontal overflow at '+width,await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth));
  await page.waitForFunction(()=>{const n=document.querySelector('#source-text [aria-current=true]');if(!n)return false;const a=n.getBoundingClientRect(),b=n.parentElement.getBoundingClientRect();return a.top>=b.top-2&&a.top<b.bottom;});
  check('Playback stays visible after resize to '+width,true);
  if(width===390){await page.$eval('#source',n=>n.scrollIntoView({block:'start'}));await page.screenshot({path:join(output,'mobile.png'),fullPage:false});}
 }
 await page.setViewport({width:1440,height:1080});
 await page.reload({waitUntil:'networkidle0'});await page.waitForSelector('body[data-ready=true]');await page.click('#notes button');await idle(page);
 check('Reload restores the note at its passage',await page.$eval('#note',n=>n.value.includes('wavelength order'))&&await page.$eval('#recording-player',n=>n.currentTime>300));
 // Browser validation of invalid input and an exact search with no result.
 await page.$eval('#query',n=>n.value='');await page.click('#search');await idle(page);check('Empty query has a usable error',await page.$eval('#error',n=>!n.hidden&&n.textContent.includes('1 to 500')));
 await page.select('#mode','phrase');await page.type('#query','submarine insurance policy');await page.click('#search');await idle(page);check('Unmatched phrase stays empty',await page.$$eval('#results .result',n=>n.length===0));
 check('No browser exceptions',errors.length===0);check('No external playback or search requests',external.length===0);
 writeFileSync(join(output,'browser-verification.json'),JSON.stringify({checked_at:new Date().toISOString(),origin,checks,retrieval,errors,external},null,2)+'\n');
} catch(error) {
 if(page){await page.screenshot({path:join(output,'failure.png'),fullPage:true});console.log(await page.evaluate(()=>({error:document.querySelector('#error')?.textContent,player:{paused:document.querySelector('video')?.paused,time:document.querySelector('video')?.currentTime,duration:document.querySelector('video')?.duration,ready:document.querySelector('video')?.readyState,error:document.querySelector('video')?.error?.message}})));}
 throw error;
} finally {if(browser)await browser.close();rmSync(profile,{recursive:true,force:true});}
