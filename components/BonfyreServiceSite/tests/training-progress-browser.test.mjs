import {execFileSync} from "node:child_process";
import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import {mkdirSync,writeFileSync} from 'node:fs';
import {resolve} from 'node:path';
import {trainingHandler,trainingMaterial} from '../training-workspace.mjs';
const free=Number(execFileSync("memory_pressure",["-Q"],{encoding:"utf8"}).match(/free percentage: (\d+)%/)?.[1]);
if(!(free>=18))throw Error("Defer browser verification until at least 18% memory is available.");
const puppeteer=createRequire(resolve('tools/creator-ingest/package.json'))('puppeteer');
let handler;const server=Bun.serve({hostname:'127.0.0.1',port:0,fetch:r=>handler(r)});
handler=trainingHandler(trainingMaterial('packs/aurekai-fitness-studio'),server.port);
const out=resolve('packs/no-simulation/fitness-studio-20261011');mkdirSync(out,{recursive:true});
const browser=await puppeteer.launch({executablePath:'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',headless:true,args:['--no-sandbox','--disable-gpu','--disable-dev-shm-usage']});
const checks=[],errors=[];const check=(name,value)=>{assert.ok(value,name);checks.push(name);console.log('PASS '+name);};
try{
  const page=await browser.newPage();page.setDefaultTimeout(15000);page.on('pageerror',e=>errors.push(e.message));
  await page.setViewport({width:390,height:844});await page.goto(`http://127.0.0.1:${server.port}/#train`,{waitUntil:'networkidle0'});
  await page.click('[data-action="routine.new"]');await page.waitForSelector('#routine-editor');
  await page.$eval('[name="routine-name"]',e=>{e.value='Progressive strength';});await page.$eval('[name="exercise-name"]',e=>{e.value='Goblet squat';});
  await page.click('.progression-options summary');await page.select('[name="progression"]','double');
  await page.$eval('#routine-editor',form=>{
    for(const [name,value] of Object.entries({'min-reps':'8','max-reps':'9','increment':'2.5','deload-after':'2'}))form.querySelector(`[name="${name}"]`).value=value;
    for(const row of form.querySelectorAll('.editor-set')){row.querySelector('[name="load"]').value='20';row.querySelector('[name="reps"]').value='8';row.querySelector('[name="effort"]').value='rir';row.querySelector('[name="effort-value"]').value='2';}
  });
  await page.click('#routine-editor button[type="submit"]');await page.waitForSelector('.target-preview');
  check('editor saves a configured progression and exposes its initial targets',await page.$eval('.target-preview',e=>e.textContent.includes('20 kg × 8')&&e.textContent.includes('First session')));
  for(let workout=0;workout<2;workout++){
    await page.click('[data-action="start"]');await page.waitForSelector('.set-row');
    check(`session ${workout+1} leaves actual effort blank`,await page.$$eval('.set-row [name="effort"]',rows=>rows.length===3&&rows.every(e=>e.value==='')));
    for(let index=0;index<3;index++){
      await page.$eval(`.set-row:nth-of-type(${index+1}) [name="effort"]`,e=>{e.value='2';});
      await page.click(`.set-row:nth-of-type(${index+1}) .set-done`);await page.waitForFunction(n=>document.querySelectorAll('.set-row.complete').length===n,{},index+1);
    }
    await page.click('[data-action="finish"]');await page.waitForSelector('.history-row');
    await page.click('a[href="#train"]');await page.waitForSelector('.target-preview');
    check(`session ${workout+1} derives the next targets from completed work`,await page.$eval('.target-preview',(e,expected)=>e.textContent.includes(expected),workout===0?'20 kg × 9':'22.5 kg × 8'));
  }
  await page.click('.target-preview summary');await page.screenshot({path:out+'/progression-390.png',fullPage:true});
  await page.click('a[href="#activity"]');await page.waitForSelector('#weight-form');
  for(const [day,weight] of [['2026-10-09','80'],['2026-10-11','81']]){
    await page.$eval('#weight-form',(form,{day,weight})=>{form.elements.day.value=day;form.elements.weight.value=weight;},{day,weight});
    await page.click('#weight-form button');await page.waitForFunction(n=>document.querySelectorAll('.weight-row').length===n,{},day==='2026-10-09'?1:2);
  }
  check('weight chart contains both dated entries',await page.$$eval('.weight-chart circle',rows=>rows.length===2));
  await page.reload({waitUntil:'networkidle0'});await page.waitForSelector('.weight-chart');
  await page.select('#creator','still');await page.waitForFunction(()=>document.querySelector('#brand-name').textContent==='Still Studio');
  check('weights and completed progression sessions survive reload and creator switch',await page.evaluate(()=>document.querySelectorAll('.weight-row').length===2&&document.querySelectorAll('.history-row').length===2));
  await page.click('a[href="#settings"]');await page.waitForSelector('#settings-form');await page.select('[name="unit"]','lb');await page.click('#settings-form button:not([type="button"])');
  await page.waitForFunction(()=>document.querySelector('#settings-form [name="unit"]').value==='lb');
  await page.click('a[href="#activity"]');await page.waitForSelector('#weight-form');await page.click('.weight-entries summary');await page.click('[data-action="weight.edit"]');await page.click('#weight-form button');
  const weights=await page.evaluate(async()=>{const {readLocal}=await import('/store.mjs');return (await readLocal('training:member:primary'))[0].value.bodyweight;});
  check('saving an unchanged pounds display retains exact canonical kilograms',weights[1].weightKg===81);
  await page.screenshot({path:out+'/weight-history-390.png',fullPage:true});
  check('mobile weight history fits the viewport',await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth));
  await page.click('.weight-entries summary');await page.click('[data-action="weight.remove"]');await page.waitForFunction(()=>document.querySelectorAll('.weight-row').length===1);
  check('removing a weight updates the chart',await page.$$eval('.weight-chart circle',rows=>rows.length===1));
  check('no browser runtime errors',errors.length===0);
  writeFileSync(out+'/progress-verification.json',JSON.stringify({checkedAt:new Date().toISOString(),browser:await browser.version(),checks,errors},null,2)+'\n');
}finally{await browser.close();server.stop(true);}
