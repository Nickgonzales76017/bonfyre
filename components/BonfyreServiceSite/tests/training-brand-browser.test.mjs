import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import {execFileSync} from 'node:child_process';
import {resolve} from 'node:path';
import {writeFileSync} from 'node:fs';
import {trainingMaterial,trainingHandler} from '../training-workspace.mjs';
// This check runs one browser and never configures or invokes a model.
const pressure=execFileSync('memory_pressure',['-Q'],{encoding:'utf8'}),free=Number(pressure.match(/free percentage: (\d+)%/)?.[1]);
if(!Number.isFinite(free)||free<18)throw Error('Defer browser verification until at least 18% memory is available.');
const puppeteer=createRequire(resolve('tools/creator-ingest/package.json'))('puppeteer');
let handler;const server=Bun.serve({hostname:'127.0.0.1',port:0,fetch:r=>handler(r)});handler=trainingHandler(trainingMaterial('packs/aurekai-fitness-studio'),server.port);
let browser;const checks=[],errors=[];const check=(name,value)=>{assert.ok(value,name);checks.push(name);console.log('PASS '+name);};
try{
  browser=await puppeteer.launch({executablePath:'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',headless:true,args:['--no-sandbox','--disable-gpu','--disable-dev-shm-usage']});
  const page=await browser.newPage();page.setDefaultTimeout(10000);page.on('pageerror',e=>errors.push(e.message));await page.setViewport({width:1440,height:1000});
  await page.goto(`http://127.0.0.1:${server.port}/#settings`,{waitUntil:'networkidle0'});await page.waitForSelector('#settings-form');
  const iconBefore=await page.$eval('link[rel=icon]',e=>e.href);
  for(const kind of ['logo','icon']){
    await page.evaluate(async kind=>{
      const canvas=document.createElement('canvas');canvas.width=kind==='logo'?320:64;canvas.height=64;const c=canvas.getContext('2d');c.fillStyle='#c5ef67';c.fillRect(0,0,canvas.width,canvas.height);c.fillStyle='#141812';c.font='bold 40px sans-serif';c.fillText(kind==='logo'?'FORGE':'F',12,46);
      const blob=await new Promise(resolve=>canvas.toBlob(resolve,'image/png')),data=new DataTransfer();data.items.add(new File([blob],kind+'.png',{type:'image/png'}));const input=document.querySelector(`[data-brand-asset="${kind}"]`);input.files=data.files;input.dispatchEvent(new Event('change',{bubbles:true}));
    },kind);await page.waitForSelector(`#brand-${kind}-preview img`);
  }
  check('upload previews wait for Save settings before changing the studio',await page.$('.mark img')===null&&await page.$eval('link[rel=icon]',e=>e.href)===iconBefore);
  await page.click('#settings-form button:not([type="button"])');await page.waitForSelector('.mark img');
  check('saving applies the logo and embedded browser icon',await page.$eval('link[rel=icon]',e=>e.href.startsWith('data:image/png;base64,')));
  await page.screenshot({path:'packs/no-simulation/fitness-studio-20261011/brand-settings-1440.png',fullPage:true});
  await page.reload({waitUntil:'networkidle0'});await page.waitForSelector('.mark img');
  check('logo and icon survive reload',await page.$$eval('.brand-asset-preview img',e=>e.length===2));
  await page.select('#creator','still');await page.waitForFunction(()=>document.querySelector('#brand-name').textContent==='Still Studio');
  check('switching studios restores its own mark and colors',await page.$('.mark img')===null&&await page.$eval('link[rel=icon]',e=>e.href.startsWith('data:image/svg+xml,')&&!e.href.includes('%23ff5432')));
  await page.select('#creator','forge');await page.waitForSelector('.mark img');await page.setViewport({width:390,height:844});
  check('wide logos remain readable without a duplicate name',await page.evaluate(()=>document.querySelector('.mark img').getBoundingClientRect().width>=100&&getComputedStyle(document.querySelector('#brand-name')).display==='none'));
  await page.screenshot({path:'packs/no-simulation/fitness-studio-20261011/brand-settings-390.png',fullPage:true});
  check('uploaded branding fits the mobile viewport',await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth));
  await page.$eval('[name="brand"]',e=>{e.value='Forge Training';});await page.click('#settings-form button:not([type="button"])');await page.waitForFunction(()=>document.title.startsWith('Forge Training'));
  check('ordinary style edits retain saved assets',await page.$('.mark img')!==null);
  await page.click('[data-action="brand.remove"][data-kind="logo"]');check('removal also waits for Save settings',await page.$('.mark img')!==null);
  await page.click('#settings-form button:not([type="button"])');await page.waitForFunction(()=>!document.querySelector('.mark img'));
  check('logo removal leaves the independently configured icon',await page.$eval('link[rel=icon]',e=>e.href.startsWith('data:image/png;base64,')));
  check('model execution remains disconnected',await page.evaluate(async()=>await (await fetch('/api/training/model-route')).json()===null));
  check('no browser runtime errors',errors.length===0);
  writeFileSync('packs/no-simulation/fitness-studio-20261011/brand-browser-verification.json',JSON.stringify({checkedAt:new Date().toISOString(),memoryFreePercentBefore:free,browser:await browser.version(),checks,errors},null,2)+'\n');
}finally{await browser?.close();server.stop(true);}
