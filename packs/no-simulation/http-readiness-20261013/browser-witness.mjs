import {createRequire} from 'node:module';
import {execFileSync,spawn} from 'node:child_process';
import {mkdtempSync,writeFileSync,rmSync,openSync,closeSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {resolve,join} from 'node:path';
import assert from 'node:assert/strict';
const root=process.cwd(),out=resolve('packs/no-simulation/http-readiness-20261013');
const free=Number(execFileSync('memory_pressure',['-Q'],{encoding:'utf8'}).match(/free percentage: (\d+)%/)?.[1]);
assert(free>=18,'Memory reserve for browser');
const puppeteer=createRequire(resolve('tools/creator-ingest/package.json'))('puppeteer');
const home=mkdtempSync(join(tmpdir(),'bonfyre-home-browser-')),db=join(home,'matrix.db'),binary=resolve('cmd/BonfyreWebAddress/bonfyre-web-address');
const reserve=Bun.serve({hostname:'127.0.0.1',port:0,fetch:()=>new Response('reserved')});const port=reserve.port;await reserve.stop(true);
const env={...process.env,BONFYRE_CMS_DB:join(home,'absent.db'),BONFYRE_WEATHER:join(home,'weather.json')};
delete env.BONFYRE_CMS_SCHEMAS;
writeFileSync(env.BONFYRE_WEATHER,JSON.stringify({sources:[{tier:'HOT'},{tier:'COLD'}]}));
const run=(...a)=>execFileSync(binary,[...a,'--db',db],{env,stdio:'pipe'});
run('bootstrap-owned');
for(const [territory,scope] of [['life','self'],['live','public'],['shop','market'],['work','institution']])run('domain','--host',`aurekai.${territory}`,'--territory',territory,'--scope',scope,'--world','live','--repr','HTML','--evidence','receipt:browser:home');
const log=openSync(join(out,'server.log'),'w');let server,browser;
const checks=[],errors=[],requests=[];
try{
 server=spawn(binary,['serve','--db',db,'--port',String(port),'--site-root',resolve('site/matrix')],{env,stdio:['ignore',log,log]});
 for(let i=0;i<60;i++){try{if((await fetch(`http://127.0.0.1:${port}/healthz`)).ok)break;}catch{} await Bun.sleep(100);}
 assert.equal((await fetch(`http://127.0.0.1:${port}/healthz`)).status,200);
 browser=await puppeteer.launch({executablePath:'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',headless:true,args:['--disable-gpu','--disable-dev-shm-usage','--no-proxy-server','--host-resolver-rules=MAP aurekai.life 127.0.0.1, MAP aurekai.live 127.0.0.1, MAP aurekai.shop 127.0.0.1, MAP aurekai.work 127.0.0.1']});
 const page=await browser.newPage();page.setDefaultTimeout(12000);page.on('pageerror',e=>errors.push(e.message));
 await page.setRequestInterception(true);
 page.on('request',r=>{requests.push(r.url());if(['aurekai.life','aurekai.live','aurekai.shop','aurekai.work'].includes(new URL(r.url()).hostname)&&new URL(r.url()).port===String(port))r.continue();else r.abort();});
 for(const width of [1440,390]){
  await page.setViewport({width,height:1000,isMobile:width<500,deviceScaleFactor:1});
  for(const territory of ['life','live','shop','work']){
   await page.goto(`http://aurekai.${territory}:${port}/`,{waitUntil:'domcontentloaded'});
   await page.waitForFunction(()=>document.querySelector('#weather')?.textContent.includes('1 available carrier'));
   const state=await page.evaluate(()=>({title:document.title,body:document.body.className,overflow:document.documentElement.scrollWidth-innerWidth,canonical:document.querySelector('link[rel="canonical"]')?.href,forms:[...document.forms].map(f=>({action:new URL(f.action).pathname,method:f.method,button:!!f.querySelector('button')&&f.querySelector('button').getBoundingClientRect().width>0})),weather:document.querySelector('#weather')?.textContent}));
   assert.equal(state.body,territory);assert(state.overflow<=1,JSON.stringify(state));assert.equal(state.canonical,`https://aurekai.${territory}/`);
   assert(state.forms.some(f=>f.action==='/_surface/intake'&&f.method==='get'&&f.button));
   assert(state.forms.some(f=>f.action==='/_surface/external-estate'&&f.method==='get'&&f.button));
   assert(state.weather.includes('1</strong>')||state.weather.includes('1 available carrier'),state.weather);
   checks.push({width,territory,...state});
  }
  await page.screenshot({path:join(out,`fallback-${width}.png`),fullPage:true});
  await page.type('input[name="url"]','https://example.test/opportunity?q=one&two=three');
  await page.click('.url-form button');
  await page.waitForFunction(()=>document.querySelector('#intake-result')?.textContent.includes('Review in Studio'));
  assert.equal(await page.$eval('#intake-result input[name="foreign_url"]',n=>n.value),'https://example.test/opportunity?q=one&two=three');
  await page.screenshot({path:join(out,`intake-${width}.png`),fullPage:true});
  checks.push({width,intakePrepared:true,urlPreserved:true,remoteBodyFetched:requests.some(u=>new URL(u).hostname==='example.test')});
  assert(!requests.some(u=>new URL(u).hostname==='example.test'));
  await Promise.all([page.waitForNavigation({waitUntil:'domcontentloaded'}),page.click('#intake-result button')]);
  assert.equal(await page.$eval('h2.subj-name',n=>n.textContent),'Review a public address');
  assert.equal(await page.$eval('.review-address',n=>n.textContent),'https://example.test/opportunity?q=one&two=three');
  const next=await page.$eval('form[action="/login"] input[name="next"]',n=>n.value);
  assert.equal(new URL(next,'https://aurekai.work').searchParams.get('foreign_url'),'https://example.test/opportunity?q=one&two=three');
  assert.equal(await page.$('form[hx-post="/studio/v1/foreign/observe"]'),null);
  await page.screenshot({path:join(out,`review-${width}.png`)});
  console.log(JSON.stringify({width,review:true}));
  checks.push({width,studioReview:true,signInContinuationPreserved:true,anonymousObservationUnavailable:true});
 }
 assert.deepEqual(errors,[]);
 writeFileSync(join(out,'browser.json'),JSON.stringify({at:new Date().toISOString(),serverWorkers:1,checks,errors},null,2)+'\n');console.log(JSON.stringify({checks:checks.length,errors}));
}finally{await browser?.close();server?.kill('SIGTERM');if(server)await new Promise(r=>server.once('exit',r));closeSync(log);rmSync(home,{recursive:true,force:true});}
