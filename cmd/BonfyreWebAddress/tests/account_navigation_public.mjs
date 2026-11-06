// Read-only public release acceptance. Fresh browser, no account fixture,
// cookie injection, storage modification or application mutation.
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import { mkdirSync, writeFileSync } from 'node:fs';
import { join, resolve } from 'node:path';
const root = resolve(import.meta.dir, '../../..');
const out = join(root, 'cmd/BonfyreExperienceStage/verification/navigation-refresh-20261020/public');
mkdirSync(out, {recursive:true});
const puppeteer = createRequire(join(root, 'tools/creator-ingest/package.json'))('puppeteer');
const browser = await puppeteer.launch({executablePath:'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',headless:true,args:['--no-sandbox','--disable-dev-shm-usage']});
const rows=[], failures=[], observations=[];
function pass(name, detail='') { rows.push([name,detail]); console.log('PASS',name,detail); }
async function open(page,url) {
  const response=await page.goto(url,{waitUntil:'networkidle0',timeout:30000});
  assert.equal(response.status(),200,url);
  await page.waitForFunction(()=>document.querySelector('[data-account-slot]')?.dataset.accountRefresh==='2' && document.querySelector('.ak-session')?.dataset.sessionState==='anonymous');
}
try {
  const context=await browser.createBrowserContext(),page=await context.newPage();
  page.setDefaultTimeout(20000);page.on('pageerror',e=>failures.push(e.message));
  await page.setRequestInterception(true);
  page.on('request',r=>{
    const u=new URL(r.url());
    if(!['GET','HEAD'].includes(r.method())||(!u.hostname.endsWith('.aurekai.live')&&!u.hostname.endsWith('.aurekai.work')&&!u.hostname.endsWith('.aurekai.life')&&!u.hostname.endsWith('.aurekai.shop')&&!['aurekai.live','aurekai.work','aurekai.life','aurekai.shop','fonts.googleapis.com','fonts.gstatic.com'].includes(u.hostname)))r.abort();else r.continue();
  });
  const selected='/packs/setup?context=self#tools';
  for(const territory of ['live','life','shop','work']) {
    await page.setViewport({width:1440,height:1000});
    await open(page,'https://aurekai.'+territory+selected);
    assert.equal(await page.$eval('#session-link',a=>new URL(a.href).searchParams.get('next')),selected);
    const count=await page.$$('.pack-tool');assert.ok(count.length>0);
    pass(territory+' current header preserves page, query and fragment',String(count.length)+' tool cards');
  }
  for(const width of [1440,390]) {
    await page.setViewport({width,height:1000});
    await open(page,'https://aurekai.live'+selected);
    const total=await page.$$eval('.pack-tool',n=>n.length);
    await page.type('[data-pack-filter]','research');
    const filtered=await page.$$eval('.pack-tool:not([hidden])',n=>n.map(x=>x.textContent));
    assert.ok(filtered.length>0&&filtered.length<total);assert.ok(filtered.every(s=>s.toLowerCase().includes('research')));
    await page.$eval('[data-pack-filter]',n=>{n.value='';n.dispatchEvent(new Event('input',{bubbles:true}));});
    assert.equal(await page.$$eval('.pack-tool:not([hidden])',n=>n.length),total);
    pass(width+' actual list search and reset',filtered.length+' -> '+total);
    for(const image of await page.$$('.pack-tool img'))await image.evaluate(n=>n.scrollIntoView());
    await page.waitForFunction(()=>[...document.querySelectorAll('.pack-tool img')].every(n=>n.complete&&n.naturalWidth>0));
    const media=await page.$$eval('.pack-tool img',n=>n.map(i=>({url:i.currentSrc,width:i.naturalWidth,height:i.naturalHeight})));
    pass(width+' decoded every referenced product preview',String(media.length));
    const card=await page.$('.pack-tool');await card.evaluate(n=>n.scrollIntoView({block:'center'}));
    await page.mouse.move(0,0);await new Promise(r=>setTimeout(r,700));
    const initial=await card.evaluate(n=>({transform:getComputedStyle(n).transform,rect:n.getBoundingClientRect().y}));
    await card.hover();await new Promise(r=>setTimeout(r,800));
    const hovered=await card.evaluate(n=>({transform:getComputedStyle(n).transform,rect:n.getBoundingClientRect().y,alternate:n.querySelector('.pack-preview-alt')?getComputedStyle(n.querySelector('.pack-preview-alt')).opacity:null}));
    assert.notEqual(initial.transform,hovered.transform);assert.ok(hovered.rect<initial.rect);
    if(hovered.alternate!==null)assert.equal(hovered.alternate,'1');
    pass(width+' real hover changes geometry and reveals alternate capture',hovered.transform);
    await page.emulateMediaFeatures([{name:'prefers-reduced-motion',value:'reduce'}]);
    assert.equal(await card.evaluate(n=>getComputedStyle(n).transitionDuration),'0s');
    assert.equal(await card.evaluate(n=>getComputedStyle(n).animationName),'none');
    pass(width+' reduced motion disables card motion');
    await page.emulateMediaFeatures([{name:'prefers-reduced-motion',value:'no-preference'}]);
    const dimensions=await page.evaluate(()=>({viewport:innerWidth,scroll:document.documentElement.scrollWidth}));
    assert.ok(dimensions.scroll<=dimensions.viewport);
    pass(width+' viewport extent',JSON.stringify(dimensions));
    assert.ok(await page.evaluate(()=>performance.getEntriesByType('resource').every(r=>!/(sql-wasm|three\.module|creator.*wasm)/.test(r.name))));
    pass(width+' directory does not start heavyweight editors');
    await page.evaluate(()=>scrollTo(0,0));await page.mouse.move(0,0);await new Promise(r=>setTimeout(r,700));
    await page.screenshot({path:join(out,'live-tools-'+width+'.png'),fullPage:true});
    const link=await page.$('.pack-tool a[data-setup-review]');
    const href=await link.evaluate(a=>a.href);await link.evaluate(a=>a.scrollIntoView());
    await Promise.all([page.waitForNavigation({waitUntil:'networkidle0'}),link.click()]);
    assert.equal(page.url(),href);assert.ok(await page.$eval('body',n=>n.innerText.length)>100);
    pass(width+' real setup detail opens its retained product identity',href);
    await page.goBack({waitUntil:'networkidle0'});
    await page.waitForFunction(()=>document.querySelector('.ak-session')?.dataset.sessionState==='anonymous');
    assert.equal(await page.$eval('#session-link',a=>new URL(a.href).searchParams.get('next')),selected);
    pass(width+' actual Back returns to same list context');
    await open(page,'https://aurekai.live/account#presence');
    assert.equal(await page.$eval('#session-link',a=>new URL(a.href).searchParams.get('next')),'/account#presence');
    const account=await page.evaluate(()=>({signedVisible:!!document.querySelector('#signed')?.getBoundingClientRect().height,signedHidden:document.querySelector('#signed')?.hidden,prompt:document.querySelector('#out')?.innerText,tools:document.querySelector('.account-tools-host')?.innerText}));
    observations.push({width,account});
    await page.screenshot({path:join(out,'live-account-anonymous-'+width+'.png'),fullPage:true});
    pass(width+' account header preserves the presence continuation');
  }
  assert.deepEqual(failures,[]);pass('no uncaught public page errors');
  await context.close();
} finally {
  await browser.close();
  writeFileSync(join(out,'checks.tsv'),'check\tdetail\n'+rows.map(r=>r.map(s=>String(s).replaceAll('\t',' ').replaceAll('\n',' ')).join('\t')).join('\n')+'\n');
  writeFileSync(join(out,'observations.txt'),JSON.stringify({scope:'fresh anonymous public browser; no account mutations',observations,failures},null,2)+'\n');
}
console.log('PUBLIC_NAVIGATION_PASS',rows.length);
