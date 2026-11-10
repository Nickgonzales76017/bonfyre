import {createRequire} from 'node:module';
import {readFileSync,writeFileSync} from 'node:fs';
import {resolve} from 'node:path';
import {execFileSync} from 'node:child_process';
const free=Number(execFileSync('memory_pressure',['-Q'],{encoding:'utf8'}).match(/free percentage: (\d+)%/)?.[1]);
if(free<18)throw Error('Insufficient memory for browser witness');
const puppeteer=createRequire(resolve('tools/creator-ingest/package.json'))('puppeteer');
const url='https://docs.aurekai.work/interfaces-and-mcp',live=process.argv.includes('--live');
const html=live?'':readFileSync(process.env.BONFYRE_DOCS_STAGED_HTML||'/private/tmp/aurekai-docs-discovery-20261013/hosts/docs.aurekai.work/interfaces-and-mcp.html','utf8');
const out=resolve('packs/no-simulation/docs-discovery-20261013'),checks=[],errors=[];
let browser;
try{
 browser=await puppeteer.launch({executablePath:'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',headless:true,args:['--disable-gpu','--disable-dev-shm-usage']});
 const page=await browser.newPage();page.setDefaultTimeout(15000);page.on('pageerror',e=>errors.push(e.message));
 if(!live){await page.setRequestInterception(true);page.on('request',r=>{if(r.isNavigationRequest()&&r.url()===url)r.respond({status:200,contentType:'text/html; charset=utf-8',body:html});else r.continue();});}
 for(const width of [1440,390]){
  await page.setViewport({width,height:1000,isMobile:width<500,deviceScaleFactor:1});
  await page.goto(url,{waitUntil:'networkidle2'});
  await page.waitForFunction(()=>[...document.querySelectorAll('h2')].some(h=>h.textContent==='Find the owners of a workflow'));
  const state=await page.evaluate(()=>({title:document.querySelector('h1')?.textContent,overflow:document.documentElement.scrollWidth-innerWidth,commands:[...document.querySelectorAll('pre')].map(n=>n.textContent).filter(t=>t.includes('bonfyre-experience-map resolve')),buildingBlocks:!!document.querySelector('.manual-pages a[href="/building-blocks"]'),headings:[...document.querySelectorAll('h2')].map(n=>n.textContent)}));
  if(state.overflow>1||!state.buildingBlocks||state.commands.length!==1||state.headings.filter(x=>x==='Browser tools and assistant interfaces').length!==1)throw Error(JSON.stringify(state));
  await page.evaluate(()=>{const h=[...document.querySelectorAll('h2')].find(h=>h.textContent==='Find the owners of a workflow');window.scrollTo({top:scrollY+h.getBoundingClientRect().top-220,behavior:'instant'});});
  await page.waitForFunction(()=>{const h=[...document.querySelectorAll('h2')].find(h=>h.textContent==='Find the owners of a workflow');return Math.abs(h.getBoundingClientRect().top-220)<2;});
  await page.screenshot({path:resolve(out,`${live?'live':'staged'}-${width}.png`)});
  checks.push({width,...state});
 }
 await page.setViewport({width:1440,height:1000});
 const outline=await page.$eval('.manual-outline',n=>[...n.querySelectorAll('a')].find(a=>a.textContent==='Find the owners of a workflow')?.getAttribute('href'));
 if(!outline)throw Error('Discovery section missing from outline');
 await page.click(`.manual-outline a[href="${outline}"]`);
 await page.waitForFunction(hash=>location.hash===hash,{},outline);
 await page.click('.manual-pages a[href="/building-blocks"]');
 await page.waitForFunction(()=>document.querySelector('h1')?.textContent==='Building blocks');
 if(errors.length)throw Error(errors.join('\n'));
 writeFileSync(resolve(out,`${live?'live':'staged'}-browser.json`),JSON.stringify({at:new Date().toISOString(),url,mode:live?'live':'staged document with live assets',checks,outlineNavigation:true,buildingBlocksNavigation:true,errors},null,2)+'\n');
 console.log(JSON.stringify({mode:live?'live':'staged',widths:checks.map(x=>x.width),outlineNavigation:true,buildingBlocksNavigation:true,errors}));
}finally{await browser?.close();}
