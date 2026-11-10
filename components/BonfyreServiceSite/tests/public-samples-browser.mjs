// Read-only browser audit of the deployed sample routes and their visible assets.
import {readFileSync,writeFileSync,mkdirSync} from 'node:fs';
import {resolve,join} from 'node:path';
import {withChrome} from '../../../integrations/browser/capture/chrome-session.mjs';
if(!process.argv.includes('--live'))throw Error('Pass --live to inspect the public work-samples site.');
const root=resolve(import.meta.dir,'../../..'),origin='https://aurekai-work-samples.pages.dev';
const content=JSON.parse(readFileSync(join(root,'packs/aurekai-work-samples/site/content.json'),'utf8'));
const out=resolve(process.env.SAMPLE_BROWSER_PROOF||join(root,'packs/no-simulation/live-samples-20261013/pages'));
mkdirSync(out,{recursive:true});
const paths=['/',...content.pages.map(page=>'/'+page.path+'/')],report={at:new Date().toISOString(),origin,pages:[],failures:[],writes:[]};
const retain=()=>writeFileSync(join(out,'browser-verification.json'),JSON.stringify(report,null,2)+'\n');
report.status='running';retain();
try{await withChrome(async({open,send,events})=>{
  for(const path of paths){
    const {page,go,targetId,sessionId}=await open();
    const start=events.length;
    try{
      await go(origin+path);
      const readiness=await page('Runtime.evaluate',{expression:`new Promise((resolve,reject)=>{const start=Date.now();const poll=()=>{const catalogue=document.querySelector('[data-catalogue-workspace]'),planner=document.querySelector('[data-planner]'),creator=document.querySelector('[data-creator]');const ready=catalogue?document.querySelector('[data-ui=inventory]')?.dataset.ready==='true':planner?document.querySelector('[data-ui=spatial]')?.dataset.ready==='true':creator?creator.dataset.importsReady==='true'&&document.querySelector('[data-audio=workspace]')?.dataset.ready==='true':true;if(ready)return resolve(true);if(Date.now()-start>25000)return reject(Error('Workspace did not become ready'));setTimeout(poll,50);};poll();})`,awaitPromise:true});
      if(readiness.exceptionDetails)throw Error('Workspace did not become ready: '+path);
      await page('Runtime.evaluate',{expression:'document.fonts.ready',awaitPromise:true});
      await page('Runtime.evaluate',{expression:`Promise.race([Promise.all([...document.images].filter(image=>image.getClientRects().length).map(image=>{image.loading='eager';return image.decode().catch(()=>null)})),new Promise(resolve=>setTimeout(resolve,5000))])`,awaitPromise:true});
      const {result}=await page('Runtime.evaluate',{expression:`({title:document.title,heading:document.querySelector('h1')?.textContent,ids:[...document.querySelectorAll('[id]')].map(n=>n.id),links:[...document.querySelectorAll('a[href]')].map(n=>({text:n.textContent.trim(),href:n.href})),brokenImages:[...document.images].filter(n=>n.getClientRects().length&&(!n.complete||!n.naturalWidth)).map(n=>n.currentSrc||n.src)})`,returnByValue:true});
      const observed={path,...result.value,widths:[]};
      for(const width of [1440,390]){
        await page('Emulation.setDeviceMetricsOverride',{width,height:900,deviceScaleFactor:1,mobile:false});
        // A breakpoint changes picture transforms. Measure their settled layout,
        // while leaving the continuously animated sample reel running.
        await page('Runtime.evaluate',{expression:`new Promise(requestAnimationFrame).then(()=>Promise.all(document.getAnimations().filter(a=>a instanceof CSSTransition).map(a=>a.finished.catch(()=>null))))`,awaitPromise:true});
        const {result:layout}=await page('Runtime.evaluate',{expression:'({fits:document.documentElement.scrollWidth<=innerWidth+1,scrollWidth:document.documentElement.scrollWidth})',returnByValue:true});
        observed.widths.push({width,...layout.value});
        const image=await page('Page.captureScreenshot',{format:'png',captureBeyondViewport:false});
        writeFileSync(join(out,(path==='/'?'index':path.replaceAll('/',''))+'-'+width+'.png'),Buffer.from(image.data,'base64'));
      }
      observed.responses=events.slice(start).filter(e=>e.sessionId===sessionId&&e.method==='Network.responseReceived').map(e=>({url:e.params.response.url,status:e.params.response.status}));
      observed.errors=events.slice(start).filter(e=>e.sessionId===sessionId&&e.method==='Runtime.exceptionThrown').map(e=>e.params.exceptionDetails.text);
      report.writes.push(...events.slice(start).filter(e=>e.sessionId===sessionId&&e.method==='Network.requestWillBeSent'&&!['GET','HEAD'].includes(e.params.request.method)).map(e=>({url:e.params.request.url,method:e.params.request.method})));
      report.pages.push(observed);retain();console.log(path,observed.heading,observed.brokenImages.length+' broken images');
    }finally{await send('Target.closeTarget',{targetId});}
  }
},{timeoutMs:120000});}catch(error){report.failures.push({kind:'browser',error:error.message});}
for(const page of report.pages){
  if(!page.heading?.trim())report.failures.push({path:page.path,kind:'missing-heading'});
  for(const width of page.widths)if(!width.fits)report.failures.push({path:page.path,kind:'overflow',width:width.width});
  for(const url of page.brokenImages)report.failures.push({path:page.path,kind:'image',url});
  for(const response of page.responses)if(response.status>=400)report.failures.push({path:page.path,kind:'http',...response});
  for(const error of page.errors)report.failures.push({path:page.path,kind:'exception',error});
  for(const link of page.links){
    const url=new URL(link.href),target=report.pages.find(p=>p.path===url.pathname);
    if(url.origin!==origin||!target||!url.hash||url.hash.includes('='))continue;
    if(!target.ids.includes(decodeURIComponent(url.hash.slice(1))))report.failures.push({path:page.path,kind:'anchor',href:link.href,text:link.text});
  }
}
if(report.writes.length)report.failures.push({kind:'unexpected-write'});
if(report.pages.length!==paths.length)report.failures.push({kind:'incomplete-pages',expected:paths.length,observed:report.pages.length});
report.status=report.failures.length?'failed':'passed';retain();
console.log(JSON.stringify({pages:report.pages.length,failures:report.failures,writes:report.writes}));
if(report.failures.length)process.exitCode=1;
