import {test,beforeAll,afterAll,expect} from 'bun:test';
import {createServer as createHttpsServer} from 'node:https';
import {execFileSync} from 'node:child_process';
import {createRequire} from 'node:module';
import {readFileSync,writeFileSync,mkdirSync,mkdtempSync,rmSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {fixture,root,pause} from './auth_continuation_fixture.mjs';
// Actual native RSS projection and browser media decoding. Test audio is generated
// explicitly here. A separate, labelled stream-window case supplies media state
// to check moving-range controls; it is not evidence of a delivered live stream.
let f,browser,media,mediaBase,nativeHTML;
const calls=[],passed=[],errors=[];
const evidence=process.env.BONFYRE_QA_OUTPUT||mkdtempSync(join(tmpdir(),'bonfyre-public-listening-'));
function wav(seconds=12){const rate=16000,n=rate*seconds,b=Buffer.alloc(44+n*2);b.write('RIFF');b.writeUInt32LE(b.length-8,4);b.write('WAVEfmt ',8);b.writeUInt32LE(16,16);b.writeUInt16LE(1,20);b.writeUInt16LE(1,22);b.writeUInt32LE(rate,24);b.writeUInt32LE(rate*2,28);b.writeUInt16LE(2,32);b.writeUInt16LE(16,34);b.write('data',36);b.writeUInt32LE(n*2,40);for(let i=0;i<n;i++)b.writeInt16LE(Math.round(1500*Math.sin(2*Math.PI*220*i/rate)),44+2*i);return b;}
const wave=wav();
const tls=mkdtempSync(join(tmpdir(),'bonfyre-media-tls-'));
function createServer(handler){
 execFileSync('/usr/bin/openssl',['req','-x509','-newkey','rsa:2048','-nodes','-keyout',join(tls,'key.pem'),'-out',join(tls,'cert.pem'),'-days','1','-subj','/CN=localhost'],{stdio:'ignore',timeout:20000});
 return createHttpsServer({key:readFileSync(join(tls,'key.pem')),cert:readFileSync(join(tls,'cert.pem'))},handler);
}
// The throwaway browser visits only fixture traffic; other requests are aborted.
afterAll(()=>rmSync(tls,{recursive:true,force:true}));
beforeAll(async()=>{
 mkdirSync(evidence,{recursive:true});
 media=createServer(async(req,res)=>{calls.push(req.url);if(!/^\/(tone|missing|art)/.test(req.url)){try{if(!f)throw Error('Native fixture not ready');const r=await fetch(f.base+req.url,{method:req.method,headers:{Host:'127.0.0.1',Accept:req.headers.accept||'*/*'},redirect:'manual',signal:AbortSignal.timeout(10000)}),headers={};for(const[k,v]of r.headers)if(!['connection','transfer-encoding','content-length','content-encoding'].includes(k))headers[k]=v;const body=Buffer.from(await r.arrayBuffer());res.writeHead(r.status,headers);res.end(body);}catch(e){errors.push('Native TLS fixture: '+e.message);res.writeHead(503).end();}return;}if(req.url.startsWith('/missing')){res.writeHead(404).end();return;}if(req.url.startsWith('/art')){res.writeHead(200,{'Content-Type':'image/svg+xml'}).end('<svg xmlns="http://www.w3.org/2000/svg" width="100" height="100"><rect width="100" height="100" fill="#24533e"/></svg>');return;}const range=/bytes=(\d+)-(\d*)/.exec(req.headers.range||'');let start=range?Number(range[1]):0,end=range&&range[2]?Math.min(wave.length-1,Number(range[2])):wave.length-1;if(start>end){res.writeHead(416).end();return;}res.writeHead(range?206:200,{'Content-Type':'audio/wav','Content-Length':end-start+1,'Accept-Ranges':'bytes','Cache-Control':'no-store',...(range?{'Content-Range':`bytes ${start}-${end}/${wave.length}`}:{})});res.end(wave.subarray(start,end+1));});
 await new Promise(resolve=>media.listen(0,'127.0.0.1',resolve));mediaBase='https://127.0.0.1:'+media.address().port;
 f=await fixture();f.run(f.web,['domain','--db',join(f.home,'matrix.db'),'--host','127.0.0.1','--territory','live','--scope','public','--world','live','--repr','HTML','--evidence','fixture:public-listening']);
 f.run(f.cms,['entry','create','internet_source',JSON.stringify({family:'music',source:'qa-listening',label:'Listening fixture',says:'Generated test WAV recordings',url:'https://example.test/listening',twin:'qa-listening',state:'observed',extract:'rss:item',title_field:'title',meta_field:'',link_field:'feed:auto',image_field:'',image_base:'',sub_field:'',body_regime:'retained-public',status:'published',tier:'warm',kind:'audio'}),'--ns','root','--db',f.db,'--schemas',join(f.home,'schemas')]);
 const rss='<rss xmlns:media="http://search.yahoo.com/mrss/"><channel><title>Listening fixture</title>'+[0,1,2].map(i=>`<item><title>Recorded fixture ${i}</title><link>https://example.test/episode-${i}</link><guid>qa-listening-${i}</guid><pubDate>${new Date(Date.now()-(i+1)*60000).toUTCString()}</pubDate><description>Exact retained input ${i}.</description><enclosure url="${mediaBase}/${i===2?'missing':'tone'}-${i}.wav" type="audio/wav" length="${wave.length}"/></item>`).join('')+'</channel></rss>';
 const file=join(f.home,'foreign/bodies/qa-listening.xml');writeFileSync(file,rss);writeFileSync(join(f.home,'foreign/receipts/qa-listening.json'),JSON.stringify({body_path:file}));
 nativeHTML=(await f.request('/listen',{host:'127.0.0.1'})).body;expect(nativeHTML).toContain('bf-audio');expect(nativeHTML).toContain(mediaBase);
 const puppeteer=createRequire(join(root,'tools/creator-ingest/package.json'))('puppeteer');browser=await puppeteer.launch({executablePath:'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',headless:true,acceptInsecureCerts:true,args:['--no-sandbox','--disable-dev-shm-usage']});
},30000);
afterAll(async()=>{await browser?.close();await f?.close();if(media)await new Promise(resolve=>media.close(resolve));writeFileSync(join(evidence,'verification.tsv'),'scope\tnative RSS + generated local WAV + Chrome, no customer account or publication\nweb_sha256\t'+(f?.fingerprint||'')+'\n'+passed.map(name=>'pass\t'+name).join('\n')+'\n'+errors.map(e=>'browser_error\t'+e.replaceAll('\n',' ')).join('\n'));console.log('PUBLIC_LISTENING_EVIDENCE '+evidence);},30000);
async function open(width=1440,prepare=null){
 const context=await browser.createBrowserContext(),page=await context.newPage();page.setDefaultTimeout(10000);
 // Request interception has no socket address. Chrome consequently requires
 // explicit loopback access for this isolated test's real local audio server.
 // Scope this permission to the disposable origin/context, never the live site.
 const transport=await page.createCDPSession();
 await transport.send('Browser.grantPermissions',{permissions:['localNetworkAccess'],origin:f.base,browserContextId:context.id});
 await page.setViewport({width,height:950});page.on('pageerror',e=>errors.push(e.message));
 if(prepare)await page.evaluateOnNewDocument(prepare);
 page.qaMediaDiagnostics=[];
 page.on('requestfailed',r=>{if(r.url().startsWith(mediaBase))page.qaMediaDiagnostics.push({failure:r.failure()?.errorText});});
 page.on('console',m=>{if(m.type()==='error')page.qaMediaDiagnostics.push({console:m.text().slice(0,300)});});
 await page.setRequestInterception(true);
 // One isolated TLS origin serves native HTML/SVG and real WAV range requests.
 // This avoids crossing Chrome's loopback permission boundary in the fixture;
 // neither HTMLMediaElement nor playback success is substituted.
 page.on('request',request=>{const url=new URL(request.url());if(url.origin===mediaBase||url.protocol==='data:')void request.continue();else void request.abort();});
 await page.goto(mediaBase+'/listen',{waitUntil:'networkidle0'});
 await page.waitForSelector('[data-deck][data-ready="true"]');
 return {page,close:()=>context.close()};
}
async function play(page,index=0){
 const buttons=await page.$$('li[data-play] .lift');if(!buttons[index])throw Error('Recording control is missing');
 try{await buttons[index].click();}finally{await Promise.all(buttons.map(b=>b.dispose()));}
 try{await page.waitForFunction(()=>{const a=document.querySelector('[data-deck] audio');return !a.paused&&a.currentTime>0.15&&a.readyState>=2;});}
 catch(error){
  const state=await page.evaluate(()=>({players:[...document.querySelectorAll('[data-deck] audio')].map(a=>({paused:a.paused,time:a.currentTime,ready:a.readyState,network:a.networkState,error:a.error?.code,src:a.currentSrc})),rows:[...document.querySelectorAll('li[data-play]')].map(n=>({title:n.dataset.title,url:n.dataset.play,selected:n.getAttribute('aria-current')})),status:document.querySelector('[data-deck-status]')?.textContent}));
  throw Error(error.message+'; actual browser media state '+JSON.stringify({...state,transport:page.qaMediaDiagnostics,mediaRequests:calls}));
 }
}
function check(name){passed.push(name);}

test('native source projection does not preload recordings or QR codes',async()=>{const before=calls.length,p=await open();try{expect(await p.page.$eval('[data-deck] audio',a=>a.getAttribute('src'))).toBeNull();expect(calls.slice(before).filter(x=>x.includes('.wav'))).toHaveLength(0);expect(await p.page.$eval('[data-deck-code-image]',n=>n.getAttribute('src'))).toBeNull();expect(await p.page.$eval('[data-deck-position]',n=>n.disabled)).toBe(true);check('No recording or code requested before user action');}finally{await p.close();}},20000);

test('shared player and recording deck never play at the same time',async()=>{
 const p=await open();try{
  expect(await p.page.$('[data-ak-player]')).not.toBeNull();
  await play(p.page);
  await p.page.evaluate(src=>{const b=document.createElement('button');b.type='button';b.dataset.akTrack='';b.dataset.src=src;b.textContent='Play shared fixture';document.querySelector('main').prepend(b);},mediaBase+'/tone-shared.wav');
  await p.page.click('[data-src$="tone-shared.wav"]');
  expect(await p.page.$eval('[data-deck] audio',a=>a.paused)).toBe(true);
  expect(await p.page.$eval('.ak-play',b=>b.getAttribute('aria-label'))).toBe('Pause');
  await play(p.page,1);
  expect(await p.page.$eval('.ak-play',b=>b.getAttribute('aria-label'))).toBe('Play');
  check('Recording deck and shared player stop one another');
 }finally{await p.close();}
},30000);

test('Stitch audio queue filters real retained recordings, selects exact sources and remains usable on mobile',async()=>{
 const before=calls.length,p=await open(390);
 try{
  expect(await p.page.$$eval('[data-deck-browser-list] button',n=>n.map(x=>x.textContent))).toEqual([
    '1. Recorded fixture 0 · example.test','2. Recorded fixture 1 · example.test','3. Recorded fixture 2 · example.test']);
  expect(calls.slice(before).some(x=>x.includes('.wav'))).toBe(false);
  await p.page.click('[data-deck-browser] summary');
  await p.page.type('[data-deck-browser-search]','fixture 1');
  expect(await p.page.$$eval('[data-deck-browser-list] li:not([hidden])',n=>n.map(x=>x.textContent))).toEqual(['2. Recorded fixture 1 · example.test']);
  expect(await p.page.$$eval('[data-deck-browser-list] li:not([hidden])',n=>n.map(x=>x.textContent))).toEqual(['2. Recorded fixture 1 · example.test']);
  await p.page.click('[data-queue-index="1"]');
  expect(await p.page.$eval('[data-deck-title]',n=>n.textContent)).toBe('Recorded fixture 1');
  expect(await p.page.$eval('[data-queue-index="1"]',n=>n.getAttribute('aria-current'))).toBe('true');
  expect(await p.page.$eval('[data-deck-browser]',n=>n.open)).toBe(false);
  await p.page.click('[data-deck-browser] summary');
  await p.page.focus('[data-deck-browser-search]');await p.page.keyboard.press('Escape');
  expect(await p.page.$eval('[data-deck-browser]',n=>n.open)).toBe(false);
  expect(await p.page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);
  check('Stitch listening queue matches exact retained rows, filters and selects without eager playback');
 }finally{await p.close();}
},20000);
test('real decoded audio progresses and native controls change speed, volume and seek position',async()=>{const p=await open();try{await play(p.page);expect(await p.page.$eval('audio',a=>a.duration)).toBeCloseTo(12,1);await p.page.select('[data-deck-rate]','1.5');expect(await p.page.$eval('audio',a=>a.playbackRate)).toBe(1.5);await p.page.$eval('[data-deck-volume]',n=>{n.value='0.35';n.dispatchEvent(new Event('input',{bubbles:true}));});expect(await p.page.$eval('audio',a=>a.volume)).toBeCloseTo(.35);await p.page.click('[data-deck-play]');expect(await p.page.$eval('audio',a=>a.paused)).toBe(true);await p.page.$eval('[data-deck-position]',n=>{n.value='500';n.dispatchEvent(new Event('input',{bubbles:true}));n.dispatchEvent(new Event('change',{bubbles:true}));});expect(await p.page.$eval('audio',a=>a.currentTime)).toBeCloseTo(6,1);expect(await p.page.$eval('[data-deck-position]',n=>n.getAttribute('aria-valuetext'))).toContain('0:06');check('Actual WAV decode, time advancement, speed, volume and seek');}finally{await p.close();}},20000);
test('selected recording opens the exact native page and existing QR representation',async()=>{const p=await open();try{await play(p.page);await p.page.click('[data-deck-play]');const href=await p.page.$eval('[data-deck-page]',n=>new URL(n.href).pathname);expect(href).toMatch(/^\/o\/qa-listening\/[a-f0-9]{24}$/);expect(await p.page.$eval('[data-deck-code-image]',n=>n.getAttribute('src'))).toBeNull();await p.page.click('[data-deck-code] summary');await p.page.waitForFunction(()=>{const i=document.querySelector('[data-deck-code-image]');return i.complete&&i.naturalWidth>0;});const response=await f.request(href+'.qr.svg',{host:'127.0.0.1'});expect(response.status).toBe(200);expect(response.headers.get('content-type')).toContain('image/svg+xml');expect(response.body).toContain('<desc>https://127.0.0.1'+href+'</desc>');await p.page.click('[data-deck-next]');await p.page.waitForFunction(old=>new URL(document.querySelector('[data-deck-page]').href).pathname!==old,{},href);expect(await p.page.$eval('[data-deck-code]',n=>n.open)).toBe(false);expect(await p.page.$eval('[data-deck-code-image]',n=>n.getAttribute('src'))).toBeNull();check('Selected source page and on-demand native QR change together');}finally{await p.close();}},20000);
test('space on another control does not hijack the page or start audio',async()=>{const p=await open();try{await p.page.evaluate(()=>{const b=document.createElement('button');b.id='unrelated';b.textContent='Another action';b.onclick=()=>b.dataset.clicked='yes';document.body.prepend(b);const e=document.createElement('div');e.contentEditable='true';e.id='editor';document.body.prepend(e);});await p.page.focus('#unrelated');await p.page.keyboard.press('Space');expect(await p.page.$eval('#unrelated',n=>n.dataset.clicked)).toBe('yes');expect(await p.page.$eval('audio',a=>a.getAttribute('src'))).toBeNull();await p.page.focus('#editor');await p.page.keyboard.type('a b');expect(await p.page.$eval('#editor',n=>n.textContent)).toBe('a b');expect(await p.page.$eval('audio',a=>a.getAttribute('src'))).toBeNull();await p.page.focus('[data-deck]');await p.page.keyboard.press('Space');await p.page.waitForFunction(()=>!document.querySelector('audio').paused);check('Focused player keyboard commands leave other controls and text editing intact');}finally{await p.close();}},20000);
test('an unavailable recording reports its failure without skipping or retrying an effect',async()=>{const p=await open();try{const before=calls.length;await p.page.$$eval('li[data-play] .lift',buttons=>buttons.at(-1).click());await p.page.waitForFunction(()=>/unavailable|could not play/.test(document.querySelector('[data-deck-status]').textContent));expect(await p.page.$eval('[data-deck-queue]',n=>n.textContent)).toBe('3 of 3');expect(await p.page.$eval('[data-deck-next]',n=>n.disabled)).toBe(true);expect(await p.page.$eval('[data-deck-page]',n=>n.hidden)).toBe(false);expect(calls.slice(before).filter(x=>x.startsWith('/tone'))).toHaveLength(0);check('Failed media remains selected with a useful source-page exit');}finally{await p.close();}},20000);
test('same emitted player supports multiple independent lists with one active recording',async()=>{const p=await open();try{await p.page.evaluate(html=>{const doc=new DOMParser().parseFromString(html,'text/html'),deck=doc.querySelector('[data-deck]'),script=deck.nextElementSibling,styles=[];let list=deck.previousElementSibling;while(list?.tagName==='STYLE'){styles.unshift(list);list=list.previousElementSibling;}const section=document.createElement('section');section.id='peer';section.append(list.cloneNode(true),...styles.map(n=>n.cloneNode(true)),deck.cloneNode(true));document.querySelector('main').append(section);const run=document.createElement('script');run.textContent=script.textContent;section.append(run);},nativeHTML);expect(await p.page.$$eval('[data-deck]',n=>n.length)).toBe(2);await play(p.page);await p.page.click('#peer [data-deck-play]');await p.page.waitForFunction(()=>{const a=[...document.querySelectorAll('[data-deck] audio')];return a[0].paused&&!a[1].paused&&a[1].currentTime>.1;});expect(await p.page.$$eval('[data-deck] audio',nodes=>nodes.filter(n=>!n.paused).length)).toBe(1);await p.page.$eval('#peer',node=>{document.dispatchEvent(new CustomEvent('htmx:beforeCleanupElement',{detail:{elt:node}}));});expect(await p.page.$eval('#peer audio',a=>a.getAttribute('src'))).toBeNull();check('Two identical native renderer instances coordinate playback and dispose independently');}finally{await p.close();}},20000);
test('phone layout, keyboard seek and reduced motion work with real media',async()=>{const p=await open(390);try{await play(p.page);await p.page.click('[data-deck-play]');await p.page.focus('[data-deck-position]');await p.page.keyboard.press('ArrowRight');expect(await p.page.$eval('audio',a=>a.currentTime)).toBeGreaterThan(0);expect(await p.page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);const rect=await p.page.$eval('[data-deck]',n=>({width:n.getBoundingClientRect().width,height:n.getBoundingClientRect().height}));expect(rect.width).toBeLessThanOrEqual(390);expect(rect.height).toBeGreaterThan(100);await p.page.emulateMediaFeatures([{name:'prefers-reduced-motion',value:'reduce'}]);expect(await p.page.$eval('[data-deck-play]',n=>getComputedStyle(n).transitionDuration)).toBe('0s');await p.page.click('[data-deck-code] summary');await p.page.waitForFunction(()=>document.querySelector('[data-deck-code-image]').naturalWidth>0);expect(await p.page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);await p.page.screenshot({path:join(evidence,'listening-phone.png'),fullPage:true});await p.page.setViewport({width:1440,height:950});await p.page.screenshot({path:join(evidence,'listening-desktop.png'),fullPage:true});check('390px player and code disclosure, keyboard seek and reduced-motion styling');}finally{await p.close();}},20000);
test('native presentation and QR test browser has no uncaught exceptions',()=>{expect(errors).toEqual([]);check('No uncaught browser exceptions');});

test('history restoration retains the selected position and preferences without autoplay',async()=>{
 const p=await open();try{
  await play(p.page);await p.page.click('[data-deck-play]');await p.page.select('[data-deck-rate]','1.5');
  await p.page.$eval('[data-deck-volume]',n=>{n.value='0.35';n.dispatchEvent(new Event('input',{bubbles:true}));});
  await p.page.$eval('audio',a=>{a.currentTime=5;});
  await p.page.evaluate(()=>{window.dispatchEvent(new PageTransitionEvent('pagehide',{persisted:true}));window.dispatchEvent(new PageTransitionEvent('pageshow',{persisted:true}));});
  expect(await p.page.$eval('audio',a=>a.paused)).toBe(true);await p.page.click('[data-deck-play]');
  await p.page.waitForFunction(()=>{const a=document.querySelector('audio');return !a.paused&&a.currentTime>=5&&a.currentTime<8;},{timeout:1500});
  expect(await p.page.$eval('audio',a=>a.playbackRate)).toBe(1.5);expect(await p.page.$eval('audio',a=>a.volume)).toBeCloseTo(.35);check('Same-page history restoration retains position and preferences without autoplay; lifecycle events dispatched explicitly');
 }finally{await p.close();}},20000);

const captureMediaActions=()=>{
  const session=navigator.mediaSession;window.__mediaHandlers={};window.__positionUpdates=[];
  const register=session.setActionHandler.bind(session),position=session.setPositionState.bind(session);
  session.setActionHandler=(name,handler)=>{register(name,handler);window.__mediaHandlers[name]=handler;};
  session.setPositionState=state=>{position(state);window.__positionUpdates.push(state||null);};
};
test('device controls operate the same decoded audio and exact selected source',async()=>{
 const p=await open(390,captureMediaActions);try{
  expect(await p.page.evaluate(()=>navigator.mediaSession.metadata)).toBeNull();await play(p.page);
  expect(await p.page.evaluate(()=>navigator.mediaSession.metadata.title)).toBe('Recorded fixture 0');
  expect(await p.page.evaluate(()=>navigator.mediaSession.playbackState)).toBe('playing');
  expect(await p.page.evaluate(()=>window.__mediaHandlers.previoustrack)).toBeNull();
  await p.page.evaluate(()=>window.__mediaHandlers.pause({action:'pause'}));
  await p.page.waitForFunction(()=>document.querySelector('audio').paused&&navigator.mediaSession.playbackState==='paused');
  await p.page.evaluate(()=>window.__mediaHandlers.seekto({action:'seekto',seekTime:4}));
  expect(await p.page.$eval('audio',a=>a.currentTime)).toBeCloseTo(4,1);
  await p.page.evaluate(()=>window.__mediaHandlers.play({action:'play'}));
  await p.page.waitForFunction(()=>!document.querySelector('audio').paused&&document.querySelector('audio').currentTime>4.1);
  await p.page.evaluate(()=>window.__mediaHandlers.nexttrack({action:'nexttrack'}));
  await p.page.waitForFunction(()=>navigator.mediaSession.metadata?.title==='Recorded fixture 1'&&!document.querySelector('audio').paused);
  expect(await p.page.$eval('[data-deck-queue]',n=>n.textContent)).toBe('2 of 3');
  const positions=await p.page.evaluate(()=>window.__positionUpdates.filter(Boolean));
  expect(positions.length).toBeGreaterThan(0);expect(positions.every(p=>p.duration===12&&p.position>=0&&p.position<=12&&p.playbackRate>0)).toBe(true);
  await p.page.evaluate(()=>window.__mediaHandlers.stop({action:'stop'}));
  expect(await p.page.$eval('audio',a=>a.paused)).toBe(true);expect(await p.page.evaluate(()=>navigator.mediaSession.metadata)).toBeNull();
  expect(await p.page.evaluate(()=>window.__mediaHandlers.play)).toBeNull();
  check('Native MediaSession metadata and registered device handlers drive actual decoded audio; callbacks invoked explicitly, not a physical lockscreen test');
 }finally{await p.close();}},20000);
test('an inactive or removed player cannot clear or control another player device session',async()=>{
 const p=await open(1440,captureMediaActions);try{
  await play(p.page);await p.page.evaluate(()=>window.__oldPause=window.__mediaHandlers.pause);
  await p.page.evaluate(html=>{const doc=new DOMParser().parseFromString(html,'text/html'),deck=doc.querySelector('[data-deck]'),script=deck.nextElementSibling,styles=[];let list=deck.previousElementSibling;while(list?.tagName==='STYLE'){styles.unshift(list);list=list.previousElementSibling;}const section=document.createElement('section');section.id='device-peer';section.append(list.cloneNode(true),...styles.map(n=>n.cloneNode(true)),deck.cloneNode(true));section.querySelector('li').dataset.title='Independent second source';document.querySelector('main').append(section);const run=document.createElement('script');run.textContent=script.textContent;section.append(run);},nativeHTML);
  await p.page.click('#device-peer [data-deck-play]');await p.page.waitForFunction(()=>navigator.mediaSession.metadata?.title==='Independent second source'&&!document.querySelector('#device-peer audio').paused);
  await p.page.evaluate(()=>window.__oldPause({action:'pause'}));expect(await p.page.$eval('#device-peer audio',a=>a.paused)).toBe(false);
  await p.page.$eval('[data-deck]',elt=>document.dispatchEvent(new CustomEvent('htmx:beforeCleanupElement',{detail:{elt}})));
  expect(await p.page.evaluate(()=>navigator.mediaSession.metadata.title)).toBe('Independent second source');
  await p.page.evaluate(()=>window.__mediaHandlers.pause({action:'pause'}));expect(await p.page.$eval('#device-peer audio',a=>a.paused)).toBe(true);
  await p.page.$eval('#device-peer',elt=>document.dispatchEvent(new CustomEvent('htmx:beforeCleanupElement',{detail:{elt}})));
  expect(await p.page.evaluate(()=>navigator.mediaSession.metadata)).toBeNull();
  check('Device-session handoff, stale callback refusal, and independent removal');
 }finally{await p.close();}},20000);
test('an unsupported device API leaves the ordinary public player working',async()=>{
 const p=await open(390,()=>Object.defineProperty(navigator,'mediaSession',{value:undefined,configurable:true}));try{
  await play(p.page);await p.page.click('[data-deck-play]');expect(await p.page.$eval('audio',a=>a.paused)).toBe(true);
  check('No MediaSession API: public controls and actual media still work');
 }finally{await p.close();}},20000);
test('stream seek windows use the browser reported current range, not Infinity arithmetic',()=>{
 const header=readFileSync(join(root,'cmd/BonfyreWebAddress/src/public_audio_deck.h'),'utf8');
 const js=header.split('\n').filter(line=>line.startsWith('"')).map(line=>JSON.parse(line.replace(/;$/,''))).join('');
 const method=js.slice(js.indexOf('function bounds(){'),js.indexOf('function seek(value)'));
 const bounds=new Function('audio',method+'return bounds();');
 expect(bounds({duration:12})).toEqual([0,12]);expect(bounds({duration:NaN})).toBeNull();
 expect(bounds({duration:Infinity,seekable:{length:0}})).toBeNull();
 const ranges={length:2,start:i=>i===1?600:100,end:i=>i===1?900:200};
 expect(bounds({duration:Infinity,seekable:ranges})).toEqual([600,900]);
 expect(bounds({duration:Infinity,seekable:{length:1,start:()=>10,end:()=>10}})).toBeNull();
 check('Pure stream-window arithmetic on explicitly supplied ranges; no claim of a live broadcast');
});
test('visible stream controls reconcile a moving range and refuse a closed window',async()=>{
 const p=await open(390,captureMediaActions);try{
  await play(p.page);await p.page.click('[data-deck-play]');
  // Explicit browser media-state fixture. The decoded recording above is real;
  // Infinity and seekable below exercise the controller, not stream delivery.
  await p.page.evaluate(()=>{
   const a=document.querySelector('audio');window.__streamWindow={start:600,end:900,current:750};
   Object.defineProperties(a,{
    duration:{get:()=>Infinity,configurable:true},
    currentTime:{get:()=>window.__streamWindow.current,set:v=>{window.__streamWindow.current=v;},configurable:true},
    seekable:{get:()=>({length:1,start:()=>window.__streamWindow.start,end:()=>window.__streamWindow.end}),configurable:true}
   });a.dispatchEvent(new Event('durationchange'));
  });
  expect(await p.page.$eval('[data-deck-position]',n=>n.disabled)).toBe(false);
  expect(await p.page.$eval('[data-deck-position]',n=>n.value)).toBe('500');
  expect(await p.page.$eval('[data-deck-elapsed]',n=>n.textContent)).toBe('2:30 behind latest');
  expect(await p.page.$eval('[data-deck-duration]',n=>n.textContent)).toBe('5:00 window');
  await p.page.$eval('[data-deck-position]',n=>{n.value='250';n.dispatchEvent(new Event('input',{bubbles:true}));});
  await p.page.evaluate(()=>{window.__streamWindow.start=800;window.__streamWindow.end=1100;});
  await p.page.$eval('[data-deck-position]',n=>n.dispatchEvent(new Event('change',{bubbles:true})));
  expect(await p.page.evaluate(()=>window.__streamWindow.current)).toBe(875);
  await p.page.evaluate(()=>window.__mediaHandlers.seekto({action:'seekto',seekTime:700}));
  expect(await p.page.evaluate(()=>window.__streamWindow.current)).toBe(800);
  await p.page.click('[data-deck-latest]');expect(await p.page.evaluate(()=>window.__streamWindow.current)).toBeCloseTo(1099.9,3);
  expect(await p.page.$eval('[data-deck-latest]',n=>n.disabled)).toBe(true);
  await p.page.evaluate(()=>{window.__streamWindow.end=1150;document.querySelector('audio').dispatchEvent(new Event('progress'));});
  expect(await p.page.$eval('[data-deck-latest]',n=>n.disabled)).toBe(false);
  expect(await p.page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);
  await p.page.evaluate(()=>{window.__streamWindow.start=1150;window.__streamWindow.end=1150;document.querySelector('audio').dispatchEvent(new Event('progress'));});
  expect(await p.page.$eval('[data-deck-position]',n=>n.disabled)).toBe(true);
  expect(await p.page.$eval('[data-deck-latest]',n=>n.disabled)).toBe(true);
  expect(await p.page.evaluate(()=>window.__mediaHandlers.seekto)).toBeNull();
  check('Explicit moving-window state: visible and device seek, drag reconciliation, latest available, closed-window refusal and phone layout');
 }finally{await p.close();}
},20000);
test('device-session integration adds no uncaught browser errors',()=>{expect(errors).toEqual([]);});
