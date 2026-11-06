import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import {readFileSync,writeFileSync,mkdirSync,mkdtempSync,rmSync} from 'node:fs';
import {resolve,join,sep} from 'node:path';
import {tmpdir} from 'node:os';
const live=process.argv.includes('--live'),root=resolve(process.env.CREATOR_BUILD??'packs/aurekai-work-samples/site/dist');
const out=resolve(process.env.CREATOR_TIMELINE_PROOF??`packs/no-simulation/product-maturity-20261015/audio/${live?'live-arrangement':'local-arrangement'}`);mkdirSync(out,{recursive:true});
const profile=mkdtempSync(join(tmpdir(),'creator-import-'));
const puppeteer=createRequire(resolve('tools/creator-ingest/package.json'))('puppeteer');
let browser;
const server=live?null:Bun.serve({hostname:'127.0.0.1',port:0,async fetch(request){
 if(!['GET','HEAD'].includes(request.method))return new Response('No writes',{status:405});
 const url=new URL(request.url);let file=resolve(root,'.'+decodeURIComponent(url.pathname));
 if(file!==root&&!file.startsWith(root+sep))return new Response('Not found',{status:404});
 if(url.pathname.endsWith('/'))file=join(file,'index.html');
 const content=Bun.file(file);return await content.exists()?new Response(content,{headers:{'Cache-Control':'no-store'}}):new Response('Not found',{status:404});
}});
const origin=live?'https://aurekai-work-samples.pages.dev':`http://127.0.0.1:${server.port}`;
const report={origin,checks:[],errors:[],writes:[],status:'running'},check=(name,value)=>{assert(value,name);report.checks.push(name);console.log('PASS '+name);};
const $=key=>`[data-audio="${key}"]`;
import {wavBytes} from '../creator-sample.mjs';
const fixtures=join(profile,'inputs');mkdirSync(fixtures,{recursive:true});
const fixture=(name,rate,channels,frames,value)=>{const path=join(fixtures,name);writeFileSync(path,new Uint8Array(wavBytes(new Float32Array(frames*channels).fill(value),{sample_rate:rate,channels,frames})));return path;};
const voice=fixture('Voice.wav',8000,1,8000,.25),bed=fixture('Bed.wav',8000,2,16000,.125),different=fixture('Other-rate.wav',16000,1,16001,.25);
const malformed=join(fixtures,'Broken.wav');writeFileSync(malformed,'not a WAV');
try{
 browser=await puppeteer.launch({headless:true,executablePath:'/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',userDataDir:profile,args:['--autoplay-policy=no-user-gesture-required']});
 const page=await browser.newPage();page.setDefaultTimeout(15000);page.on('pageerror',e=>report.errors.push(e.message));page.on('request',r=>{if(!['GET','HEAD'].includes(r.method()))report.writes.push({url:r.url(),method:r.method()});});
 const ready=async()=>page.waitForSelector('[data-audio="workspace"][data-ready=true][data-busy=false]');
 const hash=()=>page.$eval($('workspace'),n=>n.dataset.mixSha256);
 const value=(i,field)=>page.$eval(`[data-clip="${i}"] [data-edit="${field}"]`,n=>Number(n.value));
 const drag=async(i,action,seconds)=>{
  const selector=`[data-clip="${i}"] [data-gesture="${action}"]`,box=await page.$eval(selector,n=>{n.scrollIntoView({block:'center'});const b=n.getBoundingClientRect(),t=n.closest('.creator-lane-track').getBoundingClientRect();return {x:b.x+b.width/2,y:b.y+b.height/2,width:t.width};});
  await page.mouse.move(box.x,box.y);await page.mouse.down();await page.mouse.move(box.x+box.width/6*seconds,box.y,{steps:12});await page.mouse.up();await ready();
 };
 await page.setViewport({width:1440,height:1050});await page.goto(origin+'/audio/',{waitUntil:'networkidle2'});await page.waitForSelector('[data-creator][data-imports-ready=true]');await ready();
 check('default route opens a populated three-track arrangement',await page.$$('[data-clip]').then(a=>a.length===3)&&await page.$eval($('workspace'),n=>!n.hidden));
 await page.waitForSelector('[data-audio="source-wave"][data-ready=true]');
 check('self-hosted WaveSurfer source selection loads',await page.$eval($('source-wave'),n=>Boolean(n.querySelector('div')?.shadowRoot?.querySelector('[part~="region"]'))));
 check('generated samples are labeled and do not claim a saved session',await page.$eval($('saved'),n=>n.textContent==='Sample tracks · not saved.'));
 await page.screenshot({path:join(out,'initial-1440.png'),fullPage:true});
 const original=await hash();await drag(1,'move',.5);
 check('pointer drag moves a clip by snapped timeline time',await value(1,'at')===2&&await hash()!==original);
 const moved=await hash();await page.click($('undo'));await ready();check('undo restores exact rendered audio',await hash()===original&&await value(1,'at')===1.5);
 await page.click($('redo'));await ready();check('redo restores the clip placement and output',await hash()===moved&&await value(1,'at')===2);
 await drag(0,'start',.3);check('left trim changes source start and preserves the right edge',await value(0,'from')===.3&&await value(0,'at')===.3&&await value(0,'to')===1);
 await drag(0,'end',-.2);check('right trim removes audio at the source boundary',await value(0,'to')===.8);
 await page.focus('[data-clip="0"] [data-gesture=move]');await page.keyboard.press('ArrowRight');await ready();check('keyboard clip movement renders the same contract',await value(0,'at')===.4);
 await page.keyboard.down('Control');await page.keyboard.press('z');await page.keyboard.up('Control');await ready();check('keyboard undo restores the previous placement',await value(0,'at')===.3);
 await page.click('[data-select-track="2"]');await page.waitForFunction(()=>document.querySelector('[data-audio=selection-name]').textContent.includes('Harmony'));
 await page.waitForFunction(()=>Boolean(document.querySelector('[data-audio=source-wave] div')?.shadowRoot?.querySelector('[part~="region-handle-left"]')));
 const handle=await page.$eval($('source-wave'),n=>{const root=n.querySelector('div').shadowRoot,h=root.querySelector('[part~="region-handle-left"]'),b=h.getBoundingClientRect(),w=n.getBoundingClientRect();return {x:b.x+b.width/2,y:b.y+b.height/2,width:w.width};});
 await page.mouse.move(handle.x,handle.y);await page.mouse.down();await page.mouse.move(handle.x+handle.width*.2,handle.y,{steps:12});await page.mouse.up();await ready();
 check('WaveSurfer region resizing changes the native clip source',Math.abs(await value(2,'from')-.3)<.02&&await value(2,'at')===2);
 await page.$eval($('zoom'),n=>{n.value='3';n.dispatchEvent(new Event('input',{bubbles:true}));});
 check('zoom enlarges the real timeline and preserves edit values',await page.$eval('.creator-lane-scroll',n=>n.scrollWidth>n.clientWidth*2.9)&&await value(0,'at')===.3);
 await page.$eval('.creator-lane-scroll',n=>n.scrollLeft=100);await page.waitForFunction(()=>[...document.querySelectorAll('.creator-lane-scroll')].every(n=>n.scrollLeft===100));check('tracks scroll against the same time ruler',await page.$eval('.creator-ruler-scroll',n=>n.scrollLeft)===100);
 await page.click($('fit'));await page.$eval($('seek'),n=>{n.value='1';n.dispatchEvent(new Event('input',{bubbles:true}));});
 await page.click($('play'));await page.waitForSelector('[data-audio=workspace][data-playing=true]');await new Promise(r=>setTimeout(r,150));await page.click($('stop'));
 check('playback starts at the chosen cursor and retains its stop position',await page.$eval($('seek'),n=>Number(n.value))>1&&await page.$eval($('workspace'),n=>n.dataset.playing)==='false');
 await page.click($('save'));await page.waitForFunction(()=>document.querySelector('[data-audio=saved]').textContent==='Session saved in this browser.');const savedHash=await hash();
 await page.reload({waitUntil:'networkidle2'});await page.waitForSelector('[data-creator][data-imports-ready=true]');await ready();check('gesture edits and original recordings survive reload',await hash()===savedHash&&await value(0,'at')===.3&&await value(1,'at')===2);
 for(const width of [1440,390,320]){await page.setViewport({width,height:1050});await new Promise(r=>setTimeout(r,350));check(`arrangement stays inside ${width}px viewport`,await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth));if(width!==320)await page.screenshot({path:join(out,`edited-${width}.png`),fullPage:true});}
 // Exported PCM is independently calculated from the original packaged WAVs and saved clips.
 await page.click('[data-audio-view=export]');
 const downloads=join(out,'downloads');mkdirSync(downloads,{recursive:true});const cdp=await page.createCDPSession();await cdp.send('Page.setDownloadBehavior',{behavior:'allow',downloadPath:downloads});await page.click($('download'));
 let exported;for(let i=0;i<40;i++){try{exported=readFileSync(join(downloads,'aurekai-audio-session.wav'));break;}catch{await new Promise(r=>setTimeout(r,100));}}
 const stored=await page.evaluate(async()=>{const base=new URL(document.querySelector('script[src$="creator-client.mjs"]').src),{readLocal}=await import(new URL('store.mjs',base)),sample=JSON.parse(document.querySelector('[data-creator-config]').textContent),row=(await readLocal('draft:creator:'+sample.id+':imports'))[0];return {clips:row.value.clips,files:row.value.files.map(f=>Array.from(new Uint8Array(f.bytes)))};});
 const {decodeWav}=await import('../creator-import.mjs'),sources=stored.files.map(b=>decodeWav(new Uint8Array(b).buffer)),frames=Math.max(...stored.clips.map(c=>c.at+c.frames));
 check('gesture-edited export has the scheduled frame count',exported.length===44+frames*4);
 let exact=true;
 for(let f=0;f<frames&&exact;f++)for(let channel=0;channel<2;channel++){
  let expected=0;stored.clips.forEach((c,i)=>{if(!c.muted&&f>=c.at&&f<c.at+c.frames)expected=Math.fround(expected+Math.fround(sources[i].samples[(c.from+f-c.at)*2+channel]*c.gain));});
  expected=Math.max(-1,Math.min(1,expected));if(exported.readInt16LE(44+(f*2+channel)*2)!==Math.round(expected*(expected<0?32768:32767))){exact=false;break;}
 }
 check('every exported sample matches independent source placement and trim math',exact);
 // A failed optional waveform module must leave the native editor usable and retryable.
 const recovery=await browser.newPage();await recovery.setRequestInterception(true);let blocked=true;
 recovery.on('request',request=>{if(blocked&&request.url().includes('/regions.esm.js'))request.abort();else request.continue();});
 await recovery.goto(origin+'/audio/',{waitUntil:'networkidle2'});await recovery.waitForSelector('[data-audio=wave-retry]:not([hidden])');
 check('optional waveform failure preserves native playback and export',await recovery.$eval('[data-audio=play]',n=>!n.disabled)&&await recovery.$eval('[data-audio=download]',n=>!n.disabled));
 blocked=false;await recovery.click('[data-audio=wave-retry]');await recovery.waitForSelector('[data-audio=source-wave][data-ready=true]');check('waveform retry recovers without losing the saved session',await recovery.$eval('[data-audio=workspace]',n=>n.dataset.mixSha256)===savedHash);await recovery.close();
 check('no browser exceptions',report.errors.length===0);check('no remote writes or uploads',report.writes.length===0);report.status='passed';
}catch(error){report.status='failed';report.error=error.stack;process.exitCode=1;try{await (await browser.pages()).at(-1).screenshot({path:join(out,'failure.png'),fullPage:true});}catch{}}
finally{await browser?.close();server?.stop(true);rmSync(profile,{recursive:true,force:true});writeFileSync(join(out,'verification.json'),JSON.stringify(report,null,2)+'\n');console.log(JSON.stringify(report));}
