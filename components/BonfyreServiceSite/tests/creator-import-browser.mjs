import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import {readFileSync,writeFileSync,mkdirSync,mkdtempSync,rmSync} from 'node:fs';
import {resolve,join,sep} from 'node:path';
import {tmpdir} from 'node:os';
const live=process.argv.includes('--live'),root=resolve(process.env.CREATOR_BUILD??'packs/aurekai-work-samples/site/dist');
const out=resolve(process.env.CREATOR_IMPORT_PROOF??`packs/no-simulation/product-maturity-20261015/audio/${live?'live-desk':'local-desk'}`);mkdirSync(out,{recursive:true});
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
 const page=await browser.newPage();page.setDefaultTimeout(12000);page.on('pageerror',e=>report.errors.push(e.message));page.on('request',r=>{if(!['GET','HEAD'].includes(r.method()))report.writes.push({url:r.url(),method:r.method()});});
 const ready=async(p=page)=>p.waitForSelector('[data-audio="workspace"][data-ready=true][data-busy=false]');
 const set=async(selector,value,p=page)=>{await p.bringToFront();await p.click(selector,{clickCount:3});await p.keyboard.press('Backspace');await p.type(selector,value);};
 const upload=async paths=>{await (await page.$($('files'))).uploadFile(...paths);};
 const panel=async name=>{await page.click(`[data-audio-view="${name}"]`);await page.waitForSelector($(name)+':not([hidden])');};
 await page.setViewport({width:1440,height:960});await page.goto(origin+'/audio/?workspace=sample',{waitUntil:'networkidle2'});await page.waitForSelector('[data-creator][data-imports-ready=true]');
 await page.click('[data-creator-view=imports]');await upload([voice,bed]);await ready();check('two local WAVs open as editable tracks',await page.$$('[data-clip]').then(a=>a.length===2));
 check('mono and stereo inputs share a real two-second mix',await page.$eval($('duration'),n=>n.textContent)==='2.000 seconds');
 await set('[data-clip="0"] [data-edit=from]','0.25');await ready();await set('[data-clip="0"] [data-edit=to]','0.75');await ready();await set('[data-clip="0"] [data-edit=at]','0.5');await ready();
 await page.$eval('[data-clip="0"] [data-edit=gain]',n=>{n.value='0.8';n.dispatchEvent(new Event('input',{bubbles:true}));});await ready();
 const rendered=await page.$eval($('workspace'),n=>n.dataset.mixSha256);
 check('trimming and placement change the rendered session',rendered.length===64&&await page.$eval('[data-clip="0"] [data-edit=from]',n=>n.value)==='0.25');
 await page.click('[data-clip="1"] [data-edit=muted]');await ready();check('mute changes the audio bytes',await page.$eval($('workspace'),n=>n.dataset.mixSha256)!==rendered);await page.click('[data-clip="1"] [data-edit=muted]');await ready();
 await set('[data-clip="0"] [data-edit=to]','12');await page.waitForFunction(()=>document.querySelector('[data-audio="workspace"]').dataset.ready==='false');check('out-of-source trims block playback and export',await page.$eval($('play'),n=>n.disabled)&&await page.$eval($('download'),n=>n.disabled));
 await set('[data-clip="0"] [data-edit=to]','0.75');await ready();check('corrected trims recover the exact previous mix',await page.$eval($('workspace'),n=>n.dataset.mixSha256)===rendered);
 await page.click($('play'));await page.waitForFunction(()=>!document.querySelector('[data-audio="stop"]').disabled);check('imported session plays',true);await page.click($('stop'));check('stop ends playback',await page.$eval($('stop'),n=>n.disabled));
 for(const width of [1440,390,320]){await page.setViewport({width,height:960});check(`timeline fits ${width}px`,await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth));if(width!==320)await page.screenshot({path:join(out,`timeline-${width}.png`),fullPage:true});}
 await panel('export');await set($('title'),'Interview with music');await page.select($('permission'),'own');await page.click($('save'));await page.waitForFunction(()=>document.querySelector('[data-audio="saved"]').textContent==='Session saved in this browser.');
 const downloads=join(out,'downloads');mkdirSync(downloads,{recursive:true});const cdp=await page.createCDPSession();await cdp.send('Page.setDownloadBehavior',{behavior:'allow',downloadPath:downloads});await page.click($('download'));await page.click($('manifest'));
 let wav,notes;for(let i=0;i<40;i++){try{wav=readFileSync(join(downloads,'aurekai-audio-session.wav'));notes=readFileSync(join(downloads,'aurekai-audio-session.txt'),'utf8');break;}catch{await new Promise(r=>setTimeout(r,100));}}
 check('export is an actual stereo WAV with the edited duration',wav?.length===64044&&wav.readUInt32LE(24)===8000&&wav.readUInt16LE(22)===2);
 check('exported samples match an independent trim and gain calculation',wav.readInt16LE(44)===2048&&wav.readInt16LE(44+4000*4)===8601&&wav.readInt16LE(44+8000*4)===2048);
 check('session notes retain sources, edits and stated permission',notes.includes('Voice.wav')&&notes.includes('0.250 to 0.750')&&notes.includes('Creator states they made')&&notes.includes(rendered));
 await page.reload({waitUntil:'networkidle2'});await page.waitForSelector('[data-creator][data-imports-ready=true]');await ready();check('recordings and all edits survive reload',await page.$eval($('workspace'),n=>n.dataset.mixSha256)===rendered&&await page.$eval($('title'),n=>n.value)==='Interview with music');
 const other=await browser.newPage();await other.goto(page.url(),{waitUntil:'networkidle2'});await other.waitForSelector('[data-creator][data-imports-ready=true]');await ready(other);
 await set($('title'),'First tab revision');await page.click($('save'));await page.waitForFunction(()=>document.querySelector('[data-audio="saved"]').textContent==='Session saved in this browser.');
 await set($('title'),'Stale revision',other);await other.click($('save'));await other.waitForFunction(()=>document.querySelector('[data-audio="status"]').textContent.includes('another tab'));check('stale sessions cannot overwrite newer recordings or edits',await other.$eval($('save'),n=>n.disabled));await other.close();
 await panel('sources');page.once('dialog',d=>d.accept());await upload([voice,different]);await page.waitForFunction(()=>document.querySelector('[data-audio="status"]').textContent.includes('same sample rate'));check('incompatible imports preserve the previous session',await page.$eval($('workspace'),n=>n.dataset.mixSha256)===rendered);
 page.once('dialog',d=>d.accept());await upload([malformed]);await page.waitForFunction(()=>document.querySelector('[data-audio="status"]').textContent.includes('complete PCM'));check('malformed WAVs preserve the previous session',await page.$eval($('workspace'),n=>n.dataset.mixSha256)===rendered);
 await page.click('[data-creator-view=sample]');check('generated sample remains available',await page.$eval('[data-ui=sample-panel]',n=>!n.hidden));await page.goBack();await page.waitForSelector('[data-audio=workspace]:not([hidden])');check('Back restores the imported workspace',true);
 await page.reload({waitUntil:'networkidle2'});await page.waitForSelector('[data-creator][data-imports-ready=true]');await ready();check('refused imports never replace the saved source session',await page.$eval($('workspace'),n=>n.dataset.mixSha256)===rendered);
 await panel('sources');page.once('dialog',d=>d.accept());await upload([different]);await ready();check('a second session accepts a different sample rate independently',await page.$eval($('export-summary'),n=>n.textContent.includes('16000 Hz'))&&await page.$$('[data-clip]').then(a=>a.length===1));
 await set('[data-clip="0"] [data-edit=at]','0.2');await ready();check('editing preserves a source ending between milliseconds',await page.$eval('[data-clip="0"] [data-edit=to]',n=>Math.abs(Number(n.value)-1.0000625)<0.000001));
 await panel('export');check('replacement recordings reset the permission review',await page.$eval($('permission'),n=>n.value)==='unreviewed');await set($('title'),'Second recording session');await page.click($('save'));await page.waitForFunction(()=>document.querySelector('[data-audio="saved"]').textContent==='Session saved in this browser.');
 const secondHash=await page.$eval($('workspace'),n=>n.dataset.mixSha256);await page.reload({waitUntil:'networkidle2'});await page.waitForSelector('[data-creator][data-imports-ready=true]');await ready();check('second session replaces only the imported draft and survives reload',await page.$eval($('workspace'),n=>n.dataset.mixSha256)===secondHash&&await page.$eval($('title'),n=>n.value)==='Second recording session');
 // Corrupt only this disposable browser's stored source; restoration must refuse it.
 await page.evaluate(async()=>{const cfg=JSON.parse(document.querySelector('[data-creator-config]').textContent),base=new URL(document.querySelector('script[src$="creator-client.mjs"]').src),{readLocal,saveLocal}=await import(new URL('store.mjs',base));const key='draft:creator:'+cfg.id+':imports',row=(await readLocal(key)).find(r=>r.key===key);new Uint8Array(row.value.files[0].bytes)[44]^=1;await saveLocal(key,row.value,row.revision);});
 await page.reload({waitUntil:'networkidle2'});await page.waitForSelector('[data-creator][data-imports-ready=true]');check('changed saved source bytes block restoration and saving',await page.$eval($('status'),n=>n.textContent.includes('source has changed'))&&await page.$eval($('download'),n=>n.disabled)&&await page.$eval($('save'),n=>n.disabled));
 check('no browser exceptions',report.errors.length===0);check('no remote writes or uploads',report.writes.length===0);report.status='passed';
}catch(error){report.status='failed';report.error=error.stack;process.exitCode=1;}
finally{await browser?.close();server?.stop(true);rmSync(profile,{recursive:true,force:true});writeFileSync(join(out,'verification.json'),JSON.stringify(report,null,2)+'\n');console.log(JSON.stringify(report));}
