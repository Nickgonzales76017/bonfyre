import {resolve,join} from 'node:path';
import {writeFileSync} from 'node:fs';
import assert from 'node:assert/strict';
import {withChrome} from '../../../integrations/browser/capture/chrome-session.mjs';
const out=resolve(process.argv[2]||'packs/no-simulation/creator-upstream-20261013/demo');
const allowed=new Set(['index.html','mix.wav','verification.json','timeline.json']);
const server=Bun.serve({hostname:'127.0.0.1',port:0,fetch(request){const path=new URL(request.url).pathname.slice(1)||'index.html';return allowed.has(path)?new Response(Bun.file(join(out,path))):new Response('Not found',{status:404});}});
const report={status:'running',layouts:[]};
try{await withChrome(async({open,events})=>{
  const tab=await open();await tab.go(`http://127.0.0.1:${server.port}/`);
  const evaluate=async expression=>{const r=await tab.page('Runtime.evaluate',{expression,awaitPromise:true,returnByValue:true});if(r.exceptionDetails)throw Error(r.exceptionDetails.text);return r.result.value;};
  report.audio=await evaluate(`new Promise((resolve,reject)=>{const start=Date.now();const poll=()=>{if(window.demoError)return reject(Error(window.demoError));if(window.demoReady)return resolve(window.demoReady);if(Date.now()-start>5000)return reject(Error('audio decode timeout'));setTimeout(poll,25)};poll()})`);
  assert.equal(report.audio.duration,6);assert.equal(report.audio.channels,2);
  for(const width of [1440,390]){
    await tab.page('Emulation.setDeviceMetricsOverride',{width,height:900,deviceScaleFactor:1,mobile:false});
    await evaluate('new Promise(requestAnimationFrame).then(()=>new Promise(requestAnimationFrame))');
    const layout=await evaluate('({width:innerWidth,scrollWidth:document.documentElement.scrollWidth,canvasWidth:document.querySelector("canvas").width,heading:document.querySelector("h1").textContent})');
    assert(layout.scrollWidth<=width);assert(layout.canvasWidth>100);report.layouts.push(layout);
    const shot=await tab.page('Page.captureScreenshot',{format:'png'});writeFileSync(join(out,`preview-${width}.png`),Buffer.from(shot.data,'base64'));
  }
  await evaluate('document.querySelector("audio").play()');
  await Bun.sleep(400);
  report.playback=await evaluate('({time:document.querySelector("audio").currentTime,paused:document.querySelector("audio").paused,error:document.querySelector("audio").error?.code??null})');
  assert(report.playback.time>0);assert.equal(report.playback.paused,false);assert.equal(report.playback.error,null);
  report.exceptions=events.filter(e=>e.sessionId===tab.sessionId&&e.method==='Runtime.exceptionThrown');assert.equal(report.exceptions.length,0);
},{timeoutMs:20000,flags:['--autoplay-policy=no-user-gesture-required']});report.status='passed';}
catch(error){report.status='failed';report.error=error.stack;process.exitCode=1;}
finally{server.stop(true);writeFileSync(join(out,'browser-verification.json'),JSON.stringify(report,null,2)+'\n');console.log(JSON.stringify(report));}
