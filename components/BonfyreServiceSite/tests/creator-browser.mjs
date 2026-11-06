import assert from 'node:assert/strict';
import {resolve, join, sep} from 'node:path';
import {readFileSync, writeFileSync, mkdirSync, existsSync} from 'node:fs';
import {withChrome} from '../../../integrations/browser/capture/chrome-session.mjs';

const live = process.argv.includes('--live');
const root = resolve(import.meta.dir, '../../..'), site = resolve(process.env.CREATOR_BUILD || join(root, 'packs/no-simulation/creator-site-20261013/dist'));
const out = resolve(process.env.CREATOR_BROWSER_PROOF || join(root, 'packs/no-simulation/creator-site-20261013', live ? 'live' : 'local'));
mkdirSync(out, {recursive: true});
let fault = '', server;
if (!live) server = Bun.serve({hostname: '127.0.0.1', port: 0, async fetch(request) {
  const url = new URL(request.url); let path = resolve(site, '.' + decodeURIComponent(url.pathname));
  if (!path.startsWith(site + sep) && path !== site) return new Response('Not found', {status: 404});
  if (url.pathname.endsWith('/')) path = join(path, 'index.html');
  const file = Bun.file(path); if (!await file.exists()) return new Response('Not found', {status: 404});
  if (fault && path.endsWith('pulse.f32')) {if (fault === 'missing') return new Response('Missing', {status: 404}); const bytes = new Uint8Array(await file.arrayBuffer()); bytes[0] ^= 1; return new Response(bytes);}
  return new Response(file, {headers: {'Cache-Control': 'no-store'}});
}});
const origin = live ? 'https://aurekai-work-samples.pages.dev' : `http://127.0.0.1:${server.port}`;
const report = {origin, status: 'running', checks: [], layouts: [], failures: []};
const check = (name, value) => {assert(value, name); report.checks.push(name);};
const reference = JSON.parse(readFileSync(join(root, 'packs/aurekai-work-samples/audio/sample.json'))).native_sha256;
try {
  await withChrome(async ({open, events, send}) => {
    const downloads = join(out, 'downloads'); mkdirSync(downloads, {recursive: true});
    await send('Browser.setDownloadBehavior', {behavior: 'allow', downloadPath: downloads});
    const tab = await open();
    const evaluate = async (expression, target = tab) => {const r = await target.page('Runtime.evaluate', {expression, awaitPromise: true, returnByValue: true}); if (r.exceptionDetails) throw Error(JSON.stringify(r.exceptionDetails)); return r.result.value;};
    const wait = (expression, target = tab) => evaluate(`new Promise((resolve,reject)=>{const start=Date.now();const poll=()=>{if(${expression})return resolve(true);if(Date.now()-start>15000)return reject(Error('readiness timeout'));setTimeout(poll,25)};poll()})`, target);
    const ready = target => wait(`document.querySelector('[data-creator]')?.dataset.ready==='true'&&document.querySelector('[data-creator]').dataset.busy==='false'`, target);
    const setGain = async (value, target = tab) => {await evaluate(`(()=>{const input=document.querySelector('[data-gain]');input.value=${value};input.dispatchEvent(new Event('input'));})()`, target); await ready(target);};
    await tab.go(origin + '/');
    check('home page links to audio sample', await evaluate(`Boolean(document.querySelector('#samples a[href="/audio/"]'))`));
    await tab.go(origin + '/audio/?workspace=sample'); await ready(tab);
    report.initial = await evaluate(`({...document.querySelector('[data-creator]').dataset})`);
    check('initial mix matches native bytes', report.initial.mixSha256 === reference && report.initial.referenceVerified === 'true');
    for (const width of [1440, 390]) {
      await tab.page('Emulation.setDeviceMetricsOverride', {width, height: 960, deviceScaleFactor: 1, mobile: false});
      await evaluate('document.fonts.ready.then(()=>new Promise(requestAnimationFrame))');
      const layout = await evaluate(`({width:innerWidth,scroll:document.documentElement.scrollWidth,canvas:document.querySelector('canvas').width})`);
      check(`layout fits ${width}px`, layout.scroll <= width && layout.canvas > 100); report.layouts.push(layout);
      const shot = await tab.page('Page.captureScreenshot', {format: 'png'}); writeFileSync(join(out, `audio-${width}.png`), Buffer.from(shot.data, 'base64'));
    }
    await setGain(.3);
    check('gain changes rendered output', await evaluate(`document.querySelector('[data-creator]').dataset.mixSha256!==${JSON.stringify(reference)}`));
    await evaluate(`document.querySelector('[data-ui="save"]').click()`);
    await wait(`document.querySelector('[data-ui="saved"]').textContent.includes('Levels saved')`);
    await tab.go(origin + '/audio/?workspace=sample'); await ready(tab);
    check('saved levels survive reload', await evaluate(`document.querySelector('[data-gain]').value==='0.3'`));
    const history = await tab.page('Page.getNavigationHistory'), audioEntry = history.entries[history.currentIndex].id;
    await tab.go(origin + '/'); await tab.page('Page.navigateToHistoryEntry', {entryId: audioEntry});
    const backDeadline = Date.now() + 15000; let restored = false;
    while (Date.now() < backDeadline) {
      try {restored = await evaluate(`document.querySelector('[data-creator]')?.dataset.ready==='true'&&document.querySelector('[data-creator]').dataset.busy==='false'`);} catch {}
      if (restored) break; await Bun.sleep(50);
    }
    check('browser Back restores a working saved mix', restored && await evaluate(`document.querySelector('[data-gain]').value==='0.3'&&!document.querySelector('[data-ui="play"]').disabled`));
    const stale = await open(); await stale.go(origin + '/audio/?workspace=sample'); await ready(stale);
    await setGain(.6); await evaluate(`document.querySelector('[data-ui="save"]').click()`); await wait(`document.querySelector('[data-ui="saved"]').textContent.includes('Levels saved')`);
    await setGain(.2, stale); await evaluate(`document.querySelector('[data-ui="save"]').click()`, stale);
    await wait(`document.querySelector('[data-ui="saved"]').textContent.includes('another tab')`, stale); check('stale tab cannot overwrite saved levels', true);
    await evaluate(`document.querySelector('[data-ui="reset"]').click()`); await ready(tab);
    check('reset restores exact reference', await evaluate(`document.querySelector('[data-creator]').dataset.mixSha256===${JSON.stringify(reference)}`));
    await evaluate(`document.querySelector('[data-ui="play"]').click()`); await wait(`document.querySelector('[data-creator]').dataset.playing==='true'`);
    await Bun.sleep(250); check('playback starts', await evaluate(`!document.querySelector('[data-ui="stop"]').disabled`));
    await evaluate(`document.querySelector('[data-ui="stop"]').click()`); check('stop clears playback', await evaluate(`document.querySelector('[data-creator]').dataset.playing==='false'`));
    report.wav = await evaluate(`(async()=>{let blob;const original=URL.createObjectURL;URL.createObjectURL=value=>{blob=value;return original(value)};document.querySelector('[data-ui="download"]').click();URL.createObjectURL=original;const bytes=await blob.arrayBuffer();const ac=new AudioContext({sampleRate:48000});const audio=await ac.decodeAudioData(bytes.slice(0));await ac.close();return {bytes:bytes.byteLength,duration:audio.duration,channels:audio.numberOfChannels,rate:audio.sampleRate};})()`);
    check('export decodes as six-second stereo WAV', report.wav.bytes === 1152044 && report.wav.duration === 6 && report.wav.channels === 2 && report.wav.rate === 48000);
    const downloaded = join(downloads, 'aurekai-generated-mix.wav');
    const deadline = Date.now() + 5000; while (!existsSync(downloaded) && Date.now() < deadline) await Bun.sleep(25);
    check('download reaches disk as a complete WAV', existsSync(downloaded) && readFileSync(downloaded).length === 1152044);
    report.worker = await evaluate(`new Promise((resolve,reject)=>{const worker=new Worker(document.querySelector('script[src*="creator-client.mjs"]').src.replace('creator-client.mjs','creator-worker.mjs'),{type:'module'});const sample=JSON.parse(document.querySelector('[data-creator-config]').textContent);const timer=setTimeout(()=>{worker.terminate();reject(Error('worker timeout'))},10000);worker.onmessage=({data})=>{if(data.id===0){worker.postMessage({id:1,type:'render',gains:[NaN,0,0]});}else{clearTimeout(timer);worker.terminate();resolve({type:data.type,message:data.message});}};worker.postMessage({id:0,type:'init',sample});})`);
    check('worker refuses invalid gains', report.worker.type === 'error' && report.worker.message.includes('between 0 and 1'));
    if (!live) {
      for (fault of ['corrupt', 'missing']) {
        await tab.go(origin + '/audio/?workspace=sample'); await wait(`Boolean(document.querySelector('[data-creator]')?.dataset.error)`);
        check(`${fault} source blocks playback`, await evaluate(`document.querySelector('[data-ui="play"]').disabled&&document.querySelector('[data-ui="download"]').disabled`));
      }
      fault = '';
    }
    const writes = events.filter(e => e.method === 'Network.requestWillBeSent' && !['GET', 'HEAD'].includes(e.params.request.method));
    check('no server writes or uploads', writes.length === 0);
    const exceptions = events.filter(e => e.method === 'Runtime.exceptionThrown'); check('no script exceptions', exceptions.length === 0);
  }, {timeoutMs: 90000, flags: ['--autoplay-policy=no-user-gesture-required']});
  report.status = 'passed';
} catch (error) {report.status = 'failed'; report.failures.push(error.stack); process.exitCode = 1;}
finally {server?.stop(true); writeFileSync(join(out, 'verification.json'), JSON.stringify(report, null, 2) + '\n'); console.log(JSON.stringify(report));}
