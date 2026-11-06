// Browser witness for the process simulator and the research desk, at desktop
// and phone width, against a local build or the published site:
//   bun components/BonfyreServiceSite/tests/queues-research-browser.mjs http://localhost:4391 [--shots DIR]
// It uses the pages the way a visitor does: the simulator must finish a run,
// find hourly staff that lifts the answered share, and the research desk must
// file a selected passage that matches its abstract. Neither page may scroll
// sideways on a phone. OpenAlex and Europe PMC are not contacted.
import { mkdirSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { withChrome } from '../../../integrations/browser/capture/chrome-session.mjs';

const origin = process.argv[2]?.replace(/\/$/, '');
if (!origin) { console.error('usage: queues-research-browser.mjs ORIGIN [--shots DIR]'); process.exit(2); }
const at = process.argv.indexOf('--shots'), shots = at > 0 ? process.argv[at + 1] : null;
if (shots) mkdirSync(shots, { recursive: true });
const problems = [], seen = [];
const need = (ok, why) => { if (!ok) problems.push(why); };

await withChrome(async ({ open }) => {
  for (const [label, width, height, mobile] of [['desktop', 1440, 1000, false], ['phone', 390, 844, true]]) {
    for (const path of ['/queues/', '/research/', '/schema/', '/plan/', '/pricing/']) {
      const { page, go } = await open();
      await page('Emulation.setDeviceMetricsOverride', { width, height, deviceScaleFactor: mobile ? 2 : 1, mobile });
      // Each pass starts as a first visit: the working copy an earlier pass kept is cleared.
      await go(origin + path); await page('Runtime.evaluate', { expression: 'localStorage.clear()' }); await go(origin + path);
      const run = async expression => { const r = await page('Runtime.evaluate', { expression, awaitPromise: true, returnByValue: true }); if (r.exceptionDetails) throw Error(`${path} ${label}: ${r.exceptionDetails.exception?.description ?? r.exceptionDetails.text}`); return r.result.value; };
      const wait = `const wait=async(test,ms=15000)=>{const t=Date.now();while(!test()){if(Date.now()-t>ms)throw Error('timed out');await new Promise(r=>setTimeout(r,100));}};const q=s=>document.querySelector('[data-ui="'+s+'"]');`;
      const result = path === '/queues/' ? await run(`(async()=>{${wait}
        await wait(()=>q('ready')&&!q('ready').hidden&&/runs of/.test(q('run-state').textContent));
        const level=()=>[...q('results').querySelectorAll('tbody')][0].querySelectorAll('tr')[2].children[2].textContent;
        const before=level(); document.querySelector('[data-f="find"]').click(); document.querySelector('[data-f="use-suggest"]').click();
        await wait(()=>/runs of/.test(q('run-state').textContent)&&level()!==before);
        const cur=document.querySelector('[data-hire="current"]');cur.value='40';cur.dispatchEvent(new Event('input',{bubbles:true}));
        return {before,after:level(),hire:q('hire-steps').textContent,overflow:document.documentElement.scrollWidth-innerWidth};})()`)
        : path === '/pricing/' ? await run(`(async()=>{${wait}
        await wait(()=>q('ready')&&!q('ready').hidden&&q('readout').textContent);
        const read=()=>q('readout').textContent, before=read(), svg=q('curve'), r=svg.getBoundingClientRect();
        svg.dispatchEvent(new PointerEvent('pointerdown',{bubbles:true,pointerId:1,clientX:r.x+r.width*0.7,clientY:r.y+r.height/2})); svg.dispatchEvent(new PointerEvent('pointerup',{bubbles:true,pointerId:1}));
        await wait(()=>read()!==before);
        return {items:document.querySelectorAll('[data-pick]').length,before:before.slice(0,80),after:read().slice(0,80),tiers:q('tiers').textContent.includes('customers each choose'),overflow:document.documentElement.scrollWidth-innerWidth};})()`)
        : path === '/plan/' ? await run(`(async()=>{${wait}
        await wait(()=>q('ready')&&!q('ready').hidden&&document.querySelector('.dp-bar[data-bar]'));
        const before=q('outlook').textContent; q('people').click(); await wait(()=>/one person doing one thing/.test(q('outlook').textContent));
        const bar=document.querySelector('[data-bar="publish"]'); bar.dispatchEvent(new PointerEvent('pointerdown',{bubbles:true,pointerId:1,clientX:bar.getBoundingClientRect().x+2,clientY:bar.getBoundingClientRect().y+2}));
        await wait(()=>q('inspector').querySelector('[data-dep]'));
        const loop=q('inspector').querySelector('[data-dep="verify-live"]'); loop.checked=true; loop.dispatchEvent(new Event('change',{bubbles:true}));
        await wait(()=>/loop/.test(q('message').textContent));
        return {bars:document.querySelectorAll('.dp-bar[data-bar]').length,outlook:q('outlook').textContent.slice(0,300),loopRefused:/loop/.test(q('message').textContent),overflow:document.documentElement.scrollWidth-innerWidth};})()`)
        : path === '/schema/' ? await run(`(async()=>{${wait}
        await wait(()=>q('ready')&&!q('ready').hidden&&document.querySelector('[data-table="works"]'));
        const g=document.querySelector('[data-table="works"]');g.dispatchEvent(new PointerEvent('pointerdown',{bubbles:true,clientX:g.getBoundingClientRect().x+20,clientY:g.getBoundingClientRect().y+10,pointerId:1}));g.dispatchEvent(new PointerEvent('pointerup',{bubbles:true,pointerId:1}));
        await wait(()=>q('inspector').querySelector('[data-type="year"]'));
        const sel=q('inspector').querySelector('[data-type="year"]');sel.value='INTEGER';sel.dispatchEvent(new Event('change',{bubbles:true}));
        await wait(()=>!q('try').disabled);q('try').click();
        await wait(()=>q('trial').textContent.length>0);
        return {sql:/CREATE TABLE "works__new"/.test(q('sql').textContent),trial:q('trial').textContent.slice(0,400),rows:[...q('trial').querySelectorAll('tbody tr')].map(r=>[...r.children].map(c=>c.textContent)),overflow:document.documentElement.scrollWidth-innerWidth};})()`)
        : await run(`(async()=>{${wait}
        await wait(()=>q('ready')&&!q('ready').hidden&&q('abstract'));
        const ab=q('abstract'),text=ab.textContent,start=text.search(/\\b\\w{4,}\\b/),words=text.slice(start).split(' ').slice(0,7).join(' ');
        const w=document.createTreeWalker(ab,NodeFilter.SHOW_TEXT);let t=0,s,so,e,eo;for(let n=w.nextNode();n;n=w.nextNode()){if(s==null&&start<t+n.length){s=n;so=start-t}if(e==null&&start+words.length<=t+n.length){e=n;eo=start+words.length-t}t+=n.length}
        const r=document.createRange();r.setStart(s,so);r.setEnd(e,eo);getSelection().removeAllRanges();getSelection().addRange(r);
        await wait(()=>!q('file').hidden);const before=document.querySelectorAll('[data-claim-ex]').length;q('file').querySelector('[data-ui="file-save"]').click();
        await wait(()=>document.querySelectorAll('[data-claim-ex]').length===before+1);
        document.querySelector('[data-post]').click(); await wait(()=>!q('compose').hidden);
        const png=await window.aurekaiComposer.pngBlob('square'), bmp=await createImageBitmap(png), story=await createImageBitmap(await window.aurekaiComposer.pngBlob('story'));
        let webm=null; try{ const v=await window.aurekaiComposer.webmBlob('card',1); const vid=document.createElement('video'); vid.muted=true; vid.src=URL.createObjectURL(v); await new Promise(r=>{vid.onloadeddata=r;vid.onerror=r;setTimeout(r,5000)}); webm={size:v.size,type:v.type,width:vid.videoWidth,height:vid.videoHeight}; }catch(e){ webm={error:e.message}; }
        return {png:[bmp.width,bmp.height,png.size],story:[story.width,story.height],webm,quoted:words,claims:document.querySelectorAll('.rs-claim').length,mismatch:/does not match/.test(q('claims').textContent),overflow:document.documentElement.scrollWidth-innerWidth};})()`);
      seen.push({ path, label, ...result });
      need(result.overflow <= 0, `${path} scrolls sideways at ${label} width by ${result.overflow}px`);
      if (path === '/queues/') need(/agree \d+ hire/.test(result.hire), `${path} ${label}: forty people against the need produced no hiring sequence: ${result.hire}`);
      if (path === '/queues/') need(parseInt(result.after) > parseInt(result.before), `${path} ${label}: staffing found by formula did not raise the answered share (${result.before} to ${result.after})`);
      else if (path === '/pricing/') need(result.items >= 5 && result.tiers && result.before !== result.after, `${path} ${label}: dragging the price did not rework the numbers`);
      else if (path === '/plan/') need(result.bars >= 10 && result.loopRefused && /8 in 10/.test(result.outlook), `${path} ${label}: the planner did not schedule, simulate and refuse a loop`);
      else if (path === '/schema/') need(result.sql && /Every reference holds/.test(result.trial) && result.rows.every(([, b, a]) => b === a), `${path} ${label}: the migration did not keep every row and reference: ${result.trial}`);
      else if (path === '/research/' && !(result.png?.[0] === 1080 && result.png[1] === 1080 && result.png[2] > 20000 && result.story?.[1] === 1920 && result.webm?.size > 1000 && result.webm.width === 1200 && result.webm.height === 630)) need(false, `${path} ${label}: the post did not export as PNG and WebM: ${JSON.stringify({ png: result.png, story: result.story, webm: result.webm })}`);
      else need(!result.mismatch && result.claims >= 3, `${path} ${label}: a filed quotation did not match its abstract`);
      if (shots) { const { data } = await page('Page.captureScreenshot', { format: 'png', captureBeyondViewport: true }); writeFileSync(join(shots, `${path.replace(/\//g, '')}-${label}.png`), Buffer.from(data, 'base64')); }
    }
  }
}, { timeoutMs: 120_000 });

console.log(JSON.stringify({ origin, seen, problems }, null, 1));
if (problems.length) process.exit(1);
