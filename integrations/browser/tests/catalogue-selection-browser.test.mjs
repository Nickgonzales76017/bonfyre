// What this does not prove:
// - a server started in this test answers in place of the outside service, so nothing here is evidence about that service.
// - the network call is a function supplied by this test, so no request leaves the process and no remote behaviour is observed.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { withChrome } from '../capture/chrome-session.mjs';

// The page uses the actual owner stylesheet and IndexedDB module; this is not a mocked store.
test('customer selection works in Chrome, retains local work and refuses stale overwrites', async () => {
  const browserRoot = new URL('../', import.meta.url);
  const fixture = {
    stores: [{ id: 'demo', name: 'Demonstration', origin: 'https://demo.example', sha256: 'a'.repeat(64), currency: 'USD' }],
    rows: [{ store: 'demo', id: '1', title: 'Countertop display', kind: 'goods',
      source_url: 'https://demo.example/products/display', source_sha256: 'a'.repeat(64), observed_at: '2026-10-08T00:00:00Z',
      variants: [{ id: '11', label: 'Clear', price: '19.50', sku: null, available: true, shipping: null }] },
      { store: 'demo', id: '2', title: '<img src=x onerror=alert(1)>', kind: 'service',
        source_url: 'https://demo.example/products/service', source_sha256: 'a'.repeat(64), observed_at: '2026-10-08T00:00:00Z',
        variants: [{ id: '21', label: 'Preparation', price: '5.00', sku: null, available: null, shipping: false }] }]
  };
  const html = `<!doctype html><html><head><meta name="viewport" content="width=device-width, initial-scale=1">
    <link rel="stylesheet" href="/catalogue.css"><style>body{margin:0;--paper:white;--panel:#f6f6f6;--ink:#111;--muted:#555;--rule:#ddd;--hot:#234;--body:Arial,sans-serif;--display:Georgia,serif}</style></head>
    <body><main id="selection"></main><script type="module">
      import {mountCatalogueSelection} from '/catalogue-selection-view.mjs';
      import * as storage from '/extension/store.mjs';
      window.fixture=${JSON.stringify(fixture).replace(/</g, '\\u003c')};
      window.storage=storage;
      window.ready=mountCatalogueSelection(document.querySelector('#selection'),{catalogue:fixture,store:'demo',project_ref:'demo:project',title:'Plan a display',
        items:[{product_id:'1',variant_id:'11',quantity:1}],requirements:['Please confirm dimensions.']},storage).then(view=>{window.view=view;return true});
    </script></body></html>`;
  const files = new Map([
    ['/catalogue-selection-view.mjs', new URL('catalogue-selection-view.mjs', browserRoot)],
    ['/catalogue-selection.mjs', new URL('catalogue-selection.mjs', browserRoot)],
    ['/extension/store.mjs', new URL('extension/store.mjs', browserRoot)],
    ['/catalogue.css', new URL('../../../components/BonfyreServiceSite/catalogue.css', import.meta.url)]
  ]);
  const server = Bun.serve({ hostname: '127.0.0.1', port: 0, fetch(request) {
    const path = new URL(request.url).pathname;
    if (path === '/') return new Response(html, { headers: { 'content-type': 'text/html' } });
    if (files.has(path)) return new Response(readFileSync(files.get(path)), { headers: { 'content-type': path.endsWith('.css') ? 'text/css' : 'text/javascript' } });
    return new Response('Not found', { status: 404 });
  } });
  const origin = `http://127.0.0.1:${server.port}`;
  let checks = 0;
  const check = (condition, message) => { assert.ok(condition, message); checks++; };
  try {
    await withChrome(async ({ open, events }) => {
      const first = await open(); await first.go(origin);
      const run = async (tab, expression) => {
        const value = await tab.page('Runtime.evaluate', { expression, awaitPromise: true, returnByValue: true });
        if (value.exceptionDetails) throw Error(value.exceptionDetails.text + ': ' + JSON.stringify(value.exceptionDetails.exception));
        return value.result.value;
      };
      const wait = async (tab, expression) => run(tab, `(async()=>{for(let i=0;i<100;i++){if(${expression})return true;await new Promise(r=>setTimeout(r,25))}throw Error('UI wait expired')})()`);
      const click = async (tab, selector) => {
        const point = await run(tab, `(()=>{const el=document.querySelector(${JSON.stringify(selector)});el.scrollIntoView({block:'center'});const r=el.getBoundingClientRect();return{x:r.x+r.width/2,y:r.y+r.height/2}})()`);
        await tab.page('Input.dispatchMouseEvent', { type: 'mousePressed', ...point, button: 'left', clickCount: 1 });
        await tab.page('Input.dispatchMouseEvent', { type: 'mouseReleased', ...point, button: 'left', clickCount: 1 });
      };
      const status = "document.querySelector('[data-selection=status]').textContent";
      check(await wait(first, 'Boolean(window.view)'), 'mount completed');
      check(await run(first, 'view.getDraft().items[0].quantity===1'), 'prepared selection is visible first');
      await run(first, "document.querySelector('[data-selection=purpose]').focus()");
      await first.page('Input.insertText', { text: 'An apparel pop-up' });
      await click(first, '[data-selection=save]');
      check(await wait(first, `${status}.includes('Saved on this device')`), 'save completed in actual IndexedDB');
      check(await run(first, 'view.getDraft().purpose==="An apparel pop-up"'), 'keyboard input reaches selection');
      const second = await open(); await second.go(origin);
      check(await wait(second, 'Boolean(window.view)'), 'second tab mounted');
      check(await run(second, 'view.getDraft().purpose==="An apparel pop-up"'), 'second tab reopens saved work');
      await click(second, '[data-selection=add]'); await click(second, '[data-selection=save]');
      check(await wait(second, `${status}.includes('Saved on this device')`), 'second tab saves new revision');
      await click(first, '[data-selection=add]'); await click(first, '[data-selection=save]');
      check(await wait(first, `${status}.includes('another tab')`), 'stale tab cannot overwrite newer work');
      await click(first, '[data-selection=reload]');
      check(await wait(first, `${status}.includes('Reopened')`), 'explicit reload recovers current revision');
      check(await run(first, 'view.getDraft().items[0].quantity===2'), 'saved quantity preserved');
      await run(first, "document.querySelector('[data-selection=quantity]').value='-1'");
      await click(first, '[data-selection=add]');
      check(await run(first, 'view.getDraft().items[0].quantity===2'), 'negative input cannot reduce an existing selection');
      await run(first, "document.querySelector('[data-selection=quantity]').value='1';const p=document.querySelector('[data-selection=product]');p.value='2';p.dispatchEvent(new Event('change'))");
      await click(first, '[data-selection=add]');
      check(await run(first, 'view.getDraft().items.length===2'), 'another exact product option added');
      check(await run(first, "!document.querySelector('#selection img')"), 'retained markup remains text');
      check(await run(first, "document.querySelector('[data-selection=brief]').textContent.includes('Nonshipping item')"), 'service stays distinct');
      for (const width of [320, 390, 768, 1440]) {
        await first.page('Emulation.setDeviceMetricsOverride', { width, height: 900, deviceScaleFactor: 1, mobile: width < 721 });
        check(await run(first, 'document.documentElement.scrollWidth<=innerWidth'), 'no horizontal overflow at ' + width);
        check(await run(first, "getComputedStyle(document.querySelector('.cw-detail')).display!=='none'"), 'result visible at ' + width);
      }
      await run(first, "const field=document.querySelector('[data-selection=requirements]');field.value=Array(25).fill('Question').join('\\n');field.dispatchEvent(new Event('input'))");
      check(await run(first, "document.querySelector('[data-selection=save]').disabled&&view.getDraft()===null"), 'invalid visible requirements cannot save an older draft');
      const remote = events.filter(event => event.method === 'Network.requestWillBeSent').map(event => event.params.request.url)
        .filter(url => /^https?:/.test(url) && !url.startsWith(origin + '/') && url !== origin);
      check(remote.length === 0, 'no third-party request or enquiry was sent');
    }, { timeoutMs: 60000 });
    console.log(JSON.stringify({ suite: 'catalogue-selection-browser', checks, storage: 'existing IndexedDB owner', transport: 'loopback only', external_sends: 0 }));
  } finally { server.stop(true); }
});
