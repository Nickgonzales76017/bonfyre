import {resolve, join} from 'node:path';
import {writeFileSync, mkdirSync, readFileSync} from 'node:fs';
import assert from 'node:assert/strict';
import {withChrome} from '../../../integrations/browser/capture/chrome-session.mjs';
import {verifyCreatorWasm} from '../tools/wasm.mjs';

verifyCreatorWasm();
const root = resolve(import.meta.dirname, '../../..');
const args = process.argv.slice(2).filter(arg => arg !== '--serve');
const fixture = resolve(args[0] || 'packs/no-simulation/creator-upstream-20261013/demo');
const out = resolve(args[1] || 'packs/no-simulation/creator-wasm-20261013');
mkdirSync(out, {recursive: true});
const routes = new Map([
  ['/', join(root, 'cmd/BonfyreCreatorMix/tools/wasm-demo.html')],
  ['/wasm-demo.mjs', join(root, 'cmd/BonfyreCreatorMix/tools/wasm-demo.mjs')],
  ['/creator-wasm.mjs', join(root, 'Aurekai/src/creator-wasm.mjs')],
  ['/mix_kernel.wasm', join(root, 'Aurekai/native/creator/mix_kernel.wasm')],
  ...['pulse.f32', 'chime.f32', 'harmony.f32', 'mix.f32'].map(n => ['/' + n, join(fixture, n)])
]);
const server = Bun.serve({hostname: '127.0.0.1', port: 0, fetch(request) {
  const file = routes.get(new URL(request.url).pathname);
  return file ? new Response(Bun.file(file), {headers: {'Cache-Control': 'no-store'}}) : new Response('Not found', {status: 404});
}});
if (process.argv.includes('--serve')) {
  console.log(`Creator browser mixer: http://127.0.0.1:${server.port}/`);
} else {
  const report = {status: 'running', layouts: []};
  try {
    await withChrome(async ({open, events}) => {
      const tab = await open();
      await tab.go(`http://127.0.0.1:${server.port}/`);
      const evaluate = async expression => {
        const result = await tab.page('Runtime.evaluate', {expression, awaitPromise: true, returnByValue: true});
        if (result.exceptionDetails) throw new Error(JSON.stringify(result.exceptionDetails));
        return result.result.value;
      };
      report.mix = await evaluate(`new Promise((resolve, reject) => { const start = Date.now(); const poll = () => {
        if (window.creatorError) return reject(Error(window.creatorError));
        if (window.creatorProof) return resolve(window.creatorProof);
        if (Date.now() - start > 10000) return reject(Error('mix timeout')); setTimeout(poll, 25);
      }; poll(); })`);
      assert.equal(report.mix.samples, 576000); assert.equal(report.mix.native_byte_parity, true);
      assert.equal(report.mix.sha256, JSON.parse(readFileSync(join(fixture, 'verification.json'))).dense_sha256);
      for (const width of [1440, 390]) {
        await tab.page('Emulation.setDeviceMetricsOverride', {width, height: 900, deviceScaleFactor: 1, mobile: false});
        await evaluate('new Promise(requestAnimationFrame).then(() => new Promise(requestAnimationFrame))');
        const layout = await evaluate('({width: innerWidth, scrollWidth: document.documentElement.scrollWidth, canvasWidth: document.querySelector("canvas").width})');
        assert(layout.scrollWidth <= width); assert(layout.canvasWidth > 100); report.layouts.push(layout);
        const shot = await tab.page('Page.captureScreenshot', {format: 'png'});
        writeFileSync(join(out, `preview-${width}.png`), Buffer.from(shot.data, 'base64'));
      }
      report.initial = await evaluate('creatorState()'); assert(report.initial.sum > 0);
      report.muted = await evaluate(`document.querySelectorAll('input').forEach(s => {s.value = '0'; s.dispatchEvent(new Event('input'));}); creatorState()`);
      assert.equal(report.muted.sum, 0);
      report.restored = await evaluate(`document.querySelector('#reset').click(); creatorState()`);
      assert.equal(report.restored.sum, report.initial.sum);
      await evaluate(`document.querySelector('#play').click()`);
      await Bun.sleep(300);
      report.playback = await evaluate('creatorState()'); assert(report.playback.playing); assert(report.playback.clock > 0);
      report.stopped = await evaluate(`document.querySelector('#stop').click(); creatorState()`); assert.equal(report.stopped.playing, false);
      report.exceptions = events.filter(e => e.sessionId === tab.sessionId && e.method === 'Runtime.exceptionThrown');
      assert.equal(report.exceptions.length, 0);
    }, {timeoutMs: 25000, flags: ['--autoplay-policy=no-user-gesture-required']});
    report.status = 'passed';
  } catch (error) { report.status = 'failed'; report.error = error.stack; process.exitCode = 1; }
  finally {
    server.stop(true);
    writeFileSync(join(out, 'browser-verification.json'), JSON.stringify(report, null, 2) + '\n');
    console.log(JSON.stringify(report));
  }
}
