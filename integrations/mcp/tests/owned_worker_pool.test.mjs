import { afterEach, expect, test } from 'bun:test';
import { mkdtempSync, mkdirSync, writeFileSync, rmSync, realpathSync, openSync, closeSync, writeSync, existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';
import { createOwnedWorkerPool } from '../owned_worker_pool.mjs';
import { packBfp, verifyBfp } from '../v6_bfp.mjs';

const homes = [], pools = [];
afterEach(async () => {
  await Promise.all(pools.splice(0).map(pool => pool.close()));
  for (const home of homes.splice(0)) rmSync(home, { recursive: true, force: true });
});
function fixture(options = {}) {
  const home = realpathSync(mkdtempSync(join(tmpdir(), 'bonfyre-owned-workers-'))); homes.push(home);
  const entry = join(home, 'fixture.mjs');
  writeFileSync(entry, `import {serveOwnedWorker} from ${JSON.stringify(new URL('../owned_worker_pool.mjs', import.meta.url).href)};
import {threadId,parentPort} from 'node:worker_threads';
serveOwnedWorker(async (payload,{signal})=>{
  if(payload.operation==='exit')process.exit(23);
  if(payload.operation==='malformed'){parentPort.postMessage('not JSON');return new Promise(()=>{});}
  if(payload.operation==='stall')return new Promise(()=>{});
  if(payload.operation==='large')return 'x'.repeat(4096);
  if(payload.operation==='throw')throw Object.assign(Error('fixture failure'),{code:payload.code});
  if(payload.operation==='busy'){const deadline=performance.now()+payload.ms;while(performance.now()<deadline){};}
  if(payload.operation==='cooperative'){const deadline=performance.now()+payload.ms;while(performance.now()<deadline){signal.throwIfAborted();}}
  return {threadId,payload};
});`);
  const pool = createOwnedWorkerPool({ workerUrl: pathToFileURL(entry), timeoutMs: 2000, ...options }); pools.push(pool);
  return { home, pool, entry };
}
test('fixed entrypoints and execution budgets are validated before any worker starts', () => {
  expect(() => createOwnedWorkerPool({ workerUrl: new URL('https://example.test/code.js') })).toThrow();
  const f = fixture();
  expect(() => createOwnedWorkerPool({ workerUrl: pathToFileURL(f.entry), maxWorkers: 3 })).toThrow();
  expect(() => createOwnedWorkerPool({ workerUrl: pathToFileURL(f.entry), maxBytes: 100 })).toThrow();
});
test('warm workers reuse identity, snapshot payloads, and return structured errors without stack', async () => {
  const { pool } = fixture({ maxWorkers: 1 });
  const payload = { operation: 'echo', value: { a: 'before' } };
  const first = pool.run(payload); payload.value.a = 'after';
  const result = await first;
  expect(result.payload.value.a).toBe('before');
  expect((await pool.run({ operation: 'echo' })).threadId).toBe(result.threadId);
  const error = await pool.run({ operation: 'throw', code: 'ENOENT' }).catch(error => error);
  expect(error.name).toBe('WorkerExecutionError'); expect(error.code).toBe('ENOENT');
  expect(error.message).toBe('fixture failure');
  const invalid = await pool.run({ operation: 'throw', code: 'token=do-not-echo' }).catch(error => error);
  expect(invalid.code).toBeUndefined();
});
test('capacity is fail-fast with no queue; two independent slots may work concurrently', async () => {
  const { pool } = fixture({ maxWorkers: 2 });
  const a = pool.run({ operation: 'busy', ms: 120 }), b = pool.run({ operation: 'busy', ms: 120 });
  await expect(pool.run({ operation: 'echo' })).rejects.toMatchObject({ name: 'WorkerBusyError' });
  const results = await Promise.all([a, b]);
  expect(results[0].threadId).not.toBe(results[1].threadId);
  expect((await pool.run({ operation: 'echo' })).payload.operation).toBe('echo');
});
test('pre-abort, running cancellation, and timeout reject without replay and allow clean new generation', async () => {
  const { pool } = fixture({ maxWorkers: 1, timeoutMs: 150 });
  const pre = new AbortController(); pre.abort();
  await expect(pool.run({}, { signal: pre.signal })).rejects.toMatchObject({ name: 'AbortError' });
  const first = await pool.run({ operation: 'echo' });
  const controller = new AbortController();
  const active = pool.run({ operation: 'busy', ms: 3000 }, { signal: controller.signal });
  setTimeout(() => controller.abort(), 20);
  await expect(active).rejects.toMatchObject({ name: 'AbortError' });
  const next = await pool.run({ operation: 'echo' }); expect(next.threadId).not.toBe(first.threadId);
  await expect(pool.run({ operation: 'stall' })).rejects.toMatchObject({ name: 'TimeoutError' });
  expect((await pool.run({ operation: 'echo' })).threadId).not.toBe(next.threadId);
});
test('worker crash and malformed reply cannot leave calls hanging or poison capacity', async () => {
  const { pool } = fixture({ maxWorkers: 1 });
  await expect(pool.run({ operation: 'exit' })).rejects.toMatchObject({ name: 'WorkerExitError' });
  await expect(pool.run({ operation: 'malformed' })).rejects.toMatchObject({ name: 'WorkerProtocolError' });
  expect((await pool.run({ operation: 'echo' })).payload.operation).toBe('echo');
});
test('shared cancellation interrupts synchronous cooperative work without waiting for its event loop', async () => {
  const { pool } = fixture({ maxWorkers: 1, timeoutMs: 5000 });
  await pool.run({ operation: 'echo' });
  const controller = new AbortController(), started = performance.now();
  const active = pool.run({ operation: 'cooperative', ms: 2000 }, { signal: controller.signal });
  setTimeout(() => controller.abort(), 25);
  await expect(active).rejects.toMatchObject({ name: 'AbortError' });
  const elapsed = performance.now() - started;
  expect(elapsed).toBeLessThan(1000);
  expect((await pool.run({ operation: 'echo' })).payload.operation).toBe('echo');
});
test('close joins running work once, rejects active calls, and refuses later admission', async () => {
  const { pool } = fixture();
  const active = pool.run({ operation: 'stall' });
  const rejected = active.catch(error => error);
  const close = pool.close(); expect(pool.close()).toBe(close);
  await close; expect(await rejected).toMatchObject({ name: 'WorkerClosedError' });
  await expect(pool.run({})).rejects.toMatchObject({ name: 'WorkerClosedError' });
});
test('bounded JSON refuses getters, cycles, nonfinite values and oversized requests/replies', async () => {
  const { pool } = fixture({ maxBytes: 1024 }); let invoked = false;
  const getter = Object.defineProperty({}, 'secret', { enumerable: true, get() { invoked = true; return 'secret'; } });
  await expect(pool.run(getter)).rejects.toThrow('accessors'); expect(invoked).toBe(false);
  const array = []; Object.defineProperty(array, 0, { get() { invoked = true; return 1; } });
  await expect(pool.run(array)).rejects.toThrow('only data'); expect(invoked).toBe(false);
  const cycle = {}; cycle.self = cycle;
  await expect(pool.run(cycle)).rejects.toThrow('structure');
  await expect(pool.run({ value: Infinity })).rejects.toThrow('finite');
  await expect(pool.run({ value: 'x'.repeat(2048) })).rejects.toThrow('byte budget');
  await expect(pool.run({ operation: 'large' })).rejects.toMatchObject({ name: 'WorkerExecutionError' });
  expect((await pool.run({ operation: 'echo' })).payload.operation).toBe('echo');
});
test('synchronous CPU worker leaves the parent event loop responsive', async () => {
  const { pool } = fixture({ maxWorkers: 1 }); await pool.run({ operation: 'echo' });
  let ticks = 0, largestDelay = 0, previous = performance.now();
  const timer = setInterval(() => { const at = performance.now(); largestDelay = Math.max(largestDelay, at - previous); previous = at; ticks++; }, 5);
  try { await pool.run({ operation: 'busy', ms: 250 }); }
  finally { clearInterval(timer); }
  expect(ticks).toBeGreaterThan(5); expect(largestDelay).toBeLessThan(200);
});
test('fixed BFP worker verifies real archive parity, rejects escape/extract and exposes missing-file errno', async () => {
  const f = fixture(); const root = join(f.home, 'root'), home = join(f.home, 'state');
  mkdirSync(root); mkdirSync(home);
  const archive = join(home, 'fixture.bfp'); packBfp(archive, { data: Buffer.alloc(8 * 1024 * 1024, 42) });
  const pool = createOwnedWorkerPool({ workerUrl: new URL('../v6_bfp_worker.mjs', import.meta.url), timeoutMs: 5000 }); pools.push(pool);
  const request = { operation: 'verify', root, home, path: archive };
  const result = await pool.run(request), reference = verifyBfp(archive);
  expect(result).toEqual(reference);
  await expect(pool.run({ ...request, extract: join(home, 'extract') })).rejects.toThrow('only');
  expect(existsSync(join(home, 'extract'))).toBe(false);
  await expect(pool.run({ ...request, path: '/etc/passwd' })).rejects.toThrow('escapes');
  await expect(pool.run({ ...request, operation: 'pack' })).rejects.toThrow('only');
  await expect(pool.run({ ...request, path: join(home, 'absent.bfp') })).rejects.toMatchObject({ code: 'ENOENT' });
  const fd = openSync(archive, 'r+'); try { writeSync(fd, Buffer.from([1]), 0, 1, reference.manifest.sections[0].length); } finally { closeSync(fd); }
  await expect(pool.run(request)).rejects.toThrow('digest');
});
