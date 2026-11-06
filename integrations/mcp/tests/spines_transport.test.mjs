// What this does not prove:
// - a dependency is replaced by an object built in this test, so the real one is not exercised.
import { expect, test } from 'bun:test';
import { createSpinesServer, SPINES_TOOLS } from '../spines_server.mjs';
function fake() {
  const seen = [], records = [];
  const store = { home: '/private/tmp/spines-test-nonexistent', id: () => 'a'.repeat(32), digest: () => 'digest', close() {},
    status: () => ({ status: 'fixture' }), verifyReceipts: () => ({ verified: true, head: 'head' }),
    writeReceipt: args => { records.push(args); return { receipt_id: `receipt-${records.length}` }; } };
  const engine = names => Object.fromEntries(names.map(name => [name, args => { seen.push([name, args]); return { equal: true, verified: true, streams: {}, id: 'proof' }; }]));
  const temporal = engine(['append', 'registerDependency', 'closure', 'refreshViews', 'parity', 'graphPartition', 'streamVerify']);
  const reasoning = engine(['rules', 'addRule', 'saturate', 'compileHelix', 'reduce', 'authority', 'crux']);
  const model = engine(['hardwareCensus', 'discoverModels', 'inventory', 'compilePhenotype', 'kernelParity', 'registerNode', 'nodes', 'place', 'fuse']);
  return { store, temporal, reasoning, model, seen, records };
}
test('29 captured contracts and authentication are bound through the shared transport', async () => {
  const owner = await createSpinesServer({ token: 'test-token', ...fake(), stopPaths: [] });
  expect(SPINES_TOOLS).toHaveLength(29);
  expect((await owner.handle(new Request('http://localhost/health'))).status).toBe(200);
  expect((await owner.handle(new Request('http://localhost/status'))).status).toBe(401);
  const response = await owner.handle(new Request('http://localhost/mcp', { method: 'POST', headers: { authorization: 'Bearer test-token' },
    body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/list' }) }));
  expect((await response.json()).result.tools).toEqual(SPINES_TOOLS);
  expect(await owner.call('spines_status', {})).toEqual({ status: 'fixture' });
  expect(() => owner.call('r6_helix_reduce', {})).toThrow();
});
test('persistent read annotations cannot bypass STOP or perform hidden proofs', async () => {
  const f = fake(), owner = await createSpinesServer({ token: 'test-token', ...f, stopPaths: [import.meta.filename] });
  for (const name of ['spines_verify', 'r5_parity_verify', 'r5_views_refresh']) await expect(owner.call(name, {})).rejects.toThrow('STOP');
  await expect(owner.call('r5_affected_closure', { root_kind: 'mission', root_id: 'x' })).rejects.toThrow('STOP');
  expect(f.seen).toHaveLength(0);
  expect(await owner.call('spines_status', {})).toEqual({ status: 'fixture' });
});
test('public health briefly caches integrity observations and mutation invalidates the cache', async () => {
  const f = fake(); let checks = 0;
  f.store.verifyReceipts = () => { checks++; return { verified: true }; };
  const owner = await createSpinesServer({ token: 'test-token', ...f, stopPaths: [] });
  const first = await (await owner.handle(new Request('http://localhost/health'))).json();
  await owner.handle(new Request('http://localhost/health')); expect(checks).toBe(1);
  expect(first.integrity_observation_max_age_ms).toBe(1000); expect(first.checked_at_ms).toBeGreaterThan(0);
  await owner.call('r5_views_refresh', {}); await owner.handle(new Request('http://localhost/health')); expect(checks).toBe(2);
});
test('partial composite failure leaves an uncertain attempt and never replays', async () => {
  const f = fake(); f.temporal.append = async () => { throw new Error('native delta failed'); };
  const owner = await createSpinesServer({ token: 'test-token', ...f, stopPaths: [] });
  await expect(owner.call('mission_r5_bootstrap', { mission: { id: 'm', tasks: [] } })).rejects.toThrow('reconcile mission attempt');
  expect(f.records.map(row => row.state)).toEqual(['started', 'uncertain']);
});
test('successful replay does not hide failed native delta execution in mission bootstrap', async () => {
  const f = fake(); f.temporal.append = async () => ({ receipt: { state: 'failed' } });
  const owner = await createSpinesServer({ token: 'test-token', ...f, stopPaths: [] });
  const result = await owner.call('mission_r5_bootstrap', { mission: { id: 'm', tasks: [] } });
  expect(result.parity.equal).toBe(true); expect(result.state).toBe('failed');
  expect(f.records.at(-1).state).toBe('failed');
});
test('native bonfyre-mcp spines/canary/recursive/v6 clients match the retired JS golden vectors', () => {
  const make = Bun.spawnSync(['make', '-s', '-C', new URL('../../../cmd/BonfyreMcp', import.meta.url).pathname]); expect(make.exitCode).toBe(0);
  const run = Bun.spawnSync(['bash', new URL('../../../cmd/BonfyreMcp/tests/vectors.sh', import.meta.url).pathname, 'check']);
  expect(run.stdout.toString()).toContain(' 0 failed'); expect(run.exitCode).toBe(0);
}, 120000);
