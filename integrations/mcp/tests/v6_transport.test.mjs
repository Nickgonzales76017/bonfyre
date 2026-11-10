import { afterEach, expect, test } from 'bun:test';
import { existsSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { createV6Store } from '../v6_store.mjs';
import { packBfp } from '../v6_bfp.mjs';
import { createV6Server, V6_TOOLS } from '../v6_server.mjs';
import { validateArguments } from '../http_boundary.mjs';
const fixtures = [];
afterEach(async () => { for (const f of fixtures.splice(0)) { await f.owner.close(); rmSync(f.home, { recursive: true, force: true }); } });
async function fixture(options = {}) {
  const home = mkdtempSync(join(tmpdir(), 'bonfyre-v6-transport-'));
  const store = createV6Store({ home, root: process.cwd(), now: () => 1000 });
  const seen = [], method = name => async args => { seen.push([name, args]); return { state: 'candidate' }; };
  const resources = Object.fromEntries(['discoverResources', 'activateProviders', 'discoverBackends'].map(name => [name, method(name)]));
  const history = Object.fromEntries(['federate', 'sparseSnapshot'].map(name => [name, method(name)]));
  const missions = { activate: method('activate') }, bfp = { verify: async () => ({ verified: true, sha256: 'b'.repeat(64) }) };
  const owner = await createV6Server({ token: 'fixture-secret', store, resources, history, missions, bfp, stopPaths: [join(home, 'STOP')], ...options });
  const f = { home: store.home, store, owner, resources, history, missions, bfp, seen }; fixtures.push(f); return f;
}
const request = (path, authenticated = false) => new Request(`https://owner.example${path}`, { headers: authenticated ? { authorization: 'Bearer fixture-secret' } : {} });
test('default server mission action uses the durable Bun compiler without an injected provider', async () => {
  const home = mkdtempSync(join(tmpdir(), 'v6-default-compile-'));
  const store = createV6Store({ home, root: home });
  const hypervisorHome = join(home, 'hypervisor');
  const owner = await createV6Server({ token: 'fixture-secret', store, hypervisorHome, stopPaths: [] });
  fixtures.push({ home, store, owner });
  expect(existsSync(join(hypervisorHome, 'hypervisor.sqlite3'))).toBe(false);
  let result;
  try { result = await owner.call('v6_mission_activate', { confirm: true }); }
  catch (error) { throw new Error(`${error.message}: ${store.db.query("SELECT payload_json FROM receipts WHERE kind='v6.1.mission-compile.result' ORDER BY created_at DESC LIMIT 1").get()?.payload_json}`); }
  expect(result.summary).toMatchObject({ hypervisor_compiled: 10, active: 0, candidate: 10 });
  expect(existsSync(join(hypervisorHome, 'hypervisor.sqlite3'))).toBe(true);
  expect(store.verifyReceipts().verified).toBe(true);
});
test('all eighteen contracts use shared auth; dashboard escapes data and is private', async () => {
  const f = await fixture(); expect(V6_TOOLS).toHaveLength(18);
  expect((await f.owner.handle(request('/health'))).status).toBe(200);
  for (const path of ['/status', '/', '/dashboard']) expect((await f.owner.handle(request(path))).status).toBe(401);
  f.store.writeJson(join(f.home, 'missions/catalog.json'), { missions: [{ name: '<script>bad</script>', state: 'candidate' }] });
  const res = await f.owner.handle(request('/dashboard', true)), html = await res.text();
  expect(res.headers.get('content-type')).toBe('text/html; charset=utf-8');
  expect(res.headers.get('content-security-policy')).toContain("default-src 'none'");
  expect(html).toContain('&lt;script&gt;bad&lt;/script&gt;'); expect(html).not.toContain('<script>');
  const catalog = await f.owner.handle(new Request('https://owner.example/mcp', { method: 'POST', headers: { authorization: 'Bearer fixture-secret' },
    body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/list' }) }));
  expect((await catalog.json()).result.tools).toEqual(V6_TOOLS);
});
test('confirmation constants and STOP prevent any effects or admission receipt', async () => {
  const f = await fixture();
  for (const args of [{}, { confirm: false }, { confirm: 'true' }, { confirm: true, arbitrary: 1 }])
    await expect(f.owner.call('v6_history_federate', args)).rejects.toThrow();
  expect(f.store.verifyReceipts().receipts).toBe(0);
  writeFileSync(join(f.home, 'STOP'), 'paused');
  await expect(f.owner.call('v6_history_federate', { confirm: true })).rejects.toThrow('STOP');
  expect(f.seen).toHaveLength(0); expect((await f.owner.call('v6_status')).receipts.receipts).toBe(0);
});
test('confirmed action records actual completion without upgrading candidate state', async () => {
  const f = await fixture(), result = await f.owner.call('v6_resource_discover', { confirm: true, probe_external: false });
  expect(result.state).toBe('candidate'); expect(f.seen[0][0]).toBe('discoverResources');
  expect(f.seen[0][1].probe_external).toBe(false); expect(f.seen[0][1].confirm).toBeUndefined();
  expect(f.store.db.query('SELECT state FROM receipts ORDER BY seq').all().map(row => row.state)).toEqual(['started', 'completed']);
});
test('partial failure records uncertain outcome once, with no automatic retry', async () => {
  const f = await fixture(); let calls = 0;
  f.resources.activateProviders = async () => { calls++; throw new Error('transport failed'); };
  await expect(f.owner.call('v6_provider_activate', { confirm: true })).rejects.toThrow('reconcile V6 attempt');
  expect(calls).toBe(1); expect(f.store.db.query('SELECT state FROM receipts ORDER BY seq').all().map(row => row.state)).toEqual(['started', 'uncertain']);
});
test('activation marker alone cannot verify; complete historical binding fails on changed archive', async () => {
  const f = await fixture();
  f.store.writeJson(join(f.home, 'V61_ACTIVATION_COMPLETE.json'), { version: '6.1.0' });
  expect((await f.owner.call('v6_verify')).verified).toBe(false);
  const base = { schema: 'bonfyre.v6.1-activation-proof.v1', version: '6.1.0', state: 'passed', mandatory_checks: ['fixture'], checks: { fixture: true } };
  const digest = f.store.digest(base), proofPath = join(f.home, 'proof/v6.1-activation-1000.json');
  const receipt = f.store.receipt('v6.1.capability-activation', 'passed', { final_digest: digest, proof: proofPath, bfp_sha256: 'b'.repeat(64) });
  f.store.writeJson(proofPath, { ...base, final_digest: digest, receipt });
  f.store.writeJson(join(f.home, 'V61_ACTIVATION_COMPLETE.json'), { version: '6.1.0', proof: proofPath, proof_digest: digest, bfp_sha256: 'b'.repeat(64) });
  expect((await f.owner.call('v6_verify')).verified).toBe(true);
  f.bfp.verify = async () => ({ verified: true, sha256: 'c'.repeat(64) });
  const invalid = await f.owner.call('v6_verify'); expect(invalid.verified).toBe(false); expect(invalid.failures).toContain('activation BFP binding');
});
test('CLI has exact read contract and no option-based endpoint escape', async () => {
  const v6 = (...args) => Bun.spawnSync([new URL('../../../cmd/BonfyreMcp/bonfyre-mcp', import.meta.url).pathname, 'v6', ...args],
    { env: { ...process.env, BONFYRE_MCP_TOKEN_FILE: '/nonexistent-bonfyre/token', BONFYRE_V6_URL: 'http://127.0.0.1:1' } });
  expect(v6('version').stdout.toString()).toBe('bonfyre-v6 6.1.0\n');
  const escape = v6('history', '--url', 'http://evil'); expect(escape.exitCode).toBe(2);
  expect(v6('history').stderr.toString()).toContain('ENOENT');
  expect(() => validateArguments({ answer: false }, { type: 'object', properties: { answer: { const: true } } })).toThrow('constant');
});

test('default composed owner is lazy and verifies real archives through its worker boundary', async () => {
  const home = mkdtempSync(join(tmpdir(), 'bonfyre-v6-default-'));
  const store = createV6Store({ home, root: process.cwd() });
  const owner = await createV6Server({ token: 'fixture-secret', store, stopPaths: [] });
  fixtures.push({ home: store.home, store, owner });
  expect(existsSync(store.dbPath)).toBe(false);
  const path = join(store.home, 'bfp/fixture.bfp');
  const packed = packBfp(path, { 'body.txt': Buffer.from('real immutable body') });
  const result = await owner.call('r8_bfp_verify', { path });
  expect(result.verified).toBe(true); expect(result.sha256).toBe(packed.sha256);
  expect(existsSync(store.dbPath)).toBe(false);
});
