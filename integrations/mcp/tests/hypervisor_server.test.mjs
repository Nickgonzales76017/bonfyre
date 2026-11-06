// What this does not prove:
// - the network call is a function supplied by this test, so no request leaves the process and no remote behaviour is observed.
// - a dependency is replaced by an object built in this test, so the real one is not exercised.
import { afterEach, expect, test } from 'bun:test';
import { mkdtempSync, realpathSync, readdirSync, rmSync, writeFileSync, existsSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { createHypervisorServer, createHypervisorOwnerCalls, HYPERVISOR_TOOLS } from '../hypervisor_server.mjs';
const homes = [], owners = [];
afterEach(async () => { for (const owner of owners.splice(0)) await owner.close(); for (const home of homes.splice(0)) rmSync(home, { recursive: true, force: true }); });
const temp = () => { const home = realpathSync(mkdtempSync(join(tmpdir(), 'bonfyre-hypervisor-server-'))); homes.push(home); return home; };
const token = 'fixture-private-token';
async function rpc(owner, name, args = {}, authorized = true) {
  const response = await owner.handle(new Request('http://localhost/mcp', { method: 'POST', headers: { authorization: authorized ? `Bearer ${token}` : 'wrong', 'content-type': 'application/json' }, body: JSON.stringify({ jsonrpc: '2.0', id: 7, method: 'tools/call', params: { name, arguments: args } }) }));
  return { status: response.status, body: await response.json() };
}
async function mock(overrides = {}) {
  const home = temp(), calls = [];
  const record = name => (...args) => { calls.push({ name, args }); return { state: 'passed', name }; };
  const store = Object.fromEntries(['summary', 'verifyReceiptChain', 'observedSummary', 'observedReceiptChain', 'observedProviders', 'observedRoute', 'providers', 'route', 'registerSession', 'listSessions', 'missionStatus', 'promote', 'quarantine', 'approveLicense', 'events', 'close'].map(name => [name, record(name)])); store.home = home;
  store.observe = (name, ...args) => store[name](...args);
  const mission = { id: 'mission', goal: 'test', tasks: [] };
  store.getMissionRuntime = () => ({ mission, state: 'compiled', tasks: [] });
  const missions = { get: () => mission, compile: record('compile'), route: record('missionRoute'), backpropagate: record('backpropagate') };
  const adapters = Object.fromEntries(['status', 'memorySearch', 'compactCommand', 'bernsteinCompile', 'bernsteinValidate', 'fleetStatus', 'fleetLaunch', 'fleetAction', 'cavemanCode', 'invokerSubmit'].map(name => [name, record(name)]));
  const owner = await createHypervisorServer({ token, store, root: home, home, stopPaths: [join(home, 'STOP')], missions, adapters,
    discovery: { discover: record('discover') }, context: { compile: record('contextCompile'), handoff: record('handoff') },
    evaluation: { evaluate: record('evaluate'), compare: record('compare'), installPlan: record('installPlan') },
    execution: { execute: record('execute'), executeSingleTask: record('single'), status: record('missionStatus') },
    sources: { ingestSource: record('source'), sourceUpdateCheck: record('updates') },
    ownerCalls: Object.fromEntries(['fabric', 'recursive', 'canary', 'spines', 'v6'].map(name => [name, record(name)])), ...overrides });
  owners.push(owner); return { home, owner, calls, store };
}
test('all33 catalog bindings preserve schema and reject unauthenticated or malformed dispatch before owner effects', async () => {
  const f = await mock(); expect(HYPERVISOR_TOOLS).toHaveLength(33);
  expect((await rpc(f.owner, 'mission_compile', { goal: 'x' }, false)).status).toBe(401); expect(f.calls).toHaveLength(0);
  for (const [name, args] of [['mission_compile', { goal: 'x', endpoint: 'http://evil' }], ['provider_promote', { provider_id: 'p', capability: 'c', weight: '1' }], ['unknown', {}]]) expect((await rpc(f.owner, name, args)).body.result.isError).toBe(true);
  expect(f.calls).toHaveLength(0);
  expect((await rpc(f.owner, 'mission_compile', { goal: 'x' })).body.result.isError).toBe(false); expect(f.calls[0].name).toBe('compile');
});
test('legacy write-misannotations fixed and STOP blocks all mutations while pure reads remain', async () => {
  const f = await mock(); writeFileSync(join(f.home, 'STOP'), 'paused');
  for (const name of ['ecosystem_discover', 'mission_route', 'provider_evaluate', 'source_update_check']) expect(HYPERVISOR_TOOLS.find(item => item.name === name).annotations.readOnlyHint).toBe(false);
  for (const [name, args] of [['ecosystem_discover', {}], ['mission_route', { mission_id: 'm' }], ['provider_evaluate', { provider_id: 'p' }], ['source_update_check', {}], ['mission_execute', { mission_id: 'm' }]]) expect((await rpc(f.owner, name, args)).body.result.structuredContent.error).toContain('STOP');
  expect(f.calls).toHaveLength(0); expect((await rpc(f.owner, 'replay_verify')).body.result.isError).toBe(false);
});
test('all installed tools dispatch through their actual composed owner contract', async () => {
  const f = await mock();
  for (const tool of HYPERVISOR_TOOLS) {
    const args = Object.fromEntries((tool.inputSchema.required || []).map(name => [name, name === 'action' ? 'show' : 'fixture']));
    const result = await rpc(f.owner, tool.name, args); expect(result.body.result.isError).toBe(false);
  }
  expect(f.calls.some(item => item.name === 'single')).toBe(true); expect(f.calls.some(item => item.name === 'source')).toBe(true);
  const handoff = f.calls.find(item => item.name === 'handoff'); expect(handoff.args[0].provider_id).toBe('fixture');
});
test('events require auth and exact bounded query; prompts have no side effects; health is public', async () => {
  const f = await mock();
  expect((await f.owner.handle(new Request('http://localhost/events'))).status).toBe(401);
  const headers = { authorization: `Bearer ${token}` };
  for (const query of ['after=-1', 'limit=2001', 'limit=1&limit=2', 'endpoint=x']) expect((await f.owner.handle(new Request(`http://localhost/events?${query}`, { headers }))).status).toBe(400);
  expect((await f.owner.handle(new Request('http://localhost/events?after=2&limit=3', { headers }))).status).toBe(200);
  expect(f.calls[0]).toEqual({ name: 'events', args: [{ after: 2, limit: 3 }] });
  expect((await f.owner.handle(new Request('http://localhost/health'))).status).toBe(200);
  const status = (await rpc(f.owner, 'hypervisor_status')).body.result.structuredContent;
  expect(status.v6_workgraph_nodes).toBeNull(); // Missing observation is not measured zero.
});
test('background mission admission is bounded, duplicate is not replayed, close aborts and joins', async () => {
  let calls = 0, joined = false;
  const f = await mock({ maxActiveMissions: 1, execution: { status: () => ({}), executeSingleTask: () => ({}), execute: async (_id, { signal }) => {
    calls++; await new Promise(resolve => signal.addEventListener('abort', resolve, { once: true })); joined = true;
  } } });
  expect((await rpc(f.owner, 'mission_execute', { mission_id: 'one' })).body.result.structuredContent.state).toBe('queued');
  expect((await rpc(f.owner, 'mission_execute', { mission_id: 'one' })).body.result.structuredContent.state).toBe('running');
  expect((await rpc(f.owner, 'mission_execute', { mission_id: 'two' })).body.result.isError).toBe(true); expect(calls).toBe(1);
  await f.owner.close(); expect(joined).toBe(true); expect((await rpc(f.owner, 'replay_verify')).body.result.isError).toBe(true);
});
test('mission queue refuses observed completed, running and attempted work before acknowledging acceptance', async () => {
  const f = await mock();
  for (const runtime of [{ state: 'resolved', tasks: [] }, { state: 'running', tasks: [] }, { state: 'compiled', tasks: [{ state: 'started' }] }, { state: 'compiled', tasks: [{ state: 'blocked' }] }]) {
    f.store.getMissionRuntime = () => runtime;
    const value = await rpc(f.owner, 'mission_execute', { mission_id: 'old' }); expect(value.body.result.isError).toBe(true);
    expect(value.body.result.structuredContent.error).toContain('reconcile');
  }
  expect(f.calls.some(item => item.name === 'execute')).toBe(false);
});
test('downstream endpoints are trusted constructor bindings and remote endpoints need separate credential files', async () => {
  expect(() => createHypervisorOwnerCalls({ token, env: { BONFYRE_SPINES_MCP: 'https://remote.example/mcp' } })).toThrow('own explicit token');
  const calls = [], owners = createHypervisorOwnerCalls({ token, env: {}, request: async (url, init) => {
    const message = JSON.parse(init.body); calls.push({ url, message }); return Response.json({ jsonrpc: '2.0', id: message.id, result: { structuredContent: { fixture: true } } });
  } });
  await expect(owners.spines('shell', {})).rejects.toThrow('unbound'); expect(calls).toHaveLength(0);
  expect(await owners.recursive('recursive_status', {})).toEqual({ fixture: true }); expect(calls[0].url).toBe('http://127.0.0.1:3055/mcp');
});
test('missing source owner refuses explicitly and transport errors never echo bearer credentials', async () => {
  const f = await mock({ sources: {}, evaluation: { evaluate: () => { throw Error(`private ${token}`); } } });
  const absent = await rpc(f.owner, 'source_absorb', { repo: 'fixture/repo' });
  expect(absent.body.result.isError).toBe(true); expect(absent.body.result.structuredContent.error).toContain('no Python fallback');
  const failure = await rpc(f.owner, 'provider_evaluate', { provider_id: 'fixture' });
  expect(failure.body.result.isError).toBe(true); expect(JSON.stringify(failure.body)).not.toContain(token);
});
test('shutdown also joins running read probes instead of closing their store early', async () => {
  let joined = false, began;
  const ready = new Promise(resolve => { began = resolve; });
  const f = await mock({ adapters: { status: async ({ signal }) => {
    began(); await new Promise(resolve => signal.addEventListener('abort', resolve, { once: true })); joined = true; return {};
  } } });
  const pending = rpc(f.owner, 'provider_adapters_status'); await ready;
  await f.owner.close(); await pending; expect(joined).toBe(true); expect(f.calls.at(-1).name).toBe('close');
});
test('normal default factory composes real native owners lazily; compilation and context persist in disposable store', async () => {
  const home = temp(), registryPath = join(home, 'registry.json');
  writeFileSync(registryPath, JSON.stringify({ projects: [], capability_contracts: [] }));
  const owner = await createHypervisorServer({ token, home, root: home, registryPath, env: { PATH: '' }, stopPaths: [join(home, 'STOP')],
    request: async (_url, init) => { const req = JSON.parse(init.body); return Response.json({ jsonrpc: '2.0', id: req.id, result: { structuredContent: { state: 'fixture', version: 'fixture' } } }); } }); owners.push(owner);
  expect(existsSync(join(home, 'hypervisor.sqlite3'))).toBe(false);
  const compiled = await owner.call('mission_compile', { goal: 'Create a small implementation plan', budget_tokens: 160000 });
  expect(compiled.mission.id).toBeString();
  const packet = await owner.call('context_compile', { mission_id: compiled.mission.id, budget_tokens: 24000 });
  expect(packet.mission_id).toBe(compiled.mission.id); expect(packet.authority).toBe('context-representation-only');
  expect((await owner.call('replay_verify')).verified).toBe(true);
  writeFileSync(join(home, 'STOP'), 'paused');
  expect((await owner.call('capability_route', { capability: 'fixture.read' })).candidates).toEqual([]);
  expect((await owner.call('ecosystem_providers')).providers).toEqual([]);
  expect((await owner.call('hypervisor_status')).paused).toBe(true);
  await expect(owner.call('mission_route', { mission_id: compiled.mission.id })).rejects.toThrow('STOP');
});
test('cold STOP read surfaces never initialize a missing database or bootstrap its schema', async () => {
  const home = temp(), registryPath = join(home, 'registry.json');
  writeFileSync(registryPath, JSON.stringify({ projects: [], capability_contracts: [] })); writeFileSync(join(home, 'STOP'), 'paused');
  const before = readdirSync(home).sort();
  const owner = await createHypervisorServer({ token, home, root: home, registryPath, env: { PATH: '' }, stopPaths: [join(home, 'STOP')],
    request: async () => { throw Error('fixture unavailable'); } }); owners.push(owner);
  for (const [name, args] of [['hypervisor_status', {}], ['ecosystem_providers', {}], ['capability_route', { capability: 'fixture' }],
    ['session_list', {}], ['mission_status', { mission_id: 'a'.repeat(32) }], ['provider_compare', { capability: 'fixture' }], ['replay_verify', {}]]) {
    const result = await rpc(owner, name, args); expect(result.body.result.isError).toBe(true);
  }
  expect((await owner.handle(new Request('http://localhost/events', { headers: { authorization: `Bearer ${token}` } }))).status).toBe(400);
  expect(readdirSync(home).sort()).toEqual(before); expect(existsSync(join(home, 'hypervisor.sqlite3'))).toBe(false);
});
