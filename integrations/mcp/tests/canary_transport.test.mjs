import { test, expect } from 'bun:test';
import { CANARY_TOOLS, createCanaryRecursiveCall, createCanaryServer } from '../canary_server.mjs';

const token = 'canary-fixture-token';
const base = 'http://127.0.0.1:3056';
const auth = { authorization: `Bearer ${token}` };
const request = (path, { method = 'GET', headers = {}, body } = {}) => new Request(`${base}${path}`, {
  method, headers: { ...headers }, ...(body === undefined ? {} : { body: typeof body === 'string' || body instanceof Uint8Array ? body : JSON.stringify(body) }),
});
const rpc = (method, params = {}, id = 'fixture-id') => ({ jsonrpc: '2.0', id, method, params });
const post = (method, params = {}, id = 'fixture-id') => request('/mcp', { method: 'POST', headers: auth, body: rpc(method, params, id) });

function fixture(overrides = {}) {
  const calls = [];
  const method = name => (...args) => { calls.push([name, ...args]); return { owner: name, args }; };
  const store = { status: method('status'), listCertificates: method('listCertificates'),
    validateCertificate: method('validateCertificate'), upsertPolicy: method('upsertPolicy'),
    listPolicies: method('listPolicies'), setPolicyState: method('setPolicyState'),
    routeDecide: method('routeDecide'), recordOutcome: method('recordOutcome'),
    listDecisions: method('listDecisions'), listOutcomes: method('listOutcomes'),
    verifyReceipts: method('verifyReceipts'), close: method('close'), ...overrides };
  const fixtures = { runSelected: method('runSelected') };
  return { calls, store, fixtures, async server() { return createCanaryServer({ token, store, fixtures }); } };
}

test('public health, private status, and bearer auth precede body parsing and unknown routes', async () => {
  const f = fixture(), server = await f.server();
  const health = await server.handle(request('/health'));
  expect(health.status).toBe(200);
  expect(await health.json()).toMatchObject({ status: 'ok', service: 'bonfyre-fixture-canary-r4', tools: 12, runtime: 'bun' });
  expect((await server.handle(request('/status'))).status).toBe(401);
  expect((await server.handle(request('/unknown'))).status).toBe(401);
  expect((await server.handle(request('/mcp', { method: 'POST', body: '{' }))).status).toBe(401);
  expect(f.calls).toEqual([]);
  const status = await server.handle(request('/status', { headers: auth }));
  expect(status.status).toBe(200);
  expect(await status.json()).toEqual({ owner: 'status', args: [] });
  expect(status.headers.get('MCP-Protocol-Version')).toBe('2025-06-18');
  expect((await server.handle(request('/unknown', { headers: auth }))).status).toBe(404);
});

test('MCP initialization, notifications, ping, and exact twelve-tool catalog never invoke owners', async () => {
  const f = fixture(), server = await f.server();
  const initialize = await (await server.handle(post('initialize'))).json();
  expect(initialize.result.protocolVersion).toBe('2025-06-18');
  expect(initialize.result.serverInfo.name).toBe('bonfyre-fixture-canary-r4');
  const list = await (await server.handle(post('tools/list'))).json();
  expect(list.result.tools).toEqual(CANARY_TOOLS);
  expect(list.result.tools.map(item => item.name)).toHaveLength(12);
  expect((await (await server.handle(post('ping'))).json()).result).toEqual({});
  expect((await server.handle(post('notifications/initialized'))).status).toBe(202);
  expect((await server.handle(post('notifications/cancelled'))).status).toBe(202);
  expect(f.calls).toEqual([]);
});

test('bounded strict JSON and identity/schema failures never call a semantic owner', async () => {
  const f = fixture(), server = await f.server();
  const oversized = request('/mcp', { method: 'POST', headers: { ...auth, 'content-length': '10000001' }, body: '{}' });
  expect((await server.handle(oversized)).status).toBe(400);
  const invalidUtf8 = request('/mcp', { method: 'POST', headers: auth, body: new Uint8Array([0xff]) });
  expect((await server.handle(invalidUtf8)).status).toBe(400);
  const badId = await (await server.handle(request('/mcp', { method: 'POST', headers: auth,
    body: rpc('ping', {}, { unsafe: true }) }))).json();
  expect(badId.error.code).toBe(-32000);
  const badSchema = await (await server.handle(post('tools/call', { name: 'canary_route_decide',
    arguments: { mission_id: 'fixture', capability: 'read', effect_class: 'INVALID' } }))).json();
  expect(badSchema.result.isError).toBe(true);
  expect((await (await server.handle(post('unsupported'))).json()).error.code).toBe(-32000);
  expect(f.calls).toEqual([]);
});

test('all twelve canonical tools route to their injected owner and preserve wrapper shapes', async () => {
  const f = fixture(), server = await f.server();
  const cases = [
    ['canary_status', {}, 'status'],
    ['fixture_correction_run', { providers: ['rtk'], max_agent_turns: 2 }, 'runSelected'],
    ['canary_certificate_list', {}, 'listCertificates'],
    ['canary_certificate_verify', { provider: 'rtk', capability: 'context.tool_compression' }, 'validateCertificate'],
    ['canary_policy_upsert', { capability: 'x', provider: 'rtk', fallback_provider: 'local', percentage: 10, effect_classes: ['PURE'] }, 'upsertPolicy'],
    ['canary_policy_list', {}, 'listPolicies'],
    ['canary_policy_state', { policy_id: 'policy-1', state: 'suspended' }, 'setPolicyState'],
    ['canary_route_decide', { mission_id: 'mission', capability: 'x', effect_class: 'PURE' }, 'routeDecide'],
    ['canary_outcome_record', { decision_id: 'decision-1', state: 'passed' }, 'recordOutcome'],
    ['canary_decision_list', { limit: 2 }, 'listDecisions'],
    ['canary_outcome_list', { limit: 3 }, 'listOutcomes'],
    ['canary_receipt_verify', {}, 'verifyReceipts'],
  ];
  for (const [name, args] of cases) {
    const value = await (await server.handle(post('tools/call', { name, arguments: args }, name))).json();
    expect(value.id).toBe(name);
    expect(value.result.isError).toBe(false);
    expect(value.result.structuredContent).toBeDefined();
  }
  expect(f.calls.map(item => item[0])).toEqual(cases.map(item => item[2]));
  expect(f.calls.find(item => item[0] === 'runSelected')).toEqual(['runSelected', ['rtk'], { max_agent_turns: 2 }]);
  expect(f.calls.find(item => item[0] === 'routeDecide')[1]).toMatchObject({ task_id: null, fallback_provider: null });
  expect(f.calls.find(item => item[0] === 'recordOutcome')[1]).toMatchObject({ latency_ms: null, receipt_digest: null, payload: {} });
});

test('tool errors are isError results, while effect calls serialize without serializing reads', async () => {
  let releaseFirst;
  const started = [];
  const f = fixture({
    upsertPolicy: async () => { started.push('write-1'); await new Promise(resolve => { releaseFirst = resolve; }); return { first: true }; },
    routeDecide: async () => { started.push('write-2'); return { second: true }; },
    status: async () => { started.push('read'); return { status: 'ok' }; },
    listPolicies: () => { throw new Error('fixture owner refused'); },
  });
  const server = await f.server();
  const first = server.call('canary_policy_upsert', { capability: 'x', provider: 'rtk', fallback_provider: 'local', percentage: 10, effect_classes: ['PURE'] });
  await Promise.resolve();
  const second = server.call('canary_route_decide', { mission_id: 'm', capability: 'x', effect_class: 'PURE' });
  const read = server.call('canary_status', {});
  expect(await read).toEqual({ status: 'ok' });
  expect(started).toEqual(['write-1', 'read']);
  releaseFirst();
  await Promise.all([first, second]);
  expect(started).toEqual(['write-1', 'read', 'write-2']);
  const refusal = await (await server.handle(post('tools/call', { name: 'canary_policy_list', arguments: {} }))).json();
  expect(refusal.result.isError).toBe(true);
  expect(refusal.result.structuredContent.error).toContain('fixture owner refused');
  expect((await (await server.handle(post('tools/call', { name: 'unknown', arguments: {} }))).json()).result.isError).toBe(true);
});

test('response and active-request bounds refuse excess work without truncated success', async () => {
  const huge = fixture({ status: () => ({ blob: 'x'.repeat(10_000_010) }) });
  const bounded = await huge.server();
  const output = await (await bounded.handle(request('/status', { headers: auth }))).json();
  expect(output.error.message).toContain('output budget');
  expect(JSON.stringify(output).length).toBeLessThan(1000);

  let release;
  const held = fixture({ status: async () => new Promise(resolve => { release = () => resolve({ status: 'done' }); }) });
  const server = await createCanaryServer({ token, store: held.store, fixtures: held.fixtures, maxConcurrent: 1 });
  const first = server.handle(request('/status', { headers: auth }));
  await Promise.resolve();
  const refused = await server.handle(request('/status', { headers: auth }));
  expect(refused.status).toBe(503);
  release();
  expect((await first).status).toBe(200);
});

test('recursive callback bounds endpoint, identity, isError, redirects and credential diagnostics', async () => {
  expect(() => createCanaryRecursiveCall({ endpoint: 'http://remote.example/mcp', token, request: async () => {} })).toThrow('loopback or HTTPS');
  const calls = [];
  const successful = createCanaryRecursiveCall({ endpoint: 'http://127.0.0.1:3055/mcp', token,
    request: async (url, options) => {
      calls.push({ url, options });
      const sent = JSON.parse(options.body);
      return Response.json({ jsonrpc: '2.0', id: sent.id, result: { isError: false, structuredContent: { artifact_id: 'fixture-id' } } });
    } });
  expect(await successful('artifact_ingest', { path: '/fixture' })).toEqual({ artifact_id: 'fixture-id' });
  expect(calls[0].options.redirect).toBe('error');
  expect(calls[0].options.headers.authorization).toBe(`Bearer ${token}`);
  await expect(successful('unknown_owner', {})).rejects.toThrow('unknown certificate projection owner');
  const wrongId = createCanaryRecursiveCall({ endpoint: 'http://127.0.0.1:3055/mcp', token,
    request: async () => Response.json({ jsonrpc: '2.0', id: 'wrong', result: {} }) });
  await expect(wrongId('artifact_ingest', {})).rejects.toThrow('invalid response identity');
  const isError = createCanaryRecursiveCall({ endpoint: 'http://127.0.0.1:3055/mcp', token,
    request: async (_url, options) => Response.json({ jsonrpc: '2.0', id: JSON.parse(options.body).id, result: { isError: true } }) });
  await expect(isError('artifact_ingest', {})).rejects.toThrow('invalid response identity');
  const redirect = createCanaryRecursiveCall({ endpoint: 'http://127.0.0.1:3055/mcp', token,
    request: async () => new Response(null, { status: 302 }) });
  await expect(redirect('artifact_ingest', {})).rejects.toThrow('HTTP 302');
  const leakage = createCanaryRecursiveCall({ endpoint: 'http://127.0.0.1:3055/mcp', token,
    request: async () => { throw new Error(`network error carried ${token}`); } });
  await expect(leakage('artifact_ingest', {})).rejects.toThrow('[redacted]');
  await expect(leakage('artifact_ingest', {})).rejects.not.toThrow(token);
});
