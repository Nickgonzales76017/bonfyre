import { test, expect } from 'bun:test';
import { RECURSIVE_TOOLS, RECURSIVE_PROMPTS, createRecursiveServer } from '../recursive_server.mjs';

const token = 'recursive-fixture-token';
const base = 'http://127.0.0.1:3055';
const auth = { authorization: `Bearer ${token}` };
const names = [
  'recursive_status', 'tokenizer_list', 'artifact_ingest', 'artifact_get',
  'token_artifact_compile', 'token_artifact_get', 'token_delta_derive', 'flux_append',
  'delta_record', 'dependency_register', 'affected_closure_compile', 'ir_compile',
  'ir_bundle_compile', 'runtime_image_compile', 'provider_token_bid_compile',
  'lambda_motif_register', 'lambda_motif_match', 'graph_projection_compile',
  'bfp_successor_plan', 'recursive_mission_bootstrap', 'recursive_mission_finalize',
  'substrate_replay_verify',
];
const request = (path, { method = 'GET', headers = {}, body } = {}) => new Request(`${base}${path}`, {
  method, headers, ...(body === undefined ? {} : { body: typeof body === 'string' || body instanceof Uint8Array ? body : JSON.stringify(body) }),
});
const rpc = (method, params = {}, id = 'fixture-id') => ({ jsonrpc: '2.0', id, method, params });
const post = (method, params = {}, id = 'fixture-id') => request('/mcp', {
  method: 'POST', headers: auth, body: rpc(method, params, id),
});

function fixture(overrides = {}) {
  const calls = [];
  const method = (name, value = { owner: name }) => (...args) => { calls.push([name, ...args]); return value; };
  const store = {
    status: method('status', { service: 'bonfyre-recursive-substrate-v5', token_binary_status: { exit_code: 0 }, tokenizers: [] }),
    ingestArtifact: method('ingestArtifact', { artifact_id: 'artifact-1' }),
    artifact: method('artifact'),
    compileTokenArtifact: method('compileTokenArtifact', { artifact_id: 'token-1' }),
    tokenArtifact: method('tokenArtifact'),
    deriveTokenDelta: method('deriveTokenDelta'),
    appendFlux: method('appendFlux'),
    recordDelta: method('recordDelta'),
    registerDependency: method('registerDependency'),
    affectedClosure: method('affectedClosure', { closure_id: 'closure-1' }),
    compileIr: method('compileIr'),
    compileIrBundle: method('compileIrBundle', { objects: { ArtifactIR: { id: 'ir-1' } } }),
    compileRuntimeImage: method('compileRuntimeImage', { digest: 'image-digest' }),
    compileProviderBid: method('compileProviderBid'),
    registerMotif: method('registerMotif'),
    matchMotifs: method('matchMotifs'),
    graphProjection: method('graphProjection', { digest: 'graph-digest' }),
    bfpSuccessorPlan: method('bfpSuccessorPlan'),
    verifyReceipts: method('verifyReceipts', { verified: true, receipts: 0, head: null, failures: [] }),
    verifyFlux: method('verifyFlux', { verified: true, streams: {}, failures: [] }),
    writeReceipt: method('writeReceipt', { receipt_id: 'attempt-fixture' }),
    close: method('close'), ...overrides,
  };
  return { calls, store, async server() { return createRecursiveServer({ token, home: '/tmp/recursive-transport-fixture',
    root: '/tmp', tokenBinary: '/fixture/token-fabric', store }); } };
}

test('public health, private status, and authentication before body parsing or route disclosure', async () => {
  const f = fixture(), server = await f.server();
  const health = await server.handle(request('/health'));
  expect(health.status).toBe(200);
  expect(await health.json()).toMatchObject({ status: 'ok', service: 'bonfyre-recursive-substrate-v5',
    version: '5.1.0-recursive-r1', tools: 22, token_binary: true });
  expect((await server.handle(request('/status'))).status).toBe(401);
  expect((await server.handle(request('/unknown'))).status).toBe(401);
  expect((await server.handle(request('/mcp', { method: 'POST', body: '{' }))).status).toBe(401);
  const status = await server.handle(request('/status', { headers: auth }));
  expect(status.status).toBe(200);
  expect(await status.json()).toMatchObject({ service: 'bonfyre-recursive-substrate-v5' });
  expect((await server.handle(request('/unknown', { headers: auth }))).status).toBe(404);
  expect(f.calls.map(call => call[0])).toEqual(['status', 'status']);
});

test('initialize, notifications, exact legacy catalog and prompts do not invoke semantic owners', async () => {
  const f = fixture(), server = await f.server();
  const initialized = await (await server.handle(post('initialize'))).json();
  expect(initialized.result.protocolVersion).toBe('2025-06-18');
  expect(initialized.result.serverInfo).toMatchObject({ name: 'bonfyre-recursive-substrate-v5', version: '5.1.0-recursive-r1' });
  expect(initialized.result.capabilities).toMatchObject({ tools: { listChanged: false }, prompts: { listChanged: false } });
  expect((await (await server.handle(post('ping'))).json()).result).toEqual({});
  const catalog = await (await server.handle(post('tools/list'))).json();
  expect(catalog.result.tools).toEqual(RECURSIVE_TOOLS);
  expect(catalog.result.tools.map(tool => tool.name)).toEqual(names);
  expect(catalog.result.tools.find(tool => tool.name === 'affected_closure_compile').annotations.readOnlyHint).toBe(true);
  const prompts = await (await server.handle(post('prompts/list'))).json();
  expect(prompts.result.prompts).toEqual(RECURSIVE_PROMPTS);
  expect(prompts.result.prompts.map(prompt => prompt.name)).toEqual(['recursive-mission', 'tokenize-artifact']);
  const mission = await (await server.handle(post('prompts/get', { name: 'recursive-mission', arguments: { goal: 'fixture goal' } }))).json();
  expect(mission.result.messages[0].content.text).toContain('fixture goal');
  const artifact = await (await server.handle(post('prompts/get', { name: 'tokenize-artifact', arguments: { path: '/fixture/spec' } }))).json();
  expect(artifact.result.messages[0].content.text).toContain('/fixture/spec');
  expect((await server.handle(post('notifications/initialized'))).status).toBe(202);
  expect((await server.handle(post('notifications/cancelled'))).status).toBe(202);
  expect((await (await server.handle(post('resources/list'))).json()).error).toBeDefined();
  expect(f.calls).toEqual([]);
});

test('strict bounded requests and identity/schema refusals never reach owners', async () => {
  const f = fixture(), server = await f.server();
  expect((await server.handle(request('/mcp', { method: 'POST', headers: { ...auth, 'content-length': '30000001' }, body: '{}' }))).status).toBe(400);
  expect((await server.handle(request('/mcp', { method: 'POST', headers: auth, body: new Uint8Array([0xff]) }))).status).toBe(400);
  const badId = await (await server.handle(request('/mcp', { method: 'POST', headers: auth,
    body: rpc('ping', {}, { object: 'id' }) }))).json();
  expect(badId.error).toBeDefined();
  const missing = await (await server.handle(post('tools/call', { name: 'artifact_get', arguments: {} }))).json();
  expect(missing.result.isError).toBe(true);
  const unknown = await (await server.handle(post('tools/call', { name: 'recursive_self_test', arguments: {} }))).json();
  expect(unknown.result.isError).toBe(true);
  expect(f.calls).toEqual([]);
});

test('all 22 legacy tools dispatch to their owner with MCP structured results', async () => {
  const f = fixture(), server = await f.server();
  const cases = [
    ['recursive_status', {}, 'status'], ['tokenizer_list', {}, 'status'],
    ['artifact_ingest', { text: 'fixture text' }, 'ingestArtifact'],
    ['artifact_get', { artifact_id: 'artifact-1' }, 'artifact'],
    ['token_artifact_compile', { artifact_id: 'artifact-1' }, 'compileTokenArtifact'],
    ['token_artifact_get', { token_artifact_id: 'token-1' }, 'tokenArtifact'],
    ['token_delta_derive', { source_token_artifact_id: 'token-1', target_token_artifact_id: 'token-2' }, 'deriveTokenDelta'],
    ['flux_append', { stream: 'fixture', kind: 'fixture.event', payload: {} }, 'appendFlux'],
    ['delta_record', { kind: 'fixture', logical_ops: [] }, 'recordDelta'],
    ['dependency_register', { source_kind: 'mission', source_id: 'm', target_kind: 'artifact', target_id: 'a', edge_kind: 'uses' }, 'registerDependency'],
    ['affected_closure_compile', { root_kind: 'mission', root_id: 'm' }, 'affectedClosure'],
    ['ir_compile', { ir_type: 'ArtifactIR', subject_kind: 'artifact', subject_id: 'a', payload: {} }, 'compileIr'],
    ['ir_bundle_compile', { subject_kind: 'mission', subject_id: 'm', mission: {} }, 'compileIrBundle'],
    ['runtime_image_compile', {}, 'compileRuntimeImage'],
    ['provider_token_bid_compile', { provider_id: 'p', tokenizer_id: 'lexeme-v1', required_tokens: 2 }, 'compileProviderBid'],
    ['lambda_motif_register', { name: 'motif', structure: {} }, 'registerMotif'],
    ['lambda_motif_match', { structure: {} }, 'matchMotifs'],
    ['graph_projection_compile', { graph_kind: 'MissionGraph', root_kind: 'mission', root_id: 'm' }, 'graphProjection'],
    ['bfp_successor_plan', { changed_sections: [] }, 'bfpSuccessorPlan'],
    ['recursive_mission_bootstrap', { mission: { id: 'm' }, spec_path: '/fixture/spec' }, 'writeReceipt'],
    ['recursive_mission_finalize', { mission: { id: 'm' } }, 'writeReceipt'],
    ['substrate_replay_verify', {}, 'verifyReceipts'],
  ];
  for (const [name, args, firstOwner] of cases) {
    const before = f.calls.length;
    const value = await (await server.handle(post('tools/call', { name, arguments: args }, name))).json();
    expect(value.id).toBe(name);
    expect(value.result.isError).toBe(false);
    expect(value.result.structuredContent).toBeDefined();
    expect(f.calls[before][0]).toBe(firstOwner);
  }
});

test('persisting closure and other effects serialize, while real read calls remain concurrent', async () => {
  let release;
  const seen = [];
  const f = fixture({
    ingestArtifact: async () => { seen.push('ingest'); await new Promise(resolve => { release = resolve; }); return { artifact_id: 'a' }; },
    affectedClosure: () => { seen.push('closure'); return { closure_id: 'c' }; },
    artifact: () => { seen.push('read'); return { artifact_id: 'a' }; },
  });
  const server = await f.server();
  const first = server.call('artifact_ingest', { text: 'fixture' });
  await Promise.resolve();
  const second = server.call('affected_closure_compile', { root_kind: 'artifact', root_id: 'a' });
  expect(await server.call('artifact_get', { artifact_id: 'a' })).toEqual({ artifact_id: 'a' });
  expect(seen).toEqual(['ingest', 'read']);
  release();
  await Promise.all([first, second]);
  expect(seen).toEqual(['ingest', 'read', 'closure']);
});

test('owner failure is a bounded isError result, not fabricated proof', async () => {
  const f = fixture({ verifyReceipts: () => { throw new Error('fixture receipt mismatch'); } });
  const server = await f.server();
  const value = await (await server.handle(post('tools/call', { name: 'substrate_replay_verify', arguments: {} }))).json();
  expect(value.result.isError).toBe(true);
  expect(value.result.structuredContent.error).toContain('fixture receipt mismatch');
  expect((await (await server.handle(post('prompts/get', { name: 'no-such-prompt' }))).json()).error).toBeDefined();
});

test('health and status owner exceptions are HTTP unavailable, not 200 success', async () => {
  const f = fixture({ status: () => { throw new Error('fixture token status unavailable'); } });
  const server = await f.server();
  const health = await server.handle(request('/health'));
  const status = await server.handle(request('/status', { headers: auth }));
  expect(health.status).toBe(503);
  expect(status.status).toBe(503);
  expect((await health.json()).error.message).toContain('fixture token status unavailable');
  expect((await status.json()).error.message).toContain('fixture token status unavailable');
});

test('active-request bound refuses excess requests before starting another owner call', async () => {
  let release;
  const f = fixture({ status: async () => new Promise(resolve => { release = () => resolve({ token_binary_status: { exit_code: 0 } }); }) });
  const server = await createRecursiveServer({ token, home: '/tmp/recursive-transport-fixture',
    tokenBinary: '/fixture/token-fabric', store: f.store, maxConcurrent: 1 });
  const first = server.handle(request('/status', { headers: auth }));
  await Promise.resolve();
  const refused = await server.handle(request('/status', { headers: auth }));
  expect(refused.status).toBe(503);
  release();
  expect((await first).status).toBe(200);
});

test('composite mission abort retains started/uncertain receipts and stops downstream effects', async () => {
  const f = fixture({ ingestArtifact: () => { throw new Error('fixture spec changed'); } });
  const server = await f.server();
  const value = await (await server.handle(post('tools/call', { name: 'recursive_mission_bootstrap',
    arguments: { mission: { id: 'm' }, spec_path: '/fixture/spec' } }))).json();
  expect(value.result.isError).toBe(true);
  expect(value.result.structuredContent.error).toContain('reconcile attempt');
  expect(f.calls.map(call => call[0])).toEqual(['writeReceipt', 'writeReceipt']);
  expect(f.calls.map(call => call[1].state)).toEqual(['started', 'uncertain']);
  const missingId = await (await server.handle(post('tools/call', { name: 'recursive_mission_finalize',
    arguments: { mission: {} } }))).json();
  expect(missingId.result.isError).toBe(true);
  expect(f.calls).toHaveLength(2);
});
