import { afterAll, beforeAll, expect, test } from 'bun:test';
import http from 'node:http';
import { mkdtemp, writeFile, readdir, readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Database } from 'bun:sqlite';
import { createRecursiveTools } from '../recursive_overlay_tools.mjs';
import { createRecursiveOverlay, LOCAL_TOOLS, REGISTRY, RICH_CATALOG, openLedger } from '../recursive_overlay.mjs';
import { createNativeRichTools } from '../../../mcp/rich-native-tools.mjs';
import { boundedPipe } from '../bounded_pipe.mjs';
import { installedDecisionOwner } from '../../../components/BonfyreDecision/installed-runtime.mjs';

const temp = await mkdtemp(join(tmpdir(), 'bonfyre-recursive-native-'));
const calls = [];
const fixtureRun = async argv => { calls.push(argv); return { exit_code: 0, stdout: JSON.stringify({ http_status: 404, ok: true }), stderr: '', duration_ms: 2 }; };
const fixtureMatrix = (name, args) => ({ name, args, native_receipt: { exit_code: 0 } });

test('every rich descriptor has exactly one native or project owner', async () => {
  const rich = new Set(RICH_CATALOG.tools.map(item => item.name));
  const native = new Set(REGISTRY.nativeRichTools);
  const project = new Set(REGISTRY.forwardedProjectTools);
  expect([...rich].filter(name => native.has(name) && project.has(name))).toEqual([]);
  expect([...rich].filter(name => !native.has(name) && !project.has(name))).toEqual([]);
  const source = await readFile(new URL('../../../mcp/rich-native-tools.mjs', import.meta.url), 'utf8');
  const cases = new Set([...source.matchAll(/case '([a-z_]+)':/g)].map(match => match[1]));
  expect([...native].filter(name => !cases.has(name))).toEqual([]);
  expect([...cases].filter(name => !native.has(name))).toEqual([]);
});

test('native process output drains fully with bounded retained tail', async () => {
  const stream = new ReadableStream({ start(controller) { controller.enqueue(Buffer.from('first')); controller.enqueue(Buffer.from('second')); controller.close(); } });
  expect(await boundedPipe(stream, 6)).toBe('second');
});

test('recursive owner calls preserve exact CLI coordinates and source boundaries', async () => {
  const tools = createRecursiveTools({
    env: { BONFYRE_FOREIGN_ROOT: join(temp, 'foreign'), BONFYRE_PROJECT_BINARY: '/bin/bonfyre-project', BONFYRE_CMS_BINARY: '/bin/bonfyre-cms', BONFYRE_CMS_DB: '/db/cms', BONFYRE_CMS_SCHEMAS: '/schema', BONFYRE_SURFACE_ORIGIN: 'http://localhost:8787', BONFYRE_SURFACE_HOST: 'example.org' },
    run: fixtureRun, matrix: fixtureMatrix,
    request: async () => new Response(JSON.stringify({ records: [{ id: 'one' }] }), { status: 200 }),
    baseCall: async name => ({ owner: name }),
  });
  const matrix = await tools.call('matrix_address_resolve', { host: 'school.example.org', path: '/a' });
  expect(matrix.name).toBe('matrix_address_resolve');
  const cms = await tools.call('cms_record', { action: 'create', content_type: 'note', attributes: { title: 'Draft' } });
  expect(cms.native_owner).toBe('BonfyreCMS');
  expect(calls.at(-1)).toEqual(['/bin/bonfyre-cms', 'entry', 'create', 'note', '{"title":"Draft"}', '--db', '/db/cms', '--schemas', '/schema']);
  expect(cms.forbidden_inference).toContain('not published');
  expect((await tools.call('foreign_resolution', { action: 'history', source_url: 'https://example.org/a' })).native_owner).toBe('BonfyreProject');
  expect(calls.at(-1)).toEqual(['/bin/bonfyre-project', 'foreign', 'history', 'https://example.org/a']);
  await tools.call('foreign_resolution', { action: 'resolve', source_url: 'https://example.org' });
  expect(calls.at(-1).at(-1)).toBe('https://example.org');
  const surface = await tools.call('surface_project', { action: 'read', pattern: 'school', selector: 'second' });
  expect(surface.surface).toBe('http://localhost:8787/_surface/school?page=second');
  const frontier = await tools.call('recursive_closure_frontier', { host: 'school.example.org' });
  expect(Object.keys(frontier.planes)).toEqual(['exact_semantic', 'material_external', 'latent_model', 'execution', 'economic']);
  expect(frontier.operating_contract.human_submit_remains_effect_boundary).toBe(true);
  expect((await tools.read('bonfyre://closure/operating-contract')).laws.length).toBeGreaterThan(0);
  expect(() => tools.call('cms_record', { action: 'delete', content_type: 'note', entry_id: '../escape' })).toThrow();
});

test('foreign observation keeps a receipt but drops error body', async () => {
  const root = join(temp, 'observe');
  const tools = createRecursiveTools({ env: { BONFYRE_FOREIGN_ROOT: root }, matrix: fixtureMatrix, run: async argv => {
    await writeFile(argv[argv.indexOf('--receipt') + 1], '{"http_status":404}');
    await writeFile(argv[argv.indexOf('--body') + 1], 'not found');
    return { exit_code: 0, stdout: '', stderr: '', duration_ms: 1 };
  } });
  const value = await tools.call('foreign_twin_observe', { twin_id: 'school', url: 'https://example.org/missing' });
  expect(value.observation.http_status).toBe(404);
  expect(value.retention.retained).toBe(false);
  expect(value.body_path).toBeNull();
  expect((await readdir(join(root, 'bodies'))).length).toBe(0);
});

test('circuit chooses one priced capability but refuses authority inference', async () => {
  const financeDb = join(temp, 'finance.db');
  const db = new Database(financeDb);
  db.exec('CREATE TABLE services(name TEXT, category TEXT, sell_price REAL, source TEXT)');
  db.query('INSERT INTO services VALUES (?,?,?,?)').run('parcel-ownership', 'property', 2, 'book');
  db.query('INSERT INTO services VALUES (?,?,?,?)').run('parcel-map', 'property', 9, 'book');
  db.close();
  const tools = createRecursiveTools({ env: { BONFYRE_FINANCE_DB: financeDb, BONFYRE_ORGANS_DIR: join(temp, 'absent') }, request: async () => new Response(JSON.stringify({ capabilities: [{ name: 'parcel-ownership', description: 'parcel ownership concentration' }] }), { status: 200 }), projectCall: async () => ({ authority: { allowed: ['discover'], not_granted: ['execute', 'deliver'] }, state: { stdout: 'state=open_quote' } }) });
  const value = await tools.call('capability_circuit_compile', { demand: 'parcel ownership concentration', budget_usd: 5 });
  expect(value.circuit[0].capability).toBe('parcel-ownership');
  expect(value.cost.total_usd).toBe(2);
  expect(value.executable).toBe(false);
  expect(value.refused.some(item => item.candidate === 'parcel-map')).toBe(true);
});

let base, project, overlay, ledger, baseCalls = 0, decisionCalls = 0;
beforeAll(async () => {
  base = http.createServer(async (req, res) => {
    if (req.url === '/health') { res.setHeader('Content-Type', 'application/json'); res.end('{"status":"ok"}'); return; }
    baseCalls++;
    const chunks = []; for await (const chunk of req) chunks.push(chunk);
    const input = JSON.parse(Buffer.concat(chunks));
    const result = input.method === 'tools/list' ? { tools: [{ name: 'base_tool', inputSchema: { type: 'object' } }, { name: 'project_status', inputSchema: { type: 'object', properties: { stale: true } } }] }
      : input.method === 'resources/list' ? { resources: [{ uri: 'bonfyre://base' }] }
      : input.method === 'tools/call' ? { content: [{ type: 'text', text: '{"base":true}' }] }
      : { protocolVersion: '2025-06-18' };
    res.setHeader('Content-Type', 'application/json'); res.end(JSON.stringify({ jsonrpc: '2.0', id: input.id, result }));
  });
  project = http.createServer(async (req, res) => {
    const chunks = []; for await (const chunk of req) chunks.push(chunk);
    const input = JSON.parse(Buffer.concat(chunks));
    const result = input.method === 'tools/list' ? { tools: [{ name: 'institutional_prepare', inputSchema: { type: 'object', properties: { owner: { const: true } } } }, { name: 'project_status', inputSchema: { type: 'object', properties: { current_owner: { const: true } } } }, { name: 'unapproved_project_tool' }] }
      : { content: [{ type: 'text', text: '{"project":true}' }] };
    res.setHeader('Content-Type', 'application/json'); res.end(JSON.stringify({ jsonrpc: '2.0', id: input.id, result }));
  });
  await new Promise(resolve => base.listen(0, '127.0.0.1', resolve));
  await new Promise(resolve => project.listen(0, '127.0.0.1', resolve));
  ledger = openLedger(join(temp, 'mesh.db'));
  const made = createRecursiveOverlay({ token: 'secret', runtimeUrl: `http://127.0.0.1:${base.address().port}`, projectUrl: `http://127.0.0.1:${project.address().port}/mcp`, ledger, operations: { call: async name => ({ native: name }), read: async uri => ({ uri }) }, richOperations: createNativeRichTools({ env: { BONFYRE_MCP_DB: join(temp, 'mesh.db') }, previewPaths: REGISTRY.previewPaths }),
    decisionOptions: { apiKey: 'fixture-decision-key', accountId: '0123456789abcdef0123456789abcdef',
      fetcher: async (_url, options) => { decisionCalls++; const request = JSON.parse(options.body);
        return { ok: true, json: async () => ({ success: true, result: { model: request.model,
          answers: { route: { type: 'choice', choice: 'foreign_resolution', confidence: 0.8,
            probabilities: { foreign_resolution: 0.8, none: 0.2 } },
          needs_review: { type: 'noul', noul: 0.1 } } } }) }; } } });
  overlay = made.server;
  await made.ready;
  await new Promise(resolve => overlay.listen(0, '127.0.0.1', resolve));
});
afterAll(async () => { await Promise.all([base, project, overlay].map(server => new Promise(resolve => server.close(resolve)))); });
const address = () => `http://127.0.0.1:${overlay.address().port}/mcp`;
async function rpc(method, params = {}, headers = {}) {
  const response = await fetch(address(), { method: 'POST', headers: { Authorization: 'Bearer secret', 'Content-Type': 'application/json', ...headers }, body: JSON.stringify({ jsonrpc: '2.0', id: 1, method, params }) });
  return { status: response.status, body: await response.json() };
}

test('native overlay merges exact schemas, forwards base, records native calls', async () => {
  const tools = (await rpc('tools/list')).body.result.tools;
  expect(tools.length).toBe(new Set([...RICH_CATALOG.tools.map(item => item.name), ...LOCAL_TOOLS.map(item => item.name), 'institutional_prepare']).size);
  expect(tools.find(item => item.name === 'institutional_prepare').inputSchema.properties.owner.const).toBe(true);
  expect(tools.find(item => item.name === 'project_status').inputSchema.properties.current_owner.const).toBe(true);
  expect(tools.some(item => item.name === 'unapproved_project_tool')).toBe(false);
  expect((await rpc('tools/call', { name: 'estate_query', arguments: { sql: 'DROP TABLE observation' } })).body.result.structuredContent.error).toContain('read-only');
  expect((await rpc('tools/call', { name: 'base_tool', arguments: {} })).body.error.message).toContain('unsupported');
  const native = await rpc('tools/call', { name: 'matrix_address_resolve', arguments: { host: 'example.org' } });
  expect(native.body.result.structuredContent.native).toBe('matrix_address_resolve');
  expect((await rpc('resources/list')).body.result.resources.some(item => item.uri === 'bonfyre://closure/frontier')).toBe(true);
  expect((await rpc('resources/read', { uri: 'bonfyre://project/status' })).body.result.contents[0].text).toContain('project');
  expect((await rpc('resources/read', { uri: 'bonfyre://mcp/calls' })).body.result.contents[0].text).toContain('calls');
  // Prompts are the catalogue's own, each now carrying the server's icons.
  const prompts = (await rpc('prompts/list')).body.result.prompts;
  expect(prompts.map(({ icons, ...prompt }) => prompt)).toEqual(RICH_CATALOG.prompts.map(({ icons, ...prompt }) => prompt));
  for (const prompt of prompts) if (prompt.icons) expect(prompt.icons.every(icon => typeof icon.src === 'string' && icon.src.startsWith('https://'))).toBe(true);
  expect((await rpc('prompts/get', { name: 'diagnose_live_system', arguments: { focus: 'MCP' } })).body.result.messages[0].content.text).toContain('live Bonfyre MCP');
  expect((await rpc('resources/read', { uri: 'bonfyre://closure/operating-contract' })).body.result.contents[0].text).toContain('bonfyre://closure/operating-contract');
  const projectResult = await rpc('tools/call', { name: 'institutional_prepare', arguments: {} });
  expect(projectResult.body.result.structuredContent.project).toBe(true);
  expect((await rpc('tools/call', { name: 'project_status', arguments: {} })).body.result.structuredContent.project).toBe(true);
  const db = new Database(join(temp, 'mesh.db'), { readonly: true });
  expect(db.query('SELECT count(*) AS n FROM calls').get().n).toBe(4);
  expect(baseCalls).toBe(0);
  db.close();
});

test('native overlay enforces bearer and origin before any forwarding', async () => {
  expect((await fetch(address(), { method: 'POST', body: '{}' })).status).toBe(401);
  expect((await rpc('ping', {}, { Origin: 'https://attacker.example' })).status).toBe(403);
  expect((await rpc('initialize')).body.result.protocolVersion).toBe('2025-06-18');
  expect(REGISTRY.forwardedProjectTools).toContain('institutional_prepare');
  const health = await fetch(new URL('/health', address()));
  expect(health.status).toBe(200);
  expect((await health.json()).tools).toBe(new Set([...RICH_CATALOG.tools.map(item => item.name), ...LOCAL_TOOLS.map(item => item.name), 'institutional_prepare']).size);
});

test('rich tool cutovers use CUE policy and the shared call ledger', async () => {
  expect(REGISTRY.nativeRichTools).toContain('oracle_deep_flow');
  expect(REGISTRY.nativeRichTools).toContain('architecture_atlas');
  expect(new Set(REGISTRY.nativeRichTools).size).toBe(REGISTRY.nativeRichTools.length);
  expect(REGISTRY.previewPaths).toContain('/api/runtime-parity/checks/run-live-read');
  const result = await rpc('tools/call', { name: 'mcp_call_history', arguments: { limit: 2 } });
  expect(result.body.result.structuredContent.calls.length).toBe(2);
  expect(result.body.result.structuredContent.calls.some(call => call.tool === 'institutional_prepare')).toBe(true);
});

test('the live MCP catalogue exposes only read-only exact candidates to DecisionPlane', async () => {
  const descriptor = LOCAL_TOOLS.find(tool => tool.name === 'decision_tool_shortlist');
  expect(descriptor.annotations.readOnlyHint).toBe(false);
  expect(descriptor.inputSchema.properties.provider.enum).toContain('local');
  expect(descriptor.inputSchema.properties.provider.default).toBe('local');
  expect(descriptor.description).toContain('without remote fallback');
  const before = decisionCalls;
  const planned = await rpc('tools/call', { name: 'decision_tool_shortlist', arguments: {
    intent: 'Read current foreign observation history', shortlist: ['foreign_resolution'],
    information_policy_class: 'private',
  } });
  expect(planned.body.result.structuredContent).toMatchObject({ action: 'none', invoked_tool: null,
    receipt: { state: 'planned', provider: 'local', model: installedDecisionOwner.model,
      model_artifact_digest: installedDecisionOwner.artifact_digest,
      contract: { owner: 'MCP', information_policy_class: 'private' } } });
  const explicitLocal = await rpc('tools/call', { name: 'decision_tool_shortlist', arguments: {
    intent: 'Read current foreign observation history', shortlist: ['foreign_resolution'],
    information_policy_class: 'private', provider: 'local',
  } });
  expect(explicitLocal.body.result.structuredContent.receipt.request_digest)
    .toBe(planned.body.result.structuredContent.receipt.request_digest);
  expect(decisionCalls).toBe(before);
  const effecting = await rpc('tools/call', { name: 'decision_tool_shortlist', arguments: {
    intent: 'Change CMS state', shortlist: ['cms_record'], information_policy_class: 'public',
  } });
  expect(effecting.body.result.isError).toBe(true);
  expect(effecting.body.result.structuredContent.error).toContain('read-only');
  const privateExecution = await rpc('tools/call', { name: 'decision_tool_shortlist', arguments: {
    intent: 'Read current foreign observation history', shortlist: ['foreign_resolution'],
    information_policy_class: 'private', execute: true, provider: 'clef-flash',
  } });
  expect(privateExecution.body.result.isError).toBe(true);
  expect(privateExecution.body.result.structuredContent.error).toContain('information policy forbids');
  const diagnostic = await rpc('tools/call', { name: 'decision_tool_shortlist', arguments: {
    intent: 'Read current foreign observation history', shortlist: ['foreign_resolution'],
    information_policy_class: 'public', execute: true, provider: 'clef-flash',
  } });
  expect(diagnostic.body.result.structuredContent).toMatchObject({ action: 'none', invoked_tool: null,
    proposal: { suggested_candidate: 'foreign_resolution', action: 'none' },
    receipt: { state: 'diagnostic', provider: 'clef-flash' } });
  expect(decisionCalls).toBe(before + 1);
});

test('native receipt ledger retains evidence without approval tokens or patch bodies', () => {
  ledger.record('trace-secret', 'project_apply_patch', { approval_token: 'private', patch: '--- a/secret\n+++ b/secret' }, 'passed', 1, { receipt_id: 'r1' });
  const db = new Database(join(temp, 'mesh.db'), { readonly: true });
  const row = db.query("SELECT args_json FROM calls WHERE trace_id='trace-secret'").get();
  db.close();
  expect(row.args_json).not.toContain('private');
  expect(row.args_json).not.toContain('--- a/secret');
  expect(JSON.parse(row.args_json).patch.bytes).toBeGreaterThan(0);
  ledger.record('trace-intent', 'decision_tool_shortlist', { intent: 'private need' }, 'passed', 1, {});
  const check = new Database(join(temp, 'mesh.db'), { readonly: true });
  const intentRow = check.query("SELECT args_json FROM calls WHERE trace_id='trace-intent'").get();
  check.close();
  expect(intentRow.args_json).not.toContain('private need');
  expect(JSON.parse(intentRow.args_json).intent.bytes).toBeGreaterThan(0);
});

test('native delta, events and metrics share the rich mesh SQLite state', async () => {
  const delta = await fetch(new URL('/internal/delta', address()), { method: 'POST', headers: { Authorization: 'Bearer secret', 'Content-Type': 'application/json' }, body: JSON.stringify({ source: 'project', head: 'abc', status_hash: 'def' }) });
  expect(delta.status).toBe(202);
  const seq = (await delta.json()).seq;
  expect(seq).toBeGreaterThan(0);
  const events = await fetch(new URL('/events?after=0&limit=200', address()), { headers: { Authorization: 'Bearer secret' } });
  const values = await events.json();
  expect(values.events.some(item => item.kind === 'project.delta' && item.payload.seq === seq)).toBe(true);
  expect(values.next_after).toBe(values.events.at(-1).seq);
  const metrics = await fetch(new URL('/metrics', address()), { headers: { Authorization: 'Bearer secret' } });
  const totals = await metrics.json();
  expect(totals.deltas).toBe(1);
  expect(totals.calls).toBeGreaterThan(0);
  expect((await fetch(new URL('/events', address()))).status).toBe(401);
  expect((await fetch(new URL('/metrics', address()), { headers: { Authorization: 'Bearer secret', Origin: 'https://evil.example' } })).status).toBe(403);
});

test('all five rich resources bind to native rich or project owners', async () => {
  const calls = [];
  const owned = createRecursiveOverlay({
    token: 'secret', projectUrl: `http://127.0.0.1:${project.address().port}/mcp`,
    ledger: openLedger(join(temp, 'resource-ledger.db')),
    operations: { call: async () => ({}), read: async () => ({}) },
    richOperations: async (name, args) => { calls.push([name, args]); return { owner: 'rich', name, args }; },
  });
  await owned.ready;
  await new Promise(resolve => owned.server.listen(0, '127.0.0.1', resolve));
  try {
    for (const uri of RICH_CATALOG.resources.map(item => item.uri)) {
      const response = await fetch(`http://127.0.0.1:${owned.server.address().port}/mcp`, {
        method: 'POST', headers: { Authorization: 'Bearer secret', 'Content-Type': 'application/json' },
        body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'resources/read', params: { uri } }),
      });
      expect((await response.json()).result.contents[0].uri).toBe(uri);
    }
    expect(calls.map(row => row[0])).toEqual(['mesh_status', 'release_attest', 'oracle_profile_receipts', 'mcp_call_history']);
    expect(calls[2][1]).toEqual({ limit: 300 });
    expect(baseCalls).toBe(0);
  } finally { await new Promise(resolve => owned.server.close(resolve)); }
});

test('missing project owner refuses project tools and resources without a substitute', async () => {
  const absent = http.createServer(async (req, res) => {
    const chunks = []; for await (const chunk of req) chunks.push(chunk);
    const input = JSON.parse(Buffer.concat(chunks));
    res.setHeader('Content-Type', 'application/json');
    res.end(JSON.stringify({ jsonrpc: '2.0', id: input.id, result: { tools: [] } }));
  });
  await new Promise(resolve => absent.listen(0, '127.0.0.1', resolve));
  const calls = [];
  const made = createRecursiveOverlay({
    token: 'secret', projectUrl: `http://127.0.0.1:${absent.address().port}/mcp`,
    ledger: openLedger(join(temp, 'absent-owner-ledger.db')),
    operations: { call: async () => ({}), read: async () => ({}) },
    richOperations: async name => { calls.push(name); return { name }; },
  });
  await made.ready;
  await new Promise(resolve => made.server.listen(0, '127.0.0.1', resolve));
  const call = async (method, params) => {
    const response = await fetch(`http://127.0.0.1:${made.server.address().port}/mcp`, {
      method: 'POST', headers: { Authorization: 'Bearer secret', 'Content-Type': 'application/json' },
      body: JSON.stringify({ jsonrpc: '2.0', id: 1, method, params }),
    });
    return response.json();
  };
  try {
    const tool = await call('tools/call', { name: 'project_status', arguments: {} });
    expect(tool.result.isError).toBe(true);
    expect(tool.result.structuredContent.error).toContain('project owner unavailable');
    const resource = await call('resources/read', { uri: 'bonfyre://project/status' });
    expect(resource.error.message).toContain('project owner unavailable');
    expect(calls).toEqual([]);
  } finally {
    await Promise.all([made.server, absent].map(server => new Promise(resolve => server.close(resolve))));
  }
});
