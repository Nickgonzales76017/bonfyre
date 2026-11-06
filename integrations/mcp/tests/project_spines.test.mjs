import { afterEach, expect, test } from 'bun:test';
import { mkdtempSync, readFileSync, rmSync, writeFileSync, mkdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createProjectSpines, SPINES_PROJECT_TOOLS } from '../project_spines.mjs';
import { createProjectNode } from '../project_node.mjs';
import { createSpinesStore } from '../spines_store.mjs';
import { createSpinesServer } from '../spines_server.mjs';

const fixtures = [];
afterEach(() => { for (const { home, close } of fixtures.splice(0)) { close?.(); rmSync(home, { recursive: true, force: true }); } });
function fixture() {
  const home = mkdtempSync(join(tmpdir(), 'bonfyre-project-spines-'));
  const entry = { home }; fixtures.push(entry); return entry;
}
test('all existing Spines schemas are reused and hidden proof writes require write admission', () => {
  const original = JSON.parse(readFileSync(new URL('../spines_catalog.json', import.meta.url), 'utf8')).tools;
  expect(SPINES_PROJECT_TOOLS.map(t => [t.name, t.inputSchema])).toEqual(original.map(t => [t.name, t.inputSchema]));
  for (const name of ['spines_verify', 'r5_affected_closure', 'r5_parity_verify'])
    expect(SPINES_PROJECT_TOOLS.find(t => t.name === name).annotations.readOnlyHint).toBe(false);
});
test('remote route and credential are deployment-owned, never derived from caller arguments', async () => {
  const { home } = fixture(), key = join(home, 'owner-token'); writeFileSync(key, 'scoped-spines-token');
  const seen = [];
  const call = createProjectSpines({ home, token: 'project-token', env: {
    BONFYRE_SPINES_URL: 'https://execution.example', BONFYRE_SPINES_TOKEN_FILE: key,
  }, request: async (url, options) => {
    const body = JSON.parse(options.body); seen.push({ url, options, body });
    return Response.json({ jsonrpc: '2.0', id: body.id, result: { structuredContent: { rules: [] } } });
  } });
  expect(await call('r6_rules_list', {})).toEqual({ rules: [] });
  expect(seen[0].url).toBe('https://execution.example/mcp');
  expect(seen[0].options.headers.authorization).toBe('Bearer scoped-spines-token');
  expect(seen[0].options.redirect).toBe('error');
  await expect(call('unregistered', {})).rejects.toThrow('unbound');
  expect(seen).toHaveLength(1);
  const unscoped = createProjectSpines({ home, token: 'project-token', env: { BONFYRE_SPINES_URL: 'https://execution.example' },
    request: async () => { throw new Error('must not export project token'); } });
  await expect(unscoped('r6_rules_list', {})).rejects.toThrow('own configured credential');
});
test('write gate and STOP refuse before network admission, including misleading historical reads', async () => {
  const { home } = fixture(); let requests = 0;
  const request = async () => { requests++; throw new Error('must not dispatch'); };
  const closed = createProjectSpines({ home, env: {}, token: 'project-token', request });
  for (const name of ['r6_helix_compile', 'spines_verify', 'r5_parity_verify'])
    await expect(closed(name, {})).rejects.toThrow('write gate');
  const operator = join(home, 'operator'); mkdirSync(operator); writeFileSync(join(operator, 'STOP'), 'preserve');
  const stopped = createProjectSpines({ home, env: { BONFYRE_MCP_PROJECT_WRITE_ENABLED: '1', BONFYRE_OPERATOR_HOME: operator }, token: 'project-token', request });
  await expect(stopped('r6_helix_reduce', {})).rejects.toThrow('STOP');
  expect(requests).toBe(0);
});
test('Project authenticated route composes real Spines/Wasm through a simulated HTTPS owner, not a second evaluator', async () => {
  const f = fixture(), key = join(f.home, 'owner-token'); writeFileSync(key, 'spines-only');
  const store = createSpinesStore({ home: join(f.home, 'state'), projectRoot: process.cwd(),
    helixVm: '/native/unused', deltaKernel: '/native/unused', kernelProbe: '/native/unused' });
  const owner = await createSpinesServer({ token: 'spines-only', store, stopPaths: [] }); f.close = () => owner.close();
  let requests = 0;
  const project = await createProjectNode({ root: process.cwd(), home: f.home, token: 'project-only',
    env: { BONFYRE_MCP_PROJECT_WRITE_ENABLED: '1', BONFYRE_SPINES_URL: 'https://execution.example', BONFYRE_SPINES_TOKEN_FILE: key },
    owners: { reads: async () => ({}), operator: async () => ({}), native: async () => ({}) },
    request: (url, options) => { requests++; return owner.handle(new Request(url, options)); } });
  async function rpc(name, args, credential = 'project-only') {
    const result = await project.handle(new Request('https://project.example/mcp', { method: 'POST',
      headers: { authorization: `Bearer ${credential}` },
      body: JSON.stringify({ jsonrpc: '2.0', id: 42, method: 'tools/call', params: { name, arguments: args } }) }));
    return { status: result.status, body: await result.json() };
  }
  expect((await rpc('r6_helix_compile', { term: 'add(2,3)' }, 'wrong')).status).toBe(401);
  expect((await rpc('r6_helix_compile', { term: 'add(2,3)', endpoint: 'https://attacker.example' })).body.result.isError).toBe(true);
  expect(requests).toBe(0);
  const compiled = (await rpc('r6_helix_compile', { term: 'add(2,3)' })).body.result.structuredContent;
  const result = (await rpc('r6_helix_reduce', { helix_id: compiled.helix_id })).body.result;
  expect(result.isError).toBe(false); expect(result.structuredContent.witness.result).toBe(5);
  expect(result.structuredContent.probe.execution_kind).toBe('in_process_wasm');
  expect(result.structuredContent.receipt.state).toBe('passed'); expect(store.verifyReceipts().verified).toBe(true);
  expect(requests).toBe(2);
});
