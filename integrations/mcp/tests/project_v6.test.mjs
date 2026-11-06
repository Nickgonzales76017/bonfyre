import { afterEach, expect, test } from 'bun:test';
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { V6_TOOLS } from '../v6_catalog.mjs';
import { createProjectV6, V6_PROJECT_TOOLS } from '../project_v6.mjs';
import { createProjectNode } from '../project_node.mjs';

const homes = [];
afterEach(() => { for (const home of homes.splice(0)) rmSync(home, { recursive: true, force: true }); });
const fixture = () => { const home = mkdtempSync(join(tmpdir(), 'bonfyre-project-v6-')); homes.push(home); return home; };

test('all V6 contracts derive from the owner catalog, including no-argument reads', () => {
  expect(V6_PROJECT_TOOLS.map(t => [t.name, t.inputSchema])).toEqual(V6_TOOLS.map(t => [t.name, t.inputSchema]));
  expect(V6_PROJECT_TOOLS).toHaveLength(18);
  for (const name of ['v6_status', 'v6_history', 'v6_sparse', 'v6_verify'])
    expect(V6_PROJECT_TOOLS.find(t => t.name === name).inputSchema.properties).toEqual({});
});

test('V6 HTTPS uses only a dedicated credential and fixed configured endpoint', async () => {
  const home = fixture(), key = join(home, 'v6-token'); writeFileSync(key, 'v6-dedicated');
  const seen = [];
  const call = createProjectV6({ home, token: 'project-general', env: {
    BONFYRE_V6_URL: 'https://v6.example', BONFYRE_V6_TOKEN_FILE: key,
  }, request: async (url, options) => {
    const body = JSON.parse(options.body); seen.push({ url, options, body });
    return Response.json({ jsonrpc: '2.0', id: body.id, result: { structuredContent: { owner: 'v6' } } });
  } });
  expect(await call('v6_status', {})).toEqual({ owner: 'v6' });
  expect(await call('v6_proof', {})).toEqual({ owner: 'v6' });
  expect(seen.map(item => item.url)).toEqual(['https://v6.example/mcp', 'https://v6.example/mcp']);
  expect(seen.every(item => item.options.headers.authorization === 'Bearer v6-dedicated')).toBe(true);
  expect(seen.every(item => item.body.params.arguments && Object.keys(item.body.params.arguments).length === 0)).toBe(true);
  await expect(call('unregistered', {})).rejects.toThrow('unbound');
  expect(seen).toHaveLength(2);
  const noDedicated = createProjectV6({ home, token: 'project-general', env: { BONFYRE_V6_URL: 'https://v6.example' },
    request: async () => { throw new Error('must not export project credential'); } });
  await expect(noDedicated('v6_status', {})).rejects.toThrow('own configured credential');
});

test('V6 write admission and STOP refuse before outbound dispatch', async () => {
  const home = fixture(); let dispatched = 0;
  const request = async () => { dispatched++; throw new Error('must not dispatch'); };
  const gated = createProjectV6({ home, token: 'project', env: {}, request });
  await expect(gated('v6_sparse_snapshot', { confirm: true })).rejects.toThrow('write gate');
  const operator = join(home, 'operator'); mkdirSync(operator); writeFileSync(join(operator, 'STOP'), 'preserve');
  const stopped = createProjectV6({ home, token: 'project', env: {
    BONFYRE_MCP_PROJECT_WRITE_ENABLED: '1', BONFYRE_OPERATOR_HOME: operator,
  }, request });
  await expect(stopped('v6_history_federate', { confirm: true })).rejects.toThrow('STOP');
  expect(dispatched).toBe(0);
});

test('Project schema rejects endpoint selection and missing V6 confirmation before owner call', async () => {
  const home = fixture(), key = join(home, 'dedicated'); writeFileSync(key, 'v6-dedicated');
  let requests = 0;
  const node = await createProjectNode({ root: process.cwd(), home, token: 'project', env: {
    BONFYRE_V6_URL: 'https://v6.example', BONFYRE_V6_TOKEN_FILE: key,
  }, owners: { reads: async () => ({}), operator: async () => ({}), native: async () => ({}), spines: async () => ({}) },
  request: async (_url, options) => { requests++; const body = JSON.parse(options.body);
    return Response.json({ jsonrpc: '2.0', id: body.id, result: { structuredContent: { selected: body.params.name } } }); } });
  const call = async (name, args) => {
    const response = await node.handle(new Request('http://127.0.0.1/mcp', { method: 'POST',
      headers: { authorization: 'Bearer project' },
      body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name, arguments: args } }) }));
    return (await response.json()).result;
  };
  expect((await call('v6_status', { endpoint: 'https://attacker.example' })).isError).toBe(true);
  expect((await call('v6_history_federate', {})).isError).toBe(true);
  expect(requests).toBe(0);
  expect((await call('v6_status', {})).structuredContent).toEqual({ selected: 'v6_status' });
  expect(requests).toBe(1);
});
