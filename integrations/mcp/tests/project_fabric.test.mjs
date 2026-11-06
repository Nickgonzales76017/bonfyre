import { afterEach, expect, test } from 'bun:test';
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { FABRIC_TOOLS } from '../fabric_server.mjs';
import { createProjectFabric, FABRIC_PROJECT_TOOLS } from '../project_fabric.mjs';
import { createProjectNode } from '../project_node.mjs';

const homes = [];
afterEach(() => { for (const home of homes.splice(0)) rmSync(home, { recursive: true, force: true }); });
const fixture = () => { const home = mkdtempSync(join(tmpdir(), 'bonfyre-project-fabric-')); homes.push(home); return home; };

test('Project delivery operations retain exact Fabric schemas and effect annotations', () => {
  expect(FABRIC_PROJECT_TOOLS.map(t => t.name)).toEqual([
    'fabric_status', 'implementation_relay', 'task_status', 'task_cancel', 'recent_receipts',
  ]);
  for (const tool of FABRIC_PROJECT_TOOLS) {
    const { ['x-bonfyre-owner']: owner, ...contract } = tool;
    expect(owner).toBe('fabric');
    expect(contract).toEqual(FABRIC_TOOLS.find(t => t.name === tool.name));
  }
  expect(FABRIC_PROJECT_TOOLS.filter(t => !t.annotations.readOnlyHint).map(t => t.name))
    .toEqual(['implementation_relay', 'task_cancel']);
});

test('Fabric placement uses a fixed endpoint and requires a separate remote credential', async () => {
  const home = fixture(), credential = join(home, 'fabric-token'); writeFileSync(credential, 'fabric-only');
  const seen = [];
  const request = async (url, options) => {
    const body = JSON.parse(options.body); seen.push({ url, options, body });
    return Response.json({ jsonrpc: '2.0', id: body.id, result: { structuredContent: { owner: 'fabric' } } });
  };
  const call = createProjectFabric({ home, token: 'project-only', env: {
    BONFYRE_FABRIC_URL: 'https://fabric.example', BONFYRE_FABRIC_TOKEN_FILE: credential,
  }, request });
  expect(await call('task_status', { task_id: 'fixture', detail: 'summary' })).toEqual({ owner: 'fabric' });
  expect(seen[0].url).toBe('https://fabric.example/mcp');
  expect(seen[0].options.headers.authorization).toBe('Bearer fabric-only');
  expect(seen[0].body.params.arguments).toEqual({ task_id: 'fixture', detail: 'summary' });
  await expect(call('dispatch_codex', { prompt: 'not part of delivery binding' })).rejects.toThrow('unbound');
  const missing = createProjectFabric({ home, token: 'project-only', env: { BONFYRE_FABRIC_URL: 'https://fabric.example' }, request });
  await expect(missing('fabric_status', {})).rejects.toThrow('own configured credential');
  expect(seen).toHaveLength(1);
});

test('STOP blocks submissions, permits cancellation, and never bypasses the write gate', async () => {
  const home = fixture(), operator = join(home, 'operator'); mkdirSync(operator); writeFileSync(join(operator, 'STOP'), 'keep stopped');
  const calls = [];
  const request = async (_url, options) => {
    const body = JSON.parse(options.body); calls.push(body.params.name);
    return Response.json({ jsonrpc: '2.0', id: body.id, result: { structuredContent: { received: body.params.name } } });
  };
  const env = { BONFYRE_OPERATOR_HOME: operator, BONFYRE_MCP_PROJECT_WRITE_ENABLED: '1' };
  const call = createProjectFabric({ home, env, token: 'fixture', request });
  await expect(call('implementation_relay', { prompt: 'must not submit' })).rejects.toThrow('STOP');
  expect(await call('task_cancel', { task_id: 'fixture' })).toEqual({ received: 'task_cancel' });
  expect(await call('fabric_status', {})).toEqual({ received: 'fabric_status' });
  delete env.BONFYRE_MCP_PROJECT_WRITE_ENABLED;
  await expect(call('task_cancel', { task_id: 'fixture' })).rejects.toThrow('write gate');
  await expect(call('implementation_relay', { prompt: 'must not submit' })).rejects.toThrow('write gate');
  expect(calls).toEqual(['task_cancel', 'fabric_status']);
});

test('Project validates delivery inputs before forwarding and preserves owner refusals', async () => {
  const home = fixture(), calls = [];
  const node = await createProjectNode({ root: process.cwd(), home, token: 'project',
    env: { BONFYRE_MCP_PROJECT_WRITE_ENABLED: '1' },
    owners: { reads: async () => ({}), operator: async () => ({}), native: async () => ({}) },
    request: async (_url, options) => {
      const body = JSON.parse(options.body); calls.push(body.params);
      return Response.json({ jsonrpc: '2.0', id: body.id, result: {
        structuredContent: { error: 'fabric is stopped' }, isError: true,
      } });
    } });
  const call = async (name, args) => {
    const response = await node.handle(new Request('http://127.0.0.1/mcp', { method: 'POST',
      headers: { authorization: 'Bearer project' },
      body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name, arguments: args } }) }));
    return (await response.json()).result;
  };
  for (const args of [{ prompt: 'x', endpoint: 'https://elsewhere.example' },
    { prompt: 'x', source_files: [{ path: 'x', sha256: false }] }, { apply_to_project: true }])
    expect((await call('implementation_relay', args)).isError).toBe(true);
  expect(calls).toHaveLength(0);
  const args = { prompt: 'fixture', source_files: [{ path: 'scripts/new.mjs', sha256: null }], apply_to_project: false };
  const result = await call('implementation_relay', args);
  expect(result.isError).toBe(true);
  expect(result.structuredContent.error).toContain('fabric is stopped');
  expect(calls).toEqual([{ name: 'implementation_relay', arguments: args }]);
});
