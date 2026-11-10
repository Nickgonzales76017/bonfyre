import { afterAll, expect, test } from 'bun:test';
import { Database } from 'bun:sqlite';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { callHistory, createMeshLedgerServer, dispatch, REGISTRY } from '../mesh-ledger.mjs';

const dir = mkdtempSync(join(tmpdir(), 'bonfyre-mesh-ledger-'));
const path = join(dir, 'mesh.sqlite3');
const writer = new Database(path);
writer.exec('CREATE TABLE calls(id TEXT PRIMARY KEY,ts REAL NOT NULL,trace_id TEXT,tool TEXT NOT NULL,args_json TEXT NOT NULL,state TEXT NOT NULL,duration_ms INTEGER NOT NULL,result_json TEXT NOT NULL)');
const insert = writer.query('INSERT INTO calls VALUES(?,?,?,?,?,?,?,?)');
insert.run('a', 1, 'trace-a', 'project_status', '{}', 'passed', 8, '{"ok":true}');
insert.run('b', 2, 'trace-b', 'project_status', '{"path":"."}', 'failed', 13, '{"error":"unreachable"}');
insert.run('c', 3, 'trace-c', 'mesh_status', '{}', 'passed', 3, '{"ok":true}');
writer.close();
const db = new Database(path, { readonly: true, strict: true });
afterAll(() => { db.close(); rmSync(dir, { recursive: true, force: true }); });

test('CUE owns the bounded read-only ledger catalog', () => {
  expect(REGISTRY.tools.map(item => item.name)).toEqual(['mcp_call_history']);
  expect(REGISTRY.tools[0].annotations.readOnlyHint).toBe(true);
  expect(REGISTRY.resources.map(item => item.uri)).toEqual(['bonfyre://mcp/calls']);
  expect(dispatch(db, 'tools/list').tools).toEqual(REGISTRY.tools);
  expect(dispatch(db, 'resources/list').resources).toEqual(REGISTRY.resources);
  expect(dispatch(db, 'initialize').capabilities.resources.subscribe).toBe(false);
});

test('existing mesh ledger is read without mutation and filters are bound parameters', () => {
  expect(callHistory(db).calls.map(row => row.id)).toEqual(['c', 'b', 'a']);
  expect(callHistory(db, { tool: 'project_status', state: 'failed' }).calls.map(row => row.id)).toEqual(['b']);
  expect(callHistory(db, { tool: "project_status' OR 1=1 --" }).calls).toEqual([]);
  expect(callHistory(db, { limit: 1 }).calls.map(row => row.id)).toEqual(['c']);
  expect(callHistory(db, { limit: 1 }).calls[0].result).toEqual({ ok: true });
  expect(callHistory(db, { limit: 1 }).calls[0]).not.toHaveProperty('result_json');
  expect(() => db.exec("INSERT INTO calls VALUES('d',4,'t','t','{}','passed',1,'{}')")).toThrow();
});

test('MCP tool and resource preserve old ledger result shape, reject unknown surfaces', () => {
  const tool = dispatch(db, 'tools/call', { name: 'mcp_call_history', arguments: { state: 'passed', limit: 1 } });
  expect(tool.isError).toBe(false);
  expect(tool.structuredContent.calls.map(row => row.id)).toEqual(['c']);
  expect(JSON.parse(tool.content[0].text)).toEqual(tool.structuredContent);
  const resource = dispatch(db, 'resources/read', { uri: 'bonfyre://mcp/calls' });
  expect(JSON.parse(resource.contents[0].text).calls.length).toBe(3);
  for (const bad of [0, 2001, 1.5, 'abc']) expect(() => callHistory(db, { limit: bad })).toThrow('limit');
  expect(() => callHistory(db, { mutate: true })).toThrow('unknown call-history argument');
  expect(() => dispatch(db, 'tools/call', { name: 'project_apply_patch' })).toThrow('unknown tool');
  expect(() => dispatch(db, 'resources/read', { uri: 'bonfyre://project/status' })).toThrow('unknown resource');
});

test('HTTP carrier requires token and allowed origin, and serves only bounded MCP methods', async () => {
  const server = createMeshLedgerServer({ dbPath: path, token: 'fixture-token', origins: ['https://mcp.fixture.test'] });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const address = server.address();
  const base = `http://127.0.0.1:${address.port}`;
  const post = (method, params, headers = {}) => fetch(`${base}/mcp`, { method: 'POST', headers: { 'Content-Type': 'application/json', ...headers }, body: JSON.stringify({ jsonrpc: '2.0', id: 7, method, params }) });
  try {
    expect((await fetch(`${base}/health`)).status).toBe(200);
    expect((await post('tools/list')).status).toBe(401);
    expect((await post('tools/list', {}, { Authorization: 'Bearer fixture-token', Origin: 'https://wrong.test' })).status).toBe(403);
    const response = await post('tools/call', { name: 'mcp_call_history', arguments: { limit: 1 } }, { Authorization: 'Bearer fixture-token', Origin: 'https://mcp.fixture.test' });
    expect(response.status).toBe(200);
    expect(response.headers.get('MCP-Protocol-Version')).toBe('2025-06-18');
    expect((await response.json()).result.structuredContent.calls.map(row => row.id)).toEqual(['c']);
    const refused = await post('prompts/list', {}, { Authorization: 'Bearer fixture-token' });
    expect((await refused.json()).error.code).toBe(-32000);
    expect((await post('notifications/initialized', {}, { Authorization: 'Bearer fixture-token' })).status).toBe(202);
  } finally { await new Promise(resolve => server.close(resolve)); }
});
