import { test, expect } from 'bun:test';
import { TOOLS, callMatrixTool, createMatrixMcpServer } from '../matrix_aperture.mjs';

const fixture = (binary, argv, timeout) => ({ exit_code: 0, stdout: JSON.stringify({ argv, timeout }), stderr: '', duration_ms: 3 });

test('registry is one bounded read-only Matrix tool family', () => {
  expect(TOOLS.map(t => t.name)).toEqual(['matrix_address_resolve', 'matrix_subject_constellation', 'matrix_closure_compile']);
  expect(TOOLS.every(t => t.annotations.readOnlyHint && t.inputSchema.additionalProperties === false)).toBe(true);
});

test('native calls preserve argv, receipt and sparse authority boundary', () => {
  const opts = { binary: '/fixture/bonfyre-web-address', db: '/fixture/matrix.db', runner: fixture };
  const resolved = callMatrixTool('matrix_address_resolve', { host: 'EXAMPLE.COM.', path: '/work', audience: 'public' }, opts);
  expect(resolved.resolution.argv).toEqual(['resolve', '--db', '/fixture/matrix.db', '--host', 'example.com', '--path', '/work', '--audience', 'public']);
  expect(resolved.native_receipt).toEqual({ exit_code: 0, stderr: '', duration_ms: 3 });
  expect(resolved.forbidden_inference).toContain('effect authority');
  const closure = callMatrixTool('matrix_closure_compile', { host: 'example.com', format: 'zarr', timeout_seconds: 8 }, opts);
  expect(closure.closure.timeout).toBe(8000);
  expect(closure.unresolved_closures).toContain('economic commitment');
});

test('invalid hosts, paths, subjects and timeout are refused before native execution', () => {
  let calls = 0;
  const opts = { runner: () => { calls++; return fixture(); } };
  expect(() => callMatrixTool('matrix_address_resolve', { host: 'bad/host' }, opts)).toThrow('hostname');
  expect(() => callMatrixTool('matrix_address_resolve', { host: 'example.com', path: 'https://other' }, opts)).toThrow('local Matrix path');
  expect(() => callMatrixTool('matrix_subject_constellation', { subject: '../etc/passwd' }, opts)).toThrow('identifier');
  expect(() => callMatrixTool('matrix_closure_compile', { host: 'example.com', timeout_seconds: 121 }, opts)).toThrow('5..120');
  expect(calls).toBe(0);
});

test('MCP HTTP aperture authenticates and exposes only its three owner-backed tools', async () => {
  const server = createMatrixMcpServer({ token: 'fixture-token', call: (name, args) => ({ name, args, authority: false }) });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  try {
    const base = `http://127.0.0.1:${server.address().port}`;
    const rpc = (method, params = {}, headers = {}) => fetch(`${base}/mcp`, { method: 'POST', headers: { Authorization: 'Bearer fixture-token', 'Content-Type': 'application/json', ...headers }, body: JSON.stringify({ jsonrpc: '2.0', id: method, method, params }) });
    expect((await fetch(`${base}/mcp`, { method: 'POST' })).status).toBe(401);
    expect((await rpc('ping', {}, { Origin: 'https://evil.example' })).status).toBe(403);
    const init = await (await rpc('initialize')).json();
    expect(init.result.serverInfo.name).toBe('bonfyre-matrix-mcp-aperture');
    const tools = await (await rpc('tools/list')).json();
    expect(tools.result.tools).toHaveLength(3);
    const call = await (await rpc('tools/call', { name: 'matrix_address_resolve', arguments: { host: 'example.com' } })).json();
    expect(call.result.structuredContent.authority).toBe(false);
    expect(call.result.content).toHaveLength(1);
    const unknown = await (await rpc('resources/list')).json();
    expect(unknown.error.message).toContain('unsupported method');
  } finally { await new Promise(resolve => server.close(resolve)); }
});
