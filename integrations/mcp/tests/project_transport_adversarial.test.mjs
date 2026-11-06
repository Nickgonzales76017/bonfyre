import { afterAll, expect, test } from 'bun:test';
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createProjectNode } from '../project_node.mjs';
import { projectRun } from '../project_runtime.mjs';

const root = mkdtempSync(join(tmpdir(), 'bf-project-transport-adversarial-'));
const token = 'fixture-bearer-not-a-real-credential';
afterAll(() => rmSync(root, { recursive: true, force: true }));
async function fixture(implementation = async (name, args) => ({ name, args }), options = {}) {
  const calls = [];
  const owner = async (name, args) => { calls.push({ name, args }); return implementation(name, args); };
  const node = await createProjectNode({ root, home: root, token,
    owners: { reads: owner, native: owner, operator: owner }, ...options });
  const request = (message, { authorization = `Bearer ${token}`, raw, signal, headers = {} } = {}) => node.handle(new Request('http://localhost/mcp', {
    method: 'POST', headers: { Authorization: authorization, 'Content-Type': 'application/json', ...headers },
    body: raw ?? JSON.stringify(message), signal,
  }));
  const rpc = async (method, params = {}, extra = {}) => (await request({ jsonrpc: '2.0', id: 'fixture', method, params, ...extra })).json();
  return { node, request, rpc, calls };
}

test('auth refusal precedes parsing and dispatch, including malformed unauthenticated bodies', async () => {
  const f = await fixture();
  for (const authorization of ['', `Bearer ${token}x`, token]) {
    const result = await f.request(null, { raw: '{invalid', authorization });
    expect(result.status).toBe(401);
    expect(await result.json()).toEqual({ error: 'unauthorized' });
  }
  expect(f.calls).toHaveLength(0);
});

test('invalid JSON-RPC ids are never reflected as valid response ids', async () => {
  const f = await fixture();
  for (const id of [{ unexpected: 'object-id' }, ['array-id'], true]) {
    const answer = await f.rpc('ping', {}, { id });
    expect(answer.id).toBeNull();
    expect(answer.error.message).toContain('invalid request id');
  }
  expect(f.calls).toHaveLength(0);
});

test('malformed body, envelope, params and unknown tools never reach owners', async () => {
  const f = await fixture();
  for (const raw of ['{', '[]', 'null', '{"jsonrpc":"1.0","method":"tools/list"}',
    '{"jsonrpc":"2.0","method":"tools/call","params":[]}']) {
    const answer = await (await f.request(null, { raw })).json();
    expect(answer.error).toBeDefined();
  }
  const unknown = await f.rpc('tools/call', { name: '__proto__', arguments: {} });
  expect(unknown.result.isError).toBe(true);
  expect(unknown.result.structuredContent.error).toContain('unknown project tool');
  for (const arguments_ of [[], true, { path: 'a', start_line: 1.25 }, { path: 'a', cwd: '/escape' }]) {
    const answer = await f.rpc('tools/call', { name: 'project_read', arguments: arguments_ });
    expect(answer.result.isError).toBe(true);
  }
  expect(f.calls).toHaveLength(0);
});

test('all owner error envelopes redact the MCP credential, including model resources', async () => {
  const f = await fixture(async () => { throw new Error(`upstream failed with ${token}`); });
  const tool = await f.rpc('tools/call', { name: 'bonfyre_moe_status', arguments: {} });
  expect(JSON.stringify(tool)).not.toContain(token);
  expect(tool.result.isError).toBe(true);
  const resource = await f.rpc('resources/read', { uri: 'bonfyre://models/status' });
  expect(JSON.stringify(resource)).not.toContain(token);
  const value = JSON.parse(resource.result.contents[0].text);
  expect(value.status).toBe('unavailable');
  expect(value.reason).toContain('[redacted]');
  const other = await f.rpc('resources/read', { uri: 'bonfyre://atlas/summary' });
  expect(JSON.stringify(other)).not.toContain(token);
  expect(other.error).toBeDefined();
});

test('invalid resource identifiers refuse without invoking read owners', async () => {
  const f = await fixture();
  for (const uri of ['file:///etc/passwd', 'bonfyre://architecture/../secret', 'bonfyre://matrix/subject/../../secret?x=1']) {
    expect((await f.rpc('resources/read', { uri })).error).toBeDefined();
  }
  expect(f.calls).toHaveLength(0);
});

test('concurrency admission rejects excess requests and releases slots on completion', async () => {
  let release;
  const wait = new Promise(resolve => { release = resolve; });
  const f = await fixture(async () => { await wait; return { ok: true }; }, { maxConcurrent: 1 });
  const first = f.rpc('tools/call', { name: 'project_status', arguments: {} });
  try {
    for (let i = 0; i < 100 && !f.calls.length; i++) await Bun.sleep(1);
    expect(f.calls).toHaveLength(1);
    const rejected = await f.request({ jsonrpc: '2.0', id: 2, method: 'ping' });
    expect(rejected.status).toBe(503);
  } finally { release(); await first; }
  expect((await f.rpc('ping')).result).toEqual({});
});

test('oversized owner result refuses without retrying or re-invoking an effect', async () => {
  const f = await fixture(async () => ({ enormous: 'x'.repeat(10_100_000) }));
  const answer = await f.rpc('tools/call', { name: 'operator_resume', arguments: {} });
  expect(answer.error.message).toContain('response exceeds');
  expect(f.calls).toHaveLength(1);
});

test('malformed UTF-8 and excessive declared bodies refuse before dispatch', async () => {
  const f = await fixture();
  const invalid = await f.node.handle(new Request('http://localhost/mcp', { method: 'POST',
    headers: { Authorization: `Bearer ${token}` }, body: Buffer.from([0xff]) }));
  expect((await invalid.json()).error).toBeDefined();
  const huge = await f.request({ jsonrpc: '2.0', method: 'ping' }, { headers: { 'Content-Length': '20000001' } });
  expect((await huge.json()).error.message).toContain('body size');
  expect(f.calls).toHaveLength(0);
});

test('bounded runner kills a SIGTERM-resistant grandchild that retains stdout after parent exit', async () => {
  const grandchild = join(root, 'grandchild.mjs'), parent = join(root, 'parent.mjs'), pidFile = join(root, 'grandchild.pid');
  writeFileSync(grandchild, `import {writeFileSync} from 'node:fs';
process.on('SIGTERM',()=>{}); writeFileSync(${JSON.stringify(pidFile)},String(process.pid)); setInterval(()=>{},1000);`);
  writeFileSync(parent, `import {spawn} from 'node:child_process';
spawn(process.execPath,[${JSON.stringify(grandchild)}],{stdio:['ignore','inherit','inherit']});
process.on('SIGTERM',()=>process.exit(0)); setInterval(()=>{},1000);`);
  let pid;
  try {
    const result = await projectRun([process.execPath, parent], { cwd: root, timeoutMs: 400 });
    expect(result.timeout).toBe(true);
    expect(result.exit_code).toBe(124);
    expect(result.duration_ms).toBeLessThan(3000);
    expect(existsSync(pidFile)).toBe(true);
    pid = Number(readFileSync(pidFile, 'utf8'));
    let alive = true;
    for (let i = 0; i < 100 && alive; i++) {
      try { process.kill(pid, 0); await Bun.sleep(10); }
      catch (error) { if (error.code === 'ESRCH') alive = false; else throw error; }
    }
    expect(alive).toBe(false);
  } finally {
    if (!pid && existsSync(pidFile)) pid = Number(readFileSync(pidFile, 'utf8'));
    if (pid) { try { process.kill(pid, 'SIGKILL'); } catch { /* fixture already stopped */ } }
  }
}, 5000);

test('bounded runner refuses overflow, invalid UTF-8 and missing executable without truncated success', async () => {
  const overflow = await projectRun([process.execPath, '-e', "process.stdout.write('x'.repeat(100000)); setInterval(()=>{},1000)"], { timeoutMs: 1000, maxOutputBytes: 1024 });
  expect(overflow.overflow).toBe(true);
  expect(overflow.exit_code).toBe(125);
  expect(Buffer.byteLength(overflow.stdout)).toBe(1024);
  const invalid = await projectRun([process.execPath, '-e', 'process.stdout.write(Buffer.from([255]))']);
  expect(invalid.invalid_utf8).toBe(true);
  expect(invalid.exit_code).toBe(126);
  const missing = await projectRun([join(root, 'missing-executable')], { timeoutMs: 1000 });
  expect(missing.exit_code).toBe(127);
  await expect(projectRun(['python3', '-c', 'raise Exception()'])).rejects.toThrow('retired');
});
