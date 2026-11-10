// What this does not prove:
// - the network call is a function supplied by this test, so no request leaves the process and no remote behaviour is observed.
// - some checks run in embedded Python, which this estate has retired; they say nothing about a native path.
import { test, expect } from 'bun:test';
import { mkdtempSync, writeFileSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { client, purgeEstate, runConnector, loadConfig } from '../cloudflare-reconcile.mjs';

const config = loadConfig();
const account = 'a'.repeat(32);
const zone = i => (i + 1).toString(16).padStart(32, '0');
function fixture(transform = rows => rows) {
  const calls = [];
  return { calls, api: async (method, path, body) => {
    calls.push({ method, path, body });
    if (method === 'POST') return {};
    const root = new URL(`https://mock${path}`).searchParams.get('name');
    const i = config.territories.findIndex(row => row.root === root);
    return transform([{ name: root, id: zone(i) }], i);
  } };
}

test('purge preflights all exact zones and preserves authored path arguments', async () => {
  const f = fixture();
  const paths = ['/a"b', 'café?q=a b', '/a"b'];
  expect(await purgeEstate(f.api, config, paths)).toEqual({ purged: config.territories.length, everything: false });
  expect(f.calls.slice(0, config.territories.length).every(c => c.method === 'GET')).toBe(true);
  for (const [i, call] of f.calls.slice(config.territories.length).entries()) {
    expect(call.body).toEqual({ files: [`https://${config.territories[i].root}/a"b`, `https://${config.territories[i].root}/café?q=a b`] });
  }
  const all = fixture();
  await purgeEstate(all.api, config, ['--everything']);
  expect(all.calls.filter(c => c.method === 'POST').every(c => c.body.purge_everything === true)).toBe(true);
});

test('invalid paths and incomplete or ambiguous zone sets never write', async () => {
  for (const args of [[], ['--everything', '/x'], [''], ['/x\n'], ['--apply']]) {
    const f = fixture();
    await expect(purgeEstate(f.api, config, args)).rejects.toThrow();
    expect(f.calls).toHaveLength(0);
  }
  for (const transform of [
    (rows, i) => i === 1 ? [] : rows,
    rows => [...rows, ...rows],
    rows => [{ ...rows[0], name: 'wrong.example' }],
    rows => [{ ...rows[0], id: 'invalid' }],
    rows => [{ ...rows[0], id: zone(0) }],
  ]) {
    const f = fixture(transform);
    await expect(purgeEstate(f.api, config, ['/x'])).rejects.toThrow();
    expect(f.calls.filter(c => c.method === 'POST')).toHaveLength(0);
  }
});

test('API failures are checked without exposing upstream secrets', async () => {
  for (const fetcher of [
    async () => { throw new Error('secret-key'); },
    async () => new Response('secret-key', { status: 403 }),
    async () => new Response('secret-key'),
    async () => Response.json({ success: false, errors: ['secret-key'] }),
    async () => Response.json({ success: true }),
  ]) {
    let failure;
    try { await client('secret-key', fetcher)('GET', '/mock'); } catch (error) { failure = error; }
    expect(failure).toBeInstanceOf(Error);
    expect(failure.message).not.toContain('secret-key');
  }
  expect(await client('key', async () => Response.json({ success: true, result: null }))('GET', '/mock')).toBeNull();
});

test('a rejected purge stops further writes and reports only accepted roots', async () => {
  const f = fixture(), reported = [];
  let attempted = 0;
  const api = async (...args) => {
    if (args[0] === 'POST' && ++attempted === 2) throw new Error('rejected write');
    return f.api(...args);
  };
  await expect(purgeEstate(api, config, ['/changed'], root => reported.push(root))).rejects.toThrow('rejected write');
  expect(attempted).toBe(2);
  expect(reported).toEqual([config.territories[0].root]);
});

test('connector narrows credentials, forwards signals and preserves exit status', async () => {
  let finish;
  const exited = new Promise(resolve => { finish = resolve; });
  const killed = [];
  const before = ['SIGINT', 'SIGTERM'].map(s => process.listenerCount(s));
  let invoked;
  const running = runConnector(async () => 'scoped-token', config, {
    CLOUDFLARE_ACCOUNT_ID: account, CLOUDFLARE_API_TOKEN: 'admin-secret', CLOUDFLARE_info_zones: 'other-secret',
    CLOUDFLARED_BIN: '/mock/cloudflared',
  }, (argv, options) => {
    invoked = { argv, options };
    return { exited, kill: signal => killed.push(signal) };
  });
  await Promise.resolve();
  expect(invoked.argv).toEqual(['/mock/cloudflared', '--no-autoupdate', '--loglevel', 'info', 'tunnel', 'run']);
  expect(invoked.options.env.TUNNEL_TOKEN).toBe('scoped-token');
  expect(invoked.options.env).not.toHaveProperty('CLOUDFLARE_API_TOKEN');
  expect(invoked.options.env).not.toHaveProperty('CLOUDFLARE_info_zones');
  process.emit('SIGTERM'); process.emit('SIGINT');
  expect(killed).toEqual(['SIGTERM', 'SIGINT']);
  finish(7);
  expect(await running).toBe(7);
  expect(['SIGINT', 'SIGTERM'].map(s => process.listenerCount(s))).toEqual(before);
  for (const token of [null, '', 'bad token', 'x\n', 'x'.repeat(65537)]) {
    await expect(runConnector(async () => token, config, { CLOUDFLARE_ACCOUNT_ID: account }, () => {
      throw new Error('must not spawn');
    })).rejects.toThrow('invalid connector token');
  }
});

function entrypointFixture() {
  const dir = mkdtempSync(join(tmpdir(), 'bonfyre-cloudflare-entrypoints-'));
  const put = (name, body, mode = 0o600) => { const path = join(dir, name); writeFileSync(path, body, { mode }); return path; };
  const log = join(dir, 'requests.jsonl');
  const childLog = join(dir, 'child.json');
  const roots = config.territories.map(row => row.root);
  const preload = put('preload.mjs', `
import { appendFileSync } from 'node:fs';
const roots = ${JSON.stringify(roots)};
globalThis.fetch = async (url, options) => {
  const path = new URL(url).pathname;
  appendFileSync(${JSON.stringify(log)}, JSON.stringify({url, method:options.method, body:options.body})+'\\n');
  if (process.env.MOCK_HTTP_FAILURE) return new Response('secret-admin-key', {status:403});
  if (path === '/client/v4/zones' && options.method === 'GET') {
    const name = new URL(url).searchParams.get('name');
    const index = roots.indexOf(name);
    if (index < 0) throw new Error('unexpected zone');
    return Response.json({success:true,result:[{name,id:(index+1).toString(16).padStart(32,'0')}]});
  }
  if (/^\\/client\\/v4\\/zones\\/[a-f0-9]{32}\\/purge_cache$/.test(path) && options.method==='POST')
    return Response.json({success:true,result:{}});
  if (path === '/client/v4/accounts/${account}/cfd_tunnel/${config.tunnelID}/token' && options.method==='GET')
    return Response.json({success:true,result:'scoped-run-token'});
  throw new Error('unexpected network access');
};
`);
  // Absolute executables let the actual launchd-facing wrapper run with minimal PATH.
  const bun = put('bun', `#!/bin/sh\nexec '${process.execPath}' --preload '${preload}' "$@"\n`, 0o700);
  const cue = put('cue', `#!/bin/sh\nprintf '%s\\n' '${JSON.stringify(config)}'\n`, 0o700);
  const cloudflared = put('cloudflared', `#!${process.execPath}\nimport {writeFileSync} from 'node:fs';\nwriteFileSync(${JSON.stringify(childLog)}, JSON.stringify({argv:process.argv.slice(2),token:process.env.TUNNEL_TOKEN,admin:process.env.CLOUDFLARE_API_TOKEN,zones:process.env.CLOUDFLARE_info_zones}));\nprocess.exit(7);\n`, 0o700);
  for (const name of ['python', 'python3', 'curl']) put(name, '#!/bin/sh\necho retired-tool-invoked >&2\nexit 99\n', 0o700);
  const refs = put('refs.env', `CLOUDFLARE_API_TOKEN=secret-admin-key\nCLOUDFLARE_ACCOUNT_ID=${account}\nCLOUDFLARE_info_zones='Bearer secret-zone-key'\n`);
  const env = { PATH: `${dir}:/usr/bin:/bin`, HOME: dir, BONFYRE_CLOUD_REFS: refs,
    BONFYRE_BUN: bun, BONFYRE_CUE: cue, CLOUDFLARED_BIN: cloudflared };
  return { dir, log, childLog, env, run: (name, args = [], extra = {}) => Bun.spawnSync([
    '/bin/sh', resolve(import.meta.dir, '..', name), ...args,
  ], { env: { ...env, ...extra }, stdout: 'pipe', stderr: 'pipe' }) };
}

test('actual purge shell uses shared owner without Python, curl, or live network', () => {
  const f = entrypointFixture();
  try {
    const result = f.run('estate_purge.sh', ['/quote"/café?q=a b']);
    expect(result.exitCode).toBe(0);
    expect(result.stderr.toString()).toBe('');
    const calls = readFileSync(f.log, 'utf8').trim().split('\n').map(JSON.parse);
    expect(calls).toHaveLength(config.territories.length * 2);
    for (const [i, call] of calls.filter(c => c.method === 'POST').entries()) {
      expect(JSON.parse(call.body)).toEqual({ files: [`https://${config.territories[i].root}/quote"/café?q=a b`] });
    }
    const failure = f.run('estate_purge.sh', ['/x'], { MOCK_HTTP_FAILURE: '1' });
    expect(failure.exitCode).not.toBe(0);
    expect(failure.stderr.toString()).toContain('HTTP 403');
    expect(failure.stderr.toString()).not.toContain('secret-admin-key');
    expect(f.run('estate_purge.sh').exitCode).not.toBe(0);
  } finally { rmSync(f.dir, { recursive: true, force: true }); }
});

test('actual launchd connector entrypoint propagates child exit without leaking credentials', () => {
  const f = entrypointFixture();
  try {
    const result = f.run('run_cloudflared_continuation.sh');
    expect(result.exitCode).toBe(7);
    expect(result.stdout.toString()).toBe('');
    expect(result.stderr.toString()).toBe('');
    expect(JSON.parse(readFileSync(f.childLog, 'utf8'))).toEqual({
      argv: ['--no-autoupdate', '--loglevel', 'info', 'tunnel', 'run'], token: 'scoped-run-token',
    });
    const failure = f.run('run_cloudflared_continuation.sh', [], { MOCK_HTTP_FAILURE: '1' });
    expect(failure.exitCode).not.toBe(0);
    expect(failure.stderr.toString()).not.toContain('secret-admin-key');
  } finally { rmSync(f.dir, { recursive: true, force: true }); }
});
