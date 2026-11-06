import { test, expect } from 'bun:test';
import { createHash } from 'node:crypto';
import { existsSync, mkdtempSync, mkdirSync, readFileSync, realpathSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { appendJournal, gitCommand, loadConfig, pollOnce, publishDelta, pythonAsciiJson, snapshot, summarizeChanges } from '../project_delta_watcher.mjs';

test('Python-era Git snapshot retains command order, branch, status digest and bounded diff summaries', () => {
  const status = '# branch.oid abc\0# branch.head main\0# branch.upstream origin/main\0? café.txt\0';
  const calls = [];
  const output = [status, 'abcdef\n', 'M\tone.txt\n'.repeat(51), 'A\ttwo.txt\n'];
  const value = snapshot('/fixture/repo', { command: (argv, root) => {
    calls.push({ argv, root }); return output.shift();
  }, now: () => 42.5 });
  expect(calls.map(item => item.argv)).toEqual([
    ['git', 'status', '--porcelain=v2', '--branch', '-z'],
    ['git', 'rev-parse', 'HEAD'],
    ['git', 'diff', '--name-status'],
    ['git', 'diff', '--cached', '--name-status'],
  ]);
  expect(calls.every(item => item.root === '/fixture/repo')).toBe(true);
  expect(value).toEqual({
    source: 'local-project', ts: 42.5, root: '/fixture/repo', branch: 'main', upstream: 'origin/main', head: 'abcdef',
    status_hash: createHash('sha256').update(status).digest('hex'),
    working: { total: 51, counts: { M: 51 }, sample: Array.from({ length: 50 }, () => ({ status: 'M', path: 'one.txt' })), omitted: 1 },
    staged: { total: 1, counts: { A: 1 }, sample: [{ status: 'A', path: 'two.txt' }], omitted: 0 },
  });
  expect(summarizeChanges('')).toEqual({ total: 0, counts: {}, sample: [], omitted: 0 });
});

test('real tiny Git fixture produces the exact porcelain-v2 status hash without Python', () => {
  const root = mkdtempSync(join(tmpdir(), 'bonfyre-project-delta-'));
  try {
    const init = Bun.spawnSync(['git', 'init', '-q', root], { stdout: 'pipe', stderr: 'pipe' });
    expect(init.exitCode).toBe(0);
    writeFileSync(join(root, 'new.txt'), 'fixture\n');
    const value = snapshot(root, { now: () => 1 });
    const status = gitCommand(['git', 'status', '--porcelain=v2', '--branch', '-z'], root);
    expect(value.status_hash).toBe(createHash('sha256').update(status).digest('hex'));
    expect(value.root).toBe(root);
    expect(value.source).toBe('local-project');
    expect(value.head).toBe('HEAD'); // explicit legacy marker for a valid unborn repository
    expect(value.branch).toBe(status.split('\0').find(line => line.startsWith('# branch.head ')).slice('# branch.head '.length).trim());
    const child = Bun.spawnSync([process.execPath, new URL('../project_delta_watcher.mjs', import.meta.url).pathname, '--snapshot'], {
      env: { ...process.env, BONFYRE_ROOT: root, BONFYRE_PROJECT_DELTA_JOURNAL: join(root, 'must-not-exist.jsonl') },
      stdout: 'pipe', stderr: 'pipe',
    });
    expect(child.exitCode).toBe(0);
    expect(JSON.parse(child.stdout.toString()).status_hash).toBe(value.status_hash);
    expect(existsSync(join(root, 'must-not-exist.jsonl'))).toBe(false);
    expect(existsSync(join(root, '.git', 'index.lock'))).toBe(false);
    writeFileSync(join(root, 'new.txt'), 'changed content\n');
    expect(snapshot(root, { now: () => 2 }).status_hash).toBe(value.status_hash); // documented status-only invalidation limit
  } finally { rmSync(root, { recursive: true, force: true }); }
});

test('Git observer suppresses optional index locks without losing inherited environment', () => {
  let options;
  const value = gitCommand(['git', 'status'], '/fixture/repo', {
    env: { PATH: '/fixture/bin', GIT_OPTIONAL_LOCKS: '1' },
    spawn: (_argv, seen) => { options = seen; return { stdout: Buffer.from('ok\n'), exitCode: 0 }; },
  });
  expect(value).toBe('ok\n');
  expect(options.env).toEqual({ PATH: '/fixture/bin', GIT_OPTIONAL_LOCKS: '0' });
  expect(options.cwd).toBe('/fixture/repo');
  expect(options.timeout).toBe(30_000);
  expect(options.maxBuffer).toBe(16 * 1024 * 1024);
  expect(options.killSignal).toBe('SIGKILL');
});

test('failed, timed-out or oversized Git reads refuse instead of publishing a false snapshot', () => {
  const failure = { spawn: () => ({ stdout: Buffer.from('partial'), exitCode: 128 }) };
  expect(() => gitCommand(['git', 'status'], '/fixture/repo', failure)).toThrow('git status: exit 128');
  expect(() => snapshot('/fixture/repo', { command: (argv, root, opts) => gitCommand(argv, root, { ...opts, ...failure }) })).toThrow('git status --porcelain=v2 --branch -z: exit 128');
  expect(() => snapshot('/fixture/repo', { command: () => '' })).toThrow('missing porcelain-v2 branch headers');
  expect(() => gitCommand(['git', 'status'], '/fixture/repo', { spawn: () => ({ stdout: Buffer.alloc(0), exitCode: 1, exitedDueToTimeout: true }) })).toThrow('timed out');
  expect(() => gitCommand(['git', 'status'], '/fixture/repo', { spawn: () => ({ stdout: Buffer.alloc(0), exitCode: 1, exitedDueToMaxBuffer: true }) })).toThrow('16 MiB');
  expect(() => gitCommand(['git', 'status'], '/fixture/repo', { spawn: () => { throw new Error('secret path'); } })).toThrow('git status: spawn failed');
  expect(() => gitCommand(['git', 'rev-parse', 'HEAD'], '/fixture/repo', { ...failure, allowUnbornHead: true })).toThrow('exit 128');
  expect(gitCommand(['git', 'rev-parse', 'HEAD'], '/fixture/repo', {
    allowUnbornHead: true, spawn: () => ({ stdout: Buffer.from('HEAD\n'), exitCode: 128 }),
  })).toBe('HEAD\n');
});

test('journal retains Python ASCII JSON encoding, token-file precedence and source-order fields', () => {
  const home = mkdtempSync(join(tmpdir(), 'bonfyre-project-config-'));
  try {
    const root = join(home, 'repo'); mkdirSync(root);
    const tokenFile = join(home, 'token'); writeFileSync(tokenFile, ' file-token \n');
    const journal = join(home, 'nested', 'deltas.jsonl');
    const config = loadConfig({ BONFYRE_ROOT: root, BONFYRE_MCP_TOKEN_FILE: tokenFile, BONFYRE_MCP_TOKEN: 'env-token', BONFYRE_PROJECT_DELTA_JOURNAL: journal, BONFYRE_PROJECT_WATCH_INTERVAL: '0.75' }, home);
    expect(config).toMatchObject({ root: realpathSync(root), token: 'file-token', journal, intervalMs: 750, gateway: 'http://127.0.0.1:13052' });
    const payload = { source: 'local-project', path: 'café/😀' };
    appendJournal(journal, payload);
    expect(readFileSync(journal, 'utf8')).toBe('{"source":"local-project","path":"caf\\u00e9/\\ud83d\\ude00"}\n');
    expect(pythonAsciiJson(payload)).toBe('{"source":"local-project","path":"caf\\u00e9/\\ud83d\\ude00"}');
    expect(existsSync(join(home, 'nested'))).toBe(true);
    expect(() => loadConfig({ BONFYRE_ROOT: root, BONFYRE_PROJECT_WATCH_INTERVAL: 'NaN' }, home)).toThrow('invalid');
  } finally { rmSync(home, { recursive: true, force: true }); }
});

test('failed delivery backs off while Git observations and journal continue without retry storm', async () => {
  const first = { status_hash: 'a'.repeat(64), head: '1'.repeat(40) };
  const second = { status_hash: 'b'.repeat(64), head: '2'.repeat(40) };
  const third = { status_hash: 'c'.repeat(64), head: '3'.repeat(40) };
  const journal = [], posted = [], messages = [];
  let observed = first, fail = true, clock = 0, observations = 0;
  const deps = {
    observe: () => { observations++; return observed; },
    append: value => journal.push(value),
    publish: value => { posted.push(value); if (fail) throw new Error('unavailable'); },
    log: value => messages.push(value),
    now: () => clock,
  };
  let state = { lastObservedHash: '', pending: null, nextAttemptAt: 0, retryDelayMs: 750 };
  state = await pollOnce(state, deps);
  expect(state).toEqual({ lastObservedHash: first.status_hash, pending: first, nextAttemptAt: 750, retryDelayMs: 1500 });
  expect(journal).toEqual([first]);
  clock = 100;
  observed = second;
  state = await pollOnce(state, deps);
  expect(journal).toEqual([first, second]);
  expect(state.pending).toBe(second);
  expect(state.nextAttemptAt).toBe(750);
  expect(posted).toEqual([first]);
  expect(messages).toHaveLength(1);
  clock = 749;
  state = await pollOnce(state, deps);
  expect(posted).toHaveLength(1);
  expect(messages).toHaveLength(1);
  clock = 750;
  state = await pollOnce(state, deps);
  expect(posted).toEqual([first, second]);
  expect(state.nextAttemptAt).toBe(2250);
  expect(state.retryDelayMs).toBe(3000);
  clock = 800;
  observed = third;
  state = await pollOnce(state, deps);
  expect(journal).toEqual([first, second, third]);
  expect(posted).toHaveLength(2);
  expect(messages).toHaveLength(2);
  expect(state.pending).toBe(third);
  clock = 2250;
  fail = false;
  state = await pollOnce(state, deps);
  expect(posted.at(-1)).toBe(third);
  expect(state).toEqual({ lastObservedHash: third.status_hash, pending: null, nextAttemptAt: 0, retryDelayMs: 750 });
  expect(messages.filter(message => message.startsWith('delta published'))).toHaveLength(1);
  expect(observations).toBe(6);
  state = await pollOnce(state, deps);
  expect(posted).toHaveLength(3);
  observed = first;
  fail = true;
  state = await pollOnce(state, deps);
  expect(state.nextAttemptAt).toBe(3000); // success reset the first retry to 750 ms
  expect(state.retryDelayMs).toBe(1500);
});

test('transport retry delay doubles only on attempted failures and caps at 30 seconds', async () => {
  const current = { status_hash: 'a'.repeat(64), head: '1'.repeat(40) };
  let clock = 0, attempts = 0, messages = 0;
  const deps = { observe: () => current, append: () => {}, publish: () => { attempts++; throw new Error('down'); },
    log: () => { messages++; }, now: () => clock };
  let state = { lastObservedHash: '', pending: null, nextAttemptAt: 0, retryDelayMs: 750 };
  const expected = [750, 1500, 3000, 6000, 12000, 24000, 30000, 30000];
  for (const delay of expected) {
    state = await pollOnce(state, deps);
    expect(state.nextAttemptAt - clock).toBe(delay);
    expect(state.pending).toBe(current);
    clock = state.nextAttemptAt - 1;
    const before = attempts;
    state = await pollOnce(state, deps);
    expect(attempts).toBe(before);
    clock++;
  }
  expect(attempts).toBe(expected.length);
  expect(messages).toBe(expected.length);
});

test('HTTP publication uses the existing authenticated delta route and refuses a failed status', async () => {
  const seen = [];
  const request = async (url, options) => { seen.push({ url, options }); return { ok: true, body: null }; };
  await publishDelta({ source: 'local-project', path: 'café' }, { gateway: 'http://127.0.0.1:13052', token: 'fixture-token', request });
  expect(seen[0].url).toBe('http://127.0.0.1:13052/internal/delta');
  expect(seen[0].options.method).toBe('POST');
  expect(seen[0].options.headers.Authorization).toBe('Bearer fixture-token');
  expect(seen[0].options.redirect).toBe('manual');
  expect(seen[0].options.body).toBe('{"source":"local-project","path":"caf\\u00e9"}');
  await expect(publishDelta({}, { gateway: 'http://127.0.0.1:13052', token: 'x', request: async () => ({ ok: false, status: 503 }) })).rejects.toThrow('503');
  let cancelled = false;
  const body = new ReadableStream({
    start(controller) { controller.enqueue(new Uint8Array(65 * 1024)); },
    cancel() { cancelled = true; },
  });
  await expect(publishDelta({}, { gateway: 'http://127.0.0.1:13052', token: 'x', request: async () => ({ ok: true, body }) })).rejects.toThrow('64 KiB');
  expect(cancelled).toBe(true);
});

test('a failing Git observation logs one line, backs off and recovers without throwing', async () => {
  const messages = [];
  let failing = true;
  const value = { status_hash: 'h1', head: 'abc' };
  const deps = {
    observe: () => { if (failing) throw new Error('git status --porcelain=v2 --branch -z: timed out'); return value; },
    append: () => {}, publish: () => {}, log: message => messages.push(message),
    now: () => 0, retryBaseMs: 1000, observeMaxMs: 4000,
  };
  let state = { lastObservedHash: '', pending: null, nextAttemptAt: 0, retryDelayMs: 1000 };
  state = await pollOnce(state, deps);
  expect(state.observeDelayMs).toBe(1000);
  state = await pollOnce(state, deps);
  state = await pollOnce(state, deps);
  state = await pollOnce(state, deps);
  expect(state.observeDelayMs).toBe(4000);
  expect(messages).toHaveLength(4);
  expect(messages.every(message => !message.includes('\n') && message.startsWith('delta observe failed: git status'))).toBe(true);
  failing = false;
  state = await pollOnce(state, deps);
  expect(state.observeDelayMs).toBeUndefined();
  expect(state.lastObservedHash).toBe('h1');
});
