#!/usr/bin/env bun
// Native producer for the existing /internal/delta MCP gateway lane.
// The local journal records each observed Git status before a delivery attempt.
import { createHash } from 'node:crypto';
import { appendFileSync, existsSync, mkdirSync, readFileSync, realpathSync } from 'node:fs';
import { homedir } from 'node:os';
import { dirname, join } from 'node:path';

const utf8 = new TextDecoder('utf-8', { fatal: true });

export function pythonAsciiJson(value) {
  return JSON.stringify(value).replace(/[\u0080-\u{10ffff}]/gu, char => {
    const point = char.codePointAt(0);
    if (point <= 0xffff) return `\\u${point.toString(16).padStart(4, '0')}`;
    const offset = point - 0x10000;
    return `\\u${(0xd800 + (offset >> 10)).toString(16)}\\u${(0xdc00 + (offset & 1023)).toString(16)}`;
  });
}

export function summarizeChanges(text, limit = 50) {
  const lines = text.split('\n').filter(line => line.trim());
  const counts = {};
  const sample = [];
  for (const line of lines) {
    const tab = line.indexOf('\t');
    const code = (tab < 0 ? line : line.slice(0, tab)).trim();
    const file = tab < 0 ? '' : line.slice(tab + 1);
    counts[code] = (counts[code] ?? 0) + 1;
    if (sample.length < limit) sample.push({ status: code, path: file });
  }
  return { total: lines.length, counts, sample, omitted: Math.max(0, lines.length - sample.length) };
}

export function gitCommand(argv, root, { spawn = Bun.spawnSync, env = process.env, allowUnbornHead = false } = {}) {
  const operation = argv.slice(1).join(' ').slice(0, 96);
  let result;
  try {
    result = spawn(argv, { cwd: root, stdout: 'pipe', stderr: 'ignore',
      env: { ...env, GIT_OPTIONAL_LOCKS: '0' }, timeout: 30_000,
      maxBuffer: 16 * 1024 * 1024, killSignal: 'SIGKILL' });
  } catch { throw new Error(`git ${operation}: spawn failed`); }
  if (result.exitedDueToTimeout) throw new Error(`git ${operation}: timed out`);
  if (result.exitedDueToMaxBuffer) throw new Error(`git ${operation}: output exceeded 16 MiB`);
  if (!result.stdout) throw new Error(`git ${operation}: no output pipe`);
  // Python's subprocess.run(text=True) decodes strictly and folds CRLF.
  let output;
  try { output = utf8.decode(result.stdout).replaceAll('\r\n', '\n'); }
  catch { throw new Error(`git ${operation}: invalid UTF-8 output`); }
  if (result.exitCode !== 0) {
    // In an unborn repo, rev-parse HEAD exits 128 but prints "HEAD"; the
    // inherited producer reported that marker. Permit only this exact case.
    if (allowUnbornHead && operation === 'rev-parse HEAD' && result.exitCode === 128 && output.trim() === 'HEAD') return output;
    throw new Error(`git ${operation}: exit ${result.exitCode ?? 'unknown'}`);
  }
  return output;
}

export function snapshot(root, { command = gitCommand, now = () => Date.now() / 1000 } = {}) {
  const status = command(['git', 'status', '--porcelain=v2', '--branch', '-z'], root);
  const fields = status.split('\0');
  if (!fields.some(line => line.startsWith('# branch.oid ')) || !fields.some(line => line.startsWith('# branch.head '))) {
    throw new Error('git status: missing porcelain-v2 branch headers');
  }
  const unborn = fields.includes('# branch.oid (initial)');
  const head = command(['git', 'rev-parse', 'HEAD'], root, { allowUnbornHead: unborn }).trim();
  const names = command(['git', 'diff', '--name-status'], root);
  const staged = command(['git', 'diff', '--cached', '--name-status'], root);
  let branch = '';
  let upstream = '';
  // Porcelain-v2 -z includes the same branch headers as the newline variant.
  // Reading it once preserves the digest input and avoids a redundant status walk.
  for (const line of fields) {
    if (line.startsWith('# branch.head ')) {
      const value = line.slice('# branch.head '.length).trim();
      branch = value === '(detached)' ? '' : value;
    } else if (line.startsWith('# branch.upstream ')) {
      upstream = line.slice('# branch.upstream '.length).trim();
    }
  }
  return {
    source: 'local-project', ts: now(), root, branch, upstream, head,
    // Inherited contract: status shape only. Content re-edits with unchanged
    // porcelain output do not generate a new delta; this is not a file digest.
    status_hash: createHash('sha256').update(status, 'utf8').digest('hex'),
    working: summarizeChanges(names), staged: summarizeChanges(staged),
  };
}

export function loadConfig(env = process.env, home = homedir()) {
  const tokenFile = env.BONFYRE_MCP_TOKEN_FILE ?? join(home, '.bonfyre/mcp/token');
  const token = existsSync(tokenFile) ? readFileSync(tokenFile, 'utf8').trim() : (env.BONFYRE_MCP_TOKEN ?? '');
  const interval = Number(env.BONFYRE_PROJECT_WATCH_INTERVAL ?? '0.75');
  if (!Number.isFinite(interval) || interval < 0) throw new Error('invalid BONFYRE_PROJECT_WATCH_INTERVAL');
  const root = realpathSync(env.BONFYRE_ROOT ?? join(home, 'Developer/Bonfyre'));
  return {
    root, token, intervalMs: interval * 1000,
    gateway: env.BONFYRE_MCP_GATEWAY ?? 'http://127.0.0.1:13052',
    journal: env.BONFYRE_PROJECT_DELTA_JOURNAL ?? join(home, '.bonfyre/mcp/project-deltas.jsonl'),
  };
}

export function appendJournal(journal, payload) {
  mkdirSync(dirname(journal), { recursive: true });
  appendFileSync(journal, pythonAsciiJson(payload) + '\n', 'utf8');
}

export async function publishDelta(payload, { gateway, token, request = fetch } = {}) {
  const response = await request(gateway + '/internal/delta', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
    body: pythonAsciiJson(payload),
    redirect: 'manual',
    signal: AbortSignal.timeout(10_000),
  });
  if (!response.ok) {
    await response.body?.cancel().catch(() => {});
    throw new Error(`delta gateway HTTP ${response.status}`);
  }
  // The gateway normally returns a tiny receipt. Never buffer an unbounded
  // response or print its body (which could contain sensitive carrier data).
  if (response.body) {
    const reader = response.body.getReader();
    let bytes = 0;
    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        bytes += value.byteLength;
        if (bytes > 64 * 1024) {
          await reader.cancel().catch(() => {});
          throw new Error('delta gateway response exceeded 64 KiB');
        }
      }
    } finally { reader.releaseLock(); }
  }
}

export async function pollOnce(state, { observe, append, publish, log = console.log,
  now = () => performance.now(), retryBaseMs = 750, retryMaxMs = 30_000, observeMaxMs = 300_000 } = {}) {
  let current;
  try { current = await observe(); }
  catch (error) {
    // A slow or failing Git read is an observation gap, not a crash: one line,
    // then back off so a large working tree cannot become a restart/log storm.
    const observeDelayMs = Math.min(observeMaxMs, state.observeDelayMs ? state.observeDelayMs * 2 : retryBaseMs);
    log(`delta observe failed: ${error?.message ?? String(error)}; next try in ${Math.ceil(observeDelayMs / 1000)}s`);
    return { ...state, observeDelayMs };
  }
  let next = { lastObservedHash: state.lastObservedHash, pending: state.pending,
    nextAttemptAt: state.nextAttemptAt ?? 0, retryDelayMs: state.retryDelayMs ?? retryBaseMs };
  if (current.status_hash !== state.lastObservedHash) {
    await append(current);
    // Keep the existing transport deadline when a new observation supersedes
    // pending; churn must not turn an unavailable gateway into a retry storm.
    next = { ...next, lastObservedHash: current.status_hash, pending: current };
  }
  if (!next.pending || now() < next.nextAttemptAt) return next;
  try {
    await publish(next.pending);
    log(`delta published ${next.pending.status_hash.slice(0, 12)} head=${next.pending.head.slice(0, 12)}`);
    return { ...next, pending: null, nextAttemptAt: 0, retryDelayMs: retryBaseMs };
  } catch (error) {
    log(`delta publish failed: ${error?.name ?? 'Error'}: ${error?.message ?? String(error)}`);
    return { ...next, nextAttemptAt: now() + next.retryDelayMs,
      retryDelayMs: Math.min(retryMaxMs, next.retryDelayMs * 2) };
  }
}

export async function runWatcher(config = loadConfig()) {
  if (!config.token) throw new Error('MCP token missing');
  let state = { lastObservedHash: '', pending: null, nextAttemptAt: 0,
    retryDelayMs: Math.min(30_000, Math.max(750, config.intervalMs)) };
  while (true) {
    state = await pollOnce(state, {
      observe: () => snapshot(config.root),
      append: value => appendJournal(config.journal, value),
      publish: value => publishDelta(value, config),
      retryBaseMs: Math.min(30_000, Math.max(750, config.intervalMs)),
    });
    await Bun.sleep(Math.max(config.intervalMs, state.observeDelayMs ?? 0));
  }
}

if (import.meta.main) {
  const args = process.argv.slice(2);
  if (args.length === 1 && args[0] === '--snapshot') {
    // Read-only benchmark/diagnostic: no token read, journal append, or HTTP.
    const root = realpathSync(process.env.BONFYRE_ROOT ?? join(homedir(), 'Developer/Bonfyre'));
    const value = snapshot(root);
    console.log(JSON.stringify({ status_hash: value.status_hash, head: value.head, branch: value.branch,
      upstream: value.upstream, working_total: value.working.total, staged_total: value.staged.total }));
  } else if (args.length === 0) {
    runWatcher().catch(error => { console.error(error); process.exitCode = 1; });
  } else {
    console.error('usage: bun project_delta_watcher.mjs [--snapshot]');
    process.exitCode = 2;
  }
}
