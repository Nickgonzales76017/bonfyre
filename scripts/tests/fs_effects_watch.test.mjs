// What this does not prove:
// - a dependency is replaced by an object built in this test, so the real one is not exercised.
// - some checks run in embedded Python, which this estate has retired; they say nothing about a native path.
import { afterEach, beforeEach, expect, test } from 'bun:test';
import { Database } from 'bun:sqlite';
import { existsSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { parseEffect, tickEffects } from '../bonfyre-fs-effects-watch.mjs';

const watcher = fileURLToPath(new URL('../bonfyre-fs-effects-watch', import.meta.url));
let root, previous;
beforeEach(() => {
  root = mkdtempSync(join(tmpdir(), 'bonfyre-fs-effect-'));
  mkdirSync(join(root, 'effects/pending'), { recursive: true });
  previous = process.env.BONFYRE_STATE_DIR;
  process.env.BONFYRE_STATE_DIR = join(root, 'state');
});
afterEach(() => {
  if (previous === undefined) delete process.env.BONFYRE_STATE_DIR;
  else process.env.BONFYRE_STATE_DIR = previous;
  rmSync(root, { recursive: true, force: true });
});
const effect = () => ({ mission_id: 'm-fs-test', adapter: 'publish-local',
  target_uri: `file://${join(root, 'target " quoted [x].txt')}`,
  input_artifact_uri: `file://${join(root, 'input with space.txt')}`, authority: 'operator with spaces' });
function queue(name, value) {
  writeFileSync(join(root, 'effects/pending', name), typeof value === 'string' ? value : JSON.stringify(value));
}
function mockOwner(mode = 'ok') {
  const path = join(root, 'owner.mjs');
  writeFileSync(path, `#!/usr/bin/env bun
import {appendFileSync} from 'node:fs';
const args=process.argv.slice(2);
appendFileSync(${JSON.stringify(join(root, 'calls.jsonl'))},JSON.stringify(args)+'\\n');
if(args[1]==='claim') console.log('claim_token=test-token');
if(args[1]==='effect-prepare'&&${JSON.stringify(mode)}==='prepare-fail') { console.error('quoted "failure"'); process.exit(1); }
if(args[1]==='effect-commit'&&${JSON.stringify(mode)}!=='ambiguous') console.log('receipt_id=r-test\\nevent_id=e-test');
`, { mode: 0o700 });
  return path;
}

test('production entrypoint publishes through real native WorkGraph with no Python or Perl', () => {
  const payload = 'actual native publish-local witness\n';
  writeFileSync(join(root, 'input with space.txt'), payload);
  queue('publish quoted.effect', effect());
  const forbidden = join(root, 'forbidden'); mkdirSync(forbidden);
  for (const command of ['python', 'python3', 'perl'])
    writeFileSync(join(forbidden, command), '#!/bin/sh\necho forbidden-runtime >&2\nexit 97\n', { mode: 0o700 });
  const run = Bun.spawnSync([watcher, 'tick', root], { env: { ...process.env, PATH: `${forbidden}:${process.env.PATH}` } });
  expect(run.exitCode).toBe(0);
  const receiptPath = join(root, 'effects/committed/publish quoted.effect.receipt.json');
  if (!existsSync(receiptPath)) {
    const errors = readdirSync(join(root, 'effects/failed')).filter(name => name.endsWith('.json'));
    throw new Error(errors.map(name => readFileSync(join(root, 'effects/failed', name), 'utf8')).join('\n') || new TextDecoder().decode(run.stderr));
  }
  const receipt = JSON.parse(readFileSync(receiptPath, 'utf8'));
  expect(receipt.mission_id).toBe('m-fs-test');
  expect(receipt.receipt_id.length).toBeGreaterThan(0);
  expect(readFileSync(join(root, 'target " quoted [x].txt'), 'utf8')).toBe(payload);
  const db = new Database(join(root, 'state/fabric.db'), { readonly: true });
  try {
    expect(db.query('SELECT COUNT(*) AS n FROM workgraph_nodes').get().n).toBe(1);
    expect(Bun.spawnSync([watcher, 'run', root, '--once'], { env: process.env }).exitCode).toBe(0);
    expect(db.query('SELECT COUNT(*) AS n FROM workgraph_nodes').get().n).toBe(1);
  } finally { db.close(); }
});

test('structured parser rejects malformed contracts and preserves argument boundaries', async () => {
  for (const value of ['null', '[]', '"x"', '{}', '{invalid', JSON.stringify({ ...effect(), adapter: 42 }),
    JSON.stringify({ ...effect(), mission_id: 'x\nreceipt_id=spoof' })])
    expect(() => parseEffect(Buffer.from(value))).toThrow();
  const input = { ...effect(), authority: 'scope\t"quoted" * ? $(false)' };
  queue('one.effect', input);
  expect(await tickEffects(root, { native: mockOwner(), settleMs: 0 })).toEqual({ committed: 1, failed: 0, held: 0 });
  const calls = readFileSync(join(root, 'calls.jsonl'), 'utf8').trim().split('\n').map(JSON.parse);
  const prepare = calls.find(args => args[1] === 'effect-prepare');
  expect(prepare[7]).toBe(input.target_uri);
  expect(prepare[prepare.indexOf('--input-artifact-uri') + 1]).toBe(input.input_artifact_uri);
  expect(prepare[prepare.indexOf('--authority') + 1]).toBe(input.authority);
});

test('invalid JSON and native preparation failure produce parseable error sidecars without committing', async () => {
  queue('bad.effect', '{"unclosed":"');
  queue('refused.effect', effect());
  expect(await tickEffects(root, { native: mockOwner('prepare-fail'), settleMs: 0 }))
    .toEqual({ committed: 0, failed: 2, held: 0 });
  const error = JSON.parse(readFileSync(join(root, 'effects/failed/refused.effect.error.json'), 'utf8'));
  expect(error.stage).toBe('effect_prepare');
  expect(error.diagnostic).toContain('quoted "failure"');
  const calls = readFileSync(join(root, 'calls.jsonl'), 'utf8').trim().split('\n').map(JSON.parse);
  expect(calls.some(args => args[1] === 'effect-commit')).toBe(false);
});

test('ambiguous commit stays in-flight and is never replayed on a later tick', async () => {
  queue('ambiguous.effect', effect());
  const native = mockOwner('ambiguous');
  expect(await tickEffects(root, { native, settleMs: 0 })).toEqual({ committed: 0, failed: 0, held: 1 });
  expect(existsSync(join(root, 'effects/inflight/ambiguous.effect.claim/input.effect'))).toBe(true);
  const reconcile = JSON.parse(readFileSync(join(root, 'effects/inflight/ambiguous.effect.claim/reconcile.json'), 'utf8'));
  expect(reconcile.stage).toBe('effect_commit');
  const before = readFileSync(join(root, 'calls.jsonl'), 'utf8');
  await tickEffects(root, { native, settleMs: 0 });
  expect(readFileSync(join(root, 'calls.jsonl'), 'utf8')).toBe(before);
  queue('ambiguous.effect', effect());
  expect((await tickEffects(root, { native, settleMs: 0 })).held).toBe(1);
  expect(readFileSync(join(root, 'calls.jsonl'), 'utf8')).toBe(before);
});

test('concurrent watchers claim a filename only once and ignore symlinked/empty inputs', async () => {
  queue('single.effect', effect()); queue('empty.effect', '');
  const outside = join(root, 'outside.json'); writeFileSync(outside, JSON.stringify(effect()));
  symlinkSync(outside, join(root, 'effects/pending/link.effect'));
  const native = mockOwner();
  const results = await Promise.all([tickEffects(root, { native, settleMs: 10 }), tickEffects(root, { native, settleMs: 10 })]);
  expect(results.reduce((sum, result) => sum + result.committed, 0)).toBe(1);
  const calls = readFileSync(join(root, 'calls.jsonl'), 'utf8').trim().split('\n').map(JSON.parse);
  expect(calls.filter(args => args[1] === 'effect-commit').length).toBe(1);
  expect(existsSync(join(root, 'effects/pending/link.effect'))).toBe(true);
  expect(existsSync(join(root, 'effects/pending/empty.effect'))).toBe(true);
});
