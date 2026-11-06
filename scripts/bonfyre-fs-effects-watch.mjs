#!/usr/bin/env bun
// Atomic .effect publication -> native WorkGraph lifecycle. No Python and no
// shell interpolation. Authors must use temp+rename (bonfyre-fs-write-effect).
// Equal size/mtime observations cannot prove an in-place writer has finished.
import { randomUUID } from 'node:crypto';
import { appendFileSync, closeSync, constants, existsSync, fstatSync, fsyncSync,
  lstatSync, mkdirSync, openSync, readFileSync, readdirSync, realpathSync, renameSync,
  writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

const nativeOwner = fileURLToPath(new URL('../programs/bonfyre/bonfyre', import.meta.url));
const defaults = Object.freeze({ input_artifact_uri: '', verification_policy: 'sha256',
  rollback_contract: 'remove-created-target', authority: 'bonfyre-fs-effects-watch' });
const required = ['mission_id', 'adapter', 'target_uri'];
const same = (a, b) => ['dev', 'ino', 'size', 'mtimeNs', 'ctimeNs'].every(key => a[key] === b[key]);
const snapshot = path => {
  try { return lstatSync(path, { bigint: true }); }
  catch (error) { if (error.code === 'ENOENT') return null; throw error; }
};
function durableJSON(path, value) {
  const temp = `${path}.${randomUUID()}.tmp`, fd = openSync(temp, 'wx', 0o600);
  try { writeFileSync(fd, JSON.stringify(value) + '\n'); fsyncSync(fd); }
  finally { closeSync(fd); }
  renameSync(temp, path);
}
export function parseEffect(bytes) {
  const input = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes));
  if (!input || typeof input !== 'object' || Array.isArray(input)) throw new Error('effect must be a JSON object');
  const result = { ...defaults };
  for (const key of [...required, ...Object.keys(defaults)]) {
    if (Object.hasOwn(input, key)) result[key] = input[key];
    if (typeof result[key] !== 'string' || /[\0\r\n]/.test(result[key]) ||
        (key !== 'input_artifact_uri' && !result[key].trim()))
      throw new Error(`missing or invalid field: ${key}`);
  }
  return result;
}
function outputField(output, name) {
  const lines = output.split(/\r?\n/).filter(line => line.startsWith(`${name}=`));
  if (lines.length !== 1 || !lines[0].slice(name.length + 1)) throw new Error(`native result lacks one ${name}`);
  return lines[0].slice(name.length + 1);
}
function runOwner(native, args) {
  const result = Bun.spawnSync([native, ...args], { env: process.env, stdout: 'pipe', stderr: 'pipe' });
  const stdout = new TextDecoder().decode(result.stdout), stderr = new TextDecoder().decode(result.stderr);
  if (result.exitCode !== 0) {
    const error = new Error(`native owner exited ${result.exitCode}`);
    error.native_exit_code = result.exitCode; error.diagnostic = stdout + stderr;
    throw error;
  }
  return stdout;
}

export async function tickEffects(projection, { native = nativeOwner, settleMs = 200, signal } = {}) {
  if (!Number.isSafeInteger(settleMs) || settleMs < 0) throw new Error('invalid settle interval');
  const root = realpathSync(projection), effects = join(root, 'effects');
  const paths = Object.fromEntries(['pending', 'inflight', 'committed', 'failed'].map(name => [name, join(effects, name)]));
  for (const dir of Object.values(paths)) mkdirSync(dir, { recursive: true, mode: 0o700 });
  const log = event => appendFileSync(join(effects, 'watch.log'), `${new Date().toISOString()}\t${JSON.stringify(event)}\n`, { mode: 0o600 });
  const candidates = readdirSync(paths.pending).filter(name => name.endsWith('.effect')).sort()
    .map(name => ({ name, path: join(paths.pending, name), before: snapshot(join(paths.pending, name)) }))
    .filter(item => item.before?.isFile() && item.before.size > 0n);
  if (candidates.length) await Bun.sleep(settleMs);
  const counts = { committed: 0, failed: 0, held: 0 };
  for (const item of candidates) {
    signal?.throwIfAborted();
    const after = snapshot(item.path);
    if (!after || !same(item.before, after)) continue;
    // Exclusive between concurrent watchers. A surviving claim means work to
    // inspect/reconcile, never permission to replay an external effect.
    const claimDir = join(paths.inflight, `${item.name}.claim`);
    try { mkdirSync(claimDir, { mode: 0o700 }); }
    catch (error) { if (error.code === 'EEXIST') { counts.held++; continue; } throw error; }
    const claimed = join(claimDir, 'input.effect'), journalPath = join(claimDir, 'journal.json');
    let stage = 'claim_file', effect, journal;
    try {
      if (existsSync(join(paths.committed, item.name)) || existsSync(join(paths.failed, item.name)))
        throw new Error('queue filename already has a terminal record; publish with a new name');
      renameSync(item.path, claimed);
      const fd = openSync(claimed, constants.O_RDONLY | constants.O_NOFOLLOW);
      let bytes;
      try {
        const stat = fstatSync(fd, { bigint: true });
        // rename may update ctime; compare identity, size and mtime here.
        if (!stat.isFile() || ['dev', 'ino', 'size', 'mtimeNs'].some(key => stat[key] !== after[key]))
          throw new Error('effect changed while being claimed');
        bytes = readFileSync(fd);
        if (!same(stat, fstatSync(fd, { bigint: true }))) throw new Error('effect changed while being read');
      } finally { closeSync(fd); }
      stage = 'parse'; effect = parseEffect(bytes);
      const node = `n-${randomUUID()}`, worker = `fs-watch-${process.pid}`;
      journal = { schema: 'bonfyre.fs-effect-watch.v1', name: item.name, mission_id: effect.mission_id,
        node_id: node, effect_id: `${node}.effect`, worker_id: worker, stage: 'mission_create' };
      const invoke = (next, args) => {
        stage = next; journal.stage = next; durableJSON(journalPath, journal);
        return runOwner(native, args);
      };
      // Existing missions were accepted by the previous watcher. work-add
      // remains the native check that the mission is usable after create.
      try { invoke('mission_create', ['mission', 'create', effect.mission_id]); }
      catch (error) { if (error.native_exit_code === undefined) throw error; }
      invoke('work_add', ['work', 'add', effect.mission_id, node, 'fs.effect']);
      journal.claim_token = outputField(invoke('work_claim', ['work', 'claim', effect.mission_id, node, worker,
        '--lease-ms', '60000']), 'claim_token');
      const base = [effect.mission_id, node, worker, journal.claim_token, journal.effect_id];
      const prepare = ['work', 'effect-prepare', ...base, effect.target_uri, '--adapter', effect.adapter,
        '--verification-policy', effect.verification_policy, '--rollback-contract', effect.rollback_contract,
        '--authority', effect.authority];
      if (effect.input_artifact_uri) prepare.push('--input-artifact-uri', effect.input_artifact_uri);
      invoke('effect_prepare', prepare);
      const committed = invoke('effect_commit', ['work', 'effect-commit', ...base]);
      // Missing output after commit is ambiguous, not a safely retryable job.
      const receipt = { mission_id: effect.mission_id, node_id: node,
        receipt_id: outputField(committed, 'receipt_id'), event_id: outputField(committed, 'event_id') };
      durableJSON(join(paths.committed, `${item.name}.receipt.json`), receipt);
      renameSync(claimed, join(paths.committed, item.name));
      journal.stage = 'committed'; delete journal.claim_token; durableJSON(journalPath, journal);
      log({ state: 'committed', name: item.name, ...receipt }); counts.committed++;
    } catch (error) {
      const failure = { error: error.message, stage, native_exit_code: error.native_exit_code,
        diagnostic: error.diagnostic, mission_id: effect?.mission_id, node_id: journal?.node_id };
      if (stage === 'effect_commit') {
        durableJSON(join(claimDir, 'reconcile.json'), failure);
        log({ state: 'held_for_reconciliation', name: item.name, stage }); counts.held++;
      } else {
        durableJSON(join(claimDir, 'failure.json'), failure);
        if (existsSync(claimed)) {
          durableJSON(join(paths.failed, `${item.name}.error.json`), failure);
          renameSync(claimed, join(paths.failed, item.name));
        }
        log({ state: 'failed', name: item.name, stage }); counts.failed++;
      }
    }
  }
  return counts;
}

if (import.meta.main) {
  try {
    const [verb, projection, once, ...extra] = process.argv.slice(2);
    if (!['run', 'tick'].includes(verb) || !projection || (once && once !== '--once') || extra.length)
      throw new Error('usage: bonfyre-fs-effects-watch run|tick <projection-dir> [--once]');
    for (;;) {
      await tickEffects(projection);
      if (verb === 'tick' || once === '--once') break;
      await Bun.sleep(1000);
    }
  } catch (error) { console.error(`watch: ${error.message}`); process.exitCode = 2; }
}
