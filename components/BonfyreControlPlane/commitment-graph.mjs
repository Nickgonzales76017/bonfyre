import { createHash } from 'node:crypto';
import { existsSync, mkdtempSync, readFileSync, renameSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';

const repo = resolve(import.meta.dir, '../..');
const source = resolve(import.meta.dir, 'native/commitment_cli.c');
const core = resolve(repo, 'engine/core/src/commitment.c');
const header = resolve(repo, 'engine/core/include/bf_commitment.h');
const hash = createHash('sha256').update(readFileSync(source)).update(readFileSync(core))
  .update(readFileSync(header)).digest('hex').slice(0, 16);
const cli = process.env.BONFYRE_COMMITMENT_CLI || join(tmpdir(), `bonfyre-commitment-${hash}`);
if (!existsSync(cli)) {
  const built = `${cli}.${process.pid}`;
  const result = Bun.spawnSync(['cc', '-O2', '-Wall', '-Wextra', '-Werror', '-std=c11',
    '-Iengine/core/include', source, core, '-lsqlite3', '-o', built], { cwd: repo });
  if (result.exitCode !== 0) throw new Error(`native CommitmentGraph build failed: ${result.stderr}`);
  renameSync(built, cli);
}

export const OPEN = 'open', DISCHARGED = 'discharged', DELEGATED = 'delegated';
export const VIOLATED = 'violated', COMPENSATED = 'compensated';
export const TERMINAL = new Set([DISCHARGED, COMPENSATED]);
export class CommitmentError extends Error {}

export class CommitmentGraph {
  constructor(path = null) {
    this.tempDir = path ? null : mkdtempSync(join(tmpdir(), 'bonfyre-commitment-'));
    this.dbPath = path || join(this.tempDir, 'graph.db');
    // Open once to create schema even when no commitment has yet been recorded.
    this.status('__schema_probe__');
  }
  close() {
    if (this.tempDir) rmSync(this.tempDir, { recursive:true, force:true });
    this.tempDir = null;
  }
  call(operation, ...args) {
    const result = Bun.spawnSync([cli, this.dbPath, operation, ...args.map(value => String(value ?? ''))]);
    let payload;
    try { payload = JSON.parse(result.stdout.toString()); }
    catch { throw new CommitmentError(`native CommitmentGraph ${operation} failed: ${result.stderr}`); }
    if (result.exitCode === 0) return payload;
    const state = args.length && operation !== 'record' ? this.status(args[0]) : null;
    const reason = payload.error === 'transition'
      ? `${state}: ${TERMINAL.has(state) ? 'terminal' : 'not a legal move from'} ${state}`
      : payload.error === 'exists' ? `already exists in state ${this.status(args[0])}` : payload.error;
    throw new CommitmentError(`commitment ${args[0] || ''} ${reason}`);
  }
  record({ commitment_id, committer, beneficiary, resource, conditions = '',
    expires_at = '', authority = '', origin = '', enactment = '', at = '' }) {
    return this.call('record', commitment_id, committer, beneficiary, resource,
      conditions, expires_at, authority, origin, enactment, at);
  }
  status(id) { return this.call('status', id); }
  history(id) { return this.call('history', id); }
  wasEver(id, state) { return this.call('was-ever', id, state); }
  delivered(id) { return this.call('delivered', id); }
  discharge(id, { actor = '', detail = '', at = '' } = {}) { return this.call('discharge', id, actor, detail, at); }
  violate(id, { actor = '', detail = '', at = '' } = {}) { return this.call('violate', id, actor, detail, at); }
  compensate(id, { actor = '', detail = '', at = '' } = {}) { return this.call('compensate', id, actor, detail, at); }
  delegate(id, { to_committer, delegated_id, actor = '', at = '' }) {
    return this.call('delegate', id, to_committer, delegated_id, actor, at);
  }
  delegationChain(id) { return this.call('chain', id); }
  violable(now) { return this.call('violable', now); }
  outstanding(beneficiary) { return this.call('outstanding', beneficiary); }
}
