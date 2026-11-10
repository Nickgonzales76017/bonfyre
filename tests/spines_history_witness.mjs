#!/usr/bin/env bun
import { Database } from 'bun:sqlite';
import { createHash } from 'node:crypto';
import { existsSync, lstatSync, mkdirSync, mkdtempSync, readFileSync, realpathSync, rmSync, writeFileSync } from 'node:fs';
import { homedir, tmpdir } from 'node:os';
import { dirname, isAbsolute, join, relative, resolve, sep } from 'node:path';
import { createSpinesStore, readSpinesBytes } from '../integrations/mcp/spines_store.mjs';
import { createTemporalEngine } from '../integrations/mcp/spines_temporal.mjs';
import { createReasoningEngine } from '../integrations/mcp/spines_reasoning.mjs';
import { createModelEngine } from '../integrations/mcp/spines_model.mjs';

const PATH_COLUMNS = [
  ['receipts', 'path'], ['flux_events', 'segment_path'], ['deltas', 'native_manifest_path'],
  ['parity_runs', 'proof_path'], ['graph_partitions', 'path'], ['helix_artifacts', 'path'],
  ['reductions', 'witness_path'], ['crux_proofs', 'path'], ['phenotype_plans', 'path'],
  ['kernel_parity_runs', 'path'], ['placement_plans', 'path'], ['fuse_plans', 'path'],
];
const hash = bytes => createHash('sha256').update(bytes).digest('hex');
const defaultHome = join(homedir(), '.bonfyre/agent-hypervisor/native-spines-r7');
const homeArg = process.argv.slice(2).find(arg => !arg.startsWith('--'));
const sourceHome = realpathSync(resolve(homeArg || defaultHome));
const keep = process.argv.includes('--keep');
const sourceDbPath = join(sourceHome, 'spines.sqlite3');
if (!lstatSync(sourceDbPath).isFile()) throw new Error('installed spines database is not a regular file');
const temporary = realpathSync(mkdtempSync(join(tmpdir(), 'bonfyre-spines-history-')));
const copyHome = join(temporary, 'state');
mkdirSync(copyHome, { recursive: true, mode: 0o700 });
const copiedDb = join(copyHome, 'spines.sqlite3');
let copy, store, success = false;
const report = { schema: 'bonfyre.spines-history-witness.v1', measured_at: new Date().toISOString(), source_home: sourceHome, copied_home: copyHome,
  source_database_sha256: null, copied_database_sha256: null, copied_files: 0, copied_bytes: 0, missing_owned_files: [],
  rebased_columns: PATH_COLUMNS, snapshot_method: 'stable-main-and-WAL-byte-copy', source_snapshot_files: {}, defects: [] };
function confined(path) {
  if (typeof path !== 'string' || !isAbsolute(path)) throw new Error('non-absolute owned file reference');
  const full = resolve(path), rel = relative(sourceHome, full);
  if (!rel || rel === '..' || rel.startsWith(`..${sep}`) || isAbsolute(rel)) throw new Error(`referenced file outside installed home: ${path}`);
  let current = sourceHome;
  for (const part of rel.split(sep)) {
    current = join(current, part);
    if (existsSync(current) && lstatSync(current).isSymbolicLink()) throw new Error(`symlink in installed owned file path: ${path}`);
  }
  return { source: full, destination: join(copyHome, rel) };
}
try {
  // The installed WAL-mode directory is read-only to this process, so SQLite
  // cannot open it directly (it attempts a -shm sidecar). Copy the main file
  // and any WAL sidecars, then prove their bytes stayed identical through the
  // copy. Refuse a moving source instead of claiming a consistent snapshot.
  const names = ['spines.sqlite3', 'spines.sqlite3-wal', 'spines.sqlite3-shm'];
  const snapshot = new Map();
  for (const name of names) {
    const path = join(sourceHome, name);
    if (!existsSync(path)) continue;
    const bytes = readSpinesBytes(path, 512 * 1024 * 1024);
    snapshot.set(name, hash(bytes));
    report.source_snapshot_files[name] = snapshot.get(name);
    writeFileSync(join(copyHome, name), bytes, { flag: 'wx', mode: 0o600 });
  }
  if (!snapshot.has('spines.sqlite3')) throw new Error('source database disappeared');
  for (const name of names) {
    const path = join(sourceHome, name), expected = snapshot.get(name);
    if (expected === undefined ? existsSync(path) : !existsSync(path) || hash(readSpinesBytes(path, 512 * 1024 * 1024)) !== expected)
      throw new Error(`source database changed during snapshot: ${name}`);
  }
  report.source_database_sha256 = snapshot.get('spines.sqlite3');
  copy = new Database(copiedDb, { strict: true });
  if (copy.query('PRAGMA integrity_check').get().integrity_check !== 'ok') throw new Error('copied SQLite integrity check failed');
  const originals = {};
  for (const [table, column] of PATH_COLUMNS) {
    const values = copy.query(`SELECT rowid AS row_id, ${column} AS path FROM ${table}`).all();
    if (values.length > 100000) throw new Error(`historical ${table} row budget exceeded`);
    originals[`${table}.${column}`] = values;
  }
  const copied = new Map();
  for (const [table, column] of PATH_COLUMNS) for (const row of originals[`${table}.${column}`]) {
    if (row.path === null) continue;
    const { source: path, destination } = confined(row.path);
    if (copied.has(path)) continue;
    if (!existsSync(path)) { report.missing_owned_files.push({ table, column, row_id: row.row_id }); continue; }
    const bytes = readSpinesBytes(path);
    mkdirSync(dirname(destination), { recursive: true, mode: 0o700 });
    writeFileSync(destination, bytes, { flag: 'wx', mode: 0o600 });
    if (hash(readSpinesBytes(destination)) !== hash(bytes)) throw new Error('copied file digest mismatch');
    copied.set(path, destination); report.copied_files++; report.copied_bytes += bytes.length;
  }
  copy.transaction(() => {
    for (const [table, column] of PATH_COLUMNS) {
      const update = copy.query(`UPDATE ${table} SET ${column}=? WHERE rowid=?`);
      for (const row of originals[`${table}.${column}`]) {
        if (row.path === null) continue;
        const { destination } = confined(row.path);
        update.run(destination, row.row_id);
      }
    }
  }).immediate();
  copy.query('PRAGMA wal_checkpoint(TRUNCATE)').get();
  report.copied_database_sha256 = hash(readFileSync(copiedDb));
  copy.close(); copy = undefined;
  const nativeHome = join(homedir(), '.local/bin');
  store = createSpinesStore({ home: copyHome, projectRoot: process.cwd(),
    deltaKernel: join(nativeHome, 'bonfyre-delta-kernel'), helixVm: join(nativeHome, 'bonfyre-helix-vm'),
    kernelProbe: join(nativeHome, 'bonfyre-kernel-probe'), env: { PATH: process.env.PATH || '' } });
  const temporal = createTemporalEngine(store), reasoning = createReasoningEngine(store), model = createModelEngine(store);
  report.baseline_counts = store.counts();
  const baselineReceipts = store.verifyReceipts(), baselineFlux = temporal.streamVerify();
  report.baseline_receipts = { verified: baselineReceipts.verified, receipts: baselineReceipts.receipts,
    head: baselineReceipts.head, failure_count: baselineReceipts.failures.length, failure_sample: baselineReceipts.failures.slice(0, 10) };
  report.baseline_flux = { verified: baselineFlux.verified, streams: baselineFlux.streams,
    failure_count: baselineFlux.failures.length, failure_sample: baselineFlux.failures.slice(0, 10) };
  report.rules = reasoning.rules().length;
  report.models = model.inventory().length;
  report.nodes = model.nodes().length;
  const parity = temporal.parity(), afterReceipts = store.verifyReceipts();
  report.parity = { equal: parity.equal, id: parity.id, incremental_digest: parity.incremental_digest,
    rebuild_digest: parity.rebuild_digest, events: parity.events, objects: parity.objects, receipt: parity.receipt };
  report.after_parity_receipts = { verified: afterReceipts.verified, receipts: afterReceipts.receipts,
    head: afterReceipts.head, failure_count: afterReceipts.failures.length, failure_sample: afterReceipts.failures.slice(0, 10) };
  if (report.baseline_receipts.receipts !== report.baseline_counts.receipts) report.defects.push('receipt verifier omitted historical rows');
  if (!report.baseline_receipts.verified) report.defects.push('historical receipt chain did not verify');
  if (!report.baseline_flux.verified) report.defects.push('historical flux chain or segments did not verify');
  if (!report.parity.equal) report.defects.push('historical incremental replay parity failed');
  if (!report.after_parity_receipts.verified) report.defects.push('copied receipt chain failed after parity proof');
  if (report.missing_owned_files.length) report.defects.push('owned historical file references missing');
  for (const name of names) {
    const path = join(sourceHome, name), expected = snapshot.get(name);
    if (expected === undefined ? existsSync(path) : !existsSync(path) || hash(readSpinesBytes(path, 512 * 1024 * 1024)) !== expected)
      report.defects.push(`source database changed during witness: ${name}`);
  }
  report.source_unchanged = !report.defects.some(value => value.startsWith('source database changed'));
  success = report.defects.length === 0;
} catch (error) {
  report.defects.push(String(error?.message || error));
} finally {
  store?.close(); copy?.close();
  report.copy_retained = !success || keep;
  if (success && !keep) rmSync(temporary, { recursive: true, force: true });
  process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
}
if (!success) process.exitCode = 1;
