import { afterEach, expect, test } from 'bun:test';
import { Database } from 'bun:sqlite';
import { createHash } from 'node:crypto';
import { existsSync, mkdtempSync, mkdirSync, readFileSync, realpathSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createV6Store } from '../v6_store.mjs';
import { createV6History } from '../v6_history.mjs';
import { createRecursiveStore } from '../recursive_store.mjs';
import { createSpinesStore } from '../spines_store.mjs';
import { createCanaryStore } from '../canary_store.mjs';
import { createHypervisorStore } from '../hypervisor_store.mjs';

const temp = [];
test('hypervisor federation reuses the read-only owner verifier and detects forged row authority', () => {
  const f = fixture(), source = createHypervisorStore({ home: join(f.base, 'hypervisor') });
  try {
    const receipt = source.writeReceipt({ state: 'passed', payload: { result: 'café', score: 0.0000001 } });
    const history = createV6History(f.store, { sourceRoots: { hypervisor: source.receiptRoot } });
    expect(history.federate()).toMatchObject({ total_receipts: 1, failures: [] });
    const db = new Database(source.dbPath);
    try { db.query('UPDATE receipts SET provider_id=? WHERE id=?').run('forged', receipt.receipt_id); }
    finally { db.close(); }
    const changed = history.federate();
    expect(changed.total_receipts).toBe(0); expect(changed.failures[0].source).toBe('hypervisor');
  } finally { source.close(); f.store.close(); }
});
afterEach(() => { for (const path of temp.splice(0)) rmSync(path, { recursive: true, force: true }); });
function fixture() {
  const base = mkdtempSync(join(tmpdir(), 'bonfyre-v6-history-test-')); temp.push(base);
  const root = join(base, 'project'), home = join(base, 'v6'); mkdirSync(root);
  const store = createV6Store({ root, home, now: () => 1791000000 });
  return { base, root, home, store };
}

test('history construction is lazy and STOP refuses raw SQL and bundle publication', () => {
  const f = fixture(); let blocked = false;
  const history = createV6History(f.store, { sourceRoots: {}, roots: {}, checkStop: () => { if (blocked) throw new Error('STOP test'); } });
  expect(existsSync(f.store.dbPath)).toBe(false);
  blocked = true;
  expect(() => history.federate()).toThrow('STOP');
  expect(() => history.sparseSnapshot()).toThrow('STOP');
  expect(existsSync(f.store.dbPath)).toBe(false);
  expect(existsSync(join(f.store.home, 'sparse'))).toBe(false);
  f.store.close();
});

test('bundle destination symlink refuses publication outside owner', () => {
  const f = fixture(); void f.store.db;
  const outside = join(f.base, 'outside'); mkdirSync(outside);
  symlinkSync(outside, join(f.store.home, 'sparse/bundles'));
  expect(() => createV6History(f.store, { roots: {} }).sparseSnapshot()).toThrow('symlink');
  expect(existsSync(join(f.store.home, 'sparse/snapshot.json'))).toBe(false);
  f.store.close();
});

test('federation imports only owner-verified recursive receipts and rejects tampering', () => {
  const f = fixture(), sourceHome = join(f.base, 'recursive'), source = createRecursiveStore({ home: sourceHome, tokenBinary: '/nonexistent/token' });
  const receipt = source.writeReceipt({ kind: 'fixture', subject_kind: 'test', subject_id: 'one', state: 'passed', payload: { ok: true } });
  source.close();
  const roots = { 'recursive-r1': join(sourceHome, 'receipts') };
  const history = createV6History(f.store, { sourceRoots: roots });
  const first = history.federate();
  expect(first.failures).toEqual([]);
  expect(first.total_receipts).toBe(1);
  expect(first.sources['recursive-r1'].verified).toBe(true);
  expect(f.store.db.query('SELECT digest FROM federated_receipts').get().digest).toBe(receipt.digest);
  const doc = JSON.parse(readFileSync(receipt.path, 'utf8'));
  doc.payload.ok = false;
  writeFileSync(receipt.path, JSON.stringify(doc));
  const second = history.federate();
  expect(second.total_receipts).toBe(0);
  expect(second.failures[0].source).toBe('recursive-r1');
  expect(f.store.db.query('SELECT count(*) AS n FROM federated_receipts').get().n).toBe(0);
  f.store.close();
});

test('missing federation roots are explicit and do not attest a chain', () => {
  const f = fixture();
  const doc = createV6History(f.store, { sourceRoots: { 'recursive-r1': join(f.base, 'absent', 'receipts') } }).federate();
  expect(doc.total_receipts).toBe(0);
  expect(doc.sources['recursive-r1']).toMatchObject({ present: false, verified: false, receipts: 0 });
  expect(doc.failures).toEqual([]);
  f.store.close();
});

test('spines and canary federation reuse their independent Bun chain verifiers', () => {
  const f = fixture(), spinesHome = join(f.base, 'spines'), canaryHome = join(f.base, 'canary');
  const spines = createSpinesStore({ home: spinesHome, projectRoot: f.root,
    deltaKernel: '/nonexistent/delta', helixVm: '/nonexistent/helix', kernelProbe: '/nonexistent/probe' });
  spines.writeReceipt({ release: 'r7', kind: 'fixture', state: 'passed', payload: { ok: true } });
  spines.close();
  const canary = createCanaryStore({ home: canaryHome });
  canary.writeReceipt({ kind: 'fixture', state: 'passed', payload: { ok: true } });
  canary.close();
  const doc = createV6History(f.store, { sourceRoots: {
    'native-spines-r7': join(spinesHome, 'receipts'), 'fixture-canary-r4': join(canaryHome, 'receipts'),
  } }).federate();
  expect(doc.failures).toEqual([]);
  expect(doc.total_receipts).toBe(2);
  expect(doc.sources['native-spines-r7'].verified).toBe(true);
  expect(doc.sources['fixture-canary-r4'].verified).toBe(true);
  f.store.close();
});

test('sparse snapshot hashes packed bytes, skips symlinks, and stages a consistent SQLite DB', () => {
  const f = fixture(), cmd = join(f.root, 'cmd'), databaseDir = join(f.root, 'data');
  mkdirSync(cmd); mkdirSync(databaseDir);
  writeFileSync(join(cmd, 'hello.mjs'), 'export const hello = 1;\n');
  writeFileSync(join(f.base, 'outside.txt'), 'outside');
  symlinkSync(join(f.base, 'outside.txt'), join(cmd, 'link.txt'));
  const sqlitePath = join(databaseDir, 'fixture.sqlite3'), db = new Database(sqlitePath);
  db.exec('PRAGMA journal_mode=WAL; CREATE TABLE data(value TEXT); INSERT INTO data VALUES (\'first\');');
  const roots = { 'command-plane': [cmd], applications: [], databases: [databaseDir], models: [], media: [], integrations: [] };
  const snap = createV6History(f.store, { roots }).sparseSnapshot({ includeAmbient: false, maxHashBytes: 60 * 1024 * 1024,
    maxFileBytes: 10 * 1024 * 1024, bundleBytes: 60 * 1024 * 1024 });
  expect(snap.skipped).toEqual([]);
  expect(snap.file_count).toBe(2);
  expect(snap.files.find(item => item.path === realpathSync(sqlitePath)).snapshot_mode).toBe('sqlite-consistent-vacuum');
  expect(snap.files.some(item => item.path.endsWith('link.txt'))).toBe(false);
  expect(snap.bundle_source_bytes).toBe(snap.total_bytes);
  for (const bundle of snap.bundles) expect(createHash('sha256').update(readFileSync(bundle.path)).digest('hex')).toBe(bundle.sha256);
  expect(f.store.db.query('SELECT count(*) AS n FROM sparse_snapshots').get().n).toBe(1);
  expect(f.store.verifyReceipts().verified).toBe(true);
  db.close(); f.store.close();
});

test('sparse traversal refuses an exhausted entry budget without publishing a false snapshot', () => {
  const f = fixture(), cmd = join(f.root, 'cmd'); mkdirSync(cmd);
  writeFileSync(join(cmd, 'a'), 'a'); writeFileSync(join(cmd, 'b'), 'b');
  const roots = { 'command-plane': [cmd], applications: [], databases: [], models: [], media: [], integrations: [] };
  expect(() => createV6History(f.store, { roots, limits: { maxCandidates: 1 } }).sparseSnapshot({ includeAmbient: false })).toThrow('budget');
  expect(existsSync(join(f.home, 'sparse', 'snapshot.json'))).toBe(false);
  expect(f.store.db.query('SELECT count(*) AS n FROM sparse_snapshots').get().n).toBe(0);
  f.store.close();
});
