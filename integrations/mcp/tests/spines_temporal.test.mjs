// What this does not prove:
// - a dependency is replaced by an object built in this test, so the real one is not exercised.
import { afterEach, expect, test } from 'bun:test';
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { homedir, tmpdir } from 'node:os';
import { join } from 'node:path';
import { createSpinesStore } from '../spines_store.mjs';
import { createTemporalEngine } from '../spines_temporal.mjs';
import { parsedDocument } from '../canonical_json.mjs';

const native = join(homedir(), '.local/bin/bonfyre-delta-kernel');
const fixtures = [];
afterEach(() => { for (const { home, store } of fixtures.splice(0)) { store.close(); rmSync(home, { recursive: true, force: true }); } });
function fixture(options = {}) {
  const home = mkdtempSync(join(tmpdir(), 'bonfyre-temporal-'));
  let clock = 1000;
  const store = createSpinesStore({ home, projectRoot: process.cwd(), deltaKernel: native,
    helixVm: '/native/helix', kernelProbe: '/native/probe', now: () => ++clock,
    env: { PATH: process.env.PATH }, ...options });
  fixtures.push({ home, store });
  return { store, engine: createTemporalEngine(store) };
}
const mockKernel = async (argv, options) => {
  if (argv[1] !== 'delta') throw new Error('unexpected native command');
  const old = readFileSync(argv[argv.indexOf('--old') + 1]);
  const next = readFileSync(argv[argv.indexOf('--new') + 1]);
  const hash = value => createHash('sha256').update(value).digest('hex');
  writeFileSync(argv[argv.indexOf('--out') + 1], JSON.stringify({ schema: 'bonfyre.native-chunk-delta.v1',
    old_bytes: old.length, new_bytes: next.length, source_sha256: hash(old), target_sha256: hash(next), operations: [] }));
  return { argv, exit_code: 0, stdout: '', stderr: '', duration_ms: 1, timeout: false, options };
};

test('append persists legacy event/state/delta/closure/view/graph/receipt and verifies exact chain', async () => {
  const { store, engine } = fixture({ run: mockKernel });
  const first = await engine.append({ stream: 'missions', kind: 'mission.changed', subject_kind: 'mission', subject_id: 'm1',
    operation: 'upsert', payload: { state: 'ready', score: 1 },
    dependencies: [{ target_kind: 'artifact', target_id: 'a1', metadata: { reason: 'input' } }] });
  expect(first.event.previous_digest).toBeNull();
  expect(first.delta.logical_ops).toEqual([{ op: 'INSERT', path: 'score', new: 1 }, { op: 'INSERT', path: 'state', new: 'ready' }]);
  expect(first.delta.native_manifest.schema).toBe('bonfyre.native-chunk-delta.v1');
  expect(first.affected_closure.nodes).toEqual([{ kind: 'mission', id: 'm1', depth: 0 }, { kind: 'artifact', id: 'a1', depth: 1 }]);
  expect(first.views.views.ready_objects.state).toEqual([{ kind: 'mission', id: 'm1' }]);
  expect(first.graph_partition.nodes).toHaveLength(2);
  expect(store.db.query('SELECT event_count FROM flux_streams WHERE name=?').get('missions').event_count).toBe(1);
  expect(store.db.query('SELECT deleted FROM object_state WHERE subject_id=?').get('m1').deleted).toBe(0);
  expect(parsedDocument(readFileSync(first.event.segment_path, 'utf8')).calculated).toBe(first.event.digest);
  expect(engine.streamVerify()).toEqual({ verified: true, streams: { missions: first.event.digest }, failures: [] });
  expect(store.verifyReceipts().verified).toBe(true);
  expect(engine.parity().equal).toBe(true);
});

test('delete, invalidate and retract preserve old state and parity replay; tampering is detected', async () => {
  const { store, engine } = fixture({ run: mockKernel });
  const append = args => engine.append({ stream: 'events', kind: 'changed', subject_kind: 'artifact', subject_id: 'x', dependencies: [], ...args });
  await append({ operation: 'UPSERT', payload: { status: 'healthy' } });
  const invalid = await append({ operation: 'INVALIDATE', payload: { reason: 'stale' } });
  expect(invalid.delta.logical_ops).toContainEqual({ op: 'INSERT', path: '_invalid', new: true });
  await append({ operation: 'RETRACT', payload: { reason: 'withdrawn' } });
  const gone = await append({ operation: 'DELETE', payload: {} });
  expect(gone.event.previous_digest).toBeTruthy();
  expect(engine.parity().equal).toBe(true);
  expect(store.db.query('SELECT deleted FROM object_state WHERE subject_id=?').get('x').deleted).toBe(1);
  store.db.query('UPDATE flux_events SET digest=? WHERE id=?').run('corrupt', invalid.event.id);
  expect(engine.streamVerify().failures).toContainEqual({ stream: 'events', id: invalid.event.id, reason: 'digest' });
  expect(engine.streamVerify().failures.some(item => item.reason === 'chain')).toBe(true);
});

test('failed native kernel is explicit failure evidence; never claims a native manifest passed', async () => {
  const { engine } = fixture({ run: async () => ({ exit_code: 127, stderr: 'unavailable', spawn_error: true }) });
  const result = await engine.append({ stream: 's', kind: 'k', subject_kind: 'thing', subject_id: 't', operation: 'UPSERT', payload: { a: 1 } });
  expect(result.delta.native_manifest.state).toBe('failed');
  expect(result.delta.native_manifest.probe.exit_code).toBe(127);
  expect(existsSync(result.delta.native_manifest_path)).toBe(false);
  expect(result.receipt.state).toBe('failed');
});

test('bounded path and payload inputs refuse escape before any SQL or file mutation', async () => {
  const { store, engine } = fixture({ run: mockKernel });
  const base = { kind: 'k', subject_kind: 'thing', subject_id: 'id', operation: 'UPSERT', payload: {} };
  await expect(engine.append({ ...base, stream: '../escape' })).rejects.toThrow('stream path');
  await expect(engine.append({ ...base, stream: 'valid', dependencies: [{ target_kind: 'a' }] })).rejects.toThrow('target id');
  expect(store.db.query('SELECT count(*) AS n FROM flux_events').get().n).toBe(0);
});

test('stream verification catches a changed segment, missing stream owner and head/count drift', async () => {
  const { store, engine } = fixture({ run: mockKernel });
  const result = await engine.append({ stream: 's', kind: 'k', subject_kind: 'thing', subject_id: 't', operation: 'UPSERT', payload: {} });
  const original = readFileSync(result.event.segment_path, 'utf8');
  writeFileSync(result.event.segment_path, original.replace('"kind":"k"', '"kind":"z"'));
  expect(engine.streamVerify().failures).toContainEqual({ stream: 's', id: result.event.id, reason: 'segment_mismatch' });
  writeFileSync(result.event.segment_path, original);
  store.db.query('UPDATE flux_streams SET head_digest=?,event_count=? WHERE name=?').run('wrong', 9, 's');
  const drift = engine.streamVerify().failures;
  expect(drift).toContainEqual({ stream: 's', reason: 'head_mismatch' });
  expect(drift).toContainEqual({ stream: 's', reason: 'count_mismatch' });
  store.db.query('DELETE FROM flux_streams WHERE name=?').run('s');
  expect(engine.streamVerify().failures).toContainEqual({ stream: 's', reason: 'missing_stream' });
});

test('parity compares persisted standing views, refusing stale, missing and malformed view rows', async () => {
  const { store, engine } = fixture({ run: mockKernel });
  await engine.append({ stream: 's', kind: 'k', subject_kind: 'thing', subject_id: 't', operation: 'UPSERT', payload: { state: 'ready' } });
  expect(engine.parity().equal).toBe(true);
  store.db.query('UPDATE materialized_views SET state_json=? WHERE name=?').run('{}', 'ready_objects');
  expect(engine.parity().equal).toBe(false);
  store.db.query('UPDATE materialized_views SET state_json=? WHERE name=?').run('{broken', 'ready_objects');
  expect(engine.parity().view_failures).toContainEqual({ name: 'ready_objects', reason: 'invalid_state_json' });
  store.db.query('DELETE FROM materialized_views WHERE name=?').run('ready_objects');
  expect(engine.parity().view_failures).toContainEqual({ name: 'ready_objects', reason: 'missing_view' });
});

(existsSync(native) ? test : test.skip)('installed native delta binary produces a real chunk manifest in disposable storage', async () => {
  const { engine } = fixture();
  const result = await engine.append({ stream: 'native', kind: 'change', subject_kind: 'document', subject_id: 'd1',
    operation: 'UPSERT', payload: { body: 'native delta witness' } });
  expect(result.delta.native_manifest.schema).toBe('bonfyre.native-chunk-delta.v1');
  expect(result.delta.native_manifest.changed_chunks).toBeGreaterThan(0);
  expect(engine.parity().equal).toBe(true);
  expect(engine.streamVerify().verified).toBe(true);
});
