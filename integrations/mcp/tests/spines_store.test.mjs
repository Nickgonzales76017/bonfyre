import { afterEach, expect, test } from 'bun:test';
import { mkdtempSync, readFileSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { createSpinesStore, SPINES_TABLES } from '../spines_store.mjs';
import { parsedDocument } from '../canonical_json.mjs';

const fixtures = [];
afterEach(() => { for (const { home, store } of fixtures.splice(0)) { store.close(); rmSync(home, { recursive: true, force: true }); } });
function fixture(options = {}) {
  const home = mkdtempSync(join(tmpdir(), 'bonfyre-spines-store-'));
  const store = createSpinesStore({ home, projectRoot: process.cwd(), deltaKernel: '/native/delta', helixVm: '/native/helix', kernelProbe: '/native/probe',
    now: () => 1000, run: async argv => ({ argv, exit_code: 0, stdout: '{}', stderr: '', duration_ms: 0 }), ...options });
  fixtures.push({ home, store }); return store;
}
test('exact legacy schema and defaults bootstrap only on a new database', () => {
  let store = fixture(); const home = store.home;
  expect(store.db.query("SELECT name FROM sqlite_master WHERE type='table' AND name != 'sqlite_sequence' ORDER BY name").all().map(row => row.name)).toEqual([...SPINES_TABLES].sort());
  expect(store.db.query('SELECT count(*) AS n FROM rewrite_rules').get().n).toBe(8);
  expect(store.db.query('SELECT count(*) AS n FROM placement_nodes').get().n).toBe(3);
  store.db.query("DELETE FROM rewrite_rules WHERE name='add-zero-r'").run();
  store.close();
  store = createSpinesStore({ home, projectRoot: process.cwd(), deltaKernel: '/native/delta', helixVm: '/native/helix', kernelProbe: '/native/probe', now: () => 9999 });
  fixtures[0].store = store;
  expect(store.db.query('SELECT count(*) AS n FROM rewrite_rules').get().n).toBe(7);
  expect(store.db.query("SELECT updated_at FROM metadata WHERE key='version'").get().updated_at).toBe(1000);
});
test('receipt chain preserves Python float timestamps, unicode escapes and row identity', () => {
  const store = fixture(), one = store.writeReceipt({ release: 'R5', kind: 'append', state: 'passed', payload: { text: '雪', value: 1e-7 } });
  const two = store.writeReceipt({ release: 'R6', kind: 'reduce', state: 'candidate', payload: { proof: false } });
  const raw = readFileSync(one.path, 'utf8');
  expect(raw).toContain('"created_at":1000.0'); expect(raw).toContain('\\u96ea');
  expect(parsedDocument(raw).calculated).toBe(one.digest);
  expect(store.verifyReceipts()).toEqual({ verified: true, receipts: 2, head: two.digest, failures: [] });
  store.db.query('UPDATE receipts SET state=? WHERE id=?').run('authoritative', one.receipt_id);
  expect(store.verifyReceipts().failures).toContainEqual({ id: one.receipt_id, reason: 'row_document_mismatch' });
});
test('per-stream ordered verification uses an indexed search, not repeated estate scans', () => {
  const store = fixture();
  const plan = store.db.query('EXPLAIN QUERY PLAN SELECT * FROM flux_events WHERE stream=? ORDER BY seq').all('mission');
  expect(plan.some(row => row.detail.includes('SEARCH flux_events USING INDEX flux_events_stream_seq'))).toBe(true);
  expect(plan.some(row => row.detail.includes('TEMP B-TREE') || row.detail.startsWith('SCAN'))).toBe(false);
});
test('damaged, missing and path-escaping receipts never verify', () => {
  const store = fixture();
  const receipt = store.writeReceipt({ release: 'R7', kind: 'kernel', state: 'passed', payload: {} });
  writeFileSync(receipt.path, '{broken'); expect(store.verifyReceipts().verified).toBe(false);
  store.db.query('UPDATE receipts SET path=?').run('/etc/passwd'); expect(store.verifyReceipts().verified).toBe(false);
});
test('owned IO refuses traversal and symlinks before writing', () => {
  const store = fixture(); void store.db;
  expect(() => store.writeJson(join(store.home, '..', 'escape.json'), {})).toThrow('escapes');
  symlinkSync(tmpdir(), join(store.home, 'outside'));
  expect(() => store.writeJson(join(store.home, 'outside', 'escape.json'), {})).toThrow('symlink');
  symlinkSync('/nonexistent/dangling', join(store.home, 'dangling'));
  expect(() => store.writeJson(join(store.home, 'dangling'), {})).toThrow('symlink');
});
test('status probes actual configured native paths concurrently and does not claim adapter execution', async () => {
  let active = 0, maximum = 0; const seen = [];
  const store = fixture({ env: { PATH: '' }, run: async (argv, options) => {
    seen.push({ argv, options }); maximum = Math.max(maximum, ++active); await Bun.sleep(5); active--;
    return { argv, exit_code: 0, stdout: '{"status":"ok"}', stderr: '', duration_ms: 5 };
  } });
  const status = await store.status();
  expect(maximum).toBe(3); expect(seen).toHaveLength(3); expect(seen.every(item => item.options.timeoutMs === 10000)).toBe(true);
  expect(status.authority.automatic_external_effects).toBe(false);
  expect(status.external_backends.egglog.mode).toBe('contract-only');
  expect(status.receipt_chain.receipts).toBe(0);
});
test('closed store refuses later use and process wrapper preserves failed outcomes', async () => {
  const store = fixture({ run: async argv => ({ argv, exit_code: 124, timeout: true, stdout: '', stderr: '', duration_ms: 1 }) });
  expect((await store.run(['/native/probe'])).timed_out).toBe(true);
  store.close(); expect(() => store.counts()).toThrow('closed');
});
