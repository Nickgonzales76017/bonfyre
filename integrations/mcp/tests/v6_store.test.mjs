import { afterEach, expect, test } from 'bun:test';
import { existsSync, mkdtempSync, readFileSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { createV6Store, V6_TABLES } from '../v6_store.mjs';
const fixtures = [];
afterEach(() => { for (const item of fixtures.splice(0)) { item.store.close(); rmSync(item.home, { recursive: true, force: true }); } });
function fixture(options = {}) {
  const home = mkdtempSync(join(tmpdir(), 'bonfyre-v6-store-'));
  const store = createV6Store({ home, root: process.cwd(), now: () => 1000, ...options });
  fixtures.push({ home, store }); return store;
}
test('lazy construction and exact legacy schema without rewriting metadata on reopen', () => {
  const store = fixture();
  expect(existsSync(store.dbPath)).toBe(false);
  expect(store.db.query("SELECT name FROM sqlite_master WHERE type='table' AND name!='sqlite_sequence' ORDER BY name").all().map(row => row.name)).toEqual([...V6_TABLES].sort());
  store.setMeta('version', '6.1.0'); store.close();
  const reopened = createV6Store({ home: store.home, root: process.cwd(), now: () => 2000 });
  fixtures[0].store = reopened;
  expect(reopened.getMeta('version')).toBe('6.1.0');
  expect(reopened.db.query("SELECT updated_at FROM metadata WHERE key='version'").get().updated_at).toBe(1000);
});
test('V6 UTF-8 receipt identity binds floats, payload, row fields and previous head', () => {
  const store = fixture(), one = store.receipt('test', 'candidate', { text: '雪', x: 1e-7 });
  const two = store.receipt('test', 'failed', {}), raw = readFileSync(one.path, 'utf8');
  expect(raw).toContain('"created_at":1000.0'); expect(raw).toContain('雪');
  expect(store.parseDocument(raw).calculated).toBe(one.digest);
  expect(store.verifyReceipts()).toEqual({ verified: true, receipts: 2, head: two.digest, failures: [] });
  store.db.query('UPDATE receipts SET state=? WHERE id=?').run('active', one.receipt_id);
  expect(store.verifyReceipts().failures).toContain(one.receipt_id);
});
test('mission run and receipt commit together and failed insertion rolls back', () => {
  const store = fixture();
  const run = store.missionRun('test', 'failed', { actually_executed: false }, 999);
  expect(run.state).toBe('failed'); expect(store.verifyReceipts().verified).toBe(true);
  expect(store.db.query('SELECT count(*) AS n FROM mission_runs').get().n).toBe(1);
  store.db.exec("CREATE TRIGGER refuse_receipt BEFORE INSERT ON receipts BEGIN SELECT RAISE(ABORT,'refused'); END;");
  expect(() => store.missionRun('test', 'passed', {}, 999)).toThrow('refused');
  expect(store.db.query('SELECT count(*) AS n FROM mission_runs').get().n).toBe(1);
  expect(store.verifyReceipts().receipts).toBe(1);
});
test('receipt corruption, path traversal, symlinks and closed use are refused', () => {
  const store = fixture(), receipt = store.receipt('test', 'failed', {});
  writeFileSync(receipt.path, '{'); expect(store.verifyReceipts().verified).toBe(false);
  store.db.query('UPDATE receipts SET path=?').run('/etc/passwd'); expect(store.verifyReceipts().verified).toBe(false);
  expect(() => store.writeJson(join(store.home, '..', 'escape.json'), {})).toThrow('escapes');
  symlinkSync(tmpdir(), join(store.home, 'escape'));
  expect(() => store.writeJson(join(store.home, 'escape', 'x'), {})).toThrow('symlink');
  store.close(); expect(() => store.getMeta('version')).toThrow('closed');
});
