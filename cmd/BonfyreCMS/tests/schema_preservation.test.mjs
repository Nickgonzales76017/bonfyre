import { test } from 'bun:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { resolve, join } from 'node:path';
import { Database } from 'bun:sqlite';

const root = resolve(import.meta.dir, '../../..');
const binary = process.env.BONFYRE_CMS_BIN ?? join(root, 'cmd/BonfyreCMS/bonfyre-cms');
const retained = join(root, 'packs/wholesale-20k/schemas/quotation.json');

test('retained quotation schema migrates atomically, preserves rows and refuses overflow', () => {
  const folder = mkdtempSync(join(tmpdir(), 'cms-schema-preservation-'));
  const path = join(folder, 'cms.db');
  const full = JSON.parse(readFileSync(retained, 'utf8'));
  const keys = Object.keys(full.attributes);
  assert.equal(keys.length, 74, 'retained quotation projection changed; review the regression basis');
  const before = { ...full, attributes: Object.fromEntries(Object.entries(full.attributes).slice(0, 64)) };
  let sequence = 0;
  const apply = schema => {
    const file = join(folder, `${sequence++}.json`);
    writeFileSync(file, JSON.stringify(schema));
    const result = Bun.spawnSync([binary, 'schema', 'apply', file, '--db', path], { stdout: 'pipe', stderr: 'pipe' });
    return { code: result.exitCode, out: result.stdout.toString(), err: result.stderr.toString() };
  };
  const read = fn => { const db = new Database(path); try { return fn(db); } finally { db.close(); } };
  const columns = () => read(db => db.query(`PRAGMA table_info("${full.name}")`).all().map(row => row.name));
  try {
    assert.equal(apply(before).code, 0);
    const initial = columns();
    assert.ok(keys.slice(64).some(key => !initial.includes(key)));
    read(db => {
      db.query(`INSERT INTO "${full.name}"(namespace,created_at,updated_at,company) VALUES(?,?,?,?)`).run('regression-only', '2026-10-09', '2026-10-09', 'Aurekai');
      db.run("CREATE TRIGGER refuse_metadata BEFORE UPDATE ON _content_types BEGIN SELECT RAISE(ABORT,'forced metadata failure'); END");
    });
    const rejected = apply(full);
    assert.notEqual(rejected.code, 0, 'metadata failure must fail the operation');
    assert.match(rejected.err, /rolled back/);
    assert.deepEqual(columns(), initial, 'failed apply must undo added columns');
    assert.equal(read(db => JSON.parse(db.query('SELECT schema_json FROM _content_types WHERE name=?').get(full.name).schema_json).attributes.opportunity), undefined);
    read(db => db.run('DROP TRIGGER refuse_metadata'));
    assert.equal(apply(full).code, 0);
    assert.ok(keys.every(key => columns().includes(key)), 'all declared fields survive');
    assert.equal(read(db => db.query(`SELECT company FROM "${full.name}"`).get().company), 'Aurekai');
    const again = apply(full);
    assert.equal(again.code, 0);
    assert.doesNotMatch(again.err, /duplicate column|staged column/);
    const over = { name: 'oversized_schema', attributes: Object.fromEntries(Array.from({ length: 1025 }, (_, i) => [`field_${i}`, { type: 'string' }])) };
    const refused = apply(over);
    assert.notEqual(refused.code, 0);
    assert.match(refused.err, /exceeds the supported 1024 fields/);
    assert.equal(read(db => db.query("SELECT count(*) n FROM sqlite_master WHERE name='oversized_schema'").get().n), 0);
    assert.equal(read(db => db.query("SELECT count(*) n FROM _content_types WHERE name='oversized_schema'").get().n), 0);
    const escaped = { name: 'quoted_default', attributes: { note: { type: 'string', default: "Supplier's source" } } };
    assert.equal(apply(escaped).code, 0);
    read(db => db.run("INSERT INTO quoted_default(created_at,updated_at) VALUES('2026-10-09','2026-10-09')"));
    assert.equal(read(db => db.query('SELECT note FROM quoted_default').get().note), "Supplier's source");
    console.log(JSON.stringify({ owner: 'native CMS', retained_schema: full.name, fields_restored: keys.length - 64, rollback_verified: true, replay_verified: true, production_writes: 0 }));
  } finally { rmSync(folder, { recursive: true, force: true }); }
});
