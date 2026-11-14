import { test, expect } from 'bun:test';
import { existsSync, mkdirSync, mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Database } from 'bun:sqlite';
import { createFabricEstate } from '../fabric_estate.mjs';

function fixture() {
  const home = mkdtempSync(join(tmpdir(), 'fabric-estate-test-'));
  const root = join(home, '.bonfyre/estate-compiler'), operator = join(home, '.bonfyre/operator');
  mkdirSync(root, { recursive: true }); mkdirSync(operator, { recursive: true });
  const worldDbPath = join(root, 'estate.sqlite3'), operatorDbPath = join(operator, 'operator.sqlite3');
  const world = new Database(worldDbPath);
  world.exec(`CREATE TABLE entities(id TEXT PRIMARY KEY);
    CREATE TABLE signals(id TEXT,state TEXT);
    CREATE TABLE locks(name TEXT,acquired_at REAL);
    CREATE TABLE missions(
      id TEXT PRIMARY KEY,dedupe_key TEXT NOT NULL UNIQUE,family TEXT NOT NULL,title TEXT NOT NULL,details TEXT NOT NULL,
      priority INTEGER NOT NULL,state TEXT NOT NULL,lock_set_json TEXT NOT NULL,dependency_ids_json TEXT NOT NULL,
      source_signal_ids_json TEXT NOT NULL,created_at REAL NOT NULL,updated_at REAL NOT NULL,
      operator_goal_id TEXT,last_error TEXT
    );`);
  world.query('INSERT INTO entities VALUES(?)').run('entity-one'); world.close();
  const goals = new Database(operatorDbPath);
  goals.exec('CREATE TABLE goals(id TEXT,title TEXT,priority INTEGER,state TEXT,source TEXT,updated_at REAL);'); goals.close();
  return { home, worldDbPath, operatorDbPath, close: () => rmSync(home, { recursive: true, force: true }) };
}

test('status reproduces installed estate and operator read model without Python', () => {
  const f = fixture();
  try {
    const status = createFabricEstate({ home: f.home }).status();
    expect(status.paused).toBe(false);
    expect(status.world.entities).toBe(1);
    expect(status.world.missions).toEqual([]);
    expect(status.operator.goals).toEqual([]);
    expect(status.operator.running).toEqual([]);
  } finally { f.close(); }
});

test('add writes compiled mission with Python defaults, locks and no operator-goal mutation', () => {
  const f = fixture();
  try {
    let ids = 0;
    const owner = createFabricEstate({ home: f.home, now: () => 1234.5,
      randomId: () => (++ids).toString(16).padStart(32, '0') });
    const result = owner.add({ title: 'First', locks: 'app,model' });
    expect(result).toEqual({ mission_id: '00000000000000000000000000000001', dedupe_key: 'manual:00000000000000000000000000000002' });
    const db = new Database(f.worldDbPath, { readonly: true });
    const row = db.query('SELECT * FROM missions').get(); db.close();
    expect(row).toMatchObject({ title: 'First', family: 'manual', details: '', priority: 90, state: 'compiled',
      lock_set_json: '["app", "model"]', dependency_ids_json: '[]', source_signal_ids_json: '[]', created_at: 1234.5 });
    expect(owner.status().operator.goals).toEqual([]);
  } finally { f.close(); }
});

test('explicit dedupe updates intended fields but retains actual mission identity, family and locks', () => {
  const f = fixture();
  try {
    let ids = 0, time = 100;
    const owner = createFabricEstate({ home: f.home, now: () => ++time,
      randomId: () => (++ids).toString(16).padStart(32, '0') });
    const first = owner.add({ title: 'First', family: 'original', priority: 80, locks: 'a,b', dedupe: 'shared' });
    const next = owner.add({ title: 'Revised', family: 'replacement', details: 'new detail', priority: 30, locks: 'c', dedupe: 'shared' });
    expect(next).toEqual(first);
    const db = new Database(f.worldDbPath, { readonly: true });
    const row = db.query('SELECT * FROM missions').get(); db.close();
    expect(row).toMatchObject({ id: first.mission_id, title: 'Revised', details: 'new detail', priority: 80,
      family: 'original', lock_set_json: '["a", "b"]', created_at: 101, updated_at: 102 });
  } finally { f.close(); }
});

test('dedupe preserves a progressed mission and refuses changed input instead of reopening it', () => {
  const f = fixture();
  try {
    let ids = 0;
    const owner = createFabricEstate({ home: f.home, now: () => 300,
      randomId: () => (++ids).toString(16).padStart(32, '0') });
    const input = { title: 'Reviewed work', family: 'review', locks: 'source', dedupe: 'review:one' };
    const first = owner.add(input);
    const db = new Database(f.worldDbPath);
    try {
      db.query("UPDATE missions SET state='completed',updated_at=350 WHERE id=?").run(first.mission_id);
      const before = db.query('SELECT * FROM missions WHERE id=?').get(first.mission_id);
      expect(owner.add(input)).toEqual(first);
      expect(() => owner.add({ ...input, title: 'Replay with changed payload' })).toThrow('already progressed');
      expect(() => owner.add({ ...input, locks: 'another' })).toThrow('already progressed');
      expect(db.query('SELECT * FROM missions WHERE id=?').get(first.mission_id)).toEqual(before);
    } finally { db.close(); }
  } finally { f.close(); }
});

test('invalid inputs and missing DB refuse without creating a replacement database', () => {
  const f = fixture();
  try {
    const owner = createFabricEstate({ home: f.home });
    for (const args of [{}, { title: '' }, { title: 'x', priority: 101 }, { title: 'x', family: '' },
      { title: 'x', locks: '\0' }]) expect(() => owner.add(args)).toThrow();
    const absent = join(f.home, 'missing.sqlite3');
    const missing = createFabricEstate({ home: f.home, worldDbPath: absent });
    expect(() => missing.add({ title: 'x' })).toThrow('missing');
    expect(() => missing.status()).toThrow('missing');
    expect(existsSync(absent)).toBe(false);
  } finally { f.close(); }
});
