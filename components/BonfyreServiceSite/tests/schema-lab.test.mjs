import { test, expect } from 'bun:test';
import { join } from 'node:path';
import { Database } from 'bun:sqlite';
import { inspect, stage, migration, precheck, trial, bunHandle, checksOf } from '../schema-lab.mjs';
import { sampleDatabase } from '../schema-lab-material.mjs';

const site = join(import.meta.dir, '../../../packs/aurekai-work-samples/site');
const bytes = sampleDatabase(join(site, 'synthesis-four-day-week-20261019.json'), join(site, 'nyc311-phone-20261006.json'));
const open = () => { const db = Database.deserialize(bytes); db.exec('PRAGMA foreign_keys = ON'); return bunHandle(db); };

test('the sample is real retained records with working references', () => {
  const m = inspect(open()), by = Object.fromEntries(m.tables.map(t => [t.name, t]));
  expect(by.works.rows).toBe(87); expect(by.requests.rows).toBe(2225); expect(by.indexes.rows).toBe(2);
  expect(by.authorship.fks.map(f => f.table).sort()).toEqual(['people', 'works']);
  expect(by.people.indexes.some(i => i.origin === 'u')).toBe(true);
  expect(open().q('PRAGMA foreign_key_check')).toEqual([]);
});

test('retyping years keeps every row and turns text into numbers; a precheck names values that would change', () => {
  const db = open(), before = inspect(db);
  db.run("INSERT INTO works (id, title, year) VALUES ('x:1', 'Odd year', 'c. 1999')");
  const b2 = inspect(db), changes = [{ kind: 'changeType', table: 'works', column: 'year', type: 'INTEGER' }];
  const issues = precheck(db, b2, changes);
  expect(issues[0].rows).toBe(1); expect(issues[0].words).toContain('c. 1999');
  const after = stage(b2, changes), r = trial(db, b2, after, migration(b2, after));
  expect(r.ok).toBe(true); expect(r.lost).toEqual([]); expect(r.brokenCount).toBe(0);
  expect(issues[0].words).toContain('becoming 0');
  expect(db.q("SELECT DISTINCT typeof(year) t FROM works WHERE year IS NOT NULL")).toEqual([{ t: 'integer' }]);
  expect(db.q("SELECT year FROM works WHERE id = 'x:1'")[0].year).toBe(0);
  expect(db.q('SELECT COUNT(*) n FROM works')[0].n).toBe(before.tables.find(t => t.name === 'works').rows + 1);
});

test('renaming a referred-to table and column rebuilds the tables that refer to it, and references still hold', () => {
  const db = open(), b = inspect(db);
  const changes = [{ kind: 'renameTable', table: 'people', to: 'authors' }, { kind: 'renameColumn', table: 'authors', column: 'id', to: 'author_id' }];
  const after = stage(b, changes), sql = migration(b, after);
  expect(sql.join('\n')).toContain('CREATE TABLE "authorship__new"');
  const r = trial(db, b, after, sql);
  expect(r.ok).toBe(true); expect(r.brokenCount).toBe(0); expect(r.lost).toEqual([]);
  expect(r.model.tables.find(t => t.name === 'authorship').fks.find(f => f.table === 'authors').to).toEqual(['author_id']);
  expect(r.model.tables.find(t => t.name === 'authors').indexes.some(i => i.origin === 'u')).toBe(true);
});

test('a reference to rows that are not there is found before running, and the check after lists them', () => {
  const db = open(); db.run("CREATE TABLE notes (id INTEGER PRIMARY KEY, work_id TEXT, body TEXT); INSERT INTO notes (work_id, body) VALUES ('oa:W3190201265', 'ok'), ('oa:nowhere', 'orphan');");
  const b = inspect(db), changes = [{ kind: 'addForeignKey', table: 'notes', column: 'work_id', refTable: 'works', refColumn: 'id' }];
  expect(precheck(db, b, changes)).toMatchObject([{ rows: 1, blocking: true, examples: ['oa:nowhere'] }]);
  const after = stage(b, changes), r = trial(db, b, after, migration(b, after));
  expect(r.brokenCount).toBe(1); expect(r.broken[0].table).toBe('notes');
});

test('requiring a column with empty values needs a fill, and the fill is applied', () => {
  const db = open(), b = inspect(db);
  const nulls = db.q('SELECT COUNT(*) n FROM works WHERE venue IS NULL')[0].n;
  expect(precheck(db, b, [{ kind: 'setRequired', table: 'works', column: 'venue', required: true }])[0]).toMatchObject({ rows: nulls, blocking: true });
  const changes = [{ kind: 'setRequired', table: 'works', column: 'venue', required: true, fill: 'Unknown venue' }], after = stage(b, changes);
  const r = trial(db, b, after, migration(b, after));
  expect(r.ok).toBe(true); expect(db.q("SELECT COUNT(*) n FROM works WHERE venue = 'Unknown venue'")[0].n).toBe(nulls);
  expect(r.model.tables.find(t => t.name === 'works').columns.find(c => c.name === 'venue').notnull).toBe(true);
});

test('a unique index over duplicates is refused before and fails on the copy, leaving it unchanged', () => {
  const db = open(), b = inspect(db), changes = [{ kind: 'addIndex', table: 'requests', columns: ['day'], unique: true }];
  expect(precheck(db, b, changes)[0].blocking).toBe(true);
  const after = stage(b, changes), r = trial(db, b, after, migration(b, after));
  expect(r.ok).toBe(false); expect(r.step).toContain('CREATE UNIQUE INDEX');
  expect(db.q('SELECT COUNT(*) n FROM requests')[0].n).toBe(2225);
});

test('checks written into a table survive a rebuild; impossible changes are explained', () => {
  expect(checksOf('CREATE TABLE t (a INT CHECK (a > (0)), b TEXT, CHECK (length(b) < 9))')).toEqual(['CHECK (a > (0))', 'CHECK (length(b) < 9)']);
  const db = open(); db.run('CREATE TABLE scored (id INTEGER PRIMARY KEY, score INT CHECK (score BETWEEN 0 AND 10), label TEXT)');
  const b = inspect(db), after = stage(b, [{ kind: 'dropColumn', table: 'scored', column: 'label' }]);
  trial(db, b, after, migration(b, after));
  expect(() => db.run('INSERT INTO scored (score) VALUES (11)')).toThrow(/CHECK/);
  expect(() => stage(b, [{ kind: 'dropColumn', table: 'people', column: 'id' }])).toThrow(/primary key/);
  expect(() => stage(b, [{ kind: 'dropTable', table: 'works' }])).toThrow(/refer to works/);
  expect(() => stage(b, [{ kind: 'addColumn', table: 'works', name: 'lang', notnull: true }])).toThrow(/needs a value/);
  expect(() => stage(b, [{ kind: 'addForeignKey', table: 'works', column: 'id', refTable: 'authorship', refColumn: 'work_id' }])).toThrow(/key or unique/);
});
