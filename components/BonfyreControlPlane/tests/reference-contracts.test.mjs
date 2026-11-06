import { test, expect } from 'bun:test';
import { Database } from 'bun:sqlite';
import { readFileSync, existsSync, mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';

const repo = resolve(import.meta.dir, '../../..');
const checker = resolve(repo, 'scripts/reference-check.mjs');
const live = test.skipIf(!process.env.BONFYRE_LIVE_WITNESS);

function fixture(columns = false) {
  const dir = mkdtempSync(join(tmpdir(), 'bonfyre-reference-'));
  const path = join(dir, 'fixture.db');
  const db = new Database(path, { create: true });
  if (columns) db.exec(`
    CREATE TABLE reference_contract (reference TEXT, holder_type TEXT,
      holder_key TEXT, holder_field TEXT, target_type TEXT, target_key TEXT,
      cardinality TEXT, separator TEXT, required_ref TEXT, on_dangling TEXT,
      means TEXT, cannot_infer TEXT, status TEXT, target_kind TEXT,
      target_type_field TEXT, skip_when_empty TEXT);
    CREATE TABLE pat (pattern TEXT, reads TEXT, fields TEXT, organ TEXT);
    CREATE TABLE thing (name TEXT, colour TEXT);
    INSERT INTO reference_contract VALUES
      ('serves-columns','pat','pattern','fields','','','many',',','yes',
       'the surface serves nothing','which columns','not worth publishing',
       'published','column','reads','organ'),
      ('reads-a-table','pat','pattern','reads','','','one','','yes',
       'answers nothing','which type','not published','published','table','','organ');
  `);
  else db.exec(`
    CREATE TABLE reference_contract (reference TEXT, holder_type TEXT,
      holder_key TEXT, holder_field TEXT, target_type TEXT, target_key TEXT,
      cardinality TEXT, separator TEXT, required_ref TEXT, on_dangling TEXT,
      means TEXT, cannot_infer TEXT, status TEXT);
    CREATE TABLE thing (name TEXT, uses TEXT);
    CREATE TABLE part (part TEXT);
    INSERT INTO reference_contract VALUES
      ('thing-uses-parts','thing','name','uses','part','part','many',',',
       'yes','it renders nothing','which parts','a part is not a fact','published');
    INSERT INTO part VALUES ('a'),('b');
  `);
  return { db, path, close() { db.close(); rmSync(dir, { recursive: true, force: true }); } };
}
function run(path) {
  const result = Bun.spawnSync(['bun', checker, '--db', path], { cwd: repo });
  return { code: result.exitCode, stdout: result.stdout.toString(), stderr: result.stderr.toString() };
}
function withFixture(columns, callback) {
  const state = fixture(columns);
  try { callback(state); } finally { state.close(); }
}
async function surface(name) {
  const response = await fetch(`https://aurekai.live/_surface/${name}`, {
    headers: { Accept: 'application/json' }, signal: AbortSignal.timeout(30_000),
  });
  expect(response.status).toBe(200);
  return (await response.json()).records;
}

test('checker exists and is runnable', () => expect(existsSync(checker)).toBe(true));
test('resolving reference passes', () => withFixture(false, ({ db, path }) => {
  db.exec("INSERT INTO thing VALUES ('t','a,b')");
  const r = run(path);
  expect(r.code).toBe(0);
  expect(r.stdout).toContain('0 dangling');
}));
test('dangling reference fails loudly', () => withFixture(false, ({ db, path }) => {
  db.exec("INSERT INTO thing VALUES ('t','a,ghost')");
  const r = run(path);
  expect(r.code).toBe(1);
  for (const text of ['DANGLING', 'ghost', 't ->']) expect(r.stdout).toContain(text);
}));
test('required empty reference is dangling', () => withFixture(false, ({ db, path }) => {
  db.exec("INSERT INTO thing VALUES ('t','')");
  const r = run(path);
  expect(r.code).toBe(1);
  expect(r.stdout).toContain('required and empty');
}));
test('no declaration does not look like clean resolution', () => withFixture(false, ({ db, path }) => {
  db.exec('DROP TABLE reference_contract');
  const r = run(path);
  expect(r.code).toBe(1);
  expect(r.stdout).toContain('no reference contracts are declared');
  expect(r.stdout).not.toContain('0 dangling');
}));
test('missing target table is undeclarable', () => withFixture(false, ({ db, path }) => {
  db.exec("INSERT INTO reference_contract VALUES ('bad','thing','name','uses','gone','x','many',',','no','','','','published')");
  const r = run(path);
  expect(r.code).toBe(1);
  expect(r.stdout).toContain('UNDECLARABLE');
}));
test('declaration rejects nonidentifier without executing it', () => withFixture(false, ({ db, path }) => {
  db.prepare('INSERT INTO reference_contract VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)').run(
    'inj', 'thing', 'name', 'uses', 'part";DROP TABLE part--', 'part', 'many', ',', 'no', '', '', '', 'published');
  const r = run(path);
  expect(r.code).toBe(1);
  expect(r.stdout).toContain('UNDECLARABLE');
  expect(db.query('SELECT count(*) AS n FROM part').get().n).toBe(2);
}));
test('checker names no type of its own', () => {
  const source = readFileSync(checker, 'utf8');
  const body = source.slice(source.indexOf('export function check('));
  for (const name of ['world', 'internet_source', 'surface_renderer', 'presentation_template', 'territory_presentation']) {
    expect(body).not.toContain(`"${name}"`);
    expect(body).not.toContain(`'${name}'`);
  }
});

live('every declared live reference names a real field', async () => {
  const contracts = await surface('reference-contracts');
  expect(contracts.length).toBeGreaterThan(0);
  for (const c of contracts) {
    expect(c.holder_type && c.holder_field).toBeTruthy();
    const kind = (c.target_kind || 'row').toLowerCase();
    if (kind !== 'table' && !c.target_type_field) expect(c.target_type && c.target_key).toBeTruthy();
    expect(c.on_dangling).toBeTruthy();
    expect(c.cannot_infer).toBeTruthy();
  }
});
live('live estate has no dangling reference', async () => {
  const contracts = await surface('reference-contracts');
  const surfaceFor = { world: 'worlds', internet_source: 'internet-census',
    surface_renderer: 'renderers', presentation_template: 'presentation-templates',
    territory_presentation: 'territory-presentation', reference_contract: 'reference-contracts' };
  const cache = new Map();
  async function rows(type) {
    if (!surfaceFor[type]) return null;
    if (!cache.has(type)) cache.set(type, surface(surfaceFor[type]));
    return cache.get(type);
  }
  const dangling = [];
  for (const c of contracts) {
    if ((c.target_kind || 'row').toLowerCase() !== 'row' || c.target_type_field) continue;
    const targets = await rows(c.target_type);
    const holders = await rows(c.holder_type);
    if (!targets || !holders) continue;
    const known = new Set(targets.map(r => r[c.target_key]));
    for (const h of holders) {
      const raw = h[c.holder_field] || '';
      const names = c.separator ? raw.split(c.separator).map(x => x.trim()) : [raw.trim()];
      for (const name of names.filter(Boolean)) if (!known.has(name)) dangling.push([c.reference, h[c.holder_key], name]);
    }
  }
  expect(dangling).toEqual([]);
});
test('column reference resolves against row-named table', () => withFixture(true, ({ db, path }) => {
  db.exec("INSERT INTO pat VALUES ('p','thing','name,colour','')");
  const r = run(path);
  expect(r.code).toBe(0);
  expect(r.stdout).toContain('0 dangling');
}));
test('missing column is named as a column', () => withFixture(true, ({ db, path }) => {
  db.exec("INSERT INTO pat VALUES ('p','thing','name,ghost','')");
  const r = run(path);
  expect(r.code).toBe(1);
  expect(r.stdout).toContain('ghost');
  expect(r.stdout).toContain('no such column');
}));
test('row naming missing table is dangling, not undeclarable', () => withFixture(true, ({ db, path }) => {
  db.exec("INSERT INTO pat VALUES ('good','thing','name',''),('bad','gone','name','')");
  const r = run(path);
  expect(r.code).toBe(1);
  expect(r.stdout.includes('no such table') || r.stdout.includes('not a table')).toBe(true);
  expect(r.stdout).not.toContain('UNDECLARABLE');
}));
test('organ-backed holder is skipped', () => withFixture(true, ({ db, path }) => {
  db.exec("INSERT INTO pat VALUES ('organ-backed','','id,name','some-binary')");
  const r = run(path);
  expect(r.code).toBe(0);
  expect(r.stdout).not.toContain('organ-backed');
}));
