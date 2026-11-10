// The schema workspace's package: its code, SQLite compiled to WebAssembly
// (sql.js, vendored with its provenance), and a sample database made from
// records this site already retains: the research desk's 87 papers with their
// authors and indexes, and the simulator's 2,225 NYC 311 phone requests.
import { readFileSync, mkdtempSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { createHash } from 'node:crypto';
import { Database } from 'bun:sqlite';

export function sampleDatabase(researchPath, tracePath) {
  const research = JSON.parse(readFileSync(researchPath, 'utf8')), trace = JSON.parse(readFileSync(tracePath, 'utf8'));
  const dir = mkdtempSync(join(tmpdir(), 'schema-sample-')), file = join(dir, 'sample.sqlite'), db = new Database(file);
  try {
    db.exec(`PRAGMA foreign_keys = ON;
      CREATE TABLE indexes (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE);
      CREATE TABLE works (id TEXT PRIMARY KEY, doi TEXT, title TEXT NOT NULL, year TEXT, venue TEXT, cited INTEGER, abstract TEXT);
      CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE);
      CREATE TABLE authorship (work_id TEXT NOT NULL REFERENCES works(id), person_id INTEGER NOT NULL REFERENCES people(id), position INTEGER NOT NULL, PRIMARY KEY (work_id, position));
      CREATE TABLE found_in (work_id TEXT NOT NULL REFERENCES works(id), index_id INTEGER NOT NULL REFERENCES indexes(id), url TEXT NOT NULL);
      CREATE TABLE requests (id INTEGER PRIMARY KEY, day TEXT NOT NULL, minute REAL NOT NULL, channel TEXT);
      CREATE INDEX idx_requests_minute ON requests (minute);`);
    const tx = db.transaction(() => {
      const ix = new Map(); const person = new Map();
      for (const r of research.records) {
        // Years are kept as the indexes give them, as text: one of the things this sample invites changing.
        db.query('INSERT INTO works VALUES (?,?,?,?,?,?,?)').run(r.id, r.doi, r.title, r.year == null ? null : String(r.year), r.venue || null, r.cited, r.abstract);
        r.authors.forEach((name, i) => { if (!person.has(name)) person.set(name, db.query('INSERT INTO people (name) VALUES (?) RETURNING id').get(name).id); db.query('INSERT INTO authorship VALUES (?,?,?)').run(r.id, person.get(name), i + 1); });
        for (const s of r.sources) { if (!ix.has(s.name)) ix.set(s.name, db.query('INSERT INTO indexes (name) VALUES (?) RETURNING id').get(s.name).id); db.query('INSERT INTO found_in VALUES (?,?,?)').run(r.id, ix.get(s.name), s.url); }
      }
      for (const m of trace.minutes) db.query('INSERT INTO requests (day, minute, channel) VALUES (?,?,?)').run(trace.day, m, 'phone');
    });
    tx();
    db.exec('VACUUM');
    db.close();
    return readFileSync(file);
  } finally { rmSync(dir, { recursive: true, force: true }); }
}

export function schemaFiles(researchPath, tracePath) {
  const hash = body => createHash('sha256').update(body).digest('hex');
  const files = { 'sample.sqlite': sampleDatabase(researchPath, tracePath) };
  for (const name of ['schema-lab.mjs', 'schema-lab-client.mjs', 'instrument-store.mjs']) files[name] = readFileSync(join(import.meta.dir, name));
  for (const name of ['sql-wasm.js', 'sql-wasm.wasm', 'LICENSE', 'provenance.tsv']) files['vendor/sql.js-1.12.0/' + name] = readFileSync(join(import.meta.dir, 'vendor/sql.js-1.12.0', name));
  files['instrument-local-store.mjs'] = readFileSync(join(import.meta.dir, '../../integrations/browser/extension/store.mjs'));
  const identity = Object.fromEntries(Object.keys(files).sort().map(name => [name, hash(files[name])]));
  const asset_prefix = `v-${hash(JSON.stringify(identity))}/`;
  return { sample: { bytes: files['sample.sqlite'].length }, asset_prefix, files: Object.fromEntries(Object.entries(files).map(([name, body]) => [asset_prefix + name, body])) };
}
