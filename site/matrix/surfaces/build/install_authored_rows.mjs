#!/usr/bin/env bun
// Local, explicit import of authored CMS rows; this tool never opens SSH.
import { readFileSync, existsSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawnSync } from 'node:child_process';
import { Database } from 'bun:sqlite';

const here = dirname(fileURLToPath(import.meta.url));
const root = resolve(here, '../../../..');
const schemas = resolve(root, 'cmd/BonfyreCMS/content-types');
const manifest = JSON.parse(readFileSync(resolve(here, 'authored-cms-rows.json'), 'utf8'));
const identifier = /^[A-Za-z_][A-Za-z0-9_]*$/;

export function loadDatasets() {
  return manifest.datasets.map(({ type, file, expect }) => {
    if (!identifier.test(type)) throw new Error(`unsafe type ${type}`);
    const schema = JSON.parse(readFileSync(resolve(schemas, `${type}.json`), 'utf8'));
    if (schema.name !== type) throw new Error(`schema name mismatch: ${type}`);
    const unique = Object.entries(schema.attributes).filter(([, rule]) => rule.uniqueWithinNamespace).map(([name]) => name);
    if (unique.length !== 1 || !identifier.test(unique[0])) throw new Error(`${type}: expected one declared namespace key`);
    const path = resolve(here, file);
    const lines = readFileSync(path, 'utf8').trimEnd().split('\n');
    if (lines.length !== expect) throw new Error(`${type}: expected ${expect} rows, got ${lines.length}`);
    const seen = new Set();
    const rows = lines.map((line, index) => {
      const row = JSON.parse(line);
      if (!row || Array.isArray(row) || typeof row !== 'object') throw new Error(`${type}:${index + 1}: expected object`);
      for (const [name, rule] of Object.entries(schema.attributes)) {
        if (rule.required && (row[name] === undefined || row[name] === null || row[name] === '')) throw new Error(`${type}:${index + 1}: missing ${name}`);
      }
      for (const name of Object.keys(row)) if (!(name in schema.attributes)) throw new Error(`${type}:${index + 1}: unknown ${name}`);
      const key = row[unique[0]];
      if (typeof key !== 'string' || seen.has(key)) throw new Error(`${type}:${index + 1}: invalid or duplicate ${unique[0]}`);
      seen.add(key);
      return row;
    });
    return { type, path, key: unique[0], rows };
  });
}

export function existingKeys(dbPath, namespace, dataset) {
  if (!existsSync(dbPath)) throw new Error(`CMS database does not exist: ${dbPath}`);
  const db = new Database(dbPath, { readonly: true });
  try {
    const columns = db.query(`PRAGMA table_info("${dataset.type}")`).all().map(row => row.name);
    if (!columns.includes('namespace') || !columns.includes(dataset.key)) throw new Error(`${dataset.type}: schema is not installed in CMS database`);
    const placeholders = dataset.rows.map(() => '?').join(',');
    return db.query(`SELECT "${dataset.key}" AS key FROM "${dataset.type}" WHERE namespace = ? AND "${dataset.key}" IN (${placeholders})`)
      .all(namespace, ...dataset.rows.map(row => row[dataset.key])).map(row => row.key);
  } finally {
    db.close();
  }
}

export function main(args = process.argv.slice(2)) {
  const options = { apply: false, type: '', db: '', ns: '', cms: resolve(root, 'cmd/BonfyreCMS/bonfyre-cms') };
  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (arg === '--apply') options.apply = true;
    else if (['--type', '--db', '--ns', '--cms'].includes(arg) && args[i + 1]) options[arg.slice(2)] = args[++i];
    else throw new Error(`unknown or incomplete argument: ${arg}`);
  }
  const datasets = loadDatasets();
  if (options.type && !datasets.some(dataset => dataset.type === options.type)) throw new Error(`unknown authored type: ${options.type}`);
  const selected = options.type ? datasets.filter(dataset => dataset.type === options.type) : datasets;
  if (!options.apply) {
    for (const dataset of selected) console.log(JSON.stringify({ type: dataset.type, rows: dataset.rows.length, key: dataset.key, state: 'validated', writes: 0 }));
    return 0;
  }
  if (!options.type || !options.db || !options.ns) throw new Error('--apply requires --type, --db and --ns; one CMS type per atomic import');
  const dataset = selected[0];
  const collisions = existingKeys(options.db, options.ns, dataset);
  if (collisions.length) throw new Error(`${dataset.type}: existing authored keys in ${options.ns}: ${collisions.join(', ')}`);
  if (!existsSync(options.cms)) throw new Error(`CMS binary does not exist: ${options.cms}`);
  const run = spawnSync(options.cms, ['entry', 'import-jsonl', dataset.type, dataset.path, '--ns', options.ns, '--db', options.db, '--schemas', schemas], { encoding: 'utf8' });
  if (run.error) throw run.error;
  if (run.status !== 0) throw new Error(`${dataset.type}: CMS import failed (${run.status}): ${(run.stderr || run.stdout).trim()}`);
  console.log((run.stdout || '').trim());
  return 0;
}

if (import.meta.main) {
  try { process.exitCode = main(); }
  catch (error) { console.error(error.message); process.exitCode = 1; }
}
