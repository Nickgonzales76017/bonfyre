#!/usr/bin/env bun
// The CMS binary owns schema validation and writes; this only selects replay-safe create/update.
import { Database } from 'bun:sqlite';
import { readFileSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import { materialize } from './materialize_incumbent_records.mjs';

const unique = Object.freeze({ project: 'project_id', community_body: 'community',
  publication_occurrence: 'occurrence_id', external_resource: 'ext_id',
  household_opportunity: 'opportunity_id' });
const run = (binary, args) => spawnSync(binary, args, { encoding: 'utf8', maxBuffer: 4 * 1024 * 1024 });

export function ingest(input, cms, database, schemas) {
  const rows = materialize(JSON.parse(readFileSync(input, 'utf8')));
  const migrated = run(cms, ['schema', 'migrate', '--db', database, '--schemas', schemas]);
  if (migrated.status !== 0) throw new Error('CMS schema migration failed');
  const written = [];
  for (const record of rows) {
    const key = unique[record.type];
    if (!key) throw new Error('unsupported CMS owner type');
    const db = new Database(database, { readonly: true });
    let found;
    try { found = db.query(`SELECT id FROM ${record.type} WHERE namespace=? AND ${key}=?`)
      .get(record.namespace, record.body[key]); }
    finally { db.close(); }
    const command = found
      ? ['entry', 'update', record.type, String(found.id), JSON.stringify(record.body)]
      : ['entry', 'create', record.type, JSON.stringify(record.body)];
    const result = run(cms, [...command, '--ns', record.namespace, '--db', database, '--schemas', schemas]);
    if (result.status !== 0) throw new Error(`CMS write failed for ${record.type}`);
    written.push(`${found ? 'updated' : 'created'}:${record.type}`);
  }
  return { written, truth: 'receipt-bound public material only; no authority or provider effect' };
}

if (import.meta.main) {
  try {
    const argv = process.argv.slice(2), args = {};
    for (let i = 0; i < argv.length; i += 2) {
      if (!argv[i]?.startsWith('--') || !argv[i + 1]) throw new Error('expected --key value arguments');
      args[argv[i].slice(2)] = argv[i + 1];
    }
    for (const key of ['input', 'cms', 'db', 'schemas']) if (!args[key]) throw new Error(`missing --${key}`);
    console.log(JSON.stringify(ingest(args.input, args.cms, args.db, args.schemas)));
  } catch (error) { console.error(`incumbent ingest: ${error.message}`); process.exitCode = 2; }
}
