#!/usr/bin/env bun
// Staged public reader closure over admitted native SourceFrontier channels.
import { Database } from 'bun:sqlite';
import { createHash } from 'node:crypto';
import { existsSync, mkdirSync, mkdtempSync, renameSync, rmSync, writeFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

let cachedPolicy;
export function foundationPolicy() {
  if (!cachedPolicy) {
    const path = fileURLToPath(new URL('../foundation-subjects.cue', import.meta.url));
    const result = Bun.spawnSync(['cue', 'export', path, '-e', 'foundationSubjects']);
    if (result.exitCode !== 0) throw new Error(`CUE foundation subjects: ${new TextDecoder().decode(result.stderr).trim()}`);
    cachedPolicy = JSON.parse(new TextDecoder().decode(result.stdout));
  }
  return cachedPolicy;
}

const canonical = value => JSON.stringify(value, (_, item) => item && !Array.isArray(item) && typeof item === 'object'
  ? Object.fromEntries(Object.entries(item).toSorted(([a], [b]) => a.localeCompare(b))) : item);
const digest = value => createHash('sha256').update(canonical(value)).digest('hex');
const tables = db => new Set(db.query("SELECT name FROM sqlite_master WHERE type='table'").all().map(row => row.name));

export function sourceReaders(frontier, policy = foundationPolicy()) {
  if (!tables(frontier).has('channel')) throw new Error('SourceFrontier has no channel table');
  const placeholders = policy.reader_states.map(() => '?').join(',');
  const rows = frontier.query(`SELECT census_source,root_id,channel_id,material,protocol,state,
    COALESCE(last_contributed,'') AS last_contributed,COALESCE(contributions,0) AS contributions,
    COALESCE(first_seen,'') AS first_seen FROM channel WHERE state IN (${placeholders})
    AND COALESCE(census_source,'')<>'' ORDER BY census_source,channel_id`).all(...policy.reader_states);
  const seen = new Set();
  return rows.flatMap(row => {
    const source = String(row.census_source || '');
    if (!source || seen.has(source)) return [];
    seen.add(source);
    return [{ source, root_id: String(row.root_id || ''), channel_id: String(row.channel_id || ''),
      material: String(row.material || ''), protocol: String(row.protocol || ''), state: String(row.state || ''),
      last_contributed: String(row.last_contributed || ''), contributions: Number(row.contributions || 0),
      first_seen: String(row.first_seen || '') }];
  });
}

export function materializeFoundationSubjects(args, policy = foundationPolicy()) {
  if (!existsSync(args.fabricDb)) throw new Error(`SubjectFabric input does not exist: ${args.fabricDb}`);
  if (!existsSync(args.frontierDb)) throw new Error(`SourceFrontier input does not exist: ${args.frontierDb}`);
  const territories = args.territories?.length ? args.territories : policy.territories;
  for (const territory of territories) if (!policy.territories.includes(territory))
    throw new Error(`unknown foundation territory: ${territory}`);
  const frontier = new Database(args.frontierDb, { readonly: true });
  let readers;
  try { readers = sourceReaders(frontier, policy); } finally { frontier.close(); }
  if (!readers.length) throw new Error('SourceFrontier has no admitted census sources');
  const destination = resolve(args.out), directory = dirname(destination);
  mkdirSync(directory, { recursive: true });
  const staging = mkdtempSync(join(directory, '.foundation-subject-'));
  const staged = join(staging, 'index.db');
  try {
    const backup = Bun.spawnSync(['sqlite3', args.fabricDb, `.backup '${staged.replaceAll("'", "''")}'`]);
    if (backup.exitCode !== 0) throw new Error(`SubjectFabric backup failed: ${new TextDecoder().decode(backup.stderr).trim()}`);
    const db = new Database(staged);
    try {
      const existing = tables(db);
      const missing = policy.required_tables.filter(table => !existing.has(table));
      if (missing.length) throw new Error(`SubjectFabric missing required tables: ${missing.join(', ')}`);
      const readerDigest = digest(readers), compiledAt = new Date().toISOString().replace(/\.\d{3}Z$/, 'Z');
      db.transaction(() => {
        for (const territory of territories) {
          const subject = `${policy.subject_prefix}${territory}`;
          for (const table of policy.derived_tables) if (existing.has(table))
            db.query(`DELETE FROM ${table} WHERE subject=? AND scope=?`).run(subject, policy.scope);
          const core = { schema: policy.schema, subject, scope: policy.scope, basis: policy.basis,
            reader_digest: readerDigest, reader_count: readers.length, territory };
          const counts = { readers: readers.length, source_roots: new Set(readers.map(row => row.root_id)).size };
          db.query(`INSERT INTO subject_bundle(subject,scope,bundle_digest,closure_digest,scene_digest,compiled_at,counts_json,payload_json)
            VALUES(?,?,?,?,?,?,?,?)`).run(subject, policy.scope, digest(core), readerDigest, '', compiledAt,
            canonical(counts), canonical(core));
          const insert = db.query(`INSERT INTO subject_reader(subject,scope,census_source,root_id,channel_id,material,protocol,state,
            last_contributed,contributions,first_seen) VALUES(?,?,?,?,?,?,?,?,?,?,?)`);
          for (const row of readers) insert.run(subject, policy.scope, row.source, row.root_id, row.channel_id,
            row.material, row.protocol, row.state, row.last_contributed, row.contributions, row.first_seen);
        }
      })();
      if (db.query('PRAGMA integrity_check').get()?.integrity_check !== 'ok')
        throw new Error('foundation SubjectFabric integrity check failed');
      db.close();
      renameSync(staged, destination);
      const summary = { schema: policy.schema, out: destination, territories, admitted_sources: readers.length,
        reader_digest: readerDigest, truth: policy.truth, cannot_infer: policy.cannot_infer };
      if (args.manifest) writeFileSync(args.manifest, JSON.stringify(summary, null, 2) + '\n');
      return summary;
    } finally { try { db.close(); } catch { /* closed after verification */ } }
  } finally { rmSync(staging, { recursive: true, force: true }); }
}

if (import.meta.main) {
  try {
    const args = { territories: [] }, argv = process.argv.slice(2);
    for (let index = 0; index < argv.length; index++) {
      const key = argv[index];
      if (!key.startsWith('--') || !argv[index + 1]) throw new Error('expected --key value arguments');
      if (key === '--territory') args.territories.push(argv[++index]);
      else args[key.slice(2).replace(/-([a-z])/g, (_, letter) => letter.toUpperCase())] = argv[++index];
    }
    if (!args.fabricDb || !args.frontierDb || !args.out)
      throw new Error('--fabric-db, --frontier-db and --out are required');
    console.log(JSON.stringify(materializeFoundationSubjects(args)));
  } catch (error) { console.error(`foundation subjects: ${error.message}`); process.exitCode = 2; }
}
