#!/usr/bin/env bun
// Rebuildable semantic lookup over CMS-owned site fragments; no publication effect.
import { readFileSync, writeFileSync, mkdirSync, existsSync } from 'node:fs';
import { resolve, join } from 'node:path';
import { homedir } from 'node:os';
import { createHash } from 'node:crypto';
import { spawnSync } from 'node:child_process';
import { Database } from 'bun:sqlite';

const sha = value => createHash('sha256').update(value).digest('hex');
const normalize = value => String(value || '').trim().replace(/\s+/gu, ' ');

export function signature(row) {
  const parts = [];
  for (const key of ['heading', 'eyebrow', 'body', 'label']) {
    const value = normalize(row[key]);
    if (value && !parts.includes(value)) parts.push(value);
  }
  let tags;
  try { tags = JSON.parse(row.semantic_tags || '[]'); }
  catch { tags = []; }
  if (Array.isArray(tags)) {
    tags = tags.filter(tag => typeof tag === 'string' && !['migration', 'compiled-literal'].includes(tag) && !['function:', 'role:', 'equivalence:'].some(prefix => tag.startsWith(prefix)));
    if (tags.length) parts.push(tags.join(' '));
  }
  return parts.join('\n');
}

function binary(env, name) {
  const family = { 'bonfyre-vec': 'BonfyreVec', 'bonfyre-embed': 'BonfyreEmbed' }[name];
  return process.env[env] || [join(homedir(), '.bonfyre/bin', name), resolve(import.meta.dir, `../../${family}/${name}`)].find(existsSync) || name;
}

function run(program, args) {
  const result = spawnSync(program, args, { encoding: 'utf8', timeout: 1_800_000 });
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error((result.stderr || result.stdout || `exit ${result.status}`).trim().slice(-4000));
  return result.stdout.trim();
}

function rowsFromCms(dbPath, namespace, status) {
  const db = new Database(dbPath, { readonly: true });
  try {
    return status === 'all'
      ? db.query('SELECT * FROM site_fragment WHERE namespace = ?').all(namespace)
      : db.query('SELECT * FROM site_fragment WHERE namespace = ? AND status = ?').all(namespace, status);
  } finally { db.close(); }
}

export function main(args = process.argv.slice(2)) {
  const opts = { db: '', namespace: 'aurekai', status: 'published', 'work-dir': '', 'vec-db': '', backend: 'onnx', model: 'nomic-embed-text-onnx', dims: '768', apply: false };
  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (arg === '--apply') opts.apply = true;
    else if (arg.startsWith('--') && arg.slice(2) in opts && args[i + 1]) opts[arg.slice(2)] = args[++i];
    else throw new Error(`unknown or incomplete argument: ${arg}`);
  }
  if (!opts.db || !opts['work-dir'] || !opts['vec-db']) throw new Error('--db, --work-dir and --vec-db are required');
  if (!['draft', 'published', 'archived', 'all'].includes(opts.status)) throw new Error('invalid --status');
  if (!['onnx', 'hash'].includes(opts.backend)) throw new Error('invalid --backend');
  const dims = Number(opts.dims);
  if (!Number.isInteger(dims) || dims <= 0) throw new Error('invalid --dims');
  const work = resolve(opts['work-dir']), vecDb = resolve(opts['vec-db']);
  if (existsSync(work)) throw new Error(`work directory already exists; choose a fresh generation: ${work}`);
  if (opts.apply && existsSync(vecDb)) throw new Error(`vector database already exists; choose a fresh generation: ${vecDb}`);
  const rows = rowsFromCms(opts.db, opts.namespace, opts.status);
  const input = join(work, 'input'), vectors = join(work, 'vectors');
  mkdirSync(input, { recursive: true });
  mkdirSync(vectors);
  const entries = {};
  let skipped = 0;
  for (const row of rows) {
    const coordinate = String(row.coordinate || ''), body = signature(row);
    if (!coordinate || !body) { skipped++; continue; }
    const id = sha(coordinate).slice(0, 24);
    if (entries[id]) throw new Error(`coordinate digest collision: ${coordinate}`);
    writeFileSync(join(input, `${id}.txt`), `${body}\n`);
    entries[id] = { coordinate, id: row.id, page: row.page, slot: row.slot, audience: row.audience, kind: row.kind, status: row.status, source_ref: row.source_ref, source_digest: row.source_digest, signature_sha256: sha(body) };
  }
  const manifest = { schema_version: 'bonfyre.surface.semantic-index.v1', canonical_owner: 'BonfyreCMS', semantic_owner: 'BonfyreEmbed/BonfyreVec', source_db: resolve(opts.db), namespace: opts.namespace, status: opts.status, backend: opts.backend, model: opts.model, dims, queried_count: rows.length, indexable_count: Object.keys(entries).length, skipped_empty: skipped, entries, forbidden_inference: 'semantic proximity is not exact address equality, publication authority, effect authority, evidence, or permission to overwrite canonical content' };
  const manifestPath = join(work, 'manifest.json');
  writeFileSync(manifestPath, JSON.stringify(manifest, null, 2) + '\n');
  const summary = { apply: opts.apply, queried_count: rows.length, indexable_count: manifest.indexable_count, skipped_empty: skipped, manifest: manifestPath, manifest_sha256: sha(readFileSync(manifestPath)), vec_db: vecDb, backend: opts.backend, model: opts.model, dims };
  if (!opts.apply) { console.log(JSON.stringify(summary)); return 0; }
  const vec = binary('BONFYRE_VEC_BINARY', 'bonfyre-vec');
  const embed = binary('BONFYRE_EMBED_BINARY', 'bonfyre-embed');
  run(vec, ['init', vecDb]);
  run(embed, ['--input-dir', input, '--out', vectors, '--backend', opts.backend, '--model', opts.model, '--dims', String(dims), '--insert-db', vecDb, '--meta-out', join(work, 'embed-meta.json')]);
  const match = run(vec, ['count', vecDb]).match(/\b(\d+)\b/);
  const count = match ? Number(match[1]) : -1;
  if (count !== manifest.indexable_count) throw new Error(`vector count mismatch: expected ${manifest.indexable_count}, got ${count}`);
  console.log(JSON.stringify({ ...summary, vector_count: count, verified_complete: true, publication_effect: false }));
  return 0;
}

if (import.meta.main) {
  try { process.exitCode = main(); }
  catch (error) { console.error(error.message); process.exitCode = 1; }
}
