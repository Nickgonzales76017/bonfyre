#!/usr/bin/env bun
// Draft-only, create-only CMS admission. One import-jsonl transaction per plan.
import { readFileSync, writeFileSync, mkdtempSync, rmSync, existsSync } from 'node:fs';
import { tmpdir, homedir } from 'node:os';
import { resolve, join } from 'node:path';
import { spawnSync } from 'node:child_process';
import { Database } from 'bun:sqlite';

const safeIdentifier = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,191}$/;
const root = resolve(import.meta.dir, '../../..');

function cmsBinary() {
  return process.env.BONFYRE_CMS_BINARY || [join(homedir(), '.bonfyre/bin/bonfyre-cms'), join(root, 'cmd/BonfyreCMS/bonfyre-cms')].find(existsSync) || 'bonfyre-cms';
}

function run(binary, args) {
  const result = spawnSync(binary, args, { encoding: 'utf8', timeout: 120_000 });
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error((result.stderr || result.stdout || `exit ${result.status}`).trim());
  return result.stdout.trim();
}

export function prepareDrafts(plan, limit = 0) {
  const drafts = limit > 0 ? (plan.drafts || []).slice(0, limit) : (plan.drafts || []);
  const seen = new Set();
  return drafts.map((draft, index) => {
    const coordinate = String(draft.coordinate || '');
    if (!safeIdentifier.test(coordinate)) throw new Error(`draft ${index}: invalid coordinate ${coordinate}`);
    if (seen.has(coordinate)) throw new Error(`draft ${index}: duplicate coordinate ${coordinate}`);
    seen.add(coordinate);
    return { ...draft, status: 'draft' };
  });
}

function existingCoordinates(dbPath, namespace) {
  const db = new Database(dbPath, { readonly: true });
  try {
    return new Set(db.query('SELECT coordinate FROM site_fragment WHERE namespace = ?').all(namespace).map(row => row.coordinate));
  } finally { db.close(); }
}

export function main(args = process.argv.slice(2)) {
  if (!args.length) throw new Error('usage: stage_fragment_migration.mjs PLAN.json --db DB --schemas DIR [--namespace NS] [--limit N] [--apply]');
  const options = { plan: args[0], db: '', schemas: '', namespace: 'aurekai', limit: 0, apply: false };
  for (let i = 1; i < args.length; i++) {
    const arg = args[i];
    if (arg === '--apply') options.apply = true;
    else if (['--db', '--schemas', '--namespace', '--limit'].includes(arg) && args[i + 1]) options[arg.slice(2)] = args[++i];
    else throw new Error(`unknown or incomplete argument: ${arg}`);
  }
  if (!safeIdentifier.test(options.namespace)) throw new Error('namespace must be a short Bonfyre identifier');
  if (!options.db || !options.schemas) throw new Error('--db and --schemas are required');
  const plan = JSON.parse(readFileSync(options.plan, 'utf8'));
  if (plan.schema_version !== 'bonfyre.surface.fragment-migration-plan.v1') throw new Error('unsupported migration plan schema');
  const drafts = prepareDrafts(plan, Number(options.limit));
  if (!options.apply) {
    console.log(JSON.stringify({ schema_version: 'bonfyre.surface.fragment-stage-preview.v1', apply: false, candidate_count: drafts.length, namespace: options.namespace, db: options.db, write_owner: 'BonfyreCMS', read_owner: 'BonfyreCMS read-only snapshot' }));
    return 0;
  }
  const cms = cmsBinary();
  run(cms, ['schema', 'apply', resolve(options.schemas, 'site_fragment.json'), '--db', options.db, '--schemas', options.schemas]);
  const existing = existingCoordinates(options.db, options.namespace);
  const newDrafts = drafts.filter(draft => !existing.has(draft.coordinate));
  const skipped = drafts.length - newDrafts.length;
  if (newDrafts.length) {
    const dir = mkdtempSync(join(tmpdir(), 'bonfyre-fragment-stage-'));
    try {
      const jsonl = join(dir, 'drafts.jsonl');
      writeFileSync(jsonl, newDrafts.map(draft => JSON.stringify(draft)).join('\n') + '\n');
      run(cms, ['entry', 'import-jsonl', 'site_fragment', jsonl, '--ns', options.namespace, '--db', options.db, '--schemas', options.schemas]);
    } finally { rmSync(dir, { recursive: true, force: true }); }
  }
  const observed = existingCoordinates(options.db, options.namespace);
  if (newDrafts.some(draft => !observed.has(draft.coordinate))) throw new Error('CMS import did not materialize every requested coordinate');
  console.log(JSON.stringify({ schema_version: 'bonfyre.surface.fragment-stage-result.v1', apply: true, requested: drafts.length, created: newDrafts.length, skipped_existing: skipped, failed_count: 0, failed: [], namespace: options.namespace, db: options.db, write_owner: 'BonfyreCMS', read_owner: 'BonfyreCMS read-only snapshot', publication_effect: false, forbidden_inference: 'draft admission is not publication; an existing coordinate is not overwritten because authored state may have diverged from its compiled source' }));
  return 0;
}

if (import.meta.main) {
  try { process.exitCode = main(); }
  catch (error) { console.error(error.message); process.exitCode = 1; }
}
