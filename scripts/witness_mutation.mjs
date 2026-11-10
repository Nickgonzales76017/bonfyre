#!/usr/bin/env bun
import { Database } from 'bun:sqlite';
import { copyFile, mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const skipFields = new Set(['id', 'namespace', 'created_at', 'updated_at', 'published_at', 'notes', 'cannot_infer', 'caption', 'detail', 'description', 'title', 'because', 'answers', 'cost_note', 'semantic_question']);
const skipMarkers = ['skipping', '(no ', 'not reachable; skip', 'no store reachable'];
const stripAlways = new Set(['declared', 'ran_at', 'generated_at', 'seconds']);
const identifier = value => { if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(value)) throw new Error(`invalid SQL identifier: ${value}`); return `"${value}"`; };
const stable = value => JSON.stringify(value, (_key, val) => val && !Array.isArray(val) && typeof val === 'object' ? Object.fromEntries(Object.entries(val).sort(([a], [b]) => a.localeCompare(b))) : val);

export function wrongValue(value) {
  if (value === null) return 'unset-by-mutation';
  if (typeof value === 'number') return value === 0 ? 7 : Number((value * 3 + 1).toFixed(4));
  const str = String(value);
  if (!str) return 'nonempty-by-mutation';
  if (['true', '1', 'yes'].includes(str.toLowerCase())) return str.toLowerCase() === 'true' ? 'false' : '0';
  if (['false', '0', 'no'].includes(str.toLowerCase())) return str.toLowerCase() === 'false' ? 'true' : '1';
  if (str.trim() && Number.isFinite(Number(str))) return String(Number((Number(str) * 3 + 1).toFixed(4)));
  if (str.includes(',')) { const parts = str.split(',').filter(s => s.trim()); return parts.length > 1 ? parts.slice(0, -1).join(',') : `${str}-mutated`; }
  return `${str}-mutated`;
}

export function validatePlan(plan) {
  if (!plan || !plan.tables || !plan.suites) throw new Error('plan requires tables and suites');
  if (plan.effect_class !== 'PURE') throw new Error('plan must declare effect_class PURE; remote or effectful commands are not a mutation witness');
  for (const [table, suites] of Object.entries(plan.tables)) {
    identifier(table);
    if (!Array.isArray(suites)) throw new Error(`table ${table} needs suite names`);
    for (const name of suites) if (!plan.suites[name]) throw new Error(`unknown suite ${name}`);
  }
  for (const [name, spec] of Object.entries(plan.suites)) {
    if (!Array.isArray(spec.argv) || !spec.argv.length || spec.argv.some(x => typeof x !== 'string' || !x)) throw new Error(`suite ${name} requires argv array; shell command strings are refused`);
  }
  for (const [table, spec] of Object.entries(plan.producers ?? {})) {
    identifier(table);
    if (!Array.isArray(spec.argv) || !spec.argv.length || spec.argv.some(x => typeof x !== 'string' || !x)) throw new Error(`producer ${table} requires argv array`);
  }
  const rowsPer = Number(plan.rows_per_table ?? 1);
  if (!Number.isInteger(rowsPer) || rowsPer < 1 || rowsPer > 200) throw new Error('rows_per_table must be 1..200');
  return plan;
}

export function runCommand(spec, env, defaultTimeout = 600_000) {
  const timeout = Math.min(Number(spec.timeout ?? defaultTimeout) * 1_000, 3_600_000);
  if (!Number.isFinite(timeout) || timeout <= 0) throw new Error('invalid command timeout');
  try {
    const p = Bun.spawnSync(spec.argv, { cwd: root, env, stdout: 'pipe', stderr: 'pipe', timeout });
    return { code: p.exitCode ?? 124, stdout: p.stdout.toString(), stderr: p.stderr.toString() };
  } catch (error) { return { code: 127, stdout: '', stderr: String(error) }; }
}

export function suiteVerdict(spec, env, runner = runCommand) {
  const result = runner(spec, env);
  const out = result.stdout.trim();
  const tail = out.split('\n').at(-1) || `exit ${result.code}`;
  if (result.code === 0 && skipMarkers.some(marker => out.toLowerCase().includes(marker))) return ['skipped', tail];
  return [result.code === 0 ? 'pass' : 'fail', result.code === 124 ? 'timeout' : tail];
}

export function producerReceipt(spec, env, runner = runCommand) {
  const result = runner(spec, env, 600_000);
  if (result.code !== 0 || !result.stdout.trim().startsWith('{')) return null;
  try { return JSON.parse(result.stdout); } catch { return null; }
}

export function stripFingerprint(receipt, unstable = []) {
  if (!receipt || typeof receipt !== 'object' || Array.isArray(receipt)) return receipt;
  const drop = new Set([...stripAlways, ...unstable]);
  return Object.fromEntries(Object.entries(receipt).filter(([key]) => !drop.has(key)));
}

export function baselineProducers(plan, env, runner = runCommand) {
  const answers = new Map(), unstable = new Map();
  for (const [table, spec] of Object.entries(plan.producers ?? {})) {
    const first = producerReceipt(spec, env, runner), second = producerReceipt(spec, env, runner);
    const moving = first && second ? [...new Set([...Object.keys(first), ...Object.keys(second)])].filter(key => !stripAlways.has(key) && stable(first[key]) !== stable(second[key])) : [];
    unstable.set(table, moving);
    answers.set(table, stripFingerprint(first, moving));
  }
  return { answers, unstable };
}

function rowsOf(db, table) {
  try { return db.query(`SELECT * FROM ${identifier(table)} LIMIT 200`).all(); }
  catch (error) { if (/no such table/.test(error.message)) return []; throw error; }
}

async function snapshot(source, path) {
  const db = new Database(source, { readonly: true, create: false });
  try { db.query('VACUUM INTO ?').run(path); } finally { db.close(); }
}

export async function mutationWitness({ source, plan, limit = 0, apply = false, runner = runCommand, progress = () => {} }) {
  validatePlan(plan);
  if (!apply) return { schema: 'bonfyre-witness-mutation-preview.v1', effect_class: 'PURE', store: source, tables: Object.keys(plan.tables), suites: Object.keys(plan.suites), producers: Object.keys(plan.producers ?? {}), note: 'dry run; no suite executed and no scratch database created; pass --apply to run' };
  const temp = await mkdtemp(join(tmpdir(), 'bonfyre-mutation-'));
  const snapshotPath = join(temp, 'source.db'), work = join(temp, 'work.db');
  const started = performance.now();
  try {
    await snapshot(source, snapshotPath);
    await copyFile(snapshotPath, work);
    const env = { ...process.env, BONFYRE_CMS_DB: work };
    const baseline = {};
    for (const [name, spec] of Object.entries(plan.suites)) {
      const [verdict, tail] = suiteVerdict(spec, env, runner);
      baseline[name] = verdict;
      progress(`baseline ${name}: ${verdict} ${tail}`);
    }
    const usable = new Set(Object.entries(baseline).filter(([, verdict]) => verdict === 'pass').map(([name]) => name));
    const { answers, unstable } = baselineProducers(plan, env, runner);
    if (!usable.size) throw new Error('no suite passes at baseline; nothing can be demonstrated');
    const guarded = [], blind = [], skipped = [], inert = [];
    let tested = 0;
    const sourceDb = new Database(snapshotPath, { readonly: true, create: false });
    try {
      for (const [table, suiteNames] of Object.entries(plan.tables)) {
        const rows = rowsOf(sourceDb, table);
        if (!rows.length) { skipped.push([table, 'no rows']); continue; }
        const published = rows.filter(row => String(row.status ?? 'published') === 'published');
        const chosen = (published.length ? published : rows).slice(0, Number(plan.rows_per_table ?? 1));
        const watchers = suiteNames.filter(name => usable.has(name));
        if (!watchers.length) { skipped.push([table, 'no passing suite claims to read it']); continue; }
        for (const row of chosen) for (const [field, current] of Object.entries(row)) {
          if (skipFields.has(field) || field.endsWith('_at') || (limit && tested >= limit)) continue;
          const changed = wrongValue(current);
          if (String(changed) === String(current)) continue;
          if (row.id === undefined) { skipped.push([`${table}.${field}`, 'row has no id coordinate']); continue; }
          await copyFile(snapshotPath, work);
          const mutable = new Database(work);
          try { mutable.query(`UPDATE ${identifier(table)} SET ${identifier(field)}=? WHERE id=?`).run(changed, row.id); }
          catch (error) { skipped.push([`${table}.${field}`, String(error).slice(0, 60)]); mutable.close(); continue; }
          mutable.close();
          const moved = [];
          for (const [producerTable, spec] of Object.entries(plan.producers ?? {})) {
            const before = answers.get(producerTable);
            if (before === null || before === undefined) continue;
            const after = stripFingerprint(producerReceipt(spec, env, runner), unstable.get(producerTable));
            if (after === null || stable(after) !== stable(before)) moved.push(producerTable);
          }
          tested++;
          if (!moved.length) { inert.push({ table, field, was: String(current).slice(0, 40), became: String(changed).slice(0, 40) }); progress(`inert ${table}.${field}`); continue; }
          const noticed = [], wentBlank = [];
          for (const name of watchers) {
            const [verdict] = suiteVerdict(plan.suites[name], env, runner);
            if (verdict === 'fail') noticed.push(name);
            else if (verdict === 'skipped') wentBlank.push(name);
          }
          let guard = '';
          const producer = plan.producers?.[table];
          if (noticed.length && producer) {
            const before = answers.get(table), after = stripFingerprint(producerReceipt(producer, env, runner), unstable.get(table));
            guard = before === null || after === null ? 'unknown' : stable(after) !== stable(before) ? 'consequence' : 'fingerprint-only';
          }
          const entry = { table, field, was: String(current).slice(0, 40), became: String(changed).slice(0, 40), noticed_by: noticed, skipped_after_mutation: wentBlank, guard };
          if (noticed.length) guarded.push(entry);
          else if (wentBlank.length) skipped.push([`${table}.${field}`, `suite skipped after the mutation: ${wentBlank.join(', ')}`]);
          else blind.push(entry);
          progress(`${noticed.length ? 'guarded' : wentBlank.length ? 'skipped' : 'blind'} ${table}.${field}`);
        }
      }
    } finally { sourceDb.close(); }
    return { schema: 'bonfyre-witness-mutation.v1', effect_class: 'PURE', store: source, suites_at_baseline: baseline, fields_mutated: tested, inert, guarded,
      guard_strength: { consequence: guarded.filter(g => g.guard === 'consequence').length, fingerprint_only: guarded.filter(g => g.guard === 'fingerprint-only').length, ungraded: guarded.filter(g => !g.guard || g.guard === 'unknown').length },
      blind, skipped, seconds: Math.round((performance.now() - started) / 100) / 10,
      cannot_infer: ['a guarded field -> a correct value; the suite reacted to a change, which says nothing about whether the original was right', 'a blind field -> an untested one; only that no suite run here reacted', 'a passing suite -> a load-bearing one, until it has gone red for a reason'] };
  } finally { await rm(temp, { recursive: true, force: true }); }
}

export async function main(argv = process.argv.slice(2)) {
  let source, planPath, json = false, apply = false, limit = 0;
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === '--json') json = true;
    else if (arg === '--apply') apply = true;
    else if (arg === '--limit') limit = Number(argv[++i]);
    else if (arg.startsWith('--limit=')) limit = Number(arg.slice(8));
    else if (!source) source = arg;
    else if (!planPath) planPath = arg;
    else throw new Error(`unknown argument: ${arg}`);
  }
  if (!source || !planPath || !Number.isInteger(limit) || limit < 0) throw new Error('usage: witness_mutation.mjs <cms.db> <plan.json> [--json] [--limit N] [--apply]');
  const plan = JSON.parse(await readFile(planPath, 'utf8'));
  const receipt = await mutationWitness({ source, plan, limit, apply, progress: message => console.error(message) });
  if (json) console.log(JSON.stringify(receipt, null, 2));
  else console.log(`${receipt.fields_mutated ?? 0} fields mutated; ${receipt.guarded?.length ?? 0} guarded, ${receipt.blind?.length ?? 0} blind, ${receipt.inert?.length ?? 0} inert${apply ? '' : ' (dry run)'}`);
  return 0;
}

if (import.meta.main) main().then(code => process.exit(code)).catch(error => { console.error(error.message); process.exit(2); });
