#!/usr/bin/env bun
// Project an authored SourceAttention plan into the existing native Attention store.
import { existsSync } from 'node:fs';
import { loadRows } from './source_identity.mjs';
import { sourceAttentionPolicy } from './compile_source_attention.mjs';

const validAtom = (value, pattern) => typeof value === 'string' && value.length > 0 &&
  new RegExp(pattern).test(value) && !/["\\]/.test(value);

export function syncSourceAttention(planRows, options, policy = sourceAttentionPolicy().sync, spawn = Bun.spawnSync) {
  const rows = planRows.filter(row => row && typeof row === 'object' && !Array.isArray(row) && row.schema === policy.plan_schema)
    .slice(0, options.limit > 0 ? options.limit : undefined);
  let applied = 0, failed = 0;
  const commands = [];
  for (const row of rows) {
    const watch = String(row.watch_id || ''), subject = String(row.subject || '');
    const temperature = String(row.temperature || ''), signal = String(row.reheat_on || '');
    const evidence = row.evidence || {};
    const detail = `${policy.detail_prefix}:${row.basis || ''}:p${evidence.candidate_occurrences || 0}` +
      `:i${evidence.affected_closures || 0}:g${evidence.open_gaps || 0}`;
    if (!validAtom(watch, policy.atom_pattern) || !validAtom(subject, policy.atom_pattern) ||
      !policy.temperatures.includes(temperature) || !validAtom(detail, policy.atom_pattern) ||
      (signal && !validAtom(signal, policy.atom_pattern))) { failed++; continue; }
    const argv = [options.attentionBin, 'schedule', '--db', options.db, '--watch', watch,
      '--subject', subject, '--temperature', temperature, '--detail', detail];
    if (signal) argv.push('--reheat-on', signal);
    commands.push({ watch_id: watch, temperature, reheat_on: signal });
    if (options.dryRun) continue;
    const result = spawn(argv);
    if (result.exitCode === 0) applied++; else failed++;
  }
  return { schema: policy.output_schema, planned: rows.length, applied, failed,
    dry_run: !!options.dryRun, commands: options.dryRun ? commands : [],
    truth: policy.truth, cannot_infer: policy.cannot_infer };
}

if (import.meta.main) {
  try {
    const args = { dryRun: false, limit: 0 }, argv = process.argv.slice(2);
    for (let index = 0; index < argv.length; index++) {
      const arg = argv[index];
      if (arg === '--dry-run') { args.dryRun = true; continue; }
      if (!arg.startsWith('--') || !argv[index + 1]) throw new Error('expected --key value arguments');
      args[arg.slice(2).replace(/-([a-z])/g, (_, letter) => letter.toUpperCase())] = argv[++index];
    }
    if (!args.plan || !args.attentionBin || !args.db) throw new Error('--plan, --attention-bin and --db are required');
    if (!existsSync(args.attentionBin)) throw new Error('attention binary does not exist');
    args.limit = Number(args.limit) || 0;
    const result = syncSourceAttention(loadRows(args.plan), args);
    console.log(JSON.stringify(result));
    if (result.failed) process.exitCode = 1;
  } catch (error) { console.error(`source attention sync: ${error.message}`); process.exitCode = 2; }
}
