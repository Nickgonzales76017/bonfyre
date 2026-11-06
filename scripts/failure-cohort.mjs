#!/usr/bin/env bun
// Read-only failure cohort over the operator owner, with CUE-authored causes.
import { Database } from 'bun:sqlite';
import { existsSync } from 'node:fs';
import { homedir } from 'node:os';
import { join } from 'node:path';

const dbPath = process.env.BONFYRE_OPERATOR_DB ?? join(homedir(), '.bonfyre/operator/operator.sqlite3');
const policyPath = new URL('./failure-causes.cue', import.meta.url).pathname;
const json = process.argv.includes('--json');
if (!existsSync(dbPath)) {
  process.stderr.write(`no operator database at ${dbPath}\n`);
  process.exit(2);
}
const exported = Bun.spawnSync(['cue', 'export', policyPath, '-e', 'causes', '--out', 'json'], {
  stdout: 'pipe', stderr: 'pipe',
});
if (exported.exitCode !== 0) {
  process.stderr.write(exported.stderr);
  process.exit(exported.exitCode ?? 1);
}
const causes = JSON.parse(exported.stdout.toString()).map(row => ({
  ...row, matcher: new RegExp(row.pattern, 'i'),
}));
const db = new Database(dbPath, { readonly: true, strict: true });
let rows;
try {
  rows = db.query('SELECT id,title,state,attempts,last_error,updated_at FROM goals').all();
} finally {
  db.close();
}
const classify = error => {
  const value = String(error ?? '');
  const matched = causes.find(cause => cause.matcher.test(value));
  if (matched) return matched;
  if (!value.trim()) return { name: 'unrecorded', closable: true,
    remedy: 'the run failed and recorded no reason; that is its own defect' };
  return { name: 'other', closable: true,
    remedy: 'unclassified; read the rows before building anything' };
};
const clusters = new Map();
for (const row of rows) {
  const cause = classify(row.last_error);
  if (!clusters.has(cause.name)) clusters.set(cause.name, {
    cause: cause.name, closable: cause.closable, remedy: cause.remedy,
    goals: 0, attempts: 0, still_live: 0, titles: [],
  });
  const cluster = clusters.get(cause.name);
  cluster.goals++;
  cluster.attempts += Number(row.attempts ?? 0);
  if (row.state === 'retry' || row.state === 'queued') cluster.still_live++;
  if (cluster.titles.length < 3 && row.title) cluster.titles.push(row.title);
}
const totalAttempts = [...clusters.values()].reduce((sum, row) => sum + row.attempts, 0) || 1;
for (const row of clusters.values()) {
  row.recurrence = row.still_live ? 1 : 0.35;
  row.attempt_share = row.attempts / totalAttempts;
  row.stuckness = Math.round(Math.log1p(row.attempts / Math.max(row.goals, 1)) * 1000) / 1000;
  row.rank = Math.round(row.goals * row.stuckness * row.recurrence * 1000) / 1000;
}
const ordered = [...clusters.values()].filter(row => row.closable).sort((a, b) => b.rank - a.rank);
const awaiting = [...clusters.values()].filter(row => !row.closable).sort((a, b) => b.attempts - a.attempts);
if (json) {
  process.stdout.write(JSON.stringify({
    schema: 'bonfyre-failure-cohort.v1', goals_examined: rows.length,
    attempts_examined: totalAttempts, clusters: ordered, awaiting_person: awaiting,
    cannot_infer: [
      'a ranked failure -> a prioritized task',
      'an attempt count -> a cost in money',
      'clustering by error text -> distinct faults (two faults that print alike count as one)',
    ],
  }, null, 2) + '\n');
} else {
  process.stdout.write(`  ${rows.length} goals, ${totalAttempts} attempts examined\n\n`);
  process.stdout.write(`  ${'cause'.padEnd(18)}${'goals'.padStart(6)}${'attempts'.padStart(10)}${'live'.padStart(6)}${'rank'.padStart(8)}  remedy\n`);
  process.stdout.write(`  -- closable here ${'-'.repeat(58)}\n`);
  for (const row of ordered)
    process.stdout.write(`  ${row.cause.padEnd(18)}${String(row.goals).padStart(6)}${String(row.attempts).padStart(10)}${String(row.still_live).padStart(6)}${row.rank.toFixed(2).padStart(8)}  ${row.remedy.slice(0, 54)}\n`);
  if (awaiting.length) {
    process.stdout.write(`  -- past the human line; no build follows from these ${'-'.repeat(22)}\n`);
    for (const row of awaiting)
      process.stdout.write(`  ${row.cause.padEnd(18)}${String(row.goals).padStart(6)}${String(row.attempts).padStart(10)}${String(row.still_live).padStart(6)}${'--'.padStart(8)}  ${row.remedy.slice(0, 54)}\n`);
  }
  process.stdout.write('\n');
  if (ordered.length) {
    const top = ordered[0];
    process.stdout.write(`  build next: ${top.cause} (${top.goals} goals, ${top.attempts} attempts, rank ${top.rank.toFixed(2)})\n`);
    if (top.titles.length) process.stdout.write(`  e.g. ${top.titles[0].slice(0, 70)}\n`);
  }
  if (awaiting.length)
    process.stdout.write(`  waiting on a person: ${awaiting.reduce((sum, row) => sum + row.attempts, 0)} attempts across ${awaiting.reduce((sum, row) => sum + row.goals, 0)} goals\n`);
}
