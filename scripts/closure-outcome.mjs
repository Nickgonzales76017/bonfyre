#!/usr/bin/env bun
import { createHash } from 'node:crypto';
import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { homedir } from 'node:os';
import { join } from 'node:path';
import { run as closure } from './closure-algebra.mjs';

const key = (domain, subject) => createHash('sha256').update(`${domain}|${subject}`).digest('hex').slice(0, 16);
const now = () => new Date().toISOString().replace('.000Z', 'Z');

export function record(state, recordedAt = now()) {
  return {
    schema: 'bonfyre-closure-prediction.v1', domain: state.domain, subject: state.subject,
    recorded_at: recordedAt, conditions_closed: state.closed_at_fixed_point.length,
    resolvers_admitted: state.resolvers_admitted,
    predictions: Object.fromEntries((state.closure_hypotheses || []).map(row => [row.condition, row.gain])),
    closed_set: [...state.closed_at_fixed_point].sort(),
  };
}

export function score(before, state, scoredAt = now()) {
  if (before.domain !== state.domain || before.subject !== state.subject) throw new Error('the recorded prediction is for a different subject');
  const previous = new Set(before.closed_set);
  const newly = [...new Set(state.closed_at_fixed_point)].filter(condition => !previous.has(condition)).sort();
  const delta = state.resolvers_admitted - before.resolvers_admitted;
  const unchanged = !newly.length && delta === 0;
  return {
    schema: 'bonfyre-closure-outcome.v1', domain: state.domain, subject: state.subject,
    scored_at: scoredAt, recorded_at: before.recorded_at, baseline_closed: before.conditions_closed,
    now_closed: state.closed_at_fixed_point.length, newly_closed: newly,
    resolver_delta: delta, predictions_made: Object.keys(before.predictions).length,
    scored: newly.map(condition => {
      const predicted = before.predictions[condition] ?? null;
      return { condition, predicted_gain: predicted, was_predicted: predicted !== null,
        note: predicted !== null ? `closed, and the prediction said it would unlock ${predicted} resolver(s)` : 'closed, and no prediction had been recorded for it' };
    }),
    nothing_changed: unchanged,
    verdict: unchanged ? 'nothing changed since the prediction was recorded, which is not evidence the prediction was right' : `${newly.length} condition(s) closed and ${delta >= 0 ? '+' : ''}${delta} resolver(s) became admissible`,
    cannot_infer: [
      'a correct prediction -> a good decision',
      'a wrong prediction -> a bad model; the world moves for reasons the closure never saw',
      'an unchanged closure -> a confirmed prediction',
      'one cohort agreeing -> a validated planner',
    ],
  };
}

export function run({ state, ledger, scoring = false }) {
  mkdirSync(ledger, { recursive: true });
  const path = join(ledger, `${key(state.domain, state.subject)}.json`);
  if (!scoring) {
    const result = record(state);
    writeFileSync(path, JSON.stringify(result, null, 2));
    return { result, path };
  }
  if (!existsSync(path)) throw new Error(`no prediction was recorded for ${state.domain}/${state.subject}; a score needs a claim made before the change, not after`);
  return { result: score(JSON.parse(readFileSync(path, 'utf8')), state), path };
}

if (import.meta.main) {
  const args = process.argv.slice(2);
  const option = (name, fallback) => args.includes(name) ? args[args.indexOf(name) + 1] : fallback;
  const domain = option('--domain'), subject = option('--subject');
  if (!domain || !subject) { console.error('usage: bun scripts/closure-outcome.mjs --domain D --subject S [--score] [--json] [--ledger DIR]'); process.exit(2); }
  try {
    const state = closure(option('--db', process.env.BONFYRE_CMS_DB || '/var/lib/bonfyre/cms/bonfyre_cms.db'), domain, subject);
    const { result, path } = run({ state, ledger: option('--ledger', process.env.BONFYRE_PREDICTION_LEDGER || join(homedir(), '.bonfyre/gym/predictions')), scoring: args.includes('--score') });
    if (args.includes('--json')) console.log(JSON.stringify(result, null, 2));
    else if (args.includes('--score')) {
      console.log(`  ${domain} / ${subject}`);
      console.log(`  predicted at ${result.recorded_at}, scored at ${result.scored_at}`);
      console.log(`  ${result.verdict}`);
      for (const row of result.scored) console.log(`    ${row.condition}: ${row.note}`);
    } else {
      console.log(`  recorded ${Object.keys(result.predictions).length} prediction(s) for ${domain}/${subject}`);
      console.log(`  baseline: ${result.conditions_closed} closed, ${result.resolvers_admitted} resolvers admitted`);
      console.log(`  ledger: ${path}`);
    }
  } catch (error) { console.error(`closure-outcome: ${error.message}`); process.exit(2); }
}
