#!/usr/bin/env bun
import { spawnSync } from 'node:child_process';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { run as closure } from './closure-algebra.mjs';

const configPath = resolve(dirname(fileURLToPath(import.meta.url)), 'residual-vocabularies.cue');
export function loadVocabularies() {
  const result = spawnSync('cue', ['export', configPath], { encoding: 'utf8' });
  if (result.status !== 0) throw new Error(result.stderr || 'CUE export failed');
  return JSON.parse(result.stdout);
}

export function phrase(condition, templates = ['{c}']) {
  const readable = condition.replace(/[-_]/g, ' ');
  const words = new Set(readable.toLowerCase().split(/\s+/));
  const phrases = [readable];
  for (const template of templates) {
    const extra = template.toLowerCase().replace('{c}', ' ').trim().split(/\s+/).filter(Boolean);
    if (extra.some(word => words.has(word))) continue;
    phrases.push(template.replace('{c}', readable));
  }
  const seen = new Set(), unique = [];
  for (const value of phrases) {
    const key = value.trim().toLowerCase().replace(/\s+/g, ' ');
    if (!seen.has(key)) { seen.add(key); unique.push(value.trim()); }
  }
  return unique;
}

export function target(state, vocabularies, ranAt = new Date().toISOString().replace('.000Z', 'Z')) {
  const closed = new Set(state.closed_at_fixed_point || []);
  const unknown = new Set(state.unknown_not_counted_as_blocked || []);
  const waiting = new Map();
  for (const row of state.resolvers_blocked || []) for (const condition of row.waiting_on) {
    if (!waiting.has(condition)) waiting.set(condition, []);
    waiting.get(condition).push(row.resolver);
  }
  const gains = new Map((state.closure_hypotheses || []).map(row => [row.condition, row.gain]));
  const voices = new Map(vocabularies.voices.map(row => [row.kind, row]));
  const targets = [...gains.keys()].filter(condition => !closed.has(condition)).sort().map(condition => {
    const kind = state.condition_kinds?.[condition] || state.condition_resolvable_by?.[condition] || (unknown.has(condition) ? 'authority' : 'evidence');
    const voice = voices.get(kind);
    return {
      condition, kind, unlocks_resolvers: gains.get(condition) || 0,
      blocks_resolvers: [...(waiting.get(condition) || [])].sort(),
      state: unknown.has(condition) ? 'unknown' : 'unmet',
      phrasings: phrase(condition, voice?.phrasings),
      capability_that_would_answer: voice?.capability || vocabularies.fallbackCapability,
      rung: 'named',
      phrasing_vocabulary: voice ? kind : 'none declared for this kind; the plain restatement is the only phrasing offered',
    };
  }).sort((a, b) => b.unlocks_resolvers - a.unlocks_resolvers || b.blocks_resolvers.length - a.blocks_resolvers.length);
  return {
    schema: 'bonfyre-residual-target.v1', domain: state.domain, subject: state.subject,
    ran_at: ranAt, effect_class: 'PURE', targets, rung_reached: 'named', rungs: vocabularies.rungs,
    cannot_infer: [
      'a target -> a demand anyone will meet', 'a phrasing -> a query that works',
      'a named capability -> a credentialed one', 'a named target -> a measured market',
    ],
  };
}

if (import.meta.main) {
  const args = process.argv.slice(2);
  const option = (name, fallback) => args.includes(name) ? args[args.indexOf(name) + 1] : fallback;
  const domain = option('--domain'), subject = option('--subject');
  if (!domain || !subject) { console.error('usage: bun scripts/residual-target.mjs --domain D --subject S [--db DB] [--json]'); process.exit(2); }
  try {
    const result = target(closure(option('--db', process.env.BONFYRE_CMS_DB || '/var/lib/bonfyre/cms/bonfyre_cms.db'), domain, subject), loadVocabularies());
    if (args.includes('--json')) console.log(JSON.stringify(result, null, 2));
    else {
      console.log(`  ${domain} / ${subject}: ${result.targets.length} residual target(s)`);
      console.log(`  rung reached: named  (of ${result.rungs.join(' -> ')})`);
      for (const row of result.targets) console.log(`  ${row.condition} ${row.kind} unlocks ${row.unlocks_resolvers} needs ${row.capability_that_would_answer}\n      asked as: ${row.phrasings.slice(0, 3).join(' | ')}`);
    }
  } catch (error) { console.error(`residual-target: ${error.message}`); process.exit(2); }
}
