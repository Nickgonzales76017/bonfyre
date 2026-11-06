#!/usr/bin/env bun
// Scheduling projection only: pressure, reverse impact, and gaps stay distinct.
import { writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { loadRows } from './source_identity.mjs';

const rows = path => path ? loadRows(path) : [];
const byRoot = values => new Map(values.filter(row => row && row.root_id)
  .map(row => [String(row.root_id), row]));
const count = value => Number.parseInt(value || 0, 10) || 0;
let cachedPolicy;
export function sourceAttentionPolicy() {
  if (!cachedPolicy) {
    const path = fileURLToPath(new URL('../source-attention-policy.cue', import.meta.url));
    const result = Bun.spawnSync(['cue', 'export', path, '-e', 'sourceAttentionPolicy']);
    if (result.exitCode !== 0) throw new Error(`CUE source-attention policy: ${new TextDecoder().decode(result.stderr).trim()}`);
    cachedPolicy = JSON.parse(new TextDecoder().decode(result.stdout));
  }
  return cachedPolicy;
}

function impactByRoot(values) {
  const result = new Map();
  for (const row of values) {
    if (!row?.root_id) continue;
    const id = String(row.root_id);
    const current = result.get(id) || { affected_closures: 0, affected_scenes: 0,
      subject_reuse: 0, scene_reuse: 0 };
    for (const field of Object.keys(current)) {
      if (typeof row[field] === 'number' && Number.isFinite(row[field]))
        current[field] = Math.max(current[field], row[field]);
    }
    result.set(id, current);
  }
  return result;
}

function temperature(evidence, policy) {
  if (evidence.affected_closures >= policy.hot_impact_closures ||
      evidence.affected_scenes >= policy.hot_impact_scenes)
    return ['hot', 'measured_reverse_dependency_impact'];
  if (evidence.candidate_closure_reuse >= policy.hot_pressure_closures ||
      evidence.candidate_occurrences >= policy.hot_pressure_occurrences)
    return ['hot', 'repeated_scoped_candidate_pressure'];
  if (evidence.affected_closures || evidence.affected_scenes)
    return ['warm', 'measured_reverse_dependency_impact'];
  if (evidence.candidate_closure_reuse || evidence.candidate_occurrences)
    return ['warm', 'scoped_candidate_pressure'];
  if (evidence.open_gaps) return ['cool', 'open_gap_waiting_for_demand_or_resolution'];
  return ['cold', 'no_current_pressure_impact_or_gap'];
}

export function compileAttention(input) {
  const authored = sourceAttentionPolicy();
  const policy = Object.fromEntries(Object.keys(authored).filter(key => key.startsWith('hot_'))
    .map(key => [key, input.policy?.[key] ?? authored[key]]));
  for (const [key, value] of Object.entries(policy))
    if (!Number.isInteger(value) || value < 1) throw new Error(`${key} must be a positive integer`);
  const roots = byRoot(input.roots || []), pressure = byRoot(input.pressure || []);
  const impact = impactByRoot(input.impact || []), gaps = new Map(), counts = new Map();
  for (const row of input.gaps || []) if (row?.root_id) {
    const id = String(row.root_id), bucket = gaps.get(id) || [];
    bucket.push(row); gaps.set(id, bucket);
  }
  const plans = [];
  for (const [rootId, root] of roots) {
    const p = pressure.get(rootId) || {}, i = impact.get(rootId) || {}, gs = gaps.get(rootId) || [];
    const evidence = { candidate_closure_reuse: count(p.candidate_closure_reuse),
      candidate_occurrences: count(p.candidate_occurrences),
      affected_closures: count(i.affected_closures || i.subject_reuse),
      affected_scenes: count(i.affected_scenes || i.scene_reuse),
      open_gaps: gs.length, gap_kinds: [...new Set(gs.map(g => g.kind).filter(Boolean).map(String))].toSorted() };
    const [level, basis] = temperature(evidence, policy);
    counts.set(level, (counts.get(level) || 0) + 1);
    plans.push({ schema: 'bonfyre.source-attention-plan.v1', watch_id: `source-root:${rootId}`,
      subject: `SourceRoot:${rootId}`, root_id: rootId,
      root_address: root.address || root.root_address || null, temperature: level,
      reheat_on: level === 'cold' ? '' : `${level === 'cool' ? 'source-demand' : 'source-change'}:${rootId}`,
      basis, evidence, policy: { ...policy }, truth: authored.truth,
      cannot_infer: ['hot attention -> poll immediately',
        'candidate pressure -> source contribution',
        'reverse dependency impact -> visible material changed',
        'attention temperature -> semantic rank, authority, rights or economic value'] });
  }
  const order = { hot: 0, warm: 1, cool: 2, cold: 3 };
  plans.sort((a, b) => order[a.temperature] - order[b.temperature] ||
    b.evidence.affected_closures - a.evidence.affected_closures ||
    b.evidence.candidate_closure_reuse - a.evidence.candidate_closure_reuse ||
    a.root_id.localeCompare(b.root_id));
  return { plans, summary: { schema: 'bonfyre.source-attention-plan-summary.v1',
    roots: plans.length, temperatures: Object.fromEntries([...counts].toSorted(([a], [b]) => a.localeCompare(b))),
    truth: authored.truth } };
}

if (import.meta.main) {
  try {
    const args = {}, argv = process.argv.slice(2);
    for (let i = 0; i < argv.length; i += 2) {
      if (!argv[i]?.startsWith('--') || !argv[i + 1]) throw new Error('expected --key value arguments');
      args[argv[i].slice(2)] = argv[i + 1];
    }
    if (!args.roots || !args.gaps || !args.out) throw new Error('roots, gaps, and out are required');
    const policy = {};
    for (const policyKey of Object.keys(sourceAttentionPolicy()).filter(name => name.startsWith('hot_'))) {
      const flag = policyKey.replaceAll('_', '-');
      if (args[flag] !== undefined) policy[policyKey] = Number(args[flag]);
    }
    const result = compileAttention({ roots: rows(args.roots), gaps: rows(args.gaps),
      pressure: rows(args.pressure), impact: rows(args.impact), policy });
    writeFileSync(args.out, result.plans.map(row => JSON.stringify(row)).join('\n') +
      (result.plans.length ? '\n' : ''));
    console.log(JSON.stringify(result.summary));
  } catch (error) { console.error(`source attention: ${error.message}`); process.exitCode = 2; }
}
