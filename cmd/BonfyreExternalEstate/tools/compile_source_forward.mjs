#!/usr/bin/env bun
// SourceGap plans are observations and proposed routes, never effects.
import { writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { loadRows } from './source_identity.mjs';

let cachedPolicy;
export function sourceForwardPolicy() {
  if (!cachedPolicy) {
    const path = fileURLToPath(new URL('../source-forward-policy.cue', import.meta.url));
    const result = Bun.spawnSync(['cue', 'export', path, '-e', 'sourceForwardPolicy']);
    if (result.exitCode !== 0) throw new Error(`CUE source-forward policy: ${new TextDecoder().decode(result.stderr).trim()}`);
    cachedPolicy = JSON.parse(new TextDecoder().decode(result.stdout));
  }
  return cachedPolicy;
}
const number = value => {
  if (typeof value === 'boolean' || value === null || value === undefined || value === '') return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
};
const round6 = value => Math.round((value + Number.EPSILON) * 1e6) / 1e6;
const rows = path => path ? loadRows(path) : [];
const jsonl = values => values.map(value => JSON.stringify(value)).join('\n') + (values.length ? '\n' : '');
const tally = values => Object.fromEntries([...values.reduce((counts, value) =>
  counts.set(String(value), (counts.get(String(value)) || 0) + 1), new Map())]
  .toSorted(([a], [b]) => a.localeCompare(b)));

function measurements(input, policy) {
  const byId = new Map(), byAddress = new Map();
  const measurementFields = [...Object.keys(policy.benefit_weights),
    ...Object.keys(policy.cost_weights), ...policy.measurement_fields];
  for (const row of input) {
    if (!row || typeof row !== 'object') continue;
    const destinations = [];
    if (row.root_id) destinations.push([byId, String(row.root_id)]);
    if (row.root_address) destinations.push([byAddress, String(row.root_address)]);
    for (const [index, key] of destinations) {
      const value = index.get(key) || {};
      for (const field of measurementFields) {
        const n = number(row[field]);
        if (n !== null) value[field] = n;
      }
      index.set(key, value);
    }
  }
  return { byId, byAddress };
}

function dividend(values, policy) {
  const benefits = {}, costs = {};
  for (const [field, weight] of Object.entries(policy.benefit_weights))
    if (field in values) benefits[field] = values[field] * weight;
  for (const [field, weight] of Object.entries(policy.cost_weights))
    if (field in values) costs[field] = values[field] * weight;
  let score = null;
  if (Object.keys(benefits).length || Object.keys(costs).length)
    score = round6(Object.values(benefits).reduce((a, b) => a + b, 0) -
      Object.values(costs).reduce((a, b) => a + b, 0));
  const reduction = {};
  const before = values.candidate_branches_before, after = values.candidate_branches_after;
  if (before !== undefined) reduction.candidate_branches_before = before;
  if (after !== undefined) reduction.candidate_branches_after = after;
  if (before !== undefined && after !== undefined && before >= after) {
    reduction.branches_eliminated = before - after;
    reduction.reduction_ratio = before === 0 ? 0 : round6((before - after) / before);
  }
  if (values.queries_avoided !== undefined) reduction.queries_avoided = values.queries_avoided;
  return { state: score === null ? 'UNMEASURED' : 'MEASURED', score,
    benefit_components: benefits, cost_components: costs, calculation_reduction: reduction };
}

function resolutionPlan(gap, policy) {
  return [...new Set(gap.resolution_classes || [])].map(route => {
    const row = { class: route, effect: 'candidate_only' };
    if (policy.identity_disclosure_classes.includes(route)) row.identity_disclosure_required = true;
    return row;
  });
}

export function compileForward(input) {
  const policy = input.policy || sourceForwardPolicy();
  const roots = new Map((input.roots || []).filter(row => row?.root_id)
    .map(row => [row.root_id, row]));
  const { byId, byAddress } = measurements(input.demand || [], policy), plans = [], opportunities = [];
  for (const gap of input.gaps || []) {
    const root = roots.get(gap.root_id) || {};
    const values = { ...byAddress.get(root.address), ...byId.get(gap.root_id) };
    const closure = dividend(values, policy), resolution = resolutionPlan(gap, policy);
    const plan = { schema: 'bonfyre.source-gap-plan.v1', gap_id: gap.gap_id ?? null,
      root_id: gap.root_id ?? null, root_address: root.address ?? null,
      gap_kind: gap.kind ?? null, reason: gap.reason ?? null, provenance: gap.provenance || [],
      resolution_plan: resolution, closure_dividend: closure,
      cannot_infer: policy.truth };
    plans.push(plan);
    const commercial = [...new Set(resolution.map(row => row.class))]
      .filter(route => policy.relationship_classes.includes(route)).toSorted();
    if (commercial.length) opportunities.push({
      schema: 'bonfyre.source-relationship-opportunity.v1', gap_id: plan.gap_id,
      root_id: plan.root_id, root_address: plan.root_address, gap_kind: plan.gap_kind,
      possible_relationships: commercial, closure_dividend_state: closure.state,
      closure_dividend_score: closure.score,
      identity_disclosure_required: commercial.some(route => policy.identity_disclosure_classes.includes(route)),
      effect: 'candidate_only' });
  }
  plans.sort((a, b) => Number(a.closure_dividend.state !== 'MEASURED') -
    Number(b.closure_dividend.state !== 'MEASURED') ||
    (b.closure_dividend.score || 0) - (a.closure_dividend.score || 0) ||
    String(a.gap_kind || '').localeCompare(String(b.gap_kind || '')) ||
    String(a.gap_id || '').localeCompare(String(b.gap_id || '')));
  opportunities.sort((a, b) => Number(a.closure_dividend_state !== 'MEASURED') -
    Number(b.closure_dividend_state !== 'MEASURED') ||
    (b.closure_dividend_score || 0) - (a.closure_dividend_score || 0) ||
    String(a.gap_id || '').localeCompare(String(b.gap_id || '')));
  const summary = { schema: 'bonfyre.source-forward.v1', gap_plans: plans.length,
    measured_plans: plans.filter(row => row.closure_dividend.state === 'MEASURED').length,
    relationship_opportunities: opportunities.length, gap_kinds: tally(plans.map(row => row.gap_kind)),
    resolution_classes: tally(plans.flatMap(row => row.resolution_plan.map(route => route.class))), plans };
  return { summary, opportunities };
}

if (import.meta.main) {
  try {
    const args = {}, argv = process.argv.slice(2);
    for (let i = 0; i < argv.length; i += 2) {
      if (!argv[i]?.startsWith('--') || !argv[i + 1]) throw new Error('expected --key value arguments');
      args[argv[i].slice(2)] = argv[i + 1];
    }
    if (!args.roots || !args.gaps || !args.out) throw new Error('roots, gaps, and out are required');
    const result = compileForward({ roots: rows(args.roots), gaps: rows(args.gaps), demand: rows(args.demand) });
    writeFileSync(args.out, `${JSON.stringify(result.summary, null, 2)}\n`);
    if (args.opportunities) writeFileSync(args.opportunities, jsonl(result.opportunities));
    const { plans: _plans, schema: _schema, ...counts } = result.summary;
    console.log(JSON.stringify(counts));
  } catch (error) { console.error(`source forward: ${error.message}`); process.exitCode = 2; }
}
