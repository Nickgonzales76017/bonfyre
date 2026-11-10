#!/usr/bin/env bun
// Declarative engineering diagnostics over SourceRoot, impact, and Scene edges.
import { writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { loadRows } from './source_identity.mjs';

const rows = path => path ? loadRows(path) : [];
const byRoot = values => new Map(values.filter(row => row && row.root_id)
  .map(row => [String(row.root_id), row]));
const number = value => Number(value || 0) || 0;
const jsonl = values => values.map(value => JSON.stringify(value)).join('\n') + (values.length ? '\n' : '');
let cachedPolicy;
export function sourceStarvationPolicy() {
  if (!cachedPolicy) {
    const path = fileURLToPath(new URL('../source-starvation-policy.cue', import.meta.url));
    const result = Bun.spawnSync(['cue', 'export', path, '-e', 'sourceStarvationPolicy']);
    if (result.exitCode !== 0) throw new Error(`CUE source-starvation policy: ${new TextDecoder().decode(result.stderr).trim()}`);
    cachedPolicy = JSON.parse(new TextDecoder().decode(result.stdout));
  }
  return cachedPolicy;
}

const operators = Object.freeze({ eq: (a, b) => a === b, gt: (a, b) => a > b,
  gte: (a, b) => a >= b });
function matches(condition, context) {
  const compare = operators[condition.op];
  if (!compare || !(condition.field in context)) throw new Error('invalid source-starvation predicate');
  return compare(context[condition.field], condition.compare_field
    ? context[condition.compare_field] : condition.value);
}
function issueFor(rule, context) {
  if (!(rule.all || []).every(condition => matches(condition, context)) ||
      (rule.any?.length && !rule.any.some(condition => matches(condition, context)))) return null;
  return { code: rule.code, severity: rule.severity,
    evidence: Object.fromEntries(Object.entries(rule.evidence).map(([field, source]) =>
      [field, context[source]])), investigate: rule.investigate };
}

export function compileStarvation(input) {
  const policy = input.policy || sourceStarvationPolicy();
  const roots = byRoot(input.roots || []), pressure = byRoot(input.pressure || []);
  const attention = byRoot(input.attention || []), impact = new Map(), gaps = new Map(), scene = new Map(), emptyScenes = [];
  for (const row of input.gaps || []) if (row?.root_id) {
    const id = String(row.root_id), bucket = gaps.get(id) || [];
    bucket.push(row); gaps.set(id, bucket);
  }
  for (const row of input.impact || []) if (row?.root_id) {
    const id = String(row.root_id), current = impact.get(id) || { closures: 0, scenes: 0, subjects: 0 };
    current.closures = Math.max(current.closures, number(row.affected_closures || row.subject_reuse));
    current.scenes = Math.max(current.scenes, number(row.affected_scenes || row.scene_reuse));
    current.subjects = Math.max(current.subjects, number(row.affected_subjects || row.subject_reuse));
    impact.set(id, current);
  }
  for (const row of input.sceneReceipts || []) {
    if (row?.schema !== 'bonfyre.scene-source-receipt.v1') continue;
    const rootsForScene = row.source_roots || [];
    if (!rootsForScene.length) emptyScenes.push({ scene_key: String(row.scene_key || ''),
      subject_address: row.subject_address, actor_scope: row.actor_scope,
      candidate_dependencies: (row.external_candidate_dependencies || []).length,
      action_apertures: (row.action_apertures || []).length,
      schema: 'bonfyre.scene-source-starvation-diagnostic.v1',
      truth: policy.scene_truth });
    for (const root of rootsForScene) if (root?.root_id) {
      const id = String(root.root_id), bucket = scene.get(id) || new Set();
      bucket.add(String(row.scene_key || '')); scene.set(id, bucket);
    }
  }
  const diagnostics = [], counts = new Map(), threshold = input.pressureThreshold ?? policy.pressure_threshold;
  if (!Number.isInteger(threshold) || threshold < 1) throw new Error('pressure threshold must be positive');
  for (const [rootId, root] of roots) {
    const pr = pressure.get(rootId) || {}, im = impact.get(rootId) || {}, gs = gaps.get(rootId) || [];
    const att = attention.get(rootId) || {}, sc = scene.get(rootId) || new Set();
    const context = { admitted: number(root.admitted_channels), active: number(root.active_channels),
      candidate_occurrences: number(pr.candidate_occurrences),
      candidate_closure_reuse: number(pr.candidate_closure_reuse),
      impact_closures: number(im.closures), impact_subjects: im.subjects,
      scene_edges: sc.size, open_gaps: gs.length,
      gap_kinds: [...new Set(gs.map(gap => gap.kind).filter(Boolean).map(String))].toSorted(),
      attention_temperature: att.temperature, attention_basis: att.basis,
      pressure_threshold: threshold };
    const issues = policy.rules.map(rule => issueFor(rule, context)).filter(Boolean);
    if (!issues.length) continue;
    for (const found of issues) counts.set(found.code, (counts.get(found.code) || 0) + 1);
    diagnostics.push({ schema: 'bonfyre.source-starvation-diagnostic.v1', root_id: rootId,
      root_address: root.address || root.root_address, provenance: root.provenance || [],
      scene_edges: sc.size, issues, truth: policy.diagnostic_truth,
      cannot_infer: policy.cannot_infer });
  }
  const severity = new Map(policy.severity_order.map((name, rank) => [name, rank]));
  diagnostics.sort((a, b) => Math.min(...a.issues.map(i => severity.get(i.severity) ?? 9)) -
    Math.min(...b.issues.map(i => severity.get(i.severity) ?? 9)) || b.issues.length - a.issues.length ||
    a.root_id.localeCompare(b.root_id));
  return { diagnostics, emptyScenes,
    summary: { schema: 'bonfyre.source-starvation-summary.v1', roots: roots.size,
      roots_with_diagnostics: diagnostics.length,
      issue_counts: Object.fromEntries([...counts].toSorted(([a], [b]) => a.localeCompare(b))),
      scenes_without_source_dependency: emptyScenes.length,
      truth: policy.summary_truth } };
}

if (import.meta.main) {
  try {
    const args = {}, argv = process.argv.slice(2);
    for (let i = 0; i < argv.length; i += 2) {
      if (!argv[i]?.startsWith('--') || !argv[i + 1]) throw new Error('expected --key value arguments');
      args[argv[i].slice(2)] = argv[i + 1];
    }
    if (!args.roots || !args.gaps || !args.attention || !args.out)
      throw new Error('roots, gaps, attention, and output are required');
    const threshold = Number(args['pressure-threshold'] ?? sourceStarvationPolicy().pressure_threshold);
    if (!Number.isInteger(threshold) || threshold < 1) throw new Error('pressure threshold must be positive');
    const result = compileStarvation({ roots: rows(args.roots), gaps: rows(args.gaps),
      pressure: rows(args.pressure), impact: rows(args.impact), attention: rows(args.attention),
      sceneReceipts: rows(args['scene-receipts']), pressureThreshold: threshold });
    writeFileSync(args.out, jsonl(result.diagnostics));
    if (args['scene-out']) writeFileSync(args['scene-out'], jsonl(result.emptyScenes));
    console.log(JSON.stringify(result.summary));
  } catch (error) { console.error(`source starvation: ${error.message}`); process.exitCode = 2; }
}
