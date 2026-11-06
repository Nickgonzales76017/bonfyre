#!/usr/bin/env bun
// Candidate pressure over the authored Frontier projection; never contribution.
import { basename } from 'node:path';
import { writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { loadRows } from './source_identity.mjs';

let cachedPolicy;
export function sourcePressurePolicy() {
  if (!cachedPolicy) {
    const path = fileURLToPath(new URL('../source-pressure-policy.cue', import.meta.url));
    const result = Bun.spawnSync(['cue', 'export', path, '-e', 'sourcePressurePolicy']);
    if (result.exitCode !== 0) throw new Error(`CUE source pressure: ${new TextDecoder().decode(result.stderr).trim()}`);
    cachedPolicy = JSON.parse(new TextDecoder().decode(result.stdout));
  }
  return cachedPolicy;
}

const array = value => Array.isArray(value) ? value : [];
const sorted = values => [...values].toSorted();
const jsonl = rows => rows.map(row => JSON.stringify(row)).join('\n') + (rows.length ? '\n' : '');

export function sourceIndex(specs, channels, roots, policy = sourcePressurePolicy()) {
  const rootById = new Map(roots.map(root => [root.root_id, root]));
  const occurrenceRoot = new Map();
  for (const channel of channels) for (const coordinate of array(channel.provenance)) {
    const previous = occurrenceRoot.get(coordinate);
    if (previous && previous !== channel.root_id)
      throw new Error(`ambiguous Frontier provenance coordinate: ${coordinate}`);
    occurrenceRoot.set(coordinate, channel.root_id);
  }
  const byIdentity = new Map();
  for (const spec of specs) {
    const split = spec.indexOf('=');
    if (split < 1) throw new Error('--source-input must be LABEL=PATH');
    const label = spec.slice(0, split), path = spec.slice(split + 1);
    for (const [index, row] of loadRows(path).entries()) {
      if (!row || typeof row !== 'object' || Array.isArray(row)) continue;
      const coordinate = `${label}:${basename(path)}:${index + 1}`;
      const rootId = occurrenceRoot.get(coordinate);
      if (!rootId || !rootById.has(rootId))
        throw new Error(`source occurrence missing from Frontier projection: ${coordinate}`);
      const identities = policy.identity_fields.flatMap(key =>
        typeof row[key] === 'string' && row[key].trim() ? [row[key].trim()] : []);
      if (typeof row.source_ref === 'string' && row.source_ref.toUpperCase().startsWith(policy.external_prefix))
        identities.push(policy.external_alias_prefix + row.source_ref.toLowerCase());
      for (const identity of identities) {
        if (!byIdentity.has(identity)) byIdentity.set(identity, new Set());
        byIdentity.get(identity).add(rootId);
      }
    }
  }
  return { byIdentity, rootById };
}

export function candidateOccurrences(closure, policy = sourcePressurePolicy()) {
  return policy.candidate_paths.flatMap(rule => array(closure[rule.parent]).flatMap(parent =>
    parent && typeof parent === 'object' ? array(parent[rule.group]).flatMap(group =>
      group && typeof group === 'object' ? array(group[rule.leaf]).flatMap(candidate =>
        candidate && typeof candidate === 'object' && candidate.identity
          ? [{ identity: String(candidate.identity), basis: rule.basis,
            demand_id: String(parent[rule.demand_id] || ''), source: String(group[rule.source] || '') }]
          : []) : []) : []));
}

export function compileSourcePressure(specs, channelRows, rootRows, closurePaths, policy = sourcePressurePolicy()) {
  const { byIdentity, rootById } = sourceIndex(specs, channelRows, rootRows, policy);
  const pressure = new Map(), unmapped = new Map();
  let closureCount = 0;
  for (const path of closurePaths) for (const closure of loadRows(path)) {
    if (!closure || typeof closure !== 'object' || Array.isArray(closure)) continue;
    closureCount++;
    const subject = String(closure.subject || ''), scope = String(closure.scope || '');
    for (const occurrence of candidateOccurrences(closure, policy)) {
      const matched = byIdentity.get(occurrence.identity);
      if (!matched?.size) {
        unmapped.set(occurrence.identity, (unmapped.get(occurrence.identity) || 0) + 1);
        continue;
      }
      for (const rootId of matched) {
        let row = pressure.get(rootId);
        if (!row) {
          row = { subjects: new Set(), scopes: new Set(), closures: new Set(),
            demands: new Set(), identities: new Set(), occurrences: 0, pack: 0, world: 0, ambiguous: 0 };
          pressure.set(rootId, row);
        }
        row.subjects.add(subject);
        row.scopes.add(scope);
        row.closures.add(`${subject}|${scope}`);
        row.demands.add(occurrence.demand_id);
        row.identities.add(occurrence.identity);
        row.occurrences++;
        row.pack += Number(occurrence.basis === 'pack_demand');
        row.world += Number(occurrence.basis === 'world_demand');
        row.ambiguous += Number(matched.size > 1);
      }
    }
  }
  const rows = [...pressure].map(([rootId, measured]) => {
    const root = rootById.get(rootId);
    return { schema: 'bonfyre.source-pressure.v1', root_id: rootId, root_address: root.address,
      candidate_subject_reuse: measured.subjects.size, candidate_scope_reuse: measured.scopes.size,
      candidate_closure_reuse: measured.closures.size, candidate_demand_reuse: measured.demands.size,
      candidate_occurrences: measured.occurrences, pack_candidate_occurrences: measured.pack,
      world_candidate_occurrences: measured.world, distinct_candidate_identities: measured.identities.size,
      ambiguous_identity_matches: measured.ambiguous,
      provenance: sorted(root.provenance || []), states: sorted(root.states || []),
      channel_families: sorted(root.channel_families || []), truth: policy.truth,
      cannot_infer: policy.cannot_infer };
  }).toSorted((a, b) => b.candidate_closure_reuse - a.candidate_closure_reuse ||
    b.candidate_demand_reuse - a.candidate_demand_reuse || b.candidate_occurrences - a.candidate_occurrences ||
    a.root_id.localeCompare(b.root_id));
  const summary = { schema: 'bonfyre.source-pressure-summary.v1', closures_read: closureCount,
    roots_under_pressure: rows.length,
    candidate_occurrences_mapped: rows.reduce((sum, row) => sum + row.candidate_occurrences, 0),
    unmapped_candidate_identities: unmapped.size,
    unmapped_candidate_occurrences: [...unmapped.values()].reduce((sum, n) => sum + n, 0), truth: policy.truth };
  return { rows, summary, unmapped: Object.fromEntries([...unmapped].toSorted(([a], [b]) => a.localeCompare(b))) };
}

if (import.meta.main) {
  try {
    const args = { 'source-input': [], closure: [] }, argv = process.argv.slice(2);
    for (let i = 0; i < argv.length; i += 2) {
      if (!argv[i]?.startsWith('--') || !argv[i + 1]) throw new Error('expected --key value arguments');
      const key = argv[i].slice(2);
      if (key === 'source-input' || key === 'closure') args[key].push(argv[i + 1]);
      else args[key] = argv[i + 1];
    }
    if (!args['source-input'].length || !args.closure.length || !args.roots || !args.channels || !args.out)
      throw new Error('source inputs, Frontier roots and channels, closures and output are required');
    const result = compileSourcePressure(args['source-input'], loadRows(args.channels), loadRows(args.roots), args.closure);
    writeFileSync(args.out, jsonl(result.rows));
    if (args.receipts) writeFileSync(args.receipts,
      JSON.stringify({ ...result.summary, unmapped: result.unmapped }, null, 2) + '\n');
    console.log(JSON.stringify(result.summary));
  } catch (error) { console.error(`source pressure: ${error.message}`); process.exitCode = 2; }
}
