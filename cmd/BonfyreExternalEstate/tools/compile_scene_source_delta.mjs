#!/usr/bin/env bun
// Compare retained Scene dependency snapshots; temporal co-change is not causality.
import { readFileSync, writeFileSync } from 'node:fs';

const readRows = path => readFileSync(path, 'utf8').split('\n').filter(Boolean).map(JSON.parse);
const writeRows = (path, rows) => writeFileSync(path, rows.map(row => JSON.stringify(row)).join('\n') + (rows.length ? '\n' : ''));
const keyed = path => new Map(readRows(path).filter(row => row && row.scene_key)
  .map(row => [String(row.scene_key), row]));
const setOf = (row, key, field) => new Set((row?.[key] || []).filter(value => value && value[field])
  .map(value => String(value[field])));
const values = (row, key) => new Set((row?.[key] || [])
  .filter(value => ['string', 'number'].includes(typeof value)).map(String));
const sorted = items => [...items].toSorted();
const difference = (left, right) => sorted([...left].filter(value => !right.has(value)));
const union = (left, right) => new Set([...left, ...right]);
const same = (left, right) => left.size === right.size && [...left].every(value => right.has(value));
const counters = () => ({ dependent_before: 0, dependent_after: 0, scenes_changed: 0,
  scene_digest_changed: 0, roots_added: 0, roots_removed: 0, apertures_added: 0,
  apertures_removed: 0, lenses_added: 0, lenses_removed: 0 });

export function compileSceneDelta(beforeRows, afterRows, triggerRoot = '') {
  const before = new Map(beforeRows.filter(row => row && row.scene_key).map(row => [String(row.scene_key), row]));
  const after = new Map(afterRows.filter(row => row && row.scene_key).map(row => [String(row.scene_key), row]));
  const deltas = [], stats = new Map();
  for (const key of sorted(union(new Set(before.keys()), new Set(after.keys())))) {
    const b = before.get(key), c = after.get(key);
    const br = setOf(b, 'source_roots', 'root_id'), ar = setOf(c, 'source_roots', 'root_id');
    const allRoots = union(br, ar);
    if (triggerRoot && !allRoots.has(triggerRoot)) continue;
    const ba = values(b, 'action_apertures'), aa = values(c, 'action_apertures');
    const bl = values(b, 'lenses'), al = values(c, 'lenses');
    const bf = setOf(b, 'foreign_twin_dependencies', 'ref'), af = setOf(c, 'foreign_twin_dependencies', 'ref');
    const born = !b, retired = !c;
    const digestChanged = !!(b && c && b.surface_scene_digest !== c.surface_scene_digest);
    if (!(born || retired || digestChanged || !same(br, ar) || !same(ba, aa) || !same(bl, al) || !same(bf, af))) continue;
    const rootsAdded = difference(ar, br), rootsRemoved = difference(br, ar);
    const aperturesAdded = difference(aa, ba), aperturesRemoved = difference(ba, aa);
    const lensesAdded = difference(al, bl), lensesRemoved = difference(bl, al);
    deltas.push({ schema: 'bonfyre.scene-source-delta.v1', scene_key: key,
      state: born ? 'born' : retired ? 'retired' : 'changed',
      subject_address: (c || b).subject_address, actor_scope: (c || b).actor_scope,
      scene_digest_changed: digestChanged, source_roots_added: rootsAdded, source_roots_removed: rootsRemoved,
      foreign_dependencies_added: difference(af, bf), foreign_dependencies_removed: difference(bf, af),
      action_apertures_added: aperturesAdded, action_apertures_removed: aperturesRemoved,
      lenses_added: lensesAdded, lenses_removed: lensesRemoved, dependent_roots: sorted(allRoots),
      truth: 'temporal_scene_dependency_cochange_not_source_causality',
      cannot_infer: ['dependent source -> caused Scene change', 'new aperture -> source authorized or enabled action',
        'Scene digest change -> visible user-facing difference'] });
    for (const root of allRoots) {
      const counts = stats.get(root) || counters();
      counts.dependent_before += Number(br.has(root));
      counts.dependent_after += Number(ar.has(root));
      counts.scenes_changed++;
      counts.scene_digest_changed += Number(digestChanged);
      counts.roots_added += rootsAdded.length;
      counts.roots_removed += rootsRemoved.length;
      counts.apertures_added += aperturesAdded.length;
      counts.apertures_removed += aperturesRemoved.length;
      counts.lenses_added += lensesAdded.length;
      counts.lenses_removed += lensesRemoved.length;
      stats.set(root, counts);
    }
  }
  const rootRows = [];
  for (const [root_id, counts] of [...stats].toSorted((a, b) =>
    b[1].scenes_changed - a[1].scenes_changed || a[0].localeCompare(b[0]))) {
    rootRows.push(Object.assign({ schema: 'bonfyre.source-scene-cochange.v1', root_id }, counts, {
      truth: 'dependent_scene_cochange_not_source_causality',
      cannot_infer: ['co-change -> causal contribution', 'aperture delta -> source enabled action',
        'high co-change count -> source quality or authority'] }));
  }
  return { deltas, rootRows, summary: { schema: 'bonfyre.scene-source-delta-summary.v1',
    before_scenes: before.size, after_scenes: after.size, changed_scenes: deltas.length,
    roots_with_dependent_cochange: stats.size, trigger_root: triggerRoot,
    truth: 'temporal_scene_dependency_cochange_not_source_causality' } };
}

if (import.meta.main) {
  try {
    const args = {}, argv = process.argv.slice(2);
    for (let i = 0; i < argv.length; i += 2) {
      if (!argv[i]?.startsWith('--') || !argv[i + 1]) throw new Error('expected --key value arguments');
      args[argv[i].slice(2)] = argv[i + 1];
    }
    if (!args.before || !args.after || !args.out) throw new Error('missing --before, --after, or --out');
    const result = compileSceneDelta([...keyed(args.before).values()], [...keyed(args.after).values()], args['trigger-root'] || '');
    writeRows(args.out, result.deltas);
    if (args['root-out']) writeRows(args['root-out'], result.rootRows);
    console.log(JSON.stringify(result.summary));
  } catch (error) { console.error(`scene source delta: ${error.message}`); process.exitCode = 2; }
}
