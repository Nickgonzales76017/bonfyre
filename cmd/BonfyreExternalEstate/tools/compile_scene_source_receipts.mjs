#!/usr/bin/env bun
// Dependency-grounded Scene receipts, not render visibility or source authority.
import { Database } from 'bun:sqlite';
import { writeFileSync } from 'node:fs';
import { sourceRoots, rootForTopic } from './source_identity.mjs';

const TRUTH = 'scene_dependency_receipt_not_render_visibility_or_source_authority';
const value = (raw, fallback) => {
  if (raw == null) return fallback;
  if (typeof raw === 'object') return raw;
  try { return JSON.parse(raw); } catch { return fallback; }
};
const stringSet = raw => [...new Set((Array.isArray(raw) ? raw : [])
  .filter(item => ['string', 'number'].includes(typeof item) && String(item))
  .map(String))].toSorted();
const tableExists = (db, name) => !!db.query("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?").get(name);
const jsonl = rows => rows.map(row => JSON.stringify(row)).join('\n') + (rows.length ? '\n' : '');

export function compileSceneReceipts(frontierDb, sceneDb, roots) {
  const scenes = tableExists(sceneDb, 'surface_scene') ? sceneDb.query(
    "SELECT scene_key,subject_address,actor_scope,site_id,world,lenses,operations,dependencies,source_digest FROM surface_scene WHERE status='published' ORDER BY scene_key").all() : [];
  const receipts = [], rootSceneCounts = new Map();
  let scenesWithoutSourceDependency = 0, unresolvedForeignTopics = 0;
  for (const scene of scenes) {
    const sourceRootsForScene = new Map(), foreignDependencies = [], candidateDependencies = [];
    let otherDependencyCount = 0, dependencies = [];
    try {
      dependencies = frontierDb.query('SELECT DISTINCT dependency_ref,basis FROM closure_dependency WHERE subject=? AND scope=? ORDER BY dependency_ref,basis')
        .all(String(scene.subject_address), String(scene.actor_scope));
    } catch { /* A frontier without closure_dependency has no Scene source evidence. */ }
    for (const dependency of dependencies) {
      const ref = String(dependency.dependency_ref), basis = String(dependency.basis);
      if (ref.startsWith('ForeignTwin:')) {
        const root = rootForTopic(roots, ref.slice('ForeignTwin:'.length));
        foreignDependencies.push({ ref, basis });
        if (root) sourceRootsForScene.set(root.root_id, { root_id: root.root_id,
          root_address: root.root_address, provenance: [...root.provenance].toSorted() });
        else unresolvedForeignTopics++;
      } else if (basis === 'external_candidate' || ref.startsWith('external:'))
        candidateDependencies.push({ ref, basis });
      else otherDependencyCount++;
    }
    if (!sourceRootsForScene.size) scenesWithoutSourceDependency++;
    for (const rootId of sourceRootsForScene.keys()) rootSceneCounts.set(rootId, (rootSceneCounts.get(rootId) || 0) + 1);
    const operations = value(scene.operations, {});
    receipts.push({ schema: 'bonfyre.scene-source-receipt.v1', scene_key: String(scene.scene_key),
      subject_address: String(scene.subject_address), actor_scope: String(scene.actor_scope),
      site_id: String(scene.site_id || ''), world: String(scene.world || ''),
      surface_scene_digest: String(scene.source_digest || ''),
      source_roots: [...sourceRootsForScene.values()].toSorted((a, b) => a.root_id.localeCompare(b.root_id)),
      foreign_twin_dependencies: foreignDependencies, external_candidate_dependencies: candidateDependencies,
      other_dependency_count: otherDependencyCount, lenses: stringSet(value(scene.lenses, [])),
      action_apertures: stringSet(operations && typeof operations === 'object' ? operations.apertures : []),
      cms_dependencies: value(scene.dependencies, []), truth: TRUTH,
      cannot_infer: ['source dependency -> source material was visible',
        'candidate dependency -> exact owner accepted the candidate',
        'action aperture -> source enabled or authorized the action',
        'scene dependency -> source authority, rights, price, availability or effect'] });
  }
  const counts = Object.fromEntries([...rootSceneCounts].toSorted(([a], [b]) => a.localeCompare(b)));
  const summary = { schema: 'bonfyre.scene-source-receipt-summary.v1', scenes: receipts.length,
    scenes_with_source_dependencies: receipts.length - scenesWithoutSourceDependency,
    scenes_without_source_dependencies: scenesWithoutSourceDependency,
    distinct_source_roots: rootSceneCounts.size,
    source_root_scene_edges: [...rootSceneCounts.values()].reduce((sum, n) => sum + n, 0),
    unresolved_foreign_topics: unresolvedForeignTopics,
    max_scene_reuse: rootSceneCounts.size ? Math.max(...rootSceneCounts.values()) : 0,
    truth: TRUTH };
  return { receipts, summary, rootSceneCounts: counts };
}

if (import.meta.main) {
  let frontier, scenes;
  try {
    const args = { 'source-input': [] }, argv = process.argv.slice(2);
    for (let i = 0; i < argv.length; i += 2) {
      if (!argv[i]?.startsWith('--') || !argv[i + 1]) throw new Error('expected --key value arguments');
      const key = argv[i].slice(2);
      if (key === 'source-input') args[key].push(argv[i + 1]); else args[key] = argv[i + 1];
    }
    if (!args['source-input'].length || !args['frontier-db'] || !args['scene-db'] || !args.out)
      throw new Error('source inputs, frontier DB, scene DB, and output are required');
    const roots = sourceRoots(args['source-input']);
    frontier = new Database(args['frontier-db'], { readonly: true });
    scenes = new Database(args['scene-db'], { readonly: true });
    const result = compileSceneReceipts(frontier, scenes, roots);
    writeFileSync(args.out, jsonl(result.receipts));
    if (args.summary) writeFileSync(args.summary, JSON.stringify({ ...result.summary,
      root_scene_counts: result.rootSceneCounts }, null, 2) + '\n');
    console.log(JSON.stringify(result.summary));
  } catch (error) { console.error(`scene source receipts: ${error.message}`); process.exitCode = 2; }
  finally { frontier?.close(); scenes?.close(); }
}
