#!/usr/bin/env bun
// Measured reverse-dependency impact, never visible contribution or authority.
import { Database } from 'bun:sqlite';
import { readFileSync, readdirSync, writeFileSync } from 'node:fs';
import { basename, join } from 'node:path';
import { rootForTopic, sourceRoots } from './source_identity.mjs';

const TRUTH = 'reverse_dependency_impact_not_visible_material_contribution';
const FOREIGN = 'ForeignTwin:';
const jsonl = rows => rows.map(row => JSON.stringify(row)).join('\n') + (rows.length ? '\n' : '');
const sorted = values => [...values].toSorted();

export function deltaWrites(path) {
  let body;
  try { body = JSON.parse(readFileSync(path, 'utf8')); } catch { return []; }
  if (body?.schema !== 'bonfyre-fact-delta.v1') return [];
  return Array.isArray(body.writes) ? body.writes.filter(value => typeof value === 'string') : [];
}

export function compileChangeImpact(frontierDb, sceneDb, roots, deltaPaths) {
  const impact = new Map(), unmappedTopics = new Map();
  let factDeltasRead = 0;
  const affected = frontierDb.query('SELECT DISTINCT subject,scope,basis FROM closure_dependency WHERE dependency_ref=? ORDER BY subject,scope,basis');
  let sceneQuery = null;
  if (sceneDb) {
    try {
      if (sceneDb.query("SELECT 1 FROM sqlite_master WHERE type='table' AND name='surface_scene'").get())
        sceneQuery = sceneDb.query("SELECT DISTINCT scene_key FROM surface_scene WHERE status='published' AND subject_address=? AND actor_scope=?");
    } catch { /* An incomplete Scene store has no reusable published Scene evidence. */ }
  }
  for (const path of deltaPaths) {
    const writes = deltaWrites(path);
    if (!writes.length) continue;
    factDeltasRead++;
    for (const write of writes) {
      if (!write.startsWith(FOREIGN)) continue;
      const topic = write.slice(FOREIGN.length), root = rootForTopic(roots, topic);
      if (!root) {
        unmappedTopics.set(topic, (unmappedTopics.get(topic) || 0) + 1);
        continue;
      }
      let row = impact.get(root.root_id);
      if (!row) {
        row = { root, writes: new Set(), topics: new Set(), deltaFiles: new Set(),
          closures: new Set(), subjects: new Set(), scopes: new Set(), bases: new Set(), scenes: new Set() };
        impact.set(root.root_id, row);
      }
      row.writes.add(write);
      row.topics.add(topic);
      row.deltaFiles.add(basename(path));
      const closurePairs = new Map();
      for (const dependency of affected.all(write)) {
        const subject = String(dependency.subject), scope = String(dependency.scope), basis = String(dependency.basis);
        row.closures.add(JSON.stringify([subject, scope, basis]));
        row.subjects.add(subject);
        row.scopes.add(scope);
        row.bases.add(basis);
        closurePairs.set(JSON.stringify([subject, scope]), [subject, scope]);
      }
      if (sceneQuery) for (const [subject, scope] of closurePairs.values())
        for (const scene of sceneQuery.all(subject, scope)) if (scene.scene_key) row.scenes.add(String(scene.scene_key));
    }
  }
  const receipts = [...impact.values()].map(({ root, writes, topics, deltaFiles, closures, subjects, scopes, bases, scenes }) => ({
    schema: 'bonfyre.source-change-impact.v1', root_id: root.root_id, root_address: root.root_address,
    fact_delta_writes: sorted(writes), topics: sorted(topics), delta_files: sorted(deltaFiles),
    affected_subjects: subjects.size, affected_scopes: scopes.size, affected_closures: closures.size,
    affected_scenes: scenes.size, dependency_bases: sorted(bases), provenance: sorted(root.provenance),
    truth: TRUTH,
    cannot_infer: ['affected closure -> visible material changed',
      'affected scene -> source appeared in the rendered extent',
      'dependency impact -> authority, rights, availability or economic value',
      'ForeignTwin observation -> external effect'],
  })).toSorted((a, b) => b.affected_closures - a.affected_closures ||
    b.affected_scenes - a.affected_scenes || a.root_id.localeCompare(b.root_id));
  const measurements = receipts.map(row => ({ root_id: row.root_id, root_address: row.root_address,
    subject_reuse: row.affected_subjects, scene_reuse: row.affected_scenes }))
    .toSorted((a, b) => b.subject_reuse - a.subject_reuse || b.scene_reuse - a.scene_reuse || a.root_id.localeCompare(b.root_id));
  const summary = { schema: 'bonfyre.source-change-impact-summary.v1', fact_deltas_read: factDeltasRead,
    roots_with_measured_impact: receipts.length,
    affected_closures: receipts.reduce((sum, row) => sum + row.affected_closures, 0),
    affected_scenes: receipts.reduce((sum, row) => sum + row.affected_scenes, 0),
    unmapped_topics: unmappedTopics.size,
    unmapped_topic_occurrences: [...unmappedTopics.values()].reduce((sum, n) => sum + n, 0),
    truth: TRUTH };
  return { measurements, receipts, summary };
}

if (import.meta.main) {
  let frontierDb, sceneDb;
  try {
    const args = { 'source-input': [], 'fact-delta': [] }, argv = process.argv.slice(2);
    for (let i = 0; i < argv.length; i += 2) {
      if (!argv[i]?.startsWith('--') || !argv[i + 1]) throw new Error('expected --key value arguments');
      const key = argv[i].slice(2);
      if (key === 'source-input' || key === 'fact-delta') args[key].push(argv[i + 1]);
      else args[key] = argv[i + 1];
    }
    if (!args['source-input'].length || !args['frontier-db'] || !args.out ||
      (!args['fact-delta'].length && !args['fact-delta-dir']))
      throw new Error('source inputs, frontier DB, fact deltas and output are required');
    const paths = [...args['fact-delta']];
    if (args['fact-delta-dir']) paths.push(...readdirSync(args['fact-delta-dir'])
      .filter(name => name.endsWith('.json')).toSorted().map(name => join(args['fact-delta-dir'], name)));
    frontierDb = new Database(args['frontier-db'], { readonly: true });
    if (args['scene-db']) sceneDb = new Database(args['scene-db'], { readonly: true });
    const result = compileChangeImpact(frontierDb, sceneDb, sourceRoots(args['source-input']), paths);
    writeFileSync(args.out, jsonl(result.measurements));
    if (args.receipts) writeFileSync(args.receipts, jsonl(result.receipts));
    console.log(JSON.stringify(result.summary));
  } catch (error) { console.error(`source change impact: ${error.message}`); process.exitCode = 2; }
  finally { frontierDb?.close(); sceneDb?.close(); }
}
