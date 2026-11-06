#!/usr/bin/env bun
// Loss-accounted compatibility cut from Scene root dependencies to published census IDs.
import { Database } from 'bun:sqlite';
import { writeFileSync } from 'node:fs';
import { canonicalUrl, loadRows, rootAddress } from './source_identity.mjs';

const jsonl = rows => rows.map(row => JSON.stringify(row)).join('\n') + (rows.length ? '\n' : '');

export function compileLegacySceneSources(sceneReceipts, cms, legacyLimit = 32) {
  if (!Number.isInteger(legacyLimit) || legacyLimit < 1) throw new Error('--legacy-limit must be positive');
  const known = new Map();
  if (cms.query("SELECT 1 FROM sqlite_master WHERE type='table' AND name='internet_source'").get()) {
    const rows = cms.query("SELECT source,url,COALESCE(family,'') AS family,COALESCE(kind,'') AS kind,COALESCE(tier,'') AS tier FROM internet_source WHERE status='published' AND COALESCE(url,'')<>'' ORDER BY id").all();
    for (const item of rows) {
      let address;
      try { address = rootAddress({}, canonicalUrl(String(item.url))); } catch { continue; }
      if (!address) continue;
      const choices = known.get(address) || [];
      choices.push({ source: String(item.source), url: String(item.url), family: String(item.family),
        kind: String(item.kind), tier: String(item.tier) });
      known.set(address, choices);
    }
  }
  const result = [], totals = { scenes: 0, matched_sources: 0, emitted_sources: 0,
    dropped_for_legacy_limit: 0, unmatched_roots: 0 };
  for (const scene of sceneReceipts) {
    if (!scene || scene.schema !== 'bonfyre.scene-source-receipt.v1') continue;
    totals.scenes++;
    const matched = [], unmatched = [], seen = new Set();
    for (const root of scene.source_roots || []) {
      if (!root || typeof root !== 'object' || Array.isArray(root)) continue;
      const address = String(root.root_address || ''), choices = known.get(address) || [];
      if (!choices.length) { unmatched.push({ root_id: root.root_id, root_address: address }); continue; }
      for (const item of choices) {
        if (seen.has(item.source)) continue;
        seen.add(item.source);
        matched.push({ ...item, root_id: root.root_id, root_address: address });
      }
    }
    const kept = matched.slice(0, legacyLimit), dropped = matched.slice(legacyLimit);
    totals.matched_sources += matched.length;
    totals.emitted_sources += kept.length;
    totals.dropped_for_legacy_limit += dropped.length;
    totals.unmatched_roots += unmatched.length;
    result.push({ schema: 'bonfyre.scene-legacy-source-adapter.v1', scene_key: scene.scene_key,
      subject_address: scene.subject_address, actor_scope: scene.actor_scope,
      sources: kept.map(item => item.source), source_rows: kept, unmatched_scene_roots: unmatched,
      compatibility_loss: { legacy_limit: legacyLimit, matched_before_limit: matched.length,
        dropped_count: dropped.length, dropped_sources: dropped.map(item => item.source) },
      truth: 'published_census_reopen_of_scene_dependencies_only',
      cannot_infer: ['Scene dependency -> source material is visible',
        'published internet_source -> current availability or freshness',
        'legacy source list -> semantic ranking or complete Scene material',
        'dropped legacy source -> source is unimportant'] });
  }
  return { rows: result, summary: { schema: 'bonfyre.scene-legacy-source-adapter-summary.v1',
    ...totals, truth: 'published_census_reopen_of_scene_dependencies_only' } };
}

if (import.meta.main) {
  let db;
  try {
    const args = {}, argv = process.argv.slice(2);
    for (let i = 0; i < argv.length; i += 2) {
      if (!argv[i]?.startsWith('--') || !argv[i + 1]) throw new Error('expected --key value arguments');
      args[argv[i].slice(2)] = argv[i + 1];
    }
    if (!args['scene-receipts'] || !args['cms-db'] || !args.out)
      throw new Error('scene receipts, CMS DB, and output are required');
    db = new Database(args['cms-db'], { readonly: true });
    const result = compileLegacySceneSources(loadRows(args['scene-receipts']), db,
      Number(args['legacy-limit'] ?? 32));
    writeFileSync(args.out, jsonl(result.rows));
    console.log(JSON.stringify(result.summary));
  } catch (error) { console.error(`scene legacy sources: ${error.message}`); process.exitCode = 2; }
  finally { db?.close(); }
}
