#!/usr/bin/env bun
// Compile Clang literal candidates into draft-only CMS migration records.
import { readFileSync, writeFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { createHash } from 'node:crypto';

const hash = (...parts) => {
  const digest = createHash('sha256');
  for (const part of parts) digest.update(String(part)).update('\0');
  return digest.digest('hex');
};
const atom = (value, limit = 96) => (String(value).trim().replace(/[^A-Za-z0-9._:-]+/g, '-').replace(/^-+|-+$/g, '') || 'fragment').slice(0, limit);
const normalized = value => String(value).trim().replace(/\s+/gu, ' ');

export function compilePlan(manifest, sourceMap, paths, evidenceRef = '') {
  const mappings = sourceMap.mappings || {};
  const reviewRequired = new Set(sourceMap.review_required || []);
  const drafts = [], review = [], equivalences = new Map();
  for (const candidate of manifest.candidates || []) {
    const nodes = candidate.text_nodes || [];
    if (!nodes.length) continue;
    const fn = candidate.function || 'global';
    const mapping = mappings[fn];
    if (!mapping || reviewRequired.has(fn)) {
      for (const [index, text] of nodes.entries()) review.push({ function: fn, text, node_index: index, source_ref: candidate.source_ref, source_digest: candidate.source_digest, reason: 'function requires exact page/slot/audience review' });
      continue;
    }
    const page = atom(mapping.page), audience = atom(mapping.audience), slot = atom(mapping.slot || 'legacy-copy');
    for (const [index, text] of nodes.entries()) {
      const body = normalized(text);
      if (!body) continue;
      const equivalence = hash('surface-copy', body.toLowerCase());
      equivalences.set(equivalence, (equivalences.get(equivalence) || 0) + 1);
      const sourceRef = `${candidate.source_ref}:text:${index}`;
      drafts.push({
        coordinate: [page, slot, audience, 'legacy', atom(candidate.candidate_id || 'candidate', 32), index].join('.'),
        page, slot, audience, kind: 'copy', position: Number(candidate.line || 0) * 10 + index,
        body, status: 'draft', variant_key: 'legacy-compiled',
        semantic_tags: ['migration', 'compiled-literal', `function:${fn}`, `role:${candidate.role}`, `equivalence:${equivalence.slice(0, 16)}`],
        projection_contract: { carriers: ['html', 'htmx'], fallback: 'compiled' },
        source_ref: sourceRef, source_digest: hash(candidate.source_digest || '', body), evidence_ref: evidenceRef || null,
        notes: 'migration candidate only; compiled fallback remains authoritative until exact projection and publication witnesses pass',
      });
    }
  }
  const duplicates = [...equivalences.values()].filter(count => count > 1);
  return {
    schema_version: 'bonfyre.surface.fragment-migration-plan.v1',
    source_manifest: paths.manifest, source_manifest_sha256: hash(paths.manifestText), source_map: paths.map,
    draft_count: drafts.length, review_count: review.length, equivalence_groups: equivalences.size,
    duplicate_equivalence_groups: duplicates.length, duplicate_occurrences: duplicates.reduce((a, b) => a + b, 0),
    drafts, review,
    promotion_law: ['draft admission is not publication', 'compiled fallback remains until exact SurfaceIR projection passes', 'ANN/equivalence may propose reuse but cannot satisfy an exact coordinate', 'removing a compiled fallback requires a separate verified source patch'],
  };
}

export function main(args = process.argv.slice(2)) {
  const evidenceIndex = args.indexOf('--evidence-ref');
  const evidenceRef = evidenceIndex >= 0 ? args.splice(evidenceIndex, 2)[1] : '';
  if (args.length !== 3) throw new Error('usage: plan_fragment_migration.mjs LITERALS.json SOURCE_MAP.json OUTPUT.json [--evidence-ref REF]');
  const [manifestPath, mapPath, output] = args.map(resolve);
  const manifestText = readFileSync(manifestPath, 'utf8');
  const plan = compilePlan(JSON.parse(manifestText), JSON.parse(readFileSync(mapPath, 'utf8')), { manifest: manifestPath, manifestText, map: mapPath }, evidenceRef);
  writeFileSync(output, JSON.stringify(plan, null, 2) + '\n');
  console.log(JSON.stringify(Object.fromEntries(['draft_count', 'review_count', 'equivalence_groups', 'duplicate_equivalence_groups', 'duplicate_occurrences'].map(key => [key, plan[key]]))));
  return 0;
}

if (import.meta.main) {
  try { process.exitCode = main(); }
  catch (error) { console.error(error.message); process.exitCode = 1; }
}
