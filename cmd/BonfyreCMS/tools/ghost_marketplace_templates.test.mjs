import { test, expect } from 'bun:test';
import { resolve } from 'node:path';
import { compileCensus, compileRow, fingerprintRow } from './ghost_marketplace_templates.mjs';

const census = resolve(import.meta.dir, '../data/ghost-marketplace-2026-09-06.tsv');
test('238 marketplace references lower to distinct candidate templates and fingerprints', () => {
  const { docs, fingerprints } = compileCensus(census);
  expect(docs.length).toBe(238);
  expect(fingerprints.length).toBe(238);
  expect(new Set(fingerprints.map(x=>x.fingerprint_id)).size).toBe(238);
  expect(fingerprints.every(x=>x.status==='reference_candidate'&&x.visual_vector&&x.cannot_infer.includes('semantic claim'))).toBe(true);
  expect(docs.every(x=>x.status==='reference_candidate'&&x.cannot_infer.includes('Ghost backend'))).toBe(true);
  expect(docs[0]).toMatchObject({ template:'ghost-wire', family:'magazine', reference_name:'Wire', reference_source:'ghost-marketplace@2026-09-06' });
});
test('mixed categories keep full provenance but choose magazine grammar first', () => {
  const row = compileRow('Mixed Source', 'Blog,Magazine');
  expect(row.family).toBe('magazine');
  expect(row.reference_categories).toBe('Blog,Magazine');
  expect(fingerprintRow(row).visual_vector.font_href).toBeUndefined();
  expect(row.layout_tokens.font_href).toStartWith('https://fonts.googleapis.com/');
});
