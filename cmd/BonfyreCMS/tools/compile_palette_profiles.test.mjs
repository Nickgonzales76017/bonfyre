import { test, expect } from 'bun:test';
import { compilePalettes } from './compile_palette_profiles.mjs';

test('eight palette families generate 1,152 distinct reference candidates', () => {
  const rows = compilePalettes();
  expect(rows.length).toBe(1152);
  expect(new Set(rows.map(x => x.template)).size).toBe(rows.length);
  expect(new Set(rows.map(x => x.family)).size).toBe(8);
  expect(rows.every(x => x.status === 'reference_candidate' && x.renderer_mix === 'tokens-only' && x.cannot_infer.includes('not an observed design'))).toBe(true);
  expect(rows.every(x => ['ground','ink','display','accent','muted','rule','panel','face_display','face_body','face_mono','display_size','display_weight','display_transform','display_tracking','display_leading','radius','card_border','card_shadow','gap','measure'].every(role => x.layout_tokens[role]))).toBe(true);
  expect(rows[0]).toMatchObject({ template: 'estate-magazine-rose-still-day', layout_tokens: { ground: 'oklch(0.975 0.008 8)', accent: 'oklch(0.550 0.128 8)' } });
  expect(rows.at(-1).template).toBe('estate-docs-ink-loud-dusk');
});
test('bounded family count preserves ordering without creating authority', () => {
  const rows = compilePalettes(28);
  expect(rows.length).toBe(224);
  expect(rows.every(x => x.cannot_infer.includes('says nothing about the material'))).toBe(true);
});
