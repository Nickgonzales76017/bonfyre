import { test, expect } from 'bun:test';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { liveSurfaceEnabled, surface, rows } from './live-surface.mjs';

const live = test.skipIf(!liveSurfaceEnabled);

live('single term narrows', async () => {
  const everything = await rows('/_surface/worlds');
  const atlas = await rows('/_surface/worlds?character=atlas');
  expect(atlas.length).toBeGreaterThan(0);
  expect(atlas.length).toBeLessThan(everything.length);
  expect(atlas.every(row => row.character === 'atlas')).toBe(true);
});

live('terms compound', async () => {
  const hot = await rows('/_surface/internet-census?tier=HOT');
  const video = await rows('/_surface/internet-census?kind=video');
  const both = await rows('/_surface/internet-census?tier=HOT&kind=video');
  expect(both.length).toBeLessThanOrEqual(Math.min(hot.length, video.length));
  expect(both.every(row => row.tier === 'HOT' && row.kind === 'video')).toBe(true);
  const three = await rows('/_surface/internet-census?tier=HOT&kind=video&state=observed');
  expect(three.length).toBeLessThanOrEqual(both.length);
});

live('substring reaches a list-valued column', async () => {
  const spatial = await rows('/_surface/worlds?grammars=~spatial');
  expect(spatial.length).toBeGreaterThan(0);
  expect(spatial.every(row => row.grammars.includes('spatial'))).toBe(true);
  const exact = await rows('/_surface/worlds?grammars=spatial');
  expect(exact.length < spatial.length || exact.length === 0).toBe(true);
});

live('a slice never widens', async () => {
  const everything = new Set((await rows('/_surface/internet-census')).map(row => row.source));
  for (const query of ['?tier=HOT', '?kind=video', '?tier=WARM&state=observed', '?label=~spatial']) {
    const got = await rows(`/_surface/internet-census${query}`);
    expect(got.every(row => everything.has(row.source))).toBe(true);
  }
});

live('unknown field is refused with alternatives', async () => {
  const { status, body } = await surface('/_surface/worlds?nope=x');
  expect(status).toBe(400);
  expect(body.error).toBe('unknown_slice');
  expect(body.sliceable).toContain('character');
  expect(body.cannot_infer).toBeTruthy();
});

live('refusal escapes untrusted field', async () => {
  const { status, body } = await surface('/_surface/worlds?%3Cscript%3E=1');
  expect(status).toBe(400);
  expect(JSON.stringify(body)).not.toContain('<script>');
});

test('pattern selector has no hardcoded protocols default', () => {
  const main = readFileSync(resolve(import.meta.dir, '../../../cmd/BonfyreWebAddress/src/main.c'), 'utf8');
  expect(main).not.toContain('snprintf(value, sizeof(value), "%s", "protocols");');
});

live('slice retains the pattern contract', async () => {
  const whole = await surface('/_surface/worlds');
  const part = await surface('/_surface/worlds?character=zine');
  expect(whole.status).toBe(200);
  expect(part.status).toBe(200);
  for (const field of ['pattern', 'effect_class', 'reads', 'extent']) {
    expect(part.body[field]).toEqual(whole.body[field]);
  }
  expect(part.body.count).toBe(part.body.records.length);
});
