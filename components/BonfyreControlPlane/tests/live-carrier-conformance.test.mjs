import { test, expect } from 'bun:test';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { translationUnit } from './translation-unit.mjs';
import { nativeDomainProfiles, parcelLiveSurface, parcelLiveRoutes } from '../domain-profile.mjs';

const repo = resolve(import.meta.dir, '../../..');
const live = test.skipIf(!process.env.BONFYRE_LIVE_WITNESS);
const routes = parcelLiveRoutes;
const profile = nativeDomainProfiles.find(row => row.name === 'parcel');
if (!profile) throw new Error('CUE parcel profile not found');
const mappings = Object.fromEntries(profile.projections.map(p => [p.carrier,
  Object.fromEntries(p.fields.map(f => [f.semantic, f.carrier]))]));
async function get(path) {
  const response = await fetch(`${parcelLiveSurface}${path}`, {
    headers: { Accept: 'application/json' }, signal: AbortSignal.timeout(20_000),
  });
  return response.json();
}
function dig(payload, path) { return path.reduce((node, step) => node[step], payload); }
async function samples() {
  return Object.fromEntries(await Promise.all(Object.entries(routes).map(async ([carrier, route]) =>
    [carrier, dig(await get(route.path), route.dig)])));
}
for (const carrier of Object.keys(routes).sort()) {
  live(`${carrier} serves every mapped field`, async () => {
    const served = Object.keys((await samples())[carrier]);
    expect(Object.values(mappings[carrier]).filter(field => !served.includes(field))).toEqual([]);
  });
  live(`${carrier} maps every served field`, async () => {
    const served = Object.keys((await samples())[carrier]);
    expect(served.filter(field => !Object.values(mappings[carrier]).includes(field))).toEqual([]);
  });
}
live('carriers agree on shared semantic facts', async () => {
  const rows = await samples();
  const disagreement = {};
  for (const field of profile.fields.map(f => f.name).sort()) {
    const values = Object.fromEntries(Object.entries(rows).flatMap(([carrier, row]) => {
      const name = mappings[carrier][field];
      return name && name in row ? [[carrier, row[name]]] : [];
    }));
    if (new Set(Object.values(values).map(JSON.stringify)).size > 1) disagreement[field] = values;
  }
  expect(disagreement).toEqual({});
});
live('first row is same parcel across carriers', async () => {
  const rows = await samples();
  expect(new Set([rows.ogc.account, rows.reso.ListingKey, rows.mismo.account]).size).toBe(1);
});
live('live carriers do not yet apply disclosure policy', async () => {
  for (const row of Object.values(await samples())) expect(JSON.stringify(row)).not.toContain('__withheld__');
});
live('MISMO omits owner while RESO carries it', async () => {
  const rows = await samples();
  expect(rows.mismo).not.toHaveProperty('owner');
  expect(rows.mismo).not.toHaveProperty('OwnerName');
  expect(rows.reso.OwnerName).toBeTruthy();
});
live('carrier pages are stable on repeated reads', async () => {
  const reads = [];
  for (let i = 0; i < 3; i++) reads.push(await samples());
  for (const row of reads) expect(new Set([row.ogc.account, row.reso.ListingKey, row.mismo.account]).size).toBe(1);
  expect(new Set(reads.map(row => row.ogc.account)).size).toBe(1);
});
test('projection orders on stored observation_id, not parsed account', () => {
  const source = translationUnit(resolve(repo, 'cmd/BonfyreWebAddress/src/main.c'));
  const start = source.indexOf('static int parcel_projection_sql(');
  expect(start).toBeGreaterThanOrEqual(0);
  const end = source.indexOf('\n}\n', start);
  const fn = source.slice(start, end + 3);
  expect(fn).toContain('ORDER BY observation_id');
  expect(fn).not.toContain('ORDER BY account');
  expect(fn.indexOf('LIMIT') < fn.lastIndexOf('json_extract_string') || fn.includes('FROM (')).toBe(true);
});
live('internal ordering key is not served', async () => {
  for (const row of Object.values(await samples())) expect(row).not.toHaveProperty('observation_id');
});
live('carrier page answers without estate scan', async () => {
  const start = performance.now();
  await get(routes.reso.path);
  expect(performance.now() - start).toBeLessThan(8000);
});
