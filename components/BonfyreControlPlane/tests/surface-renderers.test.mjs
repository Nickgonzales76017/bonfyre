import { test, expect } from 'bun:test';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { translationUnit } from './translation-unit.mjs';

const repo = resolve(import.meta.dir, '../../..');
const surfaces = resolve(repo, 'site/matrix/surfaces');
const kit = readFileSync(resolve(surfaces, '_kit.js'), 'utf8');
const main = readFileSync(resolve(repo, 'cmd/BonfyreWebAddress/src/main.c'), 'utf8');
const names = ['vertical', 'stage', 'thread', 'board', 'community', 'market',
  'search', 'spatial', 'work', 'inspector', 'watch'];
const groups = ['social', 'visual', 'economic', 'work', 'inspect'];
const extents = new Set(['chip', 'card', 'section', 'page', 'workspace']);
const live = test.skipIf(!process.env.BONFYRE_LIVE_WITNESS);
const cache = new Map();

async function get(path, host = 'https://ron.aurekai.work', accept = 'application/json') {
  const key = `${host}${path}|${accept}`;
  if (!cache.has(key)) cache.set(key, fetch(`${host}${path}`, {
    headers: { Accept: accept }, signal: AbortSignal.timeout(25_000),
  }).then(async response => ({ status: response.status, text: await response.text(),
    headers: response.headers })));
  return cache.get(key);
}
async function json(path, host) {
  const response = await get(path, host);
  expect(response.status).toBe(200);
  return JSON.parse(response.text);
}
async function registry() { return json('/_surface/renderers'); }

live('renderers are authored in a registry, not compiled into routes', async () => {
  const data = await registry();
  expect(data.reads).toBe('surface_renderer');
  expect(data.records.map(row => row.renderer)).toEqual(names);
});

for (const name of names) {
  live(`${name} declares grammar, seam, renderable kinds and limit`, async () => {
    const row = (await registry()).records.find(row => row.renderer === name);
    expect(row.canonical_name).toBeTruthy();
    expect(row.grammar.length).toBeGreaterThan(40);
    expect(row.seam).toBeTruthy();
    expect(row.renders).toBeTruthy();
    expect(row.cannot_infer).toBeTruthy();
  });
  live(`${name} declares only valid extents`, async () => {
    const row = (await registry()).records.find(row => row.renderer === name);
    const declared = row.extents.split(/\s+/).filter(Boolean);
    expect(declared.length).toBeGreaterThan(0);
    expect(declared.every(extent => extents.has(extent))).toBe(true);
  });
}

live('renderer names no semantic owner', async () => {
  for (const row of (await registry()).records) {
    expect(row).not.toHaveProperty('reads_type');
    expect(row).not.toHaveProperty('reads_organ');
    expect(row.renders).not.toMatch(/\b(bonfyre-[a-z]+|_profile|_twin|night_subject)\b/);
  }
});

live('pattern names no renderer grammar', async () => {
  for (const row of (await json('/_surface/')).surfaces) {
    const text = JSON.stringify(row).toLowerCase();
    for (const grammar of ['masonry', 'keyboard', 'facet rail', 'compare tray', 'theatre']) {
      expect(text).not.toContain(grammar);
    }
  }
});

live('every deployed surface contributed a renderer', async () => {
  const routes = new Set((await registry()).records.map(row => row.extracted_from));
  for (const route of ['/discover', '/stage', '/channels', '/board', '/community',
    '/listings', '/parcels', '/inbox', '/territory']) expect(routes.has(route)).toBe(true);
});

live('parcels contributed both search and spatial renderers', async () => {
  const fromParcels = (await registry()).records.filter(row => row.extracted_from === '/parcels')
    .map(row => row.renderer).sort();
  expect(fromParcels).toEqual(['search', 'spatial']);
});

live('donor lineage survives renderer extraction', async () => {
  const donors = Object.fromEntries((await registry()).records.map(row => [row.renderer, row.donor]));
  const expected = { vertical: 'Pixelfed', stage: 'Mobilizon', thread: 'Zulip',
    board: 'Pinry', community: 'lemmy-ui', market: 'Next.js Commerce',
    spatial: 'MapLibre GL JS', work: 'Plane' };
  for (const [name, donor] of Object.entries(expected)) expect(donors[name]).toBe(donor);
  for (const name of ['search', 'inspector', 'watch']) expect(donors[name]).toBeFalsy();
});

test('kit carries a frame, not just a route', () => {
  expect(kit).toContain('K.frame = ');
  expect(kit).toContain('K.frameURL = ');
  const block = kit.slice(kit.indexOf('K.frame = '), kit.indexOf('K.renderers = '));
  for (const coordinate of ['subject', 'context', 'territory', 'audience', 'extent']) {
    expect(block).toContain(coordinate);
  }
});

test('changing view retains the frame', () => {
  const block = kit.slice(kit.indexOf('K.frameURL = '), kit.indexOf('K._renderers'));
  expect(block).toContain('new URLSearchParams(location.search)');
  expect(block).toContain('route || frame.route');
});

test('drawer groups views by what a person is doing', () => {
  const block = kit.slice(kit.indexOf('K.VIEW_GROUP'), kit.indexOf('K.viewsFor'));
  expect([...block.matchAll(/^\s*\['([a-z]+)',/gm)].map(match => match[1])).toEqual(groups);
});

test('a view lights only where deployed', () => {
  const block = kit.slice(kit.indexOf('K.viewsFor'), kit.indexOf('K.VIEW_CSS'));
  expect(block).toContain('lit: !!route');
  expect(block).toContain('not deployed on this host');
});

test('shared chrome mounts the view control', () => {
  const unit = translationUnit(resolve(repo, 'cmd/BonfyreWebAddress/src/main.c'));
  expect(unit).toContain('BF_VIEW_MOUNT');
  expect(unit).toContain("K.viewControl('bf-view')");
});

live('compiled estate page mounts the view control', async () => {
  const page = await get('/protocols', 'https://aurekai.work', 'text/html');
  expect(page.status).toBe(200);
  expect(page.text).toContain('id="bf-view"');
  expect(page.text).toContain('viewControl');
});

live('subject index answers for every kind', async () => {
  const doc = await json('/subjects?limit=1000');
  expect(doc.schema).toBe('bonfyre-subject-index.v1');
  expect(doc.count).toBeGreaterThan(300);
  expect(doc.kinds.length).toBeGreaterThanOrEqual(10);
  expect(doc.cannot_infer).toContain('presence is a projection');
});

live('subject index is one list rather than ours and theirs', async () => {
  const doc = await json('/subjects?limit=1000');
  expect(doc.kinds).toContain('ForeignTwin');
  expect(doc.kinds).toContain('Institution');
});

live('named subject returns its projections', async () => {
  const subject = (await json('/subjects?id=institution:leaf-lounge')).subjects[0];
  expect(subject.addresses).toBe(subject.address.length);
  expect(subject.presences).toBe(subject.presence.length);
  expect(new Set(subject.presence.map(p => p.territory)).size).toBeGreaterThan(1);
});

live('subject index stays sparse until a subject is named', async () => {
  const listing = await json('/subjects?limit=5');
  expect(listing.subjects.every(row => !('presence' in row))).toBe(true);
});

live('receipts distinguish retained from cooled bytes', async () => {
  const doc = await json('/foreign/receipts');
  expect(doc.count).toBeGreaterThan(0);
  const kept = doc.receipts.filter(row => row.body);
  const cold = doc.receipts.filter(row => !row.body);
  expect(kept.length).toBeGreaterThan(0);
  expect(cold.length).toBeGreaterThan(0);
  for (const row of kept) expect(row.body_href.startsWith('/foreign/body?twin=')).toBe(true);
});

live('retained foreign bytes are never a document on this origin', async () => {
  const receipts = await json('/foreign/receipts');
  const twin = receipts.receipts.find(row => row.body).twin;
  const response = await get(`/foreign/body?twin=${encodeURIComponent(twin)}`, undefined, '*/*');
  expect(response.status).toBe(200);
  expect(response.headers.get('Content-Type')?.startsWith('text/plain')).toBe(true);
  expect(response.headers.get('X-Content-Type-Options')).toBe('nosniff');
  expect(response.headers.get('Content-Security-Policy')).toContain('sandbox');
  expect(response.headers.get('X-Bonfyre-Content-Sha256')).toBeTruthy();
  expect(response.headers.get('X-Bonfyre-Cannot-Infer')).toContain('not a claim');
  expect(response.text).toBeTruthy();
});

for (const attack of ['../../../etc/passwd', '..%2f..%2fetc%2fpasswd', '.ssh/id_rsa',
  'aurekai-auth-md-20260902T023408Z-3f34da0315/../../../etc/hosts']) {
  live(`foreign twin identifier refuses path ${attack}`, async () => {
    const response = await get(`/foreign/body?twin=${encodeURIComponent(attack)}`);
    expect([400, 404]).toContain(response.status);
    expect(response.text).not.toContain('passwd');
    expect(response.text).not.toContain('PRIVATE KEY');
  });
}

test('receipt cannot name arbitrary file', () => {
  const unit = translationUnit(resolve(repo, 'cmd/BonfyreWebAddress/src/main.c'));
  const fn = unit.slice(unit.indexOf('static int serve_foreign_body'), unit.indexOf('static int serve_foreign_twins'));
  expect(fn).toContain('strncmp(bpath, bodies, strlen(bodies))');
  expect(fn).toContain('strstr(bpath, "..")');
});

test('safe-file check is stricter than identifier check', () => {
  const unit = translationUnit(resolve(repo, 'cmd/BonfyreWebAddress/src/main.c'));
  const fn = unit.slice(unit.indexOf('static int foreign_safe_file'), unit.indexOf('/* Retained foreign bytes'));
  expect(fn).toContain("text[i + 1] == '.'");
});

for (const name of ['console', 'link']) {
  live(`${name} surface is served and declared`, async () => {
    const page = await get(`/${name}`, undefined, 'text/html');
    expect(page.status).toBe(200);
    const declaration = JSON.parse(readFileSync(resolve(surfaces, `${name}.json`), 'utf8'));
    expect(extents.has(declaration.extent)).toBe(true);
    expect(declaration.forbidden_inference).toBeTruthy();
    expect(declaration.reads).toBeTruthy();
  });
}

test('console begins with the subject', () => {
  const page = readFileSync(resolve(surfaces, 'console.html'), 'utf8');
  expect(page).toContain('/subjects');
  expect(page.toUpperCase()).toContain('PROJECTION MATRIX');
  expect(page.toLowerCase()).toContain('one subject');
});

test('link desk derives account doors from observed metadata', () => {
  const page = readFileSync(resolve(surfaces, 'link.html'), 'utf8');
  expect(page).toContain('/foreign/receipts');
  expect(page).toContain('authorization_endpoint');
  expect(page.includes('submits nothing from here') || page.includes('submitted from here')).toBe(true);
  expect(page).not.toContain('accounts.google.com');
  expect(page).not.toContain('api.twitter.com');
});
