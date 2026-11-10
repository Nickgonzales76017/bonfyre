import { test, expect } from 'bun:test';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { translationUnit } from './translation-unit.mjs';

const repo = resolve(import.meta.dir, '../../..');
const main = readFileSync(resolve(repo, 'cmd/BonfyreWebAddress/src/main.c'), 'utf8');
const surfaces = resolve(repo, 'site/matrix/surfaces');
const layers = ['official', 'institution', 'community', 'household', 'scenario'];
const verbs = ['hide', 'add', 'specialize', 'present', 'tighten', 'default'];
const live = test.skipIf(!process.env.BONFYRE_LIVE_WITNESS);
const cache = new Map();

async function get(path, host = 'https://ron.aurekai.work') {
  const key = `${host}${path}`;
  if (!cache.has(key)) cache.set(key, fetch(key, {
    headers: { Accept: 'application/json' }, signal: AbortSignal.timeout(25_000),
  }).then(async response => ({ status: response.status, body: await response.json() })));
  return cache.get(key);
}
async function graph() {
  const result = await get('/packs');
  expect(result.status).toBe(200);
  return result.body;
}
async function closure(actor, scenario) {
  const query = new URLSearchParams({ actor });
  if (scenario) query.set('scenario', scenario);
  const result = await get(`/packs/closure?${query}`);
  expect(result.status).toBe(200);
  return result.body;
}

test('adding a product requires no WebAddress native branch', () => {
  const products = ['dating', 'grants', 'grant_readiness', 'nightlife', 'night_out',
    'foodpack', 'lease_search', 'goldwing', 'ailoss'];
  for (const product of products) {
    for (const token of [`strcmp(${product}`, `"${product}")`, `'${product}'`]) {
      let at = -1;
      while ((at = main.indexOf(token, at + 1)) >= 0) {
        const line = main.slice(main.lastIndexOf('\n', at) + 1, main.indexOf('\n', at));
        if (/^\s*(\*|\/\*|\/\/)/.test(line)) continue;
        throw new Error(`native product branch: ${product}: ${line.trim().slice(0, 90)}`);
      }
    }
  }
});

live('community fork depends on parent instead of copying it', async () => {
  const fork = (await graph()).packs.find(pack => pack.pack === 'dfw-goldwing/community');
  expect(fork.forks_from).toBe('bonfyre/motorcycle-community');
  const reached = Object.fromEntries((await closure('community:dfw-goldwing-riders')).packs
    .map(pack => [pack.pack, pack.reached]));
  expect(['activated', 'dependency']).toContain(reached['bonfyre/motorcycle-community']);
  expect(reached['bonfyre/community-base']).toBe('dependency');
  expect(reached['bonfyre/base']).toBe('dependency');
  expect(fork.overlays).toBeLessThanOrEqual(8);
});

live('pack overlay cannot widen native authority', async () => {
  expect((await graph()).overlay_verbs).toEqual(verbs);
  const fn = main.slice(main.indexOf('static int pack_verb_allowed'));
  const listed = [...fn.slice(0, fn.indexOf('\n}')).matchAll(/"([a-z]+)"/g)].map(m => m[1]);
  expect(listed).toEqual(verbs);
  for (const forbidden of ['grant', 'widen', 'authorize', 'elevate', 'trust', 'escalate']) {
    expect(listed).not.toContain(forbidden);
  }
});

test('unknown overlay verb is refused out loud', () => {
  const unit = translationUnit(resolve(repo, 'cmd/BonfyreWebAddress/src/main.c'));
  expect(unit).toContain('refused_count');
  expect(unit.includes('reported as refused') || unit.includes('refused rather than quietly ignored')).toBe(true);
  expect(unit.includes('no verb for granting authority') ||
    unit.includes('granting authority, widening disclosure')).toBe(true);
});

test('tighten narrows and never widens', () => {
  const unit = translationUnit(resolve(repo, 'cmd/BonfyreWebAddress/src/main.c'));
  const fn = unit.slice(unit.indexOf('static int pack_audience_rank'));
  const body = unit.slice(unit.indexOf('!strcmp(verb, "tighten")')).split('} else')[0];
  expect(body).toContain('r < audience_rank');
  const order = [...fn.slice(0, fn.indexOf('\n}')).matchAll(/"([a-z]+)"/g)].map(m => m[1]);
  expect(order[0]).toBe('self');
});

live('internet demand does not mirror bodies', async () => {
  const demand = (await closure('person:ron')).internet_demand;
  expect(demand.length).toBeGreaterThan(0);
  for (const item of demand) {
    expect(item.body_regime).toBe('REMOTE');
    expect(item.reheat_when).toBeTruthy();
  }
});

live('pack is not an identity', async () => {
  const subjects = (await get('/subjects?limit=1000')).body;
  expect(subjects.kinds).not.toContain('Pack');
  expect(subjects.kinds).not.toContain('PackArtifact');
  for (const pack of (await graph()).packs) {
    expect(['person', 'institution', 'community']).toContain(pack.publisher.split(':')[0]);
  }
});

live('scenario needs no institution or install', async () => {
  const scenario = (await graph()).packs.find(pack => pack.pack === 'bonfyre/night-out');
  expect(scenario.kind).toBe('scenario');
  expect(scenario.publisher).toBe('institution:bonfyre:commons');
  expect(scenario.installations).toBe(0);
  expect(scenario.activations).toBe(0);
});

live('scenario configuration does not persist', async () => {
  const plain = await closure('person:ron');
  const withIt = await closure('person:ron', 'bonfyre/night-out');
  expect(withIt.packs.length).toBeGreaterThan(plain.packs.length);
  expect(withIt.packs.some(pack => pack.pack === 'bonfyre/night-out')).toBe(true);
  expect((await closure('person:ron')).packs.some(pack => pack.pack === 'bonfyre/night-out')).toBe(false);
});

live('surface pattern still reads exactly one semantic owner', async () => {
  const catalogue = (await get('/_surface/')).body;
  for (const surface of catalogue.surfaces) {
    expect(surface.reads).toBeTruthy();
    expect(['type', 'organ']).toContain(surface.source);
  }
});

live('host string cannot invent an institution', async () => {
  const bonfyre = await get('/.well-known/bonfyre', 'https://bonfyre.aurekai.work');
  expect(bonfyre.body.institution).toBe('institution:bonfyre:commons');
  const unknown = await get('/.well-known/bonfyre', 'https://nobody-yet.aurekai.work');
  if (unknown.status === 200) expect(unknown.body.institution).not.toBe('institution:nobody-yet');
});

live('enabling behaviour grants no authority', async () => {
  const limit = (await closure('person:ron')).cannot_infer;
  expect(limit).toContain('Authority decides every effect at effect time');
  expect(limit).toContain('grants nothing');
});

live('fork does not silently activate', async () => {
  const packs = (await graph()).packs;
  expect(packs.reduce((sum, pack) => sum + pack.installations, 0))
    .toBeGreaterThan(packs.reduce((sum, pack) => sum + pack.activations, 0));
  expect((await closure('institution:fleming')).packs.some(pack => pack.pack === 'upper90/loss-operations')).toBe(false);
});

live('Ron does not get social surfaces merely because they exist', async () => {
  const answer = await closure('person:ron');
  const views = answer.views.map(view => view.view);
  expect(views).not.toContain('vertical');
  expect(views).not.toContain('stage');
  expect(answer.views_hidden).toContain('vertical');
  expect(answer.views_hidden).toContain('stage');
  expect(views[0]).toBe('work');
  expect(answer.policy.audience_ceiling).toBe('self');
  expect(answer.policy.keep_floor).toBe('SAVED');
});

live('downstream publisher needs no Bonfyre release', async () => {
  const pack = (await graph()).packs.find(entry => entry.pack === 'upper90/loss-operations');
  expect(pack.layer).toBe('institution');
  expect(pack.publisher).toBe('institution:upper90');
  expect(pack.head).toBeTruthy();
});

live('pack refers to shared grammar instead of packaging it', async () => {
  const packs = (await graph()).packs;
  const upper90 = packs.find(pack => pack.pack === 'upper90/loss-operations');
  expect(upper90.dependencies).toBeGreaterThanOrEqual(4);
  expect((await closure('institution:upper90')).packs.some(pack => pack.pack === 'upper90/operations')).toBe(true);
  expect(packs.some(pack => pack.pack.startsWith('frappe/'))).toBe(false);
});

live('community publishes governed behaviour without native change', async () => {
  const fork = (await graph()).packs.find(pack => pack.pack === 'dfw-goldwing/community');
  expect(fork.layer).toBe('community');
  expect(fork.governed_by).toMatch(/approval|quorum|vote/);
  const answer = await closure('community:dfw-goldwing-riders');
  expect(answer.applied_count).toBeGreaterThanOrEqual(3);
  expect(answer.policy.audience_ceiling).toBe('community');
});

live('two actors receive different products from one engine', async () => {
  const ron = await closure('person:ron');
  const club = await closure('community:dfw-goldwing-riders');
  const rv = ron.views.map(view => view.view);
  const cv = club.views.map(view => view.view);
  expect(rv).not.toEqual(cv);
  expect(rv.some(view => !cv.includes(view))).toBe(true);
  expect(cv.some(view => !rv.includes(view))).toBe(true);
  expect(ron.packs.map(pack => pack.pack).sort()).not.toEqual(club.packs.map(pack => pack.pack).sort());
  expect(ron.cannot_infer).toBe(club.cannot_infer);
});

live('five semantic things remain five distinct types', async () => {
  expect((await get('/subjects?limit=1')).body.schema.startsWith('bonfyre-subject')).toBe(true);
  expect((await graph()).schema).toBe('bonfyre-pack-graph.v1');
  expect((await get('/foreign/twins')).body.schema.startsWith('bonfyre.foreign')).toBe(true);
  expect((await get('/_surface/renderers')).body.reads).toBe('surface_renderer');
  expect((await get('/.well-known/bonfyre')).body.institution.startsWith('person:')).toBe(true);
});

for (const layer of layers) {
  live(`${layer} layer has authored material`, async () => {
    expect((await graph()).packs.some(pack => pack.layer === layer)).toBe(true);
  });
}

live('composition order is the authored order', async () => {
  expect((await graph()).layers).toEqual(layers);
  const fn = main.slice(main.indexOf('static int pack_layer_rank')).split('\n}')[0];
  const ranked = [...fn.matchAll(/"([a-z]+)"\)\)\s+return (\d)/g)]
    .map(([, name, rank]) => ({ name, rank: Number(rank) }));
  expect(ranked.sort((a, b) => a.rank - b.rank).map(row => row.name)).toEqual(layers);
});

test('dependency cycle is bounded rather than hung', () => {
  const unit = translationUnit(resolve(repo, 'cmd/BonfyreWebAddress/src/main.c'));
  const fn = unit.slice(unit.indexOf('static int pack_close('));
  expect(fn.slice(0, 600)).toContain('depth > 8');
});

test('pack surface shows what was refused', () => {
  const page = readFileSync(resolve(surfaces, 'packs.html'), 'utf8');
  expect(page).toContain('refused');
  expect(page).toContain('configured something it did not');
  expect(page.includes('hide, add, specialize, present, tighten and default') || page.includes('closed list')).toBe(true);
});

test('view control asks closure and not only deployment', () => {
  const kit = readFileSync(resolve(surfaces, '_kit.js'), 'utf8');
  expect(kit).toContain('K.closure = async');
  const block = kit.slice(kit.indexOf('K.viewsFor = async'), kit.indexOf('K.VIEW_CSS'));
  expect(block).toContain('closure');
  expect(block).toContain('views_hidden');
  expect(block).toContain('no pack in this closure asks for this grammar');
});
