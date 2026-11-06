import { test, expect } from 'bun:test';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';

const repo = resolve(import.meta.dir, '../../..');
const surfaces = resolve(repo, 'site/matrix/surfaces');
const kit = readFileSync(resolve(surfaces, '_kit.js'), 'utf8');
const main = readFileSync(resolve(repo, 'cmd/BonfyreWebAddress/src/main.c'), 'utf8');
const extents = ['chip', 'card', 'section', 'page', 'workspace'];
const tiers = ['FLASH', 'CHAT', 'MOMENT', 'SOCIAL', 'SAVED', 'INSTITUTIONAL',
  'EVIDENTIARY', 'ECONOMIC'];
const modes = { live: ['occurrence'], life: ['personal', 'household'],
  shop: ['exchange'], work: ['professional', 'business'] };
const live = test.skipIf(!process.env.BONFYRE_LIVE_WITNESS);

async function get(path, host = 'https://ron.aurekai.work', accept = 'application/json', options = {}) {
  const response = await fetch(`${host}${path}`, {
    ...options, headers: { Accept: accept, ...options.headers },
    signal: AbortSignal.timeout(25_000),
  });
  const text = await response.text();
  return { status: response.status, text,
    json: accept === 'application/json' ? JSON.parse(text) : undefined };
}
function modeVerbs() {
  const body = kit.slice(kit.indexOf('K.MODES = {'), kit.indexOf('K.modes = (t)'));
  return new Map([...body.matchAll(/\{ id: '([a-z]+)', label: '[^']*', of: '([a-z]+)',.*?verbs: \[([^\]]*)\]/gs)]
    .map(([, mode, territory, list]) => [`${territory}/${mode}`,
      [...list.matchAll(/'([A-Z]+)'/g)].map(match => match[1])]));
}
function territoryVerbs(territory) {
  const body = kit.slice(kit.indexOf('K.PHYSICS = {'), kit.indexOf('K.physics = (t)'));
  const at = body.indexOf(`${territory}: {`);
  return [...body.slice(at).split('verbs:')[1].split(']')[0].matchAll(/'([A-Z]+)'/g)]
    .map(match => match[1]);
}

live('person host resolves to that person', async () => {
  const answer = await get('/.well-known/bonfyre');
  expect(answer.status).toBe(200);
  expect(answer.json.host).toBe('ron.aurekai.work');
  expect(answer.json.institution).toBe('person:ron');
  expect(answer.json.canonical_host).toBe('ron.aurekai.work');
});

live('uncreated host still completes TLS', async () => {
  expect([200, 404]).toContain((await get('/.well-known/bonfyre',
    'https://nobody-yet.aurekai.work')).status);
});

live('apex hosts retain certificates', async () => {
  for (const host of ['https://aurekai.work', 'https://aurekai.live',
    'https://theleaflounge.store']) {
    expect((await get('/.well-known/bonfyre', host)).status).toBe(200);
  }
});

live('catalogue declares the extent dial', async () => {
  const { status, json } = await get('/_surface/');
  expect(status).toBe(200);
  expect(json.extents).toEqual(extents);
  expect(json.extent_parameter).toContain('extent');
  expect(json.extent_note).toContain('action layer');
});

for (const [extent, cap] of [['chip', 1], ['card', 3], ['section', 8]]) {
  live(`${extent} caps records and reports withheld rows`, async () => {
    const answer = await get(`/_surface/funding?extent=${extent}`);
    const whole = await get('/_surface/funding');
    expect(answer.status).toBe(200);
    expect(answer.json.extent).toBe(extent);
    expect(answer.json.records.length).toBeLessThanOrEqual(cap);
    if (whole.json.records.length > cap) {
      expect(answer.json.withheld_by_extent).toBe(whole.json.records.length - cap);
    }
  });
  live(`${extent} withholds action layer in the organ`, async () => {
    const answer = await get(`/_surface/funding?extent=${extent}`);
    expect(answer.status).toBe(200);
    expect(answer.json.carries_action_layer).toBe(false);
    expect(answer.json.effect_class).toBe('');
    expect(answer.json.action_layer_withheld_because).toContain('not room to act');
  });
}

live('page extent keeps the action layer', async () => {
  const answer = await get('/_surface/funding?extent=page');
  expect(answer.json.carries_action_layer).toBe(true);
  expect(answer.json.effect_class).toBeTruthy();
});

live('extent narrows and never grants above authored extent', async () => {
  const answer = await get('/_surface/funding?extent=workspace');
  expect(answer.status).toBe(409);
  expect(answer.json.error).toBe('extent_exceeds_authored');
  expect(answer.json.authored).toBe('page');
  expect(answer.json.reason).toContain('cannot grant');
});

live('unknown extent is refused with alternatives', async () => {
  const answer = await get('/_surface/funding?extent=enormous');
  expect(answer.status).toBe(400);
  expect(answer.json.error).toBe('unknown_extent');
  expect(answer.json.extents).toEqual(extents);
});

live('unset extent does not demote a page to card', async () => {
  const answer = await get('/_surface/funding');
  expect(answer.json.carries_action_layer).toBe(true);
  expect(answer.json.extent_authored).toBe('page');
});

test('six modes are declared over four territories', () => {
  const body = kit.slice(kit.indexOf('K.MODES = {'), kit.indexOf('K.modes = (t)'));
  for (const [territory, ids] of Object.entries(modes)) {
    for (const id of ids) expect(body).toContain(`id: '${id}'`);
  }
  expect(Object.values(modes).reduce((sum, list) => sum + list.length, 0)).toBe(6);
});

for (const [key, verbs] of [...modeVerbs()].sort()) {
  test(`${key} verbs stay within its territory`, () => {
    const territory = key.split('/')[0];
    const allowed = new Set(territoryVerbs(territory));
    expect(verbs.every(verb => allowed.has(verb))).toBe(true);
  });
}

for (const territory of Object.keys(modes).sort()) {
  test(`${territory} modes together use all its territory verbs`, () => {
    const union = new Set([...modeVerbs()].filter(([key]) => key.startsWith(`${territory}/`))
      .flatMap(([, verbs]) => verbs));
    expect([...union].sort()).toEqual([...new Set(territoryVerbs(territory))].sort());
  });
}

test('household and business differ from personal and professional verbs', () => {
  const verbs = modeVerbs();
  expect(verbs.get('life/personal')).not.toEqual(verbs.get('life/household'));
  expect(verbs.get('work/professional')).not.toEqual(verbs.get('work/business'));
  expect(verbs.get('life/household')).toContain('ASSIGN');
  expect(verbs.get('work/professional')).not.toContain('APPROVE');
});

test('retention lattice is declared in order', () => {
  const body = kit.slice(kit.indexOf('K.RETENTION = ['), kit.indexOf('K.retentionRank'));
  expect([...body.matchAll(/tier: '([A-Z]+)'/g)].map(match => match[1])).toEqual(tiers);
});

test('pattern tier follows what it reads, not traffic', () => {
  const body = kit.slice(kit.indexOf('K.RETENTION_OF_PATTERN = {'), kit.indexOf('K.retentionOf'));
  for (const [, tier] of body.matchAll(/: '([A-Z]+)'/g)) expect(tiers).toContain(tier);
  expect(body).not.toContain('count');
  expect(body).not.toContain('rank');
});

live('foreign graph is observed edges and not prose', async () => {
  const answer = await get('/foreign/edges');
  expect(answer.status).toBe(200);
  expect(answer.json.schema).toBe('bonfyre-foreign-graph.v1');
  for (const edge of answer.json.edges) {
    for (const field of ['from', 'predicate', 'to', 'carrier', 'observed_at',
      'evidence', 'claim_state']) expect(edge).toHaveProperty(field);
    expect(edge.claim_state).toBe('observed');
  }
});

test('observing a source is an effect owned by ForeignTwin', () => {
  const link = readFileSync(resolve(surfaces, 'link.html'), 'utf8');
  expect(link).toContain('/studio/v1/self/foreign/observe');
  expect(link).toContain('K.observeForeign');
  expect(kit.includes('robots preflight') || kit.includes('robots')).toBe(true);
});

live('observing without a session is refused', async () => {
  const response = await get('/studio/v1/self/foreign/observe', undefined,
    'application/json', { method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ twin_id: 'example', url: 'https://example.com' }) });
  expect([401, 403]).toContain(response.status);
});

test('shape lives in URL and not browser storage', () => {
  expect(kit).toContain('K.shapeToURL');
  expect(kit).toContain('K.shapeFromURL');
  const block = kit.slice(kit.indexOf('K.shapeFromURL'), kit.indexOf('K.nav = (current)'));
  expect(block).not.toContain('localStorage');
  for (const key of ['mode', 'keep', "'x'", "'d'", 'off']) expect(block).toContain(key);
});

test('global extent is one parameter, not one per pattern', () => {
  const block = kit.slice(kit.indexOf('K.shapeToURL'), kit.indexOf('K.nav = (current)'));
  expect(block).toContain("p.set('x', shape.extent)");
  expect(block).toContain("filter(([, v]) => v !== (shape.extent || 'page'))");
});

live('console serves on every host', async () => {
  for (const host of ['https://ron.aurekai.work', 'https://aurekai.work', 'https://aurekai.life']) {
    expect((await get('/console', host, 'text/html')).status).toBe(200);
  }
});

test('console reads organs and names source paths', () => {
  const page = readFileSync(resolve(surfaces, 'console.html'), 'utf8');
  for (const token of ['K.catalogue(', '/subjects', 'K.foreignEdges(', 'K.address(',
    '/foreign/twins', 'K.sourceLine(', 'K.note(', 'K.empty(']) expect(page).toContain(token);
});

live('surface takes withheld action layer from organ', async () => {
  expect(kit).toContain('K.applyExtentToFragments');
  const block = kit.slice(kit.indexOf('K.shapeBar = '), kit.indexOf('K.frame = '));
  expect(block).not.toContain('carries_action_layer');
  expect(block).not.toContain('action_layer');
  const answer = await get('/_surface/funding?extent=card');
  expect(answer.json.carries_action_layer).toBe(false);
  expect(answer.json.action_layer_withheld_because).toBeTruthy();
});

test('estate pages carry the dial', () => {
  expect(kit).toContain('K.shapeBar');
  expect(kit).toContain('K.applyExtentToFragments');
  expect(main).toContain("K.shapeBar('bf-shape')");
  expect(main).toContain('BF_SHAPE_MOUNT');
});

test('dial is absent where nothing can narrow', () => {
  const block = kit.slice(kit.indexOf('K.shapeBar = '), kit.indexOf('K.nav = (current)'));
  expect(block).toContain('if (!first)');
});

live('estate page narrows over its address', async () => {
  const answer = await get('/_surface/doors?page=protocols&extent=card', undefined, 'text/html');
  expect(answer.status).toBe(200);
  expect([...answer.text.matchAll(/class="door"/g)].length).toBeLessThanOrEqual(3);
  expect(answer.text).toContain('surface-withheld');
});
