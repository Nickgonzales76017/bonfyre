import { test, expect } from 'bun:test';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';

const surfaces = resolve(import.meta.dir, '../../../site/matrix/surfaces');
const descriptor = JSON.parse(readFileSync(resolve(surfaces, 'territory.json'), 'utf8'));
const page = readFileSync(resolve(surfaces, 'territory.html'), 'utf8');
const territories = ['life', 'live', 'shop', 'work'];
const live = test.skipIf(!process.env.BONFYRE_LIVE_WITNESS);

async function get(territory, path) {
  const response = await fetch(`https://aurekai.${territory}${path}`, {
    headers: { Accept: 'text/html' }, signal: AbortSignal.timeout(20_000),
  });
  return { status: response.status, body: await response.text() };
}

function records(html) {
  return [...html.matchAll(/<dl class="surface-record">(.*?)<\/dl>/gs)].map(([, block]) => {
    const keys = [...block.matchAll(/<dt>([^<]*)<\/dt>/g)].map(m => m[1]);
    const values = [...block.matchAll(/<dd>([^<]*)<\/dd>/g)].map(m => m[1]);
    return Object.fromEntries(keys.map((key, i) => [key, values[i]]));
  });
}

let liveByTerritory;
async function observed() {
  if (liveByTerritory) return liveByTerritory;
  liveByTerritory = Object.fromEntries(await Promise.all(territories.map(async territory => {
    const response = await get(territory, `/_surface/night-${territory}`);
    return [territory, { status: response.status, records: records(response.body) }];
  })));
  return liveByTerritory;
}

for (const territory of territories) {
  live(`${territory} catalogue serves the page's pattern`, async () => {
    const response = await get(territory, '/_surface/');
    expect(response.status).toBe(200);
    expect(response.body).toContain(`/_surface/night-${territory}`);
  });
}

test('descriptor names the paths the page fetches', () => {
  expect(page).toContain("'/_surface/' + name");
  for (const [territory, path] of Object.entries(descriptor.reads)) {
    expect(path).toBe(`/_surface/night-${territory}`);
  }
  for (const modes of Object.values(descriptor.modes)) {
    for (const spec of Object.values(modes)) {
      for (const path of spec.reads) expect(page).toContain(path.split('/').at(-1));
    }
  }
});

live('same subject appears across territories with different fields', async () => {
  const readings = await observed();
  const names = Object.values(readings).filter(({ records }) => records.length)
    .map(({ records }) => new Set(records.map(row => row.name)));
  expect(names.length).toBeGreaterThanOrEqual(2);
  const shared = [...names[0]].filter(name => names.every(set => set.has(name)));
  expect(shared.length).toBeGreaterThan(0);
  expect(shared.some(name => name.includes('Foundry'))).toBe(true);
});

live('each territory projects its own field set', async () => {
  const readings = await observed();
  const fields = Object.values(readings).filter(({ records }) => records.length)
    .map(({ records }) => new Set(records.flatMap(row => Object.keys(row))));
  const common = [...fields[0]].filter(field => fields.every(set => set.has(field)));
  expect(common).toEqual(['name']);
  for (const set of fields) expect(set.size).toBeGreaterThan(1);
});

live('observed field sets stay within descriptor', async () => {
  for (const [territory, { records: rows }] of Object.entries(await observed())) {
    for (const field of new Set(rows.flatMap(row => Object.keys(row)))) {
      expect(descriptor.observed_2026_09_04[territory]).toContain(field);
    }
  }
});

live('work is refused, not empty', async () => {
  const work = (await observed()).work;
  expect(work.status).toBe(403);
  expect(work.records).toEqual([]);
  expect(descriptor.observed_2026_09_04.work).toContain('403');
  expect(descriptor.observed_2026_09_04.work).toContain('admitted session');
});

test('refusal and absence remain distinct in declaration and page', () => {
  expect(descriptor.forbidden_inference.join(' ')).toContain('a refused projection -> an absent subject');
  for (const phrase of ['needs an admitted session', 'is empty.', 'k-empty',
    'an empty projection, not a', 'refusal']) expect(page).toContain(phrase);
});

live('life, live and shop answer without a session', async () => {
  for (const territory of ['life', 'live', 'shop']) {
    const answer = (await observed())[territory];
    expect(answer.status).toBe(200);
    expect(answer.records.length).toBeGreaterThan(0);
  }
});

test('page handles statuses htmx will not swap', () => {
  expect(page).not.toContain('hx-get');
  expect(page).toContain('r.status === 401 || r.status === 403');
  expect(page).toContain("kind: 'refused'");
  expect(descriptor.forbidden_inference.some(f => f.includes('a withheld pane -> a broken one'))).toBe(true);
});

test('withheld pane offers the session that can fill it', () => {
  expect(page).toContain('href="/login"');
  expect(page).toContain('Sign in');
});

test('page offers exactly mode verbs declared', () => {
  const block = page.match(/const MODES = \{(.*?)\n  \};/s)?.[1];
  expect(block).toBeTruthy();
  for (const [mode, verbs] of Object.entries(descriptor.mode_verbs)) {
    const escaped = mode.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const found = block.match(new RegExp(`id:'${escaped}'.*?verbs:\\s*\\[([^\\]]*)\\]`, 's'));
    expect(found).toBeTruthy();
    expect([...found[1].matchAll(/'([A-Z]+)'/g)].map(m => m[1])).toEqual(verbs);
  }
});

test('no mode offers verbs outside its territory', () => {
  for (const [territory, modes] of Object.entries(descriptor.modes)) {
    const allowed = new Set(descriptor.territory_verbs[territory]);
    for (const mode of Object.keys(modes)) {
      expect(descriptor.mode_verbs[mode].every(verb => allowed.has(verb))).toBe(true);
    }
  }
});

test('page says verbs are offered, not granted', () => {
  expect(page).toContain('is offered in this mode');
  expect(page).toContain('decided at effect time by the owner of the effect');
  expect(descriptor.forbidden_inference.join(' ')).toContain('an offered verb -> an authorized action');
});

test('page has no submitting action', () => {
  expect(page).not.toContain('hx-post');
  expect(page).not.toContain('<form');
});

test('page derives territory from host, not a per-territory route table', () => {
  for (const territory of ['life', 'shop', 'work']) expect(page).toContain(`endsWith('.${territory}')`);
  expect(page).toContain("'/_surface/' + name");
  for (const territory of territories) expect(page).not.toContain(`/_surface/night-${territory}`);
});

live('every territory host resolves to its territory', async () => {
  for (const territory of territories) {
    const response = await get(territory, '/.well-known/bonfyre');
    expect(response.status).toBe(200);
    const doc = JSON.parse(response.body);
    expect(doc.territory).toBe(territory);
    expect(doc.world).toBe('LIVE');
  }
});

test('life and work have two modes, others one', () => {
  const modes = descriptor.modes;
  expect(Object.keys(modes.life).sort()).toEqual(['household', 'personal']);
  expect(Object.keys(modes.work).sort()).toEqual(['business', 'professional']);
  expect(Object.keys(modes.live)).toHaveLength(1);
  expect(Object.keys(modes.shop)).toHaveLength(1);
  expect(Object.values(modes).reduce((sum, group) => sum + Object.keys(group).length, 0)).toBe(6);
  expect(Object.keys(modes).sort()).toEqual([...territories].sort());
});

test('mode is not a separate product', () => {
  const forbidden = descriptor.forbidden_inference.join(' ');
  expect(forbidden).toContain('a mode -> a separate product');
  expect(forbidden).toContain('rather than a fifth territory');
});

test('page offers each mode its own verbs', () => {
  expect(page).toMatch(/id:'household'.*?verbs:\s*\['PLAN'/s);
  expect(page).toMatch(/id:'business'.*?verbs:\s*\['APPROVE'/s);
  for (const mode of ['personal', 'household', 'tonight', 'market', 'professional', 'business']) {
    expect(page).toContain(`id:'${mode}'`);
  }
});

live('business mode reads patterns actually served', async () => {
  const paths = descriptor.modes.work.business.reads;
  expect(paths.length).toBeGreaterThan(0);
  for (const path of paths) expect(records((await get('work', path)).body).length).toBeGreaterThan(0);
});

test('business mode is ordered by reversibility cost', () => {
  const business = descriptor.modes.work.business;
  expect(business).not.toHaveProperty('planes');
  expect(business.axes).toEqual(['reversible', 'costly', 'irreversible']);
  expect(page).toContain("const REV = ['reversible','costly','irreversible']");
  expect(page).toContain('ordered by what undoing them costs');
  expect(descriptor.forbidden_inference.join(' ')).toContain('an installed app -> a business capability');
});

live('funding records carry cannot_infer', async () => {
  const response = await get('work', '/_surface/funding');
  const rows = records(response.body);
  expect(rows.length).toBeGreaterThan(0);
  expect(rows[0]).toHaveProperty('cannot_infer');
});

test('household is declared and unbuilt', () => {
  const household = descriptor.modes.life.household;
  expect(household.reads).toEqual([]);
  expect(household.state).toContain('unbuilt');
  expect(descriptor.observed_2026_09_04['life.household']).toBe('no pattern served');
});

test('unbuilt mode is not an unbuilt territory', () => {
  for (const phrase of ['Declared and not yet served', 'unbuilt mode ',
    'rather than an unbuilt territory']) expect(page).toContain(phrase);
  expect(descriptor.forbidden_inference.join(' ')).toContain('an unbuilt mode -> an unbuilt territory');
});

test('each pattern has its own attributable pane', () => {
  for (const phrase of ['body.appendChild(K.note(r, mode.label))', 'K.empty(mode.label,',
    "body.appendChild(K.note(routesR,'Capital routes'))"]) expect(page).toContain(phrase);
});
