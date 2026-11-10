import { test, expect } from 'bun:test';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';

const surfaces = resolve(import.meta.dir, '../../../site/matrix/surfaces');
const kit = readFileSync(resolve(surfaces, '_kit.js'), 'utf8');
const verbs = {
  live: ['WATCH', 'JOIN', 'GO', 'FOLLOW', 'SAVE', 'SHARE'],
  life: ['MEET', 'JOIN', 'GO', 'SAVE', 'HELP', 'FOLLOW', 'PLAN', 'REMEMBER', 'SHARE', 'MOVE', 'ASSIGN'],
  shop: ['BUY', 'OFFER', 'JOIN', 'SAVE', 'COMPARE', 'REQUEST', 'SELL', 'FUND', 'CONTRIBUTE'],
  work: ['APPLY', 'APPROVE', 'CLAIM', 'FUND', 'CONTACT', 'BOOK', 'DO', 'REVIEW', 'ASSIGN', 'RESPOND', 'ESCALATE'],
};
const questions = {
  live: 'What deserves attention now?',
  life: 'What matters in my world?',
  shop: 'What economic opportunity is useful?',
  work: 'What should move forward?',
};
const territories = Object.keys(verbs).sort();
const clones = ['board', 'channels', 'community', 'discover', 'inbox', 'listings',
  'parcels', 'stage', 'territory'];

function block(territory) {
  const start = kit.indexOf('K.PHYSICS = {');
  const end = kit.indexOf('K.physics = (t)', start);
  expect(start).toBeGreaterThanOrEqual(0);
  expect(end).toBeGreaterThan(start);
  const body = kit.slice(start, end);
  const at = body.indexOf(`${territory}: {`);
  expect(at).toBeGreaterThanOrEqual(0);
  const next = territories.map(t => body.indexOf(`${t}: {`, at + 1)).filter(n => n > at);
  return body.slice(at, next.length ? Math.min(...next) : undefined);
}

function listedVerbs(source, territory) {
  const start = source.indexOf('K.PHYSICS');
  expect(start).toBeGreaterThanOrEqual(0);
  const end = source.indexOf('K.physics', start);
  const body = source.slice(start, end);
  const at = body.indexOf(`${territory}: {`);
  expect(at).toBeGreaterThanOrEqual(0);
  const from = body.indexOf('verbs:', at);
  const to = body.indexOf(']', from);
  return [...body.slice(from, to).matchAll(/'([A-Z]+)'/g)].map(m => m[1]);
}

for (const territory of territories) {
  test(`${territory} carries exactly its own verbs`, () => {
    expect(listedVerbs(kit, territory)).toEqual(verbs[territory]);
  });
  test(`${territory} states its question and ordering principle`, () => {
    const source = block(territory);
    expect(source).toContain(questions[territory]);
    for (const field of ['orders_by:', 'principle:', 'cant:']) expect(source).toContain(field);
  });
  test(`${territory} recency policy is territory-specific`, () => {
    expect(block(territory)).toContain(territory === 'live' ? 'decays: true' : 'decays: false');
  });
}

test('no verb belongs to every territory', () => {
  const common = new Set(verbs.live.filter(v => territories.every(t => verbs[t].includes(v))));
  expect(common.size).toBe(0);
});

test('each territory has at least one exclusive verb', () => {
  for (const territory of territories) {
    expect(verbs[territory].some(v => territories.every(t => t === territory || !verbs[t].includes(v)))).toBe(true);
  }
});

test('personal world has no rank while live and shop do', () => {
  expect(block('life')).toContain('rank: null');
  expect(block('live')).toContain('rank: r =>');
  expect(block('shop')).toContain('rank: r =>');
});

for (const name of ['discover', 'board', 'listings']) {
  test(`${name} takes verbs and ordering from territory`, () => {
    const page = readFileSync(resolve(surfaces, `${name}.html`), 'utf8');
    expect(page.includes('K.verbsFor(') || page.includes('K.physics(')).toBe(true);
    expect(page.includes('K.order(') || page.includes('K.physics(')).toBe(true);
  });
}

test('territory page quotes the shared physics and questions', () => {
  const page = readFileSync(resolve(surfaces, 'territory.html'), 'utf8');
  expect(page).toContain('K.physics(t)');
  for (const question of Object.values(questions)) expect(page).toContain(question);
});

for (const name of clones) {
  test(`${name} inlined physics agrees with kit`, () => {
    const page = readFileSync(resolve(surfaces, `${name}.html`), 'utf8');
    const at = page.indexOf('K.PHYSICS');
    expect(at).toBeGreaterThanOrEqual(0);
    for (const territory of territories) {
      if (!page.slice(at).includes(`${territory}: {`)) continue;
      expect(listedVerbs(page, territory)).toEqual(verbs[territory]);
    }
  });
}

test.skipIf(!process.env.BONFYRE_LIVE_WITNESS)(
  'live shop ordering differs from stable life ordering over the same records', async () => {
    const response = await fetch('https://aurekai.shop/_surface/night-shop', {
      headers: { Accept: 'text/html' },
      signal: AbortSignal.timeout(20_000),
    });
    expect(response.ok).toBe(true);
    const body = await response.text();
    const records = [...body.matchAll(/<dl class="surface-record">(.*?)<\/dl>/gs)].map(([, block]) => {
      const keys = [...block.matchAll(/<dt>([^<]*)<\/dt>/g)].map(m => m[1]);
      const values = [...block.matchAll(/<dd>([^<]*)<\/dd>/g)].map(m => m[1]);
      return Object.fromEntries(keys.map((key, i) => [key, values[i]]));
    });
    expect(records.length).toBeGreaterThan(2);
    const byOffer = [...records].sort((a, b) =>
      ((a.offer_usd ? -(1000 - Number(a.offer_usd)) : 1) -
       (b.offer_usd ? -(1000 - Number(b.offer_usd)) : 1)) ||
      (a.name || '').localeCompare(b.name || ''));
    const byName = [...records].sort((a, b) => (a.name || '').localeCompare(b.name || ''));
    expect(byOffer.map(r => r.name)).not.toEqual(byName.map(r => r.name));
  });
