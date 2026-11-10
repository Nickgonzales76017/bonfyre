import { test, expect } from 'bun:test';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';

const surfaces = resolve(import.meta.dir, '../../../site/matrix/surfaces');
const kit = readFileSync(resolve(surfaces, '_kit.js'), 'utf8');
const listings = readFileSync(resolve(surfaces, 'listings.html'), 'utf8');
const stages = ['Product', 'Listing', 'Inventory', 'AvailabilityObservation', 'Offer',
  'Quote', 'Cart', 'Checkout', 'Commitment', 'Payment', 'Settlement',
  'Fulfilment', 'Delivery', 'Satisfaction', 'Ownership'];
const start = kit.indexOf('K.COMMERCE = [');
const end = kit.indexOf('K.commerceGap', start);
const declaration = kit.slice(start, end);
const facts = [...declaration.matchAll(/\{stage:'([^']+)',\s*owner:'((?:[^'\\]|\\.)*)',\s*knows:'((?:[^'\\]|\\.)*)',\s*cannot:'((?:[^'\\]|\\.)*)'/g)]
  .map(([, stage, owner, knows, cannot]) => ({ stage, owner, knows, cannot }));
const byStage = new Map(facts.map(fact => [fact.stage, fact]));
const pairs = stages.flatMap((a, i) => stages.slice(i + 1).map(b => [a, b]));
const live = test.skipIf(!process.env.BONFYRE_LIVE_WITNESS);

async function shop(path) {
  const response = await fetch(`https://aurekai.shop${path}`, {
    headers: { Accept: 'application/json' }, signal: AbortSignal.timeout(25_000),
  });
  return { status: response.status, body: await response.json() };
}

test('all fifteen commerce facts are declared in order', () => {
  expect(facts.map(fact => fact.stage)).toEqual(stages);
});

for (const stage of stages) {
  test(`${stage} names its owner and the boundary of its knowledge`, () => {
    const fact = byStage.get(stage);
    expect(fact.owner).toBeTruthy();
    expect(fact.knows).toBeTruthy();
    expect(fact.cannot).toBeTruthy();
  });
}

test('no two stages claim identical knowledge', () => {
  expect(new Set(facts.map(fact => fact.knows)).size).toBe(facts.length);
});

test('no two stages share an owner', () => {
  expect(new Set(facts.map(fact => fact.owner)).size).toBe(facts.length);
});

test('all 105 pairwise gaps remain in the test space', () => {
  expect(pairs.length).toBe(105);
});

for (const [a, b] of pairs) {
  test(`${a} does not imply ${b}`, () => {
    const earlier = byStage.get(a);
    const later = byStage.get(b);
    expect(earlier.knows).not.toBe(later.knows);
    expect(earlier.owner).not.toBe(later.owner);
    expect(earlier.cannot).not.toBe(later.cannot);
  });

  test(`${a}/${b} gap is symmetric`, () => {
    expect(kit).toContain('K.commerceGap = (a, b) =>');
    expect(kit).toContain('Math.min(i, j)');
    expect(kit).toContain('Math.max(i, j)');
  });
}

const dangerous = [
  ['Payment', 'Delivery'], ['Delivery', 'Ownership'], ['Payment', 'Ownership'],
  ['Quote', 'Commitment'], ['Listing', 'AvailabilityObservation'],
  ['Offer', 'Settlement'], ['Product', 'Ownership'], ['Checkout', 'Fulfilment'],
];
for (const [a, b] of dangerous) {
  test(`${a}/${b} is a dangerous skip rather than an adjacent step`, () => {
    expect(Math.abs(stages.indexOf(a) - stages.indexOf(b))).toBeGreaterThan(1);
  });
}

test('surface says collapses skip rather than step', () => {
  for (const phrase of ['skip rather than step', 'paid', 'delivered', 'owned']) {
    expect(listings).toContain(phrase);
  }
});

test('surface does not draw a misleading progress bar to ownership', () => {
  expect(listings.includes('not drawn as a progress bar') ||
    listings.includes('drawn as a progress bar to the end')).toBe(true);
});

test('delivery cannot skip fulfilment either', () => {
  expect(listings).toContain('delivery with no fulfilment');
});

test('ownership leaves the store for life', () => {
  expect(listings).toContain('does not stay in the store');
  expect(byStage.get('Ownership').owner).toContain('.life');
});

live('quote is reachable but payment remains gated', async () => {
  const capability = await shop('/capability');
  expect(capability.status).toBe(200);
  expect(capability.body.capabilities.length).toBeGreaterThan(0);
  const first = capability.body.capabilities[0];
  const quote = await shop(`/mpp/quote?capability=${encodeURIComponent(first.name)}`);
  expect(quote.status).toBe(200);
  expect(quote.body.note).toContain('not a reservation');
  const gate = await shop(first.resource);
  expect(gate.status).toBe(402);
  expect(gate.body.settlement.facilitator_configured).toBe(false);
});

live('delivery is empty with an explicit reason', async () => {
  const { status, body } = await shop('/epcis/events');
  expect(status).toBe(200);
  expect(body.epcisBody.eventList).toEqual([]);
  expect(body.empty_reason).toContain('invented');
});

live('estate does not invent inventory or availability', async () => {
  const { status, body } = await shop('/openrtb/bidrequest');
  expect(status).toBe(200);
  expect(body.imp).toEqual([]);
  expect(body.empty_reason).toContain('invented');
});
