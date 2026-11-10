import { test, expect } from 'bun:test';
import { readFileSync, readdirSync } from 'node:fs';
import { resolve } from 'node:path';

const repo = resolve(import.meta.dir, '../../..');
const main = [resolve(repo, 'cmd/BonfyreWebAddress/src/main.c'), ...readdirSync(resolve(repo, 'cmd/BonfyreWebAddress/src')).filter(n => /\.(c|h)$/.test(n) && n !== 'main.c').sort().map(n => resolve(repo, 'cmd/BonfyreWebAddress/src', n))].map(p => readFileSync(p, 'utf8')).join('\n');
const live = test.skipIf(!process.env.BONFYRE_LIVE_WITNESS);

async function get(path, accept = 'text/html') {
  const response = await fetch(`https://aurekai.live${path}`, {
    headers: { Accept: accept }, signal: AbortSignal.timeout(30_000),
  });
  const text = await response.text();
  return { status: response.status, text,
    json: accept === 'application/json' ? JSON.parse(text) : undefined };
}
let subjectIndex;
async function index() {
  if (!subjectIndex) subjectIndex = get('/subjects?limit=1000', 'application/json')
    .then(({ status, json }) => {
      expect(status).toBe(200);
      expect(json.subjects.length).toBeGreaterThan(0);
      return json.subjects;
    });
  return subjectIndex;
}
async function byKind() {
  const grouped = new Map();
  for (const subject of await index()) {
    if (!grouped.has(subject.kind)) grouped.set(subject.kind, []);
    grouped.get(subject.kind).push(subject.id);
  }
  return grouped;
}
const address = (kind, id) => `/s/${encodeURIComponent(kind)}/${encodeURIComponent(id)}`;
async function aperture(id) {
  const response = await get(`/_aperture?subject=${encodeURIComponent(id)}`, 'application/json');
  expect(response.status).toBe(200);
  return response.json;
}

live('every indexed kind is addressable', async () => {
  for (const [kind, ids] of await byKind()) {
    const answer = await get(address(kind, ids[0]));
    expect(answer.status).toBe(200);
    expect(answer.text).not.toContain('No subject');
    expect(answer.text).not.toContain('not addressable');
  }
});

live('nonexistent subject returns 404', async () => {
  expect((await get(address('Institution', 'institution:no-such-thing'))).status).toBe(404);
});

live('wrong kind for a real subject returns 404', async () => {
  const [kind, ids] = [...await byKind()].find(([kind]) => kind !== 'Event');
  expect((await get(address('Event', ids[0]))).status).toBe(404);
});

live('miss never becomes a soft 404', async () => {
  for (const path of ['/s/Institution/institution:nope', '/s/NotAKind/whatever', '/s/Institution/']) {
    const answer = await get(path);
    expect(answer.status !== 200 || !answer.text.includes('No subject')).toBe(true);
  }
});

live('identifier is validated, not silently rewritten', async () => {
  expect((await get(address('Institution', 'insti tution;drop'))).status).toBe(404);
});

live('canonical subject address is a cheap read', async () => {
  const [kind, ids] = [...await byKind()][0];
  const started = performance.now();
  await get(address(kind, ids[0]));
  const subjectMs = performance.now() - started;
  const surfaceStart = performance.now();
  await get('/_surface/internet-census', 'application/json');
  const surfaceMs = performance.now() - surfaceStart;
  expect(subjectMs).toBeLessThan(3000);
  expect(subjectMs).toBeLessThan(surfaceMs * 4);
});

live('subject page is a bounded identity shell', async () => {
  const grouped = await byKind();
  const kind = [...grouped].sort((a, b) => b[1].length - a[1].length)[0][0];
  const answer = await get(address(kind, grouped.get(kind)[0]));
  expect(answer.status).toBe(200);
  for (const word of ['Kind', 'Identifier', 'Ways in', 'Projected into']) expect(answer.text).toContain(word);
  expect(answer.text.length).toBeLessThan(64 * 1024);
});

live('index and address agree about a subject', async () => {
  const grouped = await byKind();
  const kind = grouped.has('Institution') ? 'Institution' : grouped.keys().next().value;
  const id = grouped.get(kind)[0];
  const listing = await get(`/subjects?id=${encodeURIComponent(id)}`, 'application/json');
  expect(listing.status).toBe(200);
  expect(listing.json.subjects[0].id).toBe(id);
  const page = await get(address(kind, id));
  expect(page.status).toBe(200);
  expect(page.text).toContain(String(listing.json.subjects[0].addresses));
});

live('page and aperture endpoint cannot disagree', async () => {
  let checked = 0;
  for (const [kind, ids] of await byKind()) {
    const id = ids[0];
    const api = await aperture(id);
    const page = await get(address(kind, id));
    expect(page.status).toBe(200);
    if (api.withheld_for_authority) expect(page.text).toContain(`${api.withheld_for_authority} further operation(s)`);
    else expect(page.text).not.toContain('further operation(s)');
    for (const item of api.apertures) expect(page.text).toContain(item.operation);
    checked++;
  }
  expect(checked).toBeGreaterThanOrEqual(5);
});

live('withheld operation is counted rather than hidden', async () => {
  let candidate;
  for (const [kind, ids] of await byKind()) {
    if ((await aperture(ids[0])).withheld_for_authority) { candidate = [kind, ids[0]]; break; }
  }
  expect(candidate).toBeTruthy();
  const page = await get(address(...candidate));
  expect(page.text).toContain('not offered to this session');
  expect(page.text).toContain('Withheld');
});

live('refusal survives a wide subject address list', async () => {
  let widest;
  for (const [kind, ids] of await byKind()) {
    const listing = await get(`/subjects?id=${encodeURIComponent(ids[0])}`, 'application/json');
    const row = listing.json?.subjects?.[0];
    if (row && (!widest || row.addresses > widest.addresses)) widest = { kind, id: ids[0], addresses: row.addresses };
  }
  expect(widest.addresses).toBeGreaterThanOrEqual(10);
  const api = await aperture(widest.id);
  if (!api.withheld_for_authority) return;
  const page = await get(address(widest.kind, widest.id));
  expect(page.text).toContain(`${api.withheld_for_authority} further operation(s)`);
});

live('offered operation links to its form', async () => {
  for (const [kind, ids] of await byKind()) {
    const api = await aperture(ids[0]);
    if (!api.apertures.length) continue;
    const page = await get(address(kind, ids[0]));
    expect(page.text).toContain('/_aperture/form?subject=');
    return;
  }
});

function subjectConditions() {
  const start = main.indexOf('static size_t subject_conditions(');
  expect(start).toBeGreaterThanOrEqual(0);
  const end = main.indexOf('\nstatic ', start);
  return main.slice(start, end);
}

test('closure domains dispatch by authored state, not C domain names', () => {
  const source = subjectConditions();
  for (const token of ['closure_domain', 'requirement_type', 'scope_field', 'state_field',
    'satisfied_state', 'subject_kind']) expect(source).toContain(token);
  for (const hardcoded of ["'capital'", '"capital"', "'feasibility'", '"feasibility"',
    'funding_requirement', 'world_requirement']) expect(source).not.toContain(hardcoded);
});

test('unknown is reported beside unmet rather than folded into it', () => {
  const source = subjectConditions();
  for (const token of ['UNKNOWN', 'nobody has asked about', 'total - met - unknown']) {
    expect(source).toContain(token);
  }
});

live('subject without a closure domain invents no conditions', async () => {
  for (const [kind, ids] of await byKind()) {
    const page = await get(address(kind, ids[0]));
    if (page.text.includes('Waiting on')) expect(page.text).toContain('closed,');
  }
});

live('closure scopes resolve to addressed subjects', async () => {
  const held = new Set((await index()).map(subject => subject.id));
  const expected = ['institution:nightgrid', 'world:nightgrid-friday-900'];
  // The historical pytest witness marked this known deployment gap as xfail.
  // Opt-in live runs surface the gap as a failure rather than silently pass it.
  expect(expected.every(id => held.has(id))).toBe(true);
});
