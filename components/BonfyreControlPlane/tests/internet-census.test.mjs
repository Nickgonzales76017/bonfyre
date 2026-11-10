import { test, expect } from 'bun:test';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { translationUnit } from './translation-unit.mjs';

const root = join(import.meta.dir, '../../..');
const read = path => readFileSync(join(root, path), 'utf8');
const home = read('site/matrix/surfaces/home.html');
const kit = read('site/matrix/surfaces/_kit.js');
const consolePage = read('site/matrix/surfaces/console.html');
const project = read('components/BonfyreProject/src/main.c');
const web = read('cmd/BonfyreWebAddress/src/main.c');
const service = read('deploy/systemd/bonfyre-reheat.service');
const timer = read('deploy/systemd/bonfyre-reheat.timer');
const host = process.env.BONFYRE_LIVE_BASE_URL ||
  (process.env.BONFYRE_LIVE_WITNESS ? 'https://aurekai.live' : undefined);
const live = Boolean(host);
const liveTest = live ? test : test.skip;
const labels = new Set('hot realtime query bulk web auth action spatial economic evidentiary'.split(' '));
const tiers = new Set(['HOT', 'WARM', 'COLD', 'FROZEN']);
const mediaKinds = new Set(['video', 'image', 'post']);

async function get(path, accept = 'application/json') {
  let response;
  try {
    response = await fetch(new URL(path, host), { headers: { Accept: accept }, signal: AbortSignal.timeout(30_000) });
  } catch (error) {
    // A live run may be offline; never turn absence of network into a false pass.
    throw new Error(`live census unreachable: ${error.message}`);
  }
  return { code: response.status, body: await response.text(), headers: response.headers };
}

async function census() {
  const response = await get('/_surface/internet-census');
  expect(response.code).toBe(200);
  return JSON.parse(response.body).records;
}

async function weather() {
  const response = await get('/foreign/weather');
  expect(response.code).toBe(200);
  return JSON.parse(response.body);
}

function functionBody(source, signature, end = '\nstatic ') {
  const start = source.indexOf(signature);
  expect(start).toBeGreaterThanOrEqual(0);
  const tail = source.slice(start + signature.length);
  const stop = tail.indexOf(end);
  expect(stop).toBeGreaterThanOrEqual(0);
  return signature + tail.slice(0, stop);
}

test('heartbeat delegates whole-census weather to native refresh owner', () => {
  expect(timer).toContain('OnUnitActiveSec=5min');
  expect(timer.toLowerCase()).toMatch(/heartbeat|not the schedule/);
  expect(service).toContain('bonfyre-resource-weather-refresh refresh');
  expect(service).toContain('--project /usr/local/bin/bonfyre-project');
  expect(service).not.toContain('/usr/local/bin/bonfyre-reheat');
  expect(project).toContain('static int foreign_reheat(');
  expect(project).toContain('strcmp(argv[2], "reheat")');
});

test('native reheat preserves authored census and honors refusal and activated demand', () => {
  const body = functionBody(project, 'static int foreign_reheat(', '\nint main(');
  expect(body).toContain('SQLITE_OPEN_READONLY');
  expect(body.toUpperCase().replaceAll('UPDATED', '')).not.toContain('UPDATE');
  expect(body).toContain('strcmp(policy, "robots_disallow")');
  expect(body).toContain('rank = 0');
  expect(body).toContain('not asked again until something changes there');
  expect(body).toContain('pack_activation');
  expect(body).not.toContain('pack_installation');
  const lastReceipt = functionBody(project, 'static int reheat_last_receipt(');
  expect(lastReceipt).toContain('while (fgets');
  expect(lastReceipt).toContain('last');
});

test('named reads do not publish partial whole-census weather', () => {
  expect(project).toContain('if (weather_path && *weather_path && !only && !reheat_only_credential)');
  expect(project).toContain('frozen; a name does not overrule a refusal');
});

test('retained bodies are staged atomically and served from the newest receipt', () => {
  const observe = functionBody(project, 'static int observe_foreign(');
  expect(observe).toContain('settle_retained_body(staged, retain_body_path');
  // Settling still ends in an atomic rename beside the target; an unchanged
  // body leaves the served file alone, and a changed one may be compressed first.
  const settle = functionBody(project, 'static int settle_retained_body(');
  expect(settle).toContain('rename(staged, retained)');
  expect(settle).toContain('strcmp(digest, prior_digest) == 0');
  expect(observe).toContain('.staging');
  expect(observe).not.toContain('/tmp/%s.staging');
  expect(observe).toContain('body_bytes > 0');
  const serve = functionBody(translationUnit(join(root, 'cmd/BonfyreWebAddress/src/main.c')), 'static int serve_foreign_body', 'static int serve_foreign_weather');
  expect(serve).toMatch(/Take the last line|last line/);
  expect(serve).toContain('memmove(receipt, last');
});

test('an absent JSON field clears the output and cannot reuse another twin body', () => {
  const studio = translationUnit(join(root, 'cmd/BonfyreWebAddress/src/main.c'));
  const definition = studio.indexOf('static int studio_json_field(', studio.indexOf('static int studio_json_field(') + 1);
  expect(definition).toBeGreaterThan(0);
  const body = studio.slice(definition, studio.indexOf('\n}\n', definition) + 3);
  expect(body).toContain("out[0] = '\\0'");
});

test('arrival reads the public carrier without retaining a remote body', () => {
  expect(home).not.toContain('/foreign/body');
  expect(home).toContain('/_surface/home-weather');
  expect(home).toContain('Checking for public weather sources.');
  expect(home).not.toContain('asked, and told no');
  expect(home).not.toContain('data-sec="locked"');
  expect(home).not.toContain('id="reels"');
  expect(home).not.toContain('id="wall"');
  expect(home).not.toContain('<img');
  expect(home).not.toContain('thumbnail');
});

test('source extraction contract stays in one interpreter, not the public page', () => {
  expect(kit).toContain('row.extract');
  expect(kit).toContain('row.title_field');
  expect(home).not.toContain('K.readSource');
  for (const shape of ['hits', 'mostread', 'trendingScore', 'features', 'eventList']) {
    const expression = new RegExp(`["']${shape}["']`);
    expect(home).not.toMatch(expression);
    expect(kit).not.toMatch(expression);
  }
  expect(kit).toContain("base.startsWith('@')");
});

test('public page leads with input, uses doors not an internal instrument panel', () => {
  const visible = home.slice(home.indexOf('<body>')).replace(/\/\*[\s\S]*?\*\//g, '').replace(/\bK\.[A-Za-z_]+/g, '');
  for (const word of ['capabilities in the book', 'things it can do', "estate's own parts", 'interaction grammar', 'SurfacePattern', 'pack closure', 'extent']) expect(visible).not.toContain(word);
  expect(home).toContain('Bonfÿre × Aurekai');
  for (const href of ['href="/discover"', 'href="/market"', 'href="/studio/"']) expect(home).toContain(href);
  expect(home).not.toContain('const rail = {');
  expect(home).toContain('action="/_surface/intake"');
  expect(home).toContain('Prepare route');
  expect(home).toContain('does not fetch, publish or claim it.');
  const intake = home.indexOf('action="/_surface/intake"');
  const doors = home.indexOf('class="wrap doors moves"');
  expect(doors).toBeGreaterThan(0);
  expect(intake).toBeLessThan(doors);
});

test('console retains named outside panel and a path to retained bytes', () => {
  expect(consolePage).toContain('const label = (s)');
  expect(consolePage.includes('budgetrequest:01135de0') || consolePage.includes('is a list nobody can use')).toBe(true);
  expect(consolePage).toContain('internet-census');
  expect(consolePage).toContain('/foreign/body?twin=');
});

test('date tokens agree between authored URL reader and browser interpreter', () => {
  for (const token of ['today', 'yesterday', 'today-iso', 'yesterday-iso', 'week-ago', 'week-ago-iso']) {
    expect(kit).toMatch(new RegExp(`\\\\?\\{${token}\\\\?\\}`));
    expect(project).toContain(`{${token}}`);
  }
});

liveTest('census is a mostly unobserved map with a bounded extraction contract', async () => {
  const rows = await census();
  expect(rows.length).toBeGreaterThan(150);
  const observed = rows.filter(r => r.state === 'observed');
  expect(observed.length).toBeGreaterThan(0);
  expect(observed.length).toBeLessThan(rows.length / 3);
  for (const row of rows) {
    for (const field of ['source', 'label', 'says', 'state']) expect(row[field]).toBeTruthy();
    expect(row.label.split(' ').every(label => labels.has(label))).toBe(true);
    const says = row.says.toLowerCase();
    for (const banned of ['surfacepattern', 'foreigntwin', 'closure', 'extent', 'addressable', 'projection']) expect(says).not.toContain(banned);
    expect(says).not.toMatch(/\borgan\b/);
    for (const field of ['url', 'extract']) expect(row[field] ?? '').not.toMatch(/(?<!\d)20\d{2}[-/]\d{2}[-/]\d{2}(?!\d)/);
    if (row.state === 'observed') {
      for (const field of ['twin', 'url', 'title_field']) expect(row[field]).toBeTruthy();
      expect(new Set([...mediaKinds, 'text']).has(row.kind)).toBe(true);
    }
    if (row.state === 'refused') {
      expect(row.tier).toBe('FROZEN');
      expect(row.twin).toBeFalsy();
    }
  }
  expect(rows.some(r => r.state === 'refused')).toBe(true);
  const media = observed.filter(r => mediaKinds.has(r.kind));
  expect(media.length).toBeGreaterThanOrEqual(3);
  for (const row of media) {
    expect(row.image_field).toBeTruthy();
    expect(row.link_field).toBeTruthy();
  }
  const federated = rows.find(r => r.source === 'sepia-videos');
  if (federated) expect(federated.image_base).toStartWith('@');
});

liveTest('published weather derives tier and generally declines to read', async () => {
  const value = await weather();
  expect(value.schema).toBe('bonfyre-resource-weather.v1');
  expect(value.sources.length).toBeGreaterThan(0);
  expect(value).toHaveProperty('read');
  expect(value).toHaveProperty('skipped');
  expect(value.cannot_infer).toContain('never a fresh fact');
  const seen = new Set();
  for (const source of value.sources) {
    expect(tiers.has(source.authored_tier)).toBe(true);
    expect(tiers.has(source.tier)).toBe(true);
    expect(source).toHaveProperty('decision');
    seen.add(source.tier);
  }
  expect(value.sources.some(s => s.decision.startsWith('not due') || s.decision.startsWith('frozen'))).toBe(true);
  expect(seen.has('HOT')).toBe(true);
  expect(seen.has('COLD') || seen.has('WARM')).toBe(true);
  expect(value.sources.filter(s => s.age_seconds < 0)).toEqual([]);
});

liveTest('retained bodies answer by twin with digest and observation time', async () => {
  const rows = (await census()).filter(r => r.state === 'observed');
  expect(rows.length).toBeGreaterThanOrEqual(6);
  const got = [];
  for (const row of rows.slice(0, 6)) {
    const response = await get(`/foreign/body?twin=${encodeURIComponent(row.twin)}`, '*/*');
    expect(response.code).toBe(200);
    expect(response.headers.get('content-type')).toStartWith('text/plain');
    expect(response.body.trim()).not.toBe('');
    const digest = response.headers.get('x-bonfyre-content-sha256');
    expect(digest).toBeTruthy();
    got.push([row.twin, digest, response.body.length]);
  }
  for (let i = 0; i < got.length; i++) for (let j = i + 1; j < got.length; j++) {
    if (got[i][2] === got[j][2]) expect(got[i][1]).toBe(got[j][1]);
  }
  const hot = rows.filter(r => r.tier === 'HOT');
  if (hot.length) {
    const response = await get(`/foreign/body?twin=${encodeURIComponent(hot[0].twin)}`, '*/*');
    expect(response.code).toBe(200);
    expect(response.headers.get('x-bonfyre-observed-at')).toBeTruthy();
  }
});

liveTest('media bodies expose item pictures and console subjects have names', async () => {
  const rows = (await census()).filter(r => r.state === 'observed' && mediaKinds.has(r.kind) && !r.extract.startsWith('rss:'));
  const pick = (node, path) => path.split('.').reduce((value, key) => value?.[key], node);
  let checked = 0;
  for (const row of rows) {
    const response = await get(`/foreign/body?twin=${encodeURIComponent(row.twin)}`, '*/*');
    if (response.code !== 200) continue;
    let doc;
    try { doc = JSON.parse(response.body); } catch { continue; }
    const items = pick(doc, row.extract ?? '');
    if (!Array.isArray(items) || !items.length) continue;
    expect(items.slice(0, 12).some(item => typeof pick(item, row.image_field) === 'string' && pick(item, row.image_field))).toBe(true);
    checked++;
  }
  expect(checked).toBeGreaterThanOrEqual(2);
  const response = await get('/subjects?limit=1000');
  expect(response.code).toBe(200);
  const subjects = JSON.parse(response.body).subjects;
  expect(subjects.filter(s => s.label).length).toBeGreaterThan(subjects.length / 2);
});

liveTest('hot twin remains readable and not older than its reported observation', async () => {
  const hot = (await census()).filter(r => r.state === 'observed' && r.tier === 'HOT').slice(0, 6);
  expect(hot.length).toBeGreaterThan(0);
  for (const row of hot) {
    const response = await get(`/foreign/body?twin=${encodeURIComponent(row.twin)}`, '*/*');
    expect(response.code).toBe(200);
    const served = response.headers.get('x-bonfyre-observed-at');
    if (served && row.last_seen) expect(served >= row.last_seen.slice(0, served.length)).toBe(true);
  }
});
