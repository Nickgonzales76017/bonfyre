import { test, expect } from 'bun:test';
import { readFileSync, readdirSync, existsSync, statSync } from 'node:fs';
import { resolve } from 'node:path';

const repo = resolve(import.meta.dir, '../../..');
const matrix = resolve(repo, 'site/matrix');
const surfaces = resolve(matrix, 'surfaces');
const live = test.skipIf(!process.env.BONFYRE_LIVE_WITNESS);
const hosts = ['life', 'live', 'shop', 'work'];
const clones = {
  discover: [['/_surface/night-<territory>'], 'vertical short-video feeds'],
  board: [['/_surface/night-<territory>'], 'visual pin boards'],
  community: [['/foreign/edges', '/observations'], 'federated instance directories'],
  listings: [['/_surface/capabilities', '/capability', '/ucp/catalog', '/acp/products', '/mpp/quote?capability='], 'faceted long-tail listing market'],
  parcels: [['/property/search?q=', '/property/parcel?account=', '/property/market?city=', '/property/act?account='], 'property and local-availability search'],
  inbox: [['/_surface/funding'], 'issue and obligation trackers'],
  channels: [['/events', '/observations', '/_surface/capabilities', '/_surface/federations'], 'stream -> topic team conversation'],
  stage: [['/_surface/night-<territory>', '/_surface/night-events'], 'live-room layout — channel rail, stage, side track, theatre mode'],
};
const names = Object.keys(clones).sort();
const page = name => readFileSync(resolve(surfaces, `${name}.html`), 'utf8');
const descriptor = name => JSON.parse(readFileSync(resolve(surfaces, `${name}.json`), 'utf8'));
const cache = new Map();
async function get(host, path, accept = '*/*') {
  const key = `${host}${path}|${accept}`;
  if (!cache.has(key)) cache.set(key, fetch(`https://aurekai.${host}${path}`, {
    headers: { Accept: accept }, signal: AbortSignal.timeout(20_000),
  }).then(async response => ({ code: response.status, body: await response.text(), headers: response.headers })));
  return cache.get(key);
}
async function json(host, path) {
  const r = await get(host, path);
  expect(r.code).toBe(200);
  return JSON.parse(r.body);
}
function records(body) {
  return [...body.matchAll(/<dl class="surface-record">(.*?)<\/dl>/gs)].map(match =>
    Object.fromEntries([...match[1].matchAll(/<dt>([^<]*)<\/dt>\s*<dd>([^<]*)<\/dd>/gs)].map(m => [m[1], m[2]])));
}
async function nav(host) {
  const { code, body } = await get(host, '/');
  expect(code).toBe(200);
  const match = body.match(/<nav[^>]*>(.*?)<\/nav>/s);
  expect(match).toBeTruthy();
  return Object.fromEntries([...match[1].matchAll(/href="([^"]+)">([^<]+)<\/a>/g)].map(m => [m[2], m[1]]));
}
function probe(path) {
  return path.replace('<territory>', 'live') +
    (path.endsWith('q=') ? 'BELKNAP' : path.endsWith('account=') ? '00000051' :
      path.endsWith('city=') ? '026' : path.endsWith('capability=') ? 'parcel-underwrite' : '');
}

for (const name of names) {
  live(`${name} is served on all four hosts`, async () => {
    for (const host of hosts) {
      const r = await get(host, `/${name}`);
      expect(r.code).toBe(200);
      expect(r.body.toLowerCase()).toContain('<html');
    }
  });
  live(`${name} source endpoints actually answer`, async () => {
    for (const path of clones[name][0]) {
      const p = probe(path);
      const r = await get('live', p);
      if (r.code === 403) expect(JSON.stringify(descriptor(name).observed_2026_09_04 || {})).toContain('403');
      else expect(r.code).toBe(200);
    }
  });
  live(`${name} has inlined kit and no unserved asset`, async () => {
    const r = await get('live', `/${name}`);
    expect(r.body).not.toContain('/surfaces/_kit');
    expect(r.body).toContain('window.K = K');
  });
  test(`${name} distinguishes refusal and empty`, () => {
    const body = page(name);
    expect(body.includes("kind: 'refused'") || body.includes('k-refused')).toBe(true);
    expect(body).toContain('needs an admitted session');
    expect(body).toContain('href="/login"');
    expect(body.includes('empty projection') || body.includes('k-empty')).toBe(true);
  });
  test(`${name} does not submit effects`, () => {
    const body = page(name);
    expect(body).not.toContain('hx-post');
    expect(body.toLowerCase()).not.toContain('method="post"');
    for (const form of body.match(/<form[^>]*>/g) || []) expect(form).toContain('role="search"');
  });
  test(`${name} renders verbs as affordances`, () => {
    expect(page(name)).toContain('k-verb');
    expect(page(name)).toContain('offered here');
  });
  test(`${name} names every path that produced it`, () => {
    const body = page(name);
    expect(body).toContain('k-source');
    for (const path of clones[name][0]) {
      if (path.includes('<territory>')) expect(body.includes("'/_surface/night-'") || body.includes("'/_surface/' + name")).toBe(true);
      else expect(body).toContain(path);
    }
  });
  test(`${name} descriptor matches page contract`, () => {
    const d = descriptor(name);
    expect(d.interaction_target).toBe(clones[name][1]);
    expect(d.reads).toEqual(clones[name][0]);
    expect(d.forbidden_inference.length).toBeGreaterThan(0);
    expect(d.borrowed).toContain('behaviour only');
  });
  live(`${name} is reachable from root navigation`, async () => {
    expect(Object.values(await nav('live'))).toContain(`/${name}`);
  });
}
live('the kit asset really is unserved', async () => expect((await get('live', '/surfaces/_kit.js')).code).toBe(404));
live('work night projection remains gated', async () => expect((await get('work', '/_surface/night-work')).code).toBe(403));
live('one service book crosses all three carriers', async () => {
  const seen = [];
  for (const [path, key] of [['/capability', 'capabilities'], ['/ucp/catalog', 'products'], ['/acp/products', 'products']]) {
    const rows = (await json('live', path))[key];
    seen.push(rows.map(r => r.name || r.descriptor.name).sort());
  }
  expect(seen[0]).toEqual(seen[1]);
  expect(seen[1]).toEqual(seen[2]);
});
live('parcel market spans more than a page', async () => {
  const r = await get('live', '/property/market?city=026');
  expect(r.code).toBe(200);
  const count = r.body.match(/Parcels observed here<\/dt><dd>([\d,]+)<\/dd>/);
  expect(count).toBeTruthy();
  expect(Number(count[1].replaceAll(',', ''))).toBeGreaterThan(100_000);
});
live('channels reads real emitted events', async () => {
  const events = await json('live', '/events');
  expect(events.length).toBeGreaterThan(0);
  for (const e of events) {
    expect(e.specversion).toBe('1.0');
    expect(e.type).toStartWith('org.bonfyre.');
    expect(e.source && e.time).toBeTruthy();
  }
});
live('authored navigation reaches every host', async () => {
  const counts = await Promise.all(hosts.map(async host => Object.keys(await nav(host)).length));
  expect(new Set(counts).size).toBe(1);
  expect(counts[1]).toBeGreaterThanOrEqual(16);
});
live('territory surface is linked', async () => expect(Object.values(await nav('live'))).toContain('/territory'));
live('every navigated path resolves', async () => {
  for (const [label, path] of Object.entries(await nav('live'))) {
    expect([200, 302]).toContain((await get('live', path)).code);
  }
});
live('quote is priced and holds nothing', async () => {
  const q = await json('live', '/mpp/quote?capability=parcel-underwrite');
  expect(q.priced).toBe(true);
  expect(q.amount).toBeTruthy();
  expect(q.currency).toBe('USD');
  expect(q.note).toContain('not a reservation');
});
live('priced resource remains gated', async () => {
  const r = await get('live', '/capability/parcel-underwrite');
  expect(r.code).toBe(402);
  const a = JSON.parse(r.body).accepts[0];
  expect(a.scheme && a.network && a.maxAmountRequired).toBeTruthy();
});
live('gate admits it cannot collect', async () => {
  const s = JSON.parse((await get('live', '/capability/parcel-underwrite')).body).settlement;
  expect(s.facilitator_configured).toBe(false);
  expect(s.note).toContain('refuses rather than treating a payment as settled');
});
live('quote and gate carry the same money', async () => {
  const q = await json('live', '/mpp/quote?capability=parcel-underwrite');
  const g = JSON.parse((await get('live', '/capability/parcel-underwrite')).body).accepts[0];
  expect(Number(g.maxAmountRequired) / 10 ** Number(g.extra.decimals)).toBe(Number(q.amount));
});
live('every priced capability quotes and gates', async () => {
  for (const cap of (await json('live', '/capability')).capabilities.filter(c => c.priced)) {
    const q = await json('live', `/mpp/quote?capability=${cap.name}`);
    expect(Number(q.amount)).toBe(Number(cap.priceUsd));
    expect((await get('live', cap.resource)).code).toBe(402);
  }
});
test('listings page walks quote, gate, and not-for-sale chain', () => {
  const body = page('listings');
  for (const text of ['/mpp/quote?capability=', 'The gate (x402)', 'facilitator_configured', 'not for sale, which is not the same as free']) expect(body).toContain(text);
});

const donors = {
  channels: ['donor-zulip.css', ['narrow-filter', 'subscription_block', 'topic-list-item', 'topic-box', 'recipient_row', 'message_header_stream', 'message_row', 'messagebox', 'message_content', 'left-sidebar-navigation-list', 'new_message_textarea']],
  discover: ['donor-pixelfed.css', ['timeline-status-component', 'card shadow-sm', 'media align-items-center', 'username', 'text-lighter', 'reaction-liked-by', 'force-dark-mode', 'btn btn-light font-weight-bold rounded-pill']],
  community: ['donor-lemmy.css', ['post-listing', 'btn-animate', 'md-div', 'col flex-grow-1', 'col-auto small text-muted ps-1', 'data-bs-theme']],
  board: ['donor-pinry.css', ['pins-container', 'grid pin-masonry', 'pin-card grid-item', 'pin-footer', 'grid-sizer', 'gutter-sizer', 'pin-preview-image', 'pin-info']],
  stage: ['donor-mobilizon.css', ['mbz-card', 'snap-center', 'datetime-container', 'datetime-container-header', 'text-violet-title', 'bg-primary']],
  listings: ['donor-nextcommerce.css', ['line-clamp-2', 'backdrop-blur-md', 'bg-blue-600', 'aspect-square', 'grid-flow-row', 'rounded-full border bg-white/70']],
};
const licences = {
  'donor-zulip.css': 'zulip', 'donor-pixelfed.css': 'pixelfed',
  'donor-pinry.css': 'pinry', 'donor-mobilizon.css': 'mobilizon',
  'donor-nextcommerce.css': 'nextcommerce', 'donor-masonry.js': null,
  'donor-maplibre.css': null, 'donor-maplibre.js': null,
};
for (const name of Object.keys(donors).sort()) {
  const [css, classes] = donors[name];
  live(`${name} loads real donor stylesheet`, async () => {
    expect(page(name)).toContain(`/assets/${css}`);
    const r = await get('live', `/assets/${css}`);
    expect(r.code).toBe(200);
    expect(r.body.length).toBeGreaterThan(10_000);
  });
  test(`${name} uses donor class names`, () => {
    for (const value of classes) expect(page(name)).toContain(value);
  });
  test(`${name} versions donor assets`, () => {
    for (const match of page(name).matchAll(/\/assets\/(donor-[a-z0-9.-]+\.(?:css|js))(\?v=[0-9a-f]+)?/g)) expect(match[2]).toBeTruthy();
  });
  test(`${name} names donor license`, () => expect(page(name).slice(0, 4000)).toContain('DONOR-LICENSES'));
  test(`${name} respects reduced motion`, () => {
    const body = page(name);
    if (body.includes('animation') || body.includes('transition')) expect(body).toContain('prefers-reduced-motion');
  });
  test(`${name} carries one estate mark`, () => {
    expect(page(name)).toContain('K.mountMark(');
    expect(page(name)).not.toContain('BONF&Yuml;RE</a>');
  });
}
for (const [asset, licence] of Object.entries(licences).sort()) {
  test(`${asset} retains attribution and vendored license`, () => {
    const assetPath = resolve(matrix, asset);
    expect(existsSync(assetPath)).toBe(true);
    expect(readFileSync(assetPath, 'utf8').slice(0, 2000)).toStartWith('/*!');
    if (!licence) return;
    const dir = resolve(matrix, 'DONOR-LICENSES', licence);
    expect(statSync(dir).isDirectory()).toBe(true);
    const files = readdirSync(dir);
    expect(files.length).toBeGreaterThan(0);
    expect(files.some(file => file.startsWith('LICENSE') || file === 'COPYING')).toBe(true);
    expect(files).toContain('COMMIT');
  });
}
test('business mode is a capital plane, not an app inventory', () => {
  const body = page('territory');
  for (const word of ['Capital reachability', 'irreversible', 'reversibility']) expect(body).toContain(word);
  expect(body.includes('capacity_state') || body.includes('cap-EVIDENCED')).toBe(true);
  expect(body).not.toContain('frappe-apps');
  expect(body.replace('<!doctype html>', '')).not.toContain('doctype');
});
live('capital plane reads deployed routes, transformations, funding', async () => {
  for (const name of ['capital-routes', 'capital-transformations', 'funding']) {
    const r = await get('work', `/_surface/${name}`);
    expect(r.code).toBe(200);
    expect(r.body).toContain('surface-record');
  }
});
live('transformations carry reversibility and horizon', async () => {
  const rows = records((await get('work', '/_surface/capital-transformations')).body);
  expect(rows.length).toBeGreaterThanOrEqual(10);
  const seen = new Set();
  for (const row of rows) {
    expect(row.reversibility).toBeTruthy();
    expect(row.duration_months).not.toBe('');
    seen.add(row.reversibility);
  }
  for (const posture of ['reversible', 'costly', 'irreversible']) expect(seen.has(posture)).toBe(true);
});
live('routes distinguish unknown and evidenced capacity', async () => {
  const states = new Set(records((await get('work', '/_surface/capital-routes')).body).map(r => r.capacity_state));
  expect(states.has('UNKNOWN')).toBe(true);
  expect(states.has('EVIDENCED')).toBe(true);
});
live('community reads federation rather than scoreboard', async () => {
  const body = page('community');
  expect(body).toContain('federations');
  expect(body.toLowerCase()).toContain('depth');
  expect((await get('live', '/_surface/federations')).body.includes('publishes here') || body.includes('publishes here')).toBe(true);
});
live('channels keeps four stream outcomes apart', async () => {
  const streams = descriptor('channels').streams;
  expect(Object.keys(streams).sort()).toEqual(['capabilities', 'federation', 'occurrences', 'sources']);
  for (const path of Object.values(streams)) expect((await get('live', path)).code).toBe(200);
  for (const line of ['K.note(res, s.id)', 'refused, needs a session', 'needs an admitted session']) expect(page('channels')).toContain(line);
});
test('channels marks topic read, not handled', () => {
  const body = page('channels');
  expect(body).toContain('bonfyre_channels_seen');
  expect(body).toContain('new since you last opened this topic');
  expect(descriptor('channels').forbidden_inference.join(' ')).toContain('a read topic -> a handled one');
});
live('source stream admits absent timestamps', async () => {
  const rows = (await json('live', '/observations')).conflicts;
  expect(rows.length).toBeGreaterThan(0);
  expect(rows[0]).not.toHaveProperty('time');
  expect(rows[0]).not.toHaveProperty('observed_at');
  expect(page('channels')).toContain('no time recorded');
});
live('stage reads room records and status fields', async () => {
  const r = await get('live', '/_surface/night-live');
  expect(r.code).toBe(200);
  const rows = records(r.body);
  expect(rows.length).toBeGreaterThanOrEqual(4);
  const fields = new Set(rows.flatMap(r => Object.keys(r)));
  for (const field of ['name', 'tonight_status', 'capacity_note', 'safety_note']) expect(fields.has(field)).toBe(true);
});
test('stage says no video, status not pixels', () => {
  expect(page('stage')).toContain('No video');
  expect(page('stage')).toContain('publishes status, not pixels');
  expect(descriptor('stage').forbidden_inference.join(' ')).toContain('a stage -> a video stream');
});
live('fill meter comes from room sentence', async () => {
  expect((await get('live', '/_surface/night-live')).body).toContain('percent');
  for (const line of ['percent/i', 'Fill the room reported', 'Described, not counted']) expect(page('stage')).toContain(line);
});
live('stage schedule is gated and says so', async () => {
  expect((await get('live', '/_surface/night-events')).code).toBe(403);
  expect(page('stage')).toContain('a refusal from one pattern');
});
live('also tonight derives from records naming each other', async () => {
  const rows = records((await get('live', '/_surface/night-live')).body);
  const names = new Set(rows.map(r => r.name));
  expect(rows.some(r => [...names].some(n => n !== r.name && (r.tonight_status || '').includes(n)))).toBe(true);
  expect(page('stage')).toContain("(x.tonight_status||'').includes(r.name)");
});
live('long-tail book is larger than priced offers', async () => {
  const book = records((await get('shop', '/_surface/capabilities')).body);
  const offers = (await json('shop', '/capability')).capabilities;
  expect(book.length).toBeGreaterThan(50);
  expect(offers.length).toBeGreaterThan(0);
  expect(offers.length).toBeLessThan(book.length);
  expect(page('listings')).toContain('no offer');
  expect(page('listings')).toContain('not the same as free');
});
live('three commerce carriers show one price', async () => {
  const body = page('listings');
  expect(body).toContain('carriers disagree about the money');
  expect(body).toContain('three carriers, one price');
  const prices = new Map();
  for (const [path, key] of [['/capability', 'capabilities'], ['/ucp/catalog', 'products'], ['/acp/products', 'products']]) {
    for (const item of (await json('shop', path))[key]) {
      const name = item.name || item.id;
      const amount = Number(item.priceUsd ?? item.price.value);
      if (!prices.has(name)) prices.set(name, new Set());
      prices.get(name).add(amount.toFixed(6));
    }
  }
  for (const values of prices.values()) expect(values.size).toBe(1);
});
live('market path uses the search-returned city code', async () => {
  const body = (await get('shop', '/property/search?q=elm')).body;
  expect([...body.matchAll(/city (\w+)/g)].length).toBeGreaterThan(0);
  expect(page('parcels')).toContain("'/property/market?city='+encodeURIComponent(r.city)");
});
test('computed parcel numbers name their measured set', () => {
  const body = page('parcels');
  for (const line of ['Median, these rows', 'not across the roll', 'A rank inside one query is not a rank in the']) expect(body).toContain(line);
  expect(descriptor('parcels').forbidden_inference.join(' ')).toContain('a median across these results -> the market');
});
test('a saved parcel makes no external claim', () => {
  const body = page('parcels');
  expect(body).toContain('the right size in the wrong place, on purpose');
  expect(body).toContain('no coordinates');
  expect(descriptor('parcels').forbidden_inference.join(' ')).toContain('a saved parcel -> a claim');
});

async function estate() { return json('live', '/.well-known/bonfyre/estate'); }
live('host names institution that holds it', async () => {
  const doc = await json('live', '/.well-known/bonfyre');
  expect(doc.institution).toBeTruthy();
  expect(doc.territory && doc.world && doc.audience).toBeTruthy();
});
live('institution is one subject across territories', async () => {
  const byInstitution = Map.groupBy((await estate()).hosts, h => h.institution);
  const multi = [...byInstitution.values()].filter(rows => rows.length > 1);
  expect(multi.length).toBeGreaterThan(0);
  for (const rows of multi) expect(new Set(rows.map(h => h.territory)).size).toBe(rows.length);
});
live('institution may hold a domain hostname does not reveal', async () => {
  const hosts = (await estate()).hosts;
  const owned = hosts.filter(h => !['live', 'life', 'shop', 'work'].some(t => h.host.endsWith(`.aurekai.${t}`)) && !h.host.startsWith('aurekai.'));
  expect(owned.length).toBeGreaterThan(0);
  expect(hosts.filter(h => h.institution === owned[0].institution).length).toBeGreaterThan(1);
});
live('address document permits cross-territory reading', async () => {
  const response = await fetch('https://aurekai.live/.well-known/bonfyre', {
    headers: { Origin: 'https://aurekai.work' }, signal: AbortSignal.timeout(20_000),
  });
  expect(response.headers.get('Access-Control-Allow-Origin')).toBe('*');
});
live('clone links only to addresses estate carries', async () => {
  const priced = (await json('shop', '/capability')).capabilities.map(c => c.name);
  const addressed = await get('shop', `/s/capability/${priced[0]}`);
  expect(addressed.code).toBe(200);
  expect(addressed.body.slice(0, 2000).toLowerCase()).not.toContain('not found');
  const book = records((await get('shop', '/_surface/capabilities')).body);
  const unaddressed = book.find(row => !priced.includes(row.id));
  if (unaddressed) expect((await get('shop', `/s/capability/${unaddressed.id}`)).body.slice(0, 2000).toLowerCase()).toContain('not found');
  for (const name of ['listings', 'channels']) expect(page(name)).toContain('no address on this estate');
});
for (const name of ['listings', 'parcels', 'channels']) {
  test(`${name} is a representation one can leave`, () => {
    const body = page(name);
    expect(body).toContain('K.subject(');
    expect(body).toContain('K.hostSwitch(');
  });
}
test('published kit equals inlined source kit', () => {
  expect(readFileSync(resolve(matrix, 'bonfyre-kit.js'), 'utf8')).toBe(readFileSync(resolve(surfaces, '_kit.js'), 'utf8'));
});
live('kit is served to compiled estate', async () => {
  const r = await get('live', '/assets/bonfyre-kit.js');
  expect(r.code).toBe(200);
  expect(r.body).toContain('K.mountMark');
  expect(r.body).toContain('K.PHYSICS');
});
live('knight image is served', async () => {
  const r = await get('live', '/assets/aurekai-knight.png');
  expect(r.code).toBe(200);
  expect(r.body.length).toBeGreaterThan(1000);
});
live('compiled estate carries mark', async () => {
  const r = await get('live', '/protocols', 'text/html');
  expect(r.code).toBe(200);
  for (const text of ['id="bf-mk"', '/assets/bonfyre-kit.js', '.wordmark']) expect(r.body).toContain(text);
});
live('landing page carries mark without duplicate wordmark', async () => {
  const r = await get('live', '/');
  expect(r.code).toBe(200);
  expect(r.body).toContain('id="bf-mk"');
  expect(r.body).toContain('/assets/bonfyre-kit.js');
  expect(r.body.replace('BONF&Yuml;RE', '')).not.toContain('BONF');
});
