#!/usr/bin/env bun
// Cross-protocol facts and machine-web shape; explicit target and optional WebSub effect.
import { createHmac } from 'node:crypto';
import { options, circuit } from './client.mjs';

export const priceMap = (document, beckn = false) => {
  const rows = beckn ? document?.message?.catalog?.['bpp/providers']?.[0]?.items : document?.products;
  return Object.fromEntries((rows || []).map(row => {
    const value = row.price?.value ?? row.price;
    return [row.id || row.name, Number(value)];
  }));
};
const text = response => typeof response.data === 'string' ? response.data : response.text;

export async function run(config, transport = circuit(config)) {
  if (!config.parcel) throw new Error('--parcel is required for cross-protocol subject assertions');
  const { http, check, skip, finish } = transport;
  const catalogue = await http('/capability');
  const priced = (catalogue.data?.capabilities || []).filter(row => row.priced);
  const expected = Object.fromEntries(priced.map(row => [row.name, Number(row.priceUsd)]));
  check('priced catalogue is nonempty', priced.length > 0);
  for (const [name, path, beckn] of [['UCP', '/ucp/catalog', false], ['ACP', '/acp/products', false], ['Beckn', '/beckn/search', true]]) {
    const response = await http(path), observed = priceMap(response.data, beckn);
    const mismatches = Object.keys(expected).filter(key => key in observed && Math.abs(observed[key] - expected[key]) > 1e-9);
    check(`${name} prices match the x402 catalogue`, response.status === 200 && mismatches.length === 0, `${priced.length} priced, diverged: ${mismatches}`);
  }
  for (const row of priced) {
    const quote = await http(`/mpp/quote?capability=${encodeURIComponent(row.name)}`);
    check(`MPP quote matches ${row.name}`, quote.status === 200 && Math.abs(Number(quote.data?.amount) - Number(row.priceUsd)) <= 1e-9);
  }

  const ogc = await http('/ogc/collections/parcels/items?limit=1');
  const reso = await http('/reso/Property?limit=1');
  check('OGC and RESO answer over the same source', ogc.data?.features_source === 'src:tad-parcel-view' && Array.isArray(reso.data?.value));
  for (const count of [100, 900]) {
    const start = performance.now(), response = await http(`/reso/Property?limit=${count}`, { timeout: 600_000 });
    check(`RESO serves ${count} rows`, response.status === 200 && response.data?.value?.length === count, `${response.data?.value?.length || 0} rows in ${((performance.now() - start) / 1000).toFixed(1)}s`);
  }
  const oversized = await http('/reso/Property?limit=1000', { timeout: 600_000 });
  check('oversized page refuses instead of truncating', oversized.status === 413 && oversized.data?.error === 'page_too_large');

  const shapes = {
    '/ap/actor': data => data?.type === 'Service' && '@context' in data,
    '/.well-known/agent.json': data => Array.isArray(data?.skills),
    '/wot/thing': data => String(data?.['@context'] || '').startsWith('https://www.w3.org/2022/wot'),
    '/epcis/events': data => data?.type === 'EPCISDocument',
    '/cap/alerts': data => data?.version === '1.2',
    '/openrtb/bidrequest': data => 'imp' in (data || {}),
    '/mismo/subject-property': data => 'MESSAGE' in (data || {}),
    '/ogc/conformance': data => (data?.conformsTo || []).some(value => value.includes('ogcapi-features')),
  };
  const badShapes = [];
  for (const [path, valid] of Object.entries(shapes)) { const response = await http(path); if (response.status !== 200 || !valid(response.data)) badShapes.push(`${path}(${response.status})`); }
  check('protocol documents remain spec-shaped', badShapes.length === 0, `${Object.keys(shapes).length - badShapes.length}/${Object.keys(shapes).length} ok; ${badShapes}`);
  for (const path of ['/cap/alerts', '/epcis/events', '/openrtb/bidrequest']) {
    const response = await http(path);
    check(`${path} explains its empty collection`, Boolean(response.data?.empty_reason));
  }
  const all = (await http('/capability')).data?.capabilities || [];
  check('executable catalogue advertises capabilities', all.length > 0, `${all.length} capabilities`);
  for (const row of all) {
    const x402 = await http(`/capability/${encodeURIComponent(row.name)}`);
    const mpp = await http(`/mpp/quote?capability=${encodeURIComponent(row.name)}`);
    const xp = x402.data?.accepts?.[0]?.extra?.priceUsd;
    const mp = mpp.data?.price_usd || mpp.data?.priceUsd;
    if (xp && mp) check(`${row.name} x402 and MPP price agree`, Math.abs(Number(xp) - Number(mp)) <= 1e-9);
    else if (!xp) check(`${row.name} unpriced refuses instead of free`, JSON.stringify(x402.data).includes('not_for_sale'));
  }

  const robots = await http('/robots.txt');
  check('robots.txt names sitemap and studio exclusion', text(robots).includes('Sitemap: ') && text(robots).includes('Disallow: /studio/'));
  const sitemap = await http('/sitemap.xml');
  check('sitemap index enumerates families', sitemap.status === 200 && text(sitemap).includes('sitemap-notes.xml'));
  const notes = await http('/sitemap-notes.xml');
  const listed = [...text(notes).matchAll(/<loc>[^<]*?\/(notes\/[^<]+)<\/loc>/g)].map(match => match[1]);
  const unreachable = [];
  for (const path of listed) if ((await http(`/${path}`)).status !== 200) unreachable.push(path);
  check('sitemap claims serve to a stranger', unreachable.length === 0, `${listed.length} listed, unreachable: ${unreachable}`);
  const feed = await http('/feed.xml');
  check('Atom feed carries entries', feed.status === 200 && text(feed).includes('<feed') && text(feed).includes('<entry>'));

  const room = `/room/parcel/${encodeURIComponent(config.parcel)}`;
  const variants = await Promise.all(['application/json', 'application/ld+json', 'text/html'].map(mime => http(room, { headers: { Accept: mime } })));
  check('JSON alternate answers JSON', text(variants[0]).trimStart().startsWith('{'));
  check('linked-data alternate answers JSON-LD', text(variants[1]).includes('"@context"') && text(variants[1]).includes('schema.org'));
  check('same address serves HTML', text(variants[2]).trimStart().toLowerCase().startsWith('<!doctype'));
  check('all carriers describe one subject', variants.every(response => text(response).includes(config.parcel)));
  check('machine representation states its limit', /not/i.test(text(variants[0])) && /appraisal/i.test(text(variants[0])));
  const head = await http('/sitemap.xml', { method: 'HEAD' });
  const etag = head.headers?.get?.('etag');
  check('HEAD provides validator', Boolean(etag));
  if (etag) { const conditional = await http('/sitemap.xml', { headers: { 'If-None-Match': etag } }); check('conditional refetch is 304', conditional.status === 304); }

  const family = `/subjects/parcel/${encodeURIComponent(config.parcel)}`;
  const manifest = await http(`${family}/manifest.json`);
  const reps = manifest.data?.representations || [];
  check('manifest names representation family', reps.length >= 4, reps.map(row => row.name).join(', '));
  const missing = [];
  for (const rep of reps) { const response = await http(`${family}/${encodeURIComponent(rep.name)}`); if (response.status !== 200) missing.push(rep.name); }
  check('manifest representations are actually served', missing.length === 0, `${missing}`);
  const index = await http(`${family}/index.json`), map = await http(`${family}/map.geojson`);
  check('representations agree on subject', index.data?.account === config.parcel && map.data?.properties?.account === config.parcel);
  check('absent geometry is explained', map.data?.geometry === null && String(map.data?.geometry_absent_because || '').includes('not a coordinate'));
  check('machine representation carries limit', (index.data?.not || []).join(' ').includes('appraisal'));
  const subjectSitemap = await http('/sitemap-subjects.xml');
  check('subjects are crawlable', (text(subjectSitemap).match(/<url>/g) || []).length > 100);

  const websubToken = process.env.BONFYRE_CIRCUIT_WEBSUB_TOKEN;
  const websubSecret = process.env.BONFYRE_CIRCUIT_WEBSUB_SECRET;
  if (websubToken) {
    const unknown = await http('/websub/unknowntoken000000000000000000');
    check('unknown callback is refused', unknown.status === 404);
    const wrongTopic = await http(`/websub/${encodeURIComponent(websubToken)}?${new URLSearchParams({ 'hub.mode': 'subscribe', 'hub.topic': 'https://evil.example/f', 'hub.challenge': 'zz' })}`);
    check('hub cannot enrol a foreign topic', wrongTopic.status === 404);
    const challenge = 'zz9';
    const ownTopic = await http(`/websub/${encodeURIComponent(websubToken)}?${new URLSearchParams({ 'hub.mode': 'subscribe', 'hub.topic': `${config.base}/feed.xml`, 'hub.challenge': challenge })}`);
    check('own topic echoes challenge', ownTopic.status === 200 && text(ownTopic).includes(challenge));
    if (config.apply && websubSecret) {
      const path = `/websub/${encodeURIComponent(websubToken)}`, payload = '{"changed":"circuit"}';
      const signed = secret => createHmac('sha256', secret).update(payload).digest('hex');
      const unsigned = await http(path, { method: 'POST', body: payload });
      const forged = await http(path, { method: 'POST', body: payload, headers: { 'X-Hub-Signature': `sha256=${signed('wrongsecret')}` } });
      const valid = await http(path, { method: 'POST', body: payload, headers: { 'X-Hub-Signature': `sha256=${signed(websubSecret)}` } });
      check('unsigned signed-subscription delivery refused', unsigned.status === 403);
      check('forged signature refused', forged.status === 403);
      check('correct signature accepted', valid.status === 204);
    } else skip('WebSub signed POST circuit', 'requires --apply and BONFYRE_CIRCUIT_WEBSUB_SECRET');
  } else skip('WebSub callback circuit', 'requires BONFYRE_CIRCUIT_WEBSUB_TOKEN');

  const membrane = await http('/membrane', { headers: { Accept: 'text/html' } });
  const html = text(membrane);
  check('membrane has a surface', membrane.status === 200 && html.trimStart().toLowerCase().startsWith('<!doctype') && html.includes('both directions'));
  check('membrane reports outbound readers', html.includes('Machines that read us') && html.includes('requests'));
  check('membrane reports inbound cost', html.includes('Sources we read') && html.includes('rung'));
  check('declared identity is not a credential', html.includes('not a credential'));
  check('passport does not assert documents', html.includes('never what its documents assert'));
  const home = await http('/');
  check('membrane reachable from landing', text(home).includes('href="/membrane"'));
  return finish();
}

if (import.meta.main) {
  try { process.exitCode = await run(options()); }
  catch (error) { console.error(error.message); process.exitCode = 1; }
}
