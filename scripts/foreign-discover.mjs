#!/usr/bin/env bun
// Source passports are projections of receipted public observations, never admission.
import { fileURLToPath } from 'node:url';
import { observePublic } from '../cmd/BonfyreSourceFrontier/tools/foreign_observation.mjs';
import { robotsAllows } from '../cmd/BonfyreSourceFrontier/tools/robots_policy.mjs';
import { verifiedObservation } from './sitemap-delta.mjs';
import { decodeFeed } from './feed-parse.mjs';
import { decodeSitemap } from './sitemap-delta.mjs';

let cachedPolicy;
export function foreignDiscoveryPolicy() {
  if (!cachedPolicy) {
    const path = fileURLToPath(new URL('./foreign-discovery.cue', import.meta.url));
    const result = Bun.spawnSync(['cue', 'export', path, '-e', 'foreignDiscovery']);
    if (result.exitCode !== 0) throw new Error(new TextDecoder().decode(result.stderr).trim());
    cachedPolicy = JSON.parse(new TextDecoder().decode(result.stdout));
  }
  return cachedPolicy;
}

const tagPattern = /<link\b[^>]*>/gi;
const attributePattern = /([\w:-]+)\s*=\s*(["'])(.*?)\2/g;
const tags = html => [...html.matchAll(tagPattern)].map(match => {
  const attrs = {};
  for (const [, key, , value] of match[0].matchAll(attributePattern)) attrs[key.toLowerCase()] = value;
  return attrs;
});
const httpAddress = (href, root) => {
  try {
    const result = new URL(href, root);
    return ['http:', 'https:'].includes(result.protocol) ? result.href : null;
  } catch { return null; }
};
const unique = rows => [...new Set(rows)];
const uniqueBy = (rows, key) => {
  const seen = new Set(), result = [];
  for (const row of rows) {
    const identity = key(row);
    if (seen.has(identity)) continue;
    seen.add(identity);
    result.push(row);
  }
  return result;
};
const validJson = text => { try { return JSON.parse(text); } catch { return null; } };

export function headDeclarations(html, root, policy = foreignDiscoveryPolicy()) {
  const result = { feeds: [], websub_hubs: [], canonical: '', structured_types: [], open_graph: {} };
  const head = html.slice(0, policy.max_head_bytes);
  for (const attrs of tags(head)) {
    if (!attrs.href) continue;
    const url = httpAddress(attrs.href, root);
    if (!url) continue;
    const rels = (attrs.rel ?? '').toLowerCase().split(/\s+/);
    if (rels.includes('hub')) result.websub_hubs.push(url);
    if (rels.includes('canonical')) result.canonical = url;
    if (rels.includes('alternate') && /rss|atom|feed\+json|json/.test(attrs.type ?? ''))
      result.feeds.push({ url, type: attrs.type.toLowerCase() });
  }
  for (const match of head.matchAll(/<script\b[^>]*application\/ld\+json[^>]*>([\s\S]*?)<\/script>/gi)) {
    const doc = validJson(match[1].trim());
    for (const node of Array.isArray(doc) ? doc : [doc]) {
      if (!node || typeof node !== 'object' || !node['@type']) continue;
      result.structured_types.push(...(Array.isArray(node['@type']) ? node['@type'] : [node['@type']]));
    }
  }
  for (const match of head.matchAll(/<meta\b[^>]*>/gi)) {
    const attrs = {};
    for (const [, key, , value] of match[0].matchAll(attributePattern)) attrs[key.toLowerCase()] = value;
    if (attrs.property?.startsWith('og:') && attrs.content != null)
      result.open_graph[attrs.property.slice(3)] = attrs.content.slice(0, 300);
  }
  return result;
}

function validCarrier(kind, raw) {
  try {
    if (kind === 'sitemap') return ['urlset', 'sitemapindex'].includes(decodeSitemap(raw).kind);
    if (raw.toString('utf8', 0, 100).trimStart().startsWith('{')) {
      const doc = JSON.parse(raw.toString('utf8'));
      return !!doc && Array.isArray(doc.items);
    }
    return ['rss', 'atom'].includes(decodeFeed(raw).carrier);
  } catch { return false; }
}

export async function discoverForeign(rootInput, options = {}, policy = foreignDiscoveryPolicy()) {
  const url = new URL(rootInput.includes('://') ? rootInput : `https://${rootInput}`);
  if (!['http:', 'https:'].includes(url.protocol)) throw new Error('HTTP(S) publisher root required');
  const root = url.origin + '/';
  const foreign = options.foreign ?? process.env.BONFYRE_FOREIGN_ROOT ?? '/var/lib/bonfyre/foreign';
  const project = options.project ?? process.env.BONFYRE_PROJECT_BIN ?? '/usr/local/bin/bonfyre-project';
  const observe = options.observe ?? (address => observePublic(project, foreign, 'foreign-discover', address,
    { maxBytes: policy.max_body_bytes, timeout: policy.observation_timeout_ms, readText: false }));
  const observations = new Map();
  const ask = async address => {
    if (!observations.has(address)) observations.set(address, await observe(address));
    const observation = observations.get(address);
    return { observation, verified: verifiedObservation(observation, policy.max_body_bytes) };
  };
  const result = { root, observed_at: (options.clock?.() ?? new Date()).toISOString().replace(/\.\d{3}Z$/, 'Z'),
    robots_status: 0, robots_allows_us: true, crawl_delay: null, sitemaps: [], feeds: [],
    websub_hubs: [], activitypub: [], well_known: [], protocol_descriptors: [],
    structured_types: [], open_graph: {}, canonical: '', conditional: {},
    best_carrier: 'html-extraction', why: '', notes: [] };
  const robotsUrl = new URL('/robots.txt', root).href;
  const robots = await ask(robotsUrl);
  result.robots_status = Number(robots.observation.receipt?.http_status ?? 0);
  if (robots.verified.raw) {
    const body = robots.verified.raw.toString('utf8');
    result.robots_allows_us = robotsAllows(body, 'BonfyreForeignDiscover', root);
    for (const match of body.matchAll(/^\s*sitemap:\s*(\S+)/gim)) {
      const address = httpAddress(match[1], root);
      if (address) result.sitemaps.push(address);
    }
    for (const match of body.matchAll(/^\s*crawl-delay:\s*(\d+(?:\.\d+)?)/gim))
      result.crawl_delay = Number(match[1]);
  } else if (robots.verified.error !== 'not observed') {
    result.notes.push(`robots observation: ${robots.verified.error}`);
  }
  if (robots.observation.receipt?.policy_decision &&
      robots.observation.receipt.policy_decision !== 'allow') result.robots_allows_us = false;
  if (result.robots_allows_us) {
    const home = await ask(root);
    if (home.verified.raw) {
      Object.assign(result, headDeclarations(home.verified.raw.toString('utf8'), root, policy));
      for (const key of ['etag', 'last_modified']) if (home.observation.receipt?.[key])
        result.conditional[key.replace('_', '-')] = home.observation.receipt[key];
    } else result.notes.push(`root observation: ${home.verified.error}`);
    if (home.observation.receipt?.policy_decision &&
        home.observation.receipt.policy_decision !== 'allow') result.robots_allows_us = false;
    if (result.robots_allows_us) {
    for (const convention of policy.known_carriers) {
      const address = new URL(convention.path, root).href;
      const answer = await ask(address);
      if (!answer.verified.raw) continue;
      if (!validCarrier(convention.kind, answer.verified.raw)) {
        result.notes.push(`${convention.path} answered but is not a ${convention.kind}; ignored`);
        continue;
      }
      if (convention.kind === 'sitemap') result.sitemaps.push(address);
      else result.feeds.push({ url: address, type: 'discovered-by-convention' });
    }
    for (const descriptor of policy.descriptors) {
      const address = new URL(descriptor.path, root).href;
      const answer = await ask(address);
      if (!answer.verified.raw || validJson(answer.verified.raw.toString('utf8')) === null) continue;
      result.well_known.push(address);
      if (descriptor.family === 'activitypub-discovery') result.activitypub.push(address);
      result.protocol_descriptors.push({ family: descriptor.family, descriptor: address,
        truth_level: 'declared_descriptor', limit: policy.descriptor_limit });
    }
    const auth = await ask(new URL('/auth.md', root).href);
    if (auth.verified.raw?.length <= policy.max_auth_bytes &&
        auth.verified.raw.toString('utf8').trimStart().startsWith('#'))
      result.protocol_descriptors.push({ family: 'auth-md-candidate',
        descriptor: new URL('/auth.md', root).href, truth_level: 'declared_descriptor',
        limit: policy.auth_limit });
    }
  }
  result.sitemaps = unique(result.sitemaps);
  result.websub_hubs = unique(result.websub_hubs);
  result.activitypub = unique(result.activitypub);
  result.structured_types = unique(result.structured_types);
  result.feeds = uniqueBy(result.feeds, row => row.url);
  result.protocol_descriptors = uniqueBy(result.protocol_descriptors,
    row => `${row.family}\0${row.descriptor}`);
  const chosen = !result.robots_allows_us ? null : policy.carrier_rank.find(rank =>
    Array.isArray(result[rank.field]) ? result[rank.field].length : Object.keys(result[rank.field] ?? {}).length);
  result.best_carrier = result.robots_allows_us ? chosen?.carrier ?? 'html-extraction' : 'none';
  result.why = result.robots_allows_us ? chosen?.why ?? policy.default_why : policy.declined_why;
  if (options.includeObservation && result.robots_allows_us) {
    const target = result.feeds[0]?.url ?? result.sitemaps[0];
    if (target) {
      const answer = await ask(target);
      result.observation = { content_sha256: answer.observation.receipt?.content_sha256,
        http_status: answer.observation.receipt?.http_status,
        policy_decision: answer.observation.receipt?.policy_decision,
        body_bytes: answer.observation.receipt?.body_bytes, receipt: answer.observation.receiptPath };
      result.observed_url = target;
    } else result.observation = { skipped: 'no cheap carrier to receipt; HTML extraction requires a separate decision' };
  }
  return result;
}

if (import.meta.main) {
  const args = process.argv.slice(2), includeObservation = args.includes('--observe');
  const roots = args.filter(arg => arg !== '--observe');
  if (!roots.length) { process.stderr.write('usage: foreign-discover.mjs [--observe] <https://root> [...]\n'); process.exit(2); }
  try {
    const results = [];
    for (const root of roots) results.push(await discoverForeign(root, { includeObservation }));
    process.stdout.write(`${JSON.stringify(results.length === 1 ? results[0] : results, null, 2)}\n`);
  } catch (error) { process.stderr.write(`${error.message}\n`); process.exitCode = 1; }
}
