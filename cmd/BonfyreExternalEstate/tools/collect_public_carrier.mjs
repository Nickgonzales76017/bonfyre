#!/usr/bin/env bun
// Public carrier observation is owned by ForeignTwin; this only narrows retained bytes.
import { createHash } from 'node:crypto';
import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { spawnSync } from 'node:child_process';

const required = (ok, message) => { if (!ok) throw new Error(message); };
const https = value => typeof value === 'string' && value.startsWith('https://');
const digestId = value => createHash('sha256').update(value).digest('hex').slice(0, 24);
const envelope = (provider, kind, payload, args, ident) => ({ provider, kind,
  source_ref: args['source-ref'], receipt_ref: `foreign:${ident}`,
  observed_at: args['observed-at'], payload });

export function normalizeProviderRecord(provider, doc, source, receipt, at) {
  required(doc && typeof doc === 'object' && !Array.isArray(doc), 'provider response must be an object');
  let x, kind, payload;
  switch (provider) {
    case '211':
      x = doc.resource || doc.data || doc;
      kind = '211_resource';
      payload = { id: String(x.id || x.resource_id || ''), title: x.name || x.title || '',
        url: x.url || x.website || '', description: x.description || '',
        functions: Array.isArray(x.services) ? x.services.join(',') : '' };
      break;
    case 'clever':
      x = doc.data || doc; x = Array.isArray(x) ? x[0] : x;
      kind = 'clever_institution';
      payload = { id: String(x?.id || ''), title: x?.name || '', url: x?.url || x?.website || '',
        description: 'Public institution record.' };
      break;
    case 'buildium':
      x = doc.data || doc;
      kind = 'property_record';
      payload = { id: String(x.Id || x.id || ''), title: x.Name || x.name || '',
        url: x.PublicUrl || x.url || '', description: 'Public property record.' };
      break;
    case 'nextdoor':
      x = doc;
      kind = 'nextdoor_place';
      payload = { id: String(x.id || x['@id'] || ''), title: x.name || x.title || '',
        url: x.url || '', description: x.description || '' };
      break;
    case 'patreon':
      x = doc.data || doc; x = Array.isArray(x) ? x[0] : x;
      kind = 'patreon_creator';
      payload = { id: String(x?.id || ''), title: x?.attributes?.full_name || x?.name || '',
        url: x?.attributes?.url || x?.url || '',
        description: 'Public creator profile; support/payment fields excluded.' };
      break;
    default: throw new Error('unsupported provider');
  }
  required(payload.id && payload.title && https(payload.url), 'normalized record needs exact id, title, HTTPS URL');
  return { provider, kind, source_ref: source, receipt_ref: receipt, observed_at: at, payload };
}

export function eventFromHtml(body, url) {
  for (const match of body.matchAll(/<script\b[^>]*\btype\s*=\s*["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script\s*>/gi)) {
    let doc;
    try { doc = JSON.parse(match[1].replaceAll('&quot;', '"').replaceAll('&#34;', '"').replaceAll('&amp;', '&')); }
    catch { continue; }
    const pool = Array.isArray(doc) ? doc : [doc, ...(Array.isArray(doc?.['@graph']) ? doc['@graph'] : [])];
    for (const item of pool) {
      if (!item || typeof item !== 'object') continue;
      const types = Array.isArray(item['@type']) ? item['@type'] : [item['@type']];
      if (types.includes('Event') && item.name) return {
        id: String(item['@id'] || item.url || url), title: item.name,
        url: item.url || url, description: item.description || '', starts_at: item.startDate || '' };
    }
  }
  throw new Error('retained page declares no Event JSON-LD');
}

export function normalizeCarrier(carrier, doc, args, ident, url) {
  let payload, kind, provider = carrier;
  switch (carrier) {
    case 'github':
      required(doc && typeof doc === 'object' && doc.id && doc.full_name && https(doc.html_url),
        'public GitHub repository response missing id, full_name, or HTTPS html_url');
      kind = 'github_repository';
      payload = { id: String(doc.id), title: doc.full_name, url: doc.html_url,
        description: doc.description || '', created_at: doc.created_at || '' };
      break;
    case 'reddit': {
      const first = doc?.data?.children?.[0]?.data || {};
      const name = first.subreddit || first.display_name;
      const page = first.subreddit_url || (name ? `https://www.reddit.com/r/${name}/` : '');
      required(name && https(page), 'public subreddit listing has no community identity');
      kind = 'reddit_community';
      payload = { id: `r/${name}`, title: `r/${name}`, url: page,
        description: first.subreddit_name_prefixed || '' };
      break;
    }
    case 'discord':
      required(doc && typeof doc === 'object' && doc.id && doc.name && https(doc.instant_invite),
        'public Discord widget lacks id, name, or public HTTPS invite');
      kind = 'discord_server';
      payload = { id: String(doc.id), title: doc.name, url: doc.instant_invite,
        description: 'Public Discord widget observed.' };
      break;
    case 'event':
      provider = args.provider;
      required(['meetup', 'eventbrite'].includes(provider), 'event provider must be meetup or eventbrite');
      kind = `${provider}_event`;
      payload = eventFromHtml(doc, url);
      break;
    case '211':
      return normalizeProviderRecord('211', doc, args['source-ref'], `foreign:${ident}`, args['observed-at']);
    default: throw new Error('unsupported public carrier');
  }
  return envelope(provider, kind, payload, args, ident);
}

function route(carrier, args) {
  let ident, url;
  switch (carrier) {
    case 'github':
      required(/^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/.test(args.repository || ''), 'repository must be OWNER/REPO');
      ident = `github-${args.repository.replace('/', '-')}`;
      url = `https://api.github.com/repos/${args.repository}`;
      break;
    case 'reddit': {
      const name = (args.subreddit || '').replace(/^r\//, '').trim();
      required(/^[A-Za-z0-9_-]+$/.test(name), 'invalid subreddit');
      ident = `reddit-${name.toLowerCase()}`;
      url = `https://www.reddit.com/r/${name}/.json?limit=1`;
      break;
    }
    case 'discord':
      required(/^\d+$/.test(args['guild-id'] || ''), 'guild id must be numeric');
      ident = `discord-${args['guild-id']}`;
      url = `https://discord.com/api/guilds/${args['guild-id']}/widget.json`;
      break;
    case 'event':
      required(['meetup', 'eventbrite'].includes(args.provider) && https(args.url), 'public HTTPS event URL required');
      ident = `${args.provider}-event-${digestId(args.url)}`;
      url = args.url;
      break;
    case '211':
      required(https(args.url), 'public HTTPS 211 resource URL required');
      ident = `211-resource-${digestId(args.url)}`;
      url = args.url;
      break;
    default: throw new Error('unsupported public carrier');
  }
  return { ident, url };
}

export function collect(carrier, args) {
  for (const key of ['project', 'foreign-root', 'source-ref', 'observed-at', 'out'])
    required(args[key], `missing --${key}`);
  const { ident, url } = route(carrier, args);
  const bodies = join(args['foreign-root'], 'bodies');
  const receipts = join(args['foreign-root'], 'receipts');
  mkdirSync(bodies, { recursive: true }); mkdirSync(receipts, { recursive: true });
  const body = join(bodies, `${ident}.body`), receipt = join(receipts, `${ident}.json`);
  const run = spawnSync(args.project, ['foreign', 'observe', ident, url,
    '--observe-right', 'public', '--body', body, '--receipt', receipt], { encoding: 'utf8' });
  required(run.status === 0 && existsSync(body) && existsSync(receipt),
    'ForeignTwin observer did not retain body and receipt');
  const raw = readFileSync(body, 'utf8');
  const doc = carrier === 'event' ? raw : JSON.parse(raw);
  const record = normalizeCarrier(carrier, doc, args, ident, url);
  writeFileSync(args.out, JSON.stringify(record) + '\n');
  return { kind: record.kind, receipt_ref: record.receipt_ref };
}

if (import.meta.main) {
  try {
    const [carrier, ...argv] = process.argv.slice(2);
    const args = {};
    for (let i = 0; i < argv.length; i += 2) {
      required(argv[i]?.startsWith('--') && argv[i + 1], 'expected --key value arguments');
      args[argv[i].slice(2)] = argv[i + 1];
    }
    console.log(JSON.stringify(collect(carrier, args)));
  } catch (error) { console.error(`public carrier collector: ${error.message}`); process.exitCode = 2; }
}
