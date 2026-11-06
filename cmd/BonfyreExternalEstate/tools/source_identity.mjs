// Shared source coordinate lowering for receipt and impact compilers.
import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';

const first = (row, names) => {
  for (const name of names) {
    const value = name.split('.').reduce((part, key) => part?.[key], row);
    if (typeof value === 'string' && value.trim()) return value.trim();
  }
  return null;
};

export function loadRows(path) {
  const text = readFileSync(path, 'utf8').trim();
  if (!text) return [];
  let value;
  try { value = JSON.parse(text); }
  catch { return text.split('\n').filter(Boolean).map(JSON.parse); }
  if (Array.isArray(value)) return value;
  if (value && typeof value === 'object') {
    for (const key of ['rows', 'items', 'data', 'sources', 'entries', 'candidates',
      'gaps', 'roots', 'measurements'])
      if (Array.isArray(value[key])) return value[key];
    return [value];
  }
  return [];
}

export function canonicalUrl(address) {
  const url = new URL(address);
  const scheme = url.protocol.toLowerCase();
  if (scheme !== 'http:' && scheme !== 'https:') throw new Error('source URL must be HTTP(S)');
  const host = url.hostname.toLowerCase().replace(/\.$/, '').replace(/^www\./, '');
  const defaultPort = (scheme === 'http:' && url.port === '80') || (scheme === 'https:' && url.port === '443');
  const port = url.port && !defaultPort ? `:${url.port}` : '';
  const path = (url.pathname || '/').replace(/\/{2,}/g, '/');
  const query = [...url.searchParams].filter(([key]) => !/^utm_/i.test(key) && !['fbclid', 'gclid'].includes(key.toLowerCase()))
    .toSorted(([a, av], [b, bv]) => a.localeCompare(b) || av.localeCompare(bv));
  const encoded = new URLSearchParams(query).toString();
  return `${scheme}//${host}${port}${path}${encoded ? `?${encoded}` : ''}`;
}

export function channelUrl(row) {
  const value = first(row, ['primary_url', 'url', 'feed', 'href', 'endpoint', 'source_url',
    'base_url', 'address', 'carrier.route', 'route', 'target', 'uri']);
  return value?.startsWith('http://') || value?.startsWith('https://') ? value : null;
}

export function rootAddress(row, url) {
  const explicit = first(row, ['source_root', 'root', 'origin']);
  if (explicit?.startsWith('http://') || explicit?.startsWith('https://')) return canonicalUrl(explicit);
  if (url) {
    const parsed = new URL(url);
    return `${parsed.protocol.toLowerCase()}//${parsed.hostname.toLowerCase().replace(/^www\./, '')}/`;
  }
  return null;
}

export function sourceRoots(specs) {
  const roots = new Map();
  for (const spec of specs) {
    const split = spec.indexOf('=');
    if (split < 1) throw new Error('--source-input must be LABEL=PATH');
    const label = spec.slice(0, split), path = spec.slice(split + 1);
    for (const row of loadRows(path)) {
      if (!row || typeof row !== 'object' || Array.isArray(row)) continue;
      const raw = channelUrl(row);
      if (!raw?.startsWith('http://') && !raw?.startsWith('https://')) continue;
      let address;
      try { address = rootAddress(row, canonicalUrl(raw)); } catch { continue; }
      if (!address) continue;
      let root = roots.get(address);
      if (!root) {
        root = { root_id: `root-${createHash('sha256').update(address).digest('hex').slice(0, 20)}`,
          root_address: address, provenance: new Set() };
        roots.set(address, root);
      }
      root.provenance.add(label);
    }
  }
  return roots;
}

export function rootForTopic(roots, topic) {
  try {
    const url = new URL(canonicalUrl(topic));
    return roots.get(`${url.protocol.toLowerCase()}//${url.hostname.toLowerCase().replace(/^www\./, '')}/`) || null;
  } catch { return null; }
}
