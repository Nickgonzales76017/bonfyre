#!/usr/bin/env bun
// Compatibility accounting projection; admission remains with native SourceFrontier.
import { createHash } from 'node:crypto';
import { basename } from 'node:path';
import { readFileSync, writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { canonicalUrl, channelUrl } from './source_identity.mjs';

let cachedPolicy;
export function frontierAccountingPolicy() {
  if (!cachedPolicy) {
    const path = fileURLToPath(new URL('../source-frontier-accounting.cue', import.meta.url));
    const result = Bun.spawnSync(['cue', 'export', path, '-e', 'sourceFrontierAccounting']);
    if (result.exitCode !== 0) throw new Error(`CUE source frontier: ${new TextDecoder().decode(result.stderr).trim()}`);
    cachedPolicy = JSON.parse(new TextDecoder().decode(result.stdout));
  }
  return cachedPolicy;
}

const token = value => String(value || '').toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '');
const unique = values => [...new Set(values)].toSorted();
const digest = (value, length) => createHash('sha256').update(value).digest('hex').slice(0, length);
const first = (row, names) => {
  for (const name of names) {
    const value = name.split('.').reduce((part, key) => part?.[key], row);
    if (typeof value === 'string' && value.trim()) return value.trim();
  }
  return null;
};
const rowsFrom = (path, policy) => {
  const content = readFileSync(path, 'utf8').trim();
  if (!content) return [];
  let parsed;
  try { parsed = JSON.parse(content); }
  catch { return content.split('\n').filter(line => line.trim()).map(JSON.parse); }
  if (Array.isArray(parsed)) return parsed;
  if (parsed && typeof parsed === 'object') {
    for (const key of policy.input_arrays) if (Array.isArray(parsed[key])) return parsed[key];
    return [parsed];
  }
  return [];
};
const terms = (row, policy) => {
  const result = [];
  for (const key of policy.term_fields) {
    const value = row[key];
    if (Array.isArray(value)) result.push(...value.map(String));
    else if (typeof value === 'string') result.push(...value.split(/[,;\s]+/));
  }
  if (row.projection && typeof row.projection === 'object' && !Array.isArray(row.projection)) {
    for (const key of policy.projection_term_fields)
      if (Array.isArray(row.projection[key])) result.push(...row.projection[key].map(String));
    if (row.projection.carrier_candidate) result.push(String(row.projection.carrier_candidate));
  }
  return result.filter(Boolean);
};
const familyOf = (row, url, policy) => {
  const declared = first(row, policy.family_fields);
  const hay = [...terms(row, policy), ...(declared ? [declared] : []), ...(url ? [url] : [])].join(' ').toLowerCase();
  for (const rule of policy.family_rules)
    if (rule.terms.some(needle => hay.includes(needle))) return rule.family;
  if (url && /\.(json|geojson)$/i.test(new URL(url).pathname)) return 'JSON_API';
  return url ? 'HTML' : 'DESCRIPTOR';
};
const stateOf = (row, policy) => {
  const value = token(first(row, policy.state_fields) || 'CANDIDATE').replaceAll('-', '_').toUpperCase();
  return value === 'ALLOW' ? 'OBSERVED' : policy.refused_states.includes(value) ? 'REFUSED' : value;
};
const rightsOf = (row, policy) => row.reuse && typeof row.reuse === 'object' && row.reuse.unknown
  ? 'UNKNOWN' : row.reuse && typeof row.reuse === 'object' && row.reuse.allows
    ? 'DECLARED' : first(row, policy.rights_fields) ? 'DECLARED' : 'UNKNOWN';
const credentialsOf = (row, policy) => {
  const explicit = first(row, policy.credential_fields);
  if (explicit) return token(explicit).replaceAll('-', '_').toUpperCase();
  const hay = terms(row, policy).join(' ').toLowerCase();
  return policy.credential_hints.some(hint => hay.includes(hint)) ? 'REQUIRED_UNKNOWN' : 'NOT_DECLARED';
};
const economicsOf = (row, policy) => unique(policy.economic_fields.flatMap(key => {
  const value = row[key];
  return Array.isArray(value) ? value : typeof value === 'string' ? value.split(/[,;]/) : [];
}).filter(value => String(value).trim()).map(value => token(value).replaceAll('-', '_').toUpperCase()));
const rootAddress = (row, url, policy) => {
  const explicit = first(row, ['source_root', 'root', 'origin']);
  if (explicit?.startsWith('http://') || explicit?.startsWith('https://')) return canonicalUrl(explicit);
  if (url) {
    const parsed = new URL(url);
    return `${parsed.protocol.toLowerCase()}//${parsed.hostname.toLowerCase().replace(/^www\./, '')}/`;
  }
  return `urn:bonfyre:source:${token(first(row, policy.root_identity_fields) || 'unknown')}`;
};
const gapApplies = (predicate, root, policy) => ({
  no_admitted_channels: () => root.admitted_channels === 0,
  no_structured_family: () => !root.channel_families.some(family => policy.structured_families.includes(family)),
  unknown_rights: () => root.rights_states.includes('UNKNOWN'),
  required_credential: () => root.credential_states.some(state => state.startsWith('REQUIRED')),
  no_publisher_identity: () => root.publisher_identities.length === 0,
  unrelated_transaction: () => root.transaction_like && root.economic_interfaces.length === 0,
})[predicate]?.() ?? (() => { throw new Error(`unknown frontier gap predicate: ${predicate}`); })();

export function compileSourceFrontier(specs, policy = frontierAccountingPolicy()) {
  const occurrences = specs.flatMap(spec => {
    const split = spec.indexOf('=');
    if (split < 0) throw new Error('--input must be LABEL=PATH');
    const label = spec.slice(0, split), path = spec.slice(split + 1);
    return rowsFrom(path, policy).flatMap((row, index) => row && typeof row === 'object' && !Array.isArray(row)
      ? [{ label, path, ordinal: index + 1, row }] : []);
  });
  const roots = new Map(), channels = new Map(), provenance = new Map();
  let declaredVirtual = 0;
  for (const { label, path, ordinal, row } of occurrences) {
    provenance.set(label, (provenance.get(label) || 0) + 1);
    const rawUrl = channelUrl(row), url = rawUrl ? canonicalUrl(rawUrl) : null;
    const address = rootAddress(row, url, policy), rootId = `root-${digest(address, 20)}`;
    const family = familyOf(row, url, policy), state = stateOf(row, policy);
    const basis = url || `${address}#${first(row, policy.channel_identity_fields) || ordinal}`;
    const channelId = `channel-${digest(basis, 20)}`;
    let channel = channels.get(channelId);
    if (!channel) {
      channel = { channel_id: channelId, root_id: rootId, address: url, family,
        states: new Set(), provenance: new Set(), capabilities: new Set() };
      channels.set(channelId, channel);
    }
    channel.states.add(state);
    channel.provenance.add(`${label}:${basename(path)}:${ordinal}`);
    for (const term of terms(row, policy)) channel.capabilities.add(token(term));
    if (Number.isInteger(row.virtual_channel_count) && row.virtual_channel_count > 0)
      declaredVirtual += row.virtual_channel_count;
    let root = roots.get(rootId);
    if (!root) {
      root = { root_id: rootId, address, channel_ids: new Set(), channel_families: new Set(),
        states: new Set(), provenance: new Set(), publisher_identities: new Set(),
        rights_states: new Set(), credential_states: new Set(), economic_interfaces: new Set(),
        transaction_like: false };
      roots.set(rootId, root);
    }
    root.channel_ids.add(channelId); root.channel_families.add(family); root.states.add(state);
    root.provenance.add(label);
    const identity = first(row, policy.publisher_identity_fields);
    if (identity) root.publisher_identities.add(identity);
    root.rights_states.add(rightsOf(row, policy));
    root.credential_states.add(credentialsOf(row, policy));
    for (const economic of economicsOf(row, policy)) root.economic_interfaces.add(economic);
    root.transaction_like ||= policy.transaction_hints.some(hint => terms(row, policy).join(' ').toLowerCase().includes(hint));
  }
  const channelRows = [...channels.values()].map(row => ({ ...row, states: unique(row.states),
    provenance: unique(row.provenance), capabilities: unique(row.capabilities) }))
    .toSorted((a, b) => a.root_id.localeCompare(b.root_id) || a.family.localeCompare(b.family) || a.channel_id.localeCompare(b.channel_id));
  const rootRows = [...roots.values()].map(raw => {
    const row = { ...raw };
    for (const key of ['channel_ids', 'channel_families', 'states', 'provenance', 'publisher_identities',
      'rights_states', 'credential_states', 'economic_interfaces']) row[key] = unique(row[key]);
    const states = row.channel_ids.map(id => channels.get(id).states);
    row.channels = row.channel_ids.length;
    row.admitted_channels = states.filter(values => [...values].some(value => policy.admitted_states.includes(value))).length;
    row.active_channels = states.filter(values => [...values].some(value => policy.active_states.includes(value))).length;
    return row;
  }).toSorted((a, b) => a.root_id.localeCompare(b.root_id));
  const gaps = rootRows.flatMap(root => policy.gap_rules.filter(rule => gapApplies(rule.predicate, root, policy))
    .map(rule => ({ gap_id: `gap-${digest(`${root.root_id}\0${rule.kind}`, 16)}`, root_id: root.root_id,
      kind: rule.kind, state: 'OPEN', resolution_classes: rule.routes, reason: rule.reason,
      channel_ids: root.channel_ids, provenance: root.provenance })))
    .toSorted((a, b) => a.kind.localeCompare(b.kind) || a.root_id.localeCompare(b.root_id));
  const counts = values => Object.fromEntries([...values.reduce((map, value) => map.set(value, (map.get(value) || 0) + 1), new Map())]
    .toSorted(([a], [b]) => a.localeCompare(b)));
  const cardinality = { input_occurrences: occurrences.length, source_roots: rootRows.length,
    channel_families: new Set(channelRows.map(row => row.family)).size, channels: channelRows.length,
    eligible_channels: channelRows.filter(row => !row.states.every(state => policy.refused_states.includes(state))).length,
    admitted_channels: channelRows.filter(row => row.states.some(state => policy.admitted_states.includes(state))).length,
    active_channels: channelRows.filter(row => row.states.some(state => policy.active_states.includes(state))).length,
    declared_virtual_channels: declaredVirtual, open_gaps: gaps.length };
  return { manifest: { schema: policy.schema, truth_level: policy.truth_level, cardinality,
    channel_family_counts: counts(channelRows.map(row => row.family)), gap_counts: counts(gaps.map(row => row.kind)),
    provenance_occurrences: Object.fromEntries([...provenance].toSorted(([a], [b]) => a.localeCompare(b))),
    cannot_infer: policy.cannot_infer }, roots: rootRows, channels: channelRows, gaps };
}

if (import.meta.main) {
  try {
    const args = { input: [] }, argv = process.argv.slice(2);
    for (let i = 0; i < argv.length; i += 2) {
      if (!argv[i]?.startsWith('--') || !argv[i + 1]) throw new Error('expected --key value arguments');
      const key = argv[i].slice(2);
      if (key === 'input') args.input.push(argv[i + 1]); else args[key] = argv[i + 1];
    }
    if (!args.input.length || !args.out) throw new Error('--input and --out are required');
    const result = compileSourceFrontier(args.input);
    writeFileSync(args.out, JSON.stringify(result.manifest, null, 2) + '\n');
    for (const key of ['roots', 'channels', 'gaps'])
      if (args[key]) writeFileSync(args[key], result[key].map(row => JSON.stringify(row)).join('\n') + (result[key].length ? '\n' : ''));
    console.log(JSON.stringify(result.manifest.cardinality));
  } catch (error) { console.error(`source frontier: ${error.message}`); process.exitCode = 2; }
}
