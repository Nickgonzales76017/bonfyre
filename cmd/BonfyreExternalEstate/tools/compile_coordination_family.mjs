#!/usr/bin/env bun
// Three exact projections over the same public-carrier / authority boundary.
import { createHash } from 'node:crypto';
import { readFileSync, writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { resolve } from 'node:path';

export const ROOTS = Object.freeze({
  'github-repository': 'project,operator,support',
  'reddit-community': 'community,operator',
  'discord-server': 'community,operator',
  'event-organizer': 'occurrence,community,operator',
  'education-institution': 'institution,program,household',
  'resource-directory-entry': 'household,institution,opportunity',
  'property-platform-record': 'property,household,institution',
  'commerce-catalogue': 'product,merchant,household',
  'neighborhood-network': 'world,community,household,business',
});
const digest = (...parts) => createHash('sha256').update(parts.join('\x1f')).digest('hex').slice(0, 24);
const requireValue = (ok, message) => { if (!ok) throw new Error(message); };
const row = (type, body) => ({ type, namespace: 'root', body: { ...body, status: 'published' } });

export function compilePhenotypes(spec) {
  const seen = new Set();
  return (spec.phenotypes || []).map(item => {
    requireValue(item.id && !seen.has(item.id) && item.roots && item.public && item.authority,
      'each phenotype needs unique id, roots, public carriers, authority');
    seen.add(item.id);
    return { phenotype: item.id, subject_roots: item.roots, public_carriers: item.public,
      authority_bridge: item.authority, bridge_state: item.bridge || 'deferred',
      cannot_infer: 'carrier participation does not grant authority or merge provider account identity into a Bonfyre subject' };
  });
}

export function compileCarrier(envelope) {
  requireValue(envelope && ['provider', 'carrier_kind', 'source_ref', 'receipt_ref', 'observed_at', 'payload']
    .every(key => envelope[key]), 'carrier envelope missing required receipt-bound field');
  const kind = envelope.carrier_kind, payload = envelope.payload;
  requireValue(Object.hasOwn(ROOTS, kind) && payload && typeof payload === 'object' && !Array.isArray(payload),
    'unknown carrier kind');
  const external = String(payload.id || ''), title = payload.title || '', href = payload.url || '';
  requireValue(external && title && typeof href === 'string' && href.startsWith('https://'),
    'carrier payload needs exact id, title, HTTPS URL');
  return row('external_participation_carrier', {
    carrier_id: `carrier-${digest(envelope.provider, kind, external)}`, provider: envelope.provider,
    carrier_kind: kind, external_id: external, title, canonical_href: href,
    candidate_roots: ROOTS[kind], source_ref: envelope.source_ref, receipt_ref: envelope.receipt_ref,
    observed_at: envelope.observed_at,
    cannot_infer: 'a public carrier is not a Bonfyre subject, membership, support relation, authority grant, payment, private-data access, or effect right',
  });
}

export function compileConnection(envelope) {
  const carrier = envelope?.carrier || {}, subject = envelope?.subject_ref || '',
    phase = envelope?.phase || '', at = envelope?.at || '';
  requireValue(subject && at && carrier.carrier_id && carrier.provider,
    'subject, carrier identity, provider, and time are required');
  const proof = envelope.proof || {}, grant = envelope.grant || {};
  let state, capabilities = [];
  switch (phase) {
    case 'observe': state = 'observed'; break;
    case 'verify':
      requireValue(proof.state === 'bridge_control_verified' && proof.receipt_ref,
        'verify requires a bridge_control_verified receipt');
      state = 'verified'; break;
    case 'connect':
      requireValue(proof.state === 'bridge_control_verified' && proof.receipt_ref,
        'connect requires a verified proof receipt');
      capabilities = grant.capabilities;
      requireValue(Array.isArray(capabilities) && capabilities.length &&
        capabilities.every(value => typeof value === 'string' && value) && grant.receipt_ref,
        'connect requires explicit capabilities and provider grant receipt');
      state = 'connected'; break;
    case 'revoke': state = 'revoked'; break;
    default: throw new Error('unknown provider connection phase');
  }
  const verified = state === 'verified' || state === 'connected';
  return row('external_provider_connection', {
    connection_id: `provider-connection-${digest(subject, carrier.carrier_id)}`,
    subject_ref: subject, carrier_id: carrier.carrier_id, provider: carrier.provider, state,
    proof_receipt_ref: verified ? proof.receipt_ref : '',
    grant_receipt_ref: state === 'connected' ? grant.receipt_ref : '',
    granted_capabilities: capabilities, verified_at: verified ? at : '',
    revoked_at: state === 'revoked' ? at : '',
    cannot_infer: 'a provider connection does not make the provider the semantic owner, prove a person or institution beyond the named proof, grant unlisted capabilities, or authorize payment, publishing, membership, or private-data import',
  });
}

if (import.meta.main) {
  try {
    const [mode, ...argv] = process.argv.slice(2), args = {};
    for (let i = 0; i < argv.length; i += 2) {
      requireValue(argv[i]?.startsWith('--') && argv[i + 1], 'expected --key value arguments');
      args[argv[i].slice(2)] = argv[i + 1];
    }
    requireValue(args.out, 'missing --out');
    const defaultSpec = resolve(fileURLToPath(import.meta.url), '../../../../schemas/control/incumbent-coordination-phenotypes.json');
    const input = mode === 'phenotypes' ? args.spec || defaultSpec : args.input;
    requireValue(input, 'missing input');
    const document = JSON.parse(readFileSync(input, 'utf8'));
    const result = mode === 'phenotypes' ? compilePhenotypes(document)
      : mode === 'carrier' ? compileCarrier(document)
      : mode === 'connection' ? compileConnection(document)
      : (() => { throw new Error('unknown coordination compiler mode'); })();
    writeFileSync(args.out, (Array.isArray(result) ? result : [result]).map(value => JSON.stringify(value)).join('\n') + '\n');
    console.log(JSON.stringify({ mode, rows: Array.isArray(result) ? result.length : 1,
      truth: 'public carrier and declared lifecycle only; no authority effect' }));
  } catch (error) { console.error(`coordination family compiler: ${error.message}`); process.exitCode = 2; }
}
