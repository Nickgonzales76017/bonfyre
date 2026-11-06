#!/usr/bin/env bun
// DemandBook projection from retained discovery and digest-bound ForeignTwin descriptors.
// A declared, fillable input is eligibility, never authorization or payment authority.
import { createHash } from 'node:crypto';
import { readdirSync } from 'node:fs';
import { homedir } from 'node:os';
import { isAbsolute, join, resolve, sep } from 'node:path';
import { atomicReport, observationReader } from './economic-reports.mjs';

const sha256 = bytes => createHash('sha256').update(bytes).digest('hex');
const object = value => value !== null && typeof value === 'object' && !Array.isArray(value);
const sorted = values => [...values].sort();
const lines = bytes => bytes.toString('utf8').split(/\r?\n/).filter(Boolean);
const estateDir = () => process.env.ESTATE_DIR || join(homedir(), '.bonfyre/estate');
const discoveryDir = () => process.env.DISCOVERY_DIR || join(homedir(), '.bonfyre/discovery');
const SUPPLIABLE = new Set(['url', 'id', 'domain', 'slug']);
const CREDENTIAL_CLASSES = {
  public_id: new Set(['address', 'email', 'domain', 'wallet', 'account', 'sender', 'payto']),
  produced: new Set(['signature', 'sig', 'proof']),
  issued: new Set(['token', 'key', 'api_key', 'apikey', 'secret', 'password', 'bearer']),
};
const PAYMENT_HEADERS = new Set(['payment-signature', 'x-payment', 'x-payment-response', 'payment-required', 'x402-payment']);
const SHA256_HEX = /^[a-f0-9]{64}$/i;
export const aggregateDemandBasis = digests => {
  const members = sorted(new Set(digests));
  if (!members.length || members.some(digest => !SHA256_HEX.test(digest))) throw new Error('invalid demand basis members');
  return { basis_sha256: members.length === 1 ? members[0] :
    sha256(`bonfyre-demand-basis.v1\0${members.join('\n')}`),
  basis_kind: members.length === 1 ? 'single_retained_body_sha256' : 'aggregate_retained_bodies_sha256',
  basis_members: members };
};

function names(path, test) {
  let entries;
  try { entries = readdirSync(path); }
  catch (error) { if (error.code === 'ENOENT') return []; throw error; }
  if (entries.length > 100000) throw new Error(`descriptor directory exceeds entry budget: ${path}`);
  return entries.filter(test).sort();
}
function stringList(value, label) {
  if (value == null) return [];
  if (!Array.isArray(value) || value.some(item => typeof item !== 'string')) throw new Error(`invalid ${label}`);
  return value.filter(Boolean);
}
function parseDeclaration(raw) {
  let doc;
  try { doc = JSON.parse(raw.toString('utf8')); }
  catch { return null; }
  if (!object(doc)) return null;
  const asks = new Set();
  let operations = 0;
  if (object(doc.paths)) for (const item of Object.values(doc.paths)) {
    if (!object(item)) continue;
    for (const spec of Object.values(item)) {
      if (!object(spec)) continue;
      operations++;
      if (Array.isArray(spec.parameters)) for (const param of spec.parameters)
        if (object(param) && param.required && param.name) asks.add(String(param.name).toLowerCase());
      const content = spec.requestBody?.content;
      if (object(content)) for (const media of Object.values(content)) {
        const required = media?.schema?.required;
        if (Array.isArray(required)) for (const field of required) asks.add(String(field).toLowerCase());
      }
    }
  }
  for (const match of JSON.stringify(doc).matchAll(/"queryParams"\s*:\s*\{\s*"properties"\s*:\s*\{(.{0,800}?)\}/gs))
    for (const key of match[1].matchAll(/"([a-zA-Z_][a-zA-Z0-9_]{1,30})"\s*:\s*\{/g)) asks.add(key[1].toLowerCase());
  return { doc, asks, operations };
}
function descriptorSecurity(doc) {
  const schemes = doc.components?.securitySchemes || doc.securityDefinitions || {};
  const credentials = new Set(), economic = new Set(), carriers = new Set();
  if (!object(schemes)) return { credentials, economic, carriers };
  for (const [name, scheme] of Object.entries(schemes)) {
    if (!object(scheme)) continue;
    const key = name.toLowerCase(), header = String(scheme.name || '').toLowerCase();
    if (key.includes('x402') || PAYMENT_HEADERS.has(header)) { economic.add(name); carriers.add('x402'); }
    else credentials.add(name);
    if (key.includes('mpp')) carriers.add('mpp');
    if (key.includes('l402')) carriers.add('l402');
  }
  return { credentials, economic, carriers };
}
function credentialClasses(asks) {
  return Object.entries(CREDENTIAL_CLASSES).filter(([, names]) => [...asks].some(name => names.has(name))).map(([label]) => label).sort();
}
function within(path, root) {
  if (!root || !isAbsolute(path)) return false;
  const file = resolve(path), base = resolve(root);
  return file.startsWith(`${base}${sep}`);
}
function receiptBinding(read, foreignRoot, twin, digest) {
  const matches = [];
  for (const suffix of ['', '-desc']) {
    const path = join(foreignRoot, 'receipts', `${twin}${suffix}.json`);
    for (const line of lines(read(path, { optional: true }))) {
      let row;
      try { row = JSON.parse(line); } catch { continue; }
      if (!object(row) || row.content_sha256 !== digest || typeof row.source_url !== 'string') continue;
      const status = Number(row.http_status || 0);
      if (status >= 200 && status < 300 && ['source_observed', 'repeat_observed'].includes(row.observation_kind) &&
          ['', 'allow'].includes(row.policy_decision || '')) matches.push({ source_url: row.source_url, receipt_path: path });
    }
  }
  matches.sort((a, b) => a.source_url.localeCompare(b.source_url) || a.receipt_path.localeCompare(b.receipt_path));
  return matches[0] || null;
}

export function matchDemand({ estate = estateDir(), discovery = discoveryDir(),
  out = join(estate, 'demand-evidence.jsonl'), descriptorEvidence = process.env.DEMAND_DESCRIPTOR_EVIDENCE || '',
  foreignRoot = process.env.BONFYRE_FOREIGN_ROOT || '', now = new Date() } = {}) {
  const read = observationReader();
  const stamp = now.toISOString().replace(/\.\d{3}Z$/, 'Z');
  const demand = new Map(), operations = new Map();
  const absorb = (id, raw, { paymentProtocols = [], credentialSchemes = [], economicSchemes = [],
    descriptorUrl = '', source = 'legacy-discovery' } = {}) => {
    const parsed = parseDeclaration(raw);
    if (!parsed) return false;
    const protocols = new Set(paymentProtocols.filter(Boolean).map(item => item.toLowerCase()));
    const credentials = new Set(credentialSchemes.filter(Boolean));
    const economic = new Set(economicSchemes.filter(Boolean));
    if (!parsed.asks.size && !protocols.size && !credentials.size && !economic.size) return false;
    const prior = demand.get(id) || { asks: new Set(), digests: new Set(), payment_protocols: new Set(),
      credential_schemes: new Set(), economic_schemes: new Set(), descriptor_url: descriptorUrl, sources: new Set() };
    for (const value of parsed.asks) prior.asks.add(value);
    for (const value of protocols) prior.payment_protocols.add(value);
    for (const value of credentials) prior.credential_schemes.add(value);
    for (const value of economic) prior.economic_schemes.add(value);
    prior.digests.add(sha256(raw));
    if (descriptorUrl) prior.descriptor_url = descriptorUrl;
    prior.sources.add(source);
    demand.set(id, prior);
    operations.set(id, (operations.get(id) || 0) + parsed.operations);
    return true;
  };

  for (const name of names(join(discovery, 'bodies'), name => name.endsWith('.body'))) {
    const raw = read(join(discovery, 'bodies', name));
    const id = `external:${name.slice(0, -5).replace(/__(openapi|x402|agentcard)$/, '').replace('external_', '')}`;
    absorb(id, raw);
  }
  const manifest = { records: 0, admitted: 0, digest_mismatch: 0, missing: 0, invalid: 0, unverified: 0 };
  const manifestPaths = new Set();
  if (descriptorEvidence) for (const line of lines(read(descriptorEvidence, { optional: true, limit: 64 * 1024 * 1024 }))) {
    let row;
    try { row = JSON.parse(line); } catch { manifest.invalid++; continue; }
    if (!object(row)) { manifest.invalid++; continue; }
    let path, expectedDigest, paymentProtocols, credentialSchemes, economicSchemes, descriptorUrl, id, legacy = false, twin = '';
    try {
      if (row.schema === 'bonfyre-demand-descriptor.v1') {
        manifest.records++;
        path = row.descriptor_path || ''; expectedDigest = row.descriptor_sha256 || '';
        paymentProtocols = stringList(row.payment_protocols, 'payment protocols');
        credentialSchemes = stringList(row.credential_schemes, 'credential schemes');
        economicSchemes = stringList(row.economic_schemes, 'economic schemes');
        descriptorUrl = row.descriptor_url || '';
        id = row.occurrence_id || `external:${String(row.host || '').toLowerCase().replace(/[^a-z0-9-]+/g, '-').replace(/^-|-$/g, '')}`;
      } else if (!row.schema && row.described && row.host && foreignRoot) {
        manifest.records++;
        legacy = true;
        const host = String(row.host).toLowerCase();
        twin = `descriptor-${sha256(host).slice(0, 16)}`;
        path = join(foreignRoot, 'bodies', `${twin}.spec.json`); expectedDigest = '';
        credentialSchemes = []; economicSchemes = []; paymentProtocols = [];
        descriptorUrl = ''; id = `external:${row.described}`;
      } else continue;
      if (typeof path !== 'string' || typeof expectedDigest !== 'string' || typeof descriptorUrl !== 'string' ||
          typeof id !== 'string' || !id || (!legacy && !SHA256_HEX.test(expectedDigest)))
        throw new Error('invalid descriptor manifest identity or digest');
    } catch { manifest.invalid++; continue; }
    if (!path) { manifest.missing++; continue; }
    if (!within(path, foreignRoot) && !within(path, discovery)) { manifest.invalid++; continue; }
    let raw;
    try { raw = read(path); }
    catch (error) { if (error.code === 'ENOENT') { manifest.missing++; continue; } throw error; }
    const digest = sha256(raw);
    if (!legacy && expectedDigest.toLowerCase() !== digest) { manifest.digest_mismatch++; continue; }
    if (legacy) {
      const binding = receiptBinding(read, foreignRoot, twin, digest);
      if (!binding) { manifest.unverified++; continue; }
      const parsed = parseDeclaration(raw);
      if (!parsed || !(parsed.doc.openapi || parsed.doc.swagger)) { manifest.invalid++; continue; }
      const security = descriptorSecurity(parsed.doc);
      paymentProtocols = sorted(security.carriers);
      credentialSchemes = sorted(security.credentials);
      economicSchemes = sorted(security.economic);
      descriptorUrl = binding.source_url;
    }
    if (absorb(id, raw, { paymentProtocols, credentialSchemes, economicSchemes, descriptorUrl, source: 'connection-describe' })) {
      manifest.admitted++;
      manifestPaths.add(resolve(path));
    } else manifest.invalid++;
  }

  const receipts = { scanned: 0, admitted: 0, unbound: 0, invalid: 0 };
  if (foreignRoot) for (const name of names(join(foreignRoot, 'bodies'), name => /^descriptor-.*\.spec\.json$/.test(name))) {
    const path = join(foreignRoot, 'bodies', name);
    if (manifestPaths.has(resolve(path))) continue;
    receipts.scanned++;
    const raw = read(path);
    const parsed = parseDeclaration(raw);
    if (!parsed || !(parsed.doc.openapi || parsed.doc.swagger)) { receipts.invalid++; continue; }
    const binding = receiptBinding(read, foreignRoot, name.slice(0, -'.spec.json'.length), sha256(raw));
    if (!binding) { receipts.unbound++; continue; }
    let url;
    try { url = new URL(binding.source_url); } catch { receipts.unbound++; continue; }
    if (!['http:', 'https:'].includes(url.protocol) || !url.host) { receipts.unbound++; continue; }
    const provider = url.host.toLowerCase().replace(/[^a-z0-9-]+/g, '-').replace(/^-|-$/g, '').slice(0, 60);
    const security = descriptorSecurity(parsed.doc);
    if (absorb(`external:${provider}`, raw, { paymentProtocols: sorted(security.carriers),
      credentialSchemes: sorted(security.credentials), economicSchemes: sorted(security.economic),
      descriptorUrl: binding.source_url, source: 'foreign-receipt' })) receipts.admitted++;
  }

  let eligible = 0, credentialOnly = 0, paymentServices = 0, written = 0;
  const paymentCounts = new Map();
  const summary = atomicReport(out, write => {
    for (const [id, info] of [...demand].sort(([a], [b]) => a.localeCompare(b))) {
      const asks = sorted(info.asks), protocols = sorted(info.payment_protocols);
      const credentialSchemes = sorted(info.credential_schemes), economicSchemes = sorted(info.economic_schemes);
      const fillable = asks.filter(ask => SUPPLIABLE.has(ask));
      const basis = aggregateDemandBasis(info.digests);
      const declaredEvidence = basis.basis_members.length === 1 ? sha256(`${id}\x1fdeclared`) :
        sha256(`bonfyre-demand-evidence.v2\0${id}\0${basis.basis_sha256}\0declared`);
      const eligibleEvidence = basis.basis_members.length === 1 ? sha256(`${id}\x1f${fillable.join(',')}\x1feligible`) :
        sha256(`bonfyre-demand-evidence.v2\0${id}\0${basis.basis_sha256}\0${fillable.join(',')}\0eligible`);
      const common = { declared_inputs: asks, declared_operations: operations.get(id) || 0,
        declared_payment_protocols: protocols, declared_credential_schemes: credentialSchemes,
        declared_economic_schemes: economicSchemes, ...basis };
      write(`${JSON.stringify({ schema: 'bonfyre-owner-participation.v1', occurrence_id: id, axis: 'discovery',
        state: 'routable', owner: 'DemandBook', from_owner: 'ForeignTwin', to_owner: 'DemandBook',
        evidence_sha256: declaredEvidence, produced_by: 'bonfyre-project',
        recorded_at: stamp, ...common, descriptor_url: info.descriptor_url || '', sources: sorted(info.sources),
        cannot_infer: ['a declaration -> a service anyone wants called',
          'a payment carrier declaration -> spend approval, payment verification or settlement'] })}\n`);
      written++;
      if (protocols.length) { paymentServices++; for (const value of protocols) paymentCounts.set(value, (paymentCounts.get(value) || 0) + 1); }
      if (!fillable.length) {
        if (asks.some(ask => Object.values(CREDENTIAL_CLASSES).some(names => names.has(ask))) || credentialSchemes.length) {
          credentialOnly++;
          write(`${JSON.stringify({ schema: 'bonfyre-credential-demand.v1', occurrence_id: id,
            ...basis, declared_inputs: asks, credential_classes: credentialClasses(info.asks),
            declared_credential_schemes: credentialSchemes, declared_economic_schemes: economicSchemes,
            declared_payment_protocols: protocols,
            blocked_by: {
              public_id: 'declared: the institution publishes an Ed25519 public key as its identity. A payment address is a separate declaration and is deliberately not made here',
              produced: 'available: bonfyre-sign holds the institution Ed25519 identity and signs; a payment signature remains separate and deliberately unheld',
              issued: 'registration, which is a commitment a person makes',
            }, cannot_infer: [
              'a public identifier -> a secret; an x402 payTo is published on purpose',
              'a blocked class -> an unsolvable one',
              'a payment carrier declaration -> a credential or payment authority'] })}\n`);
          written++;
        }
        continue;
      }
      write(`${JSON.stringify({ schema: 'bonfyre-owner-participation.v1', occurrence_id: id,
        axis: 'capability', state: 'eligible', owner: 'SupplyMatch', from_owner: 'DemandBook', to_owner: 'SupplyMatch',
        evidence_sha256: eligibleEvidence,
        produced_by: 'bonfyre-project', recorded_at: stamp, ...common, fillable_by_estate: fillable,
        cannot_infer: [
          'eligible -> authorized or activated; nobody called and nobody granted',
          'a declared input -> this service asking this estate for it',
          'fillable -> adequate; the service still decides what it accepts',
          'a payment carrier declaration -> a selected or settled payment rail'] })}\n`);
      eligible++; written++;
    }
    return { schema: 'bonfyre-demand-match.v1', services_declaring_inputs: demand.size,
      declared_operations: [...operations.values()].reduce((sum, value) => sum + value, 0),
      eligible_estate_can_fill: eligible, asks_only_for_credentials: credentialOnly,
      foreign_twin_to_demand_book: demand.size, payment_protocol_services: paymentServices,
      payment_protocols: Object.fromEntries([...paymentCounts].sort(([a], [b]) => a.localeCompare(b))),
      descriptor_manifest_records: manifest.records, descriptor_manifest_admitted: manifest.admitted,
      descriptor_manifest_missing: manifest.missing, descriptor_digest_mismatch: manifest.digest_mismatch,
      descriptor_manifest_invalid: manifest.invalid, descriptor_manifest_unverified: manifest.unverified,
      descriptor_receipt_scanned: receipts.scanned,
      descriptor_receipt_admitted: receipts.admitted, descriptor_receipt_unbound: receipts.unbound,
      descriptor_receipt_invalid: receipts.invalid,
      credential_decomposition: 'public_id declared as an Ed25519 identity, produced now available via bonfyre-sign, issued still needs a signup, keyless needs nothing',
      evidence_rows: written, spend: 0,
      truth: 'declarations_services_publish_matched_against_what_this_estate_holds' };
  });
  return summary;
}

if (import.meta.main) {
  try {
    const [out] = process.argv.slice(2);
    console.log(JSON.stringify(matchDemand({ out: out || join(estateDir(), 'demand-evidence.jsonl') })));
  } catch (error) { console.error(`demand match: ${error.message}`); process.exitCode = 1; }
}
