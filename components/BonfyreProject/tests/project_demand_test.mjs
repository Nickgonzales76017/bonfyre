import { afterEach, expect, test } from 'bun:test';
import { createHash } from 'node:crypto';
import { mkdirSync, mkdtempSync, readFileSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const project = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const temporary = [];
afterEach(() => { for (const path of temporary.splice(0)) rmSync(path, { recursive: true, force: true }); });
const sha = value => createHash('sha256').update(value).digest('hex');
function fixture() {
  const root = mkdtempSync(join(tmpdir(), 'bonfyre-demand-test-'));
  temporary.push(root);
  const estate = join(root, 'estate'), discovery = join(root, 'discovery'), foreign = join(root, 'foreign');
  for (const path of [estate, discovery, foreign, join(discovery, 'bodies'), join(foreign, 'bodies'), join(foreign, 'receipts')])
    mkdirSync(path, { recursive: true });
  return { root, estate, discovery, foreign };
}
function run({ estate, discovery, foreign, manifest, out }) {
  return Bun.spawnSync([join(project, 'tools/demand-match.sh'), out], {
    env: { ...process.env, ESTATE_DIR: estate, DISCOVERY_DIR: discovery,
      BONFYRE_FOREIGN_ROOT: foreign, DEMAND_DESCRIPTOR_EVIDENCE: manifest },
  });
}

test('standing DemandBook entrypoint binds declared supply, credential classes and receipt-backed descriptors', () => {
  const { root, estate, discovery, foreign } = fixture();
  const alpha = JSON.stringify({ paths: { '/search': { get: { parameters: [
    { name: 'url', required: true }, { name: 'token', required: true }] } } } });
  const beta = JSON.stringify({ queryParams: { properties: { api_key: {} } } });
  writeFileSync(join(discovery, 'bodies/external_alpha__openapi.body'), alpha);
  writeFileSync(join(discovery, 'bodies/external_beta__agentcard.body'), beta);
  const gamma = JSON.stringify({ openapi: '3.1.0', paths: { '/charge': { post: { requestBody: {
    content: { 'application/json': { schema: { required: ['domain'] } } } } } } } });
  const gammaPath = join(foreign, 'bodies/descriptor-gamma.spec.json');
  writeFileSync(gammaPath, gamma);
  const mapped = JSON.stringify({ openapi: '3.1.0', paths: { '/lookup': { get: {
    parameters: [{ name: 'id', required: true }] } } }, components: { securitySchemes: {
    X402Pay: { type: 'apiKey', name: 'payment-signature' }, L402Auth: { type: 'http', name: 'Authorization' },
  } } });
  const mappedPath = join(foreign, 'bodies/descriptor-mapped.spec.json');
  writeFileSync(mappedPath, mapped);
  writeFileSync(join(foreign, 'receipts/descriptor-mapped.json'), JSON.stringify({ content_sha256: sha(mapped),
    source_url: 'https://Mapped.Test/openapi.json', http_status: 200,
    observation_kind: 'source_observed', policy_decision: 'allow' }) + '\n');
  writeFileSync(join(foreign, 'bodies/descriptor-unbound.spec.json'), mapped);
  const manifest = join(root, 'manifest.jsonl');
  writeFileSync(manifest, [
    JSON.stringify({ schema: 'bonfyre-demand-descriptor.v1', occurrence_id: 'external:gamma',
      descriptor_path: gammaPath, descriptor_sha256: sha(gamma), descriptor_url: 'https://gamma.test/openapi.json',
      payment_protocols: ['x402'], credential_schemes: ['ApiKey'], economic_schemes: ['X402Pay'] }),
    JSON.stringify({ schema: 'bonfyre-demand-descriptor.v1', occurrence_id: 'external:missing',
      descriptor_path: join(foreign, 'bodies/missing.spec.json'), descriptor_sha256: sha(gamma) }),
    JSON.stringify({ schema: 'bonfyre-demand-descriptor.v1', occurrence_id: 'external:mismatch',
      descriptor_path: gammaPath, descriptor_sha256: sha('wrong') }),
    '{malformed',
  ].join('\n') + '\n');
  const out = join(estate, 'demand-evidence.jsonl');
  const result = run({ estate, discovery, foreign, manifest, out });
  expect(result.exitCode).toBe(0);
  const summary = JSON.parse(result.stdout.toString());
  expect(summary).toMatchObject({ services_declaring_inputs: 4, eligible_estate_can_fill: 3,
    asks_only_for_credentials: 1, foreign_twin_to_demand_book: 4,
    descriptor_manifest_records: 3, descriptor_manifest_admitted: 1,
    descriptor_manifest_missing: 1, descriptor_digest_mismatch: 1, descriptor_manifest_invalid: 1,
    descriptor_receipt_scanned: 2, descriptor_receipt_admitted: 1,
    descriptor_receipt_unbound: 1, payment_protocol_services: 2,
    payment_protocols: { x402: 2, l402: 1 }, evidence_rows: 8, spend: 0 });
  const rows = readFileSync(out, 'utf8').trim().split('\n').map(JSON.parse);
  const alphaEligible = rows.find(row => row.occurrence_id === 'external:alpha' && row.axis === 'capability');
  expect(alphaEligible).toMatchObject({ state: 'eligible', fillable_by_estate: ['url'],
    evidence_sha256: sha('external:alpha\x1furl\x1feligible'), basis_sha256: sha(alpha) });
  expect(alphaEligible.cannot_infer).toContain('eligible -> authorized or activated; nobody called and nobody granted');
  const betaBlocked = rows.find(row => row.occurrence_id === 'external:beta' && row.schema === 'bonfyre-credential-demand.v1');
  expect(betaBlocked.credential_classes).toEqual(['issued']);
  expect(rows.some(row => row.occurrence_id === 'external:beta' && row.state === 'eligible')).toBe(false);
  const mappedRoutable = rows.find(row => row.occurrence_id === 'external:mapped-test' && row.axis === 'discovery');
  expect(mappedRoutable).toMatchObject({ declared_payment_protocols: ['l402', 'x402'],
    declared_credential_schemes: ['L402Auth'], declared_economic_schemes: ['X402Pay'],
    descriptor_url: 'https://Mapped.Test/openapi.json', sources: ['foreign-receipt'] });
});

test('unsafe retained body refuses without replacing prior DemandBook evidence', () => {
  const { root, estate, discovery, foreign } = fixture();
  const target = join(root, 'target.json'), body = join(discovery, 'bodies/external_bad__openapi.body');
  writeFileSync(target, '{"paths":{}}');
  symlinkSync(target, body);
  const out = join(estate, 'demand-evidence.jsonl');
  writeFileSync(out, 'prior-valid-report\n');
  const result = run({ estate, discovery, foreign, manifest: '', out });
  expect(result.exitCode).not.toBe(0);
  expect(readFileSync(out, 'utf8')).toBe('prior-valid-report\n');
});

test('merged declarations bind both source digests and invalidate on either body change', () => {
  const { estate, discovery, foreign } = fixture();
  const firstPath = join(discovery, 'bodies/external_combo__openapi.body');
  const secondPath = join(discovery, 'bodies/external_combo__agentcard.body');
  const first = JSON.stringify({ paths: { '/lookup': { get: { parameters: [{ name: 'url', required: true }] } } } });
  const second = JSON.stringify({ queryParams: { properties: { domain: {} } } });
  writeFileSync(firstPath, first); writeFileSync(secondPath, second);
  const out = join(estate, 'demand-evidence.jsonl');
  const inspect = () => {
    const result = run({ estate, discovery, foreign, manifest: '', out });
    expect(result.exitCode).toBe(0);
    const rows = readFileSync(out, 'utf8').trim().split('\n').map(JSON.parse);
    return rows.filter(row => row.occurrence_id === 'external:combo');
  };
  const before = inspect();
  expect(before).toHaveLength(2);
  const members = [sha(first), sha(second)].sort();
  const aggregate = sha(`bonfyre-demand-basis.v1\0${members.join('\n')}`);
  for (const row of before) expect(row).toMatchObject({ basis_sha256: aggregate,
    basis_kind: 'aggregate_retained_bodies_sha256', basis_members: members,
    declared_inputs: ['domain', 'url'] });
  expect(before[0].evidence_sha256).not.toBe(sha('external:combo\x1fdeclared'));
  writeFileSync(firstPath, JSON.stringify({ paths: { '/lookup': { get: { parameters: [
    { name: 'slug', required: true }] } } } }));
  const after = inspect();
  expect(after[0].basis_sha256).not.toBe(aggregate);
  expect(after[1].basis_sha256).toBe(after[0].basis_sha256);
  expect(after[0].evidence_sha256).not.toBe(before[0].evidence_sha256);
});

test('invalid v1 manifest cannot suppress a later receipt-bound descriptor fallback', () => {
  const { root, estate, discovery, foreign } = fixture();
  const doc = JSON.stringify({ openapi: '3.1.0', paths: { '/lookup': { get: {
    parameters: [{ name: 'id', required: true }] } } } });
  const path = join(foreign, 'bodies/descriptor-later.spec.json');
  writeFileSync(path, doc);
  writeFileSync(join(foreign, 'receipts/descriptor-later.json'), JSON.stringify({
    content_sha256: sha(doc), source_url: 'https://later.test/openapi.json', http_status: 200,
    observation_kind: 'source_observed', policy_decision: 'allow' }) + '\n');
  const manifest = join(root, 'manifest.jsonl');
  writeFileSync(manifest, [
    JSON.stringify({ schema: 'bonfyre-demand-descriptor.v1', occurrence_id: 'external:false',
      descriptor_path: path, descriptor_sha256: '' }),
    JSON.stringify({ schema: 'bonfyre-demand-descriptor.v1', occurrence_id: 'external:false',
      descriptor_path: path, descriptor_sha256: sha('wrong') }),
  ].join('\n') + '\n');
  const out = join(estate, 'demand-evidence.jsonl');
  const result = run({ estate, discovery, foreign, manifest, out });
  expect(result.exitCode).toBe(0);
  expect(JSON.parse(result.stdout.toString())).toMatchObject({ descriptor_manifest_records: 2,
    descriptor_manifest_invalid: 1, descriptor_digest_mismatch: 1,
    descriptor_receipt_scanned: 1, descriptor_receipt_admitted: 1, eligible_estate_can_fill: 1 });
  const rows = readFileSync(out, 'utf8').trim().split('\n').map(JSON.parse);
  expect(rows.map(row => row.occurrence_id)).toEqual(['external:later-test', 'external:later-test']);
});

test('legacy manifest without a digest requires a matching ForeignTwin receipt', () => {
  const { root, estate, discovery, foreign } = fixture();
  const host = 'legacy.test';
  const twin = `descriptor-${sha(host).slice(0, 16)}`;
  const doc = JSON.stringify({ openapi: '3.1.0', paths: { '/lookup': { get: {
    parameters: [{ name: 'url', required: true }] } } } });
  writeFileSync(join(foreign, `bodies/${twin}.spec.json`), doc);
  const manifest = join(root, 'manifest.jsonl');
  writeFileSync(manifest, JSON.stringify({ described: 'legacy-test', host }) + '\n');
  const out = join(estate, 'demand-evidence.jsonl');
  const first = run({ estate, discovery, foreign, manifest, out });
  expect(first.exitCode).toBe(0);
  expect(JSON.parse(first.stdout.toString())).toMatchObject({ descriptor_manifest_records: 1,
    descriptor_manifest_admitted: 0, descriptor_manifest_unverified: 1,
    descriptor_receipt_unbound: 1, eligible_estate_can_fill: 0 });
  writeFileSync(join(foreign, `receipts/${twin}.json`), JSON.stringify({
    content_sha256: sha(doc), source_url: 'https://legacy.test/openapi.json', http_status: 200,
    observation_kind: 'source_observed', policy_decision: 'allow' }) + '\n');
  const second = run({ estate, discovery, foreign, manifest, out });
  expect(second.exitCode).toBe(0);
  expect(JSON.parse(second.stdout.toString())).toMatchObject({ descriptor_manifest_admitted: 1,
    descriptor_manifest_unverified: 0, descriptor_receipt_scanned: 0, eligible_estate_can_fill: 1 });
});
