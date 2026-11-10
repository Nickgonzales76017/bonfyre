import { test, expect } from 'bun:test';
import { Database } from 'bun:sqlite';
import { createHash } from 'node:crypto';
import { existsSync, mkdtempSync, mkdirSync, readFileSync, realpathSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { basename, join } from 'node:path';
import { createCanaryStore, canonicalJson, digestValue, treeDigest, VERSION, CERT_SCHEMA } from '../canary_store.mjs';

async function fixture(check) {
  const root = realpathSync(mkdtempSync(join(tmpdir(), 'bonfyre-canary-store-'))), stores = [];
  const home = join(root, 'canary-r4'), legacyRoot = join(root, 'shadow-r3/certificates'), work = join(root, 'fixture');
  mkdirSync(work); writeFileSync(join(work, 'sample.txt'), 'fixture\n');
  let time = 1800000000.25, version = '1.0', hook;
  const calls = [];
  const inspectProvider = name => ({ path: `/fixture/${name}`, kind: 'native', sha256: 'a'.repeat(64) });
  const run = async argv => {
    calls.push(argv); await hook?.();
    return { argv, exit_code: 0, stdout: `${basename(argv[0])} ${version}\n`, stderr: '', duration_ms: 1 };
  };
  const open = options => {
    const store = createCanaryStore({ home, legacyRoot, now: () => time, run, inspectProvider, ...options });
    stores.push(store); return store;
  };
  try { return await check({ root, home, legacyRoot, work, open, calls, now: () => time,
    advance: seconds => { time += seconds; }, setVersion: value => { version = value; }, setHook: value => { hook = value; } }); }
  finally { for (const store of stores) store.close(); rmSync(root, { recursive: true, force: true }); }
}
const seal = (store, work, extra = {}) => store.sealCertificate({ provider: 'rtk', capability: 'filter',
  state: 'passed', behavior_score: 1, fixture_root: work, evidence: [{ observed: true }], claims: [{ kind: 'fixture' }],
  cleanup_verified: true, run_id: 'run-fixture', ...extra });
const enable = store => store.upsertPolicy({ provider: 'rtk', capability: 'filter', fallback_provider: 'bonfyre-native',
  percentage: 25, effect_classes: ['READ_ONLY', 'PURE'], enable: true });
function insideMission(capability = 'filter') {
  for (let index = 0; index < 1000; index++) {
    const mission = `mission-${index}`;
    const hash = createHash('sha256').update(`${mission}\0\0${capability}`).digest('hex');
    if (Number.parseInt(hash.slice(0, 8), 16) % 10000 < 2500) return mission;
  }
  throw new Error('fixture bucket missing');
}
const route = store => store.routeDecide({ mission_id: insideMission(), capability: 'filter', effect_class: 'READ_ONLY' });

test('construction is lazy; status shape and empty receipt chain remain compatible', () => fixture(async ({ open, home }) => {
  const store = open();
  expect(existsSync(home)).toBe(false);
  const status = await store.status();
  expect(status).toMatchObject({ schema: 'bonfyre.canary-control.v1', version: VERSION,
    service: 'bonfyre-fixture-canary-r4', policies: {}, decisions: {}, outcomes: {},
    receipt_chain: { verified: true, receipts: 0, head: null, failures: [] } });
  expect(status.authority).toMatchObject({ max_canary_percentage: 25, automatic_promotion: false, automatic_suspension: true });
}));

test('canonical ASCII escaping and source-number verification preserve historical Python digests', () => fixture(async ({ open, legacyRoot, now }) => {
  expect(canonicalJson({ z: 'ÿ😀', a: 0.000001, b: -0 })).toBe('{"a":1e-06,"b":-0.0,"z":"\\u00ff\\ud83d\\ude00"}');
  const store = open();
  const body = `{"behavior_score":1.0,"capability":"filter","cleanup_verified":true,"expires_at":${now() + 1000},"provider":"rtk","provider_version":"rtk 1.0","schema":"${CERT_SCHEMA}","state":"passed","unicode":"\\u00ff"}`;
  const digest = createHash('sha256').update(body).digest('hex');
  mkdirSync(join(legacyRoot, 'rtk'), { recursive: true });
  writeFileSync(join(legacyRoot, 'rtk/filter.json'), body.slice(0, -1) + `,"digest":"${digest}"}`);
  expect((await store.validateCertificate('rtk', 'filter')).valid).toBe(true);
  const policy = await enable(store);
  expect(policy.certificate_digest).toBe(digest);
}));

test('receipt chain is serialized across stores and binds database metadata to documents', () => fixture(({ open }) => {
  const a = open(), b = open();
  const one = a.writeReceipt({ kind: 'fixture', subject: null, state: 'passed', payload: { amount: 1.25 } });
  const two = b.writeReceipt({ kind: 'fixture', subject: 'two', state: 'failed', payload: {} });
  expect(two.previous_digest).toBe(one.digest);
  expect(a.verifyReceipts()).toMatchObject({ verified: true, receipts: 2, head: two.digest });
  const db = new Database(a.dbPath);
  db.query("UPDATE receipts SET state='passed' WHERE seq=?").run(two.seq); db.close();
  expect(a.verifyReceipts().failures).toContainEqual({ seq: two.seq, reason: 'row_document_mismatch' });
  writeFileSync(one.path, '{bad');
  expect(a.verifyReceipts().verified).toBe(false);
}));

test('certificate sealing preserves both current and R3 projection and records backups', () => fixture(async ({ open, work, legacyRoot }) => {
  const store = open(), certificate = await seal(store, work);
  expect(certificate.fixture_digest).toBe(treeDigest(work));
  expect(certificate.legacy_path).toBe(join(legacyRoot, 'rtk/filter.json'));
  expect(readFileSync(certificate.path, 'utf8')).toBe(readFileSync(certificate.legacy_path, 'utf8'));
  expect((await store.validateCertificate('rtk', 'filter')).valid).toBe(true);
  await seal(store, work, { state: 'partial', behavior_score: 0.5 });
  expect((await store.listCertificates())).toHaveLength(1);
  expect((await store.listCertificates())[0]).toMatchObject({ valid: true, state: 'partial' });
  await expect(enable(store)).rejects.toThrow('only passed');
  expect(store.verifyReceipts().verified).toBe(true);
}));

test('certificate gates reject wrong identity, cleanup, expired and unknown probe without success laundering', () => fixture(async ({ open, work, advance }) => {
  const store = open(); await seal(store, work, { cleanup_verified: false });
  expect((await store.validateCertificate('rtk', 'filter')).reason).toBe('cleanup_not_verified');
  await seal(store, work, { ttl_days: 1 }); advance(86400);
  expect((await store.validateCertificate('rtk', 'filter')).reason).toBe('expired');
  const cert = await seal(store, work);
  const raw = JSON.parse(readFileSync(cert.path, 'utf8')); delete raw.digest; raw.provider = 'elsewhere';
  writeFileSync(cert.path, canonicalJson({ ...raw, digest: digestValue(raw) }));
  expect((await store.validateCertificate('rtk', 'filter')).reason).toBe('identity_mismatch');
  const unavailable = open({ inspectProvider: () => ({ path: null, kind: 'missing' }) });
  await seal(store, work);
  const result = await unavailable.validateCertificate('rtk', 'filter');
  expect(result.valid).toBe(false); expect(result.probe.exit_code).toBe(127);
}));

test('routing preserves exact hash buckets and rejects authority expansion or provider drift', () => fixture(async ({ open, work, setVersion }) => {
  const store = open(); await seal(store, work); const policy = await enable(store);
  expect((await route(store))).toMatchObject({ mode: 'canary', reason: 'eligible', selected_provider: 'rtk', policy_id: policy.id });
  const denied = await store.routeDecide({ mission_id: insideMission(), capability: 'filter', effect_class: 'EXTERNAL_WRITE' });
  expect(denied).toMatchObject({ mode: 'fallback', reason: 'effect_class_denied', selected_provider: 'bonfyre-native' });
  await expect(store.routeDecide({ mission_id: insideMission(), capability: 'filter', effect_class: 'EXTERNAL_WRITE', fallback_provider: 'rtk' })).rejects.toThrow('fallback');
  setVersion('2.0');
  expect((await route(store)).reason).toBe('certificate_provider_version_mismatch');
  expect(store.policy(policy.id).state).toBe('suspended');
  expect((await route(store)).reason).toBe('no_enabled_policy');
  expect(store.verifyReceipts().verified).toBe(true);
}));

test('all policy state transitions preserve passed certificate and exact digest binding', () => fixture(async ({ open, work }) => {
  const store = open(); await seal(store, work); const policy = await enable(store);
  for (const state of ['proposed', 'enabled', 'suspended', 'disabled']) expect((await store.setPolicyState(policy.id, state)).state).toBe(state);
  await seal(store, work, { evidence: [{ replacement: true }] });
  await expect(store.setPolicyState(policy.id, 'enabled')).rejects.toThrow('binding changed');
  for (const extra of [{ percentage: 26 }, { percentage: 0 }, { effect_classes: ['NETWORK_WRITE'] },
    { fallback_provider: 'rtk' }, { enable: 'true' }]) {
    await expect(store.upsertPolicy({ provider: 'rtk', capability: 'filter', fallback_provider: 'bonfyre-native',
      percentage: 5, effect_classes: ['READ_ONLY'], ...extra })).rejects.toThrow();
  }
}));

test('outcomes retain cancellation/fallback distinction and suspend exact failure windows', () => fixture(async ({ open, work }) => {
  const store = open(); await seal(store, work); const policy = await enable(store);
  const one = await route(store);
  expect(store.recordOutcome({ decision_id: one.id, state: 'cancelled' }).policy_suspended).toBe(false);
  expect(store.recordOutcome({ decision_id: one.id, state: 'fallback' }).policy_suspended).toBe(false);
  expect(store.recordOutcome({ decision_id: one.id, state: 'failed', latency_ms: 2 }).policy_suspended).toBe(false);
  const result = store.recordOutcome({ decision_id: one.id, state: 'failed' });
  expect(result).toMatchObject({ policy_suspended: true, suspension_reason: 'consecutive_failures' });
  expect(store.policy(policy.id).state).toBe('suspended');
  expect(store.listOutcomes(100)).toHaveLength(4);
  expect(store.listDecisions(1)).toHaveLength(1);
  expect(() => store.recordOutcome({ decision_id: one.id, state: 'unknown' })).toThrow('state');
  expect(() => store.recordOutcome({ decision_id: 'absent', state: 'passed' })).toThrow('not found');
  expect(store.verifyReceipts().verified).toBe(true);
}));

test('failure-rate guard is distinct from consecutive failures', () => fixture(async ({ open, work }) => {
  const store = open(); await seal(store, work); await enable(store); const one = await route(store);
  let result;
  for (const state of ['failed', 'passed', 'failed', 'passed', 'passed']) result = store.recordOutcome({ decision_id: one.id, state });
  expect(result).toMatchObject({ policy_suspended: true, suspension_reason: 'failure_rate' });
}));

test('policy or certificate mutation during asynchronous probes cannot commit stale authority', () => fixture(async ({ open, work, setHook, advance }) => {
  const store = open(); await seal(store, work); const policy = await enable(store);
  let once = true;
  setHook(async () => { if (once) { once = false; await store.setPolicyState(policy.id, 'disabled'); } });
  await expect(route(store)).rejects.toThrow('policy changed');
  setHook(() => advance(31 * 86400));
  expect((await store.validateCertificate('rtk', 'filter')).reason).toBe('expired');
}));

test('fixture and certificate paths refuse symlink or traversal escape', () => fixture(async ({ open, work, root }) => {
  const store = open();
  await expect(store.validateCertificate('../escape', 'filter')).rejects.toThrow('path');
  symlinkSync(root, join(work, 'escape'));
  expect(() => treeDigest(work)).toThrow('symlink');
  await expect(seal(store, work)).rejects.toThrow('symlink');
  expect(store.verifyReceipts().receipts).toBe(0);
}));

test('fixture digest preserves component-wise pathlib ordering and excludes git metadata', () => fixture(({ work }) => {
  mkdirSync(join(work, 'a')); writeFileSync(join(work, 'a/file'), 'nested'); writeFileSync(join(work, 'a.txt'), 'sibling');
  mkdirSync(join(work, '.git')); writeFileSync(join(work, '.git/config'), 'ignored');
  const rows = [['a/file', 'nested'], ['a.txt', 'sibling'], ['sample.txt', 'fixture\n']]
    .map(([path, value]) => ({ path, sha256: createHash('sha256').update(value).digest('hex'), bytes: Buffer.byteLength(value) }));
  expect(treeDigest(work)).toBe(digestValue(rows));
}));

test('only explicit Bernstein Python boundary is permitted and records its identity', () => fixture(async ({ open }) => {
  const calls = [];
  const store = open({ inspectProvider: provider => ({ path: `/fixture/${provider}`, kind: 'python_launcher',
    sha256: 'b'.repeat(64), interpreter: '/fixture/python' }), run: async argv => {
    calls.push(argv); return { argv, exit_code: 0, stdout: 'Bernstein 1.0', stderr: '', duration_ms: 1 };
  } });
  expect((await store.providerVersion('rtk')).exit_code).toBe(127);
  expect(calls).toHaveLength(0);
  const probe = await store.providerVersion('bernstein');
  expect(probe.runtime_boundary).toMatchObject({ kind: 'third_party_python', resolved_launcher: '/fixture/bernstein', sha256: 'b'.repeat(64) });
  expect(calls).toEqual([['/fixture/bernstein', '--version']]);
}));

test('exact reviewed non-Python wrappers remain available while unreviewed scripts refuse', () => fixture(async ({ open }) => {
  const calls = []; let reviewed = true;
  const store = open({ inspectProvider: provider => ({ path: `/fixture/${provider}`,
    kind: reviewed ? 'reviewed_script' : 'unverified_script', sha256: 'c'.repeat(64) }), run: async argv => {
    calls.push(argv); return { argv, exit_code: 0, stdout: 'Cavekit 1.0', stderr: '', duration_ms: 1 };
  } });
  const probe = await store.providerVersion('cavekit');
  expect(probe.exit_code).toBe(0);
  expect(probe.runtime_boundary).toMatchObject({ kind: 'reviewed_script', resolved_launcher: '/fixture/bonfyre-cavekit-provider', sha256: 'c'.repeat(64) });
  reviewed = false;
  expect((await store.providerVersion('cavekit')).exit_code).toBe(127);
  expect(calls).toHaveLength(1);
}));
