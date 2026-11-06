import { afterEach, expect, test } from 'bun:test';
import { createHash } from 'node:crypto';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Database } from 'bun:sqlite';
import { createV6Bfp } from '../v6_bfp.mjs';
import { createV6Missions, MISSION_TEMPLATES } from '../v6_missions.mjs';
import { createV6Store } from '../v6_store.mjs';

const fixtures = [];
afterEach(() => { for (const path of fixtures.splice(0)) rmSync(path, { recursive: true, force: true }); });
function fixture() {
  const root = mkdtempSync(join(tmpdir(), 'bonfyre-v6-missions-')); fixtures.push(root);
  const home = join(root, 'home');
  const store = createV6Store({ home, root, now: () => 1800000000.25 });
  const bfp = createV6Bfp({ root, home });
  return { root, home: store.home, store, bfp };
}
function seed({ home, store, bfp }) {
  const apps = [...new Set(MISSION_TEMPLATES.flatMap(item => item.apps))];
  const providers = [...new Set(MISSION_TEMPLATES.flatMap(item => item.providers))];
  const auditPath = join(store.root, 'audit.db');
  const audit = new Database(auditPath, { create: true, strict: true });
  audit.exec('CREATE TABLE audit(id INTEGER PRIMARY KEY);'); audit.close();
  store.writeJson(join(home, 'estate/estate.json'), { apps });
  store.writeJson(join(home, 'resources/resource-graph.json'), { resources: [
    ...providers.map(provider => ({ kind: 'external-provider', provider, state: 'configured' })),
    { kind: 'aurekai-integration', provider: 'firecrawl-site' },
  ] });
  store.writeJson(join(home, 'resources/provider-activation.json'), { results: providers.map(provider => ({ provider, state: 'configured' })) });
  store.writeJson(join(home, 'backends/backend-fabric.json'), { backends: [{ id: 'feldera', state: 'active' }] });
  store.writeJson(join(home, 'graphs/workgraph-v61.json'), { nodes: apps.map(app => ({ id: `app:${app}`, app, kind: 'application' })),
    edges: [{ source: 'app:wiki', target: 'app:helpdesk', kind: 'CROSS_APP_DEPENDENCY' }] });
  store.writeJson(join(home, 'sparse/snapshot.json'), { files: [
    { category: 'media', path: 'media/photo.png' }, { category: 'models', path: 'model.bin', bytes: 32 },
    { category: 'databases', path: auditPath },
  ], bundles: [{ category: 'media', bytes: 17 }], categories: { media: 1, models: 1 } });
  bfp.pack(join(home, 'bfp/bonfyre-v6.bfp'), { 'estate/estate.json': Buffer.from('{}') });
}

test('all ten local workloads are materialized, but disabled compilation never becomes activation', async () => {
  const state = fixture(); seed(state);
  try {
    const owner = createV6Missions({ store: state.store, bfp: state.bfp });
    const result = await owner.activate({ compile: false });
    expect(result.summary).toMatchObject({ cataloged: 10, active: 0, candidate: 10, hypervisor_compiled: 0, canary_runs_passed: 0, workload_artifacts: 10 });
    expect(result.catalog.missions).toHaveLength(10);
    expect(result.catalog.missions.every(mission => mission.state === 'candidate' && mission.activation.compiled === false && mission.activation.canary_passed === false)).toBe(true);
    expect(result.catalog.hypervisor_compilation.every(item => item.probe.state === 'not_executed' && item.probe.reason === 'compile_disabled')).toBe(true);
    expect(result.safe_runs.every(run => run.outcome.workload.external_effect_executed === false)).toBe(true);
    expect(result.catalog.missions.find(item => item.id === 'production.sparse-estate-deploy').activation.workload_passed).toBe(true);
    expect(result.catalog.missions.find(item => item.id === 'production.provider-credit').activation.workload_passed).toBe(true);
    expect(state.store.verifyReceipts().verified).toBe(true);
    expect(state.store.db.query('SELECT count(*) AS n FROM mission_runs').get().n).toBe(10);
  } finally { state.store.close(); }
});

test('default compile request reports unavailable native owner without invoking Python', async () => {
  const state = fixture(); seed(state);
  try {
    const result = await createV6Missions({ store: state.store, bfp: state.bfp }).activate();
    expect(result.summary.active).toBe(0);
    expect(result.catalog.hypervisor_compilation.every(item => item.probe.reason === 'native_compiler_unavailable')).toBe(true);
    expect(result.safe_runs.every(item => item.outcome.command_canary.executed === false)).toBe(true);
  } finally { state.store.close(); }
});

test('trusted injected owner proofs can activate only matching mission/effect identities', async () => {
  const state = fixture(); seed(state);
  const seen = [];
  const proof = ({ mission, intent, workload, compileProof }, kind) => ({ executed: true, verified: true, state: 'passed', kind,
    external_effect_executed: false, ...(kind === 'canary' ? { read_only_local: true } : {}),
    mission_id: mission.id, effect_class: mission.effect_class, intent_digest: intent.digest,
    ...(workload ? { workload_digest: workload.digest, compile_evidence_digest: compileProof.evidence_digest } : {}),
    evidence: { kind, mission: mission.id, intent: intent.digest, external_effect_executed: false },
    evidence_digest: state.store.digest({ kind, mission: mission.id, intent: intent.digest, external_effect_executed: false }) });
  try {
    const result = await createV6Missions({ store: state.store, bfp: state.bfp,
      compileMission: async input => { seen.push(`compile:${input.mission.id}`); return proof(input, 'compile'); },
      canaryMission: async input => { seen.push(`canary:${input.mission.id}`); return proof(input, 'canary'); } }).activate();
    expect(result.summary).toMatchObject({ active: 10, candidate: 0, hypervisor_compiled: 10, canary_runs_passed: 10 });
    expect(seen).toHaveLength(20);
    expect(result.catalog.missions.every(item => item.activation.external_effect_executed === false)).toBe(true);
    expect(state.store.verifyReceipts().verified).toBe(true);
  } finally { state.store.close(); }
});

test('an uncertain compiler outcome has an intent receipt and stops before any canary or later mission', async () => {
  const state = fixture(); seed(state);
  let canaries = 0;
  try {
    const owner = createV6Missions({ store: state.store, bfp: state.bfp,
      compileMission: async () => { throw new Error('owner response lost'); },
      canaryMission: async () => { canaries++; throw new Error('must not run'); } });
    await expect(owner.activate()).rejects.toThrow('uncertain compiler outcome');
    expect(canaries).toBe(0);
    expect(state.store.db.query('SELECT count(*) AS n FROM missions').get().n).toBe(0);
    expect(state.store.db.query('SELECT count(*) AS n FROM mission_runs').get().n).toBe(0);
    const receipts = state.store.db.query("SELECT kind,state FROM receipts ORDER BY seq").all();
    expect(receipts.some(item => item.kind === 'v6.1.mission-activation.intent' && item.state === 'started')).toBe(true);
    expect(state.store.verifyReceipts().verified).toBe(true);
  } finally { state.store.close(); }
});

test('configured database providers never mask failed or absent SQLite inventory checks', async () => {
  const state = fixture(); seed(state);
  try {
    const path = join(state.root, 'corrupt.db'); writeFileSync(path, 'not sqlite');
    state.store.writeJson(join(state.home, 'sparse/snapshot.json'), { files: [{ category: 'databases', path }], bundles: [], categories: {} });
    const result = await createV6Missions({ store: state.store, bfp: state.bfp }).activate({ compile: false });
    const audit = result.safe_runs.find(item => item.mission_id === 'production.loss-audit').outcome.workload;
    expect(audit.configured_database_providers).toContain('supabase');
    expect(audit.integrity_checks[0].ok).toBe(false);
    expect(audit.passed).toBe(false);
    state.store.writeJson(join(state.home, 'sparse/snapshot.json'), { files: [], bundles: [], categories: {} });
    const empty = await createV6Missions({ store: state.store, bfp: state.bfp }).activate({ compile: false });
    expect(empty.safe_runs.find(item => item.mission_id === 'production.loss-audit').outcome.workload.passed).toBe(false);
  } finally { state.store.close(); }
});

test('SQLite digest describes a consistent checked snapshot including committed WAL rows', async () => {
  const state = fixture(); seed(state);
  const path = join(state.root, 'wal.db'); let db;
  try {
    db = new Database(path, { create: true, strict: true });
    db.exec('PRAGMA journal_mode=WAL; CREATE TABLE events(id INTEGER PRIMARY KEY, note TEXT); INSERT INTO events(note) VALUES (\'first\');');
    state.store.writeJson(join(state.home, 'sparse/snapshot.json'), { files: [{ category: 'databases', path }], bundles: [], categories: {} });
    const baseFile = createHash('sha256').update(readFileSync(path)).digest('hex');
    const first = await createV6Missions({ store: state.store, bfp: state.bfp }).activate({ compile: false });
    const firstCheck = first.safe_runs.find(item => item.mission_id === 'production.loss-audit').outcome.workload.integrity_checks[0];
    expect(firstCheck).toMatchObject({ ok: true, quick_check: 'ok', digest_basis: 'sqlite_vacuum_snapshot' });
    db.exec("INSERT INTO events(note) VALUES ('second');");
    expect(createHash('sha256').update(readFileSync(path)).digest('hex')).toBe(baseFile);
    const second = await createV6Missions({ store: state.store, bfp: state.bfp }).activate({ compile: false });
    const secondCheck = second.safe_runs.find(item => item.mission_id === 'production.loss-audit').outcome.workload.integrity_checks[0];
    expect(secondCheck.ok).toBe(true);
    expect(secondCheck.sha256).not.toBe(firstCheck.sha256);
  } finally { db?.close(); state.store.close(); }
});

test('STOP check is enforced after compiler await and before canary', async () => {
  const state = fixture(); seed(state);
  let stopped = false, canaries = 0;
  try {
    const owner = createV6Missions({ store: state.store, bfp: state.bfp,
      checkStop: () => { if (stopped) throw new Error('STOP requested'); },
      compileMission: async ({ mission, intent }) => { stopped = true; return { executed: true, verified: true,
        external_effect_executed: false,
        mission_id: mission.id, effect_class: mission.effect_class, intent_digest: intent.digest,
        evidence: { kind: 'compile', external_effect_executed: false },
        evidence_digest: state.store.digest({ kind: 'compile', external_effect_executed: false }) }; },
      canaryMission: async () => { canaries++; } });
    await expect(owner.activate()).rejects.toThrow('STOP requested');
    expect(canaries).toBe(0);
    expect(state.store.db.query("SELECT count(*) AS n FROM receipts WHERE kind='v6.1.mission-activation.intent'").get().n).toBe(1);
  } finally { state.store.close(); }
});
