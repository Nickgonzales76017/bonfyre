import { afterEach, expect, test } from 'bun:test';
import { Database } from 'bun:sqlite';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createHypervisorStore } from '../hypervisor_store.mjs';

const fixtures = [];
afterEach(() => { for (const { store, home } of fixtures.splice(0)) { store?.close(); rmSync(home, { recursive: true, force: true }); } });
function fixture({ stop = () => {} } = {}) {
  const home = mkdtempSync(join(tmpdir(), 'bonfyre-hypervisor-store-'));
  const registryPath = join(home, 'ecosystem_registry.json');
  writeFileSync(registryPath, JSON.stringify({ capability_contracts: ['agent.executor', 'agent.reviewer'], projects: [
    { id: 'alpha', repo: 'local/alpha', homepage: 'https://example.test/alpha', license: 'MIT', integration: 'native',
      default_mode: 'active', detect: ['alpha'], capabilities: ['agent.executor', 'agent.reviewer'] },
    { id: 'beta', repo: 'owner/beta', homepage: 'https://example.test/beta', license: 'GPL', integration: 'cli',
      default_mode: 'shadow', detect: ['beta'], capabilities: ['agent.reviewer'] },
  ] }));
  let tick = 1791000000;
  const store = createHypervisorStore({ home, registryPath, now: () => tick++, checkStop: stop });
  fixtures.push({ home, store });
  return { home, store };
}

test('original table names and columns persist, registry loads without replacing observed state', () => {
  const { store } = fixture();
  store.loadRegistry();
  const db = new Database(store.dbPath);
  const tables = db.query("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name").all().map(row => row.name);
  expect(tables).toEqual(['bugs', 'capabilities', 'context_packets', 'evaluations', 'events', 'mission_tasks', 'missions',
    'provider_capabilities', 'providers', 'receipts', 'routes', 'sessions', 'source_updates', 'sources']);
  expect(db.query("SELECT effect_class FROM capabilities WHERE id='agent.executor'").get().effect_class).toBe('write');
  expect(store.providers()).toHaveLength(2);
  db.query("UPDATE providers SET installed=1,health='verified' WHERE id='alpha'").run();
  store.loadRegistry();
  expect(store.provider('alpha').health).toBe('verified');
  expect(store.provider('alpha').capabilities).toHaveLength(2);
  db.close();
});

test('registry callers cannot mutate cached authority and read-only mode never initializes schema', () => {
  const { store, home } = fixture();
  const first = store.registry(); first.projects[0].id = 'forged';
  expect(store.registry().projects[0].id).toBe('alpha');
  const untouched = createHypervisorStore({ home: join(home, 'uninitialized'), readOnly: true, registryPath: join(home, 'ecosystem_registry.json') });
  expect(() => untouched.summary()).toThrow('read-only hypervisor database missing');
  untouched.close();
  store.loadRegistry(); store.writeReceipt({ state: 'passed', payload: { amount: 1e-7, large: 1e16 } });
  const reader = createHypervisorStore({ home, readOnly: true });
  expect(reader.verifyReceiptChain().verified).toBe(true);
  expect(reader.providers()).toHaveLength(2);
  expect(() => reader.writeReceipt({ state: 'passed', payload: {} })).toThrow('read-only');
  reader.close();
});

test('explicit database path canonicalizes the macOS /var alias without weakening containment', () => {
  const { home } = fixture();
  const aliased = createHypervisorStore({ home, dbPath: join(home, 'alias.sqlite3'), registryPath: join(home, 'ecosystem_registry.json') });
  expect(aliased.summary().receipts).toBe(0);
  aliased.close();
  expect(() => createHypervisorStore({ home, dbPath: join(home, '..', 'outside.sqlite3') })).toThrow('escapes');
});

test('one-query routeMany is equivalent to separate legacy routes with promotion, license and shadow gates', () => {
  const { store } = fixture(); store.loadRegistry();
  const db = new Database(store.dbPath);
  db.query("UPDATE providers SET installed=1,health='verified' WHERE id='alpha'").run();
  db.query("UPDATE providers SET installed=1,health='reachable',source_ref='tag',source_digest='digest' WHERE id='beta'").run();
  db.query("UPDATE provider_capabilities SET score=0.8 WHERE provider_id='alpha'").run();
  db.query("UPDATE provider_capabilities SET score=0.9 WHERE provider_id='beta'").run();
  const count = store.routeQueryCount();
  const batch = store.routeMany(['agent.executor', 'agent.reviewer'], { include_shadow: true });
  expect(store.routeQueryCount() - count).toBe(1);
  expect(batch['agent.executor']).toEqual(store.route('agent.executor', { include_shadow: true }));
  expect(batch['agent.reviewer']).toEqual(store.route('agent.reviewer', { include_shadow: true }));
  expect(batch['agent.reviewer'].find(row => row.provider_id === 'beta').eligible).toBe(false);
  store.approveLicense('beta', 'fixture review');
  expect(store.route('agent.reviewer', { include_shadow: true }).find(row => row.provider_id === 'beta').eligible).toBe(true);
  expect(store.route('agent.reviewer').some(row => row.provider_id === 'beta')).toBe(false);
  expect(store.promote('alpha', 'agent.executor').state).toBe('promoted');
  expect(store.route('agent.executor')[0].route_state).toBe('promoted');
  store.quarantine('alpha', 'fixture regression');
  expect(store.route('agent.executor')).toEqual([]);
  db.close();
});

test('discovery updates declared providers and event atomically; STOP refuses mutation', () => {
  let stopped = false; const { store } = fixture({ stop: () => { if (stopped) throw new Error('STOP active'); } });
  store.loadRegistry();
  const result = store.recordDiscovery({ providers: [{ provider: 'alpha', installed: true, commands: [['alpha', '/bin/alpha']],
    version: 'alpha 1', health: 'reachable', evidence: [{ exit_code: 0 }] }] });
  expect(result.providers).toHaveLength(1);
  expect(store.provider('alpha').command_path).toBe('/bin/alpha');
  const db = new Database(store.dbPath);
  expect(db.query("SELECT count(*) AS n FROM events WHERE kind='ecosystem.discovered'").get().n).toBe(1);
  stopped = true;
  expect(() => store.recordDiscovery({ providers: [] })).toThrow('STOP');
  expect(db.query("SELECT count(*) AS n FROM events WHERE kind='ecosystem.discovered'").get().n).toBe(1);
  db.close();
});

test('default-space Python digest and row-bound chain verify; forged row or file fails', () => {
  const { store } = fixture();
  const first = store.writeReceipt({ state: 'passed', payload: { finding: 'café', amount: 2 } });
  const second = store.writeReceipt({ state: 'partial', payload: { ok: false } });
  expect(store.verifyReceiptChain()).toMatchObject({ verified: true, receipts: 2, head: second.digest });
  expect(readFileSync(first.path, 'utf8')).toContain('\\u00e9');
  const db = new Database(store.dbPath);
  db.query('UPDATE receipts SET state=? WHERE id=?').run('forged', first.receipt_id);
  expect(store.verifyReceiptChain().verified).toBe(false);
  db.query('UPDATE receipts SET state=? WHERE id=?').run('passed', first.receipt_id);
  const doc = JSON.parse(readFileSync(first.path, 'utf8')); doc.payload.finding = 'changed'; writeFileSync(first.path, JSON.stringify(doc));
  expect(store.verifyReceiptChain().verified).toBe(false);
  db.close();
});

test('mission persistence keeps exact legacy rows and durable intent receipts', () => {
  const { store } = fixture();
  const mission = { id: 'm-one', goal: 'Deliver fixture', requested_capabilities: ['agent.executor'],
    tasks: [{ id: 'T-1', title: 'Build', dependencies: [], capability: 'agent.executor' }], execution_plan: { waves: [] } };
  const created = store.createMission({ mission, spec: '# Fixture\n', graph: { tasks: mission.tasks } });
  expect(readFileSync(created.spec_path, 'utf8')).toContain('Fixture');
  expect(store.getMission('m-one').execution_plan).toEqual({ waves: [] });
  expect(store.summary().missions.compiled).toBe(1);
  const db = new Database(store.dbPath);
  expect(db.query("SELECT count(*) AS n FROM mission_tasks WHERE mission_id='m-one'").get().n).toBe(1);
  store.saveRoute({ mission_id: 'm-one', payload: { routes: {}, complete: false } });
  expect(JSON.parse(db.query("SELECT route_json FROM missions WHERE id='m-one'").get().route_json).complete).toBe(false);
  mission.bugs = [{ id: 'B-1' }];
  store.saveMission({ mission, spec: '# Updated\n', graph: { tasks: mission.tasks } });
  store.upsertBug({ mission_id: 'm-one', bug: { id: 'B-1', symptom: 'failure', root_cause: 'cause', regression_test: 'test' } });
  expect(db.query("SELECT count(*) AS n FROM bugs WHERE mission_id='m-one'").get().n).toBe(1);
  expect(store.verifyReceiptChain().verified).toBe(true);
  expect(store.summary().receipts).toBe(8);
  db.close();
});

test('backpropagation updates mission and bug in one admitted SQLite transition', () => {
  const { store } = fixture();
  const mission = { id: 'm-backprop', goal: 'Fixture', requested_capabilities: [], tasks: [], bugs: [] };
  store.createMission({ mission, spec: '# Initial', graph: { tasks: [] } });
  const bug = { id: 'B-1', symptom: 'miss', root_cause: 'gap', regression_test: 'check', evidence: [] };
  mission.bugs.push(bug);
  const result = store.saveBackpropagation({ mission, spec: '# Updated', graph: { tasks: [] }, bug });
  expect(result.bug.id).toBe('B-1');
  expect(store.getMission(mission.id).bugs).toHaveLength(1);
  const db = new Database(store.dbPath);
  expect(db.query("SELECT count(*) AS n FROM bugs WHERE mission_id='m-backprop'").get().n).toBe(1);
  expect(db.query("SELECT count(*) AS n FROM events WHERE kind='failure.backpropagated'").get().n).toBe(1);
  expect(store.verifyReceiptChain().verified).toBe(true);
  db.close();
});

test('native session identity remains stable without erasing previously assigned lineage', () => {
  const { store } = fixture();
  const first = store.registerSession({ provider_id: 'alpha', native_session_id: 'native-1', mission_id: 'm-one', cwd: '/test', metadata: { turns: 2 } });
  const second = store.registerSession({ provider_id: 'alpha', native_session_id: 'native-1', state: 'idle', metadata: { turns: 3 } });
  expect(second).toMatchObject({ id: first.id, mission_id: 'm-one', cwd: '/test', state: 'idle', metadata: { turns: 3 } });
  expect(store.listSessions({ provider_id: 'alpha', limit: 1 })).toEqual([second]);
  expect(() => store.listSessions({ limit: -1 })).toThrow('limit');
  expect(() => store.registerSession({ provider_id: 'unknown', native_session_id: 'x' })).toThrow('provider');
});

test('durable task intent survives restart, refuses replay, and binds one exact completion', () => {
  const { store, home } = fixture();
  const mission = { id: 'attempt-mission', goal: 'test', tasks: [
    { id: 'A', title: 'A', capability: 'agent.executor', dependencies: [] },
    { id: 'B', title: 'B', capability: 'agent.reviewer', dependencies: ['A'] },
  ] };
  store.createMission({ mission, spec: 'test', graph: { tasks: mission.tasks } });
  const input_digest = 'a'.repeat(64), args = { mission_id: mission.id, task_id: 'A', input_digest, provider_id: 'alpha', capability: 'agent.executor' };
  expect(() => store.beginTask({ ...args, task_id: 'B' })).toThrow('dependencies');
  const started = store.beginTask(args);
  const concurrent = createHypervisorStore({ home });
  expect(() => concurrent.beginTask(args)).toThrow('reconcile');
  concurrent.close();
  expect(() => store.completeTask({ ...args, attempt_id: 'wrong', state: 'passed' })).toThrow('no longer current');
  const completed = store.completeTask({ ...args, attempt_id: started.attempt_id, state: 'passed', result: { output: 'exact' }, evidence: [{ digest: 'body' }] });
  expect(completed.state).toBe('passed');
  expect(store.beginTask(args)).toMatchObject({ reusable: true, state: 'passed', result: { output: 'exact' } });
  expect(() => store.beginTask({ ...args, input_digest: 'b'.repeat(64) })).toThrow('input changed');
  expect(() => store.blockTask({ mission_id: mission.id, task_id: 'A', reason: 'overwrite' })).toThrow('effect state');
  expect(() => store.finishMission({ mission_id: mission.id, state: 'resolved', evidence: [] })).toThrow('completion');
  expect(store.verifyReceiptChain().verified).toBe(true);
  const db = new Database(store.dbPath);
  db.query('UPDATE receipts SET payload_json=? WHERE id=?').run('{}', completed.receipt_id);
  expect(() => store.beginTask(args)).toThrow('integrity'); db.close();
});

test('STOP after task effect leaves started intent and prevents implicit execution retry', () => {
  let stop = false;
  const { store, home } = fixture({ stop: () => { if (stop) throw new Error('STOP'); } });
  const mission = { id: 'stopped', goal: 'test', tasks: [{ id: 'A', title: 'A', dependencies: [] }] };
  store.createMission({ mission, spec: 'test', graph: { tasks: mission.tasks } });
  const args = { mission_id: mission.id, task_id: 'A', input_digest: 'c'.repeat(64) };
  const attempt = store.beginTask(args); stop = true;
  expect(() => store.completeTask({ ...args, attempt_id: attempt.attempt_id, state: 'passed' })).toThrow('STOP');
  const restarted = createHypervisorStore({ home });
  expect(() => restarted.beginTask(args)).toThrow('reconcile');
  expect(restarted.getMissionRuntime(mission.id).tasks[0].state).toBe('running'); restarted.close();
});

test('read snapshots remain available under STOP and event bytes are bounded before JSON allocation', () => {
  let stopped = false; const { store, home } = fixture({ stop: () => { if (stopped) throw new Error('STOP'); } });
  store.loadRegistry(); store.event('fixture', 'observation', { value: 'exact' });
  stopped = true;
  const reader = createHypervisorStore({ home, checkStop: () => { throw new Error('STOP'); } });
  expect(reader.observe('events').events.at(-1).payload).toEqual({ value: 'exact' });
  expect(reader.observedProviders()).toHaveLength(2);
  expect(reader.observe('listSessions')).toEqual([]);
  reader.close();
  const db = new Database(store.dbPath);
  db.query('INSERT INTO events(ts,source,kind,payload_json) VALUES(?,?,?,?)').run(1, 'fixture', 'oversized', JSON.stringify('x'.repeat(9 * 1024 * 1024)));
  expect(() => store.observe('events')).toThrow('byte budget');
  db.close();
});

test('revision and live provider gates invalidate stale dispatch and stale completion without whole-plan rereads', () => {
  const { store } = fixture(); store.loadRegistry();
  const db = new Database(store.dbPath);
  db.query("UPDATE providers SET installed=1,health='verified' WHERE id='alpha'").run();
  const mission = { id: 'cut-mission', goal: 'test', tasks: [{ id: 'A', title: 'A', capability: 'agent.executor', dependencies: [] }] };
  store.createMission({ mission, spec: 'test', graph: { tasks: mission.tasks } });
  const packet = store.persistContext({ id: store.id(), mission_id: mission.id, token_estimate: 1, sections: [], representation_digest: 'd'.repeat(64) });
  store.beginMission({ mission_id: mission.id, context_packet_id: packet.id });
  const runtime = store.getMissionRuntime(mission.id);
  const args = { mission_id: mission.id, task_id: 'A', input_digest: 'a'.repeat(64), provider_id: 'alpha', capability: 'agent.executor',
    mission_revision: runtime.mission_revision, plan_digest: 'b'.repeat(64), route_digest: 'c'.repeat(64), context_packet_id: packet.id };
  store.quarantine('alpha', 'regression');
  expect(() => store.beginTask(args)).toThrow('eligibility');
  expect(store.taskStates(mission.id, ['A'])).toEqual({ A: 'planned' });
  db.query("UPDATE providers SET mode='active',health='verified' WHERE id='alpha'").run();
  store.saveRoute({ mission_id: mission.id, payload: {} });
  expect(() => store.beginTask(args)).toThrow('cut changed');
  args.mission_revision = store.getMissionRuntime(mission.id).mission_revision;
  const attempt = store.beginTask(args);
  mission.constraints = ['New authority rule'];
  store.saveMission({ mission, spec: 'new rules', graph: { tasks: mission.tasks } });
  expect(() => store.completeTask({ ...args, attempt_id: attempt.attempt_id, state: 'passed' })).toThrow('cut changed');
  expect(store.completeTask({ ...args, attempt_id: attempt.attempt_id, state: 'uncertain', result: { reason: 'policy changed during effect' } }).state).toBe('uncertain');
  expect(() => store.finishMission({ mission_id: mission.id, state: 'blocked', mission_revision: args.mission_revision })).toThrow('changed');
  expect(store.verifyReceiptChain().verified).toBe(true); db.close();
});
