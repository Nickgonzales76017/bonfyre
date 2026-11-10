import { expect, test } from 'bun:test';
import { Database } from 'bun:sqlite';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { buildMissionGraph, compileMissionTasks, createHypervisorMissions,
  inferMissionTraits, renderMissionSpec } from '../hypervisor_missions.mjs';
import { createHypervisorStore } from '../hypervisor_store.mjs';

function memoryStore() {
  const missions = new Map(), routes = [], events = [];
  const store = {
    home: '/fixture', missionRoot: '/fixture/missions', now: () => 1800000000.25,
    id: () => 'a'.repeat(32),
    createMission({ mission, spec, graph }) {
      missions.set(mission.id, structuredClone(mission)); events.push(['create', mission.id, spec, graph]);
      return { mission_dir: `/fixture/missions/${mission.id}`, spec_path: `/fixture/missions/${mission.id}/SPEC.md`,
        graph_path: `/fixture/missions/${mission.id}/task-graph.json` };
    },
    getMission(id) { const mission = missions.get(id); if (!mission) throw new Error('not found'); return structuredClone(mission); },
    saveBackpropagation({ mission, spec, graph, bug }) {
      missions.set(mission.id, structuredClone(mission)); events.push(['backpropagation', mission.id, spec, graph, bug]);
    },
    routeMany(capabilities, options) {
      routes.push({ capabilities, options });
      return Object.fromEntries(capabilities.map(capability => [capability,
        capability === 'spines.kernel.verify' ? [{ provider_id: 'kernel-native', eligible: true, mode: 'active' }] : []]));
    },
    saveRoute(input) { events.push(['route', input]); },
  };
  return { store, missions, routes, events };
}

test('trait descriptor and task rewrites preserve the installed R7 mission DAG', () => {
  const traits = inferMissionTraits('Full production cloud PR review and model audit');
  expect(traits).toMatchObject({ high_risk: true, large: true, remote: true, pr: true, model: true, audit: true });
  const tasks = compileMissionTasks(traits), graph = buildMissionGraph(tasks);
  expect(graph.valid).toBe(true);
  expect(tasks).toHaveLength(21);
  const map = new Map(tasks.map(item => [item.id, item]));
  expect(map.get('T-003').dependencies).toEqual(['T-001C', 'T-002']);
  expect(map.get('T-004').dependencies).toEqual(['T-003R']);
  expect(map.get('T-004G').dependencies).toEqual(['T-004M']);
  expect(map.get('T-005').dependencies).toEqual(['T-004G']);
  expect(map.get('T-006P').dependencies).toEqual(['T-006R']);
  expect(map.get('T-007').dependencies).toEqual(['T-006P']);
  expect(map.get('T-008').dependencies).toEqual(['T-008S']);
  expect(graph.waves[0]).toEqual(['T-001']);
});

test('compile writes semantic plan and exact spec/graph surface, but no task execution', () => {
  const { store, events } = memoryStore();
  const owner = createHypervisorMissions({ store });
  const result = owner.compile('Build a simple dashboard', { constraints: ['Keep source private.'], budget_tokens: 1000 });
  expect(result.mission.schema).toBe('bonfyre.mission.v5');
  expect(result.mission.traits.interactive).toBe(true);
  expect(result.mission.tasks).toHaveLength(18);
  expect(result.mission.tasks.every(item => item.state === 'planned')).toBe(true);
  expect(result.mission.requested_capabilities).toContain('spines.kernel.verify');
  expect(result.mission.requested_capabilities).toContain('scheduler.deterministic_dag');
  expect(result.mission.execution_plan.schema).toBe('bonfyre.task-plan.v1');
  expect(result.mission.execution_plan.authority).toBe('candidate-plan-only');
  expect(result.mission.execution_plan.stats.tasks).toBe(18);
  expect(events).toHaveLength(1);
  expect(events[0][0]).toBe('create');
  expect(events[0][2]).toContain('## §V Invariants');
  expect(events[0][2]).toContain('## §B Bugs and Backpropagation');
  expect(events[0][3].valid).toBe(true);
  expect(renderMissionSpec(result.mission)).toBe(events[0][2]);
});

test('one batched route read binds provider choice and exposes only its downstream impact', () => {
  const { store, routes, events } = memoryStore();
  const owner = createHypervisorMissions({ store });
  const mission = owner.compile('Build a simple dashboard').mission;
  const result = owner.route(mission.id, { include_shadow: false });
  expect(routes).toHaveLength(1);
  expect(routes[0].options).toEqual({ include_shadow: false });
  expect(new Set(routes[0].capabilities).size).toBe(mission.requested_capabilities.length);
  expect(result.complete).toBe(false);
  expect(result.routes['spines.kernel.verify'][0].provider_id).toBe('kernel-native');
  expect(result.route_plan.authority).toBe('candidate-plan-only');
  expect(result.route_impact.changed).toEqual(['T-007K']);
  expect(result.route_impact.affected).toEqual(['T-007K', 'T-008', 'T-008F', 'T-008S']);
  expect(events.at(-1)[0]).toBe('route');
});

test('backpropagated invariant invalidates the current plan without authorizing reuse', () => {
  const { store, events } = memoryStore();
  const owner = createHypervisorMissions({ store });
  const mission = owner.compile('Build a simple dashboard').mission;
  const result = owner.backpropagate(mission.id, { symptom: 'wrong output', root_cause: 'stale index',
    regression_test: 'index parity fixture', generalize: true, evidence: [{ fixture: 'parity' }] });
  expect(result.bug.id).toMatch(/^B-[a-f0-9]{12}$/);
  expect(result.new_invariant).toMatch(/^V-[A-F0-9]{8}$/);
  expect(result.invalidation.context_changed).toBe(true);
  expect(result.invalidation.affected).toHaveLength(mission.tasks.length);
  expect(result.invalidation.authority).toBe('candidate-invalidation-only');
  expect(owner.get(mission.id).invariants.some(item => item.id === result.new_invariant)).toBe(true);
  expect(events.at(-1)[0]).toBe('backpropagation');
  expect(events.at(-1)[4].id).toBe(result.bug.id);
});

test('invalid goals, budgets and task graphs refuse before store mutation', () => {
  const { store, events } = memoryStore();
  const owner = createHypervisorMissions({ store });
  expect(() => owner.compile('')).toThrow('goal');
  expect(() => owner.compile('valid', { budget_tokens: -1 })).toThrow('token budget');
  expect(() => owner.compile('valid', { requested_capabilities: [123] })).toThrow('requested capabilities');
  expect(events).toHaveLength(0);
  expect(buildMissionGraph([{ id: 'one', dependencies: ['missing'] }]).valid).toBe(false);
});

test('real durable store compiles, routes once, and backpropagates with a verifiable receipt chain', () => {
  const home = mkdtempSync(join(tmpdir(), 'bonfyre-hypervisor-mission-'));
  const registryPath = join(home, 'ecosystem_registry.json');
  writeFileSync(registryPath, JSON.stringify({ capability_contracts: ['spines.kernel.verify'], projects: [{
    id: 'kernel-native', repo: 'local/kernel-native', homepage: 'https://example.test/kernel', license: 'MIT',
    integration: 'native', default_mode: 'active', capabilities: ['spines.kernel.verify'], detect: [],
  }] }));
  const store = createHypervisorStore({ home, registryPath, now: () => 1800000000.25 });
  try {
    store.loadRegistry();
    const database = new Database(store.dbPath);
    database.query("UPDATE providers SET installed=1,health='verified' WHERE id='kernel-native'").run();
    database.close();
    const owner = createHypervisorMissions({ store });
    const created = owner.compile('Build a bounded review dashboard');
    expect(readFileSync(created.spec_path, 'utf8')).toContain('## §T Tasks');
    expect(store.getMission(created.mission.id).execution_plan.digest).toBe(created.mission.execution_plan.digest);
    const before = store.routeQueryCount();
    const route = owner.route(created.mission.id);
    expect(store.routeQueryCount() - before).toBe(1);
    expect(route.route_impact.changed).toEqual(['T-007K']);
    const feedback = owner.backpropagate(created.mission.id, { symptom: 'stale output', root_cause: 'index invalidation gap',
      regression_test: 'index parity test', generalize: true });
    expect(feedback.invalidation.context_changed).toBe(true);
    expect(store.verifyReceiptChain().verified).toBe(true);
    const db = new Database(store.dbPath);
    expect(db.query('SELECT count(*) AS n FROM missions').get().n).toBe(1);
    expect(db.query('SELECT count(*) AS n FROM mission_tasks').get().n).toBe(created.mission.tasks.length);
    expect(db.query('SELECT count(*) AS n FROM bugs').get().n).toBe(1);
    db.close();
  } finally { store.close(); rmSync(home, { recursive: true, force: true }); }
});
