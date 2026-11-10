// What this does not prove:
// - a dependency is replaced by an object built in this test, so the real one is not exercised.
import { expect, test } from 'bun:test';
import { Database } from 'bun:sqlite';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createHypervisorExecution, selectExecutionProvider } from '../hypervisor_execution.mjs';
import { createHypervisorMissions, buildMissionGraph, renderMissionSpec } from '../hypervisor_missions.mjs';
import { createHypervisorStore } from '../hypervisor_store.mjs';
import { createHypervisorContext } from '../hypervisor_context.mjs';
import { compileTaskPlan, missionPlanContext } from '../mission_plan.mjs';

const DIGEST = 'a'.repeat(64), MISSION = 'b'.repeat(32), PACKET = 'c'.repeat(32);
const tasks = [
  { id: 'a', title: 'first', capability: 'substrate.affected_closure', dependencies: [] },
  { id: 'b', title: 'second', capability: 'substrate.affected_closure', dependencies: [] },
  { id: 'c', title: 'plan', capability: 'scheduler.deterministic_dag', dependencies: ['a', 'b'] },
];
function missionFor(rows = tasks) {
  const mission = { id: MISSION, goal: 'Verify bounded waves', tasks: rows, requested_capabilities: [...new Set(rows.map(row => row.capability))],
    constraints: [], interfaces: [], research: [], invariants: [], bugs: [], verification: [], promotion: {},
    context_budget: { tokens: 1000 }, traits: {} };
  mission.execution_plan = compileTaskPlan(rows, { context: missionPlanContext(mission) });
  return mission;
}
function fakeOwners(mission = missionFor()) {
  const states = new Map(mission.tasks.map(row => [row.id, { id: row.id, state: 'planned' }]));
  const route = { mission_id: mission.id, execution_plan_digest: mission.execution_plan.digest,
    routes: Object.fromEntries(mission.requested_capabilities.map(capability => [capability,
      [{ provider_id: 'owner', eligible: true, route_state: 'promoted', mode: 'active' }]])) };
  const packet = { id: PACKET, mission_id: mission.id, path: '/fixture/context.json', digest: DIGEST, sections: [], token_estimate: 20 };
  const attempts = [], completions = [], blocked = [], finished = [];
  const store = {
    missionRoot: '/fixture/missions', getMissionRuntime: () => ({ mission, route, context: packet, state: 'compiled',
      mission_revision: 1800000000.25, tasks: [...states.values()] }),
    taskStates: (_missionId, ids) => Object.fromEntries(ids.filter(id => states.has(id)).map(id => [id, states.get(id).state])),
    beginMission() { return { state: 'running' }; },
    beginTask(input) { attempts.push(input); states.get(input.task_id).state = 'running'; return { attempt_id: `${input.task_id}-attempt`, reusable: false }; },
    completeTask(input) { completions.push(input); states.get(input.task_id).state = input.state; return { receipt_id: `${input.task_id}-receipt` }; },
    blockTask(input) { blocked.push(input); states.get(input.task_id).state = 'blocked'; },
    finishMission(input) { finished.push(input); },
    missionStatus: () => ({ mission_id: mission.id, tasks: [...states.values()] }),
    verifyReceiptChain: () => ({ verified: true, failures: [], receipts: attempts.length }),
  };
  return { mission, route, packet, states, store, attempts, completions, blocked, finished,
    missions: { get: () => mission, route: () => route }, compileContext: async () => packet };
}

test('only promoted or active eligible providers execute; canary-like candidate hints have no authority', () => {
  expect(selectExecutionProvider({ routes: { cap: [
    { provider_id: 'shadow', route_state: 'promoted', eligible: false },
    { provider_id: 'candidate', mode: 'candidate', eligible: true },
    { provider_id: 'native', mode: 'active', eligible: true },
  ] } }, 'cap')?.provider_id).toBe('native');
  expect(selectExecutionProvider({ routes: { cap: [{ provider_id: 'candidate', mode: 'candidate', eligible: true }] } }, 'cap')).toBeNull();
});

test('default execution uses bounded ready-wave parallelism and persists intent before owner calls', async () => {
  const fixture = fakeOwners(); let active = 0, maximum = 0;
  const owner = createHypervisorExecution({ ...fixture, maxParallel: 2, ownerCalls: { recursive: async (_tool, args) => {
    expect(fixture.attempts.some(item => item.task_id === args.root_id || item.task_id === 'a')).toBe(true);
    maximum = Math.max(maximum, ++active); await new Promise(resolve => setTimeout(resolve, 10)); active--;
    return { closure_id: `${args.root_id}-${maximum}`, digest: DIGEST };
  } } });
  const result = await owner.execute(MISSION);
  expect(maximum).toBe(2);
  expect(result.state).toBe('passed');
  expect(result.resolution_scope).toBe('task-operations-only');
  expect(result.results.map(item => item.state)).toEqual(['passed', 'passed', 'passed']);
  expect(fixture.attempts.map(item => item.task_id)).toEqual(['a', 'b', 'c']);
  expect(fixture.finished[0].state).toBe('passed');
});

test('owner transport failure leaves uncertain result and blocks downstream execution', async () => {
  const fixture = fakeOwners();
  const owner = createHypervisorExecution({ ...fixture, ownerCalls: { recursive: async () => { throw new Error('lost response'); } } });
  const result = await owner.execute(MISSION);
  expect(result.state).toBe('blocked');
  expect(fixture.completions.filter(item => item.state === 'uncertain')).toHaveLength(2);
  expect(fixture.blocked).toEqual([]);
  expect(fixture.states.get('c').state).toBe('planned');
  expect(fixture.attempts).toHaveLength(2);
});

test('failed wave joins in-flight sibling and starts no third effect', async () => {
  const rows = ['a', 'b', 'c'].map(id => ({ id, title: id, capability: 'substrate.affected_closure', dependencies: [] }));
  const fixture = fakeOwners(missionFor(rows)); let calls = 0, siblingDone = false;
  const owner = createHypervisorExecution({ ...fixture, maxParallel: 2, ownerCalls: { recursive: async () => {
    const position = ++calls;
    if (position === 1) { await new Promise(resolve => setTimeout(resolve, 2)); return { state: 'failed' }; }
    await new Promise(resolve => setTimeout(resolve, 20)); siblingDone = true;
    return { closure_id: 'second', digest: DIGEST };
  } } });
  const result = await owner.execute(MISSION);
  expect(calls).toBe(2);
  expect(siblingDone).toBe(true);
  expect(fixture.states.get('c').state).toBe('planned');
  expect(result.state).toBe('failed');
});

test('effectful Fabric task refuses without authorization before any owner invocation', async () => {
  const fixture = fakeOwners(missionFor([{ id: 'agent', title: 'run', capability: 'agent.executor', dependencies: [] }]));
  let calls = 0, admittedClass;
  const owner = createHypervisorExecution({ ...fixture, authorizeEffect: input => { admittedClass = input.effect_class; return false; },
    ownerCalls: { fabric: async () => { calls++; } } });
  const result = await owner.execute(MISSION);
  expect(result.state).toBe('blocked'); expect(calls).toBe(0); expect(fixture.attempts).toHaveLength(0);
  expect(fixture.blocked[0].reason).toBe('effect_not_authorized');
  expect(admittedClass).toBe('EXTERNAL_IRREVERSIBLE_EFFECT');
});

test('task attempt identity includes current context, route and provider, not only definition', async () => {
  const fixture = fakeOwners(missionFor([{ id: 'plan', title: 'plan', capability: 'scheduler.deterministic_dag', dependencies: [] }]));
  const owner = createHypervisorExecution(fixture);
  await owner.executeSingleTask(MISSION, 'plan');
  const first = fixture.attempts[0].input_digest;
  fixture.packet.digest = 'e'.repeat(64);
  await owner.executeSingleTask(MISSION, 'plan');
  expect(fixture.attempts[1].input_digest).not.toBe(first);
  expect(fixture.attempts[1].context_packet_id).toBe(PACKET);
  expect(fixture.attempts[1].plan_digest).toBe(fixture.mission.execution_plan.digest);
});

test('Fabric queue receipt is not completion; terminal result requires matching durable receipt', async () => {
  const fixture = fakeOwners(missionFor([{ id: 'agent', title: 'run', capability: 'agent.executor', dependencies: [] }]));
  let polls = 0;
  fixture.route.routes['agent.executor'][0].provider_id = 'bonfyre-native';
  const owner = createHypervisorExecution({ ...fixture, authorizeEffect: () => true, sleep: async () => {}, fabricPollMs: 1,
    renderContext: () => 'Exact purpose-bound packet body',
    ownerCalls: { fabric: async name => name === 'implementation_relay' ? { task_id: 'fabric-1', state: 'queued' } :
      ++polls === 1 ? { id: 'fabric-1', state: 'running' } : { id: 'fabric-1', state: 'completed',
        result: { receipt: { receipt_id: 'receipt-1', sha256: DIGEST } }, receipts: [{ id: 'receipt-1', sha256: DIGEST }] } } });
  const result = await owner.execute(MISSION);
  expect(polls).toBe(2); expect(result.state).toBe('passed');
  expect(result.results[0].result.fabric_state).toBe('completed');
});

test('two completed verifier agent runs do not become a goal-verification claim', async () => {
  const fixture = fakeOwners(missionFor([{ id: 'verify', title: 'verify', capability: 'agent.verifier', dependencies: [] }]));
  fixture.route.routes['agent.verifier'][0].provider_id = 'bonfyre-native';
  const owner = createHypervisorExecution({ ...fixture, authorizeEffect: () => true, renderContext: () => 'Exact packet',
    ownerCalls: { fabric: async name => name === 'dual_verify' ? { task_id: 'fabric-verify' } : {
      id: 'fabric-verify', state: 'completed', result: { receipt: { receipt_id: 'rec', sha256: DIGEST }, verifiers: { claude: 'ran', codex: 'ran' } },
      receipts: [{ id: 'rec', sha256: DIGEST }],
    } } });
  const result = await owner.execute(MISSION);
  expect(result.state).toBe('blocked');
  expect(result.results[0].state).toBe('blocked');
});

test('only separate typed behavioral-verifier receipt can resolve a goal', async () => {
  const fixture = fakeOwners(missionFor([{ id: 'verify', title: 'verify', capability: 'agent.verifier', dependencies: [] }]));
  fixture.route.routes['agent.verifier'][0].provider_id = 'bonfyre-native';
  const owner = createHypervisorExecution({ ...fixture, authorizeEffect: () => true, renderContext: () => 'Exact packet',
    verifyGoal: async () => ({ verified: true, authority: 'independent-behavioral-verifier',
      evidence: [{ test: 'behavioral' }], receipt: { receipt_id: 'independent-1', digest: DIGEST } }),
    ownerCalls: { fabric: async name => name === 'dual_verify' ? { task_id: 'fabric-verify' } : {
      id: 'fabric-verify', state: 'completed', result: { receipt: { receipt_id: 'rec', sha256: DIGEST } },
      receipts: [{ id: 'rec', sha256: DIGEST }],
    } } });
  const result = await owner.execute(MISSION);
  expect(result.state).toBe('resolved');
  expect(result.resolution_scope).toBe('independent-goal-verification');
});

test('real SQLite owner stores context, intents, task completions and mission proof', async () => {
  const home = mkdtempSync(join(tmpdir(), 'bonfyre-hypervisor-execution-'));
  const registryPath = join(home, 'ecosystem_registry.json');
  writeFileSync(registryPath, JSON.stringify({ capability_contracts: ['scheduler.deterministic_dag', 'proof.lineage'], projects: [{
    id: 'native', repo: 'local/native', homepage: 'https://example.test/native', license: 'MIT', integration: 'native',
    default_mode: 'active', capabilities: ['scheduler.deterministic_dag', 'proof.lineage'], detect: [],
  }] }));
  const store = createHypervisorStore({ home, registryPath, now: () => 1800000000.25 });
  try {
    store.loadRegistry();
    const db = new Database(store.dbPath); db.query("UPDATE providers SET installed=1,health='verified' WHERE id='native'").run(); db.close();
    const mission = missionFor([{ id: 'plan', title: 'plan', capability: 'scheduler.deterministic_dag', dependencies: [] },
      { id: 'proof', title: 'proof', capability: 'proof.lineage', dependencies: ['plan'] }]);
    store.createMission({ mission, spec: renderMissionSpec(mission), graph: buildMissionGraph(mission.tasks) });
    const missions = createHypervisorMissions({ store });
    const context = createHypervisorContext({ store });
    const compileContext = args => context.compile({ ...args, budget_tokens: 10000 });
    const owner = createHypervisorExecution({ store, missions, compileContext });
    const result = await owner.execute(MISSION);
    expect(result.state).toBe('passed');
    expect(store.missionStatus(MISSION).tasks.every(row => row.state === 'passed')).toBe(true);
    expect(store.verifyReceiptChain().verified).toBe(true);
    expect(owner.status(MISSION).state).toBe('passed');
    await expect(owner.execute(MISSION)).rejects.toThrow(/already|reconcile|changed|running|uncertain/);
  } finally { store.close(); rmSync(home, { recursive: true, force: true }); }
});

test('concurrent provider quarantine refuses next wave inside durable beginTask', async () => {
  const home = mkdtempSync(join(tmpdir(), 'bonfyre-hypervisor-revocation-'));
  const registryPath = join(home, 'ecosystem_registry.json');
  writeFileSync(registryPath, JSON.stringify({ capability_contracts: ['scheduler.deterministic_dag'], projects: [{
    id: 'native', repo: 'local/native', homepage: 'https://example.test/native', license: 'MIT', integration: 'native',
    default_mode: 'active', capabilities: ['scheduler.deterministic_dag'], detect: [],
  }] }));
  const store = createHypervisorStore({ home, registryPath, now: () => 1800000000.25 });
  try {
    store.loadRegistry();
    const db = new Database(store.dbPath); db.query("UPDATE providers SET installed=1,health='verified' WHERE id='native'").run(); db.close();
    const mission = missionFor([{ id: 'first', title: 'first', capability: 'scheduler.deterministic_dag', dependencies: [] },
      { id: 'second', title: 'second', capability: 'scheduler.deterministic_dag', dependencies: ['first'] }]);
    store.createMission({ mission, spec: renderMissionSpec(mission), graph: buildMissionGraph(mission.tasks) });
    let revoked = false;
    const guarded = { ...store, completeTask(args) {
      const result = store.completeTask(args);
      if (args.task_id === 'first' && !revoked) { revoked = true; store.quarantine('native', 'capability revoked during mission'); }
      return result;
    } };
    const context = createHypervisorContext({ store: guarded });
    const owner = createHypervisorExecution({ store: guarded, missions: createHypervisorMissions({ store: guarded }),
      compileContext: args => context.compile({ ...args, budget_tokens: 10000 }) });
    const result = await owner.execute(MISSION);
    expect(revoked).toBe(true);
    expect(result.state).toBe('blocked');
    expect(store.missionStatus(MISSION).tasks.map(row => row.state)).toEqual(['passed', 'planned']);
    expect(store.verifyReceiptChain().verified).toBe(true);
  } finally { store.close(); rmSync(home, { recursive: true, force: true }); }
});
