import { createHash } from 'node:crypto';
import { compileTaskPlan, diffTaskPlans, missionPlanContext } from './mission_plan.mjs';

export const DEFAULT_CONSTRAINTS = Object.freeze([
  'Preserve unrelated dirty-tree changes.',
  'No force push, broad reset, secret mutation, payment, outreach, or public publishing.',
  'Compilation and reachability do not prove useful behavior.',
  'Every state mutation requires a receipt and a rollback or compensation path.',
  'Use native project/session state rather than flattening it into prose.',
]);
const TRAIT_TOKENS = Object.freeze({
  high_risk: ['security', 'payment', 'auth', 'migration', 'production', 'release', 'database', 'permission', 'credential'],
  large: ['all ', 'entire', 'full', '93', 'nine app', 'system', 'estate', 'platform', 'rewrite', 'compiler'],
  remote: ['oracle', 'ssh', 'cloud', 'remote'],
  interactive: ['dashboard', 'terminal', 'fleet', 'watch', 'interactive', 'ui'],
  pr: ['pull request', ' pr ', 'ci', 'review comment'],
  model: ['model', 'fpq', 'qwen', 'onnx', 'whisper', 'tensor', 'kv cache'],
  audit: ['audit', 'proof', 'replay', 'lineage', 'receipt', 'compliance'],
  memory: ['memory', 'history', 'context', 'session'],
});
const BASE_CAPABILITIES = Object.freeze([
  'spec.compile', 'memory.search', 'context.tool_compression', 'agent.architect', 'agent.executor', 'agent.reviewer',
  'agent.verifier', 'workspace.worktree', 'proof.lineage', 'proof.replay',
  'substrate.mission.bootstrap', 'substrate.affected_closure', 'substrate.runtime_image.compile',
  'substrate.mission.finalize', 'proof.substrate.replay',
  'canary.route.decide', 'canary.outcome.record', 'canary.certificate.verify',
  'spines.temporal.bootstrap', 'spines.temporal.parity', 'spines.reasoning.compile',
  'spines.authority.preflight', 'spines.model.plan', 'spines.kernel.verify', 'spines.finalize',
]);
const TRAIT_CAPABILITIES = Object.freeze({
  large: ['scheduler.deterministic_dag', 'scheduler.wave_execution', 'fleet.status_detection'],
  high_risk: ['gate.human', 'proof.audit_chain'], remote: ['deployment.remote_ssh'],
  interactive: ['fleet.pty', 'ui.terminal'], pr: ['review.pr', 'worker.pr_maintenance'],
  memory: ['memory.capture', 'memory.timeline'], audit: ['proof.audit_chain'],
  model: ['context.memory_compression'],
});
const BASE_TASKS = Object.freeze([
  ['T-001', 'Compile durable mission spec and invariants', 'spec.compile', []],
  ['T-001A', 'Canonicalize mission artifacts, compile exact TokenArtifacts, emit .flux, and compile the seven IRs', 'substrate.mission.bootstrap', ['T-001']],
  ['T-001B', 'Compile the bounded affected dependency closure and graph frontier', 'substrate.affected_closure', ['T-001A']],
  ['T-002', 'Retrieve prior cross-agent memory progressively', 'memory.search', ['T-001A']],
  ['T-003', 'Map live project, Oracle, estate, recursive substrate and session state', 'agent.architect', ['T-001B', 'T-002']],
  ['T-004', 'Construct dependency graph and select capability providers', 'scheduler.deterministic_dag', ['T-003']],
  ['T-004R', 'Compile token-aware provider demand and a candidate RuntimeImage', 'substrate.runtime_image.compile', ['T-004']],
  ['T-005', 'Execute bounded changes in isolated workspaces', 'agent.executor', ['T-004R']],
  ['T-006', 'Run targeted tests and live behavioral verification', 'agent.verifier', ['T-005']],
  ['T-007', 'Cross-review findings and reconcile disagreements', 'agent.reviewer', ['T-006']],
  ['T-008F', 'Finalize graph projection, RuntimeImage, sparse BFP successor candidate, and substrate replay', 'substrate.mission.finalize', ['T-007']],
  ['T-008', 'Seal receipts, replay head, promotion decision and follow-ups', 'proof.lineage', ['T-008F']],
]);
const ADDITIONAL_TASKS = Object.freeze({
  'T-004G': ['T-004G', 'Obtain explicit human disposition for high-risk effects', 'gate.human', ['T-004R']],
  'T-006R': ['T-006R', 'Verify remote placement, parity and rollback', 'deployment.remote_ssh', ['T-006']],
  'T-006P': ['T-006P', 'Reconcile PR checks and review threads without force push', 'worker.pr_maintenance', ['T-006']],
  'T-001C': ['T-001C', 'Append immutable mission .flux, derive .delta, update only the affected closure, and prove incremental/full parity', 'spines.temporal.bootstrap', ['T-001B']],
  'T-003R': ['T-003R', 'Saturate bounded execution alternatives, compile pure .helix, reduce a witness, and seal candidate .crux', 'spines.reasoning.compile', ['T-003']],
  'T-004A': ['T-004A', 'Classify effect authority before provider or model execution', 'spines.authority.preflight', ['T-004R']],
  'T-004M': ['T-004M', 'Compile model ExecutionPhenotypeIR, kernel parity, DistributedIR placement, and candidate .axon/.kore/.fuse', 'spines.model.plan', ['T-004A']],
  'T-007K': ['T-007K', 'Re-run native kernel parity after behavioral verification and cross-review', 'spines.kernel.verify', ['T-007']],
  'T-008S': ['T-008S', 'Replay R5 temporal state and seal the cumulative R5-R7 candidate proof', 'spines.finalize', ['T-008F']],
});
const INVARIANTS = Object.freeze([
  ['V-001', 'Unrelated working-tree changes remain byte-preserved.', 'preimage/postimage CAS receipts'],
  ['V-002', 'Every resolved claim has direct local or live runtime evidence.', 'independent verifier receipts'],
  ['V-003', 'Task release follows the declared dependency graph.', 'deterministic scheduler journal'],
  ['V-004', 'External provider state is represented by native IDs and immutable references.', 'session/source lineage records'],
  ['V-005', 'A failed test backpropagates into a bug record and, when generalizable, a new invariant.', 'bug-to-invariant link'],
  ['V-006', 'Mission source and context are represented by exact tokenizer-qualified TokenArtifacts before agent execution.', 'native Rust token manifest, byte/token span sidecars, and token receipt'],
  ['V-007', 'Only the bounded affected dependency closure advances after a logical change.', 'affected-closure graph and incremental/full parity obligation'],
  ['V-008', 'Recursive substrate output remains candidate-only until Zig authority and reconstruction proof accept it.', 'AuthorityIR, ProofIR, RuntimeImage and BFP successor receipt'],
  ['V-R5-001', 'Incremental state must equal clean replay for the same immutable .flux history.', 'R5 parity proof and stream-chain verification'],
  ['V-R6-001', 'Recursive reduction is bounded, pure, witnessed, and cannot directly perform effects.', 'R6 .helix resource bounds, native witness and AuthorityIR preflight'],
  ['V-R7-001', 'Model and placement output remains candidate-only until kernel parity and Zig authority accept it.', 'ExecutionPhenotypeIR, kernel parity, DistributedIR and .fuse candidate receipt'],
]);
const VERIFICATION = Object.freeze([
  ['P-001', 'Mission task graph is complete and dependency-valid.', 'deterministic graph validation'],
  ['P-002', 'Changed source passes affected tests.', 'changed-path test inference'],
  ['P-003', 'Live behavior matches the mission goal.', 'Oracle/runtime/CLI behavioral probe'],
  ['P-004', 'Independent Claude and Codex verification converge or preserve an explicit disagreement.', 'dual-agent review receipt'],
  ['P-005', 'Receipt chain replays without divergence.', 'SHA-256 lineage verification'],
  ['P-006', 'Token, .flux and substrate receipt chains replay without divergence.', 'native token manifest verification plus hash-linked substrate replay'],
  ['P-007', 'All seven canonical IR projections exist for the mission before authoritative execution.', 'recursive substrate IR bundle receipt'],
  ['P-R5-001', 'Affected-only R5 state equals a clean replay.', 'spines.temporal.parity'],
  ['P-R6-001', 'The selected pure term has a bounded native reduction witness.', 'spines.reasoning.compile'],
  ['P-R7-001', 'Reference kernel parity and placement constraints are satisfied.', 'spines.kernel.verify'],
]);

function text(value, name, maximum = 16384) {
  if (typeof value !== 'string' || !value.trim() || value.includes('\0') || Buffer.byteLength(value) > maximum)
    throw new Error(`invalid ${name}`);
  return value;
}
function strings(values, name, maximum = 128) {
  if (!Array.isArray(values) || values.length > maximum) throw new Error(`invalid ${name}`);
  return values.map((value, index) => text(value, `${name}[${index}]`, 4096));
}
const sha = value => createHash('sha256').update(value).digest('hex');
const task = ([id, title, capability, dependencies]) => ({ id, title, capability, dependencies: [...dependencies],
  owner: 'hypervisor', state: 'planned', acceptance: [], evidence: [] });

export function inferMissionTraits(goal) {
  const value = text(goal, 'goal').toLowerCase();
  return Object.fromEntries(Object.entries(TRAIT_TOKENS).map(([name, tokens]) => [name, tokens.some(token => value.includes(token))]));
}
export function compileMissionTasks(traits) {
  const tasks = BASE_TASKS.map(task);
  const find = id => { const item = tasks.find(value => value.id === id); if (!item) throw new Error(`mission task anchor missing: ${id}`); return item; };
  const insertBefore = (anchor, template, dependencies = null) => {
    const position = tasks.findIndex(value => value.id === anchor);
    if (position < 0) throw new Error(`mission task anchor missing: ${anchor}`);
    const item = task(ADDITIONAL_TASKS[template]); if (dependencies) item.dependencies = [...dependencies];
    tasks.splice(position, 0, item); return item;
  };
  const insertAfter = (anchor, template) => {
    const position = tasks.findIndex(value => value.id === anchor);
    if (position < 0) throw new Error(`mission task anchor missing: ${anchor}`);
    tasks.splice(position + 1, 0, task(ADDITIONAL_TASKS[template]));
  };
  if (traits.high_risk) { insertBefore('T-005', 'T-004G'); find('T-005').dependencies = ['T-004G']; }
  if (traits.remote) { insertBefore('T-007', 'T-006R'); find('T-007').dependencies = ['T-006R']; }
  if (traits.pr) {
    const prior = [...find('T-007').dependencies];
    insertBefore('T-007', 'T-006P', prior);
    find('T-007').dependencies = ['T-006P'];
  }
  insertAfter('T-001B', 'T-001C'); find('T-003').dependencies = ['T-001C', 'T-002'];
  insertAfter('T-003', 'T-003R'); find('T-004').dependencies = ['T-003R'];
  insertAfter('T-004R', 'T-004A'); insertAfter('T-004A', 'T-004M');
  if (traits.high_risk) { find('T-004G').dependencies = ['T-004M']; find('T-005').dependencies = ['T-004G']; }
  else find('T-005').dependencies = ['T-004M'];
  insertAfter('T-007', 'T-007K'); find('T-008F').dependencies = ['T-007K'];
  insertAfter('T-008F', 'T-008S'); find('T-008').dependencies = ['T-008S'];
  return tasks;
}

export function buildMissionGraph(tasks) {
  if (!Array.isArray(tasks) || tasks.length > 512) throw new Error('invalid mission tasks');
  const ids = new Set(tasks.map(item => item.id)), errors = [], indegree = new Map(), children = new Map();
  for (const item of tasks) { indegree.set(item.id, 0); children.set(item.id, []); }
  if (ids.size !== tasks.length) errors.push({ error: 'duplicate_task_id' });
  for (const item of tasks) for (const dependency of item.dependencies ?? []) {
    if (!ids.has(dependency)) { errors.push({ task: item.id, error: 'missing_dependency', dependency }); continue; }
    indegree.set(item.id, indegree.get(item.id) + 1); children.get(dependency).push(item.id);
  }
  let frontier = [...indegree].filter(([, degree]) => degree === 0).map(([id]) => id).sort();
  const waves = [], visited = new Set();
  while (frontier.length) {
    const wave = frontier; waves.push(wave); const next = [];
    for (const id of wave) { visited.add(id); for (const child of children.get(id)) {
      indegree.set(child, indegree.get(child) - 1); if (indegree.get(child) === 0) next.push(child);
    } }
    frontier = next.sort();
  }
  if (visited.size !== tasks.length) errors.push({ error: 'cycle', unresolved: [...ids].filter(id => !visited.has(id)).sort() });
  return { valid: errors.length === 0, errors, waves, nodes: tasks };
}

export function renderMissionSpec(mission) {
  const lines = [
    `# Bonfyre Mission ${mission.id}`, '', '## §G Goal', '', mission.goal, '', '## §C Constraints', '',
    ...mission.constraints.map(value => `- ${value}`), '', '## §I Interfaces', '',
    '| ID | Name | Input | Output |', '|---|---|---|---|',
    ...mission.interfaces.map(item => `| ${item.id} | ${item.name} | ${item.input} | ${item.output} |`),
    '', '## §R Research', '', '| Source | Finding | Evidence |', '|---|---|---|',
    ...mission.research.map(item => `| ${item.source ?? ''} | ${item.finding ?? ''} | ${item.evidence ?? ''} |`),
    '', '## §V Invariants', '', '| ID | Statement | Proof |', '|---|---|---|',
    ...mission.invariants.map(item => `| ${item.id} | ${item.statement} | ${item.proof ?? ''} |`),
    '', '## §T Tasks', '', '| ID | State | Capability | Dependencies | Title |', '|---|---|---|---|---|',
    ...mission.tasks.map(item => `| ${item.id} | ${item.state ?? 'planned'} | ${item.capability ?? ''} | ${item.dependencies?.join(',') || '-'} | ${item.title} |`),
    '', '## §B Bugs and Backpropagation', '', '| ID | State | Symptom | Root cause | Regression test | Invariant |',
    '|---|---|---|---|---|---|',
    ...mission.bugs.map(item => `| ${item.id} | ${item.state ?? 'open'} | ${item.symptom} | ${item.root_cause ?? ''} | ${item.regression_test ?? ''} | ${item.invariant_id || '-'} |`),
    '', '## Verification Contract', '', '| ID | Claim | Method |', '|---|---|---|',
    ...mission.verification.map(item => `| ${item.id} | ${item.claim} | ${item.method} |`),
    '', '## Promotion', '', '```json', JSON.stringify(mission.promotion, null, 2), '```',
    '', '## Context Budget', '', '```json', JSON.stringify(mission.context_budget, null, 2), '```',
  ];
  return `${lines.join('\n')}\n`;
}

function safePlan(tasks, context) {
  const graph = buildMissionGraph(tasks);
  if (!graph.valid) throw new Error(`invalid mission graph: ${JSON.stringify(graph.errors)}`);
  return { graph, plan: compileTaskPlan(tasks, { context }) };
}

export function createHypervisorMissions({ store, checkStop = () => {} } = {}) {
  if (!store?.createMission || !store?.getMission || !store?.routeMany || !store?.saveBackpropagation || !store?.saveRoute)
    throw new Error('hypervisor mission store required');
  const stopped = () => checkStop();
  function compile(goal, { constraints = [], requested_capabilities = [], budget_tokens = 160000, budget_cost_usd = null } = {}) {
    stopped(); goal = text(goal, 'goal'); constraints = strings(constraints, 'constraints');
    requested_capabilities = strings(requested_capabilities, 'requested capabilities', 512);
    if (!Number.isSafeInteger(budget_tokens) || budget_tokens <= 0 || budget_tokens > 10_000_000) throw new Error('invalid token budget');
    if (budget_cost_usd !== null && (!Number.isFinite(budget_cost_usd) || budget_cost_usd < 0)) throw new Error('invalid cost budget');
    const id = store.id(); if (!/^[a-f0-9]{32}$/.test(id)) throw new Error('invalid mission id');
    const traits = inferMissionTraits(goal), tasks = compileMissionTasks(traits);
    const capabilities = new Set([...requested_capabilities, ...BASE_CAPABILITIES]);
    for (const [trait, extras] of Object.entries(TRAIT_CAPABILITIES)) if (traits[trait]) for (const item of extras) capabilities.add(item);
    for (const item of tasks) capabilities.add(item.capability);
    const mission = { schema: 'bonfyre.mission.v5', id, goal, traits,
      constraints: [...DEFAULT_CONSTRAINTS, ...constraints], requested_capabilities: [...capabilities].sort(),
      interfaces: [{ id: 'I-001', name: 'Bonfyre Capability Contract', input: 'typed mission/context packet', output: 'typed receipt/evidence/native state pointers' }],
      research: [], invariants: INVARIANTS.map(([key, statement, proof]) => ({ id: key, statement, proof })),
      tasks, bugs: [], verification: VERIFICATION.map(([key, claim, method]) => ({ id: key, claim, method })),
      promotion: { shadow_required: true, canary_required: traits.high_risk || traits.large,
        minimum_score: traits.high_risk ? 0.75 : 0.6, retire_on_regression: true },
      context_budget: { tokens: budget_tokens, cost_usd: budget_cost_usd, compression: 'adaptive', progressive_disclosure: true },
      created_at: store.now(),
    };
    const { graph, plan } = safePlan(tasks, missionPlanContext(mission)); mission.execution_plan = plan;
    stopped();
    const paths = store.createMission({ mission, spec: renderMissionSpec(mission), graph });
    return { mission, ...paths };
  }
  function get(missionId) { return store.getMission(text(missionId, 'mission id', 128)); }
  function route(missionId, { include_shadow = true } = {}) {
    stopped(); const mission = get(missionId);
    if (typeof include_shadow !== 'boolean') throw new Error('include_shadow must be boolean');
    const routes = store.routeMany(mission.requested_capabilities, { include_shadow });
    const gaps = mission.requested_capabilities.filter(capability => !routes[capability]?.some(candidate => candidate.eligible === true));
    const selected = Object.fromEntries(mission.requested_capabilities.map(capability => [capability,
      routes[capability]?.find(candidate => candidate.eligible === true)?.provider_id ?? null]));
    const context = missionPlanContext(mission);
    const baseline = compileTaskPlan(mission.tasks, { context });
    const routePlan = compileTaskPlan(mission.tasks.map(item => selected[item.capability] ?
      { ...item, provider_selection: selected[item.capability] } : item), { context });
    const selection = { mission_id: mission.id, routes, gaps, complete: gaps.length === 0,
      execution_plan_digest: baseline.digest, route_plan: routePlan,
      route_impact: diffTaskPlans(baseline, routePlan) };
    stopped(); store.saveRoute({ mission_id: mission.id, payload: selection });
    return selection;
  }
  function backpropagate(missionId, { symptom, root_cause, regression_test, generalize = true, evidence = [] } = {}) {
    stopped(); const mission = get(missionId);
    symptom = text(symptom, 'symptom'); root_cause = text(root_cause, 'root cause');
    regression_test = text(regression_test, 'regression test');
    if (typeof generalize !== 'boolean' || !Array.isArray(evidence) || evidence.length > 128 ||
      evidence.some(item => !item || typeof item !== 'object' || Array.isArray(item))) throw new Error('invalid failure evidence');
    const bugId = `B-${sha(`${mission.id}\0${symptom}\0${root_cause}`).slice(0, 12)}`;
    const invariantId = generalize ? `V-${sha(root_cause).slice(0, 8).toUpperCase()}` : null;
    const prior = compileTaskPlan(mission.tasks, { context: missionPlanContext(mission) });
    if (invariantId && !mission.invariants.some(item => item.id === invariantId)) mission.invariants.push({
      id: invariantId, statement: `Regression class prevented: ${root_cause}. Proof: ${regression_test}`,
      proof: regression_test, source_bug: bugId,
    });
    const bug = { id: bugId, symptom, root_cause, regression_test, invariant_id: invariantId, state: 'open',
      evidence, created_at: store.now() };
    mission.bugs = [...mission.bugs.filter(item => item.id !== bugId), bug];
    const next = compileTaskPlan(mission.tasks, { context: missionPlanContext(mission) });
    mission.execution_plan = next;
    const invalidation = diffTaskPlans(prior, next);
    stopped(); store.saveBackpropagation({ mission, spec: renderMissionSpec(mission), graph: buildMissionGraph(mission.tasks), bug });
    return { bug, new_invariant: invariantId, invalidation };
  }
  return { compile, route, backpropagate, get };
}
