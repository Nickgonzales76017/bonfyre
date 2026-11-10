// Durable mission execution over the shared task plan. A route is an eligible
// candidate, never permission for an effect; owner calls are injected by the
// authenticated MCP boundary and are never retried after an uncertain outcome.
import { compileTaskPlan, missionPlanContext } from './mission_plan.mjs';
import { digestValue } from './canonical_json.mjs';
import { renderMissionSpec } from './hypervisor_missions.mjs';

const object = value => value !== null && typeof value === 'object' && !Array.isArray(value);
const own = (value, key) => object(value) && Object.hasOwn(value, key);
const token = (value, key) => typeof value === 'string' && value.length > 0 && value.length <= 512 ? value : (() => { throw new Error(`invalid ${key}`); })();
const stateOf = value => {
  if (value?.mcp_error || value?.isError) return 'failed';
  const state = object(value) && typeof value.state === 'string' ? value.state.toLowerCase() : '';
  if (['blocked', 'waiting', 'approval_required'].includes(state)) return 'blocked';
  if (state === 'cancelled') return 'cancelled';
  if (['passed', 'completed', 'resolved', 'verified', 'success'].includes(state)) return 'passed';
  return 'failed';
};
const receipt = value => object(value) && typeof value.receipt_id === 'string' && value.receipt_id.length > 0;
const named = (value, key) => object(value) && typeof value[key] === 'string' && value[key].length > 0;
const proof = value => object(value) && value.verified === true && Array.isArray(value.failures) && value.failures.length === 0;
const seal = value => object(value) && stateOf(value) === 'passed' && receipt(value.receipt) && value.receipt.state === 'passed';
const recursiveProof = value => object(value) && stateOf(value) === 'passed' && receipt(value.receipt) && value.receipt.state === 'passed';
// The substrate returns the seven canonical IR projections, each with its own
// digest, and no bundle-level digest. P-007 asks that all seven exist, so that
// is what acceptance checks; a bundle digest alone would say less.
const IR_PROJECTIONS = ['ArtifactIR', 'AuthorityIR', 'DeltaIR', 'DistributedIR', 'EquivalenceIR', 'ExecutionPhenotypeIR', 'ProofIR'];
const irBundle = value => named(value, 'digest') ||
  (object(value) && object(value.objects) && IR_PROJECTIONS.every(name => named(value.objects[name], 'digest')));
const blocked = reason => ({ state: 'blocked', reason });
const EXTERNAL_EFFECTS = new Set(['agent.architect', 'agent.executor', 'agent.reviewer', 'agent.verifier', 'worker.pr_maintenance']);

export function selectExecutionProvider(route, capability) {
  const candidates = route?.routes?.[capability];
  if (!Array.isArray(candidates)) return null;
  return candidates.find(item => item?.eligible === true && item.route_state === 'promoted') ??
    candidates.find(item => item?.eligible === true && item.mode === 'active') ?? null;
}

function validateResult(capability, value) {
  if (capability === 'substrate.affected_closure') return named(value, 'closure_id') && named(value, 'digest') ? 'passed' : 'failed';
  if (capability === 'proof.substrate.replay')
    return value?.verified === true && proof(value.receipt_chain) && proof(value.flux_chains) ? 'passed' : 'failed';
  if (stateOf(value) !== 'passed') return stateOf(value);
  switch (capability) {
    case 'spec.compile': return named(value, 'spec_path') && named(value, 'spec_digest') ? 'passed' : 'failed';
    case 'memory.search': case 'memory.timeline': case 'memory.capture':
      return named(value, 'context_packet_id') && (named(value, 'memory_artifact') || named(value, 'search_receipt_id')) ? 'passed' : 'failed';
    case 'scheduler.deterministic_dag': return named(value, 'plan_digest') && Array.isArray(value.waves) ? 'passed' : 'failed';
    case 'substrate.mission.bootstrap': return recursiveProof(value) && irBundle(value.ir_bundle) && named(value.affected_closure, 'closure_id') ? 'passed' : 'failed';
    case 'substrate.runtime_image.compile': return Array.isArray(value.provider_token_bids) && value.provider_token_bids.length === 3 &&
      value.provider_token_bids.every(item => named(item, 'bid_id')) && named(value.runtime_image, 'id') && named(value.runtime_image, 'digest') ? 'passed' : 'failed';
    case 'substrate.mission.finalize': return recursiveProof(value) && named(value.runtime_image, 'digest') && proof(value.proof?.receipts) && proof(value.proof?.flux) ? 'passed' : 'failed';
    case 'spines.temporal.bootstrap': return seal(value) && value.parity?.equal === true ? 'passed' : 'failed';
    case 'spines.reasoning.compile': return seal(value) && value.reduction?.state === 'passed' && named(value.reduction, 'reduction_id') ? 'passed' : 'failed';
    case 'spines.authority.preflight': return seal(value) && ['allowed_pure', 'allowed_read', 'candidate_only'].includes(value.authority?.decision) ? 'passed' : 'failed';
    case 'spines.model.plan': return seal(value) && value.kernel_parity?.state === 'passed' && named(value.phenotype, 'phenotype_id') ? 'passed' : 'failed';
    case 'spines.kernel.verify': return value.state === 'passed' && named(value, 'parity_id') && named(value, 'digest') && receipt(value.receipt) ? 'passed' : 'failed';
    case 'spines.finalize': return seal(value) && value.verified === true && proof(value.receipts) ? 'passed' : 'failed';
    case 'proof.lineage': case 'proof.replay': case 'proof.audit_chain': return proof(value.verification) ? 'passed' : 'failed';
    case 'deployment.remote_ssh': return value.mesh?.state === 'passed' && value.release?.state === 'passed' ? 'passed' : 'failed';
    case 'agent.verifier':
      // Fabric's dual_verify guarantees two agent runs, not that the mission
      // actually satisfies its goal. A typed verdict must come from the
      // behavioral verification owner before this task can pass.
      return value.goal_verification?.verified === true && value.goal_verification?.authority === 'independent-behavioral-verifier' &&
        named(value.goal_verification?.receipt, 'receipt_id') && named(value.goal_verification?.receipt, 'digest') &&
        Array.isArray(value.goal_verification.evidence) && value.goal_verification.evidence.length > 0 &&
        named(value, 'task_id') && named(value.receipt, 'receipt_id') &&
        named(value.receipt, 'sha256') && value.fabric_state === 'completed' ? 'passed' : 'blocked';
    case 'agent.architect': case 'agent.executor': case 'agent.reviewer': case 'worker.pr_maintenance':
      return named(value, 'task_id') && named(value.receipt, 'receipt_id') && named(value.receipt, 'sha256') &&
        value.fabric_state === 'completed' ? 'passed' : 'failed';
    default: return 'failed';
  }
}

const remote = (owner, tool, args, timeoutMs) => ({ owner, tool, args, timeoutMs });
const REMOTE = Object.freeze({
  'substrate.mission.bootstrap': ({ mission, packet, specPath }) => remote('recursive', 'recursive_mission_bootstrap',
    { mission, spec_path: specPath, context_packet_path: packet.path, tokenizer_id: 'lexeme-v1' }, 3_600_000),
  'substrate.affected_closure': ({ mission }) => remote('recursive', 'affected_closure_compile',
    { root_kind: 'mission', root_id: mission.id, max_depth: 16 }, 600_000),
  'substrate.mission.finalize': ({ mission, route }) => remote('recursive', 'recursive_mission_finalize',
    { mission, provider_routes: route, session_pointers: [], changed_sections: ['mission', 'token', 'ir', 'graph', 'proof'] }, 3_600_000),
  'proof.substrate.replay': () => remote('recursive', 'substrate_replay_verify', {}, 600_000),
  'spines.temporal.bootstrap': ({ mission, task }) => remote('spines', 'mission_r5_bootstrap', { mission, task }, 1_800_000),
  'spines.reasoning.compile': ({ mission, task, packet }) => remote('spines', 'mission_r6_reason',
    { mission, task, token_estimate: packet.token_estimate }, 1_800_000),
  'spines.authority.preflight': ({ mission, task }) => remote('spines', 'mission_r6_authority',
    { mission, task, effect_class: task.effect_class || mission.effect_class || 'CANDIDATE_WRITE' }, 600_000),
  'spines.model.plan': ({ mission, task, packet }) => remote('spines', 'mission_r7_plan',
    { mission, task, token_estimate: packet.token_estimate, model_family: mission.model_family || 'BonfyreQwenFPQ' }, 3_600_000),
  'spines.kernel.verify': () => remote('spines', 'r7_kernel_parity', { backend: 'bonfyre-reference-cpu' }, 600_000),
  'spines.finalize': ({ mission }) => remote('spines', 'mission_spines_finalize', { mission }, 3_600_000),
});

export function createHypervisorExecution({ store, missions, compileContext, ownerCalls = {}, authorizeEffect = () => false,
  checkStop = () => {}, maxParallel = 4, sleep = ms => new Promise(resolve => setTimeout(resolve, ms)),
  fabricPollMs = 5000, fabricWaitMs = 10_800_000, renderContext, verifyGoal } = {}) {
  for (const key of ['getMissionRuntime', 'taskStates', 'beginMission', 'beginTask', 'completeTask', 'blockTask', 'finishMission', 'missionStatus', 'verifyReceiptChain'])
    if (typeof store?.[key] !== 'function') throw new Error(`hypervisor execution store missing ${key}`);
  if (typeof missions?.get !== 'function' || typeof missions?.route !== 'function' || typeof compileContext !== 'function')
    throw new Error('mission, routing and real context owners required');
  if (!Number.isSafeInteger(maxParallel) || maxParallel < 1 || maxParallel > 32) throw new Error('invalid parallel budget');
  if (!Number.isSafeInteger(fabricPollMs) || fabricPollMs < 1 || fabricPollMs > 60_000 ||
      !Number.isSafeInteger(fabricWaitMs) || fabricWaitMs < fabricPollMs || fabricWaitMs > 10_800_000)
    throw new Error('invalid fabric polling budget');
  const active = new Set();
  const stopped = signal => { checkStop(); if (signal?.aborted) throw new Error('mission execution cancelled'); };
  const call = async (instruction, signal) => {
    const owner = ownerCalls[instruction.owner];
    if (typeof owner !== 'function') return blocked(`owner ${instruction.owner} unavailable`);
    return owner(instruction.tool, instruction.args, { timeoutMs: instruction.timeoutMs, signal });
  };
  async function perform({ mission, task, route, packet, plan }, signal) {
    const capability = task.capability, provider = selectExecutionProvider(route, capability);
    if (!provider) return blocked(`no eligible promoted/active provider for ${capability}`);
    const providerId = token(provider.provider_id, 'provider identity');
    const context = { mission, task, route, packet, plan, specPath: store.missionRoot ? `${store.missionRoot}/${mission.id}/SPEC.md` : null };
    if (capability === 'gate.human') return blocked('explicit human disposition required');
    if (capability === 'spec.compile') {
      if (typeof store.readMissionSpec !== 'function') return blocked('durable compiled spec verifier unavailable');
      const spec = store.readMissionSpec(mission.id);
      return named(spec, 'path') && named(spec, 'digest') && spec.text === renderMissionSpec(mission) ?
        { state: 'passed', provider: providerId, spec_path: spec.path, spec_digest: spec.digest } : blocked('durable compiled spec unavailable');
    }
    if (capability === 'scheduler.deterministic_dag')
      return { state: 'passed', provider: providerId, plan_digest: plan.digest, waves: plan.waves, authority: 'candidate-plan-only' };
    if (['proof.lineage', 'proof.replay', 'proof.audit_chain'].includes(capability)) {
      const verification = store.verifyReceiptChain();
      return { state: verification.verified ? 'passed' : 'failed', provider: providerId, verification };
    }
    if (['memory.search', 'memory.timeline', 'memory.capture'].includes(capability)) {
      if (providerId === 'cavemem' && typeof ownerCalls.memory === 'function') {
        const result = await ownerCalls.memory('memory_search', { query: mission.goal, limit: 16, semantic: true }, { timeoutMs: 120_000, signal });
        return { ...result, context_packet_id: packet.id };
      }
      const memory = packet.sections?.find(section => section.name === 'memory');
      if (!memory?.artifact || typeof store.readContextBody !== 'function') return blocked('memory evidence unavailable');
      const source = store.readContextBody(memory.artifact);
      let observed;
      try { observed = JSON.parse(source); } catch { observed = { state: 'observed', text: source }; }
      if (observed?.state === 'unavailable') return blocked('memory owner unavailable');
      return { state: 'passed', provider: providerId, context_packet_id: packet.id,
        memory_artifact: memory.artifact.digest, summary: source.slice(0, 20_000), representation: memory.mode };
    }
    if (capability === 'substrate.runtime_image.compile') {
      if (typeof ownerCalls.recursive !== 'function') return blocked('recursive owner unavailable');
      const bids = [];
      for (const provider_id of ['claude-code', 'codex', 'bonfyre-native']) {
        stopped(signal);
        const bid = await call(remote('recursive', 'provider_token_bid_compile', { mission_id: mission.id, provider_id,
          tokenizer_id: 'lexeme-v1', required_tokens: packet.token_estimate, reusable_tokens: 0,
          expected_output_tokens: Math.max(2048, Math.floor(packet.token_estimate / 8)), privacy_class: 'internal', compatible: true,
          metadata: { exact_vendor_tokenizer_bound: false, route_source: 'v5-hypervisor' } }, 120_000), signal);
        if (!named(bid, 'bid_id')) return { state: 'failed', reason: 'provider token bid lacks identity', provider_token_bids: bids, failed_bid: bid };
        bids.push(bid);
      }
      stopped(signal);
      const image = await call(remote('recursive', 'runtime_image_compile', { mission_id: mission.id, graph_roots: {}, token_roots: {},
        provider_routes: route, session_pointers: [], authority_state: { effect_class: 'CANDIDATE_WRITE', zig_authority_required: true,
          external_effects: false }, placement: { local: 'active', oracle: 'candidate' } }, 600_000), signal);
      return { state: named(image, 'id') && named(image, 'digest') ? 'passed' : 'failed', provider_token_bids: bids, runtime_image: image };
    }
    if (own(REMOTE, capability)) return call(REMOTE[capability](context), signal);
    if (capability === 'deployment.remote_ssh') {
      if (typeof ownerCalls.bonfyre !== 'function') return blocked('bonfyre placement owner unavailable');
      const mesh = await call(remote('bonfyre', 'mesh_status', {}, 60_000), signal);
      stopped(signal);
      const release = await call(remote('bonfyre', 'release_attest', {}, 120_000), signal);
      return { state: mesh?.state === 'passed' && release?.state === 'passed' ? 'passed' : 'failed', mesh, release };
    }
    const fabric = {
      'agent.architect': ['duel', { title: `Mission architecture ${mission.id}`, prompt: packet.path }],
      'agent.executor': ['implementation_relay', { title: `Mission implementation ${mission.id}`, prompt: packet.path, apply_to_project: false }],
      'agent.reviewer': ['cross_review', { title: `Mission review ${mission.id}`, instructions: `Review mission ${mission.id}; context packet ${packet.path}` }],
      'agent.verifier': ['dual_verify', { title: `Mission verification ${mission.id}`, claim: `Verify mission ${mission.id} against ${packet.path}` }],
      'worker.pr_maintenance': ['pr_converge', { instructions: `Converge mission ${mission.id} against ${packet.path}`, apply_to_project: false }],
    }[capability];
    if (fabric) {
      if (providerId !== 'bonfyre-native' || typeof ownerCalls.fabric !== 'function') return blocked('native fabric owner unavailable');
      if (typeof renderContext !== 'function') return blocked('purpose-bound context renderer unavailable');
      const prompt = renderContext(packet, 'bonfyre-hypervisor');
      if (typeof prompt !== 'string' || !prompt || Buffer.byteLength(prompt) > 8 * 1024 * 1024)
        throw new Error('invalid rendered context');
      const args = { ...fabric[1] };
      if (typeof args.prompt === 'string') args.prompt = prompt;
      if (typeof args.instructions === 'string') args.instructions = `${args.instructions}\n\n${prompt}`;
      if (typeof args.claim === 'string') args.claim = `${args.claim}\n\n${prompt}`;
      const submission = await call(remote('fabric', fabric[0], args, 120_000), signal);
      if (!named(submission, 'task_id')) return { state: 'failed', reason: 'fabric submission lacks task identity', submission };
      const deadline = performance.now() + fabricWaitMs;
      while (performance.now() < deadline) {
        stopped(signal);
        const current = await call(remote('fabric', 'task_status', { task_id: submission.task_id }, 30_000), signal);
        if (!object(current) || current.id !== submission.task_id) throw new Error('fabric task identity mismatch');
        if (['completed', 'failed', 'cancelled', 'blocked'].includes(current.state)) {
          const finalReceipt = current.result?.receipt;
          const recorded = Array.isArray(current.receipts) && current.receipts.some(item =>
            item.id === finalReceipt?.receipt_id && item.sha256 === finalReceipt?.sha256);
          const goalVerification = capability === 'agent.verifier' && current.state === 'completed' && recorded &&
            typeof verifyGoal === 'function' ? await verifyGoal({ mission, task, packet, fabric_task: current }, { signal }) : null;
          return { state: current.state === 'completed' && recorded ? 'passed' : current.state === 'blocked' ? 'blocked' : 'failed',
            fabric_state: current.state, task_id: current.id, receipt: recorded ? finalReceipt : null,
            result: current.result, goal_verification: goalVerification, submission };
        }
        await sleep(Math.min(fabricPollMs, Math.max(1, deadline - performance.now())));
      }
      throw new Error(`fabric task ${submission.task_id} remains active after bounded wait`);
    }
    return blocked(`capability ${capability} has no promoted execution owner`);
  }
  function runtime(missionId) {
    const value = store.getMissionRuntime(token(missionId, 'mission id'));
    if (!object(value) || !object(value.mission) || !object(value.route) || !object(value.context)) throw new Error('mission runtime incomplete');
    const plan = compileTaskPlan(value.mission.tasks, { context: missionPlanContext(value.mission) });
    if (value.mission.execution_plan?.digest !== plan.digest || value.route.execution_plan_digest !== plan.digest)
      throw new Error('mission or route plan changed; reroute and reconcile before execution');
    if (value.context.mission_id !== missionId || !named(value.context, 'digest') || !named(value.context, 'path'))
      throw new Error('context packet identity incomplete');
    if (value.context.task_plan_digest && value.context.task_plan_digest !== plan.digest)
      throw new Error('context packet was compiled for another task plan');
    return { ...value, packet: value.context, plan,
      taskById: new Map(value.mission.tasks.map(task => [task.id, task])),
      nodeById: new Map(plan.nodes.map(node => [node.id, node])) };
  }
  async function taskExecution(context, taskId, signal) {
    stopped(signal);
    const { mission, route, packet, plan } = context;
    const task = context.taskById.get(taskId);
    if (!task) throw new Error('task not found');
    const node = context.nodeById.get(taskId);
    const dependencyRows = store.taskStates(mission.id, node.dependencies);
    const dependencies = Object.fromEntries(node.dependencies.map(id => [id, dependencyRows[id] ?? 'missing']));
    if (Object.values(dependencies).some(value => value !== 'passed')) {
      const reason = { reason: 'dependency_not_passed', dependencies };
      store.blockTask({ mission_id: mission.id, task_id: taskId, reason: 'dependency_not_passed' });
      return { task_id: taskId, capability: task.capability, provider: null, state: 'blocked', result: reason };
    }
    const provider = selectExecutionProvider(route, task.capability), providerId = provider?.provider_id ?? null;
    if (!providerId) {
      const reason = { reason: 'no_eligible_provider', capability: task.capability };
      store.blockTask({ mission_id: mission.id, task_id: taskId, reason: 'no_eligible_provider' });
      return { task_id: taskId, capability: task.capability, provider: null, state: 'blocked', result: reason };
    }
    // A task's authored label cannot downgrade an external paid/agent effect.
    const effectClass = EXTERNAL_EFFECTS.has(task.capability) ? 'EXTERNAL_IRREVERSIBLE_EFFECT' :
      task.capability === 'deployment.remote_ssh' ? 'EXTERNAL_REVERSIBLE_EFFECT' :
      task.effect_class ?? mission.effect_class ?? 'CANDIDATE_WRITE';
    if ((EXTERNAL_EFFECTS.has(task.capability) || task.capability === 'deployment.remote_ssh') &&
      await authorizeEffect({ mission, task, provider, effect_class: effectClass, packet, signal }) !== true) {
      const reason = { reason: 'effect_not_authorized', effect_class: effectClass };
      store.blockTask({ mission_id: mission.id, task_id: taskId, reason: 'effect_not_authorized' });
      return { task_id: taskId, capability: task.capability, provider: providerId, state: 'blocked', result: reason };
    }
    stopped(signal);
    const routeDigest = digestValue(route);
    const inputDigest = digestValue({ schema: 'bonfyre.execution-task-input.v1', task_input_digest: node.input_digest,
      context_packet_digest: packet.digest, route_digest: routeDigest, provider_id: providerId, effect_class: effectClass });
    const attempt = store.beginTask({ mission_id: mission.id, task_id: taskId, input_digest: inputDigest,
      provider_id: providerId, capability: task.capability, plan_digest: plan.digest, route_digest: routeDigest,
      context_packet_id: packet.id, mission_revision: context.mission_revision });
    if (attempt.reusable === true) {
      // Even a verified receipt is not a blanket replay grant. These local
      // observations are reconstructed; owner effects remain reconciled by hand.
      if (!['spec.compile', 'scheduler.deterministic_dag', 'proof.lineage', 'proof.replay', 'proof.audit_chain'].includes(task.capability))
        throw new Error('prior owner effect requires explicit reconciliation before reuse');
      stopped(signal);
      const fresh = await perform(contextWithTask(context, task), signal);
      if (validateResult(task.capability, fresh) !== 'passed') throw new Error('prior local proof no longer verifies; reconcile before reuse');
      return { task_id: taskId, capability: task.capability, provider: providerId, state: 'passed',
        idempotent: true, fresh_observation: true, result: fresh, prior_receipt_id: attempt.receipt_id };
    }
    let invoked = false;
    try {
      stopped(signal);
      // The attempt was durably recorded. Conservatively treat any thrown
      // owner/local composite as uncertain until a complete result is stored.
      invoked = true;
      const result = await perform(contextWithTask(context, task), signal);
      const state = validateResult(task.capability, result);
      const evidence = [{ result_digest: digestValue(result), plan_digest: plan.digest, context_packet_digest: packet.digest }];
      const completion = store.completeTask({ mission_id: mission.id, task_id: taskId, attempt_id: attempt.attempt_id,
        input_digest: inputDigest, state, result, evidence });
      return { task_id: taskId, capability: task.capability, provider: providerId, state, result, receipt: completion };
    } catch (error) {
      const result = { state: invoked ? 'uncertain' : 'failed', reason: error.message };
      try { store.completeTask({ mission_id: mission.id, task_id: taskId, attempt_id: attempt.attempt_id,
        input_digest: inputDigest, state: result.state, result, evidence: [{ plan_digest: plan.digest }] }); }
      catch { /* Started intent remains durable and blocks replay. */ }
      return { task_id: taskId, capability: task.capability, provider: providerId, state: result.state, result };
    }
  }
  const contextWithTask = (context, task) => ({ ...context, task });
  async function executeSingleTask(missionId, taskId, { signal } = {}) { return taskExecution(runtime(missionId), token(taskId, 'task id'), signal); }
  async function execute(missionId, { signal } = {}) {
    missionId = token(missionId, 'mission id');
    if (active.has(missionId)) throw new Error('mission already executing in this process');
    active.add(missionId);
    try {
      stopped(signal);
      const preflight = store.getMissionRuntime(missionId);
      if (preflight.state !== 'compiled' || preflight.tasks.some(task => task.state !== 'planned'))
        throw new Error('mission has prior execution state; reconcile before another execution');
      const mission = missions.get(missionId), plan = compileTaskPlan(mission.tasks, { context: missionPlanContext(mission) });
      if (mission.execution_plan?.digest !== plan.digest) throw new Error('mission plan changed before execution');
      const route = missions.route(missionId, { include_shadow: false });
      if (route.execution_plan_digest !== plan.digest) throw new Error('route plan identity mismatch');
      stopped(signal);
      const packet = await compileContext({ mission_id: missionId, goal: mission.goal, mission,
        budget_tokens: Math.min(mission.context_budget?.tokens ?? 24_000, 48_000), target: 'bonfyre-hypervisor' });
      if (!object(packet) || packet.mission_id !== missionId || !named(packet, 'id') || !named(packet, 'path') || !named(packet, 'digest') ||
        !Number.isSafeInteger(packet.token_estimate) || packet.token_estimate < 0 || !Array.isArray(packet.sections))
        throw new Error('context compiler did not produce a complete packet');
      stopped(signal);
      store.beginMission({ mission_id: missionId, context_packet_id: packet.id, plan_digest: plan.digest, route_digest: digestValue(route) });
      const context = runtime(missionId), results = [];
      let interrupted = false;
      for (const wave of plan.waves) {
        let cursor = 0; const internalAbort = new AbortController();
        const combined = signal ? AbortSignal.any([signal, internalAbort.signal]) : internalAbort.signal;
        const workers = Array.from({ length: Math.min(maxParallel, wave.length) }, async () => {
          while (!internalAbort.signal.aborted && cursor < wave.length) {
            const taskId = wave[cursor++];
            try {
              const result = await taskExecution(context, taskId, combined);
              results.push(result);
              if (['failed', 'uncertain', 'cancelled'].includes(result.state)) internalAbort.abort();
            } catch (error) { internalAbort.abort(); throw error; }
          }
        });
        const settled = await Promise.allSettled(workers);
        if (settled.some(item => item.status === 'rejected') || internalAbort.signal.aborted) { interrupted = true; break; }
      }
      const status = store.missionStatus(missionId);
      const states = Object.fromEntries(status.tasks.map(row => [row.id, row.state]));
      const chain = store.verifyReceiptChain();
      const allPassed = !interrupted && Object.keys(states).length === plan.nodes.length &&
        Object.values(states).every(value => value === 'passed') && chain.verified === true;
      const goalVerified = results.some(value => value.capability === 'agent.verifier' && value.state === 'passed' &&
        value.result?.goal_verification?.verified === true);
      const state = allPassed ? goalVerified ? 'resolved' : 'passed' :
        Object.values(states).some(value => value === 'uncertain' || value === 'started' || value === 'running') ? 'blocked' :
        Object.values(states).some(value => value === 'failed') ? 'failed' : 'blocked';
      const final = { mission_id: missionId, state, task_states: states, results, route, context_packet: context.packet,
        receipt_chain: chain, plan_digest: plan.digest, resolution_scope: goalVerified ? 'independent-goal-verification' : 'task-operations-only' };
      store.finishMission({ mission_id: missionId, state, mission_revision: context.mission_revision,
        evidence: [{ plan_digest: plan.digest, task_states: states }] });
      return final;
    } finally { active.delete(missionId); }
  }
  return Object.freeze({ execute, executeSingleTask, status: missionId => store.missionStatus(token(missionId, 'mission id')) });
}
