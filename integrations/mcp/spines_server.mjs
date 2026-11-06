#!/usr/bin/env bun
import { existsSync, readFileSync, realpathSync } from 'node:fs';
import { homedir } from 'node:os';
import { join, resolve } from 'node:path';
import { createMcpTransport } from './http_boundary.mjs';
import { VERSION, createSpinesStore } from './spines_store.mjs';

export const SPINES_TOOLS = JSON.parse(readFileSync(new URL('./spines_catalog.json', import.meta.url), 'utf8')).tools;
const MAX_BYTES = 30_000_000;
// Legacy annotations describe intent, not persistence: these reads write proofs.
const PERSISTENT_READS = ['spines_verify', 'r5_affected_closure', 'r5_parity_verify'];
const instructions = 'R5-R7 cumulative v5 service. R5 owns immutable temporal segments, logical deltas, affected closures and parity. R6 owns bounded equivalence, pure reduction, candidate proof and authority preflight. R7 owns phenotype, kernel parity and candidate placement/fuse manifests. No authoritative or external effect is automatically executed.';
export function spinesConfig(env = process.env, userHome = homedir()) {
  const token = readFileSync(env.BONFYRE_MCP_TOKEN_FILE || join(userHome, '.bonfyre/mcp/token'), 'utf8').trim();
  const port = Number(env.BONFYRE_SPINES_PORT || 3057), host = env.BONFYRE_SPINES_HOST || '127.0.0.1';
  if (!token || !Number.isSafeInteger(port) || port < 1 || port > 65535 || !['127.0.0.1', '::1'].includes(host)) throw new Error('invalid spines listener configuration');
  return { token, port, host, env, home: resolve(env.BONFYRE_SPINES_HOME || join(userHome, '.bonfyre/agent-hypervisor/native-spines-r7')),
    projectRoot: realpathSync(env.BONFYRE_ROOT || join(userHome, 'Documents/Bonfyre')),
    deltaKernel: realpathSync(env.BONFYRE_DELTA_KERNEL || join(userHome, '.local/bin/bonfyre-delta-kernel')),
    helixVm: realpathSync(env.BONFYRE_HELIX_VM || join(userHome, '.local/bin/bonfyre-helix-vm')),
    kernelProbe: realpathSync(env.BONFYRE_KERNEL_PROBE || join(userHome, '.local/bin/bonfyre-kernel-probe')),
    recursiveMcp: env.BONFYRE_RECURSIVE_MCP || 'http://127.0.0.1:3055/mcp' };
}
export async function createSpinesServer({ token, store, temporal, reasoning, model, maxConcurrent = 16,
  stopPaths, ...options } = {}) {
  store ||= createSpinesStore({ token, ...options });
  temporal ||= (await import('./spines_temporal.mjs')).createTemporalEngine(store);
  reasoning ||= (await import('./spines_reasoning.mjs')).createReasoningEngine(store);
  model ||= (await import('./spines_model.mjs')).createModelEngine(store);
  stopPaths ||= [join(store.home, 'STOP'), join(homedir(), '.bonfyre/operator/STOP')];
  const stopped = () => stopPaths.some(existsSync);
  const check = () => { if (stopped()) throw new Error('STOP active; spines mutation refused'); };
  let healthObservation;
  const health = () => {
    const at = Date.now();
    if (!healthObservation || at - healthObservation.checked_at_ms >= 1000)
      healthObservation = { receipts_verified: store.verifyReceipts().verified, checked_at_ms: at };
    return { ...healthObservation, integrity_observation_max_age_ms: 1000, paused: stopped() };
  };
  function verify() {
    const parity = temporal.parity(), flux = temporal.streamVerify(), receipts = store.verifyReceipts();
    return { verified: parity.equal && flux.verified && receipts.verified, parity, flux, receipts,
      evidence_scope: 'Temporal replay and flux/receipt integrity only; failed recorded operations remain failed.' };
  }
  async function missionAttempt(kind, args, operation) {
    const mission = args.mission, missionId = mission.id || (kind === 'r5_bootstrap' ? store.id() : 'unknown');
    if (typeof missionId !== 'string' || !missionId || missionId.length > 16384) throw new Error('invalid mission identity');
    check();
    const intent = store.writeReceipt({ release: 'R5-R7', kind: `mission.${kind}.attempt`, subject_kind: 'mission', subject_id: missionId,
      state: 'started', payload: { arguments_digest: store.digest(args), external_effects: false } });
    try {
      const result = await operation(mission, missionId);
      check();
      const receipt = store.writeReceipt({ release: 'R5-R7', kind: `mission.${kind}.complete`, subject_kind: 'mission', subject_id: missionId,
        state: result.state, payload: { attempt_receipt_id: intent.receipt_id, result_digest: store.digest(result), external_effects: false } });
      return { ...result, attempt_receipt: intent, receipt };
    } catch (error) {
      try { store.writeReceipt({ release: 'R5-R7', kind: `mission.${kind}.uncertain`, subject_kind: 'mission', subject_id: missionId,
        state: 'uncertain', payload: { attempt_receipt_id: intent.receipt_id, reconcile_before_retry: true } }); } catch { /* Started receipt remains evidence. */ }
      throw new Error(`reconcile mission attempt ${intent.receipt_id} before retry: ${error.message}`);
    }
  }
  const dispatch = {
    spines_status: () => store.status(), spines_verify: verify,
    r5_flux_append: args => temporal.append(args), r5_dependency_register: args => temporal.registerDependency(args),
    r5_affected_closure: args => temporal.closure(args), r5_views_refresh: () => temporal.refreshViews(),
    r5_parity_verify: () => temporal.parity(), r5_graph_partition: args => temporal.graphPartition(args),
    r6_rules_list: () => ({ rules: reasoning.rules() }), r6_rule_add: args => reasoning.addRule(args),
    r6_saturate: args => reasoning.saturate(args), r6_helix_compile: args => reasoning.compileHelix(args),
    r6_helix_reduce: args => reasoning.reduce(args), r6_authority_preflight: args => reasoning.authority(args),
    r6_crux_seal: args => reasoning.crux(args),
    r7_hardware_census: args => model.hardwareCensus(args.node_id ?? 'local'), r7_model_discover: () => model.discoverModels(),
    r7_model_inventory: () => ({ models: model.inventory() }), r7_phenotype_compile: args => model.compilePhenotype(args),
    r7_kernel_parity: args => model.kernelParity(args.backend ?? 'bonfyre-reference-cpu'), r7_node_register: args => model.registerNode(args),
    r7_nodes_list: () => ({ nodes: model.nodes() }), r7_placement_compile: args => model.place(args), r7_fuse_compile: args => model.fuse(args),
    mission_r5_bootstrap: args => missionAttempt('r5_bootstrap', args, async (mission, missionId) => {
      const tasks = mission.tasks || []; if (!Array.isArray(tasks) || tasks.length > 10000) throw new Error('invalid mission tasks');
      const dependencies = tasks.filter(task => task.id).map(task => ({ target_kind: 'mission_task', target_id: String(task.id), edge_kind: 'contains' }));
      const result = await temporal.append({ stream: 'mission', kind: 'mission.temporal.bootstrap', subject_kind: 'mission', subject_id: missionId,
        operation: 'UPSERT', payload: { state: 'compiled', goal: mission.goal ?? null, task_count: tasks.length, ready: true }, dependencies });
      check(); const parity = temporal.parity();
      return { state: parity.equal && result.receipt?.state === 'passed' ? 'passed' : 'failed', mission_id: missionId, temporal: result, parity };
    }),
    mission_r6_reason: args => missionAttempt('r6_reason', args, async (mission, missionId) => {
      const tasks = mission.tasks || [], tokens = Math.max(0, args.token_estimate ?? 0);
      if (!Array.isArray(tasks) || tasks.length > 10000 || !Number.isSafeInteger(tokens)) throw new Error('invalid mission resource bounds');
      const term = `add(0,add(${tokens},mul(${tasks.length},1)))`;
      const saturation = reasoning.saturate({ term }), helix = reasoning.compileHelix({ term: saturation.selected_term, source_run_id: saturation.run_id, fuel: 10000 });
      const reduction = await reasoning.reduce({ helix_id: helix.helix_id }); check();
      const authority = reasoning.authority({ actor: 'bonfyre-hypervisor', effect_class: 'PURE', resource: `mission:${missionId}:resource-plan`, proof_refs: [reduction.reduction_id] });
      const crux = reasoning.crux({ claim: 'Bounded mission resource term reduced under pure authority.', inputs: [{ mission_id: missionId, term }],
        witnesses: [{ reduction_id: reduction.reduction_id, state: reduction.state }], authority_decision_id: authority.decision_id, classification: 'computational-witness' });
      return { state: reduction.state === 'passed' ? 'passed' : 'failed', mission_id: missionId, saturation, helix, reduction, authority, crux };
    }),
    mission_r6_authority: args => missionAttempt('r6_authority', args, (mission, missionId) => {
      const task = args.task || {}, effect = args.effect_class || task.effect_class || mission.effect_class || 'CANDIDATE_WRITE';
      const authority = reasoning.authority({ actor: 'bonfyre-hypervisor', effect_class: effect, resource: `mission:${missionId}:task:${task.id || 'unknown'}`, proof_refs: [] });
      return { state: ['allowed_pure', 'allowed_read', 'candidate_only'].includes(authority.decision) ? 'passed' : 'blocked', mission_id: missionId, authority };
    }),
    mission_r7_plan: args => missionAttempt('r7_plan', args, async (mission, missionId) => {
      const census = await model.hardwareCensus('local'); check(); const inventory = await model.discoverModels(); check();
      const phenotype = model.compilePhenotype({ mission_id: missionId, model_family: args.model_family ?? 'BonfyreQwenFPQ', token_count: Math.max(0, args.token_estimate ?? 0),
        privacy_class: mission.privacy_class ?? 'internal', latency_target_ms: mission.latency_target_ms ?? null, context_window: mission.context_window ?? null });
      const kernelParity = await model.kernelParity(); check();
      const placement = model.place({ mission_id: missionId, phenotype_id: phenotype.phenotype_id, privacy_class: mission.privacy_class ?? 'internal',
        effect_class: mission.effect_class ?? 'CANDIDATE_WRITE', data_bytes: mission.data_bytes ?? 0, requires_gpu: mission.requires_gpu ?? false, remote_allowed: mission.remote_allowed ?? true });
      const fuse = model.fuse({ mission_id: missionId, phenotype_id: phenotype.phenotype_id, placement_id: placement.placement_id });
      const state = kernelParity.state !== 'passed' ? kernelParity.state : census.state === 'passed' && inventory.receipt?.state === 'passed' ? 'passed' : 'degraded';
      return { state, mission_id: missionId, census, inventory, phenotype, kernel_parity: kernelParity, placement, fuse };
    }),
    mission_spines_finalize: args => missionAttempt('finalize', args, (_mission, missionId) => {
      const proof = verify();
      const crux = reasoning.crux({ claim: 'Cumulative R5-R7 mission spine verification.', inputs: [{ mission_id: missionId }],
        witnesses: [{ kind: 'incremental_parity', verified: proof.parity.equal, proof_id: proof.parity.id },
          { kind: 'flux_chain', verified: proof.flux.verified, heads: proof.flux.streams },
          { kind: 'receipt_chain', verified: proof.receipts.verified, head: proof.receipts.head }], classification: 'candidate-release-proof' });
      return { state: proof.verified ? 'passed' : 'failed', mission_id: missionId, ...proof, crux };
    }),
  };
  for (const tool of SPINES_TOOLS) if (!tool.annotations.readOnlyHint || PERSISTENT_READS.includes(tool.name)) {
    const operation = dispatch[tool.name]; dispatch[tool.name] = async args => {
      check(); try { return await operation(args); } finally { healthObservation = null; }
    };
  }
  const transport = createMcpTransport({ token, label: 'spines', service: 'bonfyre-native-spines-r7', version: VERSION,
    instructions, tools: SPINES_TOOLS, dispatch, statusTool: 'spines_status', serialize: PERSISTENT_READS, maxBytes: MAX_BYTES, maxConcurrent,
    health });
  return { ...transport, close: () => store.close() };
}
if (import.meta.main) {
  const config = spinesConfig(), owner = await createSpinesServer(config);
  const server = Bun.serve({ hostname: config.host, port: config.port, fetch: owner.handle, maxRequestBodySize: MAX_BYTES, idleTimeout: 0 });
  console.log(`native spines MCP listening on ${server.hostname}:${server.port} runtime=bun`);
  for (const signal of ['SIGINT', 'SIGTERM']) process.once(signal, async () => { await server.stop(); owner.close(); process.exit(0); });
}
