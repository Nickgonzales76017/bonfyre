#!/usr/bin/env bun
import { existsSync, readFileSync, realpathSync } from 'node:fs';
import { homedir } from 'node:os';
import { join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createMcpTransport, equal, json, response } from './http_boundary.mjs';
import { createOwnedMcpCall } from './owned_mcp_client.mjs';
import { createHypervisorStore } from './hypervisor_store.mjs';
import { createHypervisorMissions } from './hypervisor_missions.mjs';
import { createHypervisorDiscovery } from './hypervisor_discovery.mjs';
import { createHypervisorAdapters } from './hypervisor_adapters.mjs';
import { createHypervisorContext } from './hypervisor_context.mjs';
import { createHypervisorEvaluation } from './hypervisor_evaluation.mjs';
import { createHypervisorExecution } from './hypervisor_execution.mjs';
import { HYPERVISOR_TOOLS } from './hypervisor_catalog.mjs';
export { HYPERVISOR_TOOLS } from './hypervisor_catalog.mjs';

export const HYPERVISOR_VERSION = '6.1.0';
const MAX_BYTES = 20_000_000;
const OWNER_BINDINGS = Object.freeze({
  fabric: { env: 'BONFYRE_DUAL_FABRIC', port: 3053, status: 'fabric_status', tools: ['fabric_status', 'duel', 'implementation_relay', 'cross_review', 'dual_verify', 'pr_converge', 'task_status', 'recent_receipts'] },
  recursive: { env: 'BONFYRE_RECURSIVE', port: 3055, status: 'recursive_status', tools: ['recursive_status', 'recursive_mission_bootstrap', 'affected_closure_compile', 'recursive_mission_finalize', 'substrate_replay_verify', 'provider_token_bid_compile', 'runtime_image_compile'] },
  canary: { env: 'BONFYRE_CANARY', port: 3056, status: 'canary_status', tools: ['canary_status'] },
  spines: { env: 'BONFYRE_SPINES', port: 3057, status: 'spines_status', tools: ['spines_status', 'mission_r5_bootstrap', 'mission_r6_reason', 'mission_r6_authority', 'mission_r7_plan', 'r7_kernel_parity', 'mission_spines_finalize'] },
  v6: { env: 'BONFYRE_V6', port: 3058, status: 'v6_status', tools: ['v6_status'] },
  bonfyre: { env: 'BONFYRE', port: 13052, status: 'mesh_status', tools: ['mesh_status', 'release_attest'] },
});
const PROMPTS = [
  { name: 'absorb-project', description: 'Inspect and quarantine an external GitHub agent project.', arguments: [{ name: 'repo', description: 'owner/name or GitHub URL', required: true }] },
  { name: 'compile-mission', description: 'Compile a broad goal before choosing agents or projects.', arguments: [{ name: 'goal', description: 'Institutional goal', required: true }] },
  { name: 'evaluate-provider', description: 'Evaluate a provider capability before promotion.', arguments: [{ name: 'provider', description: 'Provider ID', required: true }, { name: 'capability', description: 'Capability contract', required: true }] },
];
const unavailable = operation => { throw new Error(`${operation} unavailable: native owner is not bound; no Python fallback or success is inferred`); };

export function hypervisorConfig(env = process.env, userHome = homedir()) {
  const token = readFileSync(env.BONFYRE_MCP_TOKEN_FILE || join(userHome, '.bonfyre/mcp/token'), 'utf8').trim();
  const host = env.BONFYRE_HYPERVISOR_HOST || '127.0.0.1', port = Number(env.BONFYRE_HYPERVISOR_PORT || 3054);
  if (!token || !['127.0.0.1', '::1'].includes(host) || !Number.isSafeInteger(port) || port < 1 || port > 65535) throw new Error('invalid hypervisor listener configuration');
  return { token, host, port, env, userHome, home: resolve(env.BONFYRE_HYPERVISOR_HOME || join(userHome, '.bonfyre/agent-hypervisor')),
    root: realpathSync(env.BONFYRE_PROJECT_ROOT || env.BONFYRE_ROOT || fileURLToPath(new URL('../..', import.meta.url))),
    registryPath: env.BONFYRE_ECOSYSTEM_REGISTRY, dbPath: env.BONFYRE_HYPERVISOR_DB };
}

export function createHypervisorOwnerCalls({ token, env = process.env, request = fetch } = {}) {
  return Object.fromEntries(Object.entries(OWNER_BINDINGS).map(([name, binding]) => {
    const endpoint = env[`${binding.env}_MCP`] || (name === 'bonfyre' ? env.BONFYRE_MCP_URL : null) || `http://127.0.0.1:${binding.port}/mcp`;
    const parsed = new URL(endpoint), local = ['127.0.0.1', 'localhost', '[::1]'].includes(parsed.hostname);
    const file = env[`${binding.env}_TOKEN_FILE`];
    if (!local && !file) throw new Error(`remote ${name} owner requires its own explicit token file`);
    const ownerToken = file ? readFileSync(file, 'utf8').trim() : token;
    return [name, createOwnedMcpCall({ endpoint, token: ownerToken, tools: binding.tools, request, maxBytes: MAX_BYTES, maxConcurrent: 4 })];
  }));
}

export async function createHypervisorServer({ token, store, missions, discovery, adapters, context, evaluation, execution, sources,
  ownerCalls, stopPaths, authorizeEffect = () => false, maxConcurrent = 16, maxActiveMissions = 4,
  request = fetch, env = process.env, root = fileURLToPath(new URL('../..', import.meta.url)), userHome = homedir(), ...options } = {}) {
  if (!token) throw new Error('hypervisor requires an authentication token');
  if (!Number.isSafeInteger(maxActiveMissions) || maxActiveMissions < 1 || maxActiveMissions > 16) throw new Error('invalid mission concurrency bound');
  root = resolve(root); env = Object.freeze({ ...env });
  const home = resolve(options.home || store?.home || join(userHome, '.bonfyre/agent-hypervisor'));
  stopPaths ||= [join(home, 'STOP'), join(userHome, '.bonfyre/operator/STOP')];
  let closed = false;
  const stopped = () => closed || stopPaths.some(existsSync);
  const checkStop = () => { if (stopped()) throw new Error('STOP active or owner closing; hypervisor mutation refused'); };
  store ||= createHypervisorStore({ ...options, home, checkStop });
  missions ||= createHypervisorMissions({ store, checkStop });
  discovery ||= createHypervisorDiscovery({ store, root, env, checkStop });
  adapters ||= createHypervisorAdapters({ store, root, home, env, checkStop });
  ownerCalls ||= createHypervisorOwnerCalls({ token, env, request });
  context ||= createHypervisorContext({ store, root, adapters, checkStop,
    owners: { fabric: ({ signal }) => ownerCalls.fabric('fabric_status', {}, { signal, timeoutMs: 10000 }) } });
  evaluation ||= createHypervisorEvaluation({ store: { ...store, latestEvaluations: (...args) => store.observe('latestEvaluations', ...args) }, home, env, checkStop });
  execution ||= createHypervisorExecution({ store, missions, compileContext: context.compile, renderContext: context.renderForProvider,
    ownerCalls, authorizeEffect, checkStop });
  if (!sources) {
    // Import only the repository-owned module; no caller path or Python bridge.
    const sourceUrl = new URL('./hypervisor_sources.mjs', import.meta.url);
    if (existsSync(fileURLToPath(sourceUrl))) sources = (await import(sourceUrl.href)).createHypervisorSources({ store, home, env, checkStop });
  }
  const active = new Map(), pending = new Set(), controllers = new Set();
  function ownedWork(operation, write = true) {
    const check = () => { if (write) checkStop(); else if (closed) throw new Error('hypervisor owner closed'); };
    check(); const controller = new AbortController(); controllers.add(controller);
    const timer = setInterval(() => { if (closed || write && stopped()) controller.abort(new Error('owner stopped')); }, 50); timer.unref();
    const promise = Promise.resolve().then(() => { check(); return operation(controller.signal); });
    pending.add(promise);
    promise.finally(() => { clearInterval(timer); controllers.delete(controller); pending.delete(promise); }).catch(() => {});
    return promise;
  }
  async function status(_args, signal) {
    const results = await Promise.all(Object.entries(OWNER_BINDINGS).filter(([name]) => name !== 'bonfyre').map(async ([name, binding]) => {
      try { const value = await ownerCalls[name](binding.status, {}, { timeoutMs: 3000, signal }); return [name, { reachable: true, value }]; }
      catch { return [name, { reachable: false }]; }
    }));
    signal?.throwIfAborted();
    const observations = Object.fromEntries(results), v6 = observations.v6?.value || {};
    return { service: 'bonfyre-agent-hypervisor', home, project_root: root, summary: store.observedSummary(), active_missions: [...active.keys()], receipt_chain: store.observedReceiptChain(),
      dual_fabric_reachable: observations.fabric.reachable, recursive_substrate_reachable: observations.recursive.reachable,
      fixture_canary_r4_reachable: observations.canary.reachable, native_spines_r7_reachable: observations.spines.reachable,
      recursive_hypervisor_v6_reachable: observations.v6.reachable, recursive_hypervisor_v6_version: v6.version ?? null,
      v6_active_missions: v6.active_missions ?? null, v6_workgraph_nodes: v6.workgraph_nodes ?? null, v6_workgraph_edges: v6.workgraph_edges ?? null,
      v6_validated_providers: v6.validated_providers ?? null, v6_core_backends_active: v6.core_backends_active ?? null, v6_sparse_payload_bytes: v6.sparse_payload_bytes ?? null,
      system_version: HYPERVISOR_VERSION, paused: stopped(), evidence_scope: 'Current authenticated owner observations; not a distributed rollout or semantic capability proof.' };
  }
  function queueMission(id) {
    checkStop(); if (active.has(id)) return { mission_id: id, state: 'running' };
    if (active.size >= maxActiveMissions) throw new Error('active mission admission limit');
    // Queue admission is not the authoritative claim, but must not acknowledge
    // known completed/running/uncertain work as newly accepted. The store's
    // revision CAS still arbitrates races after this read-only observation.
    const preflight = store.observe('getMissionRuntime', id);
    if (preflight.state !== 'compiled' || !Array.isArray(preflight.tasks) || preflight.tasks.some(task => task.state !== 'planned'))
      throw new Error('mission is not pristine compiled work; reconcile before execution');
    const promise = ownedWork(signal => execution.execute(id, { signal })); active.set(id, promise);
    promise.catch(() => {
      // Also expose pre-attempt failures: the queue acknowledged scheduling,
      // not success. Existing task receipts remain the authority for effects.
      try { checkStop(); store.event('hypervisor', 'mission.execution_incomplete', { mission_id: id, reconcile_before_retry: true }, id); } catch { /* STOP leaves existing durable state untouched. */ }
    }).finally(() => active.delete(id));
    return { mission_id: id, state: 'queued', status_tool: 'mission_status' };
  }
  const operations = {
    hypervisor_status: status,
    ecosystem_discover: (_args, signal) => discovery.discover({ signal }),
    ecosystem_providers: () => ({ providers: store.observedProviders() }),
    capability_route: args => ({ capability: args.capability, scope: args.scope ?? 'global', candidates: store.observedRoute(args.capability, { scope: args.scope ?? 'global', include_shadow: args.include_shadow ?? false }) }),
    provider_adapters_status: (_args, signal) => adapters.status({ signal }),
    memory_search: (args, signal) => adapters.memorySearch(args.query, { ...args, signal }),
    compact_command: (args, signal) => adapters.compactCommand(args.command, { ...args, signal }),
    bernstein_manifest_compile: args => adapters.bernsteinCompile(missions.get(args.mission_id), {
      task_command: `bun ${JSON.stringify(fileURLToPath(new URL('./hypervisor_cli.mjs', import.meta.url)))} task-execute {mission_id} {task_id}` }),
    bernstein_manifest_validate: (args, signal) => adapters.bernsteinValidate(args.manifest_path, { signal }),
    fleet_status: (_args, signal) => adapters.fleetStatus({ signal }),
    fleet_launch: (args, signal) => adapters.fleetLaunch({ ...args, signal }),
    fleet_session_action: (args, signal) => adapters.fleetAction(args.action, args.session, { ...args, signal }),
    caveman_code_run: (args, signal) => adapters.cavemanCode(args.prompt, { ...args, signal }),
    invoker_submit: (args, signal) => adapters.invokerSubmit(args.workflow_path, { signal }),
    task_execute: (args, signal) => execution.executeSingleTask(args.mission_id, args.task_id, { signal }),
    source_absorb: (args, signal) => sources?.ingestSource ? sources.ingestSource(args.repo, { ...args, signal }) : unavailable('source_absorb'),
    mission_compile: args => missions.compile(args.goal, args),
    mission_route: args => missions.route(args.mission_id, { include_shadow: args.include_shadow ?? true }),
    mission_execute: args => queueMission(args.mission_id),
    mission_status: args => ({ ...store.observe('missionStatus', args.mission_id), active: active.has(args.mission_id) }),
    mission_backpropagate: args => missions.backpropagate(args.mission_id, args),
    session_register: args => store.registerSession(args),
    session_list: args => ({ sessions: store.observe('listSessions', args) }),
    session_handoff: (args, signal) => context.handoff ? context.handoff({ ...args, provider_id: args.target_provider, signal }) : unavailable('session_handoff'),
    source_update_check: (args, signal) => sources?.sourceUpdateCheck ? sources.sourceUpdateCheck({ ...args, signal }) : unavailable('source_update_check'),
    context_compile: (args, signal) => { const mission = missions.get(args.mission_id); return context.compile({ ...args, mission, goal: mission.goal, signal }); },
    provider_evaluate: (args, signal) => evaluation.evaluate(args.provider_id, { ...args, signal }),
    provider_compare: args => evaluation.compare(args.capability, { providers: args.providers ?? null }),
    provider_promote: args => store.promote(args.provider_id, args.capability, { scope: args.scope ?? 'global', weight: args.weight ?? 1 }),
    provider_quarantine: args => store.quarantine(args.provider_id, args.reason),
    provider_license_approve: args => store.approveLicense(args.provider_id, args.note),
    install_plan: args => evaluation.installPlan(args.profile ?? 'recommended'),
    replay_verify: () => store.observedReceiptChain(),
  };
  const effects = new Set(HYPERVISOR_TOOLS.filter(item => !item.annotations.readOnlyHint).map(item => item.name));
  const dispatch = Object.fromEntries(Object.entries(operations).map(([name, operation]) => [name, args => {
    if (closed) throw new Error('hypervisor owner closed');
    if (name === 'mission_execute') return operation(args);
    return ownedWork(signal => operation(args, signal), effects.has(name));
  }]));
  const transport = createMcpTransport({ token, label: 'hypervisor', service: 'bonfyre-agent-hypervisor', version: HYPERVISOR_VERSION,
    tools: HYPERVISOR_TOOLS, dispatch, statusTool: 'hypervisor_status', maxConcurrent, maxBytes: MAX_BYTES, prompts: PROMPTS,
    instructions: 'Native owned compilation and execution. Candidate routes and context do not authorize effects. STOP blocks writes. Unknown or uncertain effects require reconciliation, never automatic replay. Bernstein is an explicit external Python scheduler boundary.',
    health: () => ({ paused: stopped() }),
    getPrompt: (name, args) => ({ description: name, messages: [{ role: 'user', content: { type: 'text', text: name === 'absorb-project' ? `Inspect and quarantine ${args.repo}; do not install or promote without explicit approval.` : name === 'compile-mission' ? `Compile this goal into a verified mission before execution: ${args.goal}` : `Evaluate ${args.provider} for ${args.capability}; evidence does not authorize promotion.` } }] }) });
  let closePromise;
  return { ...transport,
    handle: async req => {
      const url = new URL(req.url);
      if (req.method !== 'GET' || url.pathname !== '/events') return transport.handle(req);
      if (!equal(req.headers.get('authorization') || '', `Bearer ${token}`)) return response(401, { error: 'unauthorized' });
      try {
        if (closed) throw new Error('owner closed');
        for (const key of url.searchParams.keys()) if (!['after', 'limit'].includes(key) || url.searchParams.getAll(key).length !== 1) throw new Error('invalid event query');
        const after = Number(url.searchParams.get('after') ?? 0), limit = Number(url.searchParams.get('limit') ?? 500);
        if (!Number.isSafeInteger(after) || after < 0 || !Number.isSafeInteger(limit) || limit < 1 || limit > 2000) throw new Error('invalid event range');
        const observed = store.observe('events', { after, limit }), value = { ...observed, next_after: observed.events?.at(-1)?.seq ?? after };
        if (Buffer.byteLength(json(value)) > MAX_BYTES) throw new Error('events exceed output budget');
        return response(200, value);
      } catch (error) { return response(400, { error: String(error.message).replaceAll(token, '[redacted]').slice(0, 2000) }); }
    },
    close: () => closePromise ||= (async () => { closed = true; for (const controller of controllers) controller.abort(new Error('owner closing')); await Promise.allSettled(pending); store.close(); })(),
  };
}
if (import.meta.main) {
  const config = hypervisorConfig(), owner = await createHypervisorServer(config);
  const server = Bun.serve({ hostname: config.host, port: config.port, fetch: owner.handle, maxRequestBodySize: MAX_BYTES, idleTimeout: 0 });
  console.log(`Hypervisor MCP listening on ${server.hostname}:${server.port} runtime=bun`);
  for (const signal of ['SIGINT', 'SIGTERM']) process.once(signal, async () => { const closing = owner.close(); await server.stop(); await closing; process.exit(0); });
}
