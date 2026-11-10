#!/usr/bin/env bun
// Project MCP transport. Each operation has exactly one semantic owner;
// configured Spines placement may be local or authenticated remote HTTPS.
import { json, response, equal, object, readRequest, validateArguments } from './http_boundary.mjs';
export { validateArguments } from './http_boundary.mjs';
import { existsSync, readFileSync, realpathSync } from 'node:fs';
import { homedir } from 'node:os';
import { join } from 'node:path';
import { projectRun } from './project_runtime.mjs';
import { SPINES_PROJECT_TOOLS, createProjectSpines } from './project_spines.mjs';
import { V6_PROJECT_TOOLS, createProjectV6 } from './project_v6.mjs';
import { FABRIC_PROJECT_TOOLS, createProjectFabric } from './project_fabric.mjs';
import { PRODUCTS_PROJECT_TOOLS, createProjectProducts } from './project_products.mjs';
import { CAPABILITY_PACK_NAMES, bindCapabilityPackSchemas, capabilityPackAllows, capabilityPack as getCapabilityPack, rankCapabilityPacks, validateCapabilityPacks } from './capability_packs.mjs';
import { CONTROL_PLANE_APP_URI, CONTROL_PLANE_APP_RESOURCE, readControlPlaneApp } from './control_plane_app_resource.mjs';
import { PROJECT_MCP_SERVICE_PROFILE_NAMES, compileProjectMcpLaunchAgent, inspectProjectMcpDeployment, projectMcpSourceIdentity } from './project_service_profiles.mjs';

const projectCatalog = JSON.parse(readFileSync(new URL('./project_catalog.json', import.meta.url), 'utf8'));
const projectTools = bindCapabilityPackSchemas(projectCatalog.tools);
export const CATALOG = {
  ...projectCatalog,
  tools: [...projectTools, ...SPINES_PROJECT_TOOLS, ...V6_PROJECT_TOOLS, ...FABRIC_PROJECT_TOOLS, ...PRODUCTS_PROJECT_TOOLS],
  resources: [...projectCatalog.resources, CONTROL_PLANE_APP_RESOURCE],
};
const tools = new Map(CATALOG.tools.map(tool => [tool.name, tool]));
if (tools.size !== CATALOG.tools.length) throw new Error('duplicate Project operation identity');
const capabilityPackValidation = validateCapabilityPacks(CATALOG.tools);
if (!capabilityPackValidation.ok) throw new Error(
  `invalid capability packs: ${capabilityPackValidation.issues.map((item) => `${item.pack}:${item.role}:${item.name}:${item.issue}`).join(', ')}`);
const publicTools = CATALOG.tools.map(tool => {
  const publicTool = Object.fromEntries(Object.entries(tool).filter(([key]) => key !== 'x-bonfyre-owner'));
  if (['mcp_capability_lens', 'mcp_product_plan', 'mcp_portfolio_plan'].includes(tool.name)) publicTool._meta = { ...(publicTool._meta || {}), ui: { resourceUri: CONTROL_PLANE_APP_URI } };
  return publicTool;
});
export function projectConfig(env = process.env, home = homedir()) {
  const tokenPath = env.BONFYRE_MCP_TOKEN_FILE || join(home, '.bonfyre/mcp/token');
  const token = existsSync(tokenPath) ? readFileSync(tokenPath, 'utf8').trim() : env.BONFYRE_MCP_TOKEN || '';
  const port = Number(env.BONFYRE_PROJECT_MCP_PORT || 3051);
  if (!token) throw new Error('Bonfyre MCP token missing');
  if (!Number.isSafeInteger(port) || port < 1 || port > 65535) throw new Error('invalid MCP port');
  return { root: realpathSync(env.BONFYRE_ROOT || join(home, 'Developer/Bonfyre')), home, env, token,
    host: env.BONFYRE_PROJECT_MCP_HOST || '127.0.0.1', port };
}

const deliveryWorkflow = 'For code changes, query the relevant Atlas owner and native_tool_inventory with query and detail=summary. Once paths are known, call project_patch_preview with paths to learn write constraints before implementing. Batch related source and test ranges with project_read files; retain sha256 and use if_sha256 only for content already read. For a delegated implementation, check fabric_status admission, submit implementation_relay once with the outcome and selected source_files hashes, then poll task_status with detail=summary. Unselected source stays at the configured Git base. Read full results when review or reconciliation needs them; task_cancel stops that task. A paused broker needs an explicit operator decision and must not be automatically resumed. For direct edits, prepare against the read preimages, preview the patch, apply through the authorized owner, and run relevant tests. Reconcile returned receipts and actual source state before retrying any effect. Report what landed and what remains blocked.';
const contract = '# Aurekai MCP contract\n\nUse architecture_atlas before architecture changes and native_tool_inventory before inventing an implementation. ForeignTwin receipts are observations, not remote truth or action rights. Financial caps and costs are not cash, settlement or spending authority. Quotes and presented payments are not verification or delivery.\n\nRead tools may run in parallel. State the intended transition before effects, preserve receipt identifiers, and verify owner state afterward. A valid MCP session does not grant institutional, payment, publication, foreign or delegation authority.\n\n' + deliveryWorkflow + '\n';

function prompt(name, args) {
  if (!object(args)) throw new Error('prompt arguments must be an object');
  const definition = CATALOG.prompts.find(item => item.name === name);
  if (!definition) throw new Error('unknown prompt');
  for (const arg of definition.arguments) if (arg.required && (typeof args[arg.name] !== 'string' || !args[arg.name].trim()))
    throw new Error(`${arg.name} is required`);
  const text = key => String(args[key] || '').trim();
  let body;
  switch (name) {
    case 'institutional_circuit':
      if (!/^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,127}$/.test(text('subject'))) throw new Error('invalid subject');
      body = `Prepare an evidence- and authority-bound institutional circulation pass for \`${text('subject')}\`.\n\nObjective: ${text('objective')}\n\nFirst call architecture_atlas for the relevant owners; do not mint a new architecture. Inspect bonfyre://native/inventory and bonfyre://models/status before choosing realization. Use financial_membrane for cost/cap state, never as a payment claim. If preparation is useful, use institutional_prepare. Separate observed fact, proposal, authority blocker, capability gap, commitment, actual occurrence and economic consequence. Do not contact, publish, pay, settle or mutate a foreign system.`;
      break;
    case 'evidence_bound_intake':
      body = `Investigate this exact need: ${text('need')}\n\nPublic source: ${text('url')}\n\nUse foreign_twin_observe with a stable safe twin identifier. Treat search/index hints only as candidates. After observation, report retained receipt facts, uncertainty, missing proof, and the next lawful transition. Do not claim the source is current truth, that an account exists, or that Aurekai may act against it.`;
      break;
    case 'native_delivery_sprint':
      body = `Deliver this observable user outcome: ${text('outcome')}\n${text('surface') ? `Primary carrier: ${text('surface')}\n` : ''}\n${deliveryWorkflow}\nReuse existing owners and tests. Use project_test_profile and relevant native test targets after a change. Preserve authority, evidence and financial boundaries.`;
      break;
    case 'addressed_participation':
      body = `Resolve the exact participation cut at \`${text('host')}${text('path')}\`.\n\nPurpose: ${text('purpose')}\n\nFirst call matrix_address_resolve with the host and path. Treat a resolved address as only an address cut: it does not disclose private state or grant an action. If a subject resolves, call matrix_subject_constellation only when more lawful coordinates are useful. Then inspect architecture_atlas for Matrix, SurfaceIR, Authority, Evidence, Provider and financial owners that the purpose actually requires. Name unresolved closures explicitly; do not bootstrap demo state, infer a viewer closure, or turn the plan into an external effect.`;
      break;
    case 'product_pack_workflow': {
      const selected = text('pack');
      if (!CAPABILITY_PACK_NAMES.includes(selected)) throw new Error('invalid capability pack');
      body = `Operate the Aurekai ${selected} capability pack for this objective: ${text('objective')}\n\nStart with mcp_product_plan using pack=${selected} and this objective as intent. It composes runtime/source identity, snapshot readiness, a compact routing shortlist, handoffs and named next effects in one read-only call while preserving the original owner boundaries. If runtime is drifted, unattested, or unavailable, report that before relying on deployment claims. If snapshot readiness is context_required, follow only the named required_context/handoff carriers; do not invent account, destination, credential, or provider context. Request a full schema only for the selected tool. Before any effect, state the intended transition and preserve its receipt identity. Never turn catalogue presence, health, a budget, a quote, a credential, or a saved draft into authority it does not grant.`;
      break;
    }
    default: throw new Error('unbound prompt');
  }
  return { description: 'Aurekai native MCP workflow', messages: [{ role: 'user', content: { type: 'text', text: body } }] };
}

export async function createProjectNode({ root, home = homedir(), env = process.env, token,
  run = projectRun, request = fetch, owners = {}, maxConcurrent = 16 } = {}) {
  if (!root || !token) throw new Error('project MCP requires root and token');
  if (!Number.isSafeInteger(maxConcurrent) || maxConcurrent < 1 || maxConcurrent > 64) throw new Error('invalid concurrency bound');
  const capabilityPack = String(env.BONFYRE_PROJECT_MCP_PACK || '').trim();
  let launchedSourceSha256 = /^[a-f0-9]{64}$/.test(String(env.BONFYRE_PROJECT_MCP_SOURCE_SHA256 || ''))
    ? String(env.BONFYRE_PROJECT_MCP_SOURCE_SHA256) : null;
  if (!launchedSourceSha256) {
    try { launchedSourceSha256 = projectMcpSourceIdentity(root).aggregate_sha256; }
    catch { /* runtimeIdentity reports source_unavailable if the declared source set cannot be read */ }
  }
  function runtimeIdentity() {
    let current = null;
    let sourceError = null;
    try { current = projectMcpSourceIdentity(root); }
    catch (error) { sourceError = String(error?.message || 'source identity unavailable').slice(0, 500); }
    const currentSha256 = current?.aggregate_sha256 || null;
    const state = !currentSha256 ? 'source_unavailable'
      : !launchedSourceSha256 ? 'unattested'
      : launchedSourceSha256 === currentSha256 ? 'current'
      : 'drifted';
    let deploymentPreview = null;
    if (current && PROJECT_MCP_SERVICE_PROFILE_NAMES.includes(capabilityPack)) {
      try {
        deploymentPreview = inspectProjectMcpDeployment(capabilityPack, { root, home, sourceIdentity: current });
      } catch (error) {
        deploymentPreview = { state: 'unavailable', error: String(error?.message || 'deployment preview unavailable').slice(0, 500) };
      }
    }
    return {
      schema_version: 'bonfyre.project-mcp.runtime-identity.v1',
      state,
      capability_pack: capabilityPack || null,
      launched_source_sha256: launchedSourceSha256,
      current_source_sha256: currentSha256,
      advertised_tools: advertisedTools?.length ?? null,
      source_identity: current,
      source_error: sourceError,
      deployment_preview: deploymentPreview,
      forbidden_inference: 'Matching declared source-set digests attest only this bounded Project MCP release source set; they do not prove transitive dependency identity, deployment authority, or external service health.',
    };
  }
  async function boundedJsonResponse(response, maximum = 16 * 1024) {
    const declared = Number(response?.headers?.get?.('content-length'));
    if (Number.isFinite(declared) && declared > maximum) throw new Error('listener health response exceeds byte limit');
    let body = '';
    if (response?.body?.getReader) {
      const reader = response.body.getReader();
      const chunks = [];
      let size = 0;
      try {
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          size += value.byteLength;
          if (size > maximum) throw new Error('listener health response exceeds byte limit');
          chunks.push(Buffer.from(value));
        }
      } catch (error) {
        await reader.cancel().catch(() => {});
        throw error;
      } finally {
        reader.releaseLock();
      }
      body = Buffer.concat(chunks, size).toString('utf8');
    } else if (typeof response?.text === 'function') {
      body = await response.text();
      if (Buffer.byteLength(body) > maximum) throw new Error('listener health response exceeds byte limit');
    } else {
      throw new Error('listener health response is not readable');
    }
    try { return JSON.parse(body); } catch { throw new Error('listener health response is malformed'); }
  }

  async function probeProductListener(profile, endpoint) {
    const target = new URL('/health', endpoint);
    if (target.protocol !== 'http:' || target.hostname !== '127.0.0.1' || target.username || target.password) {
      return { state: 'invalid_target', endpoint: target.toString() };
    }
    const controller = new AbortController();
    const started = Date.now();
    let timer = null;
    try {
      const timeout = new Promise((_, reject) => {
        timer = setTimeout(() => {
          controller.abort();
          reject(new Error('listener probe timeout'));
        }, 200);
      });
      const response = await Promise.race([
        Promise.resolve(request(target, { method: 'GET', redirect: 'error', signal: controller.signal })),
        timeout,
      ]);
      if (!response || typeof response.status !== 'number') throw new Error('listener probe returned no HTTP response');
      if (response.status !== 200) {
        return { state: 'http_error', http_status: response.status, duration_ms: Date.now() - started };
      }
      const health = await boundedJsonResponse(response);
      const identityMatches = health?.service === 'bonfyre-project-mcp'
        && health?.runtime === 'bun'
        && health?.capability_pack === profile;
      return {
        state: identityMatches ? 'healthy' : 'identity_mismatch',
        http_status: response.status,
        duration_ms: Date.now() - started,
        service: health?.service || null,
        runtime: health?.runtime || null,
        capability_pack: health?.capability_pack ?? null,
        advertised_tools: Number.isSafeInteger(health?.advertised_tools) ? health.advertised_tools : null,
        runtime_identity_state: health?.runtime_identity?.state || null,
        current_source_sha256: health?.runtime_identity?.current_source_sha256 || null,
      };
    } catch (error) {
      const reason = String(error?.message || 'listener unavailable').slice(0, 300);
      return {
        state: reason.includes('timeout') || error?.name === 'AbortError' ? 'timeout' : 'unavailable',
        duration_ms: Date.now() - started,
        reason,
      };
    } finally {
      if (timer) clearTimeout(timer);
    }
  }

  if (capabilityPack && !CAPABILITY_PACK_NAMES.includes(capabilityPack)) throw new Error('invalid Project MCP capability pack');
  const apertureTools = capabilityPack ? CATALOG.tools.filter((tool) => capabilityPackAllows(tool, capabilityPack)) : CATALOG.tools;
  const apertureNames = capabilityPack ? new Set(apertureTools.map((tool) => tool.name)) : null;
  const advertisedTools = capabilityPack ? publicTools.filter((tool) => apertureNames.has(tool.name)) : publicTools;
  const readCall = owners.reads || (await import('./project_reads.mjs')).createProjectReads({ root, env, run, catalogTools: apertureTools });
  const operatorOwner = owners.operator || (await import('./project_operator.mjs')).createProjectOperator({ root, home, env, run, status: () => readCall('project_status', {}) });
  const operatorCall = typeof operatorOwner === 'function' ? operatorOwner : operatorOwner.call.bind(operatorOwner);
  const nativeCall = owners.native || (await import('./project_native.mjs')).createProjectNative({ root, home, env, run, request });
  const spinesCall = owners.spines || createProjectSpines({ home, env, token, request });
  const v6Call = owners.v6 || createProjectV6({ home, env, token, request });
  const fabricCall = owners.fabric || createProjectFabric({ home, env, token, request });
  const productsCall = owners.products || createProjectProducts({ root, home, env });
  let dispatch;
  const inflightReads = new Map();
  async function runReadBundle(requests = [], requestedConcurrency = 4) {
    const concurrency = Math.max(1, Math.min(Number(requestedConcurrency) || 4, 8));
    const bundleStarted = Date.now();
    const results = new Array(requests.length);
    const requestIds = requests.map((item) => item?.id).filter((id) => id !== undefined);
    if (new Set(requestIds).size !== requestIds.length) throw new Error('mcp_read_bundle call ids must be unique');
    const localReads = new Map();
    let deduplicatedReads = 0;
    let crossBundleCoalescedReads = 0;
    let ownerInvocations = 0;
    let cursor = 0;
    async function worker() {
      while (cursor < requests.length) {
        const index = cursor++;
        const requestSpec = requests[index] || {};
        const started = Date.now();
        try {
          const child = tools.get(requestSpec.name);
          if (!child || ['mcp_read_bundle', 'mcp_pack_snapshot'].includes(child.name)
            || (apertureNames && !apertureNames.has(child.name)))
            throw new Error('unknown read tool in this capability aperture');
          if (child.annotations?.readOnlyHint !== true) throw new Error(`mcp_read_bundle admits read-only tools only: ${child.name}`);
          const childArgs = requestSpec.arguments ?? {};
          validateArguments(childArgs, child.inputSchema);
          const sharedKey = `${child.name}\n${json(childArgs)}`;
          let shared = localReads.has(sharedKey);
          let pending = localReads.get(sharedKey);
          if (!pending) {
            pending = inflightReads.get(sharedKey);
            if (pending) {
              shared = true;
              deduplicatedReads++;
              crossBundleCoalescedReads++;
            } else {
              ownerInvocations++;
              pending = Promise.resolve().then(() => dispatch[child['x-bonfyre-owner']](child.name, childArgs));
              inflightReads.set(sharedKey, pending);
              pending.finally(() => {
                if (inflightReads.get(sharedKey) === pending) inflightReads.delete(sharedKey);
              }).catch(() => {});
            }
            localReads.set(sharedKey, pending);
          } else deduplicatedReads++;
          const value = await pending;
          results[index] = { id: requestSpec.id ?? null, name: child.name, owner: child['x-bonfyre-owner'], ok: true, shared, duration_ms: Date.now() - started, result: value };
        } catch (error) {
          results[index] = {
            id: requestSpec.id ?? null,
            name: typeof requestSpec.name === 'string' ? requestSpec.name : null,
            ok: false,
            duration_ms: Date.now() - started,
            error: String(error?.message || 'bundled read failed').replaceAll(token, '[redacted]').slice(0, 2000),
          };
        }
      }
    }
    await Promise.all(Array.from({ length: Math.min(concurrency, Math.max(1, requests.length)) }, worker));
    const wallDurationMs = Date.now() - bundleStarted;
    const sumChildDurationMs = results.reduce((sum, item) => sum + Number(item?.duration_ms || 0), 0);
    const criticalPathMs = results.reduce((maximum, item) => Math.max(maximum, Number(item?.duration_ms || 0)), 0);
    const bundle = {
      schema_version: 'bonfyre.mcp.read-bundle.v1',
      owner: 'node',
      count: results.length,
      passed: results.filter((item) => item?.ok === true).length,
      failed: results.filter((item) => item?.ok !== true).length,
      results,
      performance: { max_concurrency: concurrency, wall_duration_ms: wallDurationMs, sum_child_duration_ms: sumChildDurationMs, critical_path_ms: criticalPathMs, estimated_serialization_avoided_ms: Math.max(0, sumChildDurationMs - wallDurationMs), owner_invocations: ownerInvocations, deduplicated_reads: deduplicatedReads, cross_bundle_coalesced_reads: crossBundleCoalescedReads },
      consistency: 'Each child read is independently validated and current at its own owner boundary; the bundle is concurrent, not an atomic cross-owner snapshot.',
      forbidden_inference: 'Bundling read tools does not grant effect authority, make separate owners atomic, or convert health/discovery state into permission.',
    };
    if (Buffer.byteLength(json(bundle)) > 4 * 1024 * 1024) throw new Error('bundled read output exceeds 4 MiB; request fewer or narrower reads');
    return bundle;
  }

  async function nodeCall(name, args = {}) {
    if (name === 'mcp_runtime_identity') return runtimeIdentity();
    if (name === 'mcp_read_bundle') return runReadBundle(args.calls ?? [], args.max_concurrency);
    if (name === 'mcp_pack_snapshot') {
      if (capabilityPack && args.pack !== capabilityPack) throw new Error('pack snapshot must match the active capability aperture');
      const plan = getCapabilityPack(args.pack);
      if (!plan) throw new Error('unknown capability pack');
      const bundle = await runReadBundle(plan.snapshot_reads || [], 4);
      const results = {};
      const failures = [];
      for (const item of bundle.results) {
        if (item.ok) results[item.name] = item.result;
        else failures.push({ name: item.name, error: item.error });
      }
      const identity = runtimeIdentity();
      const requiredHandoffs = (plan.handoffs || []).filter((item) => item.required);
      const missingLocalSnapshot = bundle.count === 0;
      const snapshotState = failures.length ? 'degraded'
        : (requiredHandoffs.length || missingLocalSnapshot) ? 'context_required'
        : 'available';
      const requiredContext = snapshotState === 'context_required'
        ? [...new Set([
          ...(missingLocalSnapshot ? (plan.entry_reads || []) : []),
          ...requiredHandoffs.map((item) => item.name),
        ])]
        : [];
      const handoffEffects = (plan.handoffs || []).filter((item) => item.kind === 'effect').map((item) => item.name);
      const readiness = {
        snapshot: snapshotState,
        runtime: identity.state,
        effect_authority: 'not_inferred',
        snapshot_reads: plan.snapshot_reads?.length || 0,
        required_context: requiredContext,
        next_effects: [...new Set([...(plan.primary_effects || []), ...handoffEffects])],
      };
      return {
        schema_version: 'bonfyre.mcp.pack-snapshot.v1',
        owner: 'node',
        pack: args.pack,
        observed_at: new Date().toISOString(),
        count: bundle.count,
        passed: bundle.passed,
        failed: bundle.failed,
        results,
        failures,
        child_results: bundle.results,
        performance: bundle.performance,
        runtime_identity: identity,
        readiness,
        pack_plan: plan,
        consistency: bundle.consistency,
        forbidden_inference: 'A pack snapshot is a convenience projection over independent read owners. It does not make them atomic, fill missing account context, or grant any primary effect listed by the pack.',
      };
    }
    if (name === 'mcp_portfolio_plan') {
      const started = Date.now();
      const intent = String(args.intent || '').trim();
      if (!intent) throw new Error('portfolio intent is required');
      const effect = args.effect || 'all';
      const limit = args.limit ?? 8;
      const maxPacks = Math.max(1, Math.min(Number(args.max_packs) || 3, 5));
      const explicit = Array.isArray(args.packs)
        ? [...new Set(args.packs)]
        : [];
      let selectedPacks = [];
      let routingSelection;

      if (capabilityPack) {
        if (explicit.some((pack) => pack !== capabilityPack)) {
          throw new Error('portfolio packs must remain inside the active capability aperture');
        }
        selectedPacks = [capabilityPack];
        routingSelection = {
          state: 'aperture_constrained',
          selected_packs: selectedPacks,
          candidates: [{ pack: capabilityPack, score: null, matched_terms: [] }],
        };
      } else if (explicit.length) {
        for (const pack of explicit) if (!CAPABILITY_PACK_NAMES.includes(pack)) throw new Error('unknown capability pack');
        selectedPacks = explicit.slice(0, maxPacks);
        routingSelection = {
          state: explicit.length > maxPacks ? 'explicit_bounded' : 'explicit',
          selected_packs: selectedPacks,
          candidates: selectedPacks.map((pack) => ({ pack, score: null, matched_terms: [] })),
        };
      } else {
        const ranked = rankCapabilityPacks(intent);
        const matched = ranked.filter((candidate) => candidate.score > 0);
        if (!matched.length) {
          const identity = runtimeIdentity();
          return {
            schema_version: 'bonfyre.mcp.portfolio-plan.v1',
            owner: 'node',
            intent,
            selected_packs: [],
            routing_selection: { state: 'packs_required', selected_packs: [], candidates: ranked.slice(0, 5) },
            runtime_identity: identity,
            readiness: { state: 'packs_required', runtime: identity.state, effect_authority: 'not_inferred' },
            plans: [],
            handoffs: [],
            next_effects: [],
            performance: {
              external_round_trips: 1,
              composed_pack_plans: 0,
              round_trips_avoided: 0,
              server_duration_ms: Date.now() - started,
              owner_invocations: 0,
              deduplicated_reads: 0,
              cross_bundle_coalesced_reads: 0,
            },
            consistency: 'No declared capability-pack vocabulary matched the objective, so no portfolio was invented.',
            forbidden_inference: 'A portfolio plan is read-only routing and readiness. It grants no account, provider, payment, publication, institutional, or foreign effect authority.',
          };
        }
        selectedPacks = matched.slice(0, maxPacks).map((candidate) => candidate.pack);
        routingSelection = {
          state: matched.length > maxPacks ? 'selected_bounded' : 'selected',
          selected_packs: selectedPacks,
          candidates: ranked.slice(0, Math.max(5, maxPacks)),
        };
      }

      const fullPlans = await Promise.all(selectedPacks.map((pack) =>
        nodeCall('mcp_product_plan', { pack, intent, effect, limit })));
      const plans = fullPlans.map((plan) => ({
        pack: plan.pack,
        routing_selection: plan.routing_selection,
        runtime_identity: plan.runtime_identity,
        readiness: plan.readiness,
        route: plan.route,
        snapshot: plan.snapshot ? {
          state: plan.readiness?.snapshot || null,
          count: plan.snapshot.count,
          passed: plan.snapshot.passed,
          failed: plan.snapshot.failed,
          observed_reads: Object.keys(plan.snapshot.results || {}),
          failures: plan.snapshot.failures || [],
          performance: plan.snapshot.performance || null,
        } : null,
        lens: {
          matched: plan.lens?.matched ?? null,
          returned: plan.lens?.returned ?? null,
          performance: plan.lens?.performance || null,
        },
      }));
      const handoffMap = new Map();
      const nextEffects = new Set();
      for (const plan of plans) {
        for (const handoff of plan.route?.handoffs || []) {
          const key = `${handoff.carrier || ''}\n${handoff.name || ''}\n${handoff.kind || ''}`;
          if (!handoffMap.has(key)) handoffMap.set(key, handoff);
        }
        for (const effectName of plan.route?.next_effects || []) nextEffects.add(effectName);
      }
      const snapshotPerformance = plans.map((plan) => plan.snapshot?.performance).filter(Boolean);
      const identity = runtimeIdentity();
      const selectedToolNames = new Set();
      for (const selectedPack of selectedPacks) {
        for (const tool of CATALOG.tools) if (capabilityPackAllows(tool, selectedPack)) selectedToolNames.add(tool.name);
      }
      const selectedPublicTools = publicTools.filter((tool) => selectedToolNames.has(tool.name));
      const fullCatalogPublicBytes = Buffer.byteLength(JSON.stringify(publicTools));
      const portfolioPublicBytes = Buffer.byteLength(JSON.stringify(selectedPublicTools));
      const schemaReductionPercent = fullCatalogPublicBytes === 0 ? 0
        : Math.max(0, Math.min(100, Math.round((1 - portfolioPublicBytes / fullCatalogPublicBytes) * 10_000) / 100));
      return {
        schema_version: 'bonfyre.mcp.portfolio-plan.v1',
        owner: 'node',
        intent,
        selected_packs: selectedPacks,
        routing_selection: routingSelection,
        runtime_identity: identity,
        readiness: {
          state: plans.some((plan) => plan.readiness?.snapshot === 'degraded') ? 'degraded'
            : plans.some((plan) => plan.readiness?.snapshot === 'context_required') ? 'context_required'
            : 'available',
          runtime: identity.state,
          runtime_current: identity.state === 'current',
          deployment: identity.deployment_preview?.state || null,
          effect_authority: 'not_inferred',
          required_context: [...new Set(plans.flatMap((plan) => plan.readiness?.required_context || []))],
        },
        plans,
        handoffs: [...handoffMap.values()],
        next_effects: [...nextEffects],
        performance: {
          external_round_trips: 1,
          composed_pack_plans: plans.length,
          round_trips_avoided: Math.max(0, plans.length - 1),
          server_duration_ms: Date.now() - started,
          owner_invocations: snapshotPerformance.reduce((sum, item) => sum + Number(item.owner_invocations || 0), 0),
          deduplicated_reads: snapshotPerformance.reduce((sum, item) => sum + Number(item.deduplicated_reads || 0), 0),
          cross_bundle_coalesced_reads: snapshotPerformance.reduce((sum, item) => sum + Number(item.cross_bundle_coalesced_reads || 0), 0),
          context_economics: {
            full_catalog_tools: publicTools.length,
            portfolio_tools: selectedPublicTools.length,
            tools_avoided: Math.max(0, publicTools.length - selectedPublicTools.length),
            full_catalog_public_bytes: fullCatalogPublicBytes,
            portfolio_public_bytes: portfolioPublicBytes,
            public_bytes_avoided: Math.max(0, fullCatalogPublicBytes - portfolioPublicBytes),
            schema_reduction_percent: schemaReductionPercent,
            measurement: 'UTF-8 bytes of the unique public MCP tool definitions required by the selected pack union versus the full public Project catalogue; transport framing and host metadata are excluded.',
          },
        },
        consistency: 'Each pack retains its own owner, runtime, readiness and handoff boundaries. Overlapping reads may share one in-flight owner promise but are never cached after completion and are not made atomic.',
        forbidden_inference: 'A portfolio plan is read-only routing and readiness. It grants no account, provider, payment, publication, institutional, or foreign effect authority.',
      };
    }

    if (name === 'mcp_product_plan') {
      const started = Date.now();
      const intent = String(args.intent || '').trim();
      const effect = args.effect || 'all';
      const limit = args.limit ?? 12;
      const requestedPack = args.pack || 'auto';
      let selectedPack = null;
      let routingSelection;

      if (capabilityPack) {
        const requested = requestedPack === 'auto'
          ? capabilityPack
          : requestedPack;
        if (requested !== capabilityPack) throw new Error('product plan must match the active capability aperture');
        selectedPack = capabilityPack;
        routingSelection = {
          state: 'aperture_constrained',
          requested_pack: requestedPack,
          selected_pack: selectedPack,
          candidates: [{ pack: selectedPack, score: null, matched_terms: [] }],
        };
      } else if (requestedPack === 'auto') {
        if (!intent) throw new Error('intent is required when product plan pack is auto');
        const ranked = rankCapabilityPacks(intent);
        if (!ranked[0] || ranked[0].score === 0) {
          const identity = runtimeIdentity();
          return {
            schema_version: 'bonfyre.mcp.product-plan.v1',
            owner: 'node',
            pack: null,
            requested_pack: 'auto',
            intent,
            routing_selection: {
              state: 'pack_required',
              requested_pack: 'auto',
              selected_pack: null,
              candidates: ranked.slice(0, 3),
            },
            runtime_identity: identity,
            readiness: {
              snapshot: 'pack_required',
              runtime: identity.state,
              effect_authority: 'not_inferred',
              required_context: [],
              next_effects: [],
            },
            snapshot: null,
            lens: null,
            route: { shortlist: [], handoffs: [], next_effects: [], candidates: ranked.slice(0, 3) },
            performance: {
              external_round_trips: 1,
              composed_client_operations: 0,
              round_trips_avoided: 0,
              server_duration_ms: Date.now() - started,
              snapshot: null,
              lens_payload_reduction_percent: null,
            },
            consistency: 'No declared routing vocabulary matched the intent, so no product pack snapshot or effect route was selected.',
            forbidden_inference: 'A product plan is a read-only routing projection. It does not grant any effect, provider, account, financial, publication, or foreign authority.',
          };
        }
        const leaders = ranked.filter(candidate => candidate.score === ranked[0].score);
        selectedPack = ranked[0].pack;
        routingSelection = {
          state: leaders.length > 1 ? 'ambiguous_selected' : 'selected',
          requested_pack: 'auto',
          selected_pack: selectedPack,
          candidates: ranked.slice(0, 3),
        };
      } else {
        selectedPack = requestedPack;
        if (!CAPABILITY_PACK_NAMES.includes(selectedPack)) throw new Error('unknown capability pack');
        routingSelection = {
          state: 'explicit',
          requested_pack: requestedPack,
          selected_pack: selectedPack,
          candidates: [{ pack: selectedPack, score: null, matched_terms: [] }],
        };
      }

      const [snapshot, lens] = await Promise.all([
        nodeCall('mcp_pack_snapshot', { pack: selectedPack }),
        Promise.resolve(readCall('mcp_capability_lens', { pack: selectedPack, query: intent, effect, detail: 'summary', limit })),
      ]);
      return {
        schema_version: 'bonfyre.mcp.product-plan.v1',
        owner: 'node',
        pack: selectedPack,
        requested_pack: requestedPack,
        intent,
        routing_selection: routingSelection,
        runtime_identity: snapshot.runtime_identity,
        readiness: snapshot.readiness,
        snapshot,
        lens,
        route: {
          shortlist: lens.tools || [],
          handoffs: snapshot.pack_plan?.handoffs || lens.pack_plan?.handoffs || [],
          next_effects: snapshot.readiness?.next_effects || [],
          candidates: routingSelection.candidates,
        },
        performance: {
          external_round_trips: 1,
          composed_client_operations: 2,
          round_trips_avoided: 1,
          server_duration_ms: Date.now() - started,
          snapshot: snapshot.performance || null,
          lens_payload_reduction_percent: lens.performance?.payload_reduction_percent ?? null,
        },
        consistency: 'Runtime/readiness comes from the pack snapshot and routing comes from the capability lens; both retain their original owner boundaries and are not made atomic by this composition.',
        forbidden_inference: 'A product plan is a read-only routing projection. It does not grant any effect, provider, account, financial, publication, or foreign authority.',
      };
    }
    throw new Error(`unknown Project node tool: ${name}`);
  }
  dispatch = { reads: readCall, operator: operatorCall, native: nativeCall, spines: spinesCall, v6: v6Call, fabric: fabricCall, products: productsCall, node: nodeCall };
  if (CATALOG.tools.some(tool => typeof dispatch[tool['x-bonfyre-owner']] !== 'function')) throw new Error('unbound project tool owner');
  let active = 0, effectTail = Promise.resolve();
  async function call(name, args) {
    const tool = tools.get(name);
    if (!tool || (apertureNames && !apertureNames.has(name))) throw new Error('unknown project tool in this capability aperture');
    validateArguments(args, tool.inputSchema);
    const invoke = () => dispatch[tool['x-bonfyre-owner']](name, args);
    if (tool.annotations.readOnlyHint === true) return await invoke();
    const pending = effectTail.then(invoke);
    effectTail = pending.catch(() => {});
    return await pending;
  }
  async function productProfile(profile, sourceIdentity) {
    const compiled = compileProjectMcpLaunchAgent(profile, { root, home, sourceIdentity });
    const lens = await readCall('mcp_capability_lens', { pack: profile, limit: 1 });
    const plan = getCapabilityPack(profile);
    const requiredHandoffs = (plan?.handoffs || []).filter((item) => item.required);
    const contextMode = requiredHandoffs.length ? 'continuation-required' : (plan?.snapshot_reads?.length ? 'local-snapshot' : 'context-required');
    const capabilityCacheKey = `${plan.contract_sha256}:${lens.performance.toolset_sha256}`;
    const releaseCacheKey = `${sourceIdentity.aggregate_sha256}:${plan.contract_sha256}:${lens.performance.toolset_sha256}`;
    const deploymentPreview = inspectProjectMcpDeployment(profile, { root, home, sourceIdentity, candidate: compiled });
    const { source_identity: _repeatedSourceIdentity, ...profileBody } = compiled;
    const listener = await probeProductListener(profile, profileBody.endpoint);
    const fullAdminModelVisibleBytes = Buffer.byteLength(JSON.stringify(publicTools));
    const modelVisibleBytes = lens.performance.catalog_public_bytes;
    const contextReductionPercent = fullAdminModelVisibleBytes === 0 ? 0
      : Math.max(0, Math.min(100, Math.round((1 - modelVisibleBytes / fullAdminModelVisibleBytes) * 10_000) / 100));
    return {
      ...profileBody,
      source_identity_sha256: sourceIdentity.aggregate_sha256,
      pack_contract_sha256: plan.contract_sha256,
      toolset_sha256: lens.performance.toolset_sha256,
      capability_cache_key: capabilityCacheKey,
      release_cache_key: releaseCacheKey,
      client_cache_key: releaseCacheKey,
      deployment_preview: deploymentPreview,
      listener,
      connection_descriptor: {
        schema_version: 'bonfyre.project-mcp.connection.v1',
        product: profile,
        transport: 'http-jsonrpc',
        mcp_url: `${profileBody.endpoint}/mcp`,
        health_url: `${profileBody.endpoint}/health`,
        authentication: { type: 'bearer-token-file', token_file: profileBody.token_file },
        capability_pack: profile,
        source_identity_sha256: sourceIdentity.aggregate_sha256,
        pack_contract_sha256: plan.contract_sha256,
        toolset_sha256: lens.performance.toolset_sha256,
        capability_cache_key: capabilityCacheKey,
        release_cache_key: releaseCacheKey,
        client_cache_key: releaseCacheKey,
        app_resource_uri: CONTROL_PLANE_APP_URI,
        entry_tool: 'mcp_product_plan',
        routing_mode: 'aperture-constrained-auto',
        pack_argument_required: false,
      },
      capability_summary: {
        tools: lens.performance.aperture_tools,
        read_tools: lens.performance.aperture_read_tools,
        effect_tools: lens.performance.aperture_effect_tools,
        pack_contract_sha256: plan.contract_sha256,
        toolset_sha256: lens.performance.toolset_sha256,
        capability_cache_key: capabilityCacheKey,
        model_visible_bytes: modelVisibleBytes,
        full_admin_model_visible_bytes: fullAdminModelVisibleBytes,
        model_visible_bytes_avoided: Math.max(0, fullAdminModelVisibleBytes - modelVisibleBytes),
        context_reduction_percent: contextReductionPercent,
        context_measurement: 'UTF-8 bytes of public MCP tool definitions for this product aperture versus the full Project admin catalogue; transport framing and host metadata are excluded.',
        snapshot_reads: plan?.snapshot_reads?.length || 0,
        handoffs: lens.pack_plan?.handoffs || [],
        handoff_count: (plan?.handoffs || []).length,
        required_handoffs: requiredHandoffs.map((item) => item.name),
        required_handoff_count: requiredHandoffs.length,
        context_mode: contextMode,
      },
    };
  }

  async function resource(uri) {
    const app = await readControlPlaneApp(uri).catch((error) => {
      if (uri === CONTROL_PLANE_APP_URI) throw error;
      return null;
    });
    if (app) return app;
    const bindings = { 'bonfyre://atlas/summary': ['architecture_atlas', { op: 'summary' }],
      'bonfyre://native/inventory': ['native_tool_inventory', {}], 'bonfyre://operator/status': ['operator_status', {}],
      'bonfyre://models/status': ['bonfyre_moe_status', {}],
      'bonfyre://mcp/toolsets': ['mcp_capability_lens', {}],
      'bonfyre://mcp/runtime': ['mcp_runtime_identity', {}],
      'bonfyre://estate/capabilities': ['estate_capabilities', {}] };
    let value, mimeType = 'application/json';
    if (uri === 'bonfyre://mcp/service-profiles' || uri === 'bonfyre://mcp/products') {
      const sourceIdentity = projectMcpSourceIdentity(root);
      const profileEntries = await Promise.all(PROJECT_MCP_SERVICE_PROFILE_NAMES.map(async profile =>
        [profile, await productProfile(profile, sourceIdentity)]));
      const profiles = Object.fromEntries(profileEntries);
      const deploymentCounts = {};
      const listenerCounts = {};
      const profileValues = Object.values(profiles);
      for (const profile of profileValues) {
        const deploymentState = profile.deployment_preview?.state || 'unavailable';
        deploymentCounts[deploymentState] = (deploymentCounts[deploymentState] || 0) + 1;
        const listenerState = profile.listener?.state || 'unavailable';
        listenerCounts[listenerState] = (listenerCounts[listenerState] || 0) + 1;
      }
      value = {
        schema_version: 'bonfyre.project-mcp.service-profiles.v1',
        source_identity: sourceIdentity,
        profiles,
        contract_validation: capabilityPackValidation,
        portfolio: {
          products: profileEntries.length,
          local_snapshot_products: Object.values(profiles).filter((item) => item.capability_summary.context_mode === 'local-snapshot').length,
          continuation_required_products: Object.values(profiles).filter((item) => item.capability_summary.context_mode === 'continuation-required').length,
          context_required_products: profileValues.filter((item) => item.capability_summary.context_mode === 'context-required').length,
          context_economics: {
            full_admin_model_visible_bytes: Buffer.byteLength(JSON.stringify(publicTools)),
            smallest_product_model_visible_bytes: Math.min(...profileValues.map((item) => item.capability_summary.model_visible_bytes)),
            largest_product_model_visible_bytes: Math.max(...profileValues.map((item) => item.capability_summary.model_visible_bytes)),
            average_product_model_visible_bytes: Math.round(profileValues.reduce((sum, item) => sum + item.capability_summary.model_visible_bytes, 0) / Math.max(1, profileValues.length)),
            best_context_reduction_percent: Math.max(...profileValues.map((item) => item.capability_summary.context_reduction_percent)),
            worst_context_reduction_percent: Math.min(...profileValues.map((item) => item.capability_summary.context_reduction_percent)),
            measurement: 'Product figures measure public MCP tool-definition bytes for each narrow aperture against the full admin catalogue; they are not token or latency claims.',
          },
          deployment: deploymentCounts,
          listeners: listenerCounts,
          healthy_listeners: listenerCounts.healthy || 0,
        },
        deployment_state: 'candidate_only',
        normalization: 'The shared source identity is emitted once; each profile references its aggregate digest instead of repeating the file manifest.',
        forbidden_inference: 'A compiled service profile is not an installed LaunchAgent, a listening endpoint, a created token, or deployment authority.',
      };
    }
    else if (/^bonfyre:\/\/mcp\/products\/[a-z][a-z0-9-]{0,31}$/.test(uri)) {
      const profile = uri.slice('bonfyre://mcp/products/'.length);
      if (!PROJECT_MCP_SERVICE_PROFILE_NAMES.includes(profile)) throw new Error('unknown Project MCP product resource');
      const sourceIdentity = projectMcpSourceIdentity(root);
      value = {
        schema_version: 'bonfyre.project-mcp.product-manifest.v1',
        product: profile,
        source_identity_sha256: sourceIdentity.aggregate_sha256,
        profile: await productProfile(profile, sourceIdentity),
        contract_validation: capabilityPackValidation,
        forbidden_inference: 'A product manifest is a source-attested connection and readiness description. It is not deployment authority, a credential value, account authority, payment state, or permission for an external effect.',
      };
    }
        else if (uri === 'bonfyre://mcp/contract') { value = contract; mimeType = 'text/markdown'; }
    else if (Object.hasOwn(bindings, uri)) {
      try { value = await call(...bindings[uri]); }
      catch (error) {
        if (uri !== 'bonfyre://models/status') throw error;
        value = { schema_version: 'bonfyre.moe.service.status.v1', status: 'unavailable', reason: errorText(error),
          forbidden_inference: 'an unavailable local model is not a reason to silently route work to a remote provider' };
      }
    } else if (/^bonfyre:\/\/mcp\/toolsets\/[a-z][a-z0-9-]{0,31}$/.test(uri)) {
      const pack = uri.slice('bonfyre://mcp/toolsets/'.length);
      if (!CAPABILITY_PACK_NAMES.includes(pack)) throw new Error('unknown capability pack resource');
      value = await call('mcp_capability_lens', { pack, limit: 50 });
    } else if (/^bonfyre:\/\/mcp\/snapshots\/[a-z][a-z0-9-]{0,31}$/.test(uri)) {
      const pack = uri.slice('bonfyre://mcp/snapshots/'.length);
      if (!CAPABILITY_PACK_NAMES.includes(pack)) throw new Error('unknown capability pack snapshot resource');
      value = await call('mcp_pack_snapshot', { pack });
    } else if (/^bonfyre:\/\/architecture\/[a-z0-9][a-z0-9-]{0,127}$/.test(uri))
      value = await call('architecture_atlas', { op: 'get', id: uri.slice('bonfyre://architecture/'.length) });
    else if (/^bonfyre:\/\/matrix\/subject\/[A-Za-z0-9][A-Za-z0-9._:@/-]{0,127}$/.test(uri))
      value = await call('matrix_subject_constellation', { subject: uri.slice('bonfyre://matrix/subject/'.length) });
    else throw new Error('unknown or invalid resource URI');
    return { contents: [{ uri, mimeType, text: typeof value === 'string' ? value : json(value) }] };
  }
  const errorText = error => String(error?.message || 'project operation failed').replaceAll(token, '[redacted]').slice(0, 2000);
  async function handle(request) {
    const url = new URL(request.url);
    if (request.method === 'GET' && url.pathname === '/health') return response(200, {
      status: 'ok', service: 'bonfyre-project-mcp', runtime: 'bun', root,
      write_enabled: env.BONFYRE_MCP_PROJECT_WRITE_ENABLED === '1',
      autonomous_write_enabled: env.BONFYRE_MCP_AUTONOMOUS_WRITE_ENABLED === '1',
      operator_paused: existsSync(join(env.BONFYRE_OPERATOR_HOME || join(home, '.bonfyre/operator'), 'STOP')),
      capability_pack: capabilityPack || null,
      advertised_tools: advertisedTools.length,
      runtime_identity: runtimeIdentity(),
    });
    if (request.method !== 'POST') return response(404, { error: 'not found' });
    if (!equal(request.headers.get('authorization') || '', `Bearer ${token}`)) return response(401, { error: 'unauthorized' });
    if (url.pathname !== '/mcp') return response(404, { error: 'not found' });
    if (active >= maxConcurrent) return response(503, { error: 'project owner concurrency limit' });
    active++;
    let id = null;
    try {
      const message = await readRequest(request);
      if (!object(message) || message.jsonrpc !== '2.0' || typeof message.method !== 'string') throw new Error('invalid JSON-RPC request');
      const incomingId = message.id ?? null;
      if (!(incomingId === null || typeof incomingId === 'string' || typeof incomingId === 'number' && Number.isFinite(incomingId))) throw new Error('invalid request id');
      id = incomingId;
      const params = message.params ?? {};
      if (!object(params)) throw new Error('params must be an object');
      let result;
      switch (message.method) {
        case 'initialize': result = { protocolVersion: '2025-06-18', capabilities: {
          tools: { listChanged: false }, resources: { subscribe: false, listChanged: false },
          prompts: { listChanged: false }, completions: {}, logging: {} },
          serverInfo: { name: 'bonfyre-project-node', version: '3.0.0' }, instructions: contract }; break;
        case 'notifications/initialized': case 'notifications/cancelled': return response(202);
        case 'ping': case 'logging/setLevel': result = {}; break;
        case 'tools/list': {
          const cursor = String(params.cursor || '');
          const start = /^\d+$/.test(cursor) ? Number(cursor) : 0;
          result = { tools: advertisedTools.slice(start, start + 100) };
          if (start + result.tools.length < advertisedTools.length) result.nextCursor = String(start + result.tools.length);
          break;
        }
        case 'tools/call': {
          try {
            const value = await call(String(params.name || ''), params.arguments ?? {});
            result = { content: [{ type: 'text', text: json(value) }], structuredContent: value, isError: false };
          } catch (error) {
            const value = { error: errorText(error), tool: String(params.name || '') };
            result = { content: [{ type: 'text', text: json(value) }], structuredContent: value, isError: true };
          }
          break;
        }
        case 'resources/list': result = { resources: CATALOG.resources }; break;
        case 'resources/templates/list': result = { resourceTemplates: CATALOG.resourceTemplates }; break;
        case 'resources/read': result = await resource(String(params.uri || '')); break;
        case 'prompts/list': result = { prompts: CATALOG.prompts }; break;
        case 'prompts/get': result = prompt(String(params.name || ''), params.arguments ?? {}); break;
        case 'completion/complete': {
          let values = [];
          if (params.ref?.type === 'ref/prompt') {
            if (params.argument?.name === 'name') values = CATALOG.prompts.map(p => p.name);
            else if (params.ref?.name === 'product_pack_workflow' && params.argument?.name === 'pack') {
              values = [...CAPABILITY_PACK_NAMES];
            }
          }
          result = { completion: { values, total: values.length, hasMore: false } }; break;
        }
        default: throw new Error('unsupported method');
      }
      const payload = { jsonrpc: '2.0', id, result };
      if (Buffer.byteLength(json(payload)) > 20_000_000) throw new Error('response exceeds project output budget; inspect the owner receipt before retrying effects');
      return response(200, payload);
    } catch (error) { return response(200, { jsonrpc: '2.0', id, error: { code: -32000, message: errorText(error) } }); }
    finally { active--; }
  }
  return { handle, call, resource };
}

if (import.meta.main) {
  const config = projectConfig();
  const node = await createProjectNode(config);
  const server = Bun.serve({ hostname: config.host, port: config.port, fetch: node.handle,
    maxRequestBodySize: 20_000_000, idleTimeout: 0 });
  console.log(`project MCP listening on ${server.hostname}:${server.port} runtime=bun`);
}
