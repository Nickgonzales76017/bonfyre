#!/usr/bin/env bun
// Durable agent work is owned here; MCP readOnlyHint is not effect authority.
import { existsSync, readFileSync, realpathSync } from 'node:fs';
import { homedir } from 'node:os';
import { join, resolve } from 'node:path';
import { createMcpTransport, object } from './http_boundary.mjs';
import { createOwnedMcpCall, mcpEndpoint } from './owned_mcp_client.mjs';
import { projectRun } from './project_runtime.mjs';
import { hookEvidence } from './hook_bridge.mjs';
import { createFabricActivityBoundary } from './fabric_activity.mjs';
import { accountAuthorizerFromEnvironment } from './account_authority.mjs';

const catalog = JSON.parse(readFileSync(new URL('./fabric_catalog.json', import.meta.url), 'utf8'));
export const FABRIC_TOOLS = catalog.tools, FABRIC_PROMPTS = catalog.prompts;
export const FABRIC_VERSION = '4.0.0';
const MAX_BYTES = 20_000_000;
const instructions = 'Read fabric_status for current admission and live execution before dispatch. Use implementation_relay for isolated implementation, review and tests. To include current uncommitted source, pass source_files with whole-file hashes from project_read; unselected files stay at the configured Git base; blocked apply routes stop before final review. Poll task_status with detail=summary and inspect full results only when needed. Historical running records do not prove a live executor. Do not recursively call peer tools from a peer child session.';
const EFFECTS = ['dispatch_claude', 'dispatch_codex', 'duel', 'implementation_relay', 'cross_review', 'dual_verify', 'pr_converge', 'resume_session', 'batch_plan', 'task_cancel', 'estate_mission'];

export function fabricConfig(env = process.env, userHome = homedir()) {
  const home = resolve(env.BONFYRE_FABRIC_HOME || join(userHome, '.bonfyre/dev-fabric'));
  const token = readFileSync(env.BONFYRE_MCP_TOKEN_FILE || join(userHome, '.bonfyre/mcp/token'), 'utf8').trim();
  const port = Number(env.BONFYRE_FABRIC_PORT || 3053), host = env.BONFYRE_FABRIC_HOST || '127.0.0.1';
  if (!token || !Number.isSafeInteger(port) || port < 1 || port > 65535 || !['127.0.0.1', '::1', 'localhost'].includes(host)) throw new Error('invalid fabric listener or token');
  return { token, home, env, host, port, projectRoot: realpathSync(env.BONFYRE_PROJECT_ROOT || join(userHome, 'Developer/Bonfyre')),
    worktreeRoot: resolve((env.BONFYRE_FABRIC_WORKTREES || join(home, 'worktrees')).replace(/^~(?=\/)/, userHome)),
    policyPath: env.BONFYRE_FABRIC_POLICY || join(home, 'fabric_policy.json'),
    mcpUrl: mcpEndpoint(env.BONFYRE_MCP_URL || 'http://127.0.0.1:13052/mcp'),
    projectUrl: mcpEndpoint(env.BONFYRE_PROJECT_MCP_URL || 'http://127.0.0.1:3051/mcp'),
    ...(env.BONFYRE_PROJECT_MCP_TOKEN_FILE ? { projectToken: readFileSync(env.BONFYRE_PROJECT_MCP_TOKEN_FILE, 'utf8').trim() } : {}),
    stopPaths: [join(home, 'STOP'), join(userHome, '.bonfyre/operator/HALT')] };
}

export async function createFabricServer({ token, home, projectRoot, worktreeRoot, policyPath, policy, store, work, estate, agentRunner, authorizeAccount, pushDelivery, modelWork, nativeModel, modelMeter, outputTransfer,
  mcpUrl = 'http://127.0.0.1:13052/mcp', projectUrl = 'http://127.0.0.1:3051/mcp', projectToken, run = projectRun, request = fetch, env = process.env,
  stopPaths = [join(home || homedir(), 'STOP'), join(homedir(), '.bonfyre/operator/HALT')], maxConcurrent = 16, maxQueued = 64 } = {}) {
  if (!token) throw new Error('fabric token missing');
  if (!policy) {
    const bytes = readFileSync(policyPath); if (bytes.length > 100_000) throw new Error('fabric policy exceeds budget');
    policy = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes));
  }
  if (!object(policy) || !Number.isSafeInteger(policy.max_parallel_tasks) || policy.max_parallel_tasks < 1 || policy.max_parallel_tasks > 8
      || !Number.isSafeInteger(maxQueued) || maxQueued < 1 || maxQueued > 256 || !Array.isArray(stopPaths)) throw new Error('invalid fabric admission policy');
  store ||= (await import('./fabric_store.mjs')).createFabricStore({ home, projectRoot, worktreeRoot, run });
  if (new URL(mcpEndpoint(projectUrl)).protocol === 'https:' && !projectToken)
    throw new Error('remote Project owner requires a dedicated credential');
  const projectCall = createOwnedMcpCall({ endpoint: projectUrl, token: projectToken || token, request,
    tools: ['project_status', 'project_patch_preview', 'project_apply_low_risk_patch'] });
  const meshCall = createOwnedMcpCall({ endpoint: mcpUrl, token, request, tools: ['mesh_status', 'release_attest'] });
  const projectTool = (name, args, options) => name.startsWith('project_') ? projectCall(name, args, options) : meshCall(name, args, options);
  estate ||= (await import('./fabric_estate.mjs')).createFabricEstate();
  work ||= (await import('./fabric_work.mjs')).createFabricWork({ store, policy, projectRoot, token, mcpUrl, run, projectTool, env, stopPaths, agentRunner,
    estateStatus: () => estate.status() });
  let closed = false;
  const queue = [], running = new Map(), failures = [];
  const stopped = () => stopPaths.some(existsSync);
  authorizeAccount ||= accountAuthorizerFromEnvironment(env);
  pushDelivery ||= (await import('./fabric_push.mjs')).createFabricPushService({store,authorize:authorizeAccount,origin:env.BONFYRE_PUSH_ORIGIN,home:store.home,request,stopped:()=>closed||stopped()||policy.allow_background_tasks===false});
  const admit = () => {
    if (closed || stopped()) throw new Error('fabric is stopped');
    if (policy.allow_background_tasks === false) throw new Error('background agent tasks disabled by policy');
  };
  const execution = id => ({owned:running.has(id),queued:queue.includes(id)});
  if(!modelWork){
    const {FabricActivityError}=await import('./fabric_store.mjs');
    nativeModel ||= (await import('./native_model.mjs')).nativeModelFromEnvironment(env,{fetchImpl:request});
    modelMeter ||= (await import('./native_meter.mjs')).nativeMeterFromEnvironment(env,{root:projectRoot});
    modelWork=(await import('./fabric_model.mjs')).createFabricModelWork({store,native:nativeModel,meter:modelMeter,authorizeAccount,
      admit:({enqueue=false}={})=>{
        if(closed||stopped()||policy.allow_background_tasks===false)throw new FabricActivityError('paused','Background model work is paused.');
        if(enqueue&&queue.length>=maxQueued)throw new FabricActivityError('capacity','The account work queue is full.');
      },
      enqueue:id=>{queue.push(id);queueMicrotask(pump);},execution});
  }
  outputTransfer ||= (await import('./fabric_output_transfer.mjs')).outputTransferFromEnvironment(env,{root:projectRoot,store,
    outputArtifact:(binding,id,outputId)=>modelWork.outputArtifact(binding,id,outputId),authorizeAccount,admit});
  const owner = id => modelWork.ownsTask(id)?modelWork:work;
  function pump() {
    while (!closed && queue.length && running.size < policy.max_parallel_tasks) {
      const id = queue.shift();
      const pending = Promise.resolve().then(() => {
        if (stopped()||policy.allow_background_tasks===false) { store.updateTask(id, { state: 'blocked', completed_at: Date.now() / 1000, error: 'Background work paused before execution' }); return; }
        return owner(id).execute(id);
      }).catch(error => {
        const message = String(error?.message || error).replaceAll(token, '[redacted]').slice(0, 2000);
        try { store.updateTask(id, { state: 'uncertain', completed_at: Date.now() / 1000, error: message }); }
        catch { failures.push({ task_id: id, state: 'unreconciled' }); if (failures.length > 64) failures.shift(); }
      }).finally(() => { running.delete(id); pump(); });
      running.set(id, pending);
    }
  }
  function submit(kind, title, prompt, apply = false, source_files) {
    admit(); if (queue.length >= maxQueued) throw new Error('fabric task queue full');
    const id = store.createTask({ kind, title, prompt, apply_to_project: apply, source_files });
    queue.push(id); queueMicrotask(pump);
    return { task_id: id, state: 'queued', kind, title };
  }
  // A native account-aware owner supplies authority separately after admitting
  // its actual effect. This is not a browser/MCP tool or retroactive task tag.
  function submitScopedTask(input,authority) {
    admit();if(queue.length>=maxQueued)throw new Error('fabric task queue full');
    if(input?.kind==='model')throw new Error('Model work requires its exact native execution review');
    const result=store.createScopedTask(input,authority);
    if(!result.replayed){queue.push(result.task_id);queueMicrotask(pump);}
    return result;
  }
  async function status() {
    const inspect = async name => {
      const command = policy.agents?.[name]?.command || name;
      const version = await run([command, '--version'], { env, cwd: projectRoot, timeoutMs: 10000, maxOutputBytes: 65536 });
      return { installed: version.exit_code !== 127, version: version.exit_code === 127 ? null : version };
    };
    const [claude, codex, mesh, project] = await Promise.all([inspect('claude'), inspect('codex'),
      meshCall('mesh_status', {}, { timeoutMs: 5000 }).catch(() => ({ transport_error: 'configured mesh status unavailable' })),
      projectCall('project_status', {}, { timeoutMs: 5000 }).catch(() => ({ transport_error: 'configured Project owner unavailable' }))]);
    return { service: 'bonfyre-dual-agent-fabric', project_root: projectRoot, claude, codex, bonfyre_mcp: mesh, project_mcp: project,
      admission: { paused: stopped(), accepting_tasks: !closed && !stopped() && policy.allow_background_tasks !== false,
        background_tasks_enabled: policy.allow_background_tasks !== false },
      execution: { active_tasks: [...running.keys()], queued_tasks: [...queue], unreconciled: [...failures],
        scope: 'Executors and queue owned by this service process; stored task states are historical records.' }, ...store.summary() };
  }
  const dispatch = {
    fabric_status: status,
    dispatch_claude: args => submit('claude', args.title || 'Claude task', args.prompt),
    dispatch_codex: args => submit('codex', args.title || 'Codex task', args.prompt),
    duel: args => submit('duel', args.title || 'Claude/Codex duel', args.prompt),
    implementation_relay: args => submit('relay', args.title || 'Claude/Codex implementation relay', args.prompt, args.apply_to_project ?? false, args.source_files),
    cross_review: args => submit('review', args.title || 'Cross review', args.instructions || ''),
    dual_verify: args => submit('verify', args.title || 'Dual verification', args.claim),
    pr_converge: args => submit('pr', 'Current PR convergence', args.instructions || '', args.apply_to_project ?? false),
    resume_session: args => submit(`resume-${args.agent}`, args.session_id, args.prompt),
    batch_plan: args => submit('batch', 'Cross-agent batch plan', args.mission),
    task_status: args => {
      const value = args.detail === 'summary' ? store.taskSummary(args.task_id) : store.taskStatus(args.task_id);
      const queued = queue.indexOf(args.task_id);
      return { ...value, execution: { owned_by_this_process: running.has(args.task_id), queue_position: queued < 0 ? null : queued,
        reconciliation_required: ['queued', 'running'].includes(value.state) && !running.has(args.task_id) && queued < 0 } };
    },
    task_cancel: args => owner(args.task_id).cancel(args.task_id),
    recent_receipts: args => store.recentReceipts(args.limit ?? 50),
    estate_mission: async args => {
      admit(); const start = performance.now();
      const result = await estate.add({ title: args.title, details: args.details || '', family: args.family || 'manual', priority: args.priority ?? 90, locks: args.locks || 'general' });
      // Preserve the old stdout/result consumers without pretending a retired
      // CLI was executed. The actual owner and native result are explicit.
      return { ...result, backend: 'bun:sqlite', operation: 'estate_mission', exit_code: 0, stdout: `${JSON.stringify(result)}\n`, stderr: '', duration_ms: Math.round(performance.now() - start) };
    },
  };
  const promptText = {
    duel: args => `Call the duel tool for this mission: ${args.mission}`,
    relay: args => `Read fabric_status before dispatch. If admission is paused, report the existing STOP state without resuming it. Otherwise read the files needed for the mission with project_read and pass their whole-file hashes in implementation_relay.source_files when current uncommitted source is required. Unselected files stay at the configured Git base. Call implementation_relay for this mission: ${args.mission}\nRetain the returned task_id and poll task_status with detail=summary. A record not owned by the current service needs reconciliation; do not resubmit it merely because it is old. Inspect full results for findings or the final patch and apply receipt.`,
    cross_review: () => 'Call cross_review on the current repository changes.',
    estate: args => `Compile and add this as an estate mission before implementation: ${args.mission}`,
  };
  const transport = createMcpTransport({ token, label: 'fabric', service: 'bonfyre-dual-agent-fabric', version: FABRIC_VERSION,
    instructions, tools: FABRIC_TOOLS, prompts: FABRIC_PROMPTS, dispatch, statusTool: 'fabric_status', serialize: EFFECTS, maxBytes: MAX_BYTES, maxConcurrent,
    health: () => ({ paused: stopped(), accepting_tasks: !closed && !stopped() && policy.allow_background_tasks!==false, active_tasks: running.size, queued_tasks: queue.length, unreconciled: failures.length }),
    getPrompt: (name, args) => ({ description: name, messages: [{ role: 'user', content: { type: 'text', text: promptText[name](args) } }] }),
    routes: [
      { method: 'GET', path: '/events', run: () => ({events: store.safeEvents(500).events.map(row => {
        if (!['codex','claude'].includes(row.source)) return row;
        const evidence = hookEvidence({ ...(row.payload || {}), source: row.source, kind: row.kind });
        return { ...row, payload: evidence || {source: row.source, kind: row.kind, legacy_redacted: true} };
      })}) },
      { method: 'POST', path: '/hook', status: 202, serialize: true, schema: { type: 'object', properties: {
        source: { type: 'string' }, kind: { type: 'string' }, hook_event_name: { type: 'string' }, session_id: { type: 'string' }, thread_id: { type: 'string' },
      } }, run: payload => {
        const evidence = hookEvidence(payload);
        if (!evidence) return { accepted: false };
        return { accepted: true, seq: store.recordEvent(evidence.source, evidence.kind,
          evidence, evidence.session_id || evidence.thread_id || null) };
      } },
    ] });
  const activity=createFabricActivityBoundary({token,
    read:(binding,options)=>store.activityRead(binding,{...options,execution}),
    notification:(binding,input)=>store.notificationAction(binding,input),
    push:(binding,operation,payload)=>pushDelivery.dispatch(binding,operation,payload),
    model:async(binding,operation,payload,options)=>{
      if(operation==='model-verify-transfer')return outputTransfer.verify(binding,payload);
      const value=await modelWork.dispatch(binding,operation,payload,options);
      if(['model-read','model-cancel','model-reconcile'].includes(operation)){
        const {localTransferAllowed,...view}=value;
        return {...view,localTransferAvailable:outputTransfer.configured===true&&localTransferAllowed===true};
      }
      return value;
    }});
  pushDelivery.start();
  return { ...transport,submitScopedTask,handle:request=>new URL(request.url).pathname==='/activity'?activity(request):transport.handle(request),
    async drain() { while (queue.length || running.size) { pump(); await Promise.all(running.values()); } },
    async close() {
      if (closed) return; closed = true;
      await pushDelivery.close();
      await outputTransfer.close();
      const owned = [...queue.splice(0), ...running.keys()];
      await Promise.all(owned.map(id => owner(id).cancel(id)));
      await Promise.all(running.values()); await modelWork.close(); await work.close?.(); store.close?.();
    } };
}

if (import.meta.main) {
  const config = fabricConfig(), node = await createFabricServer(config);
  const server = Bun.serve({ hostname: config.host, port: config.port, fetch: node.handle, maxRequestBodySize: MAX_BYTES, idleTimeout: 0 });
  console.log(`dual-agent fabric listening on ${server.hostname}:${server.port} runtime=bun`);
  let stopping = false;
  for (const signal of ['SIGTERM', 'SIGINT']) process.on(signal, async () => {
    if (stopping) return; stopping = true; server.stop(); await node.close(); process.exit(0);
  });
}
