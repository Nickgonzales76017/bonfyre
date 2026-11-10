#!/usr/bin/env bun
// Existing autonomous operator, with one durable queue and policy-owned effects.
// Importing or starting while STOP is present does not seed, reclaim, or call a model.
import { existsSync, readFileSync, mkdirSync, writeFileSync, renameSync, unlinkSync, openSync, closeSync } from 'node:fs';
import { homedir } from 'node:os';
import { join, resolve } from 'node:path';
import { createHash, randomUUID } from 'node:crypto';
import { projectRun } from './project_runtime.mjs';
import { validateArguments } from './project_node.mjs';

const object = value => value !== null && typeof value === 'object' && !Array.isArray(value);
const json = value => JSON.stringify(value);
const sha = value => createHash('sha256').update(value).digest('hex');
const bounded = (value, fallback, low, high) => {
  const n = value ?? fallback;
  if (!Number.isSafeInteger(n) || n < low || n > high) throw new Error('invalid operator resource policy');
  return n;
};
export function operatorDaemonAlive(pid, { spawn = Bun.spawnSync } = {}) {
  try { process.kill(pid, 0); } catch (error) { if (error.code === 'ESRCH') return false; }
  const ps = spawn(['ps', '-o', 'command=', '-p', String(pid)], { stdout: 'pipe', stderr: 'ignore' });
  return ps.exitCode === 0 && ps.stdout.toString().includes('operator_daemon');
}

export function operatorConfig(env = process.env, userHome = homedir()) {
  const home = resolve(env.BONFYRE_OPERATOR_HOME || join(userHome, '.bonfyre/operator'));
  const policyPath = env.BONFYRE_OPERATOR_POLICY || join(home, 'policy.json');
  const tokenPath = env.BONFYRE_MCP_TOKEN_FILE || join(userHome, '.bonfyre/mcp/token');
  const token = existsSync(tokenPath) ? readFileSync(tokenPath, 'utf8').trim() : env.BONFYRE_MCP_TOKEN || '';
  const gateway = (env.BONFYRE_MCP_BASE || 'http://127.0.0.1:13052').replace(/\/$/, '');
  const url = new URL(gateway);
  if (!['http:', 'https:'].includes(url.protocol) || url.username || url.password || url.search || url.hash)
    throw new Error('invalid configured MCP gateway');
  if (url.protocol === 'http:' && !['127.0.0.1', 'localhost', '[::1]'].includes(url.hostname))
    throw new Error('operator credentials require loopback or HTTPS');
  const model = env.BONFYRE_OPERATOR_MODEL || 'gpt-5';
  return { home, userHome, policyPath, token, gateway, model, env,
    stopPath: join(home, 'STOP'), receiptDir: join(home, 'receipts'),
    loadPolicy() { const value = JSON.parse(readFileSync(policyPath, 'utf8')); if (!object(value)) throw new Error('operator policy must be an object'); return value; },
    paused() { return existsSync(join(home, 'STOP')); } };
}

// Match the established trusted provider-reference shell files, but load them
// lazily only when a model request is admitted. Never log or persist their output.
export async function loadOperatorSecrets(config, run = projectRun) {
  const files = ['provider.refs.env', 'oracle.refs.env', 'cloud.refs.env']
    .map(name => join(config.userHome, '.bonfyre/secrets', name)).filter(existsSync);
  if (!files.length) return { ...config.env };
  const result = await run(['/bin/bash', '-c', 'set -a; for refs in "$@"; do source "$refs" || exit; done; env -0', 'operator-secrets', ...files],
    { env: config.env, timeoutMs: 10_000, maxOutputBytes: 1024 * 1024 });
  if (result.exit_code !== 0) throw new Error('trusted operator secret references failed to load');
  const env = { ...config.env };
  for (const item of result.stdout.split('\0')) {
    const at = item.indexOf('=');
    if (at > 0) env[item.slice(0, at)] = item.slice(at + 1);
  }
  return env;
}

export function createOperatorTransport(config, { request = fetch, secrets = () => loadOperatorSecrets(config), now = Date.now } = {}) {
  let deadline = Infinity, key;
  const redact = text => [config.token, key].filter(Boolean).reduce((out, secret) => out.replaceAll(secret, '[REDACTED]'), String(text));
  async function requestJson(url, payload, headers, timeoutMs = 30_000) {
    const remaining = Math.min(timeoutMs, deadline - now());
    if (remaining <= 0) throw new Error('operator cycle deadline exceeded');
    const response = await request(url, { method: payload === undefined ? 'GET' : 'POST', redirect: 'error',
      headers: { accept: 'application/json', ...(payload === undefined ? {} : { 'content-type': 'application/json' }), ...headers },
      ...(payload === undefined ? {} : { body: json(payload) }), signal: AbortSignal.timeout(Math.max(1, Math.ceil(remaining))) });
    // Even a malformed or empty 401/429 body must retain its transport status
    // for quarantine. Never expose raw provider error bytes in this path.
    const malformed = reason => {
      const error = new Error(`operator HTTP ${response.status}: ${reason}`);
      if (!response.ok) { error.http_status = response.status; error.error_code = 'request_failed'; }
      return error;
    };
    const reader = response.body?.getReader();
    if (!reader) throw malformed('missing body');
    const chunks = []; let size = 0;
    try {
      while (true) {
        const { done, value } = await reader.read(); if (done) break;
        size += value.byteLength;
        if (size > 8 * 1024 * 1024) throw malformed('response exceeds 8 MiB');
        chunks.push(value);
      }
    } catch { throw malformed('body read failed or oversized'); } finally { await reader.cancel().catch(() => {}); reader.releaseLock(); }
    let value;
    try { value = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(Buffer.concat(chunks))); }
    catch { throw malformed('invalid JSON'); }
    if (!response.ok) {
      // Error bodies may contain prompts or credentials. Retain only a bounded
      // machine error code, enough to distinguish terminal quota/auth failures.
      const code = value?.error?.code || value?.error?.type;
      const safeCode = typeof code === 'string' && /^[a-zA-Z0-9_-]{1,80}$/.test(code) ? code : 'request_failed';
      const error = new Error(redact(`operator HTTP ${response.status}: ${safeCode}`));
      error.http_status = response.status; error.error_code = safeCode;
      throw error;
    }
    return value;
  }
  async function rpc(method, params = {}) {
    if (!config.token) throw new Error('MCP token missing');
    const id = randomUUID();
    const value = await requestJson(`${config.gateway}/mcp`, { jsonrpc: '2.0', id, method, params },
      { authorization: `Bearer ${config.token}`, 'MCP-Protocol-Version': '2025-06-18' }, 3_600_000);
    if (!object(value) || value.id !== id || value.error || !Object.hasOwn(value, 'result')) throw new Error('MCP response identity or error refusal');
    return value.result;
  }
  return {
    setDeadline(value) { deadline = value; }, redact,
    async listTools() {
      const tools = [], seen = new Set(); let cursor;
      for (let page = 0; page < 20; page++) {
        const value = await rpc('tools/list', cursor ? { cursor } : {});
        if (!Array.isArray(value?.tools)) throw new Error('invalid MCP tool catalog');
        for (const tool of value.tools) {
          if (!object(tool) || typeof tool.name !== 'string' || seen.has(tool.name)) throw new Error('invalid or duplicate MCP tool');
          seen.add(tool.name); tools.push(tool);
        }
        if (tools.length > 2000) throw new Error('MCP tool catalog exceeds bound');
        if (!value.nextCursor) return tools;
        if (value.nextCursor === cursor) throw new Error('MCP catalog cursor did not advance');
        cursor = value.nextCursor;
      }
      throw new Error('MCP tool catalog exceeds page budget');
    },
    async mcpCall(name, args) {
      const value = await rpc('tools/call', { name, arguments: args });
      if (value?.isError === true) throw new Error(`MCP tool ${name} refused`);
      if (object(value) && Object.hasOwn(value, 'structuredContent')) return value.structuredContent;
      for (const content of value?.content || []) if (content.type === 'text' && typeof content.text === 'string') {
        try { return JSON.parse(content.text); } catch { return { text: content.text }; }
      }
      return value;
    },
    async events(after) {
      if (!Number.isSafeInteger(after) || after < 0) throw new Error('invalid event cursor');
      if (!config.token) throw new Error('MCP token missing');
      const value = await requestJson(`${config.gateway}/events?after=${after}&limit=1000`, undefined, { authorization: `Bearer ${config.token}` });
      if (!Array.isArray(value?.events) || value.events.length > 1000 || !Number.isSafeInteger(value.next_after) || value.next_after < after)
        throw new Error('invalid operator event page');
      return value;
    },
    async response(payload, timeoutSeconds, beforeDispatch = () => {}) {
      key ??= (await secrets()).OPENAI_API_KEY;
      if (!key) throw new Error('OPENAI_API_KEY missing from provider refs');
      // Secret resolution is asynchronous. STOP, quarantine, or resource policy
      // may change while it is pending; recheck before opening a paid request.
      // A refusal here is an admission failure, not a provider failure.
      beforeDispatch();
      try { return await requestJson('https://api.openai.com/v1/responses', payload, { authorization: `Bearer ${key}` }, timeoutSeconds * 1000); }
      catch (error) { throw tagProviderFailure(error, 'openai'); }
    },
  };
}

// Only the transport that made the request may say a provider failed; goal or
// tool text can never manufacture a quarantine.
const TERMINAL_CODES = ['insufficient_quota', 'credit_balance_exhausted', 'billing_hard_limit', 'invalid_api_key'];
function tagProviderFailure(error, provider) {
  const match = Number.isSafeInteger(error?.http_status);
  const status = match ? error.http_status : 0, code = match ? error.error_code : '';
  let cause = null, cooldown = 0;
  if (TERMINAL_CODES.includes(code)) cause = code;
  else if (status === 401) cause = 'invalid_api_key';
  else if (status === 429) { cause = 'rate_limited'; cooldown = 900; }
  else if (!match && /fetch|connect|ECONNREFUSED|Unable to connect|socket/i.test(String(error?.message))) { cause = 'connection_unavailable'; cooldown = 300; }
  if (cause) error.provider_failure = { provider, cause, cooldown_seconds: cooldown };
  return error;
}

export const responseText = response => (response.output || []).filter(item => item.type === 'message')
  .flatMap(item => item.content || []).filter(item => ['output_text', 'text'].includes(item.type)).map(item => String(item.text || '')).join('');
const instructions = policy => `You are the autonomous Bonfyre power-user operator, not a conversational assistant.
You have delegated authority only under this policy:\n${JSON.stringify(policy, null, 2)}
Work until the goal is verified resolved, genuinely blocked, or must retry later. Use live MCP evidence rather than memory. Parallel read-only calls are allowed.
Apply low-risk repairs only through project_apply_low_risk_patch. The wrapper requires tests and restores failed patches through their receipts. Never request or expose approval tokens. Never use project_apply_patch.
External writes and irreversible effects are prohibited. Internal profile and deep-flow execution is allowed only within policy. Do not claim success without live verification. Finish with operator_finish exactly once.`;

export function createOperatorDaemon({ config, store, tools, transport, now = () => Date.now() / 1000, onReceipt = () => {} }) {
  let busy = false;
  const admitted = policy => !config.paused() && policy.autonomous !== false;
  async function ingest() {
    const after = Number(store.metaGet('event_cursor', '0'));
    const page = await transport.events(after);
    // Cursor and derived goals are committed together; never acknowledge first.
    store.ingestEvents(page.events, { nextAfter: page.next_after });
    return page.events;
  }
  async function runGoal(claim, policy, recentEvents = [], admission = null) {
    const { goal, run } = claim;
    const maxRounds = bounded(policy.max_rounds_per_cycle, 16, 1, 128);
    const maxCalls = bounded(policy.max_tool_calls_per_cycle, 80, 1, 1024);
    const seconds = bounded(policy.cycle_timeout_seconds, 2700, 1, 3600);
    const expires = now() + seconds;
    transport.setDeadline?.(expires * 1000);
    const { tools: apiTools, catalog } = admission ?? await tools.catalog(policy);
    const definitions = new Map(apiTools.map(tool => [tool.name, tool.parameters]));
    const guard = () => {
      const current = config.loadPolicy();
      if (!admitted(current)) throw new Error('operator paused or disabled during cycle');
      const health = store.providerStatus?.('openai');
      if (current.allow_provider_requests !== true || !Array.isArray(current.approved_provider_models) ||
          !current.approved_provider_models.includes(config.model) || (health && health.state !== 'unverified'))
        throw new Error('model provider admission withdrawn during cycle');
      if (now() >= expires) throw new Error('operator cycle deadline exceeded');
    };
    const verifyModelAdmission = () => {
      guard();
      if (!store.budgetOk(config.loadPolicy(), undefined, { continuingRunId: run.id, forModelResponse: true }).okay)
        throw new Error('daily operator model-token budget exhausted');
    };
    const request = async input => {
      verifyModelAdmission();
      const result = await transport.response({ model: config.model, instructions: instructions(policy),
        tools: apiTools, parallel_tool_calls: true, store: true, ...input }, Math.max(1, Math.ceil(expires - now())), verifyModelAdmission);
      if (!object(result) || typeof result.id !== 'string' || !result.id || !Array.isArray(result.output)) throw new Error('invalid model response');
      store.recordResponse({ run_id: run.id, response_id: result.id, usage: result.usage || {} });
      if (result.error || !['completed', undefined].includes(result.status)) throw new Error('model response did not complete');
      return result;
    };
    let response = await request({ input: `GOAL ID: ${goal.id}\nTITLE: ${goal.title}\nPRIORITY: ${goal.priority}\nATTEMPTS: ${goal.attempts}\nSOURCE: ${goal.source}\nDETAILS:\n${goal.details}\nRECENT MCP EVENTS:\n${json(recentEvents.slice(-50))}\nInspect current state and retain exact evidence.`,
      metadata: { bonfyre_operator_run: run.id, bonfyre_goal: goal.id } });
    let finish = null, toolCalls = 0, followupsAdded = 0;
    const followupLimit = bounded(policy.max_followup_goals_per_cycle, 16, 0, 16);
    const availableFollowups = () => Math.min(followupLimit,
      bounded(config.loadPolicy().max_followup_goals_per_cycle, 16, 0, 16)) - followupsAdded;
    for (let round = 0; round < maxRounds; round++) {
      const terminalOnly = toolCalls >= maxCalls;
      const calls = response.output.filter(item => item.type === 'function_call');
      if (!calls.length) break;
      if (calls.length > 1024) throw new Error('model tool-call batch exceeds bound');
      const seen = new Set();
      for (const call of calls) {
        if (typeof call.call_id !== 'string' || !call.call_id || seen.has(call.call_id)) throw new Error('invalid or duplicate function call identity');
        seen.add(call.call_id);
      }
      const outcomes = Array.from({ length: calls.length }), reads = [], writes = [];
      // A finish mixed with effects is ambiguous. Refuse the batch rather than
      // reporting completion and performing a trailing write anyway.
      if (calls.some(call => call.name === 'operator_finish') && calls.length !== 1) throw new Error('operator_finish must be a single terminal call');
      for (const [index, call] of calls.entries()) {
        // Permit the terminal control call after the last tool slot, but
        // never dispatch another MCP effect or charge another slot.
        if (toolCalls >= maxCalls && call.name !== 'operator_finish') {
          outcomes[index] = { error: 'cycle tool-call budget exhausted' }; continue;
        }
        if (toolCalls < maxCalls) toolCalls++;
        try {
          if (!definitions.has(call.name)) throw new Error('tool is not admitted by current policy');
          const args = JSON.parse(call.arguments || '{}');
          validateArguments(args, definitions.get(call.name));
          if (call.name === 'operator_finish') {
            guard(); finish = args; outcomes[index] = { accepted: true };
            // finishGoal owns the durable disposition. This terminal control
            // call is not a dispatched MCP effect or another tool-budget debit.
          } else if (call.name === 'operator_add_goal') {
            writes.push({ index, call, args, local: true });
          } else {
            const definition = catalog instanceof Map ? catalog.get(call.name) : catalog[call.name];
            (definition?.annotations?.readOnlyHint === true ? reads : writes).push({ index, call, args });
          }
        } catch (error) {
          outcomes[index] = { error: transport.redact(String(error.message)) };
          store.recordAction({ run_id: run.id, tool: String(call.name), args: {}, state: 'refused', result: outcomes[index], started_at: now(), policy: config.loadPolicy() });
        }
      }
      const execute = async item => {
        guard();
        const currentPolicy = config.loadPolicy();
        if (!store.budgetOk(currentPolicy, undefined, { continuingRunId: run.id, forModelResponse: true }).okay)
          throw new Error('daily operator model-token budget exhausted before tool dispatch');
        if (!store.budgetOk(currentPolicy, undefined, { continuingRunId: run.id }).okay) {
          // No action intent or effect is opened. The model may use its remaining
          // token allowance to close the run with the existing finish owner.
          outcomes[item.index] = { error: 'daily operator tool-call budget exhausted; no dispatch' };
          return;
        }
        if (item.local) {
          const args = item.args, title = args.title.trim(), details = (args.details || '').trim();
          if (!title) throw new Error('follow-up goal title is empty');
          if (availableFollowups() < 1) throw new Error('follow-up goal admission limit reached');
          const seq = store.recordAction({ run_id: run.id, tool: item.call.name, args, state: 'started', result: null, started_at: now(), policy: config.loadPolicy() });
          try {
            outcomes[item.index] = store.addGoal({ ...args, title, details, source: `run:${run.id}`,
              dedupe_key: args.dedupe_key?.trim() || `operator:${sha(`${title}\n${details}`).slice(0, 24)}` });
          } catch (error) {
            store.completeAction({ seq, state: 'failed', result: { error: transport.redact(String(error.message || error)) } });
            throw error;
          }
          // If recording success fails after the insertion, retain the started
          // intent for reconciliation rather than falsely recording no effect.
          store.completeAction({ seq, state: 'passed', result: outcomes[item.index] });
          followupsAdded++;
        } else outcomes[item.index] = await tools.execute(run.id, item.call.name, item.args, config.loadPolicy());
      };
      // Bounded parallel reads; mutations remain ordered and independently gated.
      let cursor = 0;
      const settled = await Promise.allSettled(Array.from({ length: Math.min(8, reads.length) }, async () => {
        while (cursor < reads.length) await execute(reads[cursor++]);
      }));
      const failed = settled.find(result => result.status === 'rejected');
      if (failed) throw failed.reason;
      for (const item of writes) await execute(item);
      if (finish) break;
      // One final model response can close the exact admitted run. A further
      // nonterminal response is refused rather than looping for more tools.
      if (round + 1 >= maxRounds || terminalOnly) break;
      response = await request({ previous_response_id: response.id, input: calls.map((call, index) => ({
        type: 'function_call_output', call_id: call.call_id, output: json(outcomes[index]),
      })) });
    }
    finish ||= { disposition: 'retry', summary: responseText(response) || 'Model ended without operator_finish.', evidence: [], retry_after_seconds: 900 };
    guard();
    const followups = finish.new_goals ?? [];
    if (!Array.isArray(followups) || followups.length > availableFollowups())
      throw new Error('follow-up goal admission limit reached');
    // Import the complete response through the existing replay-safe SQLite owner.
    // Each replay preserves already accepted goal state and refuses divergence.
    if (followups.length) {
      const batch = followups.map(item => {
        if (!object(item) || typeof item.title !== 'string' ||
            (item.details !== undefined && typeof item.details !== 'string'))
          throw new Error('invalid follow-up goal');
        const title = item.title.trim(), details = (item.details ?? '').trim();
        if (!title) throw new Error('follow-up goal title is empty');
        return { title, details, priority: bounded(item.priority, 50, 1, 100),
          source: `run:${run.id}`, recurring: false, interval_seconds: 0,
          dedupe_key: item.dedupe_key?.trim() || `operator:${sha(`${title}\n${details}`).slice(0, 24)}` };
      });
      store.importGoals(batch, { apply: true });
    }
    const finished = store.finishGoal({ run_id: run.id, ...finish, policy });
    const receipt = { schema: 'bonfyre.autonomous-power-run.v1', run_id: run.id, goal: finished.goal,
      disposition: finished.run.state, summary: finished.run.summary, evidence: finish.evidence || [], tool_calls: toolCalls,
      response_id: response.id, usage: JSON.parse(finished.run.usage_json), started_at: run.started_at, completed_at: now() };
    try { await onReceipt(receipt); }
    catch (error) {
      // The SQLite transition is already authoritative. A failed file projection
      // must not turn a completed goal into another execution of its effects.
      receipt.receipt_projection_error = transport.redact(String(error.message || error));
    }
    return receipt;
  }
  return {
    runGoal,
    async once() {
      if (busy) return { state: 'busy' };
      const policy = config.loadPolicy();
      if (!admitted(policy)) return { state: config.paused() ? 'paused' : 'disabled' };
      busy = true;
      let claim;
      try {
        const budget = store.budgetOk(policy);
        if (!budget.okay) return { state: 'budget_exhausted' };
        if (store.cyclesLastHour() >= bounded(policy.max_cycles_per_hour, 6, 1, 1000)) return { state: 'cycle_limit' };
        if (policy.allow_provider_requests !== true ||
            !Array.isArray(policy.approved_provider_models) ||
            !policy.approved_provider_models.includes(config.model)) return { state: 'provider_not_admitted' };
        const health = store.providerStatus?.('openai');
        if (health && health.state !== 'unverified') return { state: 'provider_quarantined', cause: health.cause };
        store.reconcileOrphans(policy);
        if (typeof store.selectGoal === 'function' && !store.selectGoal(policy)) return { state: 'idle' };
        const retryAfter = Number(store.metaGet?.('gateway_quarantine_until', '0') ?? '0');
        if (Number.isFinite(retryAfter) && retryAfter > now()) return { state: 'mcp_quarantined' };
        // Do not spend a goal attempt when the MCP catalog is unavailable.
        let admission;
        try { admission = await tools.catalog(policy); }
        catch {
          store.metaSet?.('gateway_quarantine_until', now() + bounded(policy.gateway_backoff_seconds, 900, 300, 86400));
          return { state: 'mcp_quarantined' };
        }
        store.metaSet?.('gateway_quarantine_until', 0);
        let recent = [];
        try { recent = await ingest(); } catch { /* event outage never acknowledges or drops a cursor */ }
        // Catalog discovery and event intake are asynchronous. STOP, quota
        // quarantine or an administrator's limits may change while they run.
        // Recheck before charging a cycle or spending a goal attempt.
        const current = config.loadPolicy();
        if (!admitted(current)) return { state: config.paused() ? 'paused' : 'disabled' };
        if (current.allow_provider_requests !== true ||
            !Array.isArray(current.approved_provider_models) ||
            !current.approved_provider_models.includes(config.model)) return { state: 'provider_not_admitted' };
        const currentHealth = store.providerStatus?.('openai');
        if (currentHealth && currentHealth.state !== 'unverified')
          return { state: 'provider_quarantined', cause: currentHealth.cause };
        if (!store.budgetOk(current).okay) return { state: 'budget_exhausted' };
        if (store.cyclesLastHour() >= bounded(current.max_cycles_per_hour, 6, 1, 1000))
          return { state: 'cycle_limit' };
        claim = store.claimGoal({ policy: current });
        if (!claim) return { state: 'idle' };
        const receipt = await runGoal(claim, current, recent, admission);
        return { state: receipt.receipt_projection_error ? 'completed_receipt_failed' : 'completed', receipt };
      } catch (error) {
        const message = transport.redact(String(error.message || error));
        if (claim) {
          const provider_failure = error.provider_failure?.provider === 'openai' ? error.provider_failure : undefined;
          try { store.failRun({ run_id: claim.run.id, error: message, policy, ...(provider_failure ? { provider_failure } : {}) }); }
          catch (failure) {
            return { state: 'reconciliation_required', run_id: claim.run.id, error: message,
              reconciliation_error: transport.redact(String(failure.message || failure)) };
          }
        }
        return { state: 'failed', ...(claim ? { run_id: claim.run.id } : {}), error: message };
      } finally { transport.setDeadline?.(Infinity); busy = false; }
    },
  };
}

export function writeOperatorReceipt(config, receipt) {
  mkdirSync(config.receiptDir, { recursive: true });
  const path = join(config.receiptDir, `${receipt.run_id}.json`), temporary = `${path}.${randomUUID()}.tmp`;
  try { writeFileSync(temporary, `${JSON.stringify(receipt, null, 2)}\n`, { flag: 'wx', mode: 0o600 }); renameSync(temporary, path); }
  finally { if (existsSync(temporary)) unlinkSync(temporary); }
}

if (import.meta.main) {
  const config = operatorConfig();
  if (process.argv.includes('--inspect')) {
    const policy = config.loadPolicy();
    console.log(json({ schema: 'bonfyre.operator-runtime.inspect.v1', runtime: 'bun', paused: config.paused(),
      autonomous: policy.autonomous !== false, model: config.model, gateway: config.gateway,
      database: join(config.home, 'operator.sqlite3'), policy_sha256: sha(readFileSync(config.policyPath)),
      startup_seeds: false, startup_model_call: false }));
  } else {
    const { createOperatorStore } = await import('./operator_store.mjs');
    const { createOperatorTools } = await import('./operator_tools.mjs');
    const store = createOperatorStore({ home: config.home });
    const transport = createOperatorTransport(config);
    const tools = createOperatorTools({ mcpCall: transport.mcpCall, listTools: transport.listTools,
      recordAction: store.recordAction, completeAction: store.completeAction, now: () => Date.now() / 1000 });
    const daemon = createOperatorDaemon({ config, store, transport, tools, onReceipt: receipt => writeOperatorReceipt(config, receipt) });
    mkdirSync(config.home, { recursive: true });
    const lock = join(config.home, 'bun-daemon.lock'), identity = json({ pid: process.pid, nonce: randomUUID() });
    if (existsSync(lock)) {
      const prior = JSON.parse(readFileSync(lock, 'utf8'));
      if (!Number.isSafeInteger(prior.pid) || prior.pid < 2) throw new Error('invalid operator lock');
      // A live pid is not proof of a live daemon: after a reboot the number is
      // reused, and a lock naming macOS's familycircled kept this daemon in a
      // crash-restart loop for days, one stack trace per launchd retry.
      if (operatorDaemonAlive(prior.pid)) {
        console.error(`operator: another operator daemon (pid ${prior.pid}) owns ${lock}; leaving it be`);
        process.exit(0);
      }
      unlinkSync(lock);
    }
    const fd = openSync(lock, 'wx', 0o600); writeFileSync(fd, identity); closeSync(fd);
    let stopping = false;
    const stop = () => { stopping = true; };
    process.on('SIGTERM', stop); process.on('SIGINT', stop);
    try {
      if (process.argv.includes('--seed')) { store.seedGoals(); console.log('operator goals seeded'); }
      else {
        console.log(json({ event: 'operator.started', runtime: 'bun', pid: process.pid, paused: config.paused(), model: config.model }));
        do {
          const result = await daemon.once();
          if (!['paused', 'idle', 'disabled', 'cycle_limit', 'budget_exhausted'].includes(result.state))
            console.log(json({ event: 'operator.cycle', state: result.state, run_id: result.run_id || result.receipt?.run_id,
              error: result.error || result.receipt?.receipt_projection_error }));
          if (stopping || process.argv.includes('--once')) break;
          const delay = result.state === 'completed' ? 1 : config.paused() ? 5 : bounded(config.loadPolicy().idle_sleep_seconds, 10, 2, 3600);
          for (let tick = 0; tick < delay * 4 && !stopping; tick++) await Bun.sleep(250);
        } while (!stopping);
      }
    } finally {
      store.close();
      if (existsSync(lock) && readFileSync(lock, 'utf8') === identity) unlinkSync(lock);
    }
  }
}
