#!/usr/bin/env bun
// Local coding-agent hook bridge. Policy remains local authority; delivery to
// the broker is telemetry and never converts a denial into permission.
import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { homedir } from 'node:os';
import { join } from 'node:path';
import { projectRun } from './project_runtime.mjs';
import { createFabricEstate } from './fabric_estate.mjs';

const MAX_INPUT = 1_000_000;
const MAX_POLICY = 100_000;
const MAX_PROMPT = 1_000_000;
const EVENTS_WITHOUT_OUTPUT = new Set(['PostToolUse', 'PostToolUseFailure', 'SubagentStart', 'SubagentStop', 'SessionEnd', 'PostCompact']);
const object = value => value !== null && typeof value === 'object' && !Array.isArray(value);
const string = value => typeof value === 'string' ? value : '';
const hookOutput = (event, value) => ({ hookSpecificOutput: { hookEventName: event, ...value } });
function localEndpoint(value) {
  const url = new URL(value);
  if (url.protocol !== 'http:' || !['127.0.0.1', 'localhost', '[::1]'].includes(url.hostname)
    || url.username || url.password || url.search || url.hash || url.pathname !== '/')
    throw new Error('fabric hook requires local broker');
  return new URL('/hook', url).href;
}
function loadPolicy(path) {
  const bytes = readFileSync(path);
  if (bytes.length > MAX_POLICY) throw new Error('fabric hook policy too large');
  const value = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes));
  if (!object(value) || !Number.isSafeInteger(value.max_peer_depth) || value.max_peer_depth < 0 || value.max_peer_depth > 100
    || !Array.isArray(value.forbidden_command_patterns) || value.forbidden_command_patterns.length > 100
    || value.forbidden_command_patterns.some(pattern => typeof pattern !== 'string' || pattern.length > 1000))
    throw new Error('invalid fabric hook policy');
  return value;
}
function gitEnv(env) {
  const result = { ...env };
  for (const name of Object.keys(result)) if (name.startsWith('GIT_')) delete result[name];
  return { ...result, GIT_CONFIG_NOSYSTEM: '1', GIT_CONFIG_GLOBAL: '/dev/null', GIT_CONFIG_COUNT: '1',
    GIT_CONFIG_KEY_0: 'core.fsmonitor', GIT_CONFIG_VALUE_0: 'false' };
}
async function gitSummary(cwd, env, run) {
  try {
    const receipt = await run(['/usr/bin/git', '--no-optional-locks', 'status', '--short'],
      { cwd, env: gitEnv(env), timeoutMs: 8000, maxOutputBytes: 200000 });
    if (receipt.exit_code !== 0) return 'git status unavailable';
    const lines = receipt.stdout.split(/\r?\n/).filter(Boolean);
    return `dirty_count=${lines.length}\n${lines.slice(0, 30).join('\n')}`;
  } catch { return 'git status unavailable'; }
}
export function fabricEstateSummary(home) {
  try {
    return JSON.stringify(createFabricEstate({ home }).status(), null, 2).slice(-12_000);
  } catch { return 'estate status unavailable'; }
}
function commandFromInput(data) {
  const input = data.tool_input || data.input || {};
  if (!object(input)) return '';
  for (const key of ['command', 'cmd', 'script']) if (typeof input[key] === 'string') return input[key];
  return '';
}
function toolName(data) { return String(data.tool_name || data.toolName || data.tool || ''); }
function deny(reason, event) { return hookOutput(event, { permissionDecision: 'deny', permissionDecisionReason: reason }); }
function policyPattern(pattern) {
  // Installed policy JSON double-escapes regex backslashes. Correct the data
  // representation so \s means whitespace, rather than a literal backslash+s.
  return new RegExp(pattern.replaceAll('\\\\', '\\'), 'i');
}
// Hook telemetry carries identities and counters, never transcript bodies.
// Keep the policy decision on original data; only this projection is exported.
const EVIDENCE_EVENTS = new Set(['Setup','SessionStart','SessionEnd','UserPromptSubmit','PreToolUse',
  'PostToolUse','PostToolUseFailure','PreCompact','PostCompact','SubagentStart','SubagentStop','Stop']);
const EVIDENCE_IDS = ['session_id','thread_id','turn_id','run_id','request_id','message_id',
  'parent_session_id','agent_id','tool_use_id','tool_name','model'];
const EVIDENCE_COUNTS = ['input_tokens','cached_input_tokens','cache_read_input_tokens',
  'cache_creation_input_tokens','output_tokens','reasoning_output_tokens'];
export function hookEvidence(data) {
  const kind = object(data) ? data.kind || data.hook_event_name : null;
  if (!object(data) || !['claude','codex'].includes(data.source)
      || !EVIDENCE_EVENTS.has(kind)) return null;
  const evidence = { source: data.source, kind };
  for (const key of EVIDENCE_IDS)
    if (typeof data[key] === 'string' && /^[a-zA-Z0-9_.:-]{1,160}$/.test(data[key]))
      evidence[key] = data[key];
  if (object(data.usage)) {
    const counts = {};
    for (const key of EVIDENCE_COUNTS)
      if (Number.isSafeInteger(data.usage[key]) && data.usage[key] >= 0)
        counts[key] = data.usage[key];
    if (Object.keys(counts).length) evidence.usage = counts;
  }
  return evidence;
}
async function postEvent(data, { token, home, env, request }) {
  let secret = token;
  if (secret === undefined) {
    try { secret = readFileSync(env.BONFYRE_MCP_TOKEN_FILE || join(home, '.bonfyre/mcp/token'), 'utf8').trim(); }
    catch { return; }
  }
  if (!secret) return;
  try {
    const url = localEndpoint(env.BONFYRE_FABRIC_URL || 'http://127.0.0.1:3053');
    const response = await request(url, { method: 'POST', redirect: 'error', signal: AbortSignal.timeout(2000),
      headers: { 'content-type': 'application/json', authorization: `Bearer ${secret}` }, body: JSON.stringify(data) });
    await response.body?.cancel();
  } catch { /* Hook policy must not depend on telemetry availability. */ }
}

export async function handleFabricHook(raw, { home = homedir(), cwd = process.cwd(), env = process.env,
  token, request = fetch, run = projectRun, estateSummary = fabricEstateSummary } = {}) {
  if (typeof raw !== 'string' || Buffer.byteLength(raw) > MAX_INPUT) throw new Error('hook input too large');
  let data;
  try { data = raw.trim() ? JSON.parse(raw) : {}; }
  catch { data = { raw }; }
  if (!object(data)) throw new Error('hook input must be an object');
  const event = String(data.hook_event_name || data.event_name || data.event || '');
  data = { ...data, source: Object.hasOwn(data, 'turn_id') ? 'codex' : 'claude', kind: event };
  const evidence = hookEvidence(data);
  if (evidence) await postEvent(evidence, { token, home, env, request });
  if (event === 'SessionStart' || event === 'Setup') {
    const git = await gitSummary(string(data.cwd) || cwd, env, run);
    const estate = estateSummary(home);
    return hookOutput(event, { additionalContext: 'Bonfyre dual-agent fabric is active. Use the Bonfyre MCP for live project/Oracle evidence. '
      + 'Use the dual-agent fabric for cross-model duels, relays, reviews, and verification. '
      + 'Do not recursively invoke a peer from inside a peer child session.\n\nCURRENT PROJECT\n'
      + git + '\n\nESTATE\n' + estate });
  }
  if (event === 'UserPromptSubmit') {
    const prompt = string(data.prompt || data.user_prompt);
    if (Buffer.byteLength(prompt) > MAX_PROMPT) throw new Error('hook prompt too large');
    const mission = prompt ? createHash('sha256').update(prompt).digest('hex').slice(0, 16) : 'unknown';
    return hookOutput('UserPromptSubmit', { additionalContext: `Bonfyre mission fingerprint: ${mission}. For complex or cross-plane work, compile an estate mission and use a Claude/Codex duel before implementation.` });
  }
  if (event === 'PreToolUse') {
    let policy;
    try { policy = loadPolicy(env.BONFYRE_FABRIC_POLICY || join(home, '.bonfyre/dev-fabric/fabric_policy.json')); }
    catch { return deny('Bonfyre fabric policy unavailable; tool use refused.', event); }
    const command = commandFromInput(data), name = toolName(data);
    const depth = Number(env.BONFYRE_PEER_DEPTH || 0);
    if (!Number.isSafeInteger(depth) || depth < 0) return deny('Invalid peer depth.', event);
    if (depth >= policy.max_peer_depth && ['bonfyre-dev-fabric', 'claude-code', 'codex-code'].some(token => name.toLowerCase().includes(token)))
      return deny('Peer recursion is blocked. Return evidence to the parent agent instead.', event);
    try {
      for (const pattern of policy.forbidden_command_patterns) if (command && policyPattern(pattern).test(command))
        return deny(`Blocked by Bonfyre fabric policy: ${pattern}`, event);
    } catch { return deny('Invalid Bonfyre fabric pattern; tool use refused.', event); }
    return null;
  }
  if (EVENTS_WITHOUT_OUTPUT.has(event)) return null;
  if (event === 'PreCompact') return hookOutput(event, { additionalContext: 'Preserve the active Bonfyre mission fingerprint, peer handoff state, changed paths, tests, live verification, receipts, and unresolved blockers through compaction.' });
  if (event === 'Stop') {
    if (data.stop_hook_active === true) return null;
    const summary = await gitSummary(string(data.cwd) || cwd, env, run);
    const dirty = Number(summary.match(/dirty_count=(\d+)/)?.[1] || 0);
    const last = string(data.last_assistant_message).toLowerCase();
    if (dirty && !['dual verification', 'cross-review', 'tests passed', 'live verification', 'receipt'].some(term => last.includes(term)))
      return { decision: 'block', reason: 'Repository changes are present without explicit peer review and verification. Run $bonfyre-cross-review or /bonfyre-cross-review, execute targeted tests, and record live verification before stopping.' };
  }
  return null;
}

export async function runFabricHookCli({ input = Bun.stdin, output = text => process.stdout.write(text), ...options } = {}) {
  let raw;
  try {
    const reader = input.stream().getReader(), chunks = [];
    let size = 0;
    try {
      while (true) {
        const { value, done } = await reader.read();
        if (done) break;
        size += value.byteLength;
        if (size > MAX_INPUT) throw new Error('hook input too large');
        chunks.push(value);
      }
    } finally { await reader.cancel().catch(() => {}); reader.releaseLock(); }
    raw = new TextDecoder('utf-8', { fatal: true }).decode(Buffer.concat(chunks));
  } catch { output(`${JSON.stringify(deny('Bonfyre hook input unavailable; tool use refused.', 'PreToolUse'))}\n`); return 1; }
  try {
    const result = await handleFabricHook(raw, options);
    if (result !== null) output(`${JSON.stringify(result)}\n`);
    return 0;
  } catch { output(`${JSON.stringify(deny('Bonfyre hook failed; tool use refused.', 'PreToolUse'))}\n`); return 1; }
}

if (import.meta.main) process.exitCode = await runFabricHookCli();
