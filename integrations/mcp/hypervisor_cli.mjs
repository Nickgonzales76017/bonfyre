#!/usr/bin/env bun
// One parser over the MCP operation contract, not another command implementation.
import { readFileSync } from 'node:fs';
import { homedir } from 'node:os';
import { join } from 'node:path';
import { createOwnedMcpCall, mcpEndpoint } from './owned_mcp_client.mjs';
import { validateArguments } from './http_boundary.mjs';
import { HYPERVISOR_TOOLS, HYPERVISOR_VERSION } from './hypervisor_server.mjs';

const COMMANDS = Object.freeze({
  status: ['hypervisor_status'], discover: ['ecosystem_discover'], providers: ['ecosystem_providers'], replay: ['replay_verify'],
  route: ['capability_route', 'capability'], 'adapter-status': ['provider_adapters_status'], 'memory-search': ['memory_search', 'query'],
  compact: ['compact_command', 'command'], 'bernstein-compile': ['bernstein_manifest_compile', 'mission_id'],
  'bernstein-validate': ['bernstein_manifest_validate', 'manifest_path'], 'fleet-status': ['fleet_status'],
  'fleet-launch': ['fleet_launch', 'path', 'title', 'command', 'message'], 'fleet-action': ['fleet_session_action', 'action', 'session'],
  'caveman-code': ['caveman_code_run', 'prompt'], 'invoker-submit': ['invoker_submit', 'workflow_path'],
  'task-execute': ['task_execute', 'mission_id', 'task_id'], absorb: ['source_absorb', 'repo'], updates: ['source_update_check'],
  compile: ['mission_compile', 'goal'], 'mission-route': ['mission_route', 'mission_id'], execute: ['mission_execute', 'mission_id'],
  mission: ['mission_status', 'mission_id'], backprop: ['mission_backpropagate', 'mission_id'], context: ['context_compile', 'mission_id'],
  evaluate: ['provider_evaluate', 'provider_id'], compare: ['provider_compare', 'capability'], promote: ['provider_promote', 'provider_id', 'capability'],
  quarantine: ['provider_quarantine', 'provider_id', 'reason'], 'approve-license': ['provider_license_approve', 'provider_id', 'note'],
  'install-plan': ['install_plan', 'profile'], 'session-register': ['session_register', 'provider_id', 'native_session_id'],
  sessions: ['session_list'], handoff: ['session_handoff', 'session_id', 'target_provider'],
});
const FLAGS = Object.freeze({ constraint: 'constraints', capability: 'requested_capabilities', 'cost-usd': 'budget_cost_usd',
  tokens: 'budget_tokens', shadow: 'include_shadow', test: 'execute_tests', provider: 'provider_id',
  mission: 'mission_id', parent: 'parent_session_id', mcp: 'mcps', goal: 'goal_mode', extended: 'execute_extended', timeout: 'timeout' });
const toolsByName = new Map(HYPERVISOR_TOOLS.map(tool => [tool.name, tool]));
const usageError = message => Object.assign(new Error(message), { code: 'CLI_USAGE' });
const wantsHelp = argv => argv.slice(0, argv.includes('--') ? argv.indexOf('--') : argv.length).some(value => ['--help', '-h'].includes(value));
const delay = (ms, signal) => new Promise((resolve, reject) => {
  signal?.throwIfAborted(); const done = () => { clearTimeout(timer); signal?.removeEventListener('abort', abort); };
  const abort = () => { done(); reject(signal.reason); };
  const timer = setTimeout(() => { done(); resolve(); }, ms); signal?.addEventListener('abort', abort, { once: true });
});
function parsed(argv) {
  if (!Array.isArray(argv) || argv.length > 1000 || argv.some(value => typeof value !== 'string' || value.includes('\0') || value.length > 1000000)) throw new Error('invalid command arguments');
  const [command, ...rest] = argv, descriptor = Object.hasOwn(COMMANDS, command) && COMMANDS[command];
  if (!descriptor) throw new Error(`usage: bonfyre-hypervisor ${Object.keys(COMMANDS).join('|')}`);
  const [name, ...positional] = descriptor, schema = toolsByName.get(name).inputSchema, properties = schema.properties;
  const args = {}; let position = 0, positionalOnly = false;
  for (let i = 0; i < rest.length; i++) {
    let argument = rest[i];
    if (!positionalOnly && argument === '--') { positionalOnly = true; continue; }
    if (positionalOnly || !argument.startsWith('--')) {
      const key = positional[position++]; if (!key || Object.hasOwn(args, key)) throw new Error('unexpected positional argument'); args[key] = argument; continue;
    }
    const equalAt = argument.indexOf('=');
    const attached = equalAt >= 0 ? argument.slice(equalAt + 1) : undefined;
    if (equalAt >= 0) argument = argument.slice(0, equalAt);
    const negative = argument.startsWith('--no-'), flag = argument.slice(negative ? 5 : 2);
    let key = FLAGS[flag] ?? flag.replaceAll('-', '_');
    if (flag === 'capability' && name !== 'mission_compile') key = 'capability';
    if (flag === 'provider' && name === 'provider_compare') key = 'providers';
    if (flag === 'parent' && name === 'fleet_launch') key = 'parent';
    if (!Object.hasOwn(properties, key)) throw new Error(`unsupported option: ${argument}`);
    const type = properties[key].type;
    if (Object.hasOwn(args, key) && type !== 'array') throw new Error(`duplicate option: ${argument}`);
    if (type === 'boolean') { if (attached !== undefined) throw new Error(`boolean option does not accept a value: ${argument}`); args[key] = !negative; continue; }
    if (negative || attached === undefined && (i + 1 >= rest.length || rest[i + 1].startsWith('--'))) throw new Error(`missing option value: ${argument}`);
    const value = attached ?? rest[++i];
    if (type === 'array') (args[key] ??= []).push(value);
    else if (type === 'integer' || type === 'number') {
      if (!(type === 'integer' ? /^[+-]?\d+$/ : /^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:e[+-]?\d+)?$/i).test(value)) throw new Error(`invalid number: ${argument}`);
      const number = Number(value);
      if (!Number.isFinite(number) || type === 'integer' && !Number.isSafeInteger(number)) throw new Error(`invalid number: ${argument}`);
      args[key] = number;
    } else if (type === 'object') { args[key] = JSON.parse(value); }
    else args[key] = value;
  }
  for (const key of schema.required ?? []) if (!Object.hasOwn(args, key)) throw new Error(`missing argument: ${key}`);
  validateArguments(args, schema);
  return { name, args };
}

export async function waitHypervisorMission(missionId, { call, signal, emit = () => {}, interval = 5, timeout = 86400,
  maxPolls = 20000, now = () => performance.now(), sleep = delay } = {}) {
  if (typeof missionId !== 'string' || !missionId || missionId.length > 512 || typeof call !== 'function') throw usageError('invalid mission wait identity');
  if (!Number.isFinite(interval) || interval < 0.01 || interval > 60 || !Number.isFinite(timeout) || timeout <= 0 || timeout > 86400 ||
      !Number.isSafeInteger(maxPolls) || maxPolls < 1 || maxPolls > 100000) throw usageError('wait exceeds interval/deadline/poll budget');
  const deadline = now() + timeout * 1000, terminal = new Set(['resolved', 'blocked', 'retry', 'failed', 'cancelled']);
  let lastState;
  for (let count = 0; count < maxPolls; count++) {
    signal?.throwIfAborted(); const remaining = deadline - now();
    if (remaining <= 0) throw Object.assign(new Error('mission wait deadline exceeded; execution was not repeated or cancelled'), { code: 'CLI_TIMEOUT' });
    const timeoutMs = Math.max(1, Math.min(120000, Math.floor(remaining))), controller = new AbortController();
    const abort = () => controller.abort(signal.reason); signal?.addEventListener('abort', abort, { once: true });
    let rejectPoll;
    const interrupted = new Promise((_, reject) => { rejectPoll = reject; });
    const onAbort = () => rejectPoll(controller.signal.reason); controller.signal.addEventListener('abort', onAbort, { once: true });
    const timer = setTimeout(() => controller.abort(Object.assign(new Error('mission wait poll deadline exceeded'), { code: 'CLI_TIMEOUT' })), timeoutMs);
    let value;
    try { value = await Promise.race([Promise.resolve().then(() => { controller.signal.throwIfAborted(); return call('mission_status', { mission_id: missionId }, { signal: controller.signal, timeoutMs }); }), interrupted]); }
    finally { clearTimeout(timer); signal?.removeEventListener('abort', abort); controller.signal.removeEventListener('abort', onAbort); }
    signal?.throwIfAborted();
    if (!value || typeof value !== 'object' || typeof value.state !== 'string' ||
        !terminal.has(value.state) && !['compiled', 'planned', 'queued', 'running'].includes(value.state) ||
        value.mission_id !== undefined && value.mission_id !== missionId || value.mission?.id !== undefined && value.mission.id !== missionId ||
        value.tasks !== undefined && (!Array.isArray(value.tasks) || value.tasks.length > 10000 || value.tasks.some(row => !row || typeof row.id !== 'string' || typeof row.state !== 'string')))
      throw new Error('invalid mission status while waiting');
    if (now() >= deadline) throw Object.assign(new Error('mission wait deadline exceeded'), { code: 'CLI_TIMEOUT' });
    if (value.state !== lastState) { emit({ mission_id: missionId, state: value.state, tasks: Object.fromEntries((value.tasks || []).map(row => [row.id, row.state])) }); lastState = value.state; }
    if (terminal.has(value.state)) return value;
    await sleep(Math.min(interval * 1000, deadline - now()), signal);
  }
  throw Object.assign(new Error('mission wait poll budget exceeded; execution was not repeated or cancelled'), { code: 'CLI_TIMEOUT' });
}

function waitOptions(argv) {
  const rest = [], options = {}; let wait = false;
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i]; if (arg === '--') { rest.push(...argv.slice(i)); break; }
    if (arg === '--wait') { if (wait) throw usageError('duplicate --wait'); wait = true; continue; }
    const match = /^--(interval|wait-timeout)(?:=(.*))?$/.exec(arg);
    if (match) { const key = match[1] === 'interval' ? 'interval' : 'timeout'; if (Object.hasOwn(options, key)) throw usageError(`duplicate ${arg}`);
      const value = match[2] ?? argv[++i]; if (typeof value !== 'string' || !/^(?:\d+(?:\.\d*)?|\.\d+)$/.test(value)) throw usageError(`invalid ${arg}`);
      options[key] = Number(value); continue; }
    rest.push(arg);
  }
  return { rest, options, wait };
}

export async function hypervisorCli(argv, { call, events, signal, emit = () => {}, now, sleep } = {}) {
  if (!Array.isArray(argv) || argv.some(value => typeof value !== 'string')) throw usageError('invalid argv');
  if (argv.length === 1 && ['version', '--version'].includes(argv[0])) return `bonfyre-hypervisor ${HYPERVISOR_VERSION}`;
  if (!argv.length) throw usageError('a command is required; use --help');
  if (wantsHelp(argv)) return `usage: bonfyre-hypervisor ${[...Object.keys(COMMANDS), 'wait', 'events'].join('|')}\nwait MISSION [--interval 5] [--wait-timeout 86400]; execute MISSION [--wait]`;
  if (argv[0] === 'events') {
    const args = { after: 0, limit: 500 };
    const seen = new Set();
    for (let i = 1; i < argv.length; i++) {
      const match = /^--(after|limit)(?:=(.*))?$/.exec(argv[i]); if (!match || seen.has(match[1])) throw usageError('invalid events option'); seen.add(match[1]);
      const value = match[2] ?? argv[++i]; if (!/^\d+$/.test(value ?? '')) throw usageError('invalid events integer'); args[match[1]] = Number(value);
    }
    if (!Number.isSafeInteger(args.after) || args.after < 0 || !Number.isSafeInteger(args.limit) || args.limit < 1 || args.limit > 2000) throw usageError('invalid events bounds');
    if (typeof events !== 'function') throw Error('missing authenticated events owner'); return events(args, { signal, timeoutMs: 120000 });
  }
  let command = argv, waiting;
  if (['wait', 'execute'].includes(argv[0])) {
    waiting = waitOptions(argv.slice(1));
    if (argv[0] === 'wait') {
      if (waiting.rest.length !== 1 || waiting.rest[0].startsWith('--') || waiting.wait) throw usageError('usage: wait MISSION [--interval N] [--wait-timeout N]');
      return waitHypervisorMission(waiting.rest[0], { call, signal, emit, now, sleep, ...waiting.options });
    }
    if (!waiting.wait && Object.keys(waiting.options).length) throw usageError('wait options require --wait');
    command = ['execute', ...waiting.rest];
    // Reject invalid wait configuration before the execution effect is sent.
    const { interval = 5, timeout = 86400 } = waiting.options;
    if (interval < 0.01 || interval > 60 || timeout <= 0 || timeout > 86400) throw usageError('invalid wait bounds');
  }
  let name, args;
  try { ({ name, args } = parsed(command)); } catch (error) { throw usageError(error.message); }
  if (typeof call !== 'function') throw new Error('missing authenticated hypervisor owner');
  const timeouts = { memory_search: 300000, bernstein_manifest_validate: 300000, fleet_status: 300000, fleet_launch: 600000,
    fleet_session_action: 600000, source_absorb: 1200000, source_update_check: 600000, provider_evaluate: 1200000,
    caveman_code_run: 3600000, invoker_submit: 3600000, task_execute: 3600000 };
  const timeoutMs = name === 'compact_command' ? Math.min(3600000, ((args.timeout ?? 1800) + 60) * 1000) : timeouts[name] ?? 120000;
  signal?.throwIfAborted(); const value = await call(name, args, { signal, timeoutMs });
  if (waiting?.wait) { emit(value); return waitHypervisorMission(args.mission_id, { call, signal, emit, now, sleep, ...waiting.options }); }
  return value;
}
export const hypervisorExitCode = (argv, value) => argv[0] === 'replay' && value?.verified !== true || argv[0] === 'task-execute' && !['passed', 'completed', 'resolved'].includes(value?.state) ? 1 : 0;

export function createHypervisorCliClient({ base, token, request = fetch }) {
  const endpoint = mcpEndpoint(`${base.replace(/\/$/, '')}/mcp`), url = new URL(endpoint);
  const call = createOwnedMcpCall({ endpoint, token, tools: HYPERVISOR_TOOLS.map(tool => tool.name), request, maxBytes: 20_000_000 });
  async function events(args, { signal, timeoutMs = 120000 } = {}) {
    if (!Number.isSafeInteger(args?.after) || args.after < 0 || !Number.isSafeInteger(args?.limit) || args.limit < 1 || args.limit > 2000 ||
        !Number.isSafeInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 120000) throw usageError('invalid events request bounds');
    signal?.throwIfAborted(); const controller = new AbortController(), abort = () => controller.abort(signal.reason);
    signal?.addEventListener('abort', abort, { once: true });
    let timer, reader, rejectAbort;
    const interrupted = new Promise((_, reject) => { rejectAbort = reject; });
    const onAbort = () => rejectAbort(new Error('events request cancelled or timed out'));
    controller.signal.addEventListener('abort', onAbort, { once: true });
    timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const target = new URL(url); target.pathname = target.pathname.replace(/\/mcp$/, '/events'); target.search = new URLSearchParams({ after: String(args.after), limit: String(args.limit) }).toString();
      const pending = request(target.href, { redirect: 'error', signal: controller.signal, headers: { authorization: `Bearer ${token}` } });
      Promise.resolve(pending).then(res => { if (controller.signal.aborted) res.body?.cancel().catch(() => {}); }).catch(() => {});
      const res = await Promise.race([pending, interrupted]);
      const declared = res.headers.get('content-length');
      if (!res.ok || !res.body || declared !== null && (!/^\d+$/.test(declared) || Number(declared) > 20_000_000)) { res.body?.cancel().catch(() => {}); throw Error('events owner response failed'); }
      reader = res.body.getReader(); const chunks = []; let size = 0;
      for (;;) { const part = await Promise.race([reader.read(), interrupted]); if (part.done) break;
        size += part.value.byteLength; if (size > 20_000_000) throw Error('events response byte budget exceeded'); chunks.push(part.value); }
      const value = JSON.parse(new TextDecoder('utf8', { fatal: true }).decode(Buffer.concat(chunks, size)));
      if (!value || !Array.isArray(value.events) || value.events.length > args.limit || !Number.isSafeInteger(value.next_after) ||
          value.next_after !== (value.events.at(-1)?.seq ?? args.after) || value.events.some((row, i) => !Number.isSafeInteger(row?.seq) || row.seq <= (i ? value.events[i - 1].seq : args.after))) throw Error('invalid events response');
      return value;
    } catch (error) { throw new Error(String(error.message).replaceAll(token, '[redacted]').slice(0, 2000));
    } finally { clearTimeout(timer); signal?.removeEventListener('abort', abort); controller.signal.removeEventListener('abort', onAbort); reader?.cancel().catch(() => {}); }
  }
  return { call, events };
}
if (import.meta.main) {
  const controller = new AbortController(), stop = () => controller.abort(new Error('CLI cancelled; remote work is not replayed or implicitly cancelled'));
  process.once('SIGINT', stop); process.once('SIGTERM', stop);
  try {
    const argv = process.argv.slice(2); let client;
    const getClient = () => {
      if (client) return client;
      const endpoint = mcpEndpoint(`${(process.env.BONFYRE_HYPERVISOR_URL || 'http://127.0.0.1:3054').replace(/\/$/, '')}/mcp`);
      if (!['127.0.0.1', 'localhost', '[::1]'].includes(new URL(endpoint).hostname) && !process.env.BONFYRE_HYPERVISOR_TOKEN_FILE)
        throw new Error('remote hypervisor requires a dedicated credential file');
      const token = readFileSync(process.env.BONFYRE_HYPERVISOR_TOKEN_FILE || process.env.BONFYRE_MCP_TOKEN_FILE || join(homedir(), '.bonfyre/mcp/token'), 'utf8').trim();
      return client = createHypervisorCliClient({ base: endpoint.replace(/\/mcp$/, ''), token });
    };
    const result = await hypervisorCli(argv, { call: (...args) => getClient().call(...args), events: (...args) => getClient().events(...args),
      signal: controller.signal, emit: value => console.log(JSON.stringify(value, null, 2)) });
    console.log(typeof result === 'string' ? result : JSON.stringify(result, null, 2)); process.exitCode = hypervisorExitCode(argv, result);
  } catch (error) { console.error(error.message); process.exitCode = controller.signal.aborted ? 130 : error.code === 'CLI_USAGE' ? 2 : error.code === 'CLI_TIMEOUT' ? 124 : 1; }
  finally { process.removeListener('SIGINT', stop); process.removeListener('SIGTERM', stop); }
}
