// Authenticated operator actions, not a sandbox for untrusted project code.
import { createHash, randomUUID } from 'node:crypto';
import { mkdirSync, openSync, writeFileSync, closeSync, renameSync, unlinkSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { nativeExecutionPlan, findProbeExecutable, nativeProbeReceipt, nativeProbePassed } from './native_probe.mjs';
import { inspectCanaryProviderExecutable } from './canary_fixtures.mjs';
import { projectRun } from './project_runtime.mjs';
import { readSpinesBytes, spinesOwnedPath } from './spines_store.mjs';
import { compileTaskPlan } from './mission_plan.mjs';

const sha = value => createHash('sha256').update(value).digest('hex');
function text(value, name, max = 65536) {
  if (typeof value !== 'string' || !value.length || value.includes('\0') || Buffer.byteLength(value) > max) throw Error(`invalid ${name}`);
  return value;
}
function argument(value, name) { text(value, name); if (value.startsWith('-')) throw Error(`${name} cannot begin with an option`); return value; }
function integer(value, name, max) { if (!Number.isSafeInteger(value) || value < 1 || value > max) throw Error(`invalid ${name}`); return value; }
function identifier(value) { if (typeof value !== 'string' || !/^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$/.test(value)) throw Error('invalid workflow identifier'); return value; }
export function splitAdapterCommand(command) {
  text(command, 'command'); const tokens = []; let token = '', quote = '', escaped = false, present = false;
  for (const char of command) {
    if (escaped) { token += char; escaped = false; present = true; continue; }
    if (char === '\\' && quote !== "'") { escaped = true; present = true; continue; }
    if (quote) { if (char === quote) quote = ''; else token += char; present = true; continue; }
    if (char === '"' || char === "'") { quote = char; present = true; continue; }
    if (/[;|&<>`$\n\r]/.test(char)) throw Error('shell operators are not supported');
    if (/\s/.test(char)) { if (present) tokens.push(token); token = ''; present = false; } else { token += char; present = true; }
  }
  if (quote || escaped) throw Error('unterminated command quoting');
  if (present) tokens.push(token);
  if (!tokens.length || tokens.length > 128) throw Error('invalid command token count');
  return tokens;
}
export function compactAdapterArgv(command) {
  const tokens = splitAdapterCommand(command), [name, action] = tokens;
  const allowed = {
    git: ['status', 'diff', 'log', 'show', 'branch', 'rev-parse', 'ls-files'],
    gh: ['status'], docker: ['ps', 'images', 'stats', 'version'],
    kubectl: ['get', 'describe', 'version'], cargo: ['check', 'test', 'build', 'clippy', 'fmt'],
    go: ['test', 'build', 'vet', 'fmt'], npm: ['test'], pnpm: ['test'],
  };
  if (allowed[name]?.includes(action)) {
    if (tokens.some(value => /^--(?:exec|ext|textconv|upload|output|config|kubeconfig|context)/.test(value) || /^-[cC]/.test(value))) throw Error('command escape option refused');
    if (name === 'git' && action === 'branch' && tokens.slice(2).some(value => !['--list', '-a', '--all', '-r', '--remotes', '-v', '-vv'].includes(value))) throw Error('only branch listing is supported');
    return ['rtk', ...tokens];
  }
  if (['cat', 'head', 'tail'].includes(name) && tokens.length > 1) return ['rtk', 'read', ...tokens.slice(1)];
  if (['rg', 'grep'].includes(name) && !tokens.some(value => /^(?:--pre|--pre-glob)(?:=|$)/.test(value))) return ['rtk', 'grep', ...tokens.slice(1)];
  if (['find', 'fd'].includes(name) && !tokens.some(value => /^(?:-exec|-execdir|-ok|-okdir|-delete|-fprint|-fprintf|-fls|--exec|--exec-batch|-x|-X)(?:=|$)/.test(value))) return ['rtk', 'find', ...tokens.slice(1)];
  throw Error('command is outside the compact-command allowlist');
}

export function createHypervisorAdapters({ store, home = store?.home, root, run = projectRun, env = process.env, checkStop = () => {} } = {}) {
  if (!store || !home || !root) throw Error('store, home and root are required');
  home = resolve(home); root = resolve(root); env = Object.freeze({ ...env });
  const check = signal => { signal?.throwIfAborted(); checkStop(); };
  const owned = path => { const absolute = resolve(path); try { return spinesOwnedPath(root, absolute); } catch { return spinesOwnedPath(home, absolute); } };
  const source = path => { const actual = owned(text(path, 'source path')); readSpinesBytes(actual, 8 * 1024 * 1024); return actual; };
  function admission(command) {
    const admitted = nativeExecutionPlan(command, { env }); if (admitted) return admitted;
    if (command === 'bernstein') {
      const owner = inspectCanaryProviderExecutable(command, env.PATH);
      if (owner.kind === 'python_launcher') return { argv: [owner.path], boundary: {
        provider: command, kind: 'declared-third-party-python-cli', launcher: owner.path, launcher_sha256: owner.sha256,
        interpreter: owner.interpreter || 'upstream launcher interpreter',
        reason: 'Actual upstream Bernstein scheduler; no native Bernstein implementation is present.' } };
    }
    throw Error(`provider executable missing or not admitted: ${command}`);
  }
  async function execute(provider, capability, argv, { cwd = root, timeout = 60, signal, effect = false } = {}) {
    check(signal); integer(timeout, 'timeout seconds', 3600); cwd = owned(cwd);
    const prefix = admission(argv[0]), actual = [...prefix.argv, ...argv.slice(1)];
    actual.forEach(value => text(value, 'argv value'));
    const attempt = randomUUID(), payload = { attempt, command: argv[0], argv_sha256: sha(JSON.stringify(actual)),
      timeout_seconds: timeout, ...(prefix.boundary ? { runtime_boundary: prefix.boundary } : {}) };
    if (effect) store.writeReceipt({ provider_id: provider, capability_id: capability, state: 'started', payload });
    const controller = new AbortController(); let stopped;
    const abort = () => controller.abort(signal.reason); signal?.addEventListener('abort', abort, { once: true });
    const timer = setInterval(() => { try { check(signal); } catch (error) { stopped = error; controller.abort(); } }, 50); timer.unref();
    let result;
    try {
      check(signal);
      // Remove ambient Git indirections and executable configuration. This does
      // not sandbox trusted build/test scripts selected by the operator.
      const childEnv = Object.fromEntries(Object.entries(env).filter(([key]) => !key.startsWith('GIT_')));
      Object.assign(childEnv, { GIT_CONFIG_NOSYSTEM: '1', GIT_CONFIG_GLOBAL: '/dev/null', GIT_CONFIG_COUNT: '2',
        GIT_CONFIG_KEY_0: 'core.fsmonitor', GIT_CONFIG_VALUE_0: 'false', GIT_CONFIG_KEY_1: 'core.hooksPath', GIT_CONFIG_VALUE_1: '/dev/null' });
      result = await run(actual, { cwd, env: childEnv, timeoutMs: timeout * 1000, maxOutputBytes: 2 * 1024 * 1024, signal: controller.signal });
    } catch { result = { exit_code: 127, stdout: '', stderr: '', execution_error: true }; }
    finally { clearInterval(timer); signal?.removeEventListener('abort', abort); }
    // Prompts, queries and session messages can be private. The receipt binds
    // the argument digest, never the raw argv echoed by the process runner.
    const evidence = nativeProbeReceipt([argv[0]], result), passed = nativeProbePassed(evidence) && !result.execution_error && !stopped && !signal?.aborted;
    const state = passed ? 'passed' : effect ? 'outcome_uncertain' : 'failed';
    // A STOP may prevent terminal persistence; the durable started receipt then
    // remains explicit uncertainty rather than falsely recording rollback.
    if (effect) store.writeReceipt({ provider_id: provider, capability_id: capability, state, payload: { ...payload, evidence } });
    if (stopped) throw stopped; check(signal);
    return { ...result, ...(prefix.boundary ? { runtime_boundary: prefix.boundary } : {}), execution_state: state };
  }
  const response = (provider, capability, result, extra = {}) => ({ provider, capability, state: result.execution_state, ...extra, result });
  async function status({ signal } = {}) {
    const entries = [['cavemem', ['cavemem', 'status']], ['rtk', ['rtk', 'gain']], ['bernstein', ['bernstein', 'workflow', 'list']],
      ['agent-deck', ['agent-deck', 'status', '-json']], ['caveman-code', ['caveman-code', '--version']], ['invoker', ['invoker-cli', '--version']]];
    const result = {};
    for (const [provider, argv] of entries) {
      check(signal); const path = findProbeExecutable(argv[0], env);
      if (!path) { result[provider] = { installed: false, state: 'unavailable' }; continue; }
      try { const raw = await execute(provider, 'provider.probe', argv, { signal, timeout: 30 });
        result[provider] = { installed: true, path, state: raw.execution_state === 'passed' ? 'reachable' : 'degraded', probe: { ...nativeProbeReceipt(argv, raw), runtime_boundary: raw.runtime_boundary } };
      } catch { check(signal); result[provider] = { installed: true, path, state: 'blocked', reason: 'executable not admitted' }; }
    }
    return result;
  }
  async function memorySearch(query, { limit = 12, semantic = true, signal } = {}) {
    argument(query, 'query'); integer(limit, 'limit', 1000); if (typeof semantic !== 'boolean') throw Error('semantic must be boolean');
    return response('cavemem', 'memory.search', await execute('cavemem', 'memory.search', ['cavemem', 'search', query, '--limit', String(limit), ...semantic ? [] : ['--no-semantic']], { timeout: 90, signal }));
  }
  async function memorySessions({ limit = 50, signal } = {}) {
    integer(limit, 'limit', 1000); return response('cavemem', 'memory.timeline', await execute('cavemem', 'memory.timeline', ['cavemem', 'status'], { signal }), { limit, limitation: 'Provider status only; not a session timeline enumeration.' });
  }
  async function compactCommand(command, { cwd = root, timeout = 1800, signal } = {}) {
    const argv = compactAdapterArgv(command);
    // RTK delegates these families through PATH. Validate the delegated owner,
    // not merely the compressor executable, before any durable attempt.
    if (!['read', 'grep', 'find'].includes(argv[1])) admission(argv[1]);
    return response('rtk', 'context.tool_compression', await execute('rtk', 'context.tool_compression', argv, { cwd, timeout, signal, effect: true }), { original_command: command, adapter_argv: argv });
  }
  function bernsteinCompile(mission, { task_command } = {}) {
    check(); const id = identifier(mission?.id); compileTaskPlan(mission.tasks);
    text(task_command, 'task command template');
    if (/\{(?!mission_id\}|task_id\})/.test(task_command)) throw Error('unknown task command placeholder');
    const ids = new Set(), lines = [`name: bonfyre-${id}`, `description: ${JSON.stringify(`Bonfyre hypervisor mission ${id}`)}`, 'version: "1.0.0"', 'nodes:'];
    let manifestBytes = Buffer.byteLength(lines.join('\n')) + 1;
    for (const task of mission.tasks) {
      const taskId = identifier(task.id), lowered = taskId.toLowerCase(); if (ids.has(lowered)) throw Error('case-insensitive task collision'); ids.add(lowered);
      const node = [`  - id: ${JSON.stringify(lowered)}`];
      if (task.dependencies?.length) node.push(`    depends_on: ${JSON.stringify(task.dependencies.map(item => identifier(item).toLowerCase()))}`);
      node.push(`    command: ${JSON.stringify(task_command.replaceAll('{mission_id}', id).replaceAll('{task_id}', taskId))}`, '    timeout_seconds: 3600');
      manifestBytes += Buffer.byteLength(node.join('\n')) + 1;
      if (manifestBytes > 8 * 1024 * 1024) throw Error('workflow manifest byte budget exceeded');
      lines.push(...node);
    }
    const path = owned(join(root, '.bernstein', 'workflows', `bonfyre-${id}.yaml`)), manifest = `${lines.join('\n')}\n`;
    check(); mkdirSync(dirname(path), { recursive: true }); owned(path);
    const temp = `${path}.${randomUUID()}.tmp`; let fd;
    try { fd = openSync(temp, 'wx', 0o600); writeFileSync(fd, manifest); closeSync(fd); fd = undefined; check(); renameSync(temp, path); }
    finally { if (fd !== undefined) closeSync(fd); try { unlinkSync(temp); } catch {} }
    return { provider: 'bernstein', capability: 'scheduler.deterministic_dag', path, manifest, authority: 'candidate-manifest-only',
      limitation: 'Task command execution and upstream scheduler behavior are not verified by compilation.', timeout_seconds: 3600 };
  }
  async function bernsteinValidate(manifestPath, { signal } = {}) {
    const path = source(manifestPath), before = sha(readSpinesBytes(path));
    const validate = await execute('bernstein', 'scheduler.deterministic_dag', ['bernstein', 'workflow', 'validate', path], { timeout: 120, signal });
    if (sha(readSpinesBytes(path)) !== before) throw Error('manifest changed during validation');
    const dry_run = await execute('bernstein', 'scheduler.deterministic_dag', ['bernstein', 'workflow', 'run', path, '--dry-run'], { timeout: 120, signal });
    if (sha(readSpinesBytes(path)) !== before) throw Error('manifest changed during dry-run');
    return { provider: 'bernstein', capability: 'scheduler.deterministic_dag', state: [validate, dry_run].every(item => item.execution_state === 'passed') ? 'passed' : 'failed', validate, dry_run, manifest_sha256: before };
  }
  async function bernsteinRun(manifestPath, goal, { timeout = 3600, signal } = {}) {
    return response('bernstein', 'scheduler.deterministic_dag', await execute('bernstein', 'scheduler.deterministic_dag', ['bernstein', 'workflow', 'run', source(manifestPath), '-g', argument(goal, 'goal')], { timeout, signal, effect: true }));
  }
  async function fleetStatus({ signal } = {}) {
    const status = await execute('agent-deck', 'fleet.status_detection', ['agent-deck', '--json', 'status'], { signal });
    const sessions = await execute('agent-deck', 'fleet.status_detection', ['agent-deck', '--json', 'list', '--all'], { signal });
    return { provider: 'agent-deck', capability: 'fleet.status_detection', state: [status, sessions].every(item => item.execution_state === 'passed') ? 'passed' : 'failed', status, sessions };
  }
  async function fleetLaunch({ path, title, command, message, group, parent, mcps = [], signal }) {
    if (!Array.isArray(mcps) || mcps.length > 32) throw Error('invalid MCP list');
    const argv = ['agent-deck', '--json', 'launch', owned(path), '-t', argument(title, 'title'), '-c', argument(command, 'command'), '-m', argument(message, 'message')];
    if (group) argv.push('-g', argument(group, 'group')); if (parent) argv.push('--parent', argument(parent, 'parent'));
    for (const mcp of mcps) argv.push('--mcp', argument(mcp, 'MCP'));
    return response('agent-deck', 'fleet.pty', await execute('agent-deck', 'fleet.pty', argv, { timeout: 180, signal, effect: true }));
  }
  async function fleetAction(action, session, { message, title, signal } = {}) {
    if (!['show', 'output', 'send', 'fork', 'start', 'stop', 'restart'].includes(action)) throw Error('unsupported fleet action');
    const argv = ['agent-deck', ...['stop', 'restart'].includes(action) ? [] : ['--json'], 'session', action, argument(session, 'session')];
    if (action === 'send') argv.push(argument(message, 'message'));
    if (action === 'fork' && title) argv.push('-t', argument(title, 'title'));
    if (action === 'start' && message) argv.push('-m', argument(message, 'message'));
    const capability = action === 'fork' ? 'session.fork' : 'fleet.pty';
    return response('agent-deck', capability, await execute('agent-deck', capability, argv, { timeout: 180, signal, effect: !['show', 'output'].includes(action) }), { action, session });
  }
  async function cavemanCode(prompt, { cwd = root, goal_mode = false, timeout = 3600, signal } = {}) {
    if (typeof goal_mode !== 'boolean') throw Error('goal_mode must be boolean');
    const argv = ['caveman-code', ...goal_mode ? ['goal', 'start'] : ['-p'], argument(prompt, 'prompt')];
    return response('caveman-code', 'agent.executor', await execute('caveman-code', 'agent.executor', argv, { cwd, timeout, signal, effect: true }), { mode: goal_mode ? 'goal' : 'print' });
  }
  async function invokerSubmit(workflowPath, { timeout = 3600, signal } = {}) {
    const provider = store.provider('invoker'); if (!provider || ![true, 1].includes(provider.license_approved)) throw Error('Invoker license approval is required before execution');
    return response('invoker', 'scheduler.persisted_dag', await execute('invoker', 'scheduler.persisted_dag', ['invoker-cli', 'run', '--live', source(workflowPath)], { timeout, signal, effect: true }));
  }
  return { status, memorySearch, memorySessions, compactCommand, bernsteinCompile, bernsteinValidate, bernsteinRun, fleetStatus, fleetLaunch, fleetAction, cavemanCode, invokerSubmit };
}
