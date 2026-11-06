// Effecting dual-agent workflows. The transport owns admission; this module
// owns real child processes, durable transitions, cancellation, and worktree CAS.
import { spawn } from 'node:child_process';
import { AsyncLocalStorage } from 'node:async_hooks';
import { randomUUID } from 'node:crypto';
import { closeSync, existsSync, openSync, writeSync } from 'node:fs';
import { homedir } from 'node:os';
import { basename, join, resolve } from 'node:path';
import { projectRun } from './project_runtime.mjs';
import { fabricGitEnvironment } from './fabric_store.mjs';

const MAX_PROMPT = 512_000, MAX_FINAL = 200_000, MAX_STDOUT = 8 * 1024 * 1024, MAX_STDERR = 1024 * 1024;
const trim = (value, limit = MAX_FINAL) => String(value ?? '').slice(-limit);
const runPassed = result => result?.state === 'passed' && result.exit_code === 0;
const stopError = reason => Object.assign(new Error(reason), { fabric_state: 'blocked' });
const cancelError = () => Object.assign(new Error('fabric task was cancelled'), { fabric_state: 'cancelled' });
const uncertainError = reason => Object.assign(new Error(reason), { fabric_state: 'uncertain' });
const fail = (reason, result) => Object.assign(new Error(reason), { fabric_state: 'failed', result });
const strictObject = value => value !== null && typeof value === 'object' && !Array.isArray(value);
const text = (value, label, max = MAX_PROMPT) => {
  if (typeof value !== 'string' || !value || value.includes('\0') || Buffer.byteLength(value) > max) throw new Error(`invalid ${label}`);
  return value;
};
const cleanDiagnostic = (value, token = '') => trim(String(value ?? '').replaceAll(token || '\0', '[redacted]'), 2000);

export function parseAgentStream(source) {
  let session_id = null;
  const final = [];
  for (const line of source.split('\n')) {
    if (!line || line.length > 1_000_000) continue;
    let item;
    try { item = JSON.parse(line); } catch { continue; }
    if (!strictObject(item)) continue;
    for (const key of ['session_id', 'sessionId', 'thread_id', 'threadId'])
      if (typeof item[key] === 'string' && item[key]) session_id = item[key];
    if (['result', 'assistant', 'message.completed', 'item.completed'].includes(item.type)) {
      for (const key of ['result', 'text', 'message', 'output_text']) if (typeof item[key] === 'string') final.push(item[key]);
      if (item.type === 'item.completed' && strictObject(item.item))
        for (const key of ['text', 'content', 'message']) if (typeof item.item[key] === 'string') final.push(item.item[key]);
    }
  }
  return { session_id, final: trim(final.join('\n')) };
}

export function typedVerdict(final) {
  let value;
  try { value = JSON.parse(String(final).trim()); } catch { return null; }
  if (!strictObject(value) || !['resolved', 'retry', 'blocked'].includes(value.state) ||
      !Array.isArray(value.evidence) || !Array.isArray(value.blocking_findings) ||
      value.evidence.some(item => typeof item !== 'string' || !item || item.length > 4096) ||
      value.blocking_findings.some(item => typeof item !== 'string' || item.length > 4096)) return null;
  return value;
}

async function defaultAgentRunner({ argv, cwd, env, stdoutPath, stderrPath, timeoutMs, signal }) {
  const started = performance.now();
  let stdoutFd, stderrFd;
  try { stdoutFd = openSync(stdoutPath, 'wx', 0o600); stderrFd = openSync(stderrPath, 'wx', 0o600); }
  catch (error) { if (stdoutFd !== undefined) closeSync(stdoutFd); throw error; }
  try {
    return await new Promise(resolve => {
      const child = spawn(argv[0], argv.slice(1), { cwd, env, detached: process.platform !== 'win32',
        shell: false, stdio: ['ignore', 'pipe', 'pipe'] });
      let stdoutBytes = 0, stderrBytes = 0, overflow = false, timeout = false, cancelled = false, spawnError = null, ioError = null;
      let hardStop, hardStopDone = Promise.resolve();
      const signalGroup = name => {
        if (!child.pid) return;
        try { if (process.platform === 'win32') child.kill(name); else process.kill(-child.pid, name); }
        catch (error) { if (error.code !== 'ESRCH') spawnError ??= error; }
      };
      const stop = () => {
        signalGroup('SIGTERM');
        if (!hardStop) hardStopDone = new Promise(done => { hardStop = setTimeout(() => { signalGroup('SIGKILL'); done(); }, 250); });
      };
      const onAbort = () => { cancelled = true; stop(); };
      signal?.addEventListener('abort', onAbort, { once: true });
      if (signal?.aborted) onAbort();
      const timer = setTimeout(() => { timeout = true; stop(); }, timeoutMs);
      for (const [stream, fd, limit] of [[child.stdout, stdoutFd, MAX_STDOUT], [child.stderr, stderrFd, MAX_STDERR]]) {
        stream.on('data', bytes => {
          if (fd === stdoutFd) stdoutBytes += bytes.length; else stderrBytes += bytes.length;
          const consumed = fd === stdoutFd ? stdoutBytes : stderrBytes;
          const remaining = Math.max(0, limit - (consumed - bytes.length));
          try {
            for (let written = 0, allowed = Math.min(bytes.length, remaining); written < allowed; )
              written += writeSync(fd, bytes, written, allowed - written);
          } catch (error) { ioError ??= error; stop(); }
          if (consumed > limit && !overflow) { overflow = true; stop(); }
        });
      }
      child.on('error', error => { spawnError = error; });
      child.on('close', async (code, childSignal) => {
        clearTimeout(timer);
        // Parent exit is not group exit. A child can leave a grandchild with
        // closed stdio behind; retain the hard-kill deadline before joining.
        await hardStopDone;
        signal?.removeEventListener('abort', onAbort);
        resolve({ exit_code: cancelled ? 130 : timeout ? 124 : overflow ? 125 : ioError || spawnError ? 127 : code ?? 128,
          duration_ms: Math.round(performance.now() - started),
          ...(cancelled ? { cancelled: true } : {}), ...(timeout ? { timeout: true } : {}),
          ...(overflow ? { overflow: true } : {}), ...(childSignal ? { signal: childSignal } : {}),
          ...(ioError ? { io_error: ioError.code || 'output' } : {}),
          ...(spawnError ? { spawn_error: spawnError.code || 'spawn' } : {}) });
      });
    });
  } finally { closeSync(stdoutFd); closeSync(stderrFd); }
}

export function createFabricWork({ store, policy, projectRoot, token = '',
  run = projectRun, agentRunner = defaultAgentRunner, projectTool, estateStatus, env = process.env,
  stopPaths = [join(homedir(), '.bonfyre/operator/HALT'), join(homedir(), '.bonfyre/dev-fabric/STOP')] } = {}) {
  if (!store || !strictObject(policy) || typeof run !== 'function' || typeof agentRunner !== 'function') throw new Error('invalid fabric owner');
  if (policy.repair_failed_tests !== undefined && typeof policy.repair_failed_tests !== 'boolean')
    throw new Error('repair_failed_tests policy must be boolean');
  projectRoot = resolve(text(projectRoot, 'project root', 16384));
  if (!Array.isArray(stopPaths) || stopPaths.some(path => typeof path !== 'string')) throw new Error('invalid STOP paths');
  const controllers = new Map(), taskAborts = new Map(), tasks = new Map(), taskContext = new AsyncLocalStorage();
  const now = () => Date.now() / 1000;
  const check = taskId => {
    if (stopPaths.some(existsSync)) throw stopError('fabric STOP is active');
    const task = store.task(taskId);
    if (!task) throw fail('fabric task is missing');
    if (task.state === 'cancelled') throw cancelError();
    if (task.state !== 'running') throw fail(`fabric task is not running: ${task.state}`);
    return task;
  };
  const ensureRun = (outcome, label) => { if (!runPassed(outcome)) throw fail(`${label} did not pass`, outcome); return outcome; };
  const small = async (argv, { cwd = projectRoot, timeoutMs = 30_000, maxOutputBytes = 2_000_000 } = {}) => {
    const taskId = taskContext.getStore();
    if (taskId) check(taskId);
    const controller = new AbortController(), callId = `small-${randomUUID()}`;
    if (taskId) controllers.set(callId, { taskId, controller });
    let result;
    try { result = await run(argv, { cwd, env: fabricGitEnvironment(env), timeoutMs, maxOutputBytes, signal: controller.signal }); }
    finally { controllers.delete(callId); }
    if (result?.cancelled || taskId && store.isTaskCancelled(taskId)) throw cancelError();
    if (!result || result.timeout || result.overflow || !Number.isInteger(result.exit_code)) throw fail(`${basename(argv[0])} exceeded execution bounds`, result);
    return result;
  };
  const checked = async (argv, options) => {
    const result = await small(argv, options);
    if (result.exit_code !== 0) throw fail(`${basename(argv[0])} exited ${result.exit_code}`, result);
    return result;
  };
  const policyNumber = (section, key, fallback) => {
    const value = policy?.[section]?.[key] ?? fallback;
    if (!Number.isSafeInteger(value) || value < 1 || value > 3600) throw new Error(`invalid policy ${section}.${key}`);
    return value;
  };
  const command = ({ agent, prompt, write, resumeSession, finalPath }) => {
    text(prompt, 'agent prompt');
    if (!['claude', 'codex'].includes(agent)) throw new Error('unknown agent');
    const config = policy.agents?.[agent];
    if (!strictObject(config)) throw new Error(`missing ${agent} policy`);
    const executable = text(config.command, `${agent} command`, 4096);
    if (executable.includes('/') && !resolve(executable).startsWith('/')) throw new Error('invalid agent executable');
    if (agent === 'claude') return [executable, '-p', prompt, '--output-format', 'stream-json', '--verbose', '--max-turns',
      String(policyNumber('max_turns', write ? 'claude_write' : 'claude_read', 18)), '--permission-mode',
      text(config[write ? 'write_permission_mode' : 'read_permission_mode'], 'Claude permission mode', 100), '--add-dir', projectRoot,
      ...(resumeSession ? ['--resume', text(resumeSession, 'session ID', 512)] : [])];
    return [executable, ...(resumeSession ? ['exec', 'resume', text(resumeSession, 'session ID', 512)] : ['exec']),
      '--json', '--sandbox', text(config[write ? 'write_sandbox' : 'read_sandbox'], 'Codex sandbox', 100),
      '-o', finalPath, prompt];
  };
  async function agent(taskId, name, role, prompt, cwd, { write = false, resumeSession = null } = {}) {
    check(taskId);
    const depth = Number(env.BONFYRE_PEER_DEPTH || 0);
    if (!Number.isSafeInteger(depth) || depth < 0 || depth >= (policy.max_peer_depth ?? 1)) throw fail('peer recursion depth refused');
    const runId = randomUUID().replaceAll('-', '').slice(0, 24);
    const finalPath = join(store.taskDir, taskId, runId, 'final.txt');
    const argv = command({ agent: name, prompt, write, resumeSession, finalPath });
    const saved = store.createRun({ run_id: runId, task_id: taskId, agent: name, role, command: argv, cwd });
    if (saved.final_path !== finalPath) throw new Error('run command path differs from durable owner path');
    const controller = new AbortController();
    controllers.set(saved.run_id, { taskId, controller });
    const childEnv = { ...fabricGitEnvironment(env), BONFYRE_PEER_DEPTH: String(depth + 1), BONFYRE_FABRIC_TASK_ID: taskId,
      BONFYRE_FABRIC_RUN_ID: saved.run_id, ...(token ? { BONFYRE_MCP_TOKEN: token } : {}) };
    let outcome;
    try {
      check(taskId);
      outcome = await agentRunner({ argv, cwd, env: childEnv, stdoutPath: saved.stdout_path,
        stderrPath: saved.stderr_path, finalPath: saved.final_path,
        timeoutMs: policyNumber('timeouts_seconds', 'single_agent', 2400) * 1000, signal: controller.signal });
    } catch (error) {
      outcome = { exit_code: 127, spawn_error: cleanDiagnostic(error.message, token), duration_ms: 0 };
    } finally { controllers.delete(saved.run_id); }
    if (!strictObject(outcome) || !Number.isInteger(outcome.exit_code)) outcome = { exit_code: 127, duration_ms: 0, spawn_error: 'invalid agent runner outcome' };
    let stdout = '', stderr = '', final = '', parsed = { session_id: null, final: '' };
    try {
      stdout = store.readText(saved.stdout_path, { maxBytes: MAX_STDOUT, missing: true });
      stderr = store.readText(saved.stderr_path, { maxBytes: MAX_STDERR, missing: true });
      parsed = parseAgentStream(stdout);
      final = existsSync(saved.final_path) ? trim(store.readText(saved.final_path, { maxBytes: MAX_STDOUT })) : parsed.final;
    } catch { outcome = { ...outcome, overflow: true, exit_code: 125 }; }
    if (Buffer.byteLength(stdout) > MAX_STDOUT || Buffer.byteLength(stderr) > MAX_STDERR) outcome = { ...outcome, overflow: true, exit_code: 125 };
    const state = controller.signal.aborted || store.task(taskId)?.state === 'cancelled' ? 'cancelled'
      : outcome.timeout ? 'timeout' : outcome.overflow ? 'failed' : outcome.exit_code === 0 && final.trim() ? 'passed' : 'failed';
    const result = { run_id: saved.run_id, task_id: taskId, agent: name, role, state, exit_code: outcome.exit_code,
      session_id: parsed.session_id, final, stdout_path: saved.stdout_path, stderr_path: saved.stderr_path,
      final_path: saved.final_path, duration_ms: outcome.duration_ms ?? 0,
      ...(outcome.timeout ? { timeout: true } : {}), ...(outcome.overflow ? { overflow: true } : {}) };
    const persisted = store.finishRun(saved.run_id, result);
    if (persisted.state === 'cancelled') throw cancelError();
    check(taskId);
    return persisted;
  }
  async function pair(taskId, roles) {
    const results = await Promise.allSettled(roles.map(([name, role, prompt, cwd]) => agent(taskId, name, role, prompt, cwd)));
    const failed = results.find(result => result.status === 'rejected');
    if (failed) throw failed.reason;
    return Object.fromEntries(roles.map(([name], index) => [name, results[index].value]));
  }
  const receipt = (taskId, kind, payload, suffix) => store.saveReceipt(taskId, kind, payload, suffix);
  const complete = (taskId, kind, result, state = 'completed') => {
    const saved = receipt(taskId, kind, result);
    const value = { ...result, receipt: saved };
    store.updateTask(taskId, { state, completed_at: now(), result_json: value });
    return value;
  };
  async function duel(taskId, task, mission = task.prompt) {
    if (!store.acquireLock('duel-read', taskId, 7200)) throw fail('duel read lane is busy');
    const base = `This is a Bonfyre dual-agent architecture duel. Work from the real repository and live Bonfyre estate. Produce concrete evidence, identify hidden assumptions, and do not make changes. The peer will independently solve the same problem.\n\nTASK:\n${mission}`;
    const analyses = await pair(taskId, [['claude', 'independent-analysis', base, projectRoot], ['codex', 'independent-analysis', base, projectRoot]]);
    for (const value of Object.values(analyses)) ensureRun(value, 'independent analysis');
    const critiques = await pair(taskId, [
      ['claude', 'peer-critique', `Act as an adversarial reviewer. Critique the Codex proposal against the real Bonfyre architecture. Identify incorrect assumptions, missing system planes, unsafe edits, and better compositions. Do not merely agree.\n\nORIGINAL TASK:\n${mission}\n\nCODEX PROPOSAL:\n${trim(analyses.codex.final, 100000)}`, projectRoot],
      ['codex', 'peer-critique', `Act as an implementation and verification critic. Critique the Claude proposal against the repository. Find unsupported claims, missing files/tests, over-broad architecture, and implementation gaps. Do not merely agree.\n\nORIGINAL TASK:\n${mission}\n\nCLAUDE PROPOSAL:\n${trim(analyses.claude.final, 100000)}`, projectRoot],
    ]);
    for (const value of Object.values(critiques)) ensureRun(value, 'peer critique');
    const synthesis = ensureRun(await agent(taskId, 'claude', 'convergence-arbiter',
      `Synthesize a final Bonfyre mission plan from two independent proposals and two adversarial critiques. Preserve disagreements as explicit choices. Produce architecture, affected planes, exact evidence, parallel work units, tests, live verification, rollback, and promotion criteria.\n\nTASK:\n${mission}\n\nCLAUDE:\n${trim(analyses.claude.final, 70000)}\n\nCODEX:\n${trim(analyses.codex.final, 70000)}\n\nCLAUDE CRITIQUE:\n${trim(critiques.claude.final, 50000)}\n\nCODEX CRITIQUE:\n${trim(critiques.codex.final, 50000)}`,
      projectRoot), 'synthesis');
    return { analyses, critiques, synthesis };
  }
  async function testCommands(taskId, worktree, base = 'HEAD') {
    check(taskId);
    const changed = (await checked(['git', '-c', 'core.fsmonitor=false', 'diff', '--name-only', '-z', base, '--'], { cwd: worktree, timeoutMs: 60000 })).stdout.split('\0').filter(Boolean);
    if (changed.some(path => path.endsWith('.py'))) throw fail('Python source changed; retired py_compile test profile refuses this relay');
    const authored = policy.test_commands || ['git diff --check'];
    if (!Array.isArray(authored) || authored.length > 100) throw fail('invalid authored test policy');
    // Shell is only used for authored policy commands. Git-controlled filenames
    // are never embedded in a shell string, including inside double quotes.
    const steps = authored.map(commandText => ({ label: commandText, argv: ['/usr/bin/env', 'bash', '-lc', commandText], cwd: worktree, authored: true }));
    for (const path of changed.filter(path => path.endsWith('.sh'))) steps.push({ label: `bash -n -- ${path}`,
      argv: ['/usr/bin/env', 'bash', '-n', '--', path], cwd: worktree });
    if (changed.some(path => path.startsWith('bonfyre-zig/'))) steps.push({ label: 'zig build', argv: ['zig', 'build'], cwd: join(worktree, 'bonfyre-zig') });
    for (const family of new Set(changed.filter(path => path.startsWith('cmd/')).map(path => path.split('/')[1]).filter(Boolean))) {
      if (!/^[A-Za-z0-9_.-]+$/.test(family) || ['.', '..'].includes(family)) throw fail('unsafe command family path');
      steps.push({ label: `make -C cmd/${family} -j2`, argv: ['make', '-C', `cmd/${family}`, '-j2'], cwd: worktree });
    }
    const results = [];
    const seen = new Set();
    for (const step of steps) {
      check(taskId);
      if (seen.has(step.label)) continue; seen.add(step.label);
      text(step.label, 'test command', 10000);
      if (step.authored && /(?:^|[^a-z])(?:python\d*(?:\.\d+)?|pypy\d*)\b/i.test(step.label)) throw fail('Python test command is retired');
      if (step.authored && (policy.forbidden_command_patterns || []).some(pattern => new RegExp(pattern).test(step.label)))
        throw fail('authored test command violates policy');
      const result = await small(step.argv, { cwd: step.cwd, timeoutMs: 1_800_000, maxOutputBytes: 2_000_000 });
      results.push({ command: step.label, ...result });
      if (result.exit_code !== 0) break;
    }
    return results;
  }
  async function completeDiff(taskId, worktree, base = 'HEAD') {
    check(taskId);
    const signal = taskAborts.get(taskId)?.signal;
    let diff = await store.diffPayload(worktree, { signal, base });
    if (diff.untracked?.length) {
      if (diff.untracked.length > 1000 || diff.untracked.some(path => typeof path !== 'string' || !path || path.startsWith('/') ||
          path.split('/').includes('..') || path.includes('\0'))) throw fail('unsafe or excessive untracked worktree paths');
      await checked(['git', '-c', 'core.fsmonitor=false', 'add', '-N', '--', ...diff.untracked], { cwd: worktree, timeoutMs: 120000 });
      diff = await store.diffPayload(worktree, { signal, base });
      if (diff.untracked?.length) throw fail('untracked files remain outside the patch receipt');
    }
    return diff;
  }
  async function relay(taskId, task, prompt = task.prompt) {
    if (task.apply_to_project && typeof projectTool !== 'function') throw stopError('Project patch owner unavailable before relay execution');
    if (!store.acquireLock('worktree-write', taskId, 10800)) throw fail('write relay lane is busy');
    check(taskId);
    const { worktree, branch, base_commit, source_snapshot } = await store.createWorktree(taskId, { base: policy.default_base || 'HEAD', signal: taskAborts.get(taskId)?.signal });
    if (source_snapshot) prompt += `\n\nSOURCE BASELINE: ${base_commit}. ${source_snapshot.files.length} explicit current-source files were captured before execution. All other paths remain at the configured Git base. Source manifest: ${source_snapshot.receipt.path}. Review changes against ${base_commit}; never include the input snapshot itself as an implementation change.`;
    const plan = ensureRun(await agent(taskId, 'claude', 'architect-plan',
      `Plan this change in the real Bonfyre repository. Map affected planes and exact files. Do not edit. Produce a constrained implementation plan, tests, live verification, and rollback contract.\n\nMISSION:\n${prompt}`,
      worktree), 'architect plan');
    const implementation = ensureRun(await agent(taskId, 'codex', 'implementation',
      `Implement the mission in this isolated Git worktree. Verify the plan against source, make the smallest coherent change, add tests, and leave a clean reviewable diff. Do not push.\n\nMISSION:\n${prompt}\n\nCLAUDE PLAN:\n${trim(plan.final, 120000)}`,
      worktree, { write: true }), 'implementation');
    check(taskId);
    const firstDiff = await completeDiff(taskId, worktree, base_commit);
    // A known refusal should not spend three more agent stages preparing an
    // application that the configured Project owner cannot admit.
    const checkApplication = async diff => {
      if (!task.apply_to_project || !diff.patch.trim()) return null;
      check(taskId);
      if (source_snapshot) {
        try { store.verifySourceSnapshot(source_snapshot); }
        catch { return {eligible:false,reason:'selected_source_changed'}; }
      }
      try {
        const preview = await projectTool('project_patch_preview', { patch: diff.patch }, { signal: taskAborts.get(taskId)?.signal });
        check(taskId);
        return { eligible: preview?.low_risk_eligible === true, preview };
      } catch (error) {
        check(taskId);
        return { eligible: false, reason: 'project_preview_unavailable', error: cleanDiagnostic(error.message, token) };
      }
    };
    const initialApplication = await checkApplication(firstDiff);
    if (initialApplication?.eligible === false) return complete(taskId, 'relay', {
      worktree, branch, source_snapshot, plan, implementation, stopped_at: 'apply_preflight',
      patch_receipt: receipt(taskId, 'relay-patch', firstDiff.patch, 'patch'),
      diff: { stat: firstDiff.stat, names: firstDiff.names, patch_sha256: firstDiff.patch_sha256 },
      apply_result: { applied: false, reason: 'apply_preflight_refused', ...initialApplication },
      next_step: 'Resolve the reported Project constraint before requesting application; the isolated candidate is retained and has not completed review or tests.'
    }, 'review_required');
    const review = ensureRun(await agent(taskId, 'claude', 'adversarial-review',
      `Review the changes against Git baseline ${base_commit} in this worktree. Check correctness, integration, proof, concurrency, security, tests, and underuse of Bonfyre. Do not edit. Return ONLY JSON {"state":"resolved|retry|blocked","evidence":["specific evidence"],"blocking_findings":["severity, exact file and actionable finding"]}. Use resolved only when there are no findings requiring repair.\n\nMISSION:\n${prompt}\n\nIMPLEMENTATION SUMMARY:\n${trim(implementation.final, 80000)}\n\nDIFF STAT:\n${firstDiff.stat}`,
      worktree), 'adversarial review');
    const reviewVerdict = typedVerdict(review.final);
    const cleanReview = reviewVerdict?.state === 'resolved' && reviewVerdict.evidence.length > 0 && reviewVerdict.blocking_findings.length === 0;
    const repair = cleanReview ? { state: 'skipped', reason: 'structured_review_has_no_repair_findings' } : ensureRun(await agent(taskId, 'codex', 'review-repair',
      `Address every valid review finding in this worktree. Reject incorrect findings with source evidence. Run targeted tests and leave the final diff ready for independent verification. Do not push.\n\nMISSION:\n${prompt}\n\nCLAUDE REVIEW:\n${trim(review.final, 120000)}`,
      worktree, { write: true }), 'review repair');
    let tests = await testCommands(taskId, worktree, base_commit);
    check(taskId);
    let testsPassed = tests.length > 0 && tests.every(item => item.exit_code === 0);
    let testRepair = null, failedTestReceipt = null;
    // Continue in the same worktree once when executable tests find a defect.
    // A repeated failure terminates without spending final-review stages.
    if (!testsPassed && policy.repair_failed_tests !== false) {
      failedTestReceipt = receipt(taskId, 'relay-failed-tests', tests);
      testRepair = ensureRun(await agent(taskId, 'codex', 'test-repair',
        `Repair the implementation in this existing worktree using the failed test evidence. Preserve test requirements and existing tests; do not weaken or remove tests to get a pass. Do not push. This is the one bounded test-repair attempt before the owner reruns verification.\n\nMISSION:\n${prompt}\n\nFAILED TEST RESULTS:\n${trim(JSON.stringify(tests), 60000)}`,
        worktree, { write: true }), 'test repair');
      tests = await testCommands(taskId, worktree, base_commit);
      check(taskId);
      testsPassed = tests.length > 0 && tests.every(item => item.exit_code === 0);
    }
    const finalDiff = await completeDiff(taskId, worktree, base_commit);
    if (!testsPassed) return complete(taskId, 'relay', { worktree, branch, source_snapshot, plan, implementation, review, repair, tests,
      test_repair: testRepair, failed_test_receipt: failedTestReceipt,
      stopped_at: 'tests', patch_receipt: receipt(taskId, 'relay-patch', finalDiff.patch, 'patch'),
      diff: { stat: finalDiff.stat, names: finalDiff.names, patch_sha256: finalDiff.patch_sha256 },
      apply_result: { applied: false, reason: 'tests_failed' },
      next_step: 'Repair the retained test failures before final verification or application.' }, 'failed');
    const repairedApplication = finalDiff.patch_sha256 === firstDiff.patch_sha256 ? initialApplication : await checkApplication(finalDiff);
    if (repairedApplication?.eligible === false) return complete(taskId, 'relay', {
      worktree, branch, source_snapshot, plan, implementation, review, repair, tests, test_repair: testRepair, failed_test_receipt: failedTestReceipt, stopped_at: 'repaired_apply_preflight',
      patch_receipt: receipt(taskId, 'relay-patch', finalDiff.patch, 'patch'),
      diff: { stat: finalDiff.stat, names: finalDiff.names, patch_sha256: finalDiff.patch_sha256 },
      apply_result: { applied: false, reason: 'apply_preflight_refused', ...repairedApplication }
    }, 'review_required');
    const evidence = `\n\nMISSION:\n${prompt}\n\nTEST RESULTS:\n${trim(JSON.stringify(tests), 60000)}`;
    const reviewPrompt = `Independently review the final worktree diff against Git baseline ${base_commit}. Do not edit. Verify mission completeness, meaningful tests, and no unrelated changes. Return ONLY JSON {"state":"resolved|retry|blocked","evidence":["specific evidence"],"blocking_findings":[]}.${evidence}`;
    const verifyPrompt = `Independently verify final worktree state against Git baseline ${base_commit} and test evidence. Do not edit. Compilation alone is not live behavior. Return ONLY JSON {"state":"resolved|retry|blocked","evidence":["specific evidence"],"blocking_findings":[]}.${evidence}`;
    const finalReviews = await pair(taskId, [
      ['codex', 'independent-final-review', reviewPrompt, worktree],
      ['claude', 'final-verifier', verifyPrompt, worktree],
    ]);
    const codexReview = ensureRun(finalReviews.codex, 'independent final review');
    const claudeVerify = ensureRun(finalReviews.claude, 'final verifier');
    // Read-only review tools are separate processes; prove they did not alter
    // the reviewed patch before using its digest as an apply candidate.
    const observedDiff = await completeDiff(taskId, worktree, base_commit);
    const reviewedPatchUnchanged = observedDiff.patch_sha256 === finalDiff.patch_sha256 && observedDiff.names === finalDiff.names;
    const patchReceipt = receipt(taskId, 'relay-patch', finalDiff.patch, 'patch');
    const verdicts = { codex: typedVerdict(codexReview.final), claude: typedVerdict(claudeVerify.final) };
    const independentlyResolved = reviewedPatchUnchanged && Object.values(verdicts).every(item => item?.state === 'resolved' && item.evidence.length > 0 && item.blocking_findings.length === 0);
    let applyResult = null;
    let sourceCurrent = true;
    if (source_snapshot) {
      try { store.verifySourceSnapshot(source_snapshot); } catch { sourceCurrent = false; }
    }
    if (task.apply_to_project && finalDiff.patch.trim()) {
      if (!sourceCurrent) applyResult = { applied: false, reason: 'selected_source_changed', next_step: 'Read current source hashes before preparing a new candidate.' };
      else if (!testsPassed || !independentlyResolved) applyResult = { applied: false,
        reason: reviewedPatchUnchanged ? 'tests_or_independent_evidence_not_resolved' : 'worktree_changed_after_review' };
      else {
        if (typeof projectTool !== 'function') throw fail('Project MCP patch owner unavailable');
        check(taskId);
        const preview = await projectTool('project_patch_preview', { patch: finalDiff.patch }, { signal: taskAborts.get(taskId)?.signal });
        check(taskId);
        if (preview?.low_risk_eligible !== true) applyResult = { applied: false, preview };
        else {
          check(taskId);
          const attempt = receipt(taskId, 'project-apply-attempt', { patch_sha256: finalDiff.patch_sha256, preview });
          try {
            const applied = await projectTool('project_apply_low_risk_patch', { patch: finalDiff.patch }, { signal: taskAborts.get(taskId)?.signal });
            if (!strictObject(applied) || !applied.receipt) throw uncertainError('Project MCP apply outcome lacks a receipt');
            applyResult = { ...applied, attempt_receipt: attempt };
            receipt(taskId, applied.applied === true ? 'project-apply-complete' : 'project-apply-refused',
              { patch_sha256: finalDiff.patch_sha256, attempt_receipt: attempt, apply_result: applied });
          } catch (error) {
            receipt(taskId, 'project-apply-uncertain', { patch_sha256: finalDiff.patch_sha256, attempt_receipt: attempt, reason: 'reconcile_project_receipt_before_retry' });
            throw uncertainError(`Project patch outcome uncertain; reconcile receipt before retry: ${cleanDiagnostic(error.message, token)}`);
          }
        }
      }
    }
    const result = { worktree, branch, source_snapshot, plan, implementation, review, repair, tests, codex_review: codexReview,
      claude_verify: claudeVerify, verdicts, test_repair: testRepair, failed_test_receipt: failedTestReceipt,
      diff: { stat: finalDiff.stat, names: finalDiff.names, patch_sha256: finalDiff.patch_sha256,
        reviewed_patch_unchanged: reviewedPatchUnchanged, selected_source_unchanged: sourceCurrent, observed_patch_sha256: observedDiff.patch_sha256 },
      patch_receipt: patchReceipt, apply_result: applyResult };
    const applicationSatisfied = !task.apply_to_project || !finalDiff.patch.trim() || applyResult?.applied === true;
    return complete(taskId, 'relay', result, !testsPassed ? 'failed' : independentlyResolved && applicationSatisfied && sourceCurrent ? 'completed' : 'review_required');
  }
  async function prEvidence(taskId) {
    check(taskId);
    const view = await small(['gh', 'pr', 'view', '--json', 'number,title,url,headRefName,baseRefName,isDraft,mergeStateStatus,reviewDecision,statusCheckRollup'], { timeoutMs: 60000 });
    let parsed;
    try { if (view.exit_code === 0) parsed = JSON.parse(view.stdout); } catch { /* No trustworthy current PR. */ }
    if (!parsed || !Number.isSafeInteger(parsed.number)) throw fail('No current GitHub PR was found for this branch');
    check(taskId);
    const checks = await small(['gh', 'pr', 'checks'], { timeoutMs: 120000 });
    check(taskId);
    const repo = await checked(['gh', 'repo', 'view', '--json', 'nameWithOwner'], { timeoutMs: 30000 });
    let nameWithOwner;
    try { nameWithOwner = JSON.parse(repo.stdout).nameWithOwner; } catch { throw fail('GitHub repository identity unavailable'); }
    if (typeof nameWithOwner !== 'string' || !/^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/.test(nameWithOwner)) throw fail('invalid GitHub repository identity');
    const comments = {};
    for (const [key, endpoint] of [['reviews', 'reviews'], ['comments', 'comments']]) {
      check(taskId);
      const result = await small(['gh', 'api', '--paginate', `repos/${nameWithOwner}/pulls/${parsed.number}/${endpoint}`], { timeoutMs: 120000, maxOutputBytes: 4_000_000 });
      try { comments[key] = result.exit_code === 0 ? JSON.parse(result.stdout) : []; } catch { comments[key] = []; }
    }
    return { view: parsed, view_raw: view, checks, ...comments };
  }
  async function workflow(taskId, task) {
    switch (task.kind) {
      case 'duel': return complete(taskId, 'duel', await duel(taskId, task));
      case 'batch': {
        const mission = task.prompt;
        const batchPrompt = `Decompose this Bonfyre mission into 5-30 independent or dependency-ordered work units. Assign Claude, Codex, both, or Estate Compiler. Include plane locks, worktree boundaries, inputs, outputs, tests, live verification, rollback, and convergence. Do not execute units yet.\n\nMISSION:\n${mission}`;
        return complete(taskId, 'duel', { ...await duel(taskId, task, batchPrompt), original_mission: mission, mode: 'batch-plan' });
      }
      case 'relay': return relay(taskId, task);
      case 'pr': {
        const evidence = await prEvidence(taskId);
        const prompt = `${task.prompt}\n\nCURRENT PR EVIDENCE\n${trim(JSON.stringify(evidence), 180000)}\n\nConverge CI failures and review findings. Do not push or force-push. Leave an isolated verified patch and receipt.`;
        const result = await relay(taskId, task, prompt);
        store.updateTask(taskId, { result_json: { ...result, pr_evidence: evidence } });
        return { ...result, pr_evidence: evidence };
      }
      case 'review': {
        const prompt = `Review current repository changes without editing. Focus on correctness, security, behavior regressions, missing tests, Bonfyre integration, and proof gaps. Return findings by severity with exact paths. Review instructions:\n${task.prompt}`;
        const reviews = await pair(taskId, [['claude', 'cross-review', prompt, projectRoot], ['codex', 'cross-review', prompt, projectRoot]]);
        for (const result of Object.values(reviews)) ensureRun(result, 'cross review');
        return complete(taskId, 'cross-review', { reviews });
      }
      case 'verify': {
        check(taskId);
        if (typeof estateStatus !== 'function') throw fail('native estate status owner unavailable');
        const estate = await estateStatus();
        if (!strictObject(estate)) throw fail('invalid native estate status');
        if (typeof projectTool !== 'function') throw fail('MCP verification owner unavailable');
        check(taskId);
        const mcp = await projectTool('mesh_status', {}, { signal: taskAborts.get(taskId)?.signal });
        check(taskId);
        const release = await projectTool('release_attest', {}, { signal: taskAborts.get(taskId)?.signal });
        check(taskId);
        const prompt = `Independently verify this Bonfyre claim from repository and live runtime evidence; do not edit. Separate present, reachable, executing, behaviorally verified, degraded, blocked, failed. Return missing evidence and exact next actions.\n\nCLAIM:\n${task.prompt}\n\nESTATE STATUS:\n${trim(JSON.stringify(estate), 80000)}\n\nMCP STATUS:\n${trim(JSON.stringify(mcp), 80000)}\n\nRELEASE:\n${trim(JSON.stringify(release), 80000)}`;
        const verifiers = await pair(taskId, [['claude', 'verification', prompt, projectRoot], ['codex', 'verification', prompt, projectRoot]]);
        for (const result of Object.values(verifiers)) ensureRun(result, 'dual verifier');
        return complete(taskId, 'dual-verification', { estate, mcp, release, verifiers });
      }
      case 'claude': case 'codex': case 'resume-claude': case 'resume-codex': {
        const resume = task.kind.startsWith('resume-'), name = resume ? task.kind.slice(7) : task.kind;
        const session = resume ? store.session(task.title, name) : null;
        if (resume && !session) throw fail('recorded agent session not found');
        const cwd = session?.cwd || projectRoot;
        const result = await agent(taskId, name, resume ? 'session-resume' : 'direct', task.prompt, cwd,
          resume ? { resumeSession: task.title } : {});
        const kind = `${name}-${resume ? 'resume' : 'dispatch'}`;
        return complete(taskId, kind, result, runPassed(result) ? 'completed' : 'failed');
      }
      default: throw fail(`unsupported fabric task kind: ${task.kind}`);
    }
  }
  async function execute(taskId) {
    text(taskId, 'task ID', 128);
    if (tasks.has(taskId)) throw new Error('fabric task already executing');
    if (stopPaths.some(existsSync)) throw stopError('fabric STOP is active');
    const task = store.claimTask(taskId);
    if (!task) throw new Error('fabric task is not queued');
    taskAborts.set(taskId, new AbortController());
    const promise = (async () => {
      try {
        const result = await taskContext.run(taskId, () => workflow(taskId, task));
        store.recordEvent('broker', 'task.completed', { task_id: taskId, kind: task.kind, state: store.task(taskId)?.state });
        return result;
      } catch (error) {
        const current = store.task(taskId);
        const state = current?.state === 'cancelled' ? 'cancelled' : error.fabric_state || 'failed';
        if (state !== 'cancelled') store.updateTask(taskId, { state, completed_at: now(), error: cleanDiagnostic(error.message, token),
          result_json: { error: cleanDiagnostic(error.message, token), ...(error.result ? { outcome: error.result } : {}) } });
        store.recordEvent('broker', `task.${state}`, { task_id: taskId, kind: task.kind, error: cleanDiagnostic(error.message, token) });
        throw error;
      } finally {
        store.releaseTaskLocks(taskId);
        taskAborts.delete(taskId);
        tasks.delete(taskId);
      }
    })();
    tasks.set(taskId, promise);
    return promise;
  }
  async function cancel(taskId) {
    text(taskId, 'task ID', 128);
    const cancelled = store.cancelTask(taskId);
    taskAborts.get(taskId)?.abort();
    for (const active of controllers.values()) if (active.taskId === taskId) active.controller.abort();
    // The worker's finally releases locks only after agent processes close.
    if (tasks.has(taskId)) await tasks.get(taskId).catch(() => {});
    return cancelled;
  }
  return { execute, cancel, active: () => [...tasks.keys()] };
}
