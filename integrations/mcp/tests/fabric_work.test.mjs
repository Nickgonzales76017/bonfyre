import { createHash } from 'node:crypto';
import { test, expect } from 'bun:test';
import { chmodSync, existsSync, mkdtempSync, mkdirSync, readFileSync, realpathSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createFabricStore } from '../fabric_store.mjs';
import { createFabricWork, parseAgentStream, typedVerdict } from '../fabric_work.mjs';
import { projectRun } from '../project_runtime.mjs';

const policy = () => ({ max_peer_depth: 1, default_base: 'HEAD', test_commands: ['git diff --check'],
  max_turns: { claude_read: 18, claude_write: 30, codex_read: 18, codex_write: 30 },
  timeouts_seconds: { single_agent: 30 },
  agents: { claude: { command: 'claude', read_permission_mode: 'plan', write_permission_mode: 'acceptEdits' },
    codex: { command: 'codex', read_sandbox: 'read-only', write_sandbox: 'workspace-write' } } });
const verdict = JSON.stringify({ state: 'resolved', evidence: ['fixture tests passed'], blocking_findings: [] });
const promptOf = argv => argv[0] === 'claude' ? argv[argv.indexOf('-p') + 1] : argv.at(-1);
const git = async (argv, cwd) => {
  const result = await projectRun(['git', ...argv], { cwd, timeoutMs: 30_000, maxOutputBytes: 100_000 });
  if (result.exit_code !== 0) throw new Error(`fixture git failed: ${argv.join(' ')}`);
};

async function fixture({ withGit = false, agentRunner, useDefaultRunner = false, run = projectRun, projectTool, estateStatus, policyOverride = {}, diskAvailableBytes = ()=>8*1024*1024*1024 } = {}) {
  const scratch = mkdtempSync(join(tmpdir(), 'bonfyre-fabric-work-'));
  const root = join(scratch, 'repo'), home = join(scratch, 'fabric');
  mkdirSync(root);
  if (withGit) {
    await git(['init', '-q'], root);
    writeFileSync(join(root, 'README.md'), 'fixture repository\n');
    await git(['add', 'README.md'], root);
    await git(['-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid', 'commit', '-qm', 'initial fixture'], root);
  }
  const store = createFabricStore({ home, projectRoot: root });
  const called = [];
  const defaultRunner = async ({ argv, cwd, stdoutPath, finalPath, signal }) => {
    called.push({ argv, cwd, signal });
    const final = promptOf(argv).includes('Return ONLY JSON') ? verdict : 'fixture completed with evidence';
    writeFileSync(stdoutPath, `${JSON.stringify({ type: 'result', session_id: `fixture-${argv[0]}`, result: final })}\n`);
    writeFileSync(finalPath, final);
    return { exit_code: 0, duration_ms: 1 };
  };
  const override = typeof policyOverride === 'function' ? policyOverride(scratch) : policyOverride;
  const basePolicy = policy(), actualPolicy = { ...basePolicy, ...override,
    agents: { ...basePolicy.agents, ...override.agents } };
  const owner = createFabricWork({ store, policy: actualPolicy, projectRoot: root, token: 'fixture-token',
    run, ...(useDefaultRunner ? {} : { agentRunner: agentRunner || defaultRunner }),
    projectTool, estateStatus, diskAvailableBytes, env: {}, stopPaths: [join(scratch, 'STOP')] });
  return { scratch, root, home, store, owner, called, cleanup() { store.close(); rmSync(scratch, { recursive: true, force: true }); } };
}

test('stream parser and typed verdict refuse malformed or unstructured agent claims', () => {
  expect(parseAgentStream('{"type":"result","session_id":"s","result":"done"}\n')).toEqual({ session_id: 's', final: 'done' });
  expect(typedVerdict('resolved')).toBeNull();
  expect(typedVerdict('{"state":"resolved","evidence":[],"blocking_findings":[]}')).toEqual({ state: 'resolved', evidence: [], blocking_findings: [] });
  expect(typedVerdict(verdict)?.state).toBe('resolved');
});

test('direct dispatch persists exact command, real run paths, session and completed receipt', async () => {
  const f = await fixture();
  try {
    const id = f.store.createTask({ kind: 'codex', title: 'fixture direct', prompt: 'Inspect fixture repo', apply_to_project: false });
    const result = await f.owner.execute(id);
    expect(result.state).toBe('passed');
    const task = f.store.taskStatus(id);
    expect(task.state).toBe('completed');
    expect(task.runs).toHaveLength(1);
    const argv = JSON.parse(task.runs[0].command_json);
    expect(argv).toContain(task.runs[0].final_path);
    expect(argv).toContain('--sandbox');
    expect(task.runs[0].state).toBe('passed');
    expect(task.receipts).toHaveLength(1);
    expect(f.store.session('fixture-codex', 'codex').cwd).toBe(realpathSync(f.root));
    expect(f.called).toHaveLength(1);
  } finally { f.cleanup(); }
});

test('duel and batch run independent analyses, peer critiques, and synthesis without editing', async () => {
  const f = await fixture();
  try {
    for (const kind of ['duel', 'batch']) {
      const id = f.store.createTask({ kind, title: kind, prompt: 'Map fixture architecture', apply_to_project: false });
      const result = await f.owner.execute(id);
      expect(f.store.task(id).state).toBe('completed');
      expect(f.store.taskStatus(id).runs).toHaveLength(5);
      expect(result.analyses.claude.state).toBe('passed');
      expect(result.critiques.codex.state).toBe('passed');
      if (kind === 'batch') expect(result.mode).toBe('batch-plan');
    }
    expect(f.store.summary().locks).toEqual([]);
  } finally { f.cleanup(); }
});

test('relay captures new files in patch and applies only after tests plus two typed resolved verdicts', async () => {
  const calls = [];
  const f = await fixture({ withGit: true,
    agentRunner: async ({ argv, cwd, stdoutPath, finalPath }) => {
      const prompt = promptOf(argv);
      const role = prompt.includes('Implement the mission') ? 'implementation' : prompt.includes('Address every valid review') ? 'repair' : 'other';
      if (role === 'implementation') writeFileSync(join(cwd, 'new-file.txt'), 'new fixture behavior\n');
      const final = prompt.includes('Return ONLY JSON') ? verdict : 'fixture completed with source evidence';
      writeFileSync(stdoutPath, `${JSON.stringify({ type: 'result', result: final })}\n`);
      writeFileSync(finalPath, final);
      return { exit_code: 0, duration_ms: 1 };
    },
    projectTool: async (name, args) => {
      calls.push([name, args]);
      return name === 'project_patch_preview' ? { low_risk_eligible: true } : { applied: true, receipt: { receipt_id: 'fixture-receipt' } };
    } });
  try {
    const id = f.store.createTask({ kind: 'relay', title: 'fixture relay', prompt: 'Add fixture file', apply_to_project: true });
    const result = await f.owner.execute(id);
    expect(f.store.task(id).state).toBe('completed');
    expect(result.tests.every(item => item.exit_code === 0)).toBe(true);
    expect(result.diff.patch_sha256).toMatch(/^[a-f0-9]{64}$/);
    expect(result.apply_result.applied).toBe(true);
    expect(calls.map(item => item[0])).toEqual(['project_patch_preview', 'project_patch_preview', 'project_apply_low_risk_patch']);
    expect(calls[2][1].patch).toContain('new-file.txt');
    expect(readFileSync(result.patch_receipt.path, 'utf8')).toContain('new-file.txt');
    expect(f.store.summary().locks).toEqual([]);
  } finally { f.cleanup(); }
});

test('unstructured reviewer text never authorizes Project MCP apply', async () => {
  const calls = [];
  const f = await fixture({ withGit: true,
    agentRunner: async ({ argv, cwd, stdoutPath, finalPath }) => {
      if (promptOf(argv).includes('Implement the mission')) writeFileSync(join(cwd, 'change.txt'), 'fixture\n');
      writeFileSync(stdoutPath, '{"type":"result","result":"looks good"}\n');
      writeFileSync(finalPath, 'looks good');
      return { exit_code: 0, duration_ms: 1 };
    }, projectTool: async name => { calls.push(name); return name === 'project_patch_preview' ? { low_risk_eligible: true } : { applied: true }; } });
  try {
    const id = f.store.createTask({ kind: 'relay', title: 'fixture review gate', prompt: 'Add change', apply_to_project: true });
    const result = await f.owner.execute(id);
    expect(f.store.task(id).state).toBe('review_required');
    expect(result.apply_result).toEqual({ applied: false, reason: 'tests_or_independent_evidence_not_resolved' });
    expect(calls).toEqual(['project_patch_preview']);
  } finally { f.cleanup(); }
});

test('a refused apply route retains the candidate and stops before review or repair agent expense', async () => {
  const roles = [], calls = [];
  const f = await fixture({ withGit: true,
    agentRunner: async ({ argv, cwd, stdoutPath, finalPath }) => {
      const prompt = promptOf(argv); roles.push(prompt);
      if (prompt.includes('Implement the mission')) writeFileSync(join(cwd, 'blocked.txt'), 'candidate\n');
      writeFileSync(stdoutPath, ''); writeFileSync(finalPath, 'fixture implementation');
      return { exit_code: 0, duration_ms: 1 };
    }, projectTool: async name => { calls.push(name); return { valid: true, low_risk_eligible: false, policy_errors: ['path outside allowed roots'] }; } });
  try {
    const id = f.store.createTask({ kind: 'relay', title: 'blocked apply', prompt: 'Prepare candidate', apply_to_project: true });
    const result = await f.owner.execute(id);
    expect(result.stopped_at).toBe('apply_preflight');
    expect(result.apply_result.preview.policy_errors).toEqual(['path outside allowed roots']);
    expect(roles).toHaveLength(2);
    expect(calls).toEqual(['project_patch_preview']);
    expect(readFileSync(result.patch_receipt.path, 'utf8')).toContain('blocked.txt');
    expect(f.store.task(id).state).toBe('review_required');
    expect(f.store.summary().locks).toEqual([]);
  } finally { f.cleanup(); }
});

test('persistent failed verification stops after one bounded repair without launching final reviewers', async () => {
  const f = await fixture({ withGit: true, policyOverride: { test_commands: ['false'] } });
  try {
    const id = f.store.createTask({ kind: 'relay', title: 'failing test', prompt: 'Fixture', apply_to_project: false });
    const result = await f.owner.execute(id);
    expect(result.stopped_at).toBe('tests');
    expect(result.tests[0].exit_code).not.toBe(0);
    expect(f.store.taskStatus(id).runs.map(run => run.role)).toEqual(['architect-plan', 'implementation', 'adversarial-review', 'test-repair']);
    expect(result.failed_test_receipt.kind).toBe('relay-failed-tests');
    expect(result.apply_result.applied).toBe(false);
    expect(f.store.task(id).state).toBe('failed');
  } finally { f.cleanup(); }
});

test('explicit test-repair policy can stop on the first failure and refuses ambiguous configuration', async () => {
  expect(() => createFabricWork({ store: {}, policy: { repair_failed_tests: 'false' }, projectRoot: '/fixture' })).toThrow('must be boolean');
  const f = await fixture({ withGit: true, policyOverride: { test_commands: ['false'], repair_failed_tests: false } });
  try {
    const id = f.store.createTask({ kind: 'relay', title: 'no repair policy', prompt: 'Fixture', apply_to_project: false });
    const result = await f.owner.execute(id);
    expect(result.stopped_at).toBe('tests');
    expect(result.test_repair).toBeNull();
    expect(f.store.taskStatus(id).runs).toHaveLength(3);
  } finally { f.cleanup(); }
});

test('a failed executable test is repaired in the existing worktree and verified without replanning', async () => {
  const f = await fixture({ withGit: true, policyOverride: { test_commands: ['test -f fixed.txt'] },
    agentRunner: async ({ argv, cwd, stdoutPath, finalPath }) => {
      const prompt = promptOf(argv);
      if (prompt.startsWith('Repair the implementation')) writeFileSync(join(cwd, 'fixed.txt'), 'repaired\n');
      const final = prompt.includes('Return ONLY JSON') ? verdict : 'fixture result';
      writeFileSync(stdoutPath, ''); writeFileSync(finalPath, final);
      return { exit_code: 0, duration_ms: 1 };
    } });
  try {
    const id = f.store.createTask({ kind: 'relay', title: 'repair failing test', prompt: 'Fixture', apply_to_project: false });
    const result = await f.owner.execute(id);
    expect(f.store.task(id).state).toBe('completed');
    expect(result.tests[0].exit_code).toBe(0);
    expect(result.test_repair.state).toBe('passed');
    expect(result.failed_test_receipt.kind).toBe('relay-failed-tests');
    const roles = f.store.taskStatus(id).runs.map(run => run.role);
    expect(roles.filter(role => role === 'architect-plan')).toHaveLength(1);
    expect(roles.filter(role => role === 'implementation')).toHaveLength(1);
    expect(roles.filter(role => role === 'test-repair')).toHaveLength(1);
    expect(readFileSync(result.patch_receipt.path, 'utf8')).toContain('fixed.txt');
  } finally { f.cleanup(); }
});

test('clean review skips repair while final verifiers start concurrently with independent test evidence', async () => {
  let started = 0, release;
  const bothStarted = new Promise(resolve => { release = resolve; });
  const finals = [];
  const f = await fixture({ withGit: true,
    agentRunner: async ({ argv, cwd, stdoutPath, finalPath }) => {
      const prompt = promptOf(argv);
      if (prompt.includes('Implement the mission')) writeFileSync(join(cwd, 'change.txt'), 'fixture\n');
      if (prompt.startsWith('Independently')) {
        finals.push(prompt); if (++started === 2) release();
        let timer;
        try { await Promise.race([bothStarted, new Promise((_, reject) => { timer = setTimeout(() => reject(new Error('final verification serialized')), 1500); })]); }
        finally { clearTimeout(timer); }
      }
      const final = prompt.includes('Return ONLY JSON') ? verdict : 'fixture result';
      writeFileSync(stdoutPath, ''); writeFileSync(finalPath, final);
      return { exit_code: 0, duration_ms: 1 };
    } });
  try {
    const id = f.store.createTask({ kind: 'relay', title: 'parallel verification', prompt: 'Fixture', apply_to_project: false });
    const result = await f.owner.execute(id);
    expect(result.repair.state).toBe('skipped');
    expect(f.store.taskStatus(id).runs).toHaveLength(5);
    expect(finals).toHaveLength(2);
    expect(finals.every(prompt => prompt.includes('TEST RESULTS:') && !prompt.includes('CODEX FINAL REVIEW:'))).toBe(true);
    expect(f.store.task(id).state).toBe('completed');
  } finally { release(); f.cleanup(); }
});

test('a repair that changes the candidate is rechecked before final reviewers', async () => {
  let previews = 0;
  const f = await fixture({ withGit: true,
    agentRunner: async ({ argv, cwd, stdoutPath, finalPath }) => {
      const prompt = promptOf(argv);
      if (prompt.includes('Implement the mission')) writeFileSync(join(cwd, 'change.txt'), 'before\n');
      if (prompt.includes('Address every valid review')) writeFileSync(join(cwd, 'change.txt'), 'after\n');
      const final = prompt.startsWith('Review the current')
        ? JSON.stringify({ state: 'retry', evidence: ['fixture observation'], blocking_findings: ['repair change.txt'] }) : 'fixture result';
      writeFileSync(stdoutPath, ''); writeFileSync(finalPath, final);
      return { exit_code: 0, duration_ms: 1 };
    }, projectTool: async name => { expect(name).toBe('project_patch_preview'); return { low_risk_eligible: ++previews === 1 }; } });
  try {
    const id = f.store.createTask({ kind: 'relay', title: 'changed candidate', prompt: 'Fixture', apply_to_project: true });
    const result = await f.owner.execute(id);
    expect(previews).toBe(2);
    expect(result.stopped_at).toBe('repaired_apply_preflight');
    expect(f.store.taskStatus(id).runs).toHaveLength(4);
    expect(result.apply_result.applied).toBe(false);
  } finally { f.cleanup(); }
});

test('post-review worktree mutation invalidates the patch receipt before any Project MCP effect', async () => {
  const calls = [];
  const f = await fixture({ withGit: true,
    agentRunner: async ({ argv, cwd, stdoutPath, finalPath }) => {
      const prompt = promptOf(argv);
      if (prompt.includes('Implement the mission')) writeFileSync(join(cwd, 'reviewed.txt'), 'before review\n');
      if (prompt.includes('Independently verify final worktree')) writeFileSync(join(cwd, 'reviewed.txt'), 'after review\n');
      const final = prompt.includes('Return ONLY JSON') ? verdict : 'fixture result';
      writeFileSync(stdoutPath, `${JSON.stringify({ type: 'result', result: final })}\n`);
      writeFileSync(finalPath, final);
      return { exit_code: 0, duration_ms: 1 };
    }, projectTool: async name => { calls.push(name); return name === 'project_patch_preview' ? { low_risk_eligible: true } : { applied: true }; } });
  try {
    const id = f.store.createTask({ kind: 'relay', title: 'mutation fixture', prompt: 'Add change', apply_to_project: true });
    const result = await f.owner.execute(id);
    expect(f.store.task(id).state).toBe('review_required');
    expect(result.diff.reviewed_patch_unchanged).toBe(false);
    expect(result.apply_result.reason).toBe('worktree_changed_after_review');
    expect(calls).toEqual(['project_patch_preview']);
  } finally { f.cleanup(); }
});

test('inferred shell syntax check passes a Git filename as argv, never shell interpolation', async () => {
  const f = await fixture({ withGit: true,
    agentRunner: async ({ argv, cwd, stdoutPath, finalPath }) => {
      const prompt = promptOf(argv);
      if (prompt.includes('Implement the mission')) writeFileSync(join(cwd, '$(touch PWNED).sh'), '#!/bin/sh\ntrue\n');
      const final = prompt.includes('Return ONLY JSON') ? verdict : 'fixture result';
      writeFileSync(stdoutPath, `${JSON.stringify({ type: 'result', result: final })}\n`);
      writeFileSync(finalPath, final);
      return { exit_code: 0, duration_ms: 1 };
    } });
  try {
    const id = f.store.createTask({ kind: 'relay', title: 'filename fixture', prompt: 'Add shell file', apply_to_project: false });
    const result = await f.owner.execute(id);
    expect(result.tests.some(item => item.command.includes('$(touch PWNED).sh'))).toBe(true);
    expect(result.tests.every(item => item.exit_code === 0)).toBe(true);
    expect(existsSync(join(result.worktree, 'PWNED'))).toBe(false);
  } finally { f.cleanup(); }
});

test('cancel aborts agent execution, preserves cancelled state, and releases lane only after join', async () => {
  let entered;
  const started = new Promise(resolve => { entered = resolve; });
  const f = await fixture({ agentRunner: ({ signal }) => new Promise(resolve => {
    entered();
    signal.addEventListener('abort', () => resolve({ exit_code: 130, duration_ms: 1, cancelled: true }), { once: true });
  }) });
  try {
    const id = f.store.createTask({ kind: 'duel', title: 'cancel fixture', prompt: 'Inspect', apply_to_project: false });
    const executing = f.owner.execute(id);
    await started;
    const receipt = await f.owner.cancel(id);
    await expect(executing).rejects.toThrow('cancelled');
    expect(receipt.state).toBe('cancelled');
    expect(f.store.task(id).state).toBe('cancelled');
    expect(f.store.summary().locks).toEqual([]);
  } finally { f.cleanup(); }
});

test('STOP refuses queued work without claiming it or invoking an agent', async () => {
  const f = await fixture();
  try {
    const id = f.store.createTask({ kind: 'claude', title: 'paused fixture', prompt: 'Inspect', apply_to_project: false });
    writeFileSync(join(f.scratch, 'STOP'), '1');
    await expect(f.owner.execute(id)).rejects.toThrow('STOP');
    expect(f.store.task(id).state).toBe('queued');
    expect(f.called).toEqual([]);
  } finally { f.cleanup(); }
});

test('STOP appearing after child start blocks later stages and releases the lane', async () => {
  let stopPath;
  const f = await fixture({ agentRunner: async ({ stdoutPath, finalPath }) => {
    await Bun.sleep(1);
    writeFileSync(stopPath, '1');
    writeFileSync(stdoutPath, '{"type":"result","result":"fixture evidence"}\n');
    writeFileSync(finalPath, 'fixture evidence');
    return { exit_code: 0, duration_ms: 1 };
  } });
  stopPath = join(f.scratch, 'STOP');
  try {
    const id = f.store.createTask({ kind: 'duel', title: 'midflight STOP fixture', prompt: 'Inspect', apply_to_project: false });
    await expect(f.owner.execute(id)).rejects.toThrow('STOP');
    expect(f.store.task(id).state).toBe('blocked');
    expect(f.store.taskStatus(id).runs.length).toBeLessThanOrEqual(2);
    expect(f.store.summary().locks).toEqual([]);
  } finally { f.cleanup(); }
});

test('review, verify and recorded-session resume retain their distinct real owner paths', async () => {
  const tools = [], estate = { service: 'fixture-native-estate', missions: 2 };
  const f = await fixture({ estateStatus: async () => estate,
    projectTool: async name => { tools.push(name); return { tool: name, state: 'fixture-observed' }; } });
  try {
    const reviewId = f.store.createTask({ kind: 'review', title: 'review fixture', prompt: 'Focus security', apply_to_project: false });
    const review = await f.owner.execute(reviewId);
    expect(Object.keys(review.reviews)).toEqual(['claude', 'codex']);
    expect(f.store.task(reviewId).state).toBe('completed');

    const verifyId = f.store.createTask({ kind: 'verify', title: 'verify fixture', prompt: 'A fixture claim', apply_to_project: false });
    const verified = await f.owner.execute(verifyId);
    expect(verified.estate).toEqual(estate);
    expect(Object.keys(verified.verifiers)).toEqual(['claude', 'codex']);
    expect(tools).toEqual(['mesh_status', 'release_attest']);

    const directId = f.store.createTask({ kind: 'codex', title: 'create session', prompt: 'Inspect', apply_to_project: false });
    await f.owner.execute(directId);
    const resumeId = f.store.createTask({ kind: 'resume-codex', title: 'fixture-codex', prompt: 'Continue', apply_to_project: false });
    await f.owner.execute(resumeId);
    const resumedArgv = JSON.parse(f.store.taskStatus(resumeId).runs[0].command_json);
    expect(resumedArgv.slice(0, 4)).toEqual(['codex', 'exec', 'resume', 'fixture-codex']);
    expect(f.store.task(resumeId).state).toBe('completed');
  } finally { f.cleanup(); }
});

test('PR convergence gathers bounded GitHub evidence and runs isolated relay without pushing', async () => {
  const ghCalls = [];
  const f = await fixture({ withGit: true,
    run: async (argv, options) => {
      if (argv[0] !== 'gh') return projectRun(argv, options);
      ghCalls.push(argv);
      const stdout = argv[1] === 'pr' && argv[2] === 'view' ? JSON.stringify({ number: 7, title: 'Fixture PR' })
        : argv[1] === 'repo' ? JSON.stringify({ nameWithOwner: 'fixture/Bonfyre' }) : '[]';
      return { argv, exit_code: 0, stdout, stderr: '', duration_ms: 1 };
    },
    agentRunner: async ({ argv, cwd, stdoutPath, finalPath }) => {
      const prompt = promptOf(argv);
      if (prompt.includes('Implement the mission')) writeFileSync(join(cwd, 'pr-change.txt'), 'fixture PR change\n');
      const final = prompt.includes('Return ONLY JSON') ? verdict : 'fixture result';
      writeFileSync(stdoutPath, `${JSON.stringify({ type: 'result', result: final })}\n`);
      writeFileSync(finalPath, final);
      return { exit_code: 0, duration_ms: 1 };
    } });
  try {
    const id = f.store.createTask({ kind: 'pr', title: 'current PR', prompt: 'Converge fixture PR', apply_to_project: false });
    const result = await f.owner.execute(id);
    expect(result.pr_evidence.view.number).toBe(7);
    expect(f.store.task(id).state).toBe('completed');
    expect(ghCalls.map(argv => argv.slice(0, 3).join(' '))).toContain('gh pr view');
    expect(ghCalls.every(argv => !argv.includes('push'))).toBe(true);
    expect(f.store.taskStatus(id).runs).toHaveLength(5);
    expect(result.repair.state).toBe('skipped');
  } finally { f.cleanup(); }
});

test('ambiguous Project MCP apply leaves an uncertain task and an attempt receipt, never retries', async () => {
  const tools = [];
  const f = await fixture({ withGit: true,
    agentRunner: async ({ argv, cwd, stdoutPath, finalPath }) => {
      const prompt = promptOf(argv);
      if (prompt.includes('Implement the mission')) writeFileSync(join(cwd, 'uncertain.txt'), 'fixture change\n');
      const final = prompt.includes('Return ONLY JSON') ? verdict : 'fixture result';
      writeFileSync(stdoutPath, `${JSON.stringify({ type: 'result', result: final })}\n`);
      writeFileSync(finalPath, final);
      return { exit_code: 0, duration_ms: 1 };
    },
    projectTool: async name => {
      tools.push(name);
      if (name === 'project_patch_preview') return { low_risk_eligible: true };
      throw new Error('fixture transport lost after write');
    } });
  try {
    const id = f.store.createTask({ kind: 'relay', title: 'uncertain fixture', prompt: 'Add fixture change', apply_to_project: true });
    await expect(f.owner.execute(id)).rejects.toThrow('reconcile receipt');
    expect(f.store.task(id).state).toBe('uncertain');
    expect(tools).toEqual(['project_patch_preview', 'project_patch_preview', 'project_apply_low_risk_patch']);
    expect(f.store.taskStatus(id).receipts.map(item => item.kind)).toContain('project-apply-attempt');
    expect(f.store.taskStatus(id).receipts.map(item => item.kind)).toContain('project-apply-uncertain');
    expect(f.store.summary().locks).toEqual([]);
  } finally { f.cleanup(); }
});

test('cancellation reaches a bounded non-agent GitHub subprocess before PR relay begins', async () => {
  let entered;
  const started = new Promise(resolve => { entered = resolve; });
  const f = await fixture({ run: (_argv, { signal }) => new Promise(resolve => {
    entered();
    signal.addEventListener('abort', () => resolve({ exit_code: 130, stdout: '', stderr: '', duration_ms: 1, cancelled: true }), { once: true });
  }) });
  try {
    const id = f.store.createTask({ kind: 'pr', title: 'cancel PR fixture', prompt: 'Converge', apply_to_project: false });
    const executing = f.owner.execute(id);
    await started;
    await f.owner.cancel(id);
    await expect(executing).rejects.toThrow('cancelled');
    expect(f.store.task(id).state).toBe('cancelled');
    expect(f.store.taskStatus(id).runs).toHaveLength(0);
  } finally { f.cleanup(); }
});

test('real subprocess runner kills an ignoring grandchild process group on cancellation', async () => {
  let marker, grandchildPid;
  const f = await fixture({ useDefaultRunner: true, policyOverride: scratch => {
    marker = join(scratch, 'grandchild-survived');
    grandchildPid = join(scratch, 'grandchild.pid');
    const childScript = join(scratch, 'ignoring-child.mjs'), launcher = join(scratch, 'fixture-agent.mjs');
    writeFileSync(childScript, `process.on('SIGTERM', () => {}); setTimeout(() => Bun.write(${JSON.stringify(marker)}, 'survived'), 1200); setInterval(() => {}, 60000);\n`);
    writeFileSync(launcher, `#!${process.execPath}\nimport { spawn } from 'node:child_process';\nconst child = spawn(process.execPath, [${JSON.stringify(childScript)}], { stdio: 'ignore' });\nawait Bun.write(${JSON.stringify(grandchildPid)}, String(child.pid));\nprocess.on('SIGTERM', () => process.exit(0));\nsetInterval(() => {}, 60000);\n`);
    chmodSync(launcher, 0o700);
    return { agents: { claude: { ...policy().agents.claude, command: launcher } } };
  } });
  try {
    const id = f.store.createTask({ kind: 'claude', title: 'process group fixture', prompt: 'Inspect', apply_to_project: false });
    const executing = f.owner.execute(id);
    for (let attempt = 0; attempt < 80 && !existsSync(grandchildPid); attempt++) await Bun.sleep(25);
    expect(existsSync(grandchildPid)).toBe(true);
    await f.owner.cancel(id);
    await expect(executing).rejects.toThrow('cancelled');
    await Bun.sleep(1300);
    expect(existsSync(marker)).toBe(false);
    expect(f.store.task(id).state).toBe('cancelled');
  } finally {
    if (existsSync(grandchildPid)) {
      try { process.kill(Number(readFileSync(grandchildPid, 'utf8')), 'SIGKILL'); } catch { /* Already reaped. */ }
    }
    f.cleanup();
  }
});

test('real subprocess output overflow never becomes a successful dispatch', async () => {
  const f = await fixture({ useDefaultRunner: true, policyOverride: scratch => {
    const launcher = join(scratch, 'noisy-agent.mjs');
    writeFileSync(launcher, `#!${process.execPath}\nprocess.stdout.write('x'.repeat(65 * 1024 * 1024)); setInterval(() => {}, 60000);\n`);
    chmodSync(launcher, 0o700);
    return { agents: { codex: { ...policy().agents.codex, command: launcher } } };
  } });
  try {
    const id = f.store.createTask({ kind: 'codex', title: 'overflow fixture', prompt: 'Inspect', apply_to_project: false });
    const result = await f.owner.execute(id);
    expect(result.state).toBe('failed');
    expect(result.overflow).toBe(true);
    expect(f.store.task(id).state).toBe('failed');
    expect(f.store.taskStatus(id).runs[0].state).toBe('failed');
  } finally { f.cleanup(); }
});

test('real subprocess deadline terminates its process group and records timeout', async () => {
  const f = await fixture({ useDefaultRunner: true, policyOverride: scratch => {
    const launcher = join(scratch, 'idle-agent.mjs');
    writeFileSync(launcher, `#!${process.execPath}\nsetInterval(() => {}, 60000);\n`);
    chmodSync(launcher, 0o700);
    return { timeouts_seconds: { single_agent: 1 }, agents: { claude: { ...policy().agents.claude, command: launcher } } };
  } });
  try {
    const id = f.store.createTask({ kind: 'claude', title: 'timeout fixture', prompt: 'Inspect', apply_to_project: false });
    const result = await f.owner.execute(id);
    expect(result.state).toBe('timeout');
    expect(result.timeout).toBe(true);
    expect(f.store.task(id).state).toBe('failed');
    expect(f.store.taskStatus(id).runs[0].state).toBe('timeout');
  } finally { f.cleanup(); }
});

test('a stale selected source fails before any agent call or Project request',async()=>{
  const calls=[];const f=await fixture({withGit:true,projectTool:async name=>{calls.push(name);return {};}});
  try{
    const id=f.store.createTask({kind:'relay',title:'stale source',prompt:'fixture',apply_to_project:true,
      source_files:[{path:'README.md',sha256:'0'.repeat(64)}]});
    await expect(f.owner.execute(id)).rejects.toThrow('source hash changed');expect(f.store.task(id).state).toBe('failed');expect(f.called).toHaveLength(0);expect(calls).toEqual([]);expect(f.store.summary().locks).toEqual([]);
  }finally{f.cleanup();}
});

test('a selected input changing during final review retains the candidate and refuses Project apply',async()=>{
  const calls=[];let f;
  f=await fixture({withGit:true,projectTool:async name=>{calls.push(name);return {low_risk_eligible:true,applied:true,receipt:{receipt_id:'fixture'}};},
    agentRunner:async({argv,cwd,stdoutPath,finalPath})=>{
      const prompt=promptOf(argv);
      if(prompt.startsWith('Implement the mission'))writeFileSync(join(cwd,'README.md'),'agent change\n');
      if(prompt.startsWith('Independently verify'))writeFileSync(join(f.root,'context.txt'),'new user context\n');
      writeFileSync(stdoutPath,'');writeFileSync(finalPath,prompt.includes('Return ONLY JSON')?verdict:'fixture');return {exit_code:0,duration_ms:1};
    }});
  try{
    writeFileSync(join(f.root,'context.txt'),'selected context\n');
    const source_files=['README.md','context.txt'].map(path=>({path,sha256:createHash('sha256').update(readFileSync(join(f.root,path))).digest('hex')}));
    const id=f.store.createTask({kind:'relay',title:'changed context',prompt:'fixture',source_files,apply_to_project:true});
    const result=await f.owner.execute(id);expect(f.store.task(id).state).toBe('review_required');expect(result.apply_result.reason).toBe('selected_source_changed');
    expect(result.diff.selected_source_unchanged).toBe(false);expect(calls).not.toContain('project_apply_low_risk_patch');
    expect(readFileSync(join(f.root,'README.md'),'utf8')).toBe('fixture repository\n');expect(readFileSync(join(f.root,'context.txt'),'utf8')).toBe('new user context\n');
    expect(existsSync(result.patch_receipt.path)).toBe(true);
  }finally{f.cleanup();}
});


test('low disk blocks a new relay before worktree creation or paid agent work',async()=>{
 const f=await fixture({withGit:true,diskAvailableBytes:()=>512*1024*1024});
 try{
  const id=f.store.createTask({kind:'relay',title:'disk refusal',prompt:'Do not run agents',apply_to_project:false});
  await expect(f.owner.execute(id)).rejects.toThrow(/disk runway below policy/);
  expect(f.store.task(id)).toMatchObject({state:'blocked'});
  expect(f.called).toHaveLength(0);
  expect(f.store.summary().locks).toEqual([]);
  expect(existsSync(join(f.home,'worktrees',id))).toBe(false);
 }finally{f.cleanup();}
});
test('healthy disk permits the existing governed relay path',async()=>{
 const f=await fixture({withGit:true,diskAvailableBytes:()=>4n*1024n*1024n*1024n});
 try{
  const id=f.store.createTask({kind:'relay',title:'disk healthy',prompt:'A fixture',apply_to_project:false});
  const result=await f.owner.execute(id);
  expect(result.tests.every(row=>row.exit_code===0)).toBe(true);
  expect(f.called.length).toBeGreaterThan(0);
 }finally{f.cleanup();}
});
