import { test, expect } from 'bun:test';
import { mkdtempSync, mkdirSync, readFileSync, rmSync, existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createOperatorTools, requiredTests } from '../operator_tools.mjs';
import { createProjectOperator } from '../project_operator.mjs';
import { projectRun } from '../project_runtime.mjs';
import { CATALOG } from '../project_node.mjs';

const policy = { mode: 'power', auto_apply_low_risk_patches: true, allow_operator_backlog_compaction: true, default_tests: ['git-diff-check'], oracle_operator_flows: ['local_oracle_execution_matrix'] };
const listed = CATALOG.tools.map(({ name }) => ({ name }));
function harness(tools = listed, call = async () => ({ ok: true })) {
  const actions = [];
  const owner = createOperatorTools({ mcpCall: call, listTools: async () => tools,
    recordAction: async (item) => { actions.push(item); return actions.length; },
    completeAction: async ({ seq, state, result }) => { Object.assign(actions[seq - 1], { state, result }); },
    now: () => 1_760_000_000 });
  return { owner, actions };
}

test('catalog intersects trusted Project and rich schemas, never trusts advertised annotations or unknown names', async () => {
  const { owner } = harness([
    { name: 'oracle_service_status', annotations: { readOnlyHint: false }, inputSchema: { type: 'object', properties: { evil: {} } } },
    { name: 'oracle_profile_run', annotations: { readOnlyHint: true } },
    { name: 'oracle_deep_flow' }, { name: 'task_start' }, { name: 'made_up_effect', annotations: { readOnlyHint: true } },
    { name: 'project_apply_patch' }, { name: 'project_test_profile' }, { name: 'cms_record' }, { name: 'operator_backlog_audit' }, { name: 'operator_goal_supersede' },
  ]);
  const result = await owner.catalog(policy);
  expect(result.tools.map((tool) => tool.name)).toEqual(expect.arrayContaining(['oracle_service_status', 'operator_backlog_audit', 'operator_goal_supersede']));
  expect(result.tools.map((tool) => tool.name)).toContain('oracle_profile_run');
  expect(result.tools.map((tool) => tool.name)).toContain('oracle_deep_flow');
  expect(result.tools.map((tool) => tool.name)).toContain('task_start');
  expect(result.tools.map((tool) => tool.name)).not.toContain('made_up_effect');
  expect(result.tools.map((tool) => tool.name)).not.toContain('project_apply_patch');
  expect(result.tools.map((tool) => tool.name)).toContain('project_test_profile');
  expect(result.catalog.oracle_service_status.annotations.readOnlyHint).toBe(true);
  expect(result.catalog.oracle_profile_run.annotations.readOnlyHint).toBe(false);
  expect(result.tools.find((tool) => tool.name === 'oracle_service_status').parameters.properties).not.toHaveProperty('evil');
  expect(result.tools.find((tool) => tool.name === 'cms_record').parameters.properties.entry_id.type).toBe('string');
  expect(result.tools.at(-2).name).toBe('operator_finish');
  expect(result.tools.at(-1).name).toBe('operator_add_goal');
  const low = await owner.catalog({ mode: 'read' });
  expect(low.tools.map((tool) => tool.name)).not.toContain('oracle_profile_run');
  expect(low.tools.map((tool) => tool.name)).not.toContain('oracle_deep_flow');
  expect(low.tools.map((tool) => tool.name)).not.toContain('task_start');
  expect(low.tools.map((tool) => tool.name)).not.toContain('operator_goal_supersede');expect((await owner.execute('read-only', 'operator_goal_supersede', { goal_ids: ['0123456789abcdef0123456789abcdef'], reason: 'fixture' }, { mode: 'read' })).error).toMatch(/not delegated/);
});

test('maturation ledger separates inspection from gated atomic seed and verdict effects', async()=>{
  const seen=[];
  const {owner}=harness([{name:'operator_maturation_ledger'}],
    async(name,args)=>{seen.push(args.action);return {action:args.action};});
  await owner.catalog(policy);
  expect((await owner.execute('run','operator_maturation_ledger',{action:'coverage'},policy)).error).toBeUndefined();
  expect((await owner.execute('run','operator_maturation_ledger',{action:'read',limit:30},policy)).error).toBeUndefined();
  expect((await owner.execute('run','operator_maturation_ledger',{action:'seed',expected_sha256:'1'.repeat(64)},policy)).error).toBeUndefined();
  const denied=await owner.execute('run','operator_maturation_ledger',{action:'record',expected_sha256:'1'.repeat(64),updates:[]},{mode:'read',auto_apply_low_risk_patches:true});
  expect(denied.error).toMatch(/require power-mode/);
  expect(seen).toEqual(['coverage','read','seed']);
});

test('unexpected composite exception after native apply remains outcome uncertain', async () => {
  const calls = [], actions = [];
  const owner = createOperatorTools({ listTools: async () => [{ name: 'project_apply_low_risk_patch' }],
    mcpCall: async (name) => {
      calls.push(name);
      if (name === 'project_patch_preview') return { valid: true, low_risk_eligible: true, metadata: { files: [{ path: 'scripts/hello.txt' }] } };
      if (name === 'project_apply_low_risk_patch') return { get applied() { throw new Error('receipt decoding failed after apply'); } };
      throw new Error('unexpected owner');
    },
    recordAction: async (item) => { actions.push(item); return 1; },
    completeAction: async ({ state, result }) => { Object.assign(actions[0], { state, result }); } });
  await owner.catalog(policy);
  const result = await owner.execute('run', 'project_apply_low_risk_patch', { patch }, policy);
  expect(result.error).toMatch(/receipt decoding failed/);
  expect(actions[0].state).toBe('outcome_uncertain');
  expect(calls).toEqual(['project_patch_preview', 'project_apply_low_risk_patch']);
});

test('recursive local CMS read is admitted, but a write requires an explicit effect grant', async () => {
  const calls = [];
  const { owner } = harness([{ name: 'cms_record' }], async (name, args) => { calls.push({ name, args }); return { native_exit: 0 }; });
  await owner.catalog(policy);
  await owner.execute('run', 'cms_record', { action: 'get', content_type: 'story', entry_id: 'entry-1' }, policy);
  expect(calls).toHaveLength(1);
  const denied = await owner.execute('run', 'cms_record', { action: 'delete', content_type: 'story', entry_id: 'entry-1' }, policy);
  expect(denied.error).toMatch(/not delegated/);
  expect(calls).toHaveLength(1);
});

test('policy and exact schemas gate Oracle flow/task execution; nested errors are failures with epoch-second audit', async () => {
  const calls = [];
  const { owner, actions } = harness([{ name: 'oracle_deep_flow' }, { name: 'task_start' }, { name: 'oracle_service_status' }], async (name, args) => {
    calls.push({ name, args });
    return name === 'oracle_service_status' ? { result: { errors: ['unavailable'] } } : { state: 'success' };
  });
  await owner.catalog(policy);
  expect((await owner.execute('run', 'oracle_deep_flow', { flow: 'max_compounding_cycle' }, policy)).error).toMatch(/does not allow/);
  expect((await owner.execute('run', 'task_start', { kind: 'flow', name: 'not-listed' }, policy)).error).toMatch(/does not allow/);
  await owner.execute('run', 'oracle_deep_flow', { flow: 'local_oracle_execution_matrix' }, policy);
  await owner.execute('run', 'oracle_service_status', { unit: 'bonfyred.service' }, policy);
  expect(calls.map((item) => item.name)).toEqual(['oracle_deep_flow', 'oracle_service_status']);
  expect(actions.at(-1)).toMatchObject({ run_id: 'run', tool: 'oracle_service_status', state: 'failed', started_at: 1_760_000_000 });
  expect(actions.at(-1)).not.toHaveProperty('startedAt');
  expect(actions.at(-1).policy).toEqual(policy);
  expect((await owner.execute('run', 'unlisted_name', {}, policy)).error).toMatch(/unknown or unobserved/);
});

test('durable started row precedes MCP; audit outage refuses effect and remote throw retains uncertainty', async () => {
  const calls = [], actions = [];
  const owner = createOperatorTools({ listTools: async () => [{ name: 'oracle_profile_run' }],
    mcpCall: async () => { calls.push('invoked'); throw new Error('remote connection dropped'); },
    recordAction: async (item) => { expect(calls).toHaveLength(0); actions.push(item); return 1; },
    completeAction: async ({ seq, state, result }) => { expect(seq).toBe(1); Object.assign(actions[0], { state, result }); },
    now: () => 1_760_000_000 });
  await owner.catalog(policy);
  const result = await owner.execute('run', 'oracle_profile_run', { profile: 'truth-replay' }, policy);
  expect(result.error).toMatch(/connection dropped/);
  expect(actions[0].state).toBe('outcome_uncertain');
  expect(calls).toHaveLength(1);
  const denied = createOperatorTools({ listTools: async () => [{ name: 'oracle_profile_run' }],
    mcpCall: async () => { calls.push('must not invoke'); },
    recordAction: async () => { throw new Error('store unavailable'); },
    completeAction: async () => {}, now: () => 1_760_000_000 });
  await denied.catalog(policy);
  expect((await denied.execute('run', 'oracle_profile_run', { profile: 'truth-replay' }, policy)).error).toMatch(/audit unavailable/);
  expect(calls).toHaveLength(1);
});

test('completion-ledger failure never reports a successful effect as complete', async () => {
  const rows = [], calls = [];
  const owner = createOperatorTools({ listTools: async () => [{ name: 'oracle_profile_run' }],
    mcpCall: async () => { calls.push('effect'); return { state: 'success' }; },
    recordAction: async (item) => { rows.push(item); return 1; },
    completeAction: async () => { throw new Error('store write failed'); } });
  await owner.catalog(policy);
  const result = await owner.execute('run', 'oracle_profile_run', { profile: 'truth-replay' }, policy);
  expect(result).toMatchObject({ outcome_uncertain: true, error: 'action completion audit unavailable' });
  expect(rows[0].state).toBe('started');
  expect(calls).toEqual(['effect']);
});

test('required tests refuse Python and use Bun syntax instead of a Python interpreter', () => {
  expect(requiredTests({ files: [{ path: 'scripts/example.mjs' }, { path: 'scripts/example.sh' }] }, policy))
    .toEqual([{ profile: 'git-diff-check' }, { profile: 'bun-syntax', paths: ['scripts/example.mjs'], timeout_seconds: 300 },
      { profile: 'shell-syntax', paths: ['scripts/example.sh'], timeout_seconds: 300 }]);
  expect(() => requiredTests({ files: [{ path: 'scripts/legacy.py' }] }, policy)).toThrow('Python');
});

function nativeFixture() {
  const base = mkdtempSync(join(tmpdir(), 'bonfyre-operator-tools-'));
  const root = join(base, 'repo'), home = join(base, 'home');
  mkdirSync(root); mkdirSync(home);
  const init = Bun.spawnSync(['git', 'init', '-q', root], { stdout: 'pipe', stderr: 'pipe' });
  if (init.exitCode !== 0) throw new Error(init.stderr.toString());
  mkdirSync(join(root, 'scripts'));
  const native = createProjectOperator({ root, home, run: projectRun, status: async () => ({}),
    env: { ...process.env, BONFYRE_OPERATOR_HOME: join(home, 'operator'), BONFYRE_MCP_AUTONOMOUS_WRITE_ENABLED: '1' } });
  return { base, root, native, cleanup: () => rmSync(base, { recursive: true, force: true }) };
}
const patch = 'diff --git a/scripts/hello.txt b/scripts/hello.txt\nnew file mode 100644\n--- /dev/null\n+++ b/scripts/hello.txt\n@@ -0,0 +1 @@\n+hello\n';

test('real native owner preview → patch receipt → required test closes without UUID assumptions', async () => {
  const fixture = nativeFixture();
  try {
    const calls = [];
    const { owner, actions } = harness(listed, async (name, args) => {
      calls.push(name);
      return fixture.native.call(name, args);
    });
    await owner.catalog(policy);
    const result = await owner.execute('real-run', 'project_apply_low_risk_patch', { patch }, policy);
    expect(result.applied).toBe(true);
    expect(result.verified).toBe(true);
    expect(result.receipt_id).toMatch(/^patch-[0-9]+-[a-f0-9]{12}$/);
    expect(result.tests.map((item) => item.result.result.exit_code)).toEqual([0]);
    expect(readFileSync(join(fixture.root, 'scripts/hello.txt'), 'utf8')).toBe('hello\n');
    expect(calls).toEqual(['project_patch_preview', 'project_apply_low_risk_patch', 'project_test_profile']);
    expect(actions).toHaveLength(1);
    expect(actions[0].state).toBe('passed');
  } finally { fixture.cleanup(); }
});

test('a failed required test uses the native receipt for verified rollback', async () => {
  const fixture = nativeFixture();
  try {
    const { owner, actions } = harness(listed, async (name, args) => name === 'project_test_profile'
      ? { profile: args.profile, result: { exit_code: 1, errors: ['fixture failure'] } }
      : fixture.native.call(name, args));
    await owner.catalog(policy);
    const result = await owner.execute('real-run', 'project_apply_low_risk_patch', { patch }, policy);
    expect(result.verified).toBe(false);
    expect(result.reverted).toBe(true);
    expect(existsSync(join(fixture.root, 'scripts/hello.txt'))).toBe(false);
    expect(actions[0].state).toBe('reverted');
  } finally { fixture.cleanup(); }
});
test('finish tool advertises the same bounded native follow-up admission', async () => {
  const { owner } = harness();
  const exposed = await owner.catalog(policy);
  const finish = exposed.tools.find(item => item.name === 'operator_finish');
  expect(finish.parameters.properties.new_goals.maxItems).toBe(16);
});
