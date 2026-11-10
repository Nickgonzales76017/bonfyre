import { expect, test } from 'bun:test';
import { mkdtempSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { projectRun } from '../project_runtime.mjs';
import { hypervisorCli, waitHypervisorMission, createHypervisorCliClient, hypervisorExitCode } from '../hypervisor_cli.mjs';
const call = async (name, args) => ({ name, args });
test('native CLI uses the same operation schemas for positional and typed flags', async () => {
  expect(await hypervisorCli(['task-execute', 'm-one', 'T-001'], { call })).toEqual({ name: 'task_execute', args: { mission_id: 'm-one', task_id: 'T-001' } });
  expect(await hypervisorCli(['compile', 'Goal', '--constraint', 'One', '--constraint', 'Two', '--capability', 'agent.executor', '--tokens', '1000'], { call }))
    .toEqual({ name: 'mission_compile', args: { goal: 'Goal', constraints: ['One', 'Two'], requested_capabilities: ['agent.executor'], budget_tokens: 1000 } });
  expect(await hypervisorCli(['mission-route', 'm-one', '--no-shadow'], { call })).toEqual({ name: 'mission_route', args: { mission_id: 'm-one', include_shadow: false } });
  expect(await hypervisorCli(['evaluate', 'native', '--capability', 'agent.executor'], { call })).toEqual({ name: 'provider_evaluate', args: { provider_id: 'native', capability: 'agent.executor' } });
  expect(await hypervisorCli(['compare', 'agent.executor', '--provider', 'a', '--provider', 'b'], { call })).toEqual({ name: 'provider_compare', args: { capability: 'agent.executor', providers: ['a', 'b'] } });
  expect(await hypervisorCli(['--version'])).toBe('bonfyre-hypervisor 6.1.0');
});
test('installed positional and flag aliases preserve all formerly missing command contracts', async () => {
  expect(await hypervisorCli(['fleet-launch', '/project', 'Title', 'codex', 'Message', '--group', 'team', '--parent', 'p', '--mcp', 'one', '--mcp', 'two'], { call }))
    .toEqual({ name: 'fleet_launch', args: { path: '/project', title: 'Title', command: 'codex', message: 'Message', group: 'team', parent: 'p', mcps: ['one', 'two'] } });
  expect(await hypervisorCli(['caveman-code', 'Goal', '--goal'], { call })).toEqual({ name: 'caveman_code_run', args: { prompt: 'Goal', goal_mode: true } });
  expect(await hypervisorCli(['evaluate', 'p', '--extended'], { call })).toEqual({ name: 'provider_evaluate', args: { provider_id: 'p', execute_extended: true } });
  expect(await hypervisorCli(['install-plan', 'fleet'], { call })).toEqual({ name: 'install_plan', args: { profile: 'fleet' } });
  expect(await hypervisorCli(['memory-search', '--', '--help'], { call })).toEqual({ name: 'memory_search', args: { query: '--help' } });
  expect(await hypervisorCli(['context', 'm', '--tokens=1000'], { call })).toEqual({ name: 'context_compile', args: { mission_id: 'm', budget_tokens: 1000 } });
  for (const args of [['install-plan', 'invalid'], ['fleet-action', 'destroy', 's'], ['context', 'm', '--tokens', '1.0'], ['execute', 'm', '--wait', '--interval', '0'], ['execute', 'm', '--interval', '2']])
    await expect(hypervisorCli(args, { call })).rejects.toThrow();
});
test('execute --wait dispatches once, then polls status and emits only changed states before final result', async () => {
  const calls = [], output = [], states = ['queued', 'running', 'running', 'resolved']; let clock = 0;
  const result = await hypervisorCli(['execute', 'm', '--wait', '--interval', '1'], { emit: value => output.push(value), now: () => clock,
    sleep: async ms => { clock += ms; }, call: async (name, args) => { calls.push({ name, args }); return name === 'mission_execute' ? { mission_id: 'm', state: 'queued' } : { mission_id: 'm', state: states.shift(), tasks: [{ id: 'T', state: 'planned' }] }; } });
  expect(result.state).toBe('resolved'); expect(calls.filter(item => item.name === 'mission_execute')).toHaveLength(1);
  expect(calls.filter(item => item.name === 'mission_status')).toHaveLength(4); expect(output.map(x => x.state)).toEqual(['queued', 'queued', 'running', 'resolved']);
});
test('wait terminal, identity, deadline and poll-budget boundaries never re-execute', async () => {
  for (const state of ['resolved', 'blocked', 'retry', 'failed', 'cancelled']) {
    const observed = []; const result = await hypervisorCli(['wait', 'm'], { call: async (name, args) => { observed.push(name); return { mission_id: args.mission_id, state, tasks: [] }; } });
    expect(result.state).toBe(state); expect(observed).toEqual(['mission_status']);
  }
  await expect(waitHypervisorMission('m', { call: async () => ({ mission_id: 'other', state: 'resolved' }) })).rejects.toThrow('invalid mission');
  let clock = 0, polls = 0;
  await expect(waitHypervisorMission('m', { timeout: 1, interval: 1, now: () => clock, sleep: async ms => { clock += ms; }, call: async () => { polls++; return { state: 'running' }; } })).rejects.toThrow('deadline'); expect(polls).toBe(1);
  await expect(waitHypervisorMission('m', { maxPolls: 2, sleep: async () => {}, call: async () => ({ state: 'running' }) })).rejects.toThrow('poll budget');
});
test('cancellation and stalled status calls terminate the wait without a second execution or poll', async () => {
  const controller = new AbortController(); let calls = 0, receivedSignal;
  const pending = waitHypervisorMission('m', { signal: controller.signal, call: async (_name, _args, opts) => { calls++; receivedSignal = opts.signal; return new Promise(() => {}); } });
  await Bun.sleep(5); controller.abort(new Error('fixture cancellation')); await expect(pending).rejects.toThrow('fixture cancellation');
  expect(calls).toBe(1); expect(receivedSignal.aborted).toBe(true);
  await expect(waitHypervisorMission('m', { timeout: 0.02, call: async () => new Promise(() => {}) })).rejects.toThrow('deadline');
});
test('events command uses bounded authenticated GET and validates cursors without MCP execution', async () => {
  const requests = [], client = createHypervisorCliClient({ base: 'http://127.0.0.1:3054', token: 'fixture-secret', request: async (url, options) => {
    requests.push({ url, options }); return Response.json({ events: [{ seq: 3, payload: {} }], next_after: 3 });
  } });
  expect(await hypervisorCli(['events', '--after', '2', '--limit=10'], client)).toEqual({ events: [{ seq: 3, payload: {} }], next_after: 3 });
  expect(requests[0].url).toBe('http://127.0.0.1:3054/events?after=2&limit=10'); expect(requests[0].options.headers.authorization).toBe('Bearer fixture-secret');
  await expect(hypervisorCli(['events', '--limit', '2001'], client)).rejects.toThrow('bounds'); expect(requests).toHaveLength(1);
  const hung = createHypervisorCliClient({ base: 'http://127.0.0.1:3054', token: 'fixture-secret', request: async () => new Promise(() => {}) });
  await expect(hung.events({ after: 0, limit: 1 }, { timeoutMs: 10 })).rejects.toThrow('timed out');
});
test('method deadlines and terminal exit status preserve the installed contracts within explicit shared runtime cap', async () => {
  const seen = [], observer = async (name, args, options) => { seen.push({ name, options }); return { name, args }; };
  await hypervisorCli(['memory-search', 'q'], { call: observer }); await hypervisorCli(['compact', 'git status', '--timeout', '100'], { call: observer });
  await hypervisorCli(['task-execute', 'm', 't'], { call: observer });
  expect(seen.map(row => row.options.timeoutMs)).toEqual([300000, 160000, 3600000]);
  for (const state of ['passed', 'completed', 'resolved']) expect(hypervisorExitCode(['task-execute'], { state })).toBe(0);
  expect(hypervisorExitCode(['task-execute'], { state: 'uncertain' })).toBe(1);
  expect(hypervisorExitCode(['wait'], { state: 'failed' })).toBe(0); // Historical wait prints disposition; task-execute is the enforcing CLI.
  expect(hypervisorExitCode(['replay'], { verified: false })).toBe(1); // Intentional stronger integrity check.
});
test('CLI refuses ambiguous options and malformed numbers before contacting an owner', async () => {
  let calls = 0; const forbidden = async () => { calls++; };
  for (const args of [['task-execute', 'm'], ['status', 'extra'], ['context', 'm', '--tokens', 'NaN'],
    ['context', 'm', '--tokens', '10', '--tokens', '20'], ['status', '--host', 'evil'], ['context', 'm', '--tokens']])
    await expect(hypervisorCli(args, { call: forbidden })).rejects.toThrow();
  expect(calls).toBe(0);
});
test('real Bun CLI entrypoint exposes help/version and returns parser exit2 without contacting any service', async () => {
  const home = mkdtempSync(join(tmpdir(), 'hypervisor-cli-entry-')), tokenFile = join(home, 'token'); writeFileSync(tokenFile, 'fixture-token');
  const path = fileURLToPath(new URL('../hypervisor_cli.mjs', import.meta.url));
  const env = { ...process.env, BONFYRE_HYPERVISOR_TOKEN_FILE: tokenFile, BONFYRE_HYPERVISOR_URL: 'http://127.0.0.1:1' };
  try {
    const help = await projectRun([process.execPath, path, '--help'], { env, timeoutMs: 5000 }); expect(help.exit_code).toBe(0); expect(help.stdout).toContain('wait|events');
    const version = await projectRun([process.execPath, path, '--version'], { env, timeoutMs: 5000 }); expect(version.exit_code).toBe(0); expect(version.stdout).toBe('bonfyre-hypervisor 6.1.0\n');
    for (const args of [[], ['unknown-command'], ['status', '--host', 'evil'], ['execute', 'm', '--wait', '--interval', '0']]) {
      const result = await projectRun([process.execPath, path, ...args], { env: { ...env, BONFYRE_HYPERVISOR_TOKEN_FILE: join(home, 'absent') }, timeoutMs: 5000 });
      expect(result.exit_code).toBe(2); expect(result.stderr).not.toContain('HTTP'); expect(result.stderr).not.toContain('ENOENT');
    }
  } finally { rmSync(home, { recursive: true, force: true }); }
});
