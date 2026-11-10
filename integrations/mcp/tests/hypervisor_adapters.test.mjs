import { afterEach, expect, test } from 'bun:test';
import { chmodSync, copyFileSync, existsSync, mkdirSync, mkdtempSync, readFileSync, realpathSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { createHypervisorAdapters, compactAdapterArgv, splitAdapterCommand } from '../hypervisor_adapters.mjs';
import { projectRun } from '../project_runtime.mjs';
const homes = [];
afterEach(() => { for (const home of homes.splice(0)) rmSync(home, { recursive: true, force: true }); });
function fixture(options = {}) {
  const home = realpathSync(mkdtempSync(join(tmpdir(), 'bonfyre-adapters-'))); homes.push(home);
  const bin = join(home, 'bin'); mkdirSync(bin); const calls = [], receipts = [];
  for (const name of ['cavemem', 'rtk', 'bernstein', 'agent-deck', 'caveman-code', 'invoker-cli', 'git', 'cargo']) { copyFileSync('/bin/echo', join(bin, name)); chmodSync(join(bin, name), 0o700); }
  const store = { home, provider: () => ({ license_approved: false }), writeReceipt: value => receipts.push(structuredClone(value)) };
  const run = async (argv, args) => { calls.push({ argv, args }); return { exit_code: 0, stdout: 'private-provider-output', stderr: '', duration_ms: 1 }; };
  const owner = createHypervisorAdapters({ store, root: home, env: { PATH: bin, GIT_DIR: '/wrong', GIT_CONFIG_COUNT: '99' }, run, ...options });
  const manifest = join(home, 'workflow.yaml'); writeFileSync(manifest, 'nodes: []\n');
  return { home, bin, calls, receipts, store, owner, manifest };
}
test('construction is lazy and all provider status probes redact raw output', async () => {
  const f = fixture(); expect(f.calls).toHaveLength(0); expect(f.receipts).toHaveLength(0); expect(existsSync(join(f.home, 'adapters'))).toBe(false);
  const status = await f.owner.status(); expect(Object.keys(status)).toHaveLength(6); expect(f.calls).toHaveLength(6);
  expect(Object.values(status).every(value => value.state === 'reachable')).toBe(true); expect(JSON.stringify(status)).not.toContain('private-provider-output');
});
test('memory search and status preserve real raw result with precise non-timeline limitation', async () => {
  const f = fixture(); expect((await f.owner.memorySearch('quoted query', { limit: 7, semantic: false })).result.stdout).toBe('private-provider-output');
  expect(f.calls[0].argv.slice(1)).toEqual(['search', 'quoted query', '--limit', '7', '--no-semantic']);
  expect((await f.owner.memorySessions()).limitation).toContain('not a session');
  await expect(f.owner.memorySearch('--execute')).rejects.toThrow('option'); expect(f.calls).toHaveLength(2);
});
test('compact parsing refuses arbitrary execution, option escapes and shell evaluation', () => {
  expect(splitAdapterCommand('git diff "a b"')).toEqual(['git', 'diff', 'a b']);
  expect(compactAdapterArgv('cargo test --offline')).toEqual(['rtk', 'cargo', 'test', '--offline']);
  for (const command of ['sh -c echo', 'rtk err sh', 'python x.py', 'git -c x=y status', 'git diff --ext-diff', 'git branch -D main', 'find . -exec sh {} ;', 'rg --pre=sh x', 'git status | sh', 'git status $(x)', 'git status "unterminated']) expect(() => compactAdapterArgv(command)).toThrow();
});
test('compact execution records paired hashed evidence and sanitizes ambient Git state', async () => {
  const f = fixture(); const value = await f.owner.compactCommand('git status'); expect(value.state).toBe('passed'); expect(f.receipts.map(x => x.state)).toEqual(['started', 'passed']);
  expect(JSON.stringify(f.receipts)).not.toContain('private-provider-output'); expect(f.calls[0].args.env.GIT_DIR).toBeUndefined(); expect(f.calls[0].args.env.GIT_CONFIG_COUNT).toBe('2');
  await expect(f.owner.compactCommand('git status', { timeout: 86400 })).rejects.toThrow('timeout'); expect(f.calls).toHaveLength(1);
  writeFileSync(join(f.bin, 'git'), '#!/usr/bin/env python3\n'); await expect(f.owner.compactCommand('git status')).rejects.toThrow('not admitted'); expect(f.calls).toHaveLength(1);
});
test('Bernstein DAG compiles a bounded candidate and refuses cycles, aliases and escaping IDs', () => {
  const f = fixture(); const mission = { id: 'demo', tasks: [{ id: 'A' }, { id: 'B', dependencies: ['A'] }] };
  const value = f.owner.bernsteinCompile(mission, { task_command: 'native task {mission_id} {task_id}' });
  expect(readFileSync(value.path, 'utf8')).toBe(value.manifest); expect(value.manifest).toContain('depends_on: ["a"]'); expect(value.manifest).toContain('timeout_seconds: 3600'); expect(value.authority).toBe('candidate-manifest-only');
  expect(() => f.owner.bernsteinCompile({ id: '../bad', tasks: [] }, { task_command: 'x' })).toThrow();
  expect(() => f.owner.bernsteinCompile({ id: 'x', tasks: [{ id: 'A' }, { id: 'a' }] }, { task_command: 'x' })).toThrow('collision');
  expect(() => f.owner.bernsteinCompile({ id: 'x', tasks: [{ id: 'A', dependencies: ['B'] }, { id: 'B', dependencies: ['A'] }] }, { task_command: 'x' })).toThrow();
  expect(() => f.owner.bernsteinCompile(mission, { task_command: '{evil}' })).toThrow('placeholder');
  expect(() => f.owner.bernsteinCompile({ id: 'oversize', tasks: Array.from({ length: 150 }, (_, i) => ({ id: `t${i}` })) }, { task_command: 'x'.repeat(65536) })).toThrow('byte budget');
  expect(existsSync(join(f.home, '.bernstein', 'workflows', 'bonfyre-oversize.yaml'))).toBe(false);
});
test('Bernstein validation and execution call real command contracts; third-party Python is explicit', async () => {
  const f = fixture(); writeFileSync(join(f.bin, 'bernstein'), '#!/usr/bin/env python3\nprint("fixture only")\n');
  const validated = await f.owner.bernsteinValidate(f.manifest); expect(validated.state).toBe('passed'); expect(f.calls[0].argv.slice(1)).toEqual(['workflow', 'validate', f.manifest]);
  expect(validated.validate.runtime_boundary.kind).toBe('declared-third-party-python-cli');
  const result = await f.owner.bernsteinRun(f.manifest, 'fixture goal'); expect(result.state).toBe('passed'); expect(f.calls[2].args.timeoutMs).toBe(3600000);
  writeFileSync(join(f.bin, 'cavemem'), '#!/usr/bin/env python3\n'); await expect(f.owner.memorySessions()).rejects.toThrow('not admitted');
});
test('fleet complete action vocabulary and both Caveman modes preserve argv without shell', async () => {
  const f = fixture(); expect((await f.owner.fleetStatus()).state).toBe('passed');
  await f.owner.fleetLaunch({ path: f.home, title: 'T', command: 'codex', message: 'literal ; prompt', group: 'G', parent: 'P', mcps: ['M'] });
  expect(f.calls[2].argv.slice(1)).toEqual(['--json', 'launch', f.home, '-t', 'T', '-c', 'codex', '-m', 'literal ; prompt', '-g', 'G', '--parent', 'P', '--mcp', 'M']);
  for (const action of ['show', 'output', 'send', 'fork', 'start', 'stop', 'restart']) expect((await f.owner.fleetAction(action, 'session1', { message: 'M', title: 'T' })).state).toBe('passed');
  await expect(f.owner.fleetAction('destroy', 'x')).rejects.toThrow();
  expect((await f.owner.cavemanCode('prompt')).mode).toBe('print'); expect((await f.owner.cavemanCode('prompt', { goal_mode: true })).mode).toBe('goal');
  expect(JSON.stringify(f.receipts)).not.toContain('literal ; prompt'); expect(JSON.stringify(f.receipts)).not.toContain('"prompt"');
});
test('Invoker license is checked before any run and unknown scripts or escaped paths refuse', async () => {
  const f = fixture(); await expect(f.owner.invokerSubmit(f.manifest)).rejects.toThrow('license'); expect(f.calls).toHaveLength(0);
  f.store.provider = () => ({ license_approved: 1 }); expect((await f.owner.invokerSubmit(f.manifest)).state).toBe('passed');
  await expect(f.owner.invokerSubmit('/etc/passwd')).rejects.toThrow();
  symlinkSync('/etc', join(f.home, 'outside')); await expect(f.owner.invokerSubmit(join(f.home, 'outside', 'passwd'))).rejects.toThrow();
  writeFileSync(join(f.bin, 'invoker-cli'), '#!/bin/sh\necho yes\n'); await expect(f.owner.invokerSubmit(f.manifest)).rejects.toThrow('not admitted');
});
test('failed effect is uncertain, overflow cannot become passed, STOP cancels and joins execution', async () => {
  const f = fixture({ run: async () => ({ exit_code: 0, overflow: true, stdout: '', stderr: '' }) });
  expect((await f.owner.cavemanCode('p')).state).toBe('outcome_uncertain'); expect(f.receipts.at(-1).state).toBe('outcome_uncertain');
  let stop = false, joined = false;
  const g = fixture({ checkStop: () => { if (stop) throw Error('STOP'); }, run: async (_argv, { signal }) => { await new Promise(resolve => signal.addEventListener('abort', resolve, { once: true })); joined = true; return { exit_code: 130, cancelled: true }; } });
  const pending = g.owner.cavemanCode('p'); setTimeout(() => { stop = true; }, 10); await expect(pending).rejects.toThrow('STOP'); expect(joined).toBe(true);
  const controller = new AbortController(); controller.abort(); await expect(g.owner.memorySessions({ signal: controller.signal })).rejects.toThrow();
});
test('real disposable Bun provider returns semantic stdout through bounded subprocess owner', async () => {
  const f = fixture({ run: projectRun }); const path = join(f.bin, 'cavemem');
  writeFileSync(path, `#!${process.execPath}\nconsole.log(JSON.stringify({args:process.argv.slice(2),fixture:true}));\n`); chmodSync(path, 0o700);
  const result = await f.owner.memorySearch('needle', { semantic: false }); expect(result.state).toBe('passed');
  expect(JSON.parse(result.result.stdout)).toEqual({ args: ['search', 'needle', '--limit', '12', '--no-semantic'], fixture: true });
});
