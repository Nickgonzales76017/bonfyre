import { afterEach, expect, test } from 'bun:test';
import { chmodSync, mkdtempSync, mkdirSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { digestValue } from '../canonical_json.mjs';
import { CERT_SCHEMA } from '../canary_store.mjs';
import { createHypervisorEvaluation } from '../hypervisor_evaluation.mjs';
import { createHypervisorStore } from '../hypervisor_store.mjs';

const homes = [];
afterEach(() => { for (const home of homes.splice(0)) rmSync(home, { recursive: true, force: true }); });
function fixture(providerId, capabilities, overrides = {}) {
  const home = mkdtempSync(join(tmpdir(), 'bonfyre-hypervisor-eval-')); homes.push(home);
  const provider = { id: providerId, mode: 'candidate', license: 'MIT', license_approved: 0,
    source_ref: null, source_digest: null, capabilities: capabilities.map(capability_id => ({ capability_id })), ...overrides };
  const persisted = [], store = { home, id: () => 'a'.repeat(32), provider: id => id === providerId ? provider : null,
    persistEvaluation: value => { persisted.push(value); return { ...value.document, path: join(home, 'evaluations', `${value.id}.json`) }; },
    latestEvaluations: () => [],
    registry: () => ({ projects: [{ id: providerId, default_mode: provider.mode, license: provider.license,
      install: { native: 'reviewed' }, notes: 'fixture' }] }), };
  const generic = [], semantic = [];
  const probe = async argv => { generic.push(argv); return { argv, exit_code: 0, stdout: '', stderr: '', duration_ms: 10 }; };
  const execute = async (argv, options) => { semantic.push({ argv, options }); return { receipt: { argv, exit_code: 0, duration_ms: 10 }, stdout: '', stderr: '' }; };
  return { home, store, provider, persisted, generic, semantic,
    make: options => createHypervisorEvaluation({ store, home, probe, semantic: execute, now: () => 1791000000, ...options }) };
}

test('active native reachability and traits never fabricate semantic behavior', async () => {
  const f = fixture('bonfyre-native', ['agent.executor'], { mode: 'active' });
  const result = await f.make().evaluate('bonfyre-native');
  expect(result.state).not.toBe('passed');
  expect(result.capabilities[0].probe.behavior_score).toBe(0);
  expect(f.persisted[0].capabilities[0].evidence.generic_probes).toHaveLength(1);
  expect(f.generic).toEqual([['bonfyre-dev', 'status']]);
});

test('Spines R5/R6/R7 capability predicates use one real verifier response and a sealed smoke document', async () => {
  const f = fixture('bonfyre-native-spines-r7', ['spines.temporal.parity', 'reasoning.helix.reduce', 'placement.compile']);
  const logDir = join(f.home, 'native-spines-r7', 'logs'); mkdirSync(logDir, { recursive: true });
  writeFileSync(join(logDir, 'latest-full-smoke.json'), JSON.stringify({ state: 'passed', r5: { parity: { equal: true } },
    r6: { reduction: { state: 'passed' }, authority: { decision: 'candidate_only' } },
    r7: { parity: { state: 'passed' }, fuse: { fuse: { activation: 'candidate' } } } }));
  const evaluator = f.make({ semantic: async (argv, options) => { f.semantic.push({ argv, options });
    return { receipt: { argv, exit_code: 0, duration_ms: 20 }, stdout: JSON.stringify({ verified: true }), stderr: '' }; } });
  const result = await evaluator.evaluate('bonfyre-native-spines-r7');
  expect(result.capabilities.map(item => item.probe.behavior_score)).toEqual([1, 1, 1]);
  expect(f.semantic.filter(item => item.argv.join(' ') === 'bonfyre-spines verify')).toHaveLength(1);
  expect(f.persisted[0].capabilities.map(item => item.capability)).toHaveLength(3);
  expect(result.capabilities.every(item => item.probe.probes[0].stdout === undefined)).toBe(true);
});

test('R3 shadow certificate binds compact digest, cleanup, expiry, provider and current version', async () => {
  const f = fixture('rtk', ['context.tool_compression']);
  const dir = join(f.home, 'shadow-r3', 'certificates', 'rtk'); mkdirSync(dir, { recursive: true });
  const path = join(dir, 'context.tool_compression.json');
  const body = { schema: CERT_SCHEMA, provider: 'rtk', capability: 'context.tool_compression', state: 'passed', behavior_score: 1,
    expires_at: 1900000000, cleanup_verified: true, provider_version: 'rtk 1.0' };
  writeFileSync(path, JSON.stringify({ ...body, digest: digestValue(body) }));
  const semantic = async argv => ({ receipt: { argv, exit_code: 0, duration_ms: 10 }, stdout: 'rtk 1.0\n', stderr: '' });
  const valid = await f.make({ semantic }).evaluate('rtk');
  expect(valid.capabilities[0].probe).toMatchObject({ certificate_valid: true, behavior_score: 1 });
  const tampered = { ...body, provider_version: 'wrong', digest: digestValue(body) };
  writeFileSync(path, JSON.stringify(tampered));
  const invalid = await f.make({ semantic }).evaluate('rtk');
  expect(invalid.capabilities[0].probe.certificate_valid).toBeUndefined();
  expect(invalid.capabilities[0].probe.shadow_refusal).toContain('mismatch');
  writeFileSync(path, JSON.stringify({ ...body, digest: digestValue(body) }));
  const swapping = await f.make({ semantic: async argv => { writeFileSync(path, JSON.stringify({ ...body, digest: digestValue(body), note: 'swapped' }));
    return { receipt: { argv, exit_code: 0, duration_ms: 10 }, stdout: 'rtk 1.0\n', stderr: '' }; } }).evaluate('rtk');
  expect(swapping.capabilities[0].probe.shadow_refusal).toContain('changed');
});

test('RTK compares real outputs and exit status; version/help does not substitute for output reduction', async () => {
  const f = fixture('rtk', ['context.tool_compression']);
  const semantic = async argv => ({ receipt: { argv, exit_code: 0, duration_ms: 10 },
    stdout: argv[0] === 'git' ? 'long status output\n' : 'short\n', stderr: '' });
  const result = await f.make({ semantic }).evaluate('rtk');
  expect(result.capabilities[0].probe.behavior_score).toBe(1);
  expect(result.capabilities[0].probe.note).toContain('raw_bytes=19');
  const noReduction = await f.make({ semantic: async argv => ({ receipt: { argv, exit_code: 0, duration_ms: 10 },
    stdout: argv[0] === 'git' ? 'x' : 'much longer output', stderr: '' }) }).evaluate('rtk');
  expect(noReduction.capabilities[0].probe.behavior_score).toBe(.4);
  expect(noReduction.state).not.toBe('passed');
});

test('Bernstein dry-run is exact third-party boundary and Agent Deck help remains partial', async () => {
  const bern = fixture('bernstein', ['scheduler.deterministic_dag']);
  const result = await bern.make({ semantic: async (argv, options) => { bern.semantic.push({ argv, options });
    return { receipt: { argv, exit_code: 0, duration_ms: 10, runtime_boundary: { kind: 'declared-third-party-python-cli' } }, stdout: '', stderr: '' }; } })
    .evaluate('bernstein');
  expect(result.capabilities[0].probe.behavior_score).toBe(1);
  expect(bern.semantic).toHaveLength(2);
  expect(bern.semantic.every(item => item.options.allowBernstein === true)).toBe(true);
  const deck = fixture('agent-deck', ['session.resume']);
  const surface = await deck.make().evaluate('agent-deck');
  expect(surface.capabilities[0].probe.behavior_score).toBe(.4);
  expect(surface.state).not.toBe('passed');
});

test('compare and install plans are bounded and never auto-install', () => {
  const f = fixture('rtk', ['context.tool_compression']);
  const evaluator = f.make();
  expect(evaluator.compare('context.tool_compression', { providers: ['rtk'] }).winner).toBeNull();
  expect(evaluator.installPlan('lean').steps.map(item => item.provider)).toEqual(['rtk']);
  expect(evaluator.installPlan('lean').automatic).toBe(false);
  expect(() => evaluator.installPlan('unknown')).toThrow('unknown profile');
});

test('default semantic admission refuses unreviewed Python executables without spawning', async () => {
  const f = fixture('rtk', ['context.tool_compression']);
  const bin = join(f.home, 'bin'); mkdirSync(bin);
  for (const name of ['git', 'rtk']) {
    const path = join(bin, name); writeFileSync(path, '#!/usr/bin/env python3\nraise RuntimeError("must not run")\n'); chmodSync(path, 0o755);
  }
  let invocations = 0;
  const evaluator = createHypervisorEvaluation({ store: f.store, home: f.home,
    probe: async argv => ({ argv, exit_code: 0, duration_ms: 10 }), env: { PATH: bin },
    run: async () => { invocations++; throw new Error('must not spawn'); }, now: () => 1791000000 });
  const result = await evaluator.evaluate('rtk');
  expect(invocations).toBe(0);
  expect(result.capabilities[0].probe.behavior_score).toBe(0);
  expect(result.capabilities[0].probe.probes.every(item => item.refused === 'reviewed-native-executable-required')).toBe(true);
});

test('certificate parent symlink is refused before a shadow pass', async () => {
  const f = fixture('rtk', ['context.tool_compression']);
  const elsewhere = mkdtempSync(join(tmpdir(), 'bonfyre-shadow-outside-')); homes.push(elsewhere);
  mkdirSync(join(elsewhere, 'rtk'));
  writeFileSync(join(elsewhere, 'rtk', 'context.tool_compression.json'), '{}');
  mkdirSync(join(f.home, 'shadow-r3')); symlinkSync(elsewhere, join(f.home, 'shadow-r3', 'certificates'));
  const result = await f.make().evaluate('rtk');
  expect(result.capabilities[0].probe.shadow_refusal).toContain('unreadable');
  expect(result.capabilities[0].probe.certificate_valid).not.toBe(true);
});

test('real store records row-bound evaluation evidence and ranks the latest persisted result', async () => {
  const home = mkdtempSync(join(tmpdir(), 'bonfyre-hypervisor-eval-store-')); homes.push(home);
  const registryPath = join(home, 'ecosystem_registry.json');
  writeFileSync(registryPath, JSON.stringify({ capability_contracts: ['context.tool_compression'], projects: [
    { id: 'rtk', repo: 'local/rtk', homepage: 'https://example.test/rtk', license: 'MIT', integration: 'native',
      default_mode: 'candidate', detect: ['rtk'], capabilities: ['context.tool_compression'] },
  ] }));
  const store = createHypervisorStore({ home, registryPath, now: () => 1791000000 });
  try {
    store.loadRegistry();
    const probe = async argv => ({ argv, exit_code: 0, duration_ms: 10 });
    const semantic = async argv => ({ receipt: { argv, exit_code: 0, duration_ms: 10 },
      stdout: argv[0] === 'git' ? 'long status output\n' : 'short\n', stderr: '' });
    const evaluation = createHypervisorEvaluation({ store, home, probe, semantic, now: () => 1791000000 });
    const result = await evaluation.evaluate('rtk', { capability: 'context.tool_compression' });
    expect(result.state).toBe('passed');
    expect(result.capabilities[0].probe.behavior_score).toBe(1);
    const compared = evaluation.compare('context.tool_compression', { providers: ['rtk'] });
    expect(compared.winner).toBe('rtk');
    expect(compared.ranking).toHaveLength(1);
    expect(compared.ranking[0].state).toBe('passed');
    expect(compared.ranking[0].evidence.capability_probe.probes).toHaveLength(2);
    expect(store.verifyReceiptChain().verified).toBe(true);
  } finally { store.close(); }
});
