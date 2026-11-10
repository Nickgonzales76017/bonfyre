import { afterEach, expect, test } from 'bun:test';
import { createHash } from 'node:crypto';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createSpinesStore } from '../spines_store.mjs';
import { createReasoningEngine, parseSpinesTerm } from '../spines_reasoning.mjs';

const fixtures = [];
afterEach(() => { for (const { home, store } of fixtures.splice(0)) { store.close(); rmSync(home, { recursive: true, force: true }); } });
const sha = text => createHash('sha256').update(text).digest('hex');
function fixture(options = {}) {
  const home = mkdtempSync(join(tmpdir(), 'bonfyre-spines-reasoning-'));
  const calls = [];
  const store = createSpinesStore({ home, projectRoot: process.cwd(), deltaKernel: '/native/delta', helixVm: '/native/helix',
    kernelProbe: '/native/probe', now: () => 1234, env: { PATH: '' }, run: async (argv, params) => {
      calls.push({ argv, params });
      return { argv, exit_code: 0, stdout: JSON.stringify({ schema: 'bonfyre.helix-witness.v1', state: 'passed',
        term_sha256: sha(argv[3]), steps: 3, fuel_remaining: Number(argv[5]) - 3, result: 3 }), stderr: '', duration_ms: 1 };
    }, ...options });
  fixtures.push({ home, store });
  return { store, reasoning: createReasoningEngine(store, { wasmPath: `${home}/absent.wasm` }), calls };
}

test('parser preserves application, variable, integer and floating distinctions with strict admission', () => {
  expect(parseSpinesTerm('add(?x,mul(2,3.0))')).toEqual({ kind: 'app', name: 'add', args: ['?x',
    { kind: 'app', name: 'mul', args: [{ kind: 'number', value: 2, floating: false }, { kind: 'number', value: 3, floating: true }] }] });
  for (const invalid of ['add(', 'add(1 2)', 'add(1),mul(2)', '?', '1e3', '9'.repeat(64)])
    expect(() => parseSpinesTerm(invalid)).toThrow();
  expect(() => parseSpinesTerm('add('.repeat(70) + '1' + ')'.repeat(70))).toThrow('complexity');
});

test('rule registration retains legacy identity law, SQL row and R6 receipt', () => {
  const { store, reasoning } = fixture();
  const registered = reasoning.addRule({ name: 'cancel', lhs: 'sub(?x,?x)', rhs: '0', guard: { b: 2, a: 1 } });
  expect(registered.rule_id).toBe(sha(`cancel\0sub(?x,?x)\0${'0'}\0${store.digest({ b: 2, a: 1 })}`));
  expect(registered.receipt.state).toBe('passed');
  expect(reasoning.rules().find(rule => rule.name === 'cancel').guard).toEqual({ b: 2, a: 1 });
  expect(store.db.query('SELECT digest FROM rewrite_rules WHERE id=?').get(registered.rule_id).digest)
    .toBe(store.digest({ name: 'cancel', lhs: 'sub(?x,?x)', rhs: '0', guard: { b: 2, a: 1 } }));
  expect(store.verifyReceipts().verified).toBe(true);
});

test('bounded saturation selects cheapest rewritten term and identifies run without claiming egglog execution', () => {
  const { store, reasoning } = fixture();
  const result = reasoning.saturate({ term: 'add(mul(2,3),0)' });
  expect(result.selected_term).toBe('6');
  expect(result.backend).toBe('bonfyre-bounded-egraph-r6');
  expect(result.egglog_adapter.mode).toBe('contract-only');
  expect(result.run_id).toBe(sha(`add(mul(2,3),0)\0${'6'}\0${result.digest}`));
  expect(store.db.query('SELECT selected_term FROM saturation_runs WHERE id=?').get(result.run_id).selected_term).toBe('6');
  expect(result.terms[0]).toEqual({ term: '6', cost: 1 });
  expect(store.verifyReceipts().verified).toBe(true);
});

test('max term budget truncates and refused malformed programs never create receipts', () => {
  const { store, reasoning } = fixture();
  const before = store.verifyReceipts().receipts;
  expect(() => reasoning.addRule({ name: 'bad', lhs: 'add(', rhs: '0' })).toThrow();
  expect(() => reasoning.saturate({ term: 'add(1,2)', max_iterations: 101 })).toThrow('max_iterations');
  expect(() => reasoning.saturate({ term: 'add(1,2)', rule_ids: ['SQL injection'] })).toThrow('rule_ids');
  expect(store.verifyReceipts().receipts).toBe(before);
  const bounded = reasoning.saturate({ term: 'add(1,2)', max_terms: 2 });
  expect(bounded.truncated).toBe(true);
  expect(bounded.terms).toHaveLength(2);
});

test('compiled Helix remains PURE and reduction invokes actual configured native owner contract', async () => {
  const { store, reasoning, calls } = fixture();
  const compiled = reasoning.compileHelix({ term: 'add(1,2)', fuel: 20_000_000, max_depth: 6000,
    labels: { shared: 'left' } });
  expect(compiled.resource_bounds).toEqual({ fuel: 1000000, max_depth: 4096, effect_scope: 'PURE' });
  expect(store.readJson(compiled.path).digest).toBe(compiled.digest);
  expect(compiled.helix_id).toBe(sha(`add(1,2)\0${compiled.digest}`));
  const result = await reasoning.reduce({ helix_id: compiled.helix_id });
  expect(calls).toHaveLength(1);
  expect(calls[0].argv).toEqual(['/native/helix', 'reduce', '--term', 'add(1,2)', '--fuel', '1000000']);
  expect(calls[0].params.timeoutMs).toBe(60000);
  expect(result.state).toBe('passed');
  expect(result.hvm4_adapter.mode).toBe('contract-only');
  expect(result.receipt.state).toBe('passed');
  expect(store.readJson(result.witness_path).digest).toBe(result.digest);
  expect(store.verifyReceipts().verified).toBe(true);
});

test('declared Helix depth is enforced before compiling and before either execution backend', async () => {
  const { store, reasoning, calls } = fixture();
  expect(() => reasoning.compileHelix({ term: 'add(1,mul(2,3))', max_depth: 2 })).toThrow('max_depth');
  expect(() => reasoning.compileHelix({ term: '1', fuel: 0 })).toThrow('resource bounds');
  const compiled = reasoning.compileHelix({ term: 'add(1,mul(2,3))', max_depth: 3 });
  store.db.query('UPDATE helix_artifacts SET resource_bounds_json=? WHERE id=?')
    .run('{"fuel":10000,"max_depth":2,"effect_scope":"PURE"}', compiled.helix_id);
  await expect(reasoning.reduce({ helix_id: compiled.helix_id })).rejects.toThrow('bounds mismatch');
  expect(calls).toHaveLength(0);
});

test('native failure stays failed, and cannot become proof by malformed stdout or status', async () => {
  const { store, reasoning } = fixture({ run: async argv => ({ argv, exit_code: 1, stdout: '{"state":"passed"}', stderr: 'failed', duration_ms: 1 }) });
  const compiled = reasoning.compileHelix({ term: 'add(1,2)' });
  const failed = await reasoning.reduce({ helix_id: compiled.helix_id });
  expect(failed.state).toBe('failed');
  expect(failed.receipt.state).toBe('failed');
  expect(store.verifyReceipts().verified).toBe(true);
});

test('native claimed success with wrong identity, invalid result or inconsistent fuel cannot pass', async () => {
  for (const forged of [
    { state: 'passed', result: 3 },
    { schema: 'bonfyre.helix-witness.v1', state: 'passed', term_sha256: '0'.repeat(64), steps: 3, fuel_remaining: 97, result: 3 },
    { schema: 'bonfyre.helix-witness.v1', state: 'passed', term_sha256: sha('add(1,2)'), steps: 3, fuel_remaining: 99, result: 3 },
    { schema: 'bonfyre.helix-witness.v1', state: 'passed', term_sha256: sha('add(1,2)'), steps: 3, fuel_remaining: 97, result: 'NaN' },
  ]) {
    const { reasoning } = fixture({ run: async argv => ({ argv, exit_code: 0, stdout: JSON.stringify(forged), stderr: '', duration_ms: 1 }) });
    const compiled = reasoning.compileHelix({ term: 'add(1,2)', fuel: 100 });
    const result = await reasoning.reduce({ helix_id: compiled.helix_id });
    expect(result.state).toBe('failed');
    expect(result.witness_validation).toBe('native_witness_identity_or_bounds_mismatch');
  }
});

test('mutated persisted Helix row or artifact refuses execution before native invocation', async () => {
  const { store, reasoning, calls } = fixture();
  const compiled = reasoning.compileHelix({ term: 'add(1,2)' });
  store.db.query('UPDATE helix_artifacts SET term=? WHERE id=?').run('add(9,9)', compiled.helix_id);
  await expect(reasoning.reduce({ helix_id: compiled.helix_id })).rejects.toThrow('body mismatch');
  expect(calls).toHaveLength(0);
  store.db.query('UPDATE helix_artifacts SET term=?, resource_bounds_json=? WHERE id=?').run('add(1,2)', '{"fuel":999999999,"max_depth":256,"effect_scope":"PURE"}', compiled.helix_id);
  await expect(reasoning.reduce({ helix_id: compiled.helix_id })).rejects.toThrow('bounds mismatch');
  expect(calls).toHaveLength(0);
});

test('authority decision table preserves nonpromotion of candidate and external effects', () => {
  const { store, reasoning } = fixture();
  const expected = [
    ['PURE', 'allowed_pure', 'passed', false],
    ['READ_ONLY', 'allowed_read', 'passed', false],
    ['CANDIDATE_WRITE', 'candidate_only', 'passed', true],
    ['REVERSIBLE_INTERNAL_WRITE', 'review_required', 'blocked', true],
    ['AUTHORITATIVE_INTERNAL_WRITE', 'denied', 'blocked', true],
    ['EXTERNAL_REVERSIBLE_EFFECT', 'denied', 'blocked', true],
    ['EXTERNAL_IRREVERSIBLE_EFFECT', 'denied', 'blocked', true],
  ];
  for (const [effect_class, decision, receipt, requires_zig] of expected) {
    const result = reasoning.authority({ actor: 'tester', effect_class, resource: 'record/7', proof_refs: ['proof/3'] });
    expect([result.decision, result.receipt.state, result.requires_zig, result.zig_executed]).toEqual([decision, receipt, requires_zig, false]);
    expect(result.decision_id).toBe(sha(`tester\0${effect_class}\0record/7\0${result.digest}`));
  }
  expect(store.db.query('SELECT COUNT(*) AS n FROM authority_decisions').get().n).toBe(7);
  expect(store.verifyReceipts().verified).toBe(true);
});

test('Crux proof retains candidate classification and explicit nonauthoritative state', () => {
  const { store, reasoning } = fixture();
  const result = reasoning.crux({ claim: 'A equals B', inputs: [{ body: 'sha256:1' }], witnesses: [{ state: 'observed' }] });
  expect(result.authoritative).toBe(false);
  expect(result.classification).toBe('candidate');
  expect(result.proof_id).toBe(sha(`A equals B\0${result.digest}`));
  expect(store.readJson(result.path).authoritative).toBe(false);
  expect(store.db.query('SELECT classification FROM crux_proofs WHERE id=?').get(result.proof_id).classification).toBe('candidate');
  expect(store.verifyReceipts().verified).toBe(true);
});
