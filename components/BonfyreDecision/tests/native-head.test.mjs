import { afterAll, beforeAll, expect, test } from 'bun:test';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createRequire } from 'node:module';
import { nativeDecisionBinding, compileNativeDecisionHead, createNativeDecisionEvaluator,
  createNativeDecisionBackbone } from '../native-head.mjs';
import { createClefTextEncoder } from '../schema-encoder.mjs';
import { assessDecision, compileDecisionCase, decisionDigest } from '../decision-plane.mjs';
import { mcpToolCase, flowWorkCase } from '../frontier-adapters.mjs';
import { headFixture, packedSpans } from './native-head-fixture.mjs';
import { referenceHead } from './head-reference.mjs';
import { compileQuestionSchema } from '../typed-decision.mjs';

let binding, scalar;
const build = mkdtempSync(join(tmpdir(), 'bonfyre-decision-native-test-'));
beforeAll(() => {
  for (const mode of ['accelerated', 'scalar']) {
    const out = join(build, mode);
    const result = Bun.spawnSync(['make', '-C', new URL('../native', import.meta.url).pathname,
      `BUILD=${out}`, `SCALAR=${mode === 'scalar' ? 1 : 0}`]);
    if (result.exitCode !== 0) throw new Error(new TextDecoder().decode(result.stderr));
    const lib = nativeDecisionBinding(join(out, 'decision_head.node'));
    if (mode === 'scalar') scalar = lib; else binding = lib;
  }
}, 30000);
afterAll(() => rmSync(build, { recursive: true, force: true }));

function run(fixture, api = binding) {
  const head = compileNativeDecisionHead(fixture.config, fixture.tensors, { binding: api });
  const packed = packedSpans(fixture.fields);
  const size = api.workspace(head.handle, { tokens: fixture.hidden.length / fixture.config.hidden_size,
    fields: fixture.fields.length, options: packed.options.length / 2 });
  const workspace = new Float32Array(size);
  return api.run(head.handle, fixture.hidden, packed.fields, packed.options, fixture.lexical_options, workspace);
}

test('full learned-head topology agrees with independent dense equations in scalar and BLAS paths', () => {
  for (const options of [{}, { seed: 23, routing_layers: 2, layers: 1 },
    { seed: 101, hidden_size: 9, width: 6, heads: 3, feedforward: 11 },
    { routing_layers: 0, layers: 0 }, { zero: true }, { hidden_size: 13, width: 1, heads: 1 }]) {
    const fixture = headFixture(binding, options), expected = referenceHead(fixture);
    for (const actual of [run(fixture), run(fixture, scalar)]) {
      expect(actual.length).toBe(expected.length);
      actual.forEach((v, i) => expect(v).toBeCloseTo(expected[i], 5));
    }
  }
});

test('joint field routing is permutation equivariant and not an independent-softmax substitute', () => {
  const f = headFixture(binding), baseline = run(f);
  const permuted = structuredClone(f);
  permuted.fields = [f.fields[2], f.fields[0], f.fields[1]];
  permuted.lexical_options = Float32Array.from([
    ...f.lexical_options.slice(4 * f.config.hidden_size), ...f.lexical_options.slice(0, 4 * f.config.hidden_size)]);
  const result = run(permuted), expected = [...baseline.slice(4), ...baseline.slice(0, 4)];
  result.forEach((v, i) => expect(v).toBeCloseTo(expected[i], 5));
  const altered = structuredClone(f);
  altered.fields[2].question_span = [0, 1];
  expect(Math.abs(run(altered)[0] - baseline[0])).toBeGreaterThan(1e-6);
  const lexical = structuredClone(f); lexical.lexical_options.fill(0);
  expect(Math.abs(run(lexical)[0] - baseline[0])).toBeGreaterThan(1e-4);
});

test('residual packing crosses field and 32-option tile boundaries without changing dense equations', () => {
  for (const counts of [[1], [31], [32], [33], [65], [7, 30, 4, 35]]) {
    const f = headFixture(binding, { hidden_size: 9, width: 6, heads: 3, feedforward: 11 });
    f.fields = counts.map((count, i) => ({ id: `field-${i}`, type: 'choice', question_span: [i, i + 1],
      options: Array.from({ length: count }, (_, j) => ({ id: String(j), span: [j % 17, j % 17 + 1] })) }));
    f.lexical_options = Float32Array.from({ length: counts.reduce((a, b) => a + b) * f.config.hidden_size },
      (_, i) => ((i * 13) % 97 - 48) / 100);
    const expected = referenceHead(f);
    for (const actual of [run(f), run(f, scalar)])
      actual.forEach((value, i) => expect(value).toBeCloseTo(expected[i], 5));
  }
});

test('native boundary refuses bad shapes, nonfinite weights, aliases, spans and undersized arenas', () => {
  const f = headFixture(binding), h = compileNativeDecisionHead(f.config, f.tensors, { binding });
  const p = packedSpans(f.fields), n = binding.workspace(h.handle, { tokens: 18, fields: 3, options: 7 });
  const workspace = new Float32Array(n);
  const call = (hidden = f.hidden, fields = p.fields, options = p.options, scratch = workspace) =>
    binding.run(h.handle, hidden, fields, options, f.lexical_options, scratch);
  expect(() => call(f.hidden.slice(1))).toThrow('shape');
  expect(() => call(f.hidden, p.fields, p.options, workspace.slice(1))).toThrow('short workspace');
  const bad = p.options.slice(); bad[1] = 1000;
  expect(() => call(f.hidden, p.fields, bad)).toThrow('spans');
  const nan = f.hidden.slice(); nan[0] = NaN;
  expect(() => call(nan)).toThrow('nonfinite');
  const words = p.fields.slice(); words[0] = 99;
  expect(() => call(f.hidden, words)).toThrow('spans');
  expect(() => call(workspace.subarray(0, f.hidden.length))).toThrow('overlap');
  expect(() => call(new Float32Array(new SharedArrayBuffer(f.hidden.byteLength)))).toThrow('typed input');
  expect(() => binding.run({}, f.hidden, p.fields, p.options, f.lexical_options, workspace)).toThrow('handle');
  expect(() => compileNativeDecisionHead(f.config, f.tensors, { binding, maxWeightBytes: 4 })).toThrow('budget');
  const corrupt = structuredClone(f.tensors); corrupt['type_embedding.weight'].data[0] = Infinity;
  expect(() => compileNativeDecisionHead(f.config, corrupt, { binding })).toThrow('nonfinite weight');
  const baseline = call();
  f.tensors['type_embedding.weight'].data.fill(1000);
  expect([...call()]).toEqual([...baseline]); // importer snapshots weights
});

test('workspace grows linearly with sequence length rather than allocating attention score matrices', () => {
  const f = headFixture(binding), h = compileNativeDecisionHead(f.config, f.tensors, { binding });
  const size = tokens => binding.workspace(h.handle, { tokens, fields: 3, options: 7 });
  expect(size(32768) - size(16384)).toBe(2 * (size(16384) - size(8192)));
});

test('native typed-buffer admission never invokes JS getters or trusts spoofed backing buffers', () => {
  for (const api of [binding, scalar]) {
    const direct = createRequire(import.meta.url)(join(build, api === binding ? 'accelerated' : 'scalar', 'decision_head.node'));
    const f = headFixture(api), h = compileNativeDecisionHead(f.config, f.tensors, { binding: api });
    const p = packedSpans(f.fields);
    const scratch = new Float32Array(api.workspace(h.handle, { tokens: 18, fields: 3, options: 7 }));
    const baseline = api.run(h.handle, f.hidden, p.fields, p.options, f.lexical_options, scratch);
    let getterCalls = 0;
    const hiddenBuffer = f.hidden.buffer;
    for (const array of [f.hidden, p.fields, p.options, f.lexical_options, scratch]) {
      Object.defineProperty(array, 'buffer', { configurable: true, get() {
        getterCalls++;
        // Reentrant user code here used to be able to detach earlier native
        // pointers. Do not execute a UAF reproduction in the test process.
        throw new Error('typed-array buffer property must never be consulted');
      } });
    }
    const descriptor = Object.getOwnPropertyDescriptor(globalThis, 'SharedArrayBuffer');
    try {
      Object.defineProperty(globalThis, 'SharedArrayBuffer', { configurable: true, get() {
        getterCalls++; throw new Error('mutable global constructor must not be consulted');
      } });
      expect([...api.run(h.handle, f.hidden, p.fields, p.options, f.lexical_options, scratch)]).toEqual([...baseline]);
      expect([...direct.run(h.handle, f.hidden, p.fields, p.options, f.lexical_options, scratch)]).toEqual([...baseline]);
    } finally { Object.defineProperty(globalThis, 'SharedArrayBuffer', descriptor); }
    expect(getterCalls).toBe(0);
    expect(hiddenBuffer.byteLength).toBe(f.hidden.length * 4);

    const sharedBuffer = new SharedArrayBuffer(f.hidden.length * 4);
    const shared = new Float32Array(sharedBuffer);
    Object.defineProperty(shared, 'buffer', { value: new ArrayBuffer(shared.length * 4) });
    expect(() => api.run(h.handle, shared, p.fields, p.options, f.lexical_options, scratch)).toThrow('typed input');
    expect(() => direct.run(h.handle, shared, p.fields, p.options, f.lexical_options, scratch)).toThrow('typed input');
    Object.setPrototypeOf(sharedBuffer, ArrayBuffer.prototype);
    expect(() => direct.run(h.handle, shared, p.fields, p.options, f.lexical_options, scratch)).toThrow('typed input');
    const detached = new Float32Array(f.hidden.length);
    structuredClone(detached.buffer, { transfer: [detached.buffer] });
    expect(() => api.run(h.handle, detached, p.fields, p.options, f.lexical_options, scratch)).toThrow('typed input');
    expect(() => direct.run(h.handle, detached, p.fields, p.options, f.lexical_options, scratch)).toThrow('typed input');

    const layout = api.layout(f.config), last = layout.at(-1);
    const weights = new Float32Array(last.offset + last.rows * last.columns);
    Object.defineProperty(weights, 'buffer', { get() { getterCalls++; throw new Error('weight buffer getter'); } });
    expect(api.compile(f.config, weights)).toBeDefined();
    expect(direct.compile(f.config, weights)).toBeDefined();
    const sharedWeights = new Float32Array(new SharedArrayBuffer(weights.length * 4));
    Object.defineProperty(sharedWeights, 'buffer', { value: new ArrayBuffer(weights.length * 4) });
    expect(() => direct.compile(f.config, sharedWeights)).toThrow('weight shape');
    expect(getterCalls).toBe(0);
  }
});

test('native evaluator maps every option by identity and binds immutable backbone provenance', async () => {
  const f = headFixture(binding), head = compileNativeDecisionHead(f.config, f.tensors, { binding });
  const questions = { priority: { type: 'score', criteria: ['low', 'medium', 'high'] },
    route: { type: 'choice', criteria: { zulu: 'z', alpha: 'a' } }, truth: { type: 'noul' } };
  const schema = compileQuestionSchema(questions), state = { record: 'test' };
  const backbone = { artifact_digest: 'a'.repeat(64), encoder_digest: 'b'.repeat(64),
    async encodeAndForward({ request_digest }) { return { ...f, request_digest, truncated: false,
      backbone_artifact_digest: this.artifact_digest, encoder_digest: this.encoder_digest }; } };
  const evaluator = createNativeDecisionEvaluator({ model: 'synthetic', head, backbone });
  const baseline = [...run(f)], expected = [...baseline.slice(4), baseline[3], baseline[2], ...baseline.slice(0, 2)];
  const request = { state, questions, schema };
  const results = await Promise.all([evaluator.evaluate(request), evaluator.evaluate(request)]);
  for (const logits of results) logits.forEach((v, i) => expect(v).toBeCloseTo(expected[i], 6));
  await expect(createNativeDecisionEvaluator({ model: 'small', head, backbone, maxTokens: 2 }).evaluate(request))
    .rejects.toThrow('token budget');
  await expect(createNativeDecisionEvaluator({ model: 'small', head, backbone, maxWorkspaceBytes: 4 }).evaluate(request))
    .rejects.toThrow('workspace budget');
  const canceled = AbortSignal.abort(new Error('canceled'));
  await expect(evaluator.evaluate({ ...request, signal: canceled })).rejects.toThrow('canceled');
  backbone.artifact_digest = 'c'.repeat(64);
  await expect(evaluator.evaluate(request)).rejects.toThrow('complete request');
});

test('ContextCut, MCP and Flow consume actual native-head outputs through the common local path', async () => {
  const f = headFixture(binding), head = compileNativeDecisionHead(f.config, f.tensors, { binding });
  let calls = 0;
  const backbone = { artifact_digest: 'a'.repeat(64), encoder_digest: 'b'.repeat(64), deterministic: true,
    // Explicit synthetic backbone witness; no trained model claim.
    async encodeAndForward({ questions, request_digest }) {
      calls++;
      let cursor = 0;
      const fields = Object.entries(questions).reverse().map(([id, q]) => {
        const start = cursor++;
        const options = q.type === 'noul' ? ['false', 'true'] : q.type === 'score'
          ? q.criteria.map((_, i) => String(i)).reverse() : Object.keys(q.criteria).reverse();
        return { id, type: q.type, question_span: [start, cursor],
          options: options.map(id => ({ id, span: [cursor++, cursor] })) };
      });
      const options = fields.reduce((n, field) => n + field.options.length, 0);
      return { request_digest, truncated: false, backbone_artifact_digest: backbone.artifact_digest,
        encoder_digest: backbone.encoder_digest, fields,
        hidden: Float32Array.from({ length: (cursor + 1) * f.config.hidden_size }, (_, i) => (i % 13) / 13),
        lexical_options: Float32Array.from({ length: options * f.config.hidden_size }, (_, i) => (i % 11) / 11) };
    } };
  const localEvaluator = createNativeDecisionEvaluator({ model: 'native-head-synthetic-backbone', head, backbone });
  const cases = [compileDecisionCase({ program: 'context_passage', owner: 'ContextCut',
    subject_id: 'body', mask_digest: decisionDigest('body'), gate_state: 'ADMIT', information_policy_class: 'private',
    frontier: [{ id: 'body', description: 'body', owner_ref: 'ContextCut:test' }],
    state: { purpose: 'Read', passage: { id: 'body', text: 'Private context' } } }),
    mcpToolCase({ tools: [{ name: 'read', description: 'Read', annotations: { readOnlyHint: true } }] }, ['read'], 'Read', 'private'),
    flowWorkCase({ id: 'work', information_policy_class: 'private' },
      [{ id: 'review', description: 'Review', owner_ref: 'Flow:review' }], decisionDigest('work'))];
  for (const compiled of cases) {
    const receipt = await assessDecision(compiled, { execute: true, localEvaluator,
      fetcher: () => { throw new Error('must not egress'); } });
    expect(receipt).toMatchObject({ provider: 'local', state: 'diagnostic', model_identity_state: 'artifact_bound', action: 'none' });
    expect(receipt.proposal.action).toBe('none');
    expect(Object.keys(receipt.answers)).toEqual(Object.keys(compiled.questions).sort());
  }
  expect(calls).toBe(3);
  for (let repeat = 0; repeat < 10; repeat++) for (const compiled of cases) {
    const reused = await assessDecision(compiled, { execute: true, localEvaluator });
    expect(reused.inference).toMatchObject({ state: 'reused', executed: false });
    expect(reused.proposal.action).toBe('none');
  }
  expect(calls).toBe(3); // 33 shared assessments, three native executions.
  const truncated = { ...backbone, encodeAndForward: async request => ({ ...await backbone.encodeAndForward(request), truncated: true }) };
  await expect(assessDecision(cases[0], { execute: true, localEvaluator: createNativeDecisionEvaluator({ model: 'bad', head, backbone: truncated }) }))
    .rejects.toThrow('complete request');
});

test('released schema encoder feeds prefill and option-only output embeddings into native head', async () => {
  const f = headFixture(binding), head = compileNativeDecisionHead(f.config, f.tensors, { binding });
  const encoder = createClefTextEncoder({ artifactDigest: 'a'.repeat(64), tokenizerDigest: 'b'.repeat(64),
    tokenizer: { deterministic: true, encode: text => [...Buffer.from(text)] } });
  let prefills = 0, gathered = [];
  const runtime = { artifact_digest: 'a'.repeat(64), hidden_size: f.config.hidden_size, deterministic: true,
    // A deterministic token-based fixture, not Qwen activations or tokenizer.
    forward: ({ input_ids }) => { prefills++;
      return Float32Array.from({ length: input_ids.length * f.config.hidden_size }, (_, i) =>
        (input_ids[Math.floor(i / f.config.hidden_size)] % 17 + i % f.config.hidden_size) / 17); },
    gatherOutputEmbeddings: ({ token_ids }) => { gathered = [...token_ids];
      return Float32Array.from({ length: token_ids.length * f.config.hidden_size }, (_, i) =>
        (token_ids[Math.floor(i / f.config.hidden_size)] + i % f.config.hidden_size) / 256); } };
  const backbone = createNativeDecisionBackbone({ encoder, runtime });
  const state = { title: 'A report' }, questions = {
    route: { type: 'choice', criteria: { z: 'Archive', a: 'Read' } },
    priority: { type: 'score', criteria: ['routine', 'now'] },
    keep: { type: 'noul' } };
  const request_digest = decisionDigest({ state, questions });
  const record = await backbone.encodeAndForward({ state, questions, request_digest });
  const encoded = encoder.encodeRecord({ state, questions });
  const unique = new Set(encoded.questions.flatMap(q => q.option_spans.flatMap(([start, end]) => encoded.input_ids.slice(start, end))));
  expect(gathered.length).toBe(unique.size);
  expect(gathered.length).toBeLessThan(encoded.input_ids.length);
  expect(record.fields[0].options.map(option => option.id)).toEqual(['a', 'z']);
  let column = 0;
  for (const field of record.fields) for (const { span: [start, end] } of field.options)
    for (let c = 0; c < f.config.hidden_size; c++) {
      const mean = encoded.input_ids.slice(start, end).reduce((sum, token) => sum + (token + c) / 256, 0) / (end - start);
      expect(record.lexical_options[column++]).toBeCloseTo(mean, 6);
    }
  const evaluator = createNativeDecisionEvaluator({ model: 'schema-prefill-fixture', head, backbone });
  expect(evaluator.deterministic).toBe(true);
  const schema = compileQuestionSchema(questions);
  const actual = await evaluator.evaluate({ state, questions, schema });
  const expected = referenceHead({ ...f, ...record });
  const permuted = [expected[1], expected[0], ...expected.slice(2)];
  actual.forEach((value, i) => expect(value).toBeCloseTo(permuted[i], 5));
  const before = prefills;
  await expect(createNativeDecisionBackbone({ encoder, runtime, maxActivationBytes: 4 })
    .encodeAndForward({ state, questions, request_digest })).rejects.toThrow('admission budget');
  expect(prefills).toBe(before);
  expect(() => createNativeDecisionBackbone({ encoder, runtime: { ...runtime, artifact_digest: 'c'.repeat(64) } }))
    .toThrow('matching pinned');
  const invalid = createNativeDecisionBackbone({ encoder, runtime: { ...runtime,
    forward: () => new Float32Array(1) } });
  await expect(invalid.encodeAndForward({ state, questions, request_digest })).rejects.toThrow('invalid F32');
});

test('two backbone adapters serialize one runtime through output snapshots, including async recycled buffers', async () => {
  const encoder = createClefTextEncoder({ artifactDigest: 'a'.repeat(64), tokenizerDigest: 'b'.repeat(64),
    tokenizer: { deterministic: true, encode: text => [...Buffer.from(text)] } });
  let calls = 0, hidden, embedding;
  const runtime = { artifact_digest: 'a'.repeat(64), hidden_size: 2,
    async forward({ input_ids }) {
      calls++; hidden ??= new Float32Array(input_ids.length * 2);
      hidden.fill(calls); return hidden;
    },
    async gatherOutputEmbeddings({ token_ids }) {
      embedding ??= new Float32Array(token_ids.length * 2);
      embedding.fill(-calls); return embedding;
    } };
  const left = createNativeDecisionBackbone({ encoder, runtime });
  const right = createNativeDecisionBackbone({ encoder, runtime });
  const questions = { q: { type: 'noul' } };
  const request = state => ({ state, questions, request_digest: decisionDigest({ state, questions }) });
  const [a, b] = await Promise.all([left.encodeAndForward(request('a')), right.encodeAndForward(request('b'))]);
  expect(a.hidden.every(x => x === 1)).toBe(true);
  expect(b.hidden.every(x => x === 2)).toBe(true);
  expect(a.lexical_options.every(x => x === -1)).toBe(true);
  expect(b.lexical_options.every(x => x === -2)).toBe(true);
  expect(left.deterministic).toBe(false);
});
