import { expect, test } from 'bun:test';
import { assessDecision, assessDecisionCases, compileDecisionCase, decisionDigest, decisionPlanePolicy } from '../decision-plane.mjs';
import { clearLocalDecisionInference } from '../local-inference.mjs';
import { flowWorkCase, mcpToolCase } from '../frontier-adapters.mjs';
import { answersFromLogits } from '../typed-decision.mjs';

const caseFor = (text = 'one', overrides = {}) => compileDecisionCase({
  program: 'context_passage', owner: 'ContextCut', subject_id: text,
  mask_digest: decisionDigest('owner-frontier'), gate_state: 'ADMIT', information_policy_class: 'private',
  frontier: [{ id: text, description: text, owner_ref: 'ContextCut:fixture' }],
  state: { purpose: 'Explain', passage: { id: text, text } }, ...overrides,
});
const owner = () => {
  let calls = 0;
  const local = { model: 'deterministic-fixture-not-clef', artifact_digest: 'a'.repeat(64),
    question_semantics: 'joint_schema', deterministic: true,
    evaluate: ({ schema }) => { calls++; return new Float32Array(schema.width); } };
  return { local, calls: () => calls };
};
const run = (compiled, local, policy, extras = {}) => assessDecision(compiled,
  { execute: true, localEvaluator: local, ...extras }, policy);

test('ordinary ContextCut, MCP and Flow assessments reuse logits but rebind each receipt', async () => {
  const { local, calls } = owner();
  const cases = [caseFor(),
    mcpToolCase({ tools: [{ name: 'read', description: 'Read', annotations: { readOnlyHint: true } }] },
      ['read'], 'Read the report', 'private'),
    flowWorkCase({ id: 'job', title: 'Inspect', information_policy_class: 'private' },
      [{ id: 'inspect', description: 'Inspect', owner_ref: 'Flow:inspect' }], decisionDigest('work'))];
  for (const compiled of cases) {
    const first = await run(compiled, local);
    first.answers[Object.keys(first.answers)[0]] = { corrupt: true };
    const second = await run(compiled, local);
    expect(second.inference).toMatchObject({ state: 'reused', executed: false });
    expect(second.proposal.action).toBe('none');
    expect(second.answers).not.toEqual(first.answers);
  }
  expect(calls()).toBe(3);
  const batch = await assessDecisionCases(cases, { execute: true, localEvaluator: local, maxCalls: 3 });
  expect(batch.summary).toMatchObject({ local_calls: 0, reused_local_calls: 3, provider_calls: 0 });
  // Same model input, distinct subject identity, never a reused authority receipt.
  const alias = compileDecisionCase({ ...cases[0].contract, subject_id: 'alias', state: cases[0].state });
  const receipt = await run(alias, local);
  expect(receipt.case_digest).toBe(alias.case_digest);
  expect(receipt.contract.subject_id).toBe('alias');
  expect(receipt.inference.state).toBe('reused');
});

test('model, recipe, state, mask, gate and information-policy changes invalidate reuse', async () => {
  const { local, calls } = owner();
  await run(caseFor(), local);
  await run(caseFor('other'), local);
  await run(caseFor('one', { mask_digest: decisionDigest('changed-mask') }), local);
  await run(caseFor('one', { gate_state: 'SHADOW' }), local);
  await run(caseFor('one', { information_policy_class: 'public' }), local);
  local.artifact_digest = 'b'.repeat(64); await run(caseFor(), local);
  local.model = 'another-fixture'; await run(caseFor(), local);
  local.question_semantics = 'independent_evaluation'; await run(caseFor(), local);
  const policy = structuredClone(decisionPlanePolicy());
  policy.programs.context_passage.outputs.rank_hint.stat = 'all_upper';
  const changed = compileDecisionCase({ ...caseFor().contract, state: caseFor().state }, policy);
  await run(changed, local, policy);
  expect(calls()).toBe(9);
  const stale = caseFor(); stale.state.passage.text = 'altered';
  await expect(run(stale, local)).rejects.toThrow('changed after compilation');
  expect(calls()).toBe(9);
});

test('bounded LRU retains no more than authored entries or logit bytes', async () => {
  const { local, calls } = owner(), policy = structuredClone(decisionPlanePolicy());
  policy.local_inference_residency = { max_entries: 2, max_logit_bytes: 64 };
  await run(caseFor('a'), local, policy); await run(caseFor('b'), local, policy);
  await run(caseFor('a'), local, policy); await run(caseFor('c'), local, policy);
  await run(caseFor('a'), local, policy); await run(caseFor('b'), local, policy);
  expect(calls()).toBe(4);
  policy.local_inference_residency.max_logit_bytes = 1;
  await run(caseFor('a'), local, policy); await run(caseFor('a'), local, policy);
  expect(calls()).toBe(6);
  policy.local_inference_residency.max_logit_bytes = 64;
  await run(caseFor('a'), local, policy);
  clearLocalDecisionInference(local);
  await run(caseFor('a'), local, policy);
  expect(calls()).toBe(8);
});

test('nondeterministic owners, failures, shared buffers and cancellation never populate cache', async () => {
  const { local, calls } = owner(); delete local.deterministic;
  await run(caseFor(), local); await run(caseFor(), local);
  expect(calls()).toBe(2);
  local.deterministic = true;
  const normal = local.evaluate;
  for (const bad of [() => { throw new Error('failed'); },
    ({ schema }) => new Float32Array(schema.width).fill(NaN),
    ({ schema }) => new Float32Array(new SharedArrayBuffer(schema.width * 4))]) {
    local.evaluate = bad;
    await expect(run(caseFor(), local)).rejects.toThrow();
  }
  const controller = new AbortController();
  local.evaluate = ({ schema }) => { controller.abort(); return new Float32Array(schema.width); };
  await expect(run(caseFor(), local, undefined, { signal: controller.signal })).rejects.toThrow();
  local.evaluate = normal;
  const receipt = await run(caseFor(), local);
  expect(receipt.inference.state).toBe('computed');
  expect(calls()).toBe(3);
  await expect(run(caseFor(), local, undefined, { signal: controller.signal })).rejects.toThrow();
});

test('cache owns output buffers and clear during pending inference prevents resurrection', async () => {
  const { local } = owner(), buffer = new Float32Array(8);
  local.evaluate = () => buffer;
  const first = await run(caseFor(), local);
  buffer.fill(100); buffer[0] = -100;
  expect((await run(caseFor(), local)).answers).toEqual(first.answers);
  let release;
  local.evaluate = () => new Promise(resolve => { release = resolve; });
  const pending = run(caseFor(), local);
  clearLocalDecisionInference(local); release(buffer);
  await pending;
  const next = run(caseFor(), local); release(buffer);
  expect((await next).inference.executed).toBe(true);
});

test('concurrent synchronous and asynchronous owners may safely recycle their output buffer', async () => {
  for (const asynchronous of [false, true]) {
    const { local } = owner(), shared = new Float32Array(8);
    local.evaluate = ({ state, schema }) => {
      shared.fill(0); shared[schema.fields.find(field => field.id === 'relevant').offset] = state.passage.text === 'positive' ? 10 : -10;
      return asynchronous ? Promise.resolve(shared) : shared;
    };
    const [positive, negative] = await Promise.all([
      run(caseFor('positive'), local), run(caseFor('negative'), local),
    ]);
    expect(positive.answers.relevant.noul).toBeGreaterThan(0.99);
    expect(negative.answers.relevant.noul).toBeLessThan(0.01);
  }
});

test('batch execution never merges distinct owner masks and mutable provenance is refused', async () => {
  const { local, calls } = owner();
  const result = await assessDecisionCases([caseFor(), caseFor('one', { mask_digest: decisionDigest('another-mask') })],
    { execute: true, maxCalls: 2, localEvaluator: local });
  expect(result.summary.local_calls).toBe(2);
  expect(calls()).toBe(2);
  clearLocalDecisionInference(local);
  let release;
  local.evaluate = () => new Promise(resolve => { release = resolve; });
  const pending = run(caseFor(), local);
  const queued = run(caseFor('queued'), local);
  // Register rejection handlers before completing the producer.
  const completed = Promise.allSettled([pending, queued]);
  local.artifact_digest = 'c'.repeat(64);
  release(new Float32Array(8));
  for (const record of await completed) {
    expect(record.status).toBe('rejected');
    expect(record.reason.message).toContain('owner changed');
  }
  await expect(run(caseFor(), { ...local, model: {} })).rejects.toThrow('pinned model');
});

test('caller mutation while inference awaits cannot rewrite the captured owner receipt', async () => {
  const { local } = owner(); let release;
  local.evaluate = () => new Promise(resolve => { release = resolve; });
  const compiled = caseFor(), digest = compiled.case_digest;
  const pending = run(compiled, local);
  compiled.contract.mask_digest = 'f'.repeat(64);
  compiled.contract.subject_id = 'forged';
  compiled.contract.frontier[0].id = 'forged';
  release(new Float32Array(8));
  const receipt = await pending;
  expect(receipt.case_digest).toBe(digest);
  expect(receipt.contract.subject_id).toBe('one');
  expect(receipt.contract.mask_digest).toBe(decisionDigest('owner-frontier'));
  expect(receipt.proposal.candidate_id).toBe('one');
});

test('typed local results preserve exact zero probabilities and cache only owned answers', async () => {
  let calls = 0;
  const source = { relevant: { type: 'noul', noul: 1 }, usable: { type: 'noul', noul: 0 },
    contradicts: { type: 'noul', noul: 0 }, instructs: { type: 'noul', noul: 0 } };
  const local = { model: 'synthetic-systemone-not-clef', artifact_digest: 'd'.repeat(64),
    question_semantics: 'joint_schema', deterministic: true,
    evaluate: () => { calls++; return { answers: source }; } };
  const first = await run(caseFor('typed'), local);
  expect(first.answers.usable.noul).toBe(0);
  expect(first.answers.relevant.noul).toBe(1);
  source.usable.noul = 1;
  first.answers.usable.noul = 1;
  const second = await run(caseFor('typed'), local);
  expect(second.inference).toMatchObject({ state: 'reused', executed: false });
  expect(second.answers.usable.noul).toBe(0);
  expect(calls).toBe(1);
  second.answers.usable.noul = 1;
  expect((await run(caseFor('typed'), local)).answers.usable.noul).toBe(0);
  const policy = structuredClone(decisionPlanePolicy());
  policy.local_inference_residency.max_logit_bytes = 1;
  await run(caseFor('typed'), local, policy);
  await run(caseFor('typed'), local, policy);
  expect(calls).toBe(3);
});

test('malformed typed local answers are refused and never cached', async () => {
  let calls = 0, probability = 2;
  const local = { model: 'synthetic-systemone-not-clef', artifact_digest: 'e'.repeat(64),
    question_semantics: 'joint_schema', deterministic: true,
    evaluate: () => { calls++; return { answers: {
      relevant: { type: 'noul', noul: probability }, usable: { type: 'noul', noul: 0 },
      contradicts: { type: 'noul', noul: 0 }, instructs: { type: 'noul', noul: 0 },
    } }; } };
  await expect(run(caseFor('malformed'), local)).rejects.toThrow('invalid noul');
  probability = 0;
  expect((await run(caseFor('malformed'), local)).inference.executed).toBe(true);
  expect((await run(caseFor('malformed'), local)).inference.state).toBe('reused');
  expect(calls).toBe(2);
});

test('typed local score legends must match the entry-time rubric before normalization or caching', async () => {
  const compiled = flowWorkCase({ id: 'legend-job', title: 'Inspect', information_policy_class: 'private' },
    [{ id: 'inspect', description: 'Inspect', owner_ref: 'Flow:inspect' }], decisionDigest('legend-mask'));
  const rubric = compiled.questions.urgency.criteria;
  const valid = Object.fromEntries(rubric.map((criterion, index) => [String(index), criterion]).reverse());
  const malformed = [null, rubric, {}, { ...valid, 0: 'Conflicting rubric' },
    { ...valid, extra: 'Unrequested level' }, { ...valid, 0: 123 }];
  let calls = 0, legend, rewriteRubric = false;
  const local = { model: 'synthetic-legend-not-clef', artifact_digest: 'f'.repeat(64),
    question_semantics: 'joint_schema', deterministic: true,
    evaluate: ({ schema, questions }) => {
      calls++;
      const answers = answersFromLogits(schema, new Float32Array(schema.width));
      if (rewriteRubric) questions.urgency.criteria[0] = 'Conflicting rubric';
      answers.urgency.legend = legend;
      return { answers };
    } };
  for (const invalid of malformed) {
    legend = invalid;
    await expect(run(compiled, local)).rejects.toThrow('legend disagrees with the authored criteria');
  }
  // An owner cannot make its conflicting returned legend valid by mutating
  // the request it was handed, including across the owned inference boundary.
  rewriteRubric = true;
  legend = { ...valid, 0: 'Conflicting rubric' };
  await expect(run(compiled, local)).rejects.toThrow('legend disagrees with the authored criteria');
  expect(compiled.questions.urgency.criteria).toEqual(rubric);
  rewriteRubric = false;
  legend = valid;
  const receipt = await run(compiled, local);
  expect(receipt.inference.executed).toBe(true);
  expect(receipt.answers.urgency.legend).toBeUndefined();
  expect((await run(compiled, local)).inference.state).toBe('reused');
  expect(calls).toBe(malformed.length + 2);
  // Missing legends remain compatible with the internal native answer ABI.
  clearLocalDecisionInference(local);
  legend = undefined;
  expect((await run(compiled, local)).inference.executed).toBe(true);
});
