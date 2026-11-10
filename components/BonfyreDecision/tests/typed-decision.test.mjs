import { expect, test } from 'bun:test';
import { answersFromLogits, compileQuestionSchema, compileDecisionProjection, conjunctionBounds,
  decodeDecisionAnswers, executeDecisionProjection, minimumExpectedLoss } from '../typed-decision.mjs';
import { assessDecision, assessDecisionCases, compileDecisionCase, decisionDigest,
  decisionModelPassport, decisionPlanePolicy, decisionProposal, decisionReceiptRequest } from '../decision-plane.mjs';
import { mcpToolCase, flowWorkCase } from '../frontier-adapters.mjs';

const questions = {
  keep: { type: 'noul' },
  route: { type: 'choice', criteria: { fast: 'Local', slow: 'Deep review' } },
  priority: { type: 'score', criteria: ['routine', 'soon', 'now'] },
};
const schema = compileQuestionSchema(questions);
const response = () => ({
  keep: { type: 'noul', noul: 0.8 },
  route: { type: 'choice', choice: 'fast', confidence: 0.4, probabilities: { fast: 0.8, slow: 0.2 } },
  priority: { type: 'score', score: 1.3, confidence: 0.1, probabilities: { 0: 0.2, 1: 0.3, 2: 0.5 } },
});
const passage = (id = 'one', information_policy_class = 'public') => compileDecisionCase({
  program: 'context_passage', owner: 'ContextCut', subject_id: id,
  mask_digest: decisionDigest(id), gate_state: 'ADMIT', information_policy_class,
  frontier: [{ id, description: id, owner_ref: 'ContextCut:test' }],
  state: { purpose: 'Explain', passage: { id, text: `body ${id}` } },
});
const local = {
  model: 'fixture-not-clef', artifact_digest: 'a'.repeat(64), question_semantics: 'joint_schema',
  evaluate: ({ schema }) => new Float64Array(schema.width),
};

test('all primitives lower to a bounded contiguous typed representation', () => {
  expect(schema.width).toBe(7);
  expect(schema.fields.map(f => f.offset)).toEqual([0, 2, 4]);
  const decoded = decodeDecisionAnswers(schema, response());
  expect([...decoded.columns]).toEqual([0.8, 1 - 0.8, 0.8, 0.2, 0.2, 0.3, 0.5]);
  expect(decoded.moments.route.maximum).toBe(0.8);
  expect(decoded.moments.route.provider_confidence).toBe(0.4);
  expect(decoded.moments.priority.expectation).toBe(1.3);
  expect(() => compileQuestionSchema({})).toThrow('empty schema');
  expect(() => compileQuestionSchema(questions, { max_questions: 2 })).toThrow('budget');
  expect(() => compileQuestionSchema({ bad: { type: 'generate' } })).toThrow('unknown');
});

test('malformed marginal answers cannot become machine branches', () => {
  for (const bad of [NaN, Infinity, -1, 1.01]) {
    const answers = response(); answers.keep.noul = bad;
    expect(() => decodeDecisionAnswers(schema, answers)).toThrow('invalid noul');
  }
  const badWinner = response(); badWinner.route.choice = 'slow';
  expect(() => decodeDecisionAnswers(schema, badWinner)).toThrow('maximum');
  const badMean = response(); badMean.priority.score = 1.2;
  expect(() => decodeDecisionAnswers(schema, badMean)).toThrow('expectation');
  const badMass = response(); badMass.route.probabilities.fast = 0.81;
  expect(() => decodeDecisionAnswers(schema, badMass)).toThrow('sum to one');
  const incomplete = response(); delete incomplete.priority.probabilities;
  expect(() => decodeDecisionAnswers(schema, incomplete)).toThrow('probability distribution');
  const extra = { ...response(), hidden: { type: 'noul', noul: 1 } };
  expect(() => decodeDecisionAnswers(schema, extra)).toThrow('shape');
  // Four-decimal wire rounding is accepted, never silently normalized.
  const thirds = compileQuestionSchema({ x: { type: 'choice', criteria: { a: '', b: '', c: '' } } });
  expect(decodeDecisionAnswers(thirds, { x: { type: 'choice', choice: 'b', confidence: 0.3333,
    probabilities: { a: 0.3333, b: 0.3333, c: 0.3333 } } }).columns[0]).toBe(0.3333);
});

test('default uncertainty distinguishes TypeSafe concentration from Clef peak probability', () => {
  const categorical = decodeDecisionAnswers(schema, response()).moments.route;
  expect(categorical.maximum).toBe(0.8);
  expect(categorical.concentration).toBeCloseTo(0.6, 12);
  expect(categorical.provider_confidence).toBe(0.4);
  const score = compileQuestionSchema({ x: { type: 'score', criteria: ['low', 'middle', 'high'] } });
  const stats = probabilities => decodeDecisionAnswers(score, { x: { type: 'score',
    probabilities: Object.fromEntries(probabilities.map((p, i) => [String(i), p])),
    score: probabilities.reduce((sum, p, i) => sum + p * i, 0), confidence: 0 } }).moments.x;
  expect(stats([0, 0.5, 0.5])).toMatchObject({ concentration: 0.25, ordinal_concentration: 0.25 });
  expect(stats([0.5, 0, 0.5])).toMatchObject({ concentration: 0.25, ordinal_concentration: 0 });
});

test('logit lowering is stable, finite, and preserves all allowed options', () => {
  const logits = new Float64Array([10000, 10000, -10000, 10000, 1, 2, 3]);
  const answers = answersFromLogits(schema, logits);
  expect(answers.keep.noul).toBe(0.5);
  expect(answers.route.choice).toBe('slow');
  expect(answers.route.probabilities).toEqual({ fast: 0, slow: 1 });
  expect(decodeDecisionAnswers(schema, answers).columns.length).toBe(7);
  expect(() => answersFromLogits(schema, new Float64Array(6))).toThrow('schema');
  logits[0] = Infinity;
  expect(() => answersFromLogits(schema, logits)).toThrow('nonfinite');
});

test('composition retains dependence-free bounds rather than inventing joint probabilities', () => {
  expect(conjunctionBounds([0.9, 0.8])).toEqual({ lower: 0.7000000000000002, upper: 0.8 });
  expect(conjunctionBounds([0.2, 0.8])).toEqual({ lower: 0, upper: 0.2 });
  const projection = compileDecisionProjection(schema, {
    weight: { stat: 'probability', question: 'keep', option: 'true' },
    rank: { stat: 'expectation', question: 'priority' },
  });
  expect(executeDecisionProjection(projection, decodeDecisionAnswers(schema, response())))
    .toEqual({ weight: 0.8, rank: 1.3 });
  expect(() => compileDecisionProjection(schema, { bad: { stat: 'all_lower', questions: ['keep', 'keep'] } }))
    .toThrow('distinct');
  expect(() => compileDecisionProjection(schema, { bad: { stat: 'probability', question: 'route', option: 'invented' } }))
    .toThrow('unknown option');
});

test('conjunction bounds contain every joint distribution on a finite exhaustive grid', () => {
  for (let both = 0; both <= 10; both++) {
    for (let leftOnly = 0; leftOnly <= 10 - both; leftOnly++) {
      for (let rightOnly = 0; rightOnly <= 10 - both - leftOnly; rightOnly++) {
        const bounds = conjunctionBounds([(both + leftOnly) / 10, (both + rightOnly) / 10]);
        expect(bounds.lower).toBeLessThanOrEqual(both / 10 + 1e-12);
        expect(bounds.upper).toBeGreaterThanOrEqual(both / 10 - 1e-12);
      }
    }
  }
});

test('logit softmax is invariant to a constant shift in each field', () => {
  const input = new Float64Array([-3, 7, 10, 11, -1, 0, 1]);
  const shifted = Float64Array.from(input, x => x + 10000);
  const left = decodeDecisionAnswers(schema, answersFromLogits(schema, input)).columns;
  const right = decodeDecisionAnswers(schema, answersFromLogits(schema, shifted)).columns;
  left.forEach((p, i) => expect(p).toBeCloseTo(right[i], 12));
});

test('asymmetric owner losses can prefer review despite the most likely answer', () => {
  const decoded = decodeDecisionAnswers(schema, response());
  expect(minimumExpectedLoss(decoded, 'route', {
    fast: { fast: 0, slow: 10 }, slow: { fast: 1, slow: 0 }, review: { fast: 0.25, slow: 0.25 },
  })).toMatchObject({ minimizing_actions: ['review'], minimum_expected_loss: 0.25, action: 'none' });
  expect(() => minimumExpectedLoss(decoded, 'route', { fast: { fast: 0 } })).toThrow('every outcome');
});

test('normal assessments execute authored programs, including private local logits without egress', async () => {
  let network = 0;
  const compiled = passage('private', 'private');
  const receipt = await assessDecision(compiled, { execute: true, localEvaluator: local,
    fetcher: () => { network++; throw new Error('unexpected egress'); } });
  expect(network).toBe(0);
  expect(receipt.proposal).toMatchObject({ candidate_id: 'private', relevance: 0.5,
    rank_hint: 0, rank_upper: 0.5, action: 'none' });
  expect(decisionReceiptRequest(compiled, receipt).model).toBe(local.model);
  expect(receipt.request_digest).toBe(decisionDigest(decisionReceiptRequest(compiled, receipt)));
  expect(decisionModelPassport(receipt)).toContain(local.artifact_digest);
  expect(decisionModelPassport({ ...receipt, model_artifact_digest: 'b'.repeat(64) }))
    .not.toBe(decisionModelPassport(receipt));
  const planned = await assessDecision(compiled, { localEvaluator: local });
  expect(planned).toMatchObject({ state: 'planned', provider: 'local', model_identity_state: 'artifact_bound' });
  expect(planned.request_digest).toBe(receipt.request_digest);
  const run = await assessDecisionCases([compiled, compiled], { execute: true, localEvaluator: local, maxCalls: 1 });
  expect(run.summary).toMatchObject({ provider_calls: 0, local_calls: 1, duplicate_calls_avoided: 1 });
  const stale = structuredClone(compiled); stale.questions.relevant.instructions = 'Different question';
  await expect(assessDecision(stale)).rejects.toThrow('changed after compilation');
  const changedPolicy = structuredClone(decisionPlanePolicy());
  changedPolicy.programs.context_passage.outputs.rank_hint.stat = 'all_upper';
  expect(() => decisionProposal(receipt, changedPolicy)).toThrow('changed since assessment');
});

test('runtime provenance distinguishes a hosted alias from a reported revision', async () => {
  for (const [model, identity] of [['clef-flash', 'mutable_alias'], ['clef-fixture-r1', 'provider_reported_revision']]) {
    const receipt = await assessDecision(passage(), { provider: 'clef-flash', execute: true, apiKey: 'test', accountId: 'a'.repeat(32),
      fetcher: async (_url, options) => {
        const schema = compileQuestionSchema(JSON.parse(options.body).questions);
        return { ok: true, json: async () => ({ success: true,
          result: { model, answers: answersFromLogits(schema, new Float64Array(schema.width)) } }) };
      } });
    expect(receipt.model_identity_state).toBe(identity);
    expect(receipt.question_semantics).toBe('joint_schema');
  }
});

test('MCP and Flow use the same authored risk and review logic by default', async () => {
  const tool = mcpToolCase({ tools: [{ name: 'read', description: 'Read', annotations: { readOnlyHint: true } }] },
    ['read'], 'Read', 'private');
  const flow = flowWorkCase({ id: 'work', title: 'Read', information_policy_class: 'private' },
    [{ id: 'review', description: 'Review', owner_ref: 'Flow:review' }], decisionDigest('flow'));
  for (const compiled of [tool, flow]) {
    const receipt = await assessDecision(compiled, { execute: true, localEvaluator: local });
    expect(receipt.proposal).toMatchObject({ suggested_candidate: 'none', action: 'none',
      selection: { disposition: 'review', reason: 'review_probability' } });
    expect(receipt.proposal.distribution.none).toBe(0.5);
  }
});

test('one failed assessment cancels siblings and stops scheduling subsequent cases', async () => {
  let calls = 0;
  await expect(assessDecisionCases(Array.from({ length: 20 }, (_, i) => passage(String(i))), {
    provider: 'clef-flash', execute: true, maxCalls: 20, concurrency: 2, apiKey: 'test', accountId: 'a'.repeat(32),
    fetcher: async (_url, { signal }) => {
      calls++;
      if (calls === 1) throw new Error('test provider failure');
      await new Promise((_, reject) => signal.addEventListener('abort', () => reject(signal.reason), { once: true }));
    },
  })).rejects.toThrow('test provider failure');
  expect(calls).toBeLessThanOrEqual(2);
});
