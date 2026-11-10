import { expect, test } from 'bun:test';
import { binomialRiskUpper, compileQuestionSchema, decodeDecisionAnswers, scoreDecisionLabels } from '../typed-decision.mjs';
import { assessDecision, decisionDigest, decisionRequest, evaluateDecisionOutcomes,
  validateDecisionReceipt, validateDecisionResponse } from '../decision-plane.mjs';
import { flowWorkCase, mcpToolCase } from '../frontier-adapters.mjs';

test('proper losses penalize confidently wrong distributions and preserve ordinal distance', () => {
  const schema = compileQuestionSchema({ keep: { type: 'noul' },
    route: { type: 'choice', criteria: { a: 'A', b: 'B' } },
    score: { type: 'score', criteria: ['low', 'middle', 'high'] } });
  const decoded = decodeDecisionAnswers(schema, {
    keep: { type: 'noul', noul: 0.8 },
    route: { type: 'choice', choice: 'a', confidence: 1, probabilities: { a: 1, b: 0 } },
    score: { type: 'score', score: 1.5, confidence: 0.5, probabilities: { 0: 0, 1: 0.5, 2: 0.5 } },
  });
  const labels = { keep: 'true', route: 'b', score: '2' };
  const scores = scoreDecisionLabels(decoded, labels);
  expect(scores.keep.brier).toBeCloseTo(0.08, 12);
  expect(scores.keep.log_loss).toBeCloseTo(-Math.log(0.8), 12);
  expect(scores.route).toMatchObject({ brier: 2, log_loss: null, infinite_log_loss: true, correct: false });
  expect(scores.score.ranked_probability_score).toBe(0.125);
  expect(scoreDecisionLabels(decoded, { ...labels, score: '0' }).score.ranked_probability_score).toBe(0.625);
  expect(() => scoreDecisionLabels(decoded, { ...labels, keep: true })).toThrow('allowed option');
  expect(() => scoreDecisionLabels(decoded, { ...labels, score: 'new' })).toThrow('allowed option');
  expect(() => scoreDecisionLabels(decoded, { keep: 'true' })).toThrow('every question');
});

test('exact risk limits agree with known binomial tails and finite-sample coverage', () => {
  expect(binomialRiskUpper(0, 0)).toBe(1);
  expect(binomialRiskUpper(0, 1)).toBeCloseTo(0.95, 14);
  expect(binomialRiskUpper(0, 100)).toBeCloseTo(1 - 0.05 ** 0.01, 14);
  // For n=2,k=1: P[X<=1]=1-p^2, so upper=sqrt(0.95).
  expect(binomialRiskUpper(1, 2)).toBeCloseTo(Math.sqrt(0.95), 14);
  expect(binomialRiskUpper(10, 10)).toBe(1);
  for (let n = 1; n <= 16; n++) {
    const bounds = Array.from({ length: n + 1 }, (_, k) => binomialRiskUpper(k, n));
    for (let index = 1; index < 100; index++) {
      const p = index / 100;
      let mass = (1 - p) ** n, failure = 0;
      for (let k = 0; k <= n; k++) {
        if (bounds[k] < p) failure += mass;
        mass *= (n - k) / (k + 1) * p / (1 - p);
      }
      expect(failure).toBeLessThanOrEqual(0.05 + 1e-12);
    }
  }
  for (const args of [[-1, 1], [2, 1], [0, 1.5], [0, 100001], [0, 1, NaN], [0, 1, 1]])
    expect(() => binomialRiskUpper(...args)).toThrow('risk counts');
});

test('ordinary MCP and Flow receipts share revision-bound outcome evaluation without model self-labels', async () => {
  const local = { model: 'synthetic-outcomes-not-clef', artifact_digest: 'a'.repeat(64),
    question_semantics: 'joint_schema', evaluate: ({ schema }) => new Float64Array(schema.width) };
  const cases = [
    mcpToolCase({ tools: [{ name: 'read', description: 'Read', annotations: { readOnlyHint: true } }] },
      ['read'], 'Read evidence', 'private'),
    flowWorkCase({ id: 'work:1', title: 'Review evidence', information_policy_class: 'private' },
      [{ id: 'review', description: 'Review', owner_ref: 'Flow:review' }], decisionDigest('exact owner')),
  ];
  const rows = await Promise.all(cases.map(async (compiled, i) => {
    const labels = i ? { phenotype: 'review', urgency: '1', needs_review: 'true' } : { route: 'read', needs_review: 'true' };
    const evaluation_unit = `fixture:${i}`;
    const label_receipt = { schema: 'bonfyre.decision-label.v1', labels, evaluation_unit,
      case_digest: compiled.case_digest, source_ref: `external-reviewed-fixture:${i}` };
    return { case: compiled, receipt: await assessDecision(compiled, { localEvaluator: local, execute: true }),
      labels, label_receipt, label_receipt_digest: decisionDigest(label_receipt), evaluation_unit }; }));
  const result = evaluateDecisionOutcomes(rows);
  expect(result).toMatchObject({ cases: 2, action: 'none' });
  expect(result.groups).toHaveLength(2);
  expect(result.groups.map(group => group.identity.owner).sort()).toEqual(['Flow', 'MCP']);
  expect(result.groups[0].fields.route.brier).toBe(0.5);
  expect(result.groups[0].identity.model).toContain(local.artifact_digest);
  expect(JSON.stringify(result)).not.toContain('Read evidence');
  expect(() => evaluateDecisionOutcomes([...rows, rows[0]])).toThrow('distinct held-out');
  expect(() => evaluateDecisionOutcomes(rows, { targetRequestDigests: new Set([rows[0].receipt.request_digest]) }))
    .toThrow('distinct held-out');
  const tampered = structuredClone(rows[0]); tampered.receipt.answers.needs_review.noul = 0.9;
  expect(() => validateDecisionReceipt(tampered.case, tampered.receipt)).toThrow('mutated');
  const wrongArtifact = { ...rows[0].receipt, model_artifact_digest: 'b'.repeat(64) };
  expect(() => validateDecisionReceipt(rows[0].case, wrongArtifact)).toThrow('artifact mismatch');
  const wrongIdentity = { ...rows[0].receipt, model_identity_state: 'provider_reported_revision' };
  expect(() => validateDecisionReceipt(rows[0].case, wrongIdentity)).toThrow('identity mismatch');
  const relabeled = structuredClone(rows); relabeled[0].labels.needs_review = 'false';
  expect(() => evaluateDecisionOutcomes(relabeled)).toThrow('label receipt');
  relabeled[0].label_receipt.labels = relabeled[0].labels;
  relabeled[0].label_receipt_digest = decisionDigest(relabeled[0].label_receipt);
  expect(evaluateDecisionOutcomes(relabeled).evidence_digest).not.toBe(result.evidence_digest);
  const changed = structuredClone(rows[0]); changed.case.state.intent = 'Changed after assessment';
  expect(() => evaluateDecisionOutcomes([changed])).toThrow('stale');
});

test('held-out replay measures authored selection loss against always-review without authorizing effects', async () => {
  const tools = { tools: [
    { name: 'read_a', description: 'Read A', annotations: { readOnlyHint: true } },
    { name: 'read_b', description: 'Read B', annotations: { readOnlyHint: true } },
  ] };
  const local = { model: 'synthetic-selection-not-clef-or-jev', artifact_digest: 'c'.repeat(64),
    question_semantics: 'joint_schema',
    evaluate: ({ state, schema }) => {
      const logits = new Float64Array(schema.width);
      const route = schema.fields.find(field => field.id === 'route');
      const review = schema.fields.find(field => field.id === 'needs_review');
      logits[route.offset + route.options.indexOf('read_a')] = 10;
      logits[review.offset] = state.intent === 'review' ? 10 : -10;
      logits[review.offset + 1] = state.intent === 'review' ? -10 : 10;
      return logits;
    } };
  const labelsByIntent = { correct: { route: 'read_a', needs_review: 'false' },
    wrong: { route: 'read_b', needs_review: 'true' },
    review: { route: 'read_a', needs_review: 'true' } };
  const rows = await Promise.all(Object.entries(labelsByIntent).map(async ([intent, labels]) => {
    const compiled = mcpToolCase(tools, ['read_a', 'read_b'], intent, 'private');
    const evaluation_unit = `held-out:${intent}`;
    const label_receipt = { schema: 'bonfyre.decision-label.v1', labels, evaluation_unit,
      case_digest: compiled.case_digest, source_ref: `reviewed-fixture:${intent}` };
    return { case: compiled, receipt: await assessDecision(compiled, { localEvaluator: local, execute: true }),
      labels, label_receipt, label_receipt_digest: decisionDigest(label_receipt), evaluation_unit };
  }));
  const report = evaluateDecisionOutcomes(rows);
  expect(report.action).toBe('none');
  expect(report.groups).toHaveLength(1);
  expect(report.groups[0].selection).toEqual({ candidate_suggestions: 2, review_deferrals: 1,
    wrong_suggestions: 1, review_required_bypassed: 1,
    realized_loss_units: 1.25, always_review_loss_units: 0.75,
    net_saved_loss_units: -0.5, mean_realized_loss_units: 1.25 / 3,
    mean_always_review_loss_units: 0.25 });
  expect(report.cannot_infer).toContain('measured operational value');
  const changedLabel = structuredClone(rows);
  changedLabel[1].labels.route = 'read_a';
  expect(() => evaluateDecisionOutcomes(changedLabel)).toThrow('label receipt');
});

test('a supplied score legend must match the authored rubric; internal answers may omit the echo', () => {
  const compiled = flowWorkCase({ id: 'work:legend', title: 'Inspect', information_policy_class: 'private' },
    [{ id: 'review', description: 'Review', owner_ref: 'Flow:review' }], decisionDigest('legend read'));
  const request = decisionRequest(compiled, 'jev');
  const score = { type: 'score', score: 1, confidence: 0.2,
    probabilities: { 0: 0, 1: 1, 2: 0 } };
  const response = { model: 'jev-latest', answers: {
    phenotype: { type: 'choice', choice: 'review', confidence: 0.1,
      probabilities: { review: 1, none: 0 } },
    urgency: { ...score, legend: { 0: 'routine', 1: 'soon', 2: 'immediate' } },
    needs_review: { type: 'noul', noul: 0 },
  } };
  expect(validateDecisionResponse(response, request).urgency.score).toBe(1);
  expect(validateDecisionResponse({ ...response, answers: { ...response.answers, urgency: score } }, request).urgency.score).toBe(1);
  expect(() => validateDecisionResponse({ ...response, answers: { ...response.answers,
    urgency: { ...response.answers.urgency, legend: { 0: 'routine', 1: 'immediate', 2: 'soon' } } } },
  request)).toThrow('score legend disagrees');
});
