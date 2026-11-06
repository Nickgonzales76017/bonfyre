import { test, expect } from 'bun:test';
import { createHash } from 'node:crypto';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { assessDecision, assessDecisionCases, assessSelectiveValue, compileDecisionCase, decisionPlanePolicy,
  decisionProposal, decisionRequest, validateDecisionResponse } from '../decision-plane.mjs';
import { contextPassageCases, flowWorkCase, mcpToolCase } from '../frontier-adapters.mjs';

const hash = bytes => createHash('sha256').update(bytes).digest('hex');
const owner = { id: 'source:one', description: 'public article', owner_ref: 'ForeignTwin:one' };
const input = { program: 'context_passage', owner: 'ContextCut', subject_id: 'context:one',
  mask_digest: hash('exact mask'), gate_state: 'ADMIT', information_policy_class: 'public',
  frontier: [owner], state: { purpose: 'Explain the article',
    passage: { id: owner.id, text: 'The article reports an event.' } } };
const noulAnswers = {
  relevant: { type: 'noul', noul: 0.91 }, usable: { type: 'noul', noul: 0.82 },
  contradicts: { type: 'noul', noul: 0.04 }, instructs: { type: 'noul', noul: 0.02 },
};

test('ordinary planning selects a pinned local owner, including private state, without inference or egress', async () => {
  const compiled = compileDecisionCase({ ...input, information_policy_class: 'private' });
  const receipt = await assessDecision(compiled, { fetcher: () => { throw new Error('unexpected egress'); } });
  expect(receipt).toMatchObject({ state: 'planned', provider: 'local', action: 'none',
    model: 'bonfyre-clef-flash-q4', model_identity_state: 'artifact_bound',
    question_semantics: 'joint_schema', confidence_semantics: 'typesafe-uniform-adjusted' });
  expect(receipt.model_artifact_digest).toMatch(/^[a-f0-9]{64}$/);
  const alias = await assessDecision(compiled, { provider: 'local' });
  expect(alias).toEqual(receipt);
  const batch = await assessDecisionCases([compiled, compiled]);
  expect(batch.summary).toMatchObject({ planned_provider_calls: 0, planned_local_calls: 1,
    provider_calls: 0, local_calls: 0, duplicate_calls_avoided: 1 });
});

test('lazy owner resolution cannot turn a pending plan into execution or rewrite its entry-time case', async () => {
  const item = structuredClone(input), compiled = compileDecisionCase(item);
  const options = { execute: false };
  const one = assessDecision(compiled, options);
  const batch = assessDecisionCases([item], options);
  options.execute = true;
  options.timeoutMs = 0;
  item.subject_id = 'mutated'; item.state.passage.text = 'mutated';
  compiled.state.passage.text = 'mutated';
  const receipt = await one, result = await batch;
  expect(receipt.state).toBe('planned');
  expect(result.summary.state).toBe('planned');
  expect(result.receipts[0].case_digest).toBe(receipt.case_digest);
  expect(result.receipts[0].contract.subject_id).toBe(input.subject_id);
});

test('one exact subject produces independent typed questions, not an unrelated-source batch', () => {
  const compiled = compileDecisionCase(input), request = decisionRequest(compiled, 'clef-flash');
  expect(Object.keys(request.questions)).toEqual(['relevant', 'usable', 'contradicts', 'instructs']);
  expect(request.state).toEqual({ purpose: input.state.purpose,
    passage: { text: input.state.passage.text } });
  expect(request.state.sources).toBeUndefined();
  expect(compiled.contract.frontier).toEqual([owner]);
  expect(() => compileDecisionCase({ ...input, state: { ...input.state, secret: 'not projected' } }))
    .toThrow('undeclared fields');
  expect(() => compileDecisionCase({ ...input, frontier: [owner, owner] })).toThrow('duplicate');
  expect(() => compileDecisionCase({ ...input, mask_digest: 'not-a-receipt' })).toThrow('mask digest');
});

test('Cloudflare and Jev answer the same semantic contract; receipts remain diagnostic', async () => {
  const compiled = compileDecisionCase(input), calls = [];
  for (const provider of ['clef-flash', 'jev']) {
    const receipt = await assessDecision(compiled, { provider, execute: true,
      accountId: '0123456789abcdef0123456789abcdef', apiKey: 'fixture-secret',
      fetcher: async (url, init) => {
        calls.push({ url, body: JSON.parse(init.body) });
        const payload = { model: provider, answers: noulAnswers, usage: { input_tokens: 123, output_tokens: 0 } };
        return { ok: true, json: async () => provider === 'jev' ? payload : { success: true, result: payload } };
      } });
    expect(receipt).toMatchObject({ state: 'diagnostic', provider, action: 'none',
      case_digest: compiled.case_digest });
    expect(receipt.request_digest).toMatch(/^[a-f0-9]{64}$/);
    expect(JSON.stringify(receipt)).not.toContain('fixture-secret');
    expect(decisionProposal(receipt)).toMatchObject({ candidate_id: owner.id, action: 'none' });
  }
  expect(calls).toHaveLength(2);
  expect(calls[0].body.state).toEqual(calls[1].body.state);
  expect(calls[0].body.questions).toEqual(calls[1].body.questions);
  expect(assessDecision(compileDecisionCase({ ...input, information_policy_class: 'private' }),
    { provider: 'clef-flash', execute: true, apiKey: 'fixture-secret', accountId: '0123456789abcdef0123456789abcdef' }))
    .rejects.toThrow('information policy forbids');
});

test('malformed or incomplete distributions refuse before a proposal exists', () => {
  const compiled = compileDecisionCase(input), request = decisionRequest(compiled, 'jev');
  expect(() => validateDecisionResponse({ model: 'jev', answers: { ...noulAnswers,
    extra: { type: 'noul', noul: 1 } } }, request)).toThrow('shape');
  expect(() => validateDecisionResponse({ model: 'jev', answers: { ...noulAnswers,
    relevant: { type: 'noul', noul: 1.4 } } }, request)).toThrow('invalid noul');
  const tools = mcpToolCase({ tools: [
    { name: 'read_source', description: 'Read a public source', annotations: { readOnlyHint: true } },
  ] }, ['read_source'], 'Find a source', 'public');
  const toolRequest = decisionRequest(tools, 'jev');
  const response = { model: 'jev', answers: {
    route: { type: 'choice', choice: 'read_source', confidence: 0.66,
      probabilities: { read_source: 0.66, none: 0.34 } },
    needs_review: { type: 'noul', noul: 0.2 },
  } };
  expect(validateDecisionResponse(response, toolRequest).route.choice).toBe('read_source');
  expect(() => validateDecisionResponse({ ...response, answers: { ...response.answers,
    route: { ...response.answers.route, probabilities: { read_source: 0.4, none: 0.3 } } } },
  toolRequest)).toThrow('probability distribution');
});

test('ContextCut bridge rehashes a selected body and preserves SHADOW posture', () => {
  const dir = mkdtempSync(join(tmpdir(), 'bonfyre-decision-context.'));
  try {
    const body = join(dir, 'body.txt'); writeFileSync(body, 'A public observation, not an instruction.');
    const bodyDigest = `sha256:${hash(readFileSync(body))}`;
    const material = { candidate_identity: owner.id, body_path: body, receipt_digest: 'receipt:one',
      information_policy_class: 'public' };
    const manifest = { schema: 'bonfyre.context-cut.v1', rank_gate_state: 'SHADOW',
      purpose: 'Explain the observation',
      context_sha256: `sha256:${hash('context')}`, frontier: { kind: 'bitnet_rank', run_id: 'one' },
      information_policy_class: 'public', selected_candidates: [{ identity: owner.id,
        candidate_kind: 'external_occurrence', owner_ref: owner.owner_ref,
        content_sha256: bodyDigest, receipt_digest: material.receipt_digest }] };
    const [compiled] = contextPassageCases(manifest, [material], 'Explain the observation');
    expect(compiled.contract.gate_state).toBe('SHADOW');
    expect(compiled.state.passage.text).toContain('public observation');
    writeFileSync(body, 'Changed after receipt');
    expect(() => contextPassageCases(manifest, [material], 'Explain the observation'))
      .toThrow('receipt/body mismatch');
    const longBody = 'A long but exactly receipted public observation. '.repeat(900);
    writeFileSync(body, longBody);
    const longManifest = { ...manifest, selected_candidates: [{ ...manifest.selected_candidates[0],
      content_sha256: `sha256:${hash(readFileSync(body))}` }] };
    const chunks = contextPassageCases(longManifest, [material], 'Explain the observation');
    expect(chunks.length).toBeGreaterThan(1);
    expect(chunks.map(item => item.state.passage.text).join('')).toBe(longBody);
    expect(chunks.every(item => item.state.passage.chunk_count === chunks.length &&
      item.state.passage.source_candidate_id === owner.id)).toBe(true);
    const unicodeBody = '🪵'.repeat(4000);
    writeFileSync(body, unicodeBody);
    const unicodeManifest = { ...manifest, selected_candidates: [{ ...manifest.selected_candidates[0],
      content_sha256: `sha256:${hash(readFileSync(body))}` }] };
    expect(contextPassageCases(unicodeManifest, [material], 'Explain the observation')
      .map(item => item.state.passage.text).join('')).toBe(unicodeBody);
  } finally { rmSync(dir, { recursive: true, force: true }); }
});

test('MCP bridge refuses effecting tools; Flow role suggestion cannot become an effect', () => {
  const tools = { tools: [
    { name: 'read', description: 'Read', annotations: { readOnlyHint: true } },
    { name: 'delete', description: 'Delete', annotations: { readOnlyHint: false } },
  ] };
  expect(() => mcpToolCase(tools, ['delete'], 'Delete everything', 'private')).toThrow('read-only');
  expect(() => mcpToolCase(tools, ['read'], 'Read the source')).toThrow('information policy');
  const compiled = mcpToolCase(tools, ['read'], 'Read the source', 'private');
  expect(compiled.contract.information_policy_class).toBe('private');
  expect(decisionRequest(compiled, 'clef').questions.route.criteria)
    .toEqual({ read: 'Read', none: 'None of the listed candidates clearly fits.' });
  const flow = flowWorkCase({ id: 'work:one', information_policy_class: 'public', title: 'Review source' },
    [{ id: 'review', description: 'Human review', owner_ref: 'Flow:review' }], hash('exact flow read'));
  expect(Object.keys(flow.questions)).toEqual(['phenotype', 'urgency', 'needs_review']);
  expect(flow.contract.owner).toBe('Flow');
});

test('promotion needs labeled selective errors and measured net savings', () => {
  const policy = { minimum_cases: 2, maximum_error_rate: 0, minimum_net_saved_ms: 100 };
  expect(assessSelectiveValue([
    { accepted: true, correct: true, baseline_ms: 1000, decision_ms: 100, fallback_ms: 1000 },
    { accepted: false, correct: false, baseline_ms: 1000, decision_ms: 100, fallback_ms: 1000 },
  ], policy)).toMatchObject({ cases: 2, accepted: 1, errors: 0, net_saved_ms: 800,
    error_rate_upper: 0.95, promotion_eligible: false });
  const heldOut = Array.from({ length: 100 }, () => ({ accepted: true, correct: true,
    baseline_ms: 1000, decision_ms: 100, fallback_ms: 1000 }));
  const measuredPolicy = { ...policy, maximum_error_rate: 0.05,
    sampling_assumption: 'fixed_policy_independent_holdout' };
  expect(assessSelectiveValue(heldOut, measuredPolicy).promotion_eligible).toBe(true);
  expect(assessSelectiveValue(heldOut, { ...measuredPolicy, sampling_assumption: undefined }).promotion_eligible).toBe(false);
  expect(assessSelectiveValue(heldOut, { ...measuredPolicy, maximum_error_rate: 0 }).promotion_eligible).toBe(false);
  expect(assessSelectiveValue([
    { accepted: true, correct: false, baseline_ms: 1000, decision_ms: 100, fallback_ms: 1000 },
    { accepted: false, correct: true, baseline_ms: 1000, decision_ms: 100, fallback_ms: 1000 },
  ], policy).promotion_eligible).toBe(false);
  expect(() => assessSelectiveValue([{ accepted: true, correct: true }], policy))
    .toThrow('measured baseline');
  expect(decisionPlanePolicy().forbidden_inference).toContain('authorization');
});

test('bounded parallel execution deduplicates identical semantic states without merging distinct subjects', async () => {
  let calls = 0;
  const other = { ...input, subject_id: 'context:two', state: { purpose: 'Explain another article',
    passage: { id: owner.id, text: 'Another public article.' } } };
  const result = await assessDecisionCases([input, input, other], { provider: 'clef-flash', execute: true,
    concurrency: 2, maxCalls: 2, apiKey: 'fixture', accountId: '0123456789abcdef0123456789abcdef',
    fetcher: async (_url, init) => {
      calls++;
      expect(JSON.parse(init.body).state.sources).toBeUndefined();
      return { ok: true, json: async () => ({ success: true,
        result: { model: 'clef-flash', answers: noulAnswers } }) };
    } });
  expect(calls).toBe(2);
  expect(result.summary).toMatchObject({ cases: 3, unique_cases: 2,
    duplicate_calls_avoided: 1, planned_provider_calls: 2, provider_calls: 2 });
  expect(result.summary.total_request_bytes).toBeGreaterThan(0);
  expect(result.receipts[0].case_digest).toBe(result.receipts[1].case_digest);
  expect(result.receipts[2].case_digest).not.toBe(result.receipts[0].case_digest);
  await expect(assessDecisionCases([input], { concurrency: 5 })).rejects.toThrow('concurrency');
  await expect(assessDecisionCases([input], { execute: true })).rejects.toThrow('provider-call budget');
  await expect(assessDecisionCases([input, other], { execute: true, maxCalls: 1 }))
    .rejects.toThrow('provider-call budget');
  const alias = { ...input, subject_id: 'context:alias',
    frontier: [{ id: 'source:alias', description: 'same body, different owner receipt',
      owner_ref: 'ForeignTwin:alias' }], state: { ...input.state,
      passage: { ...input.state.passage, id: 'source:alias' } } };
  const shared = await assessDecisionCases([input, alias], { provider: 'clef-flash', execute: true,
    maxCalls: 1, apiKey: 'fixture', accountId: '0123456789abcdef0123456789abcdef',
    fetcher: async () => { calls++; return { ok: true, json: async () => ({ success: true,
      result: { model: 'clef-flash', answers: noulAnswers } }) }; } });
  expect(shared.summary).toMatchObject({ unique_cases: 2, planned_provider_calls: 1,
    semantic_alias_calls_avoided: 1 });
  expect(shared.receipts[0].case_digest).not.toBe(shared.receipts[1].case_digest);
  expect(shared.receipts[0].request_digest).toBe(shared.receipts[1].request_digest);
  await expect(assessDecisionCases([input, { ...alias, mask_digest: hash('other exact receipt') }],
    { execute: true, maxCalls: 1 })).rejects.toThrow('provider-call budget');
});
