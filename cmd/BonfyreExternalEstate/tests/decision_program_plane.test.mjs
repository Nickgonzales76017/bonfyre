import { test, expect } from 'bun:test';
import { Database } from 'bun:sqlite';
import { mkdtempSync, rmSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { admittedDescriptors, compareDecisionRuns, compileDecisionProgramPlane, decisionPolicy, requestFor, validateAnswers } from '../tools/decision_program_plane.mjs';
import { installedDecisionOwner } from '../../../components/BonfyreDecision/installed-runtime.mjs';
import { answersFromLogits } from '../../../components/BonfyreDecision/typed-decision.mjs';

test('admitted Jev descriptors compile to exact non-effecting batches', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'bonfyre-jev-plane.'));
  try {
    const frontierPath = join(dir, 'frontier.db'), archivePath = join(dir, 'items.db');
    const frontier = new Database(frontierPath);
    frontier.exec(`CREATE TABLE channel(channel_id TEXT,exact_address TEXT,title TEXT,material TEXT,protocol TEXT,
      carries TEXT,cadence TEXT,census_source TEXT,contributions INTEGER,state TEXT);
      INSERT INTO channel VALUES('c1','https://example.org/feed','Example','audio','RSS','','daily','source:one',2,'admitted');
      INSERT INTO channel VALUES('c2','https://example.org/hidden','Hidden','audio','RSS','','daily','source:hidden',0,'candidate');`);
    frontier.close();
    const archive = new Database(archivePath);
    archive.exec(`CREATE TABLE item(source TEXT); INSERT INTO item VALUES('source:one');`);
    archive.close();
    const descriptors = admittedDescriptors(frontierPath, archivePath);
    expect(descriptors).toHaveLength(1);
    expect(descriptors[0]).toMatchObject({ channel_id: 'c1', retained_items: 1,
      retained_items_state: 'observed' });
    const planned = await compileDecisionProgramPlane(descriptors, { provider: 'jev', out: join(dir, 'plan.jsonl') });
    expect(planned.summary).toMatchObject({ mode: 'plan', admitted_sources: 1, planned: 1,
      diagnostic_assessments: 0, batches: 1 });
    expect(planned.rows[0]).toMatchObject({ state: 'planned', action: 'none', sources: 1 });
    const cli = Bun.spawnSync(['bun', new URL('../tools/decision_program_plane.mjs', import.meta.url).pathname,
      '--provider', 'jev', '--frontier', frontierPath, '--archive', archivePath, '--out', join(dir, 'plan.jsonl')]);
    expect(cli.exitCode).toBe(0);
    expect(JSON.parse(readFileSync(join(dir, 'plan.jsonl'), 'utf8'))).toEqual(planned.rows[0]);
    const clefPlan = Bun.spawnSync(['bun', new URL('../tools/decision_program_plane.mjs', import.meta.url).pathname,
      '--provider', 'clef-flash', '--frontier', frontierPath, '--out', join(dir, 'clef-plan.jsonl')]);
    expect(clefPlan.exitCode).toBe(0);
    expect(JSON.parse(readFileSync(join(dir, 'clef-plan.jsonl'), 'utf8'))).toMatchObject({
      provider: 'clef-flash', model: 'clef-flash', state: 'planned', action: 'none' });
    const localPlan = Bun.spawnSync(['bun', new URL('../tools/decision_program_plane.mjs', import.meta.url).pathname,
      '--frontier', frontierPath, '--out', join(dir, 'local-plan.jsonl')]);
    expect(localPlan.exitCode).toBe(0);
    expect(JSON.parse(readFileSync(join(dir, 'local-plan.jsonl'), 'utf8'))).toMatchObject({
      provider: 'local', model: installedDecisionOwner.model,
      model_artifact_digest: installedDecisionOwner.artifact_digest, state: 'planned', action: 'none' });
    const unbounded = Bun.spawnSync(['bun', new URL('../tools/decision_program_plane.mjs', import.meta.url).pathname,
      '--execute', '--frontier', frontierPath, '--out', join(dir, 'unbounded.jsonl')]);
    expect(unbounded.exitCode).toBe(2);
    expect(unbounded.stderr.toString()).toContain('--execute requires --limit 1..21');
    const noCalls = Bun.spawnSync(['bun', new URL('../tools/decision_program_plane.mjs', import.meta.url).pathname,
      '--execute', '--limit', '1', '--frontier', frontierPath, '--out', join(dir, 'no-calls.jsonl')]);
    expect(noCalls.exitCode).toBe(2);
    expect(noCalls.stderr.toString()).toContain('explicit maximum call budget');
  } finally { rmSync(dir, { recursive: true, force: true }); }
});

test('typed Jev answers cannot bypass exact review or absent material', () => {
  const source = { retained_items_state: 'unknown' }, sourceWithMaterial = { retained_items_state: 'observed' };
  const request = requestFor([source]);
  expect(request.model).toBe('clef-flash');
  expect(typeof request.questions.s0_program.instructions).toBe('string');
  expect(request.questions.s0_program.instructions).toContain('state.sources[0]');
  const probabilities = { read: 0, listen: 1, watch: 0, events: 0, images: 0, review: 0 };
  const payload = { answers: { s0_program: { type: 'choice', choice: 'listen', confidence: 0.95, probabilities },
    s0_attention: { type: 'score', score: 0.8, confidence: 0.9, probabilities: { 0: 0.2, 1: 0.8, 2: 0 } },
    s0_needs_exact_review: { type: 'noul', noul: 0.1 } } };
  expect(validateAnswers(payload, [source])[0]).toMatchObject({ diagnostic_lane: 'review', action: 'none' });
  expect(validateAnswers(payload, [sourceWithMaterial])[0]).toMatchObject({ diagnostic_lane: 'listen', action: 'none' });
  expect(() => validateAnswers({ answers: { ...payload.answers,
    s0_program: { ...payload.answers.s0_program, probabilities: { ...probabilities, listen: 0.5 } } } }, [source]))
    .toThrow('probabilities do not sum to one');
  expect(() => validateAnswers({ answers: { ...payload.answers,
    s0_attention: { type: 'score', score: 9, confidence: 0.9 } } }, [source]))
    .toThrow('probability distribution');
});

test('credentialed diagnostic response remains receipt-bound and non-effecting', async () => {
  const source = { retained_items_state: 'observed', channel_id: 'c1' };
  const probabilities = { read: 0, listen: 1, watch: 0, events: 0, images: 0, review: 0 };
  const response = { model: 'fixture', answers: {
    s0_program: { type: 'choice', choice: 'listen', confidence: 0.96, probabilities },
    s0_attention: { type: 'score', score: 0.8, confidence: 0.9, probabilities: { 0: 0.2, 1: 0.8, 2: 0 } },
    s0_needs_exact_review: { type: 'noul', noul: 0.1 },
  } };
  const calls = [];
  const result = await compileDecisionProgramPlane([source], { provider: 'jev', execute: true, maxCalls: 1,
    informationPolicyClass: 'public', apiKey: 'fixture-key',
    out: '/not-written', fetcher: async (url, options) => {
      calls.push({ url, authorization: options.headers.Authorization, body: JSON.parse(options.body) });
      return { ok: true, json: async () => response };
    } });
  expect(calls).toHaveLength(1);
  expect(calls[0].body.questions.s0_program.type).toBe('choice');
  expect(result.summary).toMatchObject({ mode: 'execute', planned: 0, diagnostic_assessments: 1 });
  expect(result.rows[0]).toMatchObject({ state: 'diagnostic', diagnostic_lane: 'listen', action: 'none' });
  expect(result.rows[0].request_digest).toMatch(/^[a-f0-9]{64}$/);
  expect(result.rows[0].response_digest).toMatch(/^[a-f0-9]{64}$/);
  await expect(compileDecisionProgramPlane([source], { provider: 'jev', execute: true, maxCalls: 1,
    informationPolicyClass: 'public', out: '/not-written' }))
    .rejects.toThrow('requires TYPESAFE_API_KEY');
});

test('Clef providers lower the same questions to bounded account-scoped REST diagnostics', async () => {
  const accountId = '0123456789abcdef0123456789abcdef';
  const sources = Array.from({ length: 22 }, (_, index) => ({
    channel_id: `c${index}`, retained_items_state: 'observed' }));
  const probabilities = { read: 0, listen: 1, watch: 0, events: 0, images: 0, review: 0 };
  for (const provider of ['clef', 'clef-flash']) {
    const calls = [];
    const result = await compileDecisionProgramPlane(sources, { provider, execute: true, maxCalls: sources.length,
      informationPolicyClass: 'public',
      accountId, apiKey: 'fixture-cloudflare-token', fetcher: async (url, options) => {
        const body = JSON.parse(options.body);
        calls.push({ url, options, body });
        const answers = {};
        for (let index = 0; index < body.state.sources.length; index++) {
          answers[`s${index}_program`] = { type: 'choice', choice: 'listen', confidence: 0.96, probabilities };
          answers[`s${index}_attention`] = { type: 'score', score: 0.8, confidence: 0.9,
            probabilities: { 0: 0.2, 1: 0.8, 2: 0 } };
          answers[`s${index}_needs_exact_review`] = { type: 'noul', noul: 0.1 };
        }
        return { ok: true, json: async () => ({ success: true, result: { model: provider, answers },
          errors: [], messages: [] }) };
      } });
    expect(calls).toHaveLength(22);
    expect(calls.every(call => Object.keys(call.body.questions).length === 3 && call.body.state.sources.length === 1)).toBe(true);
    expect(calls[0].url).toBe(`https://api.cloudflare.com/client/v4/accounts/${accountId}/ai/run/@cf/cloudflare/${provider}`);
    expect(calls[0].options.headers.Authorization).toBe('Bearer fixture-cloudflare-token');
    expect(calls[0].body.model).toBe(provider);
    expect(result.summary).toMatchObject({ provider, mode: 'execute', batches: 22,
      diagnostic_assessments: 22 });
    expect(result.rows).toHaveLength(22);
    expect(result.rows[21]).toMatchObject({ provider, model: provider, state: 'diagnostic',
      diagnostic_lane: 'listen', action: 'none' });
    expect(JSON.stringify(result)).not.toContain('fixture-cloudflare-token');
  }
});

test('Clef fails closed on question budget, account, credentials, endpoint override, and REST envelope', async () => {
  const source = { retained_items_state: 'observed' };
  const sources = Array.from({ length: 22 }, () => source);
  await expect(compileDecisionProgramPlane(sources, { provider: 'clef', batchSize: 22 }))
    .rejects.toThrow('--batch-size must be 1..1');
  expect(() => requestFor(sources, undefined, { model: 'clef', max_questions: 64 }))
    .toThrow('one semantic source');
  await expect(compileDecisionProgramPlane([source], { provider: 'clef', execute: true, maxCalls: 1, informationPolicyClass: 'public',
    accountId: 'wrong', apiKey: 'fixture' })).rejects.toThrow('CLOUDFLARE_ACCOUNT_ID');
  await expect(compileDecisionProgramPlane([source], { provider: 'clef', execute: true, maxCalls: 1, informationPolicyClass: 'public',
    accountId: '0123456789abcdef0123456789abcdef' })).rejects.toThrow('CLOUDFLARE_API_TOKEN');
  await expect(compileDecisionProgramPlane([source], { provider: 'clef', execute: true, maxCalls: 1, informationPolicyClass: 'public',
    accountId: '0123456789abcdef0123456789abcdef', apiKey: 'fixture', endpoint: 'https://example.org' }))
    .rejects.toThrow('--endpoint is only supported for the Jev provider');
  await expect(compileDecisionProgramPlane([source], { provider: 'clef', execute: true, maxCalls: 1, informationPolicyClass: 'public',
    accountId: '0123456789abcdef0123456789abcdef', apiKey: 'fixture',
    fetcher: async () => ({ ok: true, json: async () => ({ success: false, result: { answers: {} } }) }) }))
    .rejects.toThrow('Cloudflare response has no successful result');
  await expect(compileDecisionProgramPlane([source], { provider: 'unknown' }))
    .rejects.toThrow('unknown decision provider');
});

test('two receipt-bound model assessments become review work, never a model vote', () => {
  const source = { channel_id: 'c1', address: 'https://example.org/feed' };
  const base = { state: 'diagnostic', action: 'none', source, latency_ms: 20,
    request_digest: 'a'.repeat(64), response_digest: 'b'.repeat(64),
    needs_exact_review_probability: 0.1,
    program_probabilities: { read: 0.6, listen: 0.4 } };
  const flash = { ...base, provider: 'clef-flash', model: 'clef-flash',
    program_choice: 'read', diagnostic_lane: 'review', program_confidence: 0.6 };
  const clef = { ...base, provider: 'clef', model: 'clef', response_digest: 'c'.repeat(64),
    program_choice: 'listen', diagnostic_lane: 'listen', program_confidence: 0.9,
    program_probabilities: { read: 0.1, listen: 0.9 } };
  const compared = compareDecisionRuns([flash], [clef]);
  expect(compared.summary).toMatchObject({ compared: 1, disagreements: 1, review_required: 1 });
  expect(compared.rows[0]).toMatchObject({ channel_id: 'c1', disagreement: true,
    review_required: true, action: 'none' });
  expect(compared.rows[0].choices).toHaveLength(2);
  expect(() => compareDecisionRuns([flash], [])).toThrow('same exact channel set');
  expect(() => compareDecisionRuns([flash, flash], [clef])).toThrow('unique receipt-bound');
  expect(() => compareDecisionRuns([{ ...flash, action: 'publish' }], [clef])).toThrow('unique receipt-bound');
  expect(() => compareDecisionRuns([flash], [{ ...clef, source: { ...source, address: 'https://other.org' } }]))
    .toThrow('same exact source descriptor');
});

test('default and explicit local planning share the installed passport without inference or egress', async () => {
  const source = { channel_id: 'private-source', retained_items_state: 'unknown', information_policy_class: 'private' };
  const fetcher = () => { throw new Error('unexpected hosted call'); };
  const planned = await compileDecisionProgramPlane([source], { fetcher });
  const alias = await compileDecisionProgramPlane([source], { provider: 'local', fetcher });
  expect(alias).toEqual(planned);
  expect(planned.rows[0]).toMatchObject({ provider: 'local', model: installedDecisionOwner.model,
    model_artifact_digest: installedDecisionOwner.artifact_digest, model_identity_state: 'artifact_bound',
    question_semantics: 'joint_schema', confidence_semantics: 'typesafe-uniform-adjusted',
    information_policy_class: 'private', state: 'planned', action: 'none' });
  expect(planned.summary).toMatchObject({ provider_calls: 0, local_calls: 0 });
  await expect(compileDecisionProgramPlane([source], { execute: true })).rejects.toThrow('maximum call budget');
  await expect(compileDecisionProgramPlane([source, source], { execute: true, maxCalls: 1 }))
    .rejects.toThrow('maximum call budget');
  const cancelled = new AbortController(); cancelled.abort(new Error('offline pre-start cancellation'));
  await expect(compileDecisionProgramPlane([source], { execute: true, maxCalls: 1, signal: cancelled.signal }))
    .rejects.toThrow('offline pre-start cancellation');
  await expect(compileDecisionProgramPlane([source], { endpoint: 'https://example.org' }))
    .rejects.toThrow('local placement does not accept');
});

test('injected local execution shares the numeric owner cache and remains source-bound and non-effecting', async () => {
  let calls = 0;
  const local = { model: 'synthetic-estate-not-clef', artifact_digest: 'a'.repeat(64),
    question_semantics: 'joint_schema', confidence_semantics: 'maximum_option_probability', deterministic: true,
    evaluate: ({ schema }) => { calls++; return new Float32Array(schema.width); } };
  const source = { channel_id: 'one', title: 'private title', retained_items_state: 'unknown',
    information_policy_class: 'private' };
  const options = { execute: true, maxCalls: 2, localEvaluator: local,
    fetcher: () => { throw new Error('unexpected egress'); } };
  const result = await compileDecisionProgramPlane([source, source], options);
  expect(result.summary).toMatchObject({ local_calls: 1, reused_local_calls: 1, provider_calls: 0 });
  expect(calls).toBe(1);
  expect(result.rows[0]).toMatchObject({ provider: 'local', model: local.model,
    model_artifact_digest: local.artifact_digest, source, diagnostic_lane: 'review', action: 'none' });
  expect(result.rows[0].source_digest).toMatch(/^[a-f0-9]{64}$/);
  expect(result.rows[0].program_digest).toMatch(/^[a-f0-9]{64}$/);
  const second = await compileDecisionProgramPlane([source], options);
  expect(second.summary).toMatchObject({ local_calls: 0, reused_local_calls: 1 });
  await compileDecisionProgramPlane([{ ...source, channel_id: 'different-exact-source' }], options);
  expect(calls).toBe(2);
  local.artifact_digest = 'b'.repeat(64);
  await compileDecisionProgramPlane([source], options);
  expect(calls).toBe(3);
  await expect(compileDecisionProgramPlane([source], { ...options, provider: 'jev' }))
    .rejects.toThrow('injected local evaluator requires local placement');
});

test('local source snapshots survive an awaited owner and malformed score legends never cache', async () => {
  const source = { channel_id: 'before', retained_items_state: 'observed' };
  let release, calls = 0, conflict = true;
  const local = { model: 'synthetic-estate-not-clef', artifact_digest: 'c'.repeat(64),
    question_semantics: 'joint_schema', deterministic: true,
    evaluate: ({ schema, questions }) => {
      calls++;
      const answers = answersFromLogits(schema, new Float32Array(schema.width));
      answers.s0_attention.legend = Object.fromEntries(questions.s0_attention.criteria.map((criterion, index) =>
        [String(index), conflict && index === 0 ? 'WRONG RUBRIC' : criterion]));
      return { answers };
    } };
  const options = { execute: true, maxCalls: 1, localEvaluator: local };
  await expect(compileDecisionProgramPlane([source], options)).rejects.toThrow('legend disagrees');
  conflict = false;
  const corrected = await compileDecisionProgramPlane([source], options);
  expect(corrected.summary.local_calls).toBe(1);
  expect(calls).toBe(2);
  local.evaluate = ({ schema }) => new Promise(resolve => { release = () => resolve(new Float32Array(schema.width)); });
  const pending = compileDecisionProgramPlane([source], options);
  source.channel_id = 'after';
  options.execute = false;
  release();
  expect((await pending).rows[0]).toMatchObject({ source: { channel_id: 'before' }, state: 'diagnostic' });
});

test('hosted selection requires public attestation and all retry attempts share the call budget', async () => {
  let calls = 0;
  const source = { channel_id: 'one', retained_items_state: 'observed' };
  const options = { provider: 'jev', execute: true, maxCalls: 1, apiKey: 'fixture',
    fetcher: async () => { calls++; return { ok: false, status: 429 }; } };
  await expect(compileDecisionProgramPlane([source], options)).rejects.toThrow('explicitly public');
  await expect(compileDecisionProgramPlane([{ ...source, information_policy_class: 'private' }],
    { ...options, informationPolicyClass: 'public' })).rejects.toThrow('explicitly public');
  expect(calls).toBe(0);
  await expect(compileDecisionProgramPlane([source], { ...options, informationPolicyClass: 'public' }))
    .rejects.toThrow('maximum call budget exhausted');
  expect(calls).toBe(1);
  // Hosted responses use the same authored legend validation as local answers.
  const request = requestFor([source]);
  const schema = (await import('../../../components/BonfyreDecision/typed-decision.mjs')).compileQuestionSchema(request.questions);
  const answers = answersFromLogits(schema, new Float32Array(schema.width));
  answers.s0_attention.legend = { 0: 'wrong', 1: 'wrong', 2: 'wrong' };
  expect(() => validateAnswers({ answers }, [source])).toThrow('legend disagrees');
  expect(decisionPolicy().default_provider).toBe('local');
});
