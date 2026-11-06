#!/usr/bin/env bun
// Typed judgments over exact, finite frontiers. Never an authority or effect owner.
import { createHash } from 'node:crypto';
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { compileQuestionSchema, compileDecisionProjection,
  decodeDecisionAnswers, executeDecisionProjection, compileDecisionSelection,
  selectDecisionCandidate, runDecisionRecords, scoreDecisionLabels,
  binomialRiskUpper } from './typed-decision.mjs';
import { evaluateLocalDecision } from './local-inference.mjs';

const text = bytes => new TextDecoder().decode(bytes);
const sorted = value => JSON.stringify(value, (_, item) => item && typeof item === 'object' && !Array.isArray(item)
  ? Object.fromEntries(Object.entries(item).toSorted(([a], [b]) => a < b ? -1 : a > b ? 1 : 0)) : item);
export const decisionDigest = value => createHash('sha256').update(sorted(value)).digest('hex');
let cachedPolicy;
const programPlans = new Map();
function programPlan(questions, program) {
  const key = decisionDigest({ questions, outputs: program.outputs, selection: program.selection });
  if (programPlans.has(key)) return programPlans.get(key);
  const schema = compileQuestionSchema(questions);
  const plan = { schema, projection: compileDecisionProjection(schema, program.outputs),
    selection: program.selection ? compileDecisionSelection(schema, program.selection) : null };
  // Dynamic finite frontiers are common; retain a bounded working set only.
  if (programPlans.size >= 128) programPlans.delete(programPlans.keys().next().value);
  programPlans.set(key, plan);
  return plan;
}

export function decisionPlanePolicy() {
  if (cachedPolicy) return cachedPolicy;
  // The native products (cmd/BonfyreDecide) compile this CUE export in; a
  // checkout reads and exports the CUE itself.
  const path = fileURLToPath(new URL('./decision-plane.cue', import.meta.url));
  const result = Bun.spawnSync(['cue', 'export', path, '-e', 'decisionPlane']);
  if (result.exitCode !== 0) throw new Error(`DecisionPlane CUE: ${text(result.stderr).trim()}`);
  return cachedPolicy = JSON.parse(text(result.stdout));
}

function exactFrontier(frontier, policy) {
  if (!Array.isArray(frontier) || !frontier.length || frontier.length > policy.max_candidates)
    throw new Error(`exact frontier must have 1..${policy.max_candidates} candidates`);
  const identities = new Set();
  return frontier.map(row => {
    if (!row || typeof row.id !== 'string' || !row.id || typeof row.description !== 'string' ||
        !row.description || typeof row.owner_ref !== 'string' || !row.owner_ref || identities.has(row.id))
      throw new Error('exact frontier has missing or duplicate candidate identity, owner, or description');
    identities.add(row.id);
    return { id: row.id, description: row.description, owner_ref: row.owner_ref };
  });
}

// A question may name the case's own subject: {{watch.name}} is replaced from
// the declared state before the case is digested, so the receipt binds the
// exact wording the model was asked. A small model separates a business from
// a namesake far better when the question names both (measured 22 of 23 on
// archived "Target" mentions, against 4 of 23 for a generic question).
function fillQuestionText(questions, state) {
  const fill = text => typeof text !== 'string' ? text : text.replace(/\{\{([a-z_]+(?:\.[a-z_]+)*)\}\}/g, (_, path) => {
    const value = path.split('.').reduce((at, key) => at?.[key], state);
    if (typeof value !== 'string' || !value.trim()) throw new Error(`decision question names {{${path}}}, which the state does not carry`);
    return value.trim();
  });
  for (const question of Object.values(questions)) {
    question.instructions = fill(question.instructions);
    if (Array.isArray(question.criteria)) question.criteria = question.criteria.map(fill);
    else if (question.criteria) question.criteria = Object.fromEntries(Object.entries(question.criteria).map(([key, text]) => [key, fill(text)]));
  }
  return questions;
}

export function compileDecisionCase(input, policy = decisionPlanePolicy()) {
  const program = Object.hasOwn(policy.programs, input.program) && policy.programs[input.program];
  if (!program) throw new Error(`unknown decision program: ${input.program}`);
  if (input.owner !== program.owner || !/^[a-f0-9]{64}$/.test(input.mask_digest || '') ||
      !['ADMIT', 'SHADOW'].includes(input.gate_state) ||
      typeof input.subject_id !== 'string' || !input.subject_id ||
      typeof input.information_policy_class !== 'string' || !input.information_policy_class)
    throw new Error('decision case requires exact owner, subject, mask digest, gate state, and information policy');
  const frontier = exactFrontier(input.frontier, policy);
  if (!input.state || typeof input.state !== 'object' || Array.isArray(input.state) ||
      Object.keys(input.state).some(key => !program.state_fields.includes(key)))
    throw new Error('decision state contains undeclared fields');
  const state = {};
  for (const field of program.state_fields) {
    if (field === 'candidates' || field === 'candidate_roles') {
      state[field] = frontier.map(({ id, description }) => ({ id, description }));
    } else if (!Object.hasOwn(input.state, field)) throw new Error(`decision state missing ${field}`);
    else state[field] = input.state[field];
  }
  if (input.program === 'context_passage' &&
      (frontier.length !== 1 || state.passage?.id !== frontier[0].id))
    throw new Error('context passage must name its single exact frontier member');
  if (Buffer.byteLength(sorted(state), 'utf8') > policy.max_state_bytes)
    throw new Error('decision state exceeds authored byte budget; narrow the exact state first');
  const questions = fillQuestionText(structuredClone(program.questions), state);
  for (const question of Object.values(questions)) {
    if (question.type === 'choice' && !question.criteria)
      // 'none' is the authored abstention option, not an admissible owner ID.
      if (frontier.some(row => row.id === 'none')) throw new Error('reserved decision candidate: none');
    if (question.type === 'choice' && !question.criteria)
      question.criteria = Object.fromEntries([...frontier.map(row => [row.id, row.description]),
        ['none', 'None of the listed candidates clearly fits.']]);
  }
  programPlan(questions, program);
  const contract = { schema: policy.schema, program: input.program, owner: input.owner,
    program_digest: decisionDigest(program),
    subject_id: input.subject_id, mask_digest: input.mask_digest, gate_state: input.gate_state,
    information_policy_class: input.information_policy_class, frontier };
  const case_digest = decisionDigest({ contract, state, questions });
  return { contract, state, questions, case_digest, cannot_infer: policy.forbidden_inference };
}

export function decisionRequest(compiled, providerName, policy = decisionPlanePolicy()) {
  const provider = Object.hasOwn(policy.providers, providerName) && policy.providers[providerName];
  if (!provider) throw new Error(`unknown decision provider: ${providerName}`);
  if (Object.keys(compiled.questions).length > provider.max_questions)
    throw new Error('decision program exceeds provider question budget');
  const program = policy.programs[compiled.contract.program];
  // prompt_fields: the part of the state the model reads. A field a question
  // already names through {{...}} is not repeated, because a small model
  // reads a repeated description as evidence for it.
  const state = compiled.contract.program === 'context_passage'
    ? { purpose: compiled.state.purpose, passage: { text: compiled.state.passage.text } }
    : Array.isArray(program?.prompt_fields)
      ? Object.fromEntries(program.prompt_fields.map(field => [field, compiled.state[field]]))
      : compiled.state;
  return { model: provider.model, state, questions: compiled.questions };
}

export function decisionReceiptRequest(compiled, receipt, policy = decisionPlanePolicy()) {
  if (receipt.provider !== 'local') return decisionRequest(compiled, receipt.provider, policy);
  if (!/^[a-f0-9]{64}$/.test(receipt.model_artifact_digest || '') || !receipt.model)
    throw new Error('local decision receipt requires a pinned model artifact');
  return { ...decisionRequest(compiled, receipt.request_provider, policy), model: receipt.model };
}

export const decisionModelPassport = receipt => receipt.provider === 'local'
  ? `local:${receipt.model}:${receipt.model_artifact_digest}:${receipt.question_semantics}`
  : `${receipt.provider}:${receipt.model}`;

export function validateDecisionResponse(response, request, policy = decisionPlanePolicy()) {
  if (!response || typeof response.model !== 'string' || !response.model)
    throw new Error('decision response shape does not match the request');
  decodeDecisionAnswers(compileQuestionSchema(request.questions), response.answers, policy);
  const answers = Object.create(null);
  for (const [id, question] of Object.entries(request.questions)) {
    const answer = response.answers[id];
    if (question.type === 'noul') {
      answers[id] = { type: 'noul', noul: answer.noul };
    } else if (question.type === 'choice') {
      const keys = Object.keys(question.criteria);
      answers[id] = { type: 'choice', choice: answer.choice, confidence: answer.confidence,
        probabilities: Object.fromEntries(keys.map(key => [key, answer.probabilities[key]])) };
    } else if (question.type === 'score') {
      // Jev's hosted schema requires a legend; the shared native/internal
      // answer shape currently omits it. Until those paths are separated,
      // missing legend is compatibility-only, not hosted wire conformance.
      // A supplied legend must never conflict with the authored rubric.
      if (answer.legend !== undefined && decisionDigest(answer.legend) !==
          decisionDigest(Object.fromEntries(question.criteria.map((criterion, index) => [String(index), criterion]))))
        throw new Error(`${id}: score legend disagrees with the authored criteria`);
      const keys = question.criteria.map((_, index) => String(index));
      answers[id] = { type: 'score', score: answer.score, confidence: answer.confidence,
        probabilities: Object.fromEntries(keys.map(key => [key, answer.probabilities[key]])) };
    } else throw new Error(`undeclared decision question type: ${question.type}`);
  }
  return answers;
}

function endpoint(provider, accountId) {
  if (provider.response_envelope === 'direct') return provider.endpoint;
  if (!/^[a-f0-9]{32}$/i.test(accountId || '')) throw new Error(`decision execution requires ${provider.account_env}`);
  return provider.endpoint.replace('{account_id}', accountId);
}

// Absence of a hosted provider selects the shared, pinned native owner. Import
// lazily to keep planning free of model residency and avoid the wire validator
// dependency cycle. Explicit injected owners remain useful for embedded hosts.
async function executionOptions(options) {
  if (options.localEvaluator || (options.provider && options.provider !== 'local')) return options;
  const { installedDecisionOwner } = await import('./installed-runtime.mjs');
  return { ...options, provider: undefined, localEvaluator: installedDecisionOwner };
}

export async function assessDecision(compiled, options = {}, policy = decisionPlanePolicy()) {
  options = { ...options };
  const rebuilt = compileDecisionCase({ ...compiled.contract, state: compiled.state }, policy);
  if (rebuilt.case_digest !== compiled.case_digest || decisionDigest(rebuilt.questions) !== decisionDigest(compiled.questions))
    throw new Error('decision case changed after compilation');
  compiled = rebuilt; // Own the contract; caller mutation during inference cannot rewrite the receipt.
  compiled.state = JSON.parse(sorted(compiled.state));
  if (!options.localEvaluator && (!options.provider || options.provider === 'local'))
    options = await executionOptions(options);
  const providerName = options.provider && options.provider !== 'local'
    ? options.provider : policy.default_request_provider;
  const provider = policy.providers[providerName];
  const request = decisionRequest(compiled, providerName, policy);
  const timeout = options.timeoutMs ?? (options.localEvaluator ? 240000 : 45000);
  if (!Number.isSafeInteger(timeout) || timeout < 1) throw new Error('decision timeout must be a positive integer');
  options.signal?.throwIfAborted();
  const request_digest = decisionDigest(request);
  const base = { schema: policy.schema, contract: compiled.contract, case_digest: compiled.case_digest,
    provider: providerName, request_provider: providerName, requested_model: provider.model, request_digest,
    cannot_infer: policy.forbidden_inference, action: 'none' };
  // A trusted embedded inference owner can supply schema logits directly. It
  // does not impersonate a hosted model or grant a private-state egress permit.
  if (options.localEvaluator) {
    const local = options.localEvaluator;
    const confidence = local.confidence_semantics ?? 'maximum_option_probability';
    if (typeof local.evaluate !== 'function' || typeof local.model !== 'string' || !local.model ||
        !/^[a-f0-9]{64}$/.test(local.artifact_digest || '') ||
        !['joint_schema', 'independent_evaluation'].includes(local.question_semantics) ||
        !['maximum_option_probability', 'vendor_distribution_statistic', 'typesafe-uniform-adjusted'].includes(confidence))
      throw new Error('local decision owner requires a pinned model artifact and question semantics');
    const localBase = { ...base, provider: 'local', requested_model: local.model, model: local.model,
      model_identity_state: 'artifact_bound',
      model_artifact_digest: local.artifact_digest, question_semantics: local.question_semantics,
      confidence_semantics: confidence, request_digest: decisionDigest({ ...request, model: local.model }) };
    if (!options.execute) return { ...localBase, state: 'planned' };
    const localRequest = JSON.parse(sorted(request));
    const schema = compileQuestionSchema(localRequest.questions), started = performance.now();
    const signal = options.signal ? AbortSignal.any([options.signal, AbortSignal.timeout(timeout)]) : AbortSignal.timeout(timeout);
    const { answers, inference } = await evaluateLocalDecision(local,
      { state: localRequest.state, questions: localRequest.questions, schema }, {
        signal, ...policy.local_inference_residency,
        key: decisionDigest({ request: localBase.request_digest, model: localBase.model,
          artifact: localBase.model_artifact_digest, semantics: localBase.question_semantics, confidence,
          owner: compiled.contract.owner, mask: compiled.contract.mask_digest,
          gate: compiled.contract.gate_state, information_policy: compiled.contract.information_policy_class,
          program: compiled.contract.program_digest }),
      });
    signal.throwIfAborted();
    const receipt = { ...localBase,
      state: 'diagnostic', response_digest: decisionDigest({ model: localBase.model, artifact: localBase.model_artifact_digest, answers }),
      latency_ms: Math.round(performance.now() - started), usage: null, answers,
      answers_digest: decisionDigest(answers), inference };
    return { ...receipt, proposal: decisionProposal(receipt, policy) };
  }
  if (!options.execute) return { ...base, state: 'planned' };
  if (!policy.remote_information_policy_classes.includes(compiled.contract.information_policy_class))
    throw new Error('information policy forbids a hosted decision model for this state');
  const key = options.apiKey || process.env[provider.credential_env];
  if (!key) throw new Error(`decision execution requires ${provider.credential_env}`);
  const url = endpoint(provider, options.accountId || process.env[provider.account_env]);
  const started = performance.now();
  const response = await (options.fetcher || fetch)(url, { method: 'POST',
    headers: { Authorization: `Bearer ${key}`, 'Content-Type': 'application/json', Accept: 'application/json' },
    body: sorted(request), signal: options.signal
      ? AbortSignal.any([options.signal, AbortSignal.timeout(timeout)]) : AbortSignal.timeout(timeout) });
  if (!response.ok) throw new Error(`decision provider HTTP ${response.status}`);
  const envelope = await response.json();
  const payload = provider.response_envelope === 'direct' ? envelope : envelope?.success === true ? envelope.result : null;
  if (!payload) throw new Error('decision provider returned an unsuccessful envelope');
  const answers = validateDecisionResponse(payload, request, policy);
  const receipt = { ...base, state: 'diagnostic', model: payload.model, response_digest: decisionDigest(envelope),
    model_identity_state: payload.model === provider.model ? 'mutable_alias' : 'provider_reported_revision',
    question_semantics: provider.question_semantics, confidence_semantics: provider.confidence_semantics,
    latency_ms: Math.round(performance.now() - started), usage: payload.usage || null, answers,
    answers_digest: decisionDigest(answers) };
  return { ...receipt, proposal: decisionProposal(receipt, policy) };
}

// One semantic state per call; concurrent calls are bounded and exact duplicates share a result.
// Cross-run provider aliases are deliberately not cached because a stable model revision is unknown.
export async function assessDecisionCases(cases, options = {}, policy = decisionPlanePolicy()) {
  if (!Array.isArray(cases)) throw new Error('decision cases must be an array');
  options = { ...options };
  const limit = options.concurrency === undefined ? policy.max_concurrency : Number(options.concurrency);
  if (!Number.isInteger(limit) || limit < 1 || limit > policy.max_concurrency)
    throw new Error(`decision concurrency must be 1..${policy.max_concurrency}`);
  const compiled = cases.map(item => compileDecisionCase(item.contract
    ? { ...item.contract, state: item.state } : item, policy));
  for (const item of compiled) item.state = JSON.parse(sorted(item.state));
  if (!options.localEvaluator && (!options.provider || options.provider === 'local'))
    options = await executionOptions(options);
  const unique = new Map(compiled.map(item => [item.case_digest, item]));
  const providerName = options.provider && options.provider !== 'local'
    ? options.provider : policy.default_request_provider;
  const groups = new Map();
  for (const item of unique.values()) {
    const key = decisionDigest({ request: decisionRequest(item, providerName, policy),
      information_policy_class: item.contract.information_policy_class,
      gate_state: item.contract.gate_state, owner: item.contract.owner,
      mask_digest: item.contract.mask_digest, program_digest: item.contract.program_digest });
    const group = groups.get(key) || [];
    group.push(item); groups.set(key, group);
  }
  if (options.execute && (!Number.isSafeInteger(options.maxCalls) || options.maxCalls < 1 ||
      groups.size > options.maxCalls))
    throw new Error('decision execution requires an explicit maximum provider-call budget');
  if (options.execute && !options.localEvaluator && [...unique.values()].some(item =>
    !policy.remote_information_policy_classes.includes(item.contract.information_policy_class)))
    throw new Error('information policy forbids a hosted decision model for this state');
  const representatives = [...groups.values()].map(group => group[0]);
  const requestBytes = representatives.reduce((sum, item) => sum +
    Buffer.byteLength(sorted(decisionRequest(item, providerName, policy)), 'utf8'), 0);
  const results = new Map();
  let localCalls = 0, reusedLocalCalls = 0;
  await runDecisionRecords([...groups.values()], async (group, _index, signal) => {
      const receipt = await assessDecision(group[0], { ...options, signal }, policy);
      if (receipt.inference?.executed) localCalls++;
      if (receipt.inference?.state === 'reused') reusedLocalCalls++;
      for (const item of group) results.set(item.case_digest,
        { ...receipt, contract: item.contract, case_digest: item.case_digest,
          ...(receipt.state === 'diagnostic' ? { proposal: decisionProposal({ ...receipt,
            contract: item.contract, case_digest: item.case_digest }, policy) } : {}) });
  }, { concurrency: limit, signal: options.signal });
  return { receipts: compiled.map(item => results.get(item.case_digest)),
    summary: { cases: compiled.length, unique_cases: unique.size,
      duplicate_calls_avoided: compiled.length - groups.size,
      semantic_alias_calls_avoided: unique.size - groups.size,
      planned_provider_calls: options.localEvaluator ? 0 : groups.size,
      planned_local_calls: options.localEvaluator ? groups.size : 0,
      total_request_bytes: requestBytes,
      provider_calls: options.execute && !options.localEvaluator ? groups.size : 0,
      local_calls: localCalls, reused_local_calls: reusedLocalCalls,
      state: options.execute ? 'diagnostic' : 'planned' } };
}

export function decisionProposal(receipt, policy = decisionPlanePolicy()) {
  if (receipt?.state !== 'diagnostic' || receipt.action !== 'none') throw new Error('only diagnostic receipts produce proposals');
  if (receipt.answers_digest !== decisionDigest(receipt.answers)) throw new Error('decision answers changed since assessment');
  const program = Object.hasOwn(policy.programs, receipt.contract.program) && policy.programs[receipt.contract.program];
  if (!program || receipt.contract.program_digest !== decisionDigest(program))
    throw new Error('decision program changed since assessment');
  const questions = structuredClone(program.questions);
  for (const question of Object.values(questions)) {
    if (question.type === 'choice' && !question.criteria)
      question.criteria = Object.fromEntries([...receipt.contract.frontier.map(row => [row.id, row.description]),
        ['none', 'None of the listed candidates clearly fits.']]);
  }
  const plan = programPlan(questions, program), decoded = decodeDecisionAnswers(plan.schema, receipt.answers, policy);
  const selection = plan.selection ? selectDecisionCandidate(decoded, plan.selection) : null;
  return { subject_id: receipt.contract.subject_id,
    ...(program.candidate_output ? { candidate_id: receipt.contract.frontier[0].id } : {}),
    ...executeDecisionProjection(plan.projection, decoded),
    ...(selection ? { suggested_candidate: selection.candidate, selection } : {}),
    uncertainty: decoded.moments, action: 'none' };
}

// Validate before any downstream learning/evaluation/cut consumes the answer.
// Digests detect corruption, not a forged attestation from an untrusted caller.
export function validateDecisionReceipt(compiled, receipt, policy = decisionPlanePolicy()) {
  const rebuilt = compiled?.contract && compiled?.state
    ? compileDecisionCase({ ...compiled.contract, state: compiled.state }, policy) : null;
  if (!rebuilt || rebuilt.case_digest !== compiled.case_digest ||
      decisionDigest(rebuilt.questions) !== decisionDigest(compiled.questions) ||
      receipt?.state !== 'diagnostic' || receipt.action !== 'none' ||
      receipt.case_digest !== rebuilt.case_digest ||
      decisionDigest(receipt.contract) !== decisionDigest(rebuilt.contract) ||
      !/^[a-f0-9]{64}$/.test(receipt.response_digest || '') ||
      receipt.answers_digest !== decisionDigest(receipt.answers))
    throw new Error('decision receipt is stale, mutated, incomplete, or effecting');
  const request = decisionReceiptRequest(rebuilt, receipt, policy);
  if (receipt.request_digest !== decisionDigest(request)) throw new Error('decision receipt request mismatch');
  const identity = receipt.provider === 'local' ? 'artifact_bound'
    : receipt.model === request.model ? 'mutable_alias' : 'provider_reported_revision';
  if (receipt.model_identity_state !== identity ||
      (receipt.provider === 'local' && !['joint_schema', 'independent_evaluation'].includes(receipt.question_semantics)))
    throw new Error('decision receipt model identity mismatch');
  if (receipt.provider === 'local' && (receipt.requested_model !== receipt.model ||
      receipt.response_digest !== decisionDigest({ model: receipt.model,
        artifact: receipt.model_artifact_digest, answers: receipt.answers })))
    throw new Error('decision receipt local artifact mismatch');
  validateDecisionResponse({ model: receipt.model, answers: receipt.answers }, request, policy);
  return decodeDecisionAnswers(compileQuestionSchema(request.questions), receipt.answers, policy);
}

export function validateDecisionLabels(row, labels) {
  const source = row.label_receipt;
  if (!source || source.schema !== 'bonfyre.decision-label.v1' ||
      typeof row.evaluation_unit !== 'string' || !row.evaluation_unit ||
      source.evaluation_unit !== row.evaluation_unit || source.case_digest !== row.case?.case_digest ||
      typeof source.source_ref !== 'string' || !source.source_ref ||
      decisionDigest(source.labels) !== decisionDigest(labels) ||
      row.label_receipt_digest !== decisionDigest(source))
    throw new Error('decision labels require a content-bound external label receipt');
}

// All owners use the same outcome algebra. Exact labels come from an external
// evaluator, NEVER the model's own answers. Per-field scores remain separate:
// multiplying marginals would invent the joint record likelihood for Clef.
// Selection replay evaluates the authored relative loss table, not actual
// effect value, permission, or a new model-generated action.
export function evaluateDecisionOutcomes(rows, { targetRequestDigests = new Set() } = {}, policy = decisionPlanePolicy()) {
  if (!Array.isArray(rows) || !rows.length || rows.length > policy.evaluation.max_cases)
    throw new Error('decision evaluation requires bounded labeled rows');
  const requests = new Set(), units = new Set(), groups = new Map();
  for (const row of rows) {
    const decoded = validateDecisionReceipt(row.case, row.receipt, policy), receipt = row.receipt;
    validateDecisionLabels(row, row.labels);
    if (typeof row.evaluation_unit !== 'string' || !row.evaluation_unit || units.has(row.evaluation_unit) ||
        !/^[a-f0-9]{64}$/.test(row.label_receipt_digest || '') ||
        requests.has(receipt.request_digest) || targetRequestDigests.has(receipt.request_digest))
      throw new Error('decision evaluation requires distinct held-out units and label receipts');
    const scores = scoreDecisionLabels(decoded, row.labels);
    requests.add(receipt.request_digest); units.add(row.evaluation_unit);
    const identity = { model: decisionModelPassport(receipt), owner: receipt.contract.owner,
      program: receipt.contract.program_digest, questions: decisionDigest(row.case.questions),
      information_policy_class: receipt.contract.information_policy_class };
    const key = decisionDigest(identity);
    const program = policy.programs[receipt.contract.program];
    const selectionPlan = programPlan(row.case.questions, program).selection;
    const group = groups.get(key) || { identity, cases: 0, exact_records: 0, fields: Object.create(null),
      selection: selectionPlan ? { candidate_suggestions: 0, review_deferrals: 0,
        wrong_suggestions: 0, review_required_bypassed: 0,
        realized_loss_units: 0, always_review_loss_units: 0 } : null };
    group.cases++;
    if (Object.values(scores).every(score => score.correct)) group.exact_records++;
    if (selectionPlan) {
      const replay = selectDecisionCandidate(decoded, selectionPlan);
      const truth = row.labels[selectionPlan.question];
      const outcomeLoss = selectionPlan.losses[replay.candidate][truth];
      const baselineLoss = selectionPlan.losses[selectionPlan.abstain][truth];
      if (!Number.isFinite(outcomeLoss) || !Number.isFinite(baselineLoss))
        throw new Error('decision selection loss is not finite');
      group.selection.candidate_suggestions += Number(replay.disposition === 'candidate');
      group.selection.review_deferrals += Number(replay.disposition === 'review');
      group.selection.wrong_suggestions += Number(replay.disposition === 'candidate' && replay.candidate !== truth);
      group.selection.review_required_bypassed += Number(replay.disposition === 'candidate' &&
        row.labels[program.selection.review_question] === 'true');
      group.selection.realized_loss_units += outcomeLoss;
      group.selection.always_review_loss_units += baselineLoss;
      if (!Number.isFinite(group.selection.realized_loss_units) ||
          !Number.isFinite(group.selection.always_review_loss_units))
        throw new Error('decision selection aggregate loss overflow');
    }
    for (const [id, score] of Object.entries(scores)) {
      const field = group.fields[id] ||= { correct: 0, brier: 0, finite_log_loss_sum: 0,
        infinite_log_loss_cases: 0, ranked_probability_score: score.ranked_probability_score === null ? null : 0 };
      field.correct += Number(score.correct); field.brier += score.brier;
      field.infinite_log_loss_cases += Number(score.infinite_log_loss);
      field.finite_log_loss_sum += score.log_loss ?? 0;
      if (field.ranked_probability_score !== null) field.ranked_probability_score += score.ranked_probability_score;
    }
    groups.set(key, group);
  }
  const results = [...groups.values()].map(group => ({ ...group,
    exact_record_rate: group.exact_records / group.cases,
    selection: group.selection ? { ...group.selection,
      net_saved_loss_units: group.selection.always_review_loss_units - group.selection.realized_loss_units,
      mean_realized_loss_units: group.selection.realized_loss_units / group.cases,
      mean_always_review_loss_units: group.selection.always_review_loss_units / group.cases } : null,
    fields: Object.fromEntries(Object.entries(group.fields).map(([id, field]) => [id, {
      ...field, accuracy: field.correct / group.cases, brier: field.brier / group.cases,
      mean_log_loss: field.infinite_log_loss_cases ? null : field.finite_log_loss_sum / group.cases,
      ranked_probability_score: field.ranked_probability_score === null ? null : field.ranked_probability_score / group.cases,
    }])),
  }));
  return { schema: 'bonfyre.decision-outcomes.v1', cases: rows.length,
    evidence_digest: decisionDigest(rows.map(row => ({ case: row.case.case_digest,
      request: row.receipt.request_digest, answers: row.receipt.answers_digest,
      model: decisionModelPassport(row.receipt),
      label_receipt: row.label_receipt_digest, labels: row.labels, unit: row.evaluation_unit }))),
    groups: results, action: 'none',
    cannot_infer: 'proper scoring or authored relative loss on supplied labels -> measured operational value, trained model, calibrated production accuracy, independent sampling, authority, or effect permission' };
}

export function assessSelectiveValue(rows, { minimum_cases, maximum_error_rate, minimum_net_saved_ms,
  risk_alpha = decisionPlanePolicy().evaluation.risk_alpha, sampling_assumption } = {}) {
  if (![minimum_cases, maximum_error_rate, minimum_net_saved_ms].every(Number.isFinite) ||
      !Number.isSafeInteger(minimum_cases) || minimum_cases < 1 || maximum_error_rate < 0 || maximum_error_rate > 1 || minimum_net_saved_ms < 0 ||
      !Number.isFinite(risk_alpha) || risk_alpha <= 0 || risk_alpha >= 1 ||
      !Array.isArray(rows) || rows.length > decisionPlanePolicy().evaluation.max_cases)
    throw new Error('a measured promotion policy is required');
  let proposed = 0, errors = 0, baseline_ms = 0, total_ms = 0;
  for (const row of rows) {
    if (![row.baseline_ms, row.decision_ms, row.fallback_ms].every(value =>
      Number.isFinite(value) && value >= 0) || typeof row.correct !== 'boolean' || typeof row.accepted !== 'boolean')
      throw new Error('evaluation requires labeled outcomes and measured baseline, decision, and fallback costs');
    baseline_ms += row.baseline_ms;
    total_ms += row.decision_ms + (row.accepted ? 0 : row.fallback_ms);
    if (row.accepted) { proposed++; if (!row.correct) errors++; }
  }
  const error_rate = proposed ? errors / proposed : null, net_saved_ms = baseline_ms - total_ms;
  if (![baseline_ms, total_ms, net_saved_ms].every(Number.isFinite)) throw new Error('evaluation cost overflow');
  const error_rate_upper = binomialRiskUpper(errors, proposed, risk_alpha);
  return { cases: rows.length, accepted: proposed, errors, error_rate, error_rate_upper,
    risk_alpha, sampling_assumption: sampling_assumption ?? 'unspecified', baseline_ms,
    decision_plus_fallback_ms: total_ms, net_saved_ms,
    promotion_eligible: rows.length >= minimum_cases && proposed > 0 &&
      sampling_assumption === 'fixed_policy_independent_holdout' &&
      error_rate_upper <= maximum_error_rate && net_saved_ms >= minimum_net_saved_ms };
}

export function writeReceipts(path, receipts) {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, receipts.map(sorted).join('\n') + (receipts.length ? '\n' : ''));
}

if (import.meta.main) {
  try {
    const [mode, input, output, provider, maximum] = process.argv.slice(2);
    if (!['plan', 'run', 'plan-context', 'run-context'].includes(mode) || !input || !output)
      throw new Error('usage: decision-plane.mjs plan|run|plan-context|run-context <cases.jsonl|context-cut.json> <receipts.jsonl> [local|hosted-provider] [max-calls for run]');
    const contextMode = mode.endsWith('-context');
    const cases = contextMode
      ? (await import('./frontier-adapters.mjs')).contextCasesFromManifest(JSON.parse(readFileSync(input, 'utf8')))
      : readFileSync(input, 'utf8').split(/\r?\n/).filter(Boolean).map(line => JSON.parse(line));
    const result = await assessDecisionCases(cases, { provider, execute: mode.startsWith('run'),
      maxCalls: maximum === undefined ? undefined : Number(maximum) });
    writeReceipts(output, result.receipts);
    console.log(JSON.stringify({ ...result.summary, output }));
  } catch (error) { console.error(`decision-plane: ${error.message}`); process.exitCode = 2; }
  finally { await (await import('./installed-runtime.mjs')).closeInstalledDecisionEvaluator(); }
}
