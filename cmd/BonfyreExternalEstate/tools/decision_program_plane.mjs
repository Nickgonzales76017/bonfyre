#!/usr/bin/env bun
// Receipt-bound diagnostic questions over admitted descriptors, never effects.
import { Database } from 'bun:sqlite';
import { createHash } from 'node:crypto';
import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { decisionPlanePolicy, validateDecisionResponse } from '../../../components/BonfyreDecision/decision-plane.mjs';
import { compileQuestionSchema, decodeDecisionAnswers, runDecisionRecords } from '../../../components/BonfyreDecision/typed-decision.mjs';
import { evaluateLocalDecision } from '../../../components/BonfyreDecision/local-inference.mjs';
import { installedDecisionOwner, closeInstalledDecisionEvaluator } from '../../../components/BonfyreDecision/installed-runtime.mjs';

let cachedPolicy;
export function decisionPolicy() {
  if (!cachedPolicy) {
    const path = fileURLToPath(new URL('../decision-program-plane.cue', import.meta.url));
    const result = Bun.spawnSync(['cue', 'export', path, '-e', 'decisionProgramPlane']);
    if (result.exitCode !== 0) throw new Error(new TextDecoder().decode(result.stderr).trim());
    cachedPolicy = JSON.parse(new TextDecoder().decode(result.stdout));
    cachedPolicy.providers = decisionPlanePolicy().providers;
  }
  return cachedPolicy;
}

const canonical = value => JSON.stringify(value, (_, item) => item && !Array.isArray(item) && typeof item === 'object'
  ? Object.fromEntries(Object.entries(item).toSorted(([a], [b]) => a < b ? -1 : a > b ? 1 : 0)) : item);
const digest = value => createHash('sha256').update(canonical(value)).digest('hex');

function providerFor(name, policy) {
  const provider = policy.providers[name];
  if (!Object.hasOwn(policy.providers, name) || !provider) throw new Error(`unknown decision provider: ${name}`);
  return provider;
}

function endpointFor(provider, options) {
  if (provider.response_envelope === 'direct') return options.endpoint || provider.endpoint;
  if (options.endpoint) throw new Error('--endpoint is only supported for the Jev provider');
  if (!/^[0-9a-f]{32}$/i.test(options.accountId || ''))
    throw new Error(`--execute requires a 32-hex-character ${provider.account_env}`);
  return provider.endpoint.replace('{account_id}', options.accountId);
}

function answerBody(response, provider) {
  if (provider.response_envelope === 'direct') return response;
  if (response?.success !== true || !response.result || typeof response.result !== 'object' ||
      Array.isArray(response.result)) throw new Error('Cloudflare response has no successful result');
  return response.result;
}

export function admittedDescriptors(frontierPath, archivePath) {
  const frontier = new Database(frontierPath, { readonly: true });
  let channels;
  try {
    channels = frontier.query(`SELECT channel_id,exact_address,title,material,protocol,carries,cadence,census_source,contributions
      FROM channel WHERE state IN ('admitted','active') AND COALESCE(census_source,'')<>'' ORDER BY exact_address`).all();
  } finally { frontier.close(); }
  const archiveAvailable = !!archivePath && existsSync(archivePath), counts = new Map();
  if (archiveAvailable) {
    const archive = new Database(archivePath, { readonly: true });
    try { for (const row of archive.query('SELECT source,COUNT(*) AS count FROM item GROUP BY source').all())
      counts.set(row.source, row.count); } finally { archive.close(); }
  }
  return channels.map(row => ({ channel_id: row.channel_id, address: row.exact_address,
    title: row.title || '', declared_material: row.material || 'unknown', protocol: row.protocol || 'unknown',
    measured_carries: row.carries || '', cadence: row.cadence || '', census_source: row.census_source,
    contributions: Number(row.contributions || 0),
    retained_items: archiveAvailable ? Number(counts.get(row.census_source) || 0) : null,
    retained_items_state: archiveAvailable ? 'observed' : 'unknown' }));
}

export function requestFor(batch, policy = decisionPolicy(), provider = providerFor(policy.default_request_provider, policy)) {
  if (batch.length !== 1) throw new Error('one semantic source per decision record; unrelated sources require separate records');
  const questions = {};
  batch.forEach((_, index) => {
    for (const spec of policy.question_specs) {
      if (!['choice', 'score', 'noul'].includes(spec.type) ||
          typeof policy.questions[spec.id] !== 'string' || !policy[spec.criteria_field])
        throw new Error(`invalid authored decision question: ${spec.id}`);
      questions[`s${index}_${spec.id}`] = { type: spec.type,
        instructions: `For state.sources[${index}]: ${policy.questions[spec.id]}`,
        criteria: policy[spec.criteria_field] };
    }
  });
  if (Object.keys(questions).length < 1 || Object.keys(questions).length > provider.max_questions)
    throw new Error(`request exceeds ${provider.max_questions} questions for ${provider.model}`);
  return { model: provider.model, state: { sources: batch }, questions };
}

export function validateAnswers(payload, batch, policy = decisionPolicy()) {
  const request = requestFor(batch, policy);
  const answers = validateDecisionResponse({ ...payload, model: payload?.model || request.model }, request);
  const schema = compileQuestionSchema(request.questions);
  const decoded = decodeDecisionAnswers(schema, answers);
  return batch.map((source, index) => {
    const program = answers[`s${index}_program`], attention = answers[`s${index}_attention`];
    const exact = answers[`s${index}_needs_exact_review`];
    const probabilities = program.probabilities;
    const review = source.retained_items_state !== 'observed' || exact.noul >= policy.exact_review_floor;
    return { source, program_choice: program.choice, program_confidence: program.confidence,
      program_probabilities: probabilities, attention_score: attention.score ?? null,
      attention_confidence: attention.confidence, needs_exact_review_probability: exact.noul,
      attention_probabilities: attention.probabilities,
      uncertainty: decoded.moments,
      diagnostic_lane: probabilities[program.choice] >= policy.lane_probability_floor && !review ? program.choice : 'review',
      action: 'none' };
  });
}

export async function postDecision(endpoint, key, request, policy = decisionPolicy(), fetcher = fetch, signal, admitAttempt = () => {}) {
  let failure;
  for (let attempt = 0; attempt < policy.retry_attempts; attempt++) {
    signal?.throwIfAborted();
    admitAttempt();
    try {
      const response = await fetcher(endpoint, { method: 'POST', body: canonical(request),
        headers: { Authorization: `Bearer ${key}`, 'Content-Type': 'application/json',
          Accept: 'application/json', 'User-Agent': policy.agent },
        signal: signal ? AbortSignal.any([signal, AbortSignal.timeout(policy.request_timeout_ms)])
          : AbortSignal.timeout(policy.request_timeout_ms) });
      if (!response.ok) {
        failure = new Error(`HTTP ${response.status}`);
        if (!policy.retry_statuses.includes(response.status)) break;
      } else return await response.json();
    } catch (error) { failure = error; }
    if (attempt + 1 < policy.retry_attempts) await Bun.sleep((1 + attempt) * 1000);
  }
  throw new Error(`decision request refused: ${failure?.message || 'unknown failure'}`);
}

export async function compileDecisionProgramPlane(frontier, options = {}, policy = decisionPolicy()) {
  // Own entry-time inputs before any scheduler yield or evaluator callback.
  options = { ...options };
  if (!Array.isArray(frontier)) throw new Error('decision frontier must be an array');
  frontier = JSON.parse(canonical(frontier));
  if (frontier.some(source => !source || typeof source !== 'object' || Array.isArray(source)))
    throw new Error('decision sources must be descriptor objects');
  policy = structuredClone(policy);
  const providerName = options.provider ?? policy.default_provider;
  if (options.localEvaluator && providerName !== 'local')
    throw new Error('an injected local evaluator requires local placement');
  const local = providerName === 'local' ? options.localEvaluator || installedDecisionOwner : null;
  const provider = providerFor(local ? policy.default_request_provider : providerName, policy);
  const confidence = local?.confidence_semantics ?? 'maximum_option_probability';
  if (local && (typeof local.evaluate !== 'function' || typeof local.model !== 'string' || !local.model ||
      !/^[a-f0-9]{64}$/.test(local.artifact_digest || '') ||
      !['joint_schema', 'independent_evaluation'].includes(local.question_semantics) ||
      !['maximum_option_probability', 'vendor_distribution_statistic', 'typesafe-uniform-adjusted'].includes(confidence)))
    throw new Error('local decision owner requires a pinned artifact and declared semantics');
  const localIdentity = local ? { model: local.model, model_artifact_digest: local.artifact_digest,
    question_semantics: local.question_semantics, confidence_semantics: confidence,
    model_identity_state: 'artifact_bound' } : null;
  const localDeclaration = local ? [local.model, local.artifact_digest, local.question_semantics,
    local.confidence_semantics, local.evaluate, local.deterministic] : null;
  const unchangedLocal = () => {
    if (local && [local.model, local.artifact_digest, local.question_semantics,
      local.confidence_semantics, local.evaluate, local.deterministic].some((value, index) => value !== localDeclaration[index]))
      throw new Error('local decision owner changed during execution');
  };
  const questionsPerSource = policy.question_specs.length;
  const maxBatchSize = Math.min(1, Math.floor(provider.max_questions / questionsPerSource));
  const batchSize = options.batchSize === undefined ? maxBatchSize : Number(options.batchSize);
  if (!Number.isInteger(batchSize) || batchSize < 1 || batchSize > maxBatchSize)
    throw new Error(`--batch-size must be 1..${maxBatchSize} for ${providerName}`);
  if (options.execute && (!Number.isSafeInteger(options.maxCalls) || options.maxCalls < 1 || frontier.length > options.maxCalls))
    throw new Error('decision execution requires an explicit maximum call budget covering all source records');
  if (local && options.endpoint) throw new Error('local placement does not accept an endpoint override');
  if (options.execute && !local && frontier.some(source =>
    (source.information_policy_class ?? options.informationPolicyClass) !== 'public'))
    throw new Error('hosted decision execution requires explicitly public source descriptors');
  const endpoint = options.execute && !local ? endpointFor(provider, options) : null;
  if (options.execute && !local && !endpoint.startsWith('https://'))
    throw new Error('--execute endpoint must use https');
  if (options.execute && !local && !options.apiKey) throw new Error(`--execute requires ${provider.credential_env}`);
  const sharedPolicy = decisionPlanePolicy();
  const residency = { ...sharedPolicy.local_inference_residency };
  const programDigest = digest(policy);
  let attempts = 0, localCalls = 0, reusedLocalCalls = 0;
  const admitAttempt = () => {
    if (attempts >= options.maxCalls) throw new Error('decision maximum call budget exhausted');
    attempts++;
  };
  const rows = await runDecisionRecords(frontier, async (source, index, signal) => {
    unchangedLocal();
    const batch = [source], request = requestFor(batch, policy, provider);
    if (local) request.model = localIdentity.model;
    const requestDigest = digest(request);
    const provenance = { program_digest: programDigest, source_digest: digest(source),
      information_policy_class: source.information_policy_class ?? options.informationPolicyClass ?? 'unspecified',
      request_provider: local ? policy.default_request_provider : providerName };
    if (!options.execute) {
      return { schema: policy.schema, provider: providerName, model: request.model, ...localIdentity,
        ...provenance,
        state: 'planned', action: 'none', batch: index,
        sources: batch.length, request_digest: requestDigest, cannot_infer: policy.planned_cannot_infer };
    }
    const started = performance.now();
    let response, result, inference;
    if (local) {
      admitAttempt();
      const localRequest = JSON.parse(canonical(request));
      const deadline = AbortSignal.any([signal, AbortSignal.timeout(policy.local_request_timeout_ms)]);
      const evaluated = await evaluateLocalDecision(local, { state: localRequest.state,
        questions: localRequest.questions, schema: compileQuestionSchema(localRequest.questions) }, {
        ...residency, signal: deadline,
        key: digest({ owner: 'ExternalEstate:DecisionProgram', request: requestDigest,
          program: programDigest, source: digest(source), identity: localIdentity,
          information_policy: source.information_policy_class ?? options.informationPolicyClass ?? 'unspecified' }),
      });
      deadline.throwIfAborted();
      unchangedLocal();
      inference = evaluated.inference;
      if (inference.executed) localCalls++;
      if (inference.state === 'reused') reusedLocalCalls++;
      result = { model: localIdentity.model, answers: evaluated.answers };
      response = { ...result, artifact: localIdentity.model_artifact_digest };
    } else {
      response = await postDecision(endpoint, options.apiKey, request, policy, options.fetcher, signal, admitAttempt);
      result = answerBody(response, provider);
    }
    const latencyMs = Math.round(performance.now() - started);
    const responseDigest = digest(response);
    const [assessment] = validateAnswers(result, batch, policy);
      return { schema: policy.schema, provider: providerName, state: 'diagnostic', model: result.model || provider.model,
        question_semantics: provider.question_semantics, confidence_semantics: provider.confidence_semantics,
        ...localIdentity, ...provenance, ...(inference ? { inference } : {}),
        request_digest: requestDigest, response_digest: responseDigest, latency_ms: latencyMs, ...assessment,
        cannot_infer: policy.diagnostic_cannot_infer };
  }, { concurrency: sharedPolicy.max_concurrency, signal: options.signal });
  return { rows, summary: { schema: policy.schema, provider: providerName,
    mode: options.execute ? 'execute' : 'plan',
    admitted_sources: frontier.length, planned: options.execute ? 0 : frontier.length,
    diagnostic_assessments: options.execute ? frontier.length : 0,
    batches: Math.ceil(frontier.length / batchSize), provider_calls: local ? 0 : attempts,
    local_calls: localCalls, reused_local_calls: reusedLocalCalls, out: options.out } };
}

// A model comparison is an operator review artifact, not a vote or an authority upgrade.
export function compareDecisionRuns(left, right) {
  const index = rows => {
    const byChannel = new Map();
    for (const row of rows) {
      const channel = row?.source?.channel_id;
      const probabilities = row?.program_probabilities;
      const values = probabilities && typeof probabilities === 'object' && !Array.isArray(probabilities)
        ? Object.values(probabilities) : [];
      if (row?.state !== 'diagnostic' || row.action !== 'none' ||
          !channel || !/^[a-f0-9]{64}$/.test(row.request_digest || '') ||
          !/^[a-f0-9]{64}$/.test(row.response_digest || '') ||
          !Object.hasOwn(probabilities || {}, row.program_choice) || !values.length ||
          !Number.isFinite(row.latency_ms) || byChannel.has(channel))
        throw new Error('comparison requires unique receipt-bound diagnostic rows');
      decodeDecisionAnswers(compileQuestionSchema({ program: { type: 'choice',
        criteria: Object.fromEntries(Object.keys(probabilities).map(key => [key, key])) } }),
      { program: { type: 'choice', choice: row.program_choice,
        confidence: row.program_confidence, probabilities } });
      byChannel.set(channel, row);
    }
    return byChannel;
  };
  const lhs = index(left), rhs = index(right);
  if (lhs.size !== rhs.size || [...lhs.keys()].some(key => !rhs.has(key)))
    throw new Error('comparison requires the same exact channel set');
  const rows = [...lhs.keys()].toSorted().map(channel => {
    const a = lhs.get(channel), b = rhs.get(channel);
    if (a.provider === b.provider) throw new Error('comparison requires distinct models');
    if (canonical(a.source) !== canonical(b.source))
      throw new Error('comparison requires the same exact source descriptor');
    const choices = [a, b].map(row => ({ provider: row.provider, model: row.model,
      choice: row.program_choice, diagnostic_lane: row.diagnostic_lane,
      confidence: row.program_confidence, probabilities: row.program_probabilities,
      attention_score: row.attention_score, attention_confidence: row.attention_confidence,
      exact_review_probability: row.needs_exact_review_probability,
      latency_ms: row.latency_ms, request_digest: row.request_digest,
      response_digest: row.response_digest }));
    const disagreement = a.program_choice !== b.program_choice;
    return { schema: 'bonfyre-decision-review.v1', channel_id: channel,
      source_address: a.source.address, choices, disagreement,
      review_required: disagreement || choices.some(choice => choice.diagnostic_lane === 'review'),
      action: 'none', cannot_infer: 'model agreement, confidence or speed cannot establish source truth, authority, rights or publication' };
  });
  return { rows, summary: { schema: 'bonfyre-decision-review-summary.v1',
    compared: rows.length, disagreements: rows.filter(row => row.disagreement).length,
    review_required: rows.filter(row => row.review_required).length } };
}

const readJsonl = path => readFileSync(path, 'utf8').split(/\r?\n/).filter(Boolean).map(line => JSON.parse(line));
const writeJsonl = (path, rows) => {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, rows.map(row => canonical(row)).join('\n') + (rows.length ? '\n' : ''));
};

if (import.meta.main) {
  try {
    const args = { execute: false }, argv = process.argv.slice(2);
    for (let index = 0; index < argv.length; index++) {
      const key = argv[index];
      if (key === '--execute') { args.execute = true; continue; }
      if (!key.startsWith('--') || !argv[index + 1]) throw new Error('expected --key value arguments');
      args[key.slice(2).replace(/-([a-z])/g, (_, letter) => letter.toUpperCase())] = argv[++index];
    }
    if (args.compareLeft || args.compareRight) {
      if (!args.compareLeft || !args.compareRight || !args.out)
        throw new Error('--compare-left, --compare-right and --out are required together');
      const result = compareDecisionRuns(readJsonl(args.compareLeft), readJsonl(args.compareRight));
      writeJsonl(args.out, result.rows);
      console.log(JSON.stringify(result.summary));
      process.exit(0);
    }
    if (!args.frontier || !args.out || !existsSync(args.frontier)) throw new Error('--frontier must be an existing frontier.db and --out is required');
    if (args.execute && (args.limit === undefined || !Number.isInteger(Number(args.limit)) ||
        Number(args.limit) < 1 || Number(args.limit) > decisionPolicy().max_execute_sources))
      throw new Error(`--execute requires --limit 1..${decisionPolicy().max_execute_sources}`);
    if ((args.provider ?? decisionPolicy().default_provider) !== 'local') {
      const provider = providerFor(args.provider, decisionPolicy());
      args.apiKey = process.env[provider.credential_env] || '';
      if (provider.account_env) args.accountId = process.env[provider.account_env] || '';
    }
    if (args.maxCalls !== undefined) args.maxCalls = Number(args.maxCalls);
    const descriptors = admittedDescriptors(args.frontier, args.archive);
    if (args.limit !== undefined) {
      const limit = Number(args.limit);
      if (!Number.isInteger(limit) || limit < 1) throw new Error('--limit must be a positive integer');
      descriptors.length = Math.min(descriptors.length, limit);
    }
    const result = await compileDecisionProgramPlane(descriptors, args);
    writeJsonl(args.out, result.rows);
    console.log(JSON.stringify(result.summary));
  } catch (error) { console.error(`decision program plane: ${error.message}`); process.exitCode = 2; }
  finally { await closeInstalledDecisionEvaluator(); }
}
