#!/usr/bin/env bun
// Reuse an exact ContextCut's token spans; a model may alter a purpose-specific
// representation only under an explicit measured policy, never owner standing.
import { createHash, randomUUID } from 'node:crypto';
import { closeSync, fsyncSync, mkdirSync, openSync, readFileSync, renameSync, unlinkSync,
  writeFileSync } from 'node:fs';
import { dirname } from 'node:path';
import { contextPassageCases } from './frontier-adapters.mjs';
import { compileDecisionCase, decisionDigest, decisionProposal, decisionReceiptRequest, decisionModelPassport,
  validateDecisionReceipt, validateDecisionLabels, assessSelectiveValue, decisionPlanePolicy } from './decision-plane.mjs';

const digest = bytes => `sha256:${createHash('sha256').update(bytes).digest('hex')}`;
const jsonLines = path => readFileSync(path, 'utf8').split(/\r?\n/).filter(Boolean).map(line => JSON.parse(line));
function omitFrom(proposal, thresholds) {
  // Only a confident low-value passage can be omitted. Contradictions and
  // injection suspicion remain visible to the downstream review boundary.
  return proposal.relevance <= thresholds.irrelevance_max &&
    proposal.usability <= thresholds.usability_max &&
    proposal.contradiction <= thresholds.contradiction_max &&
    proposal.instruction_attempt < thresholds.instruction_review_min;
}

export function evaluateDecisionCutSamples(samples, thresholds, policy, targetDigests = new Set(), expectedModel = '') {
  if (!Array.isArray(samples) || samples.length > decisionPlanePolicy().evaluation.max_cases ||
      !policy || !Number.isSafeInteger(policy.minimum_cases) ||
      policy.minimum_cases < 1 || typeof policy.maximum_false_omission_rate !== 'number' ||
      policy.maximum_false_omission_rate < 0 || policy.maximum_false_omission_rate > 1 ||
      !Number.isFinite(policy.minimum_net_saved_ms) || policy.minimum_net_saved_ms < 0)
    throw new Error('decision cut promotion requires a measured evaluation policy');
  const seen = new Set(), requests = new Set(), units = new Set(), bodies = new Set(), sources = new Set();
  const outcomes = [];
  for (const sample of samples) {
    const receipt = sample.receipt, compiled = sample.case;
    const rebuilt = compiled?.contract && compiled?.state
      ? compileDecisionCase({ ...compiled.contract, state: compiled.state }) : null;
    if (receipt?.state !== 'diagnostic' || receipt.contract?.program !== 'context_passage' ||
        !rebuilt || rebuilt.case_digest !== compiled.case_digest ||
        receipt.case_digest !== compiled.case_digest ||
        decisionDigest(receipt.contract) !== decisionDigest(compiled.contract) ||
        receipt.request_digest !== decisionDigest(decisionReceiptRequest(compiled, receipt)) ||
        decisionModelPassport(receipt) !== expectedModel || receipt.action !== 'none' ||
        !/^[a-f0-9]{64}$/.test(receipt.response_digest || '') || seen.has(receipt.case_digest) ||
        targetDigests.has(receipt.case_digest) || targetDigests.has(receipt.request_digest) ||
        requests.has(receipt.request_digest) || typeof sample.expected_keep !== 'boolean' ||
        ![sample.baseline_ms, sample.decision_ms, sample.fallback_ms].every(value =>
          Number.isFinite(value) && value >= 0))
      throw new Error('evaluation requires distinct held-out labeled decision receipts and measured costs');
    validateDecisionReceipt(compiled, receipt);
    validateDecisionLabels(sample, { expected_keep: sample.expected_keep });
    const passage = compiled.state.passage;
    // A whole-body omission gate must not count correlated chunks as separate
    // trials. Until grouped-body evaluation is supplied, only complete bounded
    // single-body examples qualify. Refuse rather than overstate sample size.
    if (passage.chunk_count !== 1 || passage.chunk_index !== 0 ||
        typeof passage.text !== 'string' || digest(Buffer.from(passage.text)) !== passage.content_sha256 ||
        typeof passage.source_candidate_id !== 'string' || !passage.source_candidate_id ||
        passage.source_candidate_id !== passage.id || units.has(sample.evaluation_unit) ||
        bodies.has(passage.content_sha256) || sources.has(passage.source_candidate_id) ||
        targetDigests.has(passage.content_sha256) || targetDigests.has(passage.source_candidate_id))
      throw new Error('evaluation requires distinct held-out complete bodies, not correlated chunks or target lineage');
    units.add(sample.evaluation_unit); bodies.add(passage.content_sha256); sources.add(passage.source_candidate_id);
    seen.add(receipt.case_digest);
    requests.add(receipt.request_digest);
    const proposal = decisionProposal(receipt), omit = omitFrom(proposal, thresholds);
    outcomes.push({ accepted: omit, correct: !sample.expected_keep,
      baseline_ms: sample.baseline_ms, decision_ms: sample.decision_ms, fallback_ms: sample.fallback_ms });
  }
  const measured = assessSelectiveValue(outcomes, { ...policy,
    maximum_error_rate: policy.maximum_false_omission_rate });
  return { ...measured, omitted: measured.accepted, false_omissions: measured.errors,
    false_omission_rate: measured.error_rate, false_omission_rate_upper: measured.error_rate_upper };
}
function atomic(path, bytes) {
  mkdirSync(dirname(path), { recursive: true });
  const temporary = `${path}.${randomUUID()}.tmp`, fd = openSync(temporary, 'wx', 0o600);
  try { writeFileSync(fd, bytes); fsyncSync(fd); closeSync(fd); renameSync(temporary, path); }
  catch (error) { try { closeSync(fd); } catch {} try { unlinkSync(temporary); } catch {} throw error; }
}

export function compileDecisionCut(manifest, materialRows, purpose, receipts, options) {
  if (!options?.out || !options?.thresholds || !Array.isArray(receipts))
    throw new Error('decision cut requires an output, explicit thresholds, and receipts');
  const t = options.thresholds;
  if (![t.irrelevance_max, t.usability_max, t.contradiction_max, t.instruction_review_min]
    .every(value => typeof value === 'number' && value >= 0 && value <= 1))
    throw new Error('decision cut thresholds must be bounded and explicit');
  const cases = contextPassageCases(manifest, materialRows, purpose);
  const expected = new Map(cases.map(item => [item.case_digest, item]));
  if (receipts.length !== expected.size) throw new Error('decision cut requires one receipt per exact selected case');
  const byCase = new Map(), models = new Set();
  for (const receipt of receipts) {
    const compiled = expected.get(receipt?.case_digest);
    if (!compiled || byCase.has(receipt.case_digest) ||
        decisionDigest(receipt.contract) !== decisionDigest(compiled.contract) ||
        receipt?.state !== 'diagnostic' || receipt?.action !== 'none' ||
        typeof receipt.model !== 'string' || !receipt.model ||
        !/^[a-f0-9]{64}$/.test(receipt.response_digest || '') ||
        receipt.request_digest !== decisionDigest(decisionReceiptRequest(compiled, receipt)))
      throw new Error('decision cut receipt is missing, duplicated, stale, or effecting');
    validateDecisionReceipt(compiled, receipt);
    byCase.set(receipt.case_digest, receipt);
    models.add(decisionModelPassport(receipt));
  }
  if (models.size !== 1) throw new Error('decision cut cannot mix model passports');
  const evaluations = options.evaluation_rows;
  const measured = evaluations && options.evaluation_policy
    ? evaluateDecisionCutSamples(evaluations, t, options.evaluation_policy,
      new Set([...expected.keys(), ...receipts.map(receipt => receipt.request_digest),
        ...cases.flatMap(item => [item.state.passage.content_sha256, item.state.passage.source_candidate_id])]), [...models][0]) : null;
  // A provider echoing a different model name is not an immutable revision
  // attestation. Only locally pinned artifacts currently support promotion.
  const reusableModel = receipts.every(receipt => receipt.provider === 'local' && receipt.model_identity_state === 'artifact_bound');
  const executable = manifest.rank_gate_state === 'ADMIT' && reusableModel && measured?.promotion_eligible === true;
  if (options.require_executable && !executable)
    throw new Error('decision cut lacks an ADMIT frontier, revision-bound model and measured selective promotion');

  const tokenBody = readFileSync(manifest.token_body);
  if (manifest.token_encoding !== 'u32le' || digest(tokenBody) !== manifest.token_sha256 ||
      tokenBody.length !== manifest.selected_tokens * 4 || !Array.isArray(manifest.token_spans))
    throw new Error('source ContextCut token body or spans are invalid');
  const spans = new Map();
  for (const span of manifest.token_spans) {
    if (spans.has(span.content_sha256) || !Number.isSafeInteger(span.offset_tokens) ||
        !Number.isSafeInteger(span.token_count) || span.token_count < 1 ||
        span.offset_tokens < 0 || span.offset_tokens + span.token_count > manifest.selected_tokens)
      throw new Error('source ContextCut has invalid or duplicate token spans');
    spans.set(span.content_sha256, span);
  }
  const materials = new Map(materialRows.map(row => [row.candidate_identity, row]));
  const byCandidate = new Map();
  for (const item of cases) {
    const id = item.state.passage.source_candidate_id;
    const bucket = byCandidate.get(id) || [];
    bucket.push(item);
    byCandidate.set(id, bucket);
  }
  const kept = [], omitted = [], proposals = [], content = [], tokenParts = [], tokenSpans = [], seen = new Set();
  let tokenCount = 0;
  for (const candidate of manifest.selected_candidates) {
    const chunks = byCandidate.get(candidate.identity);
    if (!chunks?.length) throw new Error(`selected ContextCut body has no decision chunks: ${candidate.identity}`);
    const chunkProposals = chunks.map(item => decisionProposal(byCase.get(item.case_digest)));
    proposals.push(...chunkProposals);
    const keep = !chunkProposals.every(proposal => omitFrom(proposal, t));
    if (!keep) { omitted.push(candidate.identity); continue; }
    const span = spans.get(candidate.content_sha256), material = materials.get(candidate.identity);
    if (!span || !material) throw new Error(`selected ContextCut body has no token span: ${candidate.identity}`);
    kept.push(candidate);
    if (seen.has(candidate.content_sha256)) continue;
    const body = readFileSync(material.body_path);
    if (digest(body) !== candidate.content_sha256) throw new Error('ContextCut body changed after decision assessment');
    content.push(Buffer.from(`<bonfyre-material cid="${candidate.content_sha256}" receipt="${candidate.receipt_digest}" evidence="${candidate.evidence_ref}">\n`));
    content.push(Buffer.from(new TextDecoder('utf-8', { fatal: false }).decode(body), 'utf8'),
      Buffer.from('\n</bonfyre-material>\n'));
    tokenParts.push(tokenBody.subarray(span.offset_tokens * 4, (span.offset_tokens + span.token_count) * 4));
    tokenSpans.push({ content_sha256: candidate.content_sha256, offset_tokens: tokenCount,
      token_count: span.token_count });
    tokenCount += span.token_count; seen.add(candidate.content_sha256);
  }
  if (!kept.length) throw new Error('decision cut selected no material; preserve the original ContextCut');
  const contextBytes = Buffer.concat(content), tokenBytes = Buffer.concat(tokenParts);
  const out = options.out, contextPath = `${out}.context`, tokenPath = `${out}.tokens.u32le`;
  const result = { schema: 'bonfyre.decision-cut.v1', source_context_sha256: manifest.context_sha256,
    source_token_sha256: manifest.token_sha256, source_rank_gate_state: manifest.rank_gate_state,
    state: executable ? 'execution_candidate' : 'diagnostic_only',
    purpose, provider_model: [...models][0], model_identity_reusable: reusableModel, selected_candidates: kept,
    omitted_by_decision: omitted, recoverable_frontier: manifest.selected_candidates.map(row => row.identity),
    proposals, evaluation: measured, evaluation_digest: evaluations ? decisionDigest(evaluations) : null,
    thresholds: t, token_count_before: manifest.selected_tokens, token_count_after: tokenCount,
    tokens_avoided_in_derived_representation: manifest.selected_tokens - tokenCount,
    context_body: contextPath, context_sha256: digest(contextBytes),
    token_body: tokenPath, token_sha256: digest(tokenBytes), token_encoding: 'u32le', token_spans: tokenSpans,
    cannot_infer: 'model filtering or a smaller derived cut -> evidence completeness, truth, authority, safety, or permission for an effect' };
  atomic(contextPath, contextBytes); atomic(tokenPath, tokenBytes);
  atomic(out, Buffer.from(JSON.stringify(result) + '\n'));
  return result;
}

if (import.meta.main) {
  try {
    const [source, materials, purpose, receipts, thresholds, out, evaluation, policy, requireFlag] = process.argv.slice(2);
    if (![source, materials, purpose, receipts, thresholds, out].every(Boolean))
      throw new Error('usage: decision-cut.mjs <context-cut.json> <materials.jsonl> <purpose> <receipts.jsonl> <thresholds.json> <out> [held-out-evaluation.jsonl evaluation-policy.json [--require-executable]]');
    if (!!evaluation !== !!policy || (requireFlag && requireFlag !== '--require-executable'))
      throw new Error('held-out evaluation and policy must be supplied together');
    const result = compileDecisionCut(JSON.parse(readFileSync(source, 'utf8')), jsonLines(materials), purpose,
      jsonLines(receipts), { thresholds: JSON.parse(readFileSync(thresholds, 'utf8')), out,
        evaluation_rows: evaluation ? jsonLines(evaluation) : undefined,
        evaluation_policy: policy ? JSON.parse(readFileSync(policy, 'utf8')) : undefined,
        require_executable: requireFlag === '--require-executable' });
    console.log(JSON.stringify({ state: result.state, tokens_avoided: result.tokens_avoided_in_derived_representation,
      selected: result.selected_candidates.length, omitted: result.omitted_by_decision.length }));
  } catch (error) { console.error(`decision cut: ${error.message}`); process.exitCode = 2; }
}
