import { test, expect } from 'bun:test';
import { createHash } from 'node:crypto';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { assessDecision, compileDecisionCase, decisionDigest } from '../decision-plane.mjs';
import { contextPassageCases } from '../frontier-adapters.mjs';
import { compileDecisionCut } from '../decision-cut.mjs';

const hash = bytes => `sha256:${createHash('sha256').update(bytes).digest('hex')}`;
const resultFor = (relevant, usable) => ({ model: 'clef-flash-fixture-r1', answers: {
  relevant: { type: 'noul', noul: relevant }, usable: { type: 'noul', noul: usable },
  contradicts: { type: 'noul', noul: 0.01 }, instructs: { type: 'noul', noul: 0.01 },
} });

test('typed decisions can produce a smaller receipted ContextCut without losing the recoverable frontier', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'bonfyre-decision-cut.'));
  try {
    const purpose = 'Explain public event', materials = [], selected = [], spans = [];
    for (const [index, body] of ['The event was announced today.',
      'Unrelated cooking note. '.repeat(1800)].entries()) {
      const path = join(dir, `body-${index}.txt`);
      writeFileSync(path, body);
      const id = `source:${index}`, cid = hash(Buffer.from(body));
      materials.push({ candidate_identity: id, body_path: path, receipt_digest: `receipt:${index}`,
        information_policy_class: 'public' });
      selected.push({ identity: id, candidate_kind: 'external_occurrence',
        owner_ref: `ForeignTwin:${index}`, content_sha256: cid, receipt_digest: `receipt:${index}`,
        evidence_ref: `evidence:${index}` });
      spans.push({ content_sha256: cid, offset_tokens: index * 2, token_count: 2 });
    }
    const tokenPath = join(dir, 'source.tokens');
    const tokenBody = Buffer.alloc(16);
    [11, 12, 21, 22].forEach((value, index) => tokenBody.writeUInt32LE(value, index * 4));
    writeFileSync(tokenPath, tokenBody);
    const manifest = { schema: 'bonfyre.context-cut.v1', rank_gate_state: 'ADMIT',
      purpose,
      frontier: { kind: 'bitnet_rank', run_id: 'run:one' }, context_sha256: hash('source context'),
      token_body: tokenPath, token_sha256: hash(tokenBody), token_encoding: 'u32le',
      selected_tokens: 4, token_spans: spans, selected_candidates: selected,
      information_policy_class: 'public' };
    const cases = contextPassageCases(manifest, materials, purpose);
    expect(cases.length).toBeGreaterThan(2);
    const receipts = await Promise.all(cases.map((item, index) => assessDecision(item,
      { provider: 'clef-flash', execute: true, accountId: '0123456789abcdef0123456789abcdef', apiKey: 'fixture',
        fetcher: async () => ({ ok: true, json: async () => ({ success: true,
          result: resultFor(index === 0 ? 0.95 : 0.1, index === 0 ? 0.9 : 0.2) }) }) } )));
    const thresholds = { irrelevance_max: 0.2, usability_max: 0.2,
      contradiction_max: 0.2, instruction_review_min: 0.5 };
    const out = join(dir, 'derived.json');
    const result = compileDecisionCut(manifest, materials, purpose, receipts, { out, thresholds });
    expect(result).toMatchObject({ state: 'diagnostic_only', token_count_before: 4,
      token_count_after: 2, tokens_avoided_in_derived_representation: 2,
      omitted_by_decision: ['source:1'], recoverable_frontier: ['source:0', 'source:1'] });
    expect(readFileSync(result.token_body)).toEqual(tokenBody.subarray(0, 8));
    expect(readFileSync(result.context_body, 'utf8')).toContain('The event was announced');
    expect(readFileSync(result.context_body, 'utf8')).not.toContain('Unrelated cooking');
    const localEvaluator = { model: 'fixture-logits-not-clef', artifact_digest: 'a'.repeat(64),
        question_semantics: 'joint_schema', evaluate: ({ state, schema }) => {
          const relevant = state.passage.text.startsWith('The event');
          const logits = new Float64Array(schema.width);
          for (const field of schema.fields) {
            const p = ['relevant', 'usable'].includes(field.id) && relevant ? 0.95 : 0.01;
            logits.set([Math.log(p), Math.log1p(-p)], field.offset);
          }
          return logits;
        } };
    const localReceipts = await Promise.all(cases.map(item => assessDecision(item, { execute: true, localEvaluator })));
    const localCut = compileDecisionCut(manifest, materials, purpose, localReceipts,
      { out: join(dir, 'local-derived.json'), thresholds });
    expect(localCut.provider_model).toContain('local:fixture-logits-not-clef:');
    expect(localCut.token_count_after).toBe(2);
    expect(localCut.state).toBe('diagnostic_only');
    expect(readFileSync(localCut.token_body)).toEqual(tokenBody.subarray(0, 8));
    expect(() => compileDecisionCut(manifest, materials, purpose, receipts,
      { out, thresholds, require_executable: true })).toThrow('measured selective promotion');
    // Synthetic held-out fixture, not a claim of production calibration.
    const evalCases = Array.from({ length: 65 }, (_, index) => {
      const id = `held-out:${index}`, text = `${index === 0 ? 'The event' : 'Unrelated'} evaluation body ${index}.`;
      return compileDecisionCase({ ...cases[0].contract, subject_id: id, mask_digest: decisionDigest(id),
        frontier: [{ id, description: 'held-out body', owner_ref: 'ContextCut:evaluation' }],
        state: { purpose, passage: { id, source_candidate_id: id, text,
          content_sha256: hash(text), chunk_index: 0, chunk_count: 1 } } }); });
    const evalReceipts = await Promise.all(evalCases.map(item => assessDecision(item, { execute: true, localEvaluator })));
    const evalRows = evalReceipts.map((receipt, index) => {
      const evaluation_unit = `independent-body:${index}`;
      const label_receipt = { schema: 'bonfyre.decision-label.v1', evaluation_unit,
        case_digest: evalCases[index].case_digest, labels: { expected_keep: index === 0 },
        source_ref: `test-fixture-review:${index}` };
      return { receipt, case: evalCases[index], evaluation_unit, label_receipt,
        label_receipt_digest: decisionDigest(label_receipt),
        expected_keep: index === 0, baseline_ms: 1000, decision_ms: 100, fallback_ms: 1000 }; });
    const evaluationPolicy = { minimum_cases: 60, maximum_false_omission_rate: 0.05,
      minimum_net_saved_ms: 100, sampling_assumption: 'fixed_policy_independent_holdout' };
    const promoted = compileDecisionCut(manifest, materials, purpose, localReceipts, { out, thresholds,
      evaluation_rows: evalRows, evaluation_policy: evaluationPolicy, require_executable: true });
    expect(promoted.state).toBe('execution_candidate');
    expect(promoted.evaluation.false_omission_rate_upper).toBeGreaterThan(0);
    expect(promoted.evaluation.false_omission_rate_upper).toBeLessThan(0.05);
    expect(() => compileDecisionCut(manifest, materials, purpose, localReceipts, { out, thresholds,
      evaluation_rows: evalRows.slice(0, 2), evaluation_policy: { ...evaluationPolicy, minimum_cases: 2 },
      require_executable: true })).toThrow('measured selective promotion');
    expect(promoted.evaluation.net_saved_ms).toBe(900 * evalRows.length - 1000);
    const reassessedPassage = async overrides => {
      const original = evalRows[0];
      const passage = { ...original.case.state.passage, ...overrides };
      const compiled = compileDecisionCase({ ...original.case.contract,
        frontier: [{ ...original.case.contract.frontier[0], id: passage.id }],
        state: { purpose, passage } });
      const label_receipt = { ...original.label_receipt, case_digest: compiled.case_digest };
      return { ...original, case: compiled, receipt: await assessDecision(compiled, { execute: true, localEvaluator }),
        label_receipt, label_receipt_digest: decisionDigest(label_receipt) };
    };
    for (const mutation of [
      { id: cases[0].state.passage.id, source_candidate_id: cases[0].state.passage.source_candidate_id },
      { content_sha256: hash('not the assessed text') },
      { chunk_count: 2 },
    ]) {
      const row = await reassessedPassage(mutation);
      expect(() => compileDecisionCut(manifest, materials, purpose, localReceipts, { out, thresholds,
        evaluation_rows: [row, ...evalRows.slice(1)], evaluation_policy: evaluationPolicy }))
        .toThrow('complete bodies');
    }
    const renamedTargets = cases.map((item, index) => compileDecisionCase({ ...item.contract,
      subject_id: `renamed-target:${index}`, state: item.state }));
    const leakedEvaluation = renamedTargets.map((item, index) => ({
      ...evalRows[index], case: item,
      receipt: { ...receipts[index], contract: item.contract, case_digest: item.case_digest },
    }));
    expect(() => compileDecisionCut(manifest, materials, purpose, receipts, { out, thresholds,
      evaluation_rows: leakedEvaluation, evaluation_policy: { minimum_cases: 2,
        maximum_false_omission_rate: 0, minimum_net_saved_ms: 100 } })).toThrow('held-out');
    const aliases = receipts.map(receipt => ({ ...receipt, model_identity_state: 'mutable_alias' }));
    expect(() => compileDecisionCut(manifest, materials, purpose, aliases, { out, thresholds,
      evaluation_rows: evalRows, evaluation_policy: { minimum_cases: 2,
        maximum_false_omission_rate: 0, minimum_net_saved_ms: 100 }, require_executable: true }))
      .toThrow('model identity mismatch');
    // A reported hosted name is never treated as an immutable trained artifact.
    expect(() => compileDecisionCut(manifest, materials, purpose, receipts,
      { out, thresholds, require_executable: true })).toThrow('revision-bound');
    const materialPath = join(dir, 'materials.jsonl'), receiptPath = join(dir, 'receipts.jsonl');
    const thresholdPath = join(dir, 'thresholds.json'), evaluationPath = join(dir, 'evaluation.jsonl');
    const evaluationPolicyPath = join(dir, 'evaluation-policy.json'), sourcePath = join(dir, 'source.json');
    writeFileSync(materialPath, materials.map(row => JSON.stringify(row)).join('\n') + '\n');
    writeFileSync(receiptPath, localReceipts.map(row => JSON.stringify(row)).join('\n') + '\n');
    writeFileSync(thresholdPath, JSON.stringify(thresholds));
    writeFileSync(evaluationPath, evalRows.map(row => JSON.stringify(row)).join('\n') + '\n');
    writeFileSync(evaluationPolicyPath, JSON.stringify(evaluationPolicy));
    writeFileSync(sourcePath, JSON.stringify(manifest));
    const cliOut = join(dir, 'cli-derived.json');
    const cli = Bun.spawnSync(['bun', new URL('../decision-cut.mjs', import.meta.url).pathname,
      sourcePath, materialPath, purpose, receiptPath, thresholdPath, cliOut,
      evaluationPath, evaluationPolicyPath, '--require-executable']);
    expect(cli.exitCode, cli.stderr.toString()).toBe(0);
    expect(JSON.parse(readFileSync(cliOut, 'utf8')).state).toBe('execution_candidate');
    const tamperedRows = [{ ...evalRows[0], case: { ...evalRows[0].case,
      state: { ...evalRows[0].case.state, purpose: 'changed after evaluation' } } }, ...evalRows.slice(1)];
    expect(() => compileDecisionCut(manifest, materials, purpose, receipts, { out, thresholds,
      evaluation_rows: tamperedRows, evaluation_policy: { minimum_cases: 2,
        maximum_false_omission_rate: 0, minimum_net_saved_ms: 100 } }))
      .toThrow('held-out');
    expect(() => compileDecisionCut({ ...manifest, rank_gate_state: 'SHADOW' }, materials, purpose,
      receipts, { out, thresholds, evaluation_rows: evalRows,
        evaluation_policy: { minimum_cases: 2, maximum_false_omission_rate: 0,
          minimum_net_saved_ms: 100 }, require_executable: true })).toThrow('stale');
  } finally { rmSync(dir, { recursive: true, force: true }); }
});
