#!/usr/bin/env bun
// Explicit integration witness: real pinned local weights, never part of the
// offline unit suite. These small authored examples are not a quality benchmark.
import { strict as assert } from 'node:assert';
import { writeFileSync } from 'node:fs';
import { cpus, totalmem } from 'node:os';
import { assessDecision, compileDecisionCase, decisionDigest } from '../decision-plane.mjs';
import { flowWorkCase, mcpToolCase } from '../frontier-adapters.mjs';
import { closeInstalledDecisionEvaluator, installedDecisionOwner } from '../installed-runtime.mjs';
import { compileDecisionProgramPlane } from '../../../cmd/BonfyreExternalEstate/tools/decision_program_plane.mjs';

const context = compileDecisionCase({ program: 'context_passage', owner: 'ContextCut',
  subject_id: 'witness:passage', mask_digest: decisionDigest('witness:exact-frontier'),
  gate_state: 'ADMIT', information_policy_class: 'private',
  frontier: [{ id: 'passage', description: 'Authored witness text', owner_ref: 'ContextCut:witness' }],
  state: { purpose: 'What time does the library open on Monday?',
    passage: { id: 'passage', text: 'The library opens at 9 AM on Monday and closes at 6 PM.' } } });
const mcp = mcpToolCase({ tools: [
  { name: 'read_library_hours', description: 'Read the library opening hours', annotations: { readOnlyHint: true } },
  { name: 'read_weather', description: 'Read the weather forecast', annotations: { readOnlyHint: true } },
] }, ['read_library_hours', 'read_weather'], 'Find the library opening time on Monday', 'private');
const flow = flowWorkCase({ id: 'witness:work', information_policy_class: 'private',
  title: 'Review a source document and check its quoted facts' }, [
  { id: 'fact_review', description: 'Check source documents and verify factual claims', owner_ref: 'Flow:review' },
  { id: 'layout', description: 'Design visual page layouts', owner_ref: 'Flow:design' },
], decisionDigest('witness:flow-frontier'));

const receipts = [];
const started = performance.now();
let failure;
try {
  const plan = await assessDecision(context);
  assert.equal(plan.provider, 'local');
  assert.equal(plan.model_artifact_digest, installedDecisionOwner.artifact_digest);
  for (const [name, compiled] of [['context-cold', context], ['mcp-warm', mcp],
    ['flow-warm', flow], ['context-warm-repeat', context]]) {
    const began = performance.now();
    // Deliberately no provider/localEvaluator: exercise the ordinary default.
    const receipt = await assessDecision(compiled, { execute: true,
      fetcher: () => { throw new Error('hosted egress must not occur'); } });
    assert.equal(receipt.provider, 'local');
    assert.equal(receipt.action, 'none');
    assert.equal(receipt.inference.executed, true);
    assert.equal(receipt.inference.state, 'uncached');
    assert.equal(receipt.model_artifact_digest, installedDecisionOwner.artifact_digest);
    receipts.push({ name, end_to_end_ms: performance.now() - began, receipt });
    console.log(JSON.stringify({ name, latency_ms: receipt.latency_ms, answers: receipt.answers }));
  }
  const began = performance.now();
  const estate = await compileDecisionProgramPlane([{ channel_id: 'witness:source',
    address: 'https://example.invalid/library', title: 'Library written announcements',
    declared_material: 'articles', protocol: 'rss', measured_carries: 'written articles',
    cadence: 'weekly', census_source: 'witness:library', contributions: 2,
    retained_items: 2, retained_items_state: 'observed', information_policy_class: 'private' }],
  { execute: true, maxCalls: 1, fetcher: () => { throw new Error('hosted egress must not occur'); } });
  const [receipt] = estate.rows;
  assert.equal(receipt.provider, 'local'); assert.equal(receipt.action, 'none');
  assert.equal(receipt.model_artifact_digest, installedDecisionOwner.artifact_digest);
  assert.equal(estate.summary.local_calls, 1);
  receipts.push({ name: 'external-estate-warm', end_to_end_ms: performance.now() - began, receipt });
  console.log(JSON.stringify({ name: 'external-estate-warm', latency_ms: receipt.latency_ms,
    diagnostic_lane: receipt.diagnostic_lane }));
} catch (error) { failure = error.message; }
finally {
  try { await closeInstalledDecisionEvaluator(); }
  catch (error) { failure = [failure, error.message].filter(Boolean).join('; '); }
}
const witness = { schema: 'bonfyre.trained-clef-witness.v1', observed_at: new Date().toISOString(),
  state: failure ? 'incomplete' : 'complete', ...(failure ? { failure } : {}),
  hardware: { cpu: cpus()[0]?.model, memory_bytes: totalmem(), platform: process.platform, arch: process.arch },
  runtime: installedDecisionOwner, total_ms: performance.now() - started, receipts,
  limitations: ['Five authored integration examples, not held-out production accuracy or calibrated correctness.',
    'Cold measurement includes full artifact hashing and startup; warm measurements include shared projection.',
    'Q4_K_M native runtime is not a BF16 parity witness or an end-to-end speedup comparison.',
    'No authorization, tool invocation, work claim or source admission follows from these answers.'] };
if (process.argv[2]) writeFileSync(process.argv[2], JSON.stringify(witness, null, 2) + '\n');
console.log(JSON.stringify({ state: witness.state, failure, total_ms: witness.total_ms }));
if (failure) process.exitCode = 1;
