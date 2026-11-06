#!/usr/bin/env bun
// Disposable, real-token-engine proof. No live recursive home, HTTP listener, or provider execution.
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { lstatSync, mkdtempSync, readFileSync, realpathSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { RECURSIVE_TOOLS, createRecursiveServer } from '../integrations/mcp/recursive_server.mjs';
import { createRecursiveStore } from '../integrations/mcp/recursive_store.mjs';

const sha = bytes => createHash('sha256').update(bytes).digest('hex');
const irNames = ['ArtifactIR', 'DeltaIR', 'EquivalenceIR', 'DistributedIR', 'ExecutionPhenotypeIR', 'AuthorityIR', 'ProofIR'];
const expectedTools = RECURSIVE_TOOLS.map(tool => tool.name);

export async function runRecursiveNativeWitness({ tokenBinary } = {}) {
  assert.equal(typeof tokenBinary, 'string', 'explicit --token-binary is required');
  const binary = realpathSync(tokenBinary);
  assert(lstatSync(binary).isFile(), 'token binary must be a regular file');
  const scratch = mkdtempSync(join(tmpdir(), 'bonfyre-recursive-native-'));
  const home = join(scratch, 'substrate');
  let owner;
  try {
    owner = await createRecursiveServer({ token: 'recursive-disposable-fixture-token', home, tokenBinary: binary });
    const invoked = [];
    const call = async (name, args = {}) => {
      const result = await owner.call(name, args);
      invoked.push(name);
      return result;
    };
    const initial = await call('recursive_status');
    assert.equal(initial.token_binary_status.exit_code, 0, 'native token engine status failed');
    const tokenizers = await call('tokenizer_list');
    assert(tokenizers.tokenizers.some(item => item.id === 'lexeme-v1'));

    const sourceText = 'alpha beta gamma\n';
    const targetText = 'alpha brave beta gamma\n';
    const source = await call('artifact_ingest', { text: sourceText, kind: 'witness', classification: 'internal' });
    const target = await call('artifact_ingest', { text: targetText, kind: 'witness', classification: 'internal' });
    assert.notEqual(source.artifact_id, target.artifact_id);
    assert.equal((await call('artifact_get', { artifact_id: source.artifact_id })).canonical_digest, sha(sourceText));
    const sourceTokens = await call('token_artifact_compile', { artifact_id: source.artifact_id, tokenizer_id: 'lexeme-v1' });
    const targetTokens = await call('token_artifact_compile', { artifact_id: target.artifact_id, tokenizer_id: 'lexeme-v1' });
    const sourceTokenRecord = await call('token_artifact_get', { token_artifact_id: sourceTokens.artifact_id });
    const targetTokenRecord = await call('token_artifact_get', { token_artifact_id: targetTokens.artifact_id });
    assert.equal(sourceTokenRecord.tokenizer_id, targetTokenRecord.tokenizer_id);
    const tokenDelta = await call('token_delta_derive', {
      source_token_artifact_id: sourceTokens.artifact_id, target_token_artifact_id: targetTokens.artifact_id,
    });
    assert(Array.isArray(tokenDelta.operations) && tokenDelta.operations.length > 0);
    assert.equal(tokenDelta.delta_record.delta_id.length, 64);
    // The metadata delta references unchanged predecessor chunks and new target
    // chunks. Reassemble exactly those bytes; REPLACE/INSERT are not inferred
    // from the predecessor alone and require their addressed target body.
    const sourceBody = readFileSync(sourceTokenRecord.canonical_path);
    const targetBody = readFileSync(targetTokenRecord.canonical_path);
    const selectedChunks = Array(targetTokenRecord.chunks.length);
    for (const operation of tokenDelta.operations) {
      if (operation.old_index !== undefined && operation.old_index !== null) {
        const oldChunk = sourceTokenRecord.chunks[operation.old_index];
        assert(oldChunk, 'delta references a missing predecessor chunk');
        assert.equal(operation.old_digest, oldChunk.digest);
      }
      if (operation.op === 'DELETE') continue;
      const newChunk = targetTokenRecord.chunks[operation.new_index];
      assert(newChunk, 'delta references a missing target chunk');
      const body = operation.op === 'COPY'
        ? sourceBody.subarray(sourceTokenRecord.chunks[operation.old_index].byte_start, sourceTokenRecord.chunks[operation.old_index].byte_end)
        : targetBody.subarray(newChunk.byte_start, newChunk.byte_end);
      assert.equal(sha(body), newChunk.digest);
      if (operation.new_digest !== undefined) assert.equal(operation.new_digest, newChunk.digest);
      assert.equal(selectedChunks[operation.new_index], undefined, 'delta duplicated a target chunk');
      selectedChunks[operation.new_index] = body;
    }
    assert(selectedChunks.every(Boolean), 'delta omitted a target chunk');
    const reconstructedDigest = sha(Buffer.concat(selectedChunks));
    assert.equal(reconstructedDigest, target.canonical_digest, 'chunk delta did not reconstruct target bytes');

    const flux = await call('flux_append', { stream: 'witness', kind: 'witness.observed', subject_kind: 'artifact',
      subject_id: target.artifact_id, payload: { source: source.artifact_id, target: target.artifact_id } });
    assert.equal(flux.stream, 'witness');
    const logical = await call('delta_record', { kind: 'witness', source_id: source.artifact_id,
      target_id: target.artifact_id, logical_ops: [{ op: 'REPLACE', field: 'text' }],
      physical_alternatives: [{ encoding: 'RAW', eligible: true }], invalidations: [] });
    assert.equal(logical.delta_id.length, 64);
    const missionId = 'fixture-native-mission';
    await call('dependency_register', { source_kind: 'mission', source_id: missionId,
      target_kind: 'artifact', target_id: source.artifact_id, edge_kind: 'uses' });
    const closure = await call('affected_closure_compile', { root_kind: 'mission', root_id: missionId });
    assert(closure.nodes.some(node => node.kind === 'artifact' && node.id === source.artifact_id));
    const individual = await call('ir_compile', { ir_type: 'ArtifactIR', subject_kind: 'mission', subject_id: missionId,
      payload: { source: source.artifact_id } });
    assert.equal(individual.ir_type, 'ArtifactIR');
    const bundle = await call('ir_bundle_compile', { subject_kind: 'mission', subject_id: missionId,
      mission: { id: missionId, goal: 'disposable native proof' }, artifact_ids: [source.artifact_id, target.artifact_id],
      token_artifact_ids: [sourceTokens.artifact_id, targetTokens.artifact_id], delta_ids: [logical.delta_id] });
    assert.deepEqual(Object.keys(bundle.objects), irNames);
    const image = await call('runtime_image_compile', { mission_id: missionId, active_bfp_root: 'root-before',
      graph_roots: {}, token_roots: { lexeme: targetTokens.artifact_id }, provider_routes: {},
      authority_state: { effect_class: 'CANDIDATE_WRITE', external_effects: false }, placement: { local: 'candidate' } });
    assert.equal(image.mission_id, missionId);
    const bid = await call('provider_token_bid_compile', { mission_id: missionId, provider_id: 'fixture-provider',
      tokenizer_id: 'lexeme-v1', required_tokens: 10, reusable_tokens: 3, expected_output_tokens: 2,
      compatible: false });
    assert.equal(bid.uncached_tokens, 7);
    assert.equal(bid.compatible, false);
    const motif = await call('lambda_motif_register', { name: 'fixture-motif', structure: { shape: 'chain', arity: 2 },
      reconstruction_checks: 1 });
    assert.equal(motif.reconstruction_checks, 1);
    const exact = await call('lambda_motif_match', { structure: { shape: 'chain', arity: 2 } });
    assert.equal(exact.match, 'exact');
    const approximate = await call('lambda_motif_match', { structure: { shape: 'chain', arity: 3 } });
    assert.equal(approximate.match, 'approximate');
    const graph = await call('graph_projection_compile', { graph_kind: 'MissionGraph', root_kind: 'mission', root_id: missionId });
    assert.equal(graph.root_id, missionId);
    const plan = await call('bfp_successor_plan', { source_root: 'root-before', target_root: 'root-after',
      changed_sections: ['mission', 'proof'], token_chunk_ids: [], logical_delta_ids: [logical.delta_id],
      destination: { node: 'oracle', activation: 'not_authorized' } });
    assert.equal(plan.destination.activation, 'not_authorized');
    assert.equal(plan.receipt.state, 'candidate');

    const specPath = join(scratch, 'mission-spec.txt');
    writeFileSync(specPath, 'Fixture mission specification.\n');
    const bootstrap = await call('recursive_mission_bootstrap', { mission: { id: missionId, goal: 'disposable native proof' }, spec_path: specPath });
    assert.equal(bootstrap.state, 'passed');
    assert.equal(bootstrap.spec_tokens.artifact_id.length, 64);
    assert.deepEqual(Object.keys(bootstrap.ir_bundle.objects), irNames);
    assert.equal(bootstrap.attempt_receipt.state, 'started');
    const cachedMissionId = 'fixture-native-mission-cached';
    const cachedBootstrap = await call('recursive_mission_bootstrap', {
      mission: { id: cachedMissionId, goal: 'same spec, cached token manifest' }, spec_path: specPath,
    });
    const canonicalTokenId = bootstrap.spec_tokens.id ?? bootstrap.spec_tokens.artifact_id;
    assert.equal(cachedBootstrap.spec_tokens.id, canonicalTokenId, 'second bootstrap must reuse the token artifact');
    assert.notEqual(canonicalTokenId, cachedBootstrap.spec_artifact.artifact_id, 'token and source artifact IDs must not be confused');
    assert(cachedBootstrap.affected_closure.edges.some(edge => edge.target_kind === 'token_artifact' && edge.target_id === canonicalTokenId),
      'cached token dependency must target the token artifact ID');
    assert.deepEqual(cachedBootstrap.ir_bundle.objects.ArtifactIR.payload.token_artifact_ids, [canonicalTokenId]);
    const finalize = await call('recursive_mission_finalize', { mission: { id: missionId },
      source_bfp_root: 'root-before', target_bfp_root: 'root-after' });
    assert.equal(finalize.state, 'passed');
    assert.equal(finalize.bfp_successor_candidate.destination.activation, 'not_authorized');
    assert.equal(finalize.bfp_successor_candidate.receipt.state, 'candidate');
    const proof = await call('substrate_replay_verify');
    assert.equal(proof.verified, true);
    assert.deepEqual(proof.receipt_chain.failures, []);
    assert.deepEqual(proof.flux_chains.failures, []);
    assert.deepEqual(new Set(invoked), new Set(expectedTools));

    owner.close(); owner = undefined;
    const reopened = createRecursiveStore({ home, tokenBinary: binary });
    try {
      const receipts = reopened.verifyReceipts(), fluxChains = reopened.verifyFlux();
      assert.equal(receipts.verified, true, 'receipt chain failed after store reopen');
      assert.equal(fluxChains.verified, true, 'flux chains failed after store reopen');
      assert.equal(receipts.head, proof.receipt_chain.head);
      return {
        schema: 'bonfyre.recursive.native-witness.v1', runtime: 'bun', scope: 'disposable local native token engine; no HTTP, live state, provider, or BFP activation',
        token_binary_sha256: sha(readFileSync(binary)), tool_count: invoked.length, unique_tool_count: new Set(invoked).size,
        tool_names: expectedTools, tokenizer_id: 'lexeme-v1',
        source: { digest: source.canonical_digest, token_artifact_id: sourceTokens.artifact_id, chunk_count: sourceTokenRecord.chunk_count },
        target: { digest: target.canonical_digest, token_artifact_id: targetTokens.artifact_id, chunk_count: targetTokenRecord.chunk_count },
        token_delta: { operation_count: tokenDelta.operations.length, operations: tokenDelta.operations,
          record_id: tokenDelta.delta_record.delta_id, reconstructed_sha256: reconstructedDigest,
          reconstruction_scope: 'COPY from predecessor plus addressed target chunks for INSERT/REPLACE' },
        ir_types: irNames, provider_bid: { uncached_tokens: bid.uncached_tokens, compatible: bid.compatible },
        candidate_only: plan.destination.activation === 'not_authorized' && finalize.bfp_successor_candidate.destination.activation === 'not_authorized',
        mission: { id: missionId, bootstrap_state: bootstrap.state, cached_bootstrap_state: cachedBootstrap.state,
          cached_token_artifact_id: canonicalTokenId, finalize_state: finalize.state },
        receipt_chain: receipts, flux_chains: fluxChains,
      };
    } finally { reopened.close(); }
  } finally {
    try { owner?.close(); }
    finally { rmSync(scratch, { recursive: true, force: true }); }
  }
}

if (import.meta.main) {
  const at = process.argv.indexOf('--token-binary');
  assert(at >= 0 && process.argv[at + 1] && !process.argv[at + 1].startsWith('-'), 'usage: bun tests/recursive_native_witness.mjs --token-binary PATH');
  const witness = await runRecursiveNativeWitness({ tokenBinary: process.argv[at + 1] });
  console.log(JSON.stringify(witness, null, 2));
}
