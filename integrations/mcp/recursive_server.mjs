#!/usr/bin/env bun
// Artifact/token/IR orchestration over the existing native token engine.
import { readFileSync, realpathSync } from 'node:fs';
import { homedir } from 'node:os';
import { join, resolve } from 'node:path';
import { randomUUID } from 'node:crypto';
import { createMcpTransport } from './http_boundary.mjs';
import { digestValue } from './canonical_json.mjs';

const catalog = JSON.parse(readFileSync(new URL('./recursive_catalog.json', import.meta.url), 'utf8'));
export const RECURSIVE_TOOLS = catalog.tools;
export const RECURSIVE_PROMPTS = catalog.prompts;
export const VERSION = '5.1.0-recursive-r1';
const MAX_BYTES = 30_000_000;
const instructions = 'This is the v5 recursive substrate beneath the Agent Hypervisor. Canonicalize and tokenize before agent execution, compute affected closures, compile the seven IRs, preserve candidate-only authority, and verify receipt/flux chains before resolved claims.';

export function recursiveConfig(env = process.env, userHome = homedir()) {
  const token = readFileSync(env.BONFYRE_MCP_TOKEN_FILE || join(userHome, '.bonfyre/mcp/token'), 'utf8').trim();
  const port = Number(env.BONFYRE_RECURSIVE_PORT || 3055);
  if (!token) throw new Error('Bonfyre MCP token missing');
  if (!Number.isSafeInteger(port) || port < 1 || port > 65535) throw new Error('invalid recursive port');
  return { token, port, host: env.BONFYRE_RECURSIVE_HOST || '127.0.0.1',
    home: resolve(env.BONFYRE_RECURSIVE_HOME || join(userHome, '.bonfyre/recursive-substrate-v5')),
    tokenBinary: realpathSync(env.BONFYRE_TOKEN_FABRIC_BIN || join(userHome, '.local/bin/bonfyre-token-fabric')) };
}

export async function createRecursiveServer({ token, home, tokenBinary, store, run, maxConcurrent = 16 } = {}) {
  store ||= (await import('./recursive_store.mjs')).createRecursiveStore({ home, tokenBinary, ...(run ? { run } : {}) });
  const verify = () => {
    const receipt_chain = store.verifyReceipts(), flux_chains = store.verifyFlux();
    return { verified: receipt_chain.verified && flux_chains.verified, receipt_chain, flux_chains };
  };
  async function missionAttempt(kind, args, operation) {
    const mission = { ...args.mission }, missionId = mission.id || (kind === 'bootstrap' ? randomUUID().replaceAll('-', '') : null);
    if (typeof missionId !== 'string' || !missionId || missionId.includes('\0')) throw new Error('mission.id is required for finalization and must be a nonempty string');
    // Record intent before the first constituent write. A failed composite is
    // not rolled back across native files and SQLite, and is never auto-replayed.
    const intent = store.writeReceipt({ kind: `mission.${kind}.attempt`, subject_kind: 'mission', subject_id: missionId,
      state: 'started', payload: { arguments_digest: digestValue(args), external_effects: false } });
    try {
      const result = await operation(mission, missionId);
      const receipt = store.writeReceipt({ kind: `mission.${kind}.complete`, subject_kind: 'mission', subject_id: missionId,
        state: result.state, payload: { attempt_receipt_id: intent.receipt_id, result_digest: digestValue(result), external_effects: false } });
      return { ...result, attempt_receipt: intent, receipt };
    } catch (error) {
      try { store.writeReceipt({ kind: `mission.${kind}.uncertain`, subject_kind: 'mission', subject_id: missionId,
        state: 'uncertain', payload: { attempt_receipt_id: intent.receipt_id, reason: 'composite_not_completed', reconcile_before_retry: true } }); }
      catch { /* The durable started receipt still forbids an inferred completion. */ }
      throw new Error(`mission ${kind} did not complete; reconcile attempt ${intent.receipt_id} before retry: ${error.message}`);
    }
  }
  async function bootstrap(args) {
    return missionAttempt('bootstrap', args, async (mission, missionId) => {
      const tokenizer = args.tokenizer_id ?? 'lexeme-v1';
      const spec = store.ingestArtifact({ path: args.spec_path, kind: 'mission-spec', schema_name: 'bonfyre.mission.v5', provenance: { mission_id: missionId } });
      const specTokens = await store.compileTokenArtifact(spec.artifact_id, tokenizer);
      // Newly compiled native manifests use artifact_id for the token body;
      // cached SQL records use id and reserve artifact_id for their source.
      const artifacts = [spec.artifact_id], tokens = [specTokens.id ?? specTokens.artifact_id];
      let context = null, contextTokens = null;
      if (args.context_packet_path) {
        context = store.ingestArtifact({ path: args.context_packet_path, kind: 'context-packet', schema_name: 'bonfyre.context-packet.v5', provenance: { mission_id: missionId } });
        contextTokens = await store.compileTokenArtifact(context.artifact_id, tokenizer);
        artifacts.push(context.artifact_id); tokens.push(contextTokens.id ?? contextTokens.artifact_id);
      }
      for (const [targetKind, ids, edgeKind] of [['artifact', artifacts, 'uses'], ['token_artifact', tokens, 'tokenized_as']])
        for (const targetId of ids) store.registerDependency({ source_kind: 'mission', source_id: missionId, target_kind: targetKind, target_id: targetId, edge_kind: edgeKind });
      const irBundle = store.compileIrBundle({ subject_kind: 'mission', subject_id: missionId, mission, artifact_ids: artifacts, token_artifact_ids: tokens });
      const closure = store.affectedClosure({ root_kind: 'mission', root_id: missionId, max_depth: 12 });
      const event = store.appendFlux({ stream: 'mission', kind: 'mission.recursive_bootstrapped', subject_kind: 'mission', subject_id: missionId,
        payload: { artifacts, token_artifacts: tokens, ir_ids: Object.fromEntries(Object.entries(irBundle.objects).map(([key, value]) => [key, value.id])), closure_id: closure.closure_id } });
      return { state: 'passed', mission_id: missionId, spec_artifact: spec, spec_tokens: specTokens, context_artifact: context, context_tokens: contextTokens,
        ir_bundle: irBundle, affected_closure: closure, flux_event: event };
    });
  }
  async function finalize(args) {
    return missionAttempt('finalize', args, async (_mission, missionId) => {
      const graph = store.graphProjection({ graph_kind: 'MissionGraph', root_kind: 'mission', root_id: missionId, max_depth: 16 });
      const status = await store.status();
      const image = store.compileRuntimeImage({ mission_id: missionId, active_bfp_root: args.source_bfp_root || null,
        graph_roots: { MissionGraph: graph.digest }, token_roots: {}, provider_routes: args.provider_routes ?? {}, session_pointers: args.session_pointers ?? [],
        authority_state: { effect_class: 'CANDIDATE_WRITE', zig_authority_required: true, external_effects: false }, placement: { local: 'active', oracle: 'candidate' } });
      const bfp = store.bfpSuccessorPlan({ source_root: args.source_bfp_root || null, target_root: args.target_bfp_root || image.digest,
        changed_sections: args.changed_sections?.length ? args.changed_sections : ['mission', 'ir', 'proof'], logical_delta_ids: [],
        rollback_root: args.source_bfp_root || null, destination: { node: 'oracle', activation: 'not_authorized' } });
      const proof = { receipts: store.verifyReceipts(), flux: store.verifyFlux() };
      return { state: proof.receipts.verified && proof.flux.verified ? 'passed' : 'failed', mission_id: missionId,
        graph_projection: graph, runtime_image: image, bfp_successor_candidate: bfp, proof, status };
    });
  }
  const dispatch = {
    recursive_status: () => store.status(),
    tokenizer_list: async () => ({ tokenizers: (await store.status()).tokenizers }),
    artifact_ingest: args => store.ingestArtifact(args),
    artifact_get: args => store.artifact(args.artifact_id),
    token_artifact_compile: args => store.compileTokenArtifact(args.artifact_id, args.tokenizer_id ?? 'lexeme-v1'),
    token_artifact_get: args => store.tokenArtifact(args.token_artifact_id),
    token_delta_derive: args => store.deriveTokenDelta(args.source_token_artifact_id, args.target_token_artifact_id),
    flux_append: args => store.appendFlux(args),
    delta_record: args => store.recordDelta(args),
    dependency_register: args => store.registerDependency(args),
    affected_closure_compile: args => store.affectedClosure(args),
    ir_compile: args => store.compileIr(args),
    ir_bundle_compile: args => store.compileIrBundle(args),
    runtime_image_compile: args => store.compileRuntimeImage(args),
    provider_token_bid_compile: args => store.compileProviderBid(args),
    lambda_motif_register: args => store.registerMotif(args),
    lambda_motif_match: args => store.matchMotifs(args.structure),
    graph_projection_compile: args => store.graphProjection(args),
    bfp_successor_plan: args => store.bfpSuccessorPlan(args),
    recursive_mission_bootstrap: bootstrap,
    recursive_mission_finalize: finalize,
    substrate_replay_verify: verify,
  };
  const getPrompt = (name, args) => ({ description: name, messages: [{ role: 'user', content: { type: 'text', text: name === 'recursive-mission'
    ? `Compile this goal through v5 mission_compile, then call recursive_mission_bootstrap before agent execution and recursive_mission_finalize afterward: ${args.goal}`
    : `Call artifact_ingest for this path, then token_artifact_compile with lexeme-v1: ${args.path}` } }] });
  const transport = createMcpTransport({ token, label: 'recursive', service: 'bonfyre-recursive-substrate-v5', version: VERSION,
    instructions, tools: RECURSIVE_TOOLS, dispatch, statusTool: 'recursive_status', prompts: RECURSIVE_PROMPTS, getPrompt,
    serialize: ['affected_closure_compile'], maxBytes: MAX_BYTES, maxConcurrent,
    health: async () => ({ token_binary: (await store.status()).token_binary_status.exit_code === 0 }) });
  return { ...transport, close: () => store.close?.() };
}

if (import.meta.main) {
  const config = recursiveConfig(), owner = await createRecursiveServer(config);
  const server = Bun.serve({ hostname: config.host, port: config.port, fetch: owner.handle, maxRequestBodySize: MAX_BYTES, idleTimeout: 0 });
  console.log(`recursive MCP listening on ${server.hostname}:${server.port} runtime=bun`);
}
