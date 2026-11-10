#!/usr/bin/env bun
// A ContextCut is a disposable representation, never evidence or authority.
import { Database } from 'bun:sqlite';
import { createHash, randomUUID } from 'node:crypto';
import { closeSync, existsSync, fsyncSync, mkdirSync, openSync, readFileSync, renameSync, statSync, unlinkSync, writeFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';

const materialKeys = ['body_path', 'candidate_identity', 'content_sha256', 'evidence_ref', 'information_policy_class', 'receipt_digest', 'temporal_frontier'];
const required = ['address-db', 'material-manifest', 'tokenizer', 'tokenizer-passport', 'gigatoken-bin', 'token-cache-db', 'information-policy-class', 'purpose', 'out', 'admission-bin', 'admission-db', 'admission-plane', 'admission-volume', 'token-budget', 'admission-estimated-bytes'];
const optional = ['rank-db', 'provider-db', 'run-id', 'provider-run-id', 'provider-query-id', 'provider-exact-owner-evidence', 'admission-free-bytes', 'max-candidates'];
const accepted = new Set([...required, ...optional]);
const digest = bytes => createHash('sha256').update(bytes).digest('hex');
const tagged = bytes => `sha256:${digest(bytes)}`;
const strictPath = path => { const absolute = resolve(path); if (!existsSync(absolute)) throw new Error(`missing path: ${absolute}`); return absolute; };
const jsonLines = path => new TextDecoder('utf-8', { fatal: true }).decode(readFileSync(path)).split('\n').map((line, i) => ({ line, number: i + 1 })).filter(item => item.line.trim());
const sameTuple = (row, tuple) => row && tuple.every((value, index) => row[index] === value);

function argsOf(argv) {
  if (argv.shift() !== 'compile') throw new Error('usage: context_cut.mjs compile [options]');
  const args = {};
  for (let index = 0; index < argv.length; index++) {
    const key = argv[index];
    if (key === '--shadow-diagnostic') { args[key.slice(2)] = true; continue; }
    if (!key.startsWith('--') || !accepted.has(key.slice(2)) || !argv[index + 1] || argv[index + 1].startsWith('--') || key.slice(2) in args) throw new Error(`invalid option: ${key}`);
    args[key.slice(2)] = argv[++index];
  }
  for (const key of required) if (!args[key]) throw new Error(`--${key} is required`);
  if (!args.purpose.trim() || Buffer.byteLength(args.purpose, 'utf8') > 2048)
    throw new Error('--purpose must be nonempty and at most 2048 bytes');
  if (!!args['rank-db'] === !!args['provider-db']) throw new Error('exactly one rank frontier is required');
  if (args['rank-db'] && !args['run-id']) throw new Error('--run-id is required with --rank-db');
  if (args['provider-db'] && (!args['provider-run-id'] || !args['provider-query-id'] || !args['provider-exact-owner-evidence'] || !args['shadow-diagnostic'])) throw new Error('provider frontier requires exact-owner evidence and --shadow-diagnostic');
  for (const key of ['token-budget', 'admission-estimated-bytes', 'max-candidates']) {
    args[key] = Number(args[key] ?? 32);
    if (!Number.isSafeInteger(args[key]) || args[key] < 1) throw new Error(`${key} must be positive`);
  }
  if (args['admission-free-bytes'] !== undefined && (!Number.isSafeInteger(Number(args['admission-free-bytes'])) || Number(args['admission-free-bytes']) < 0)) throw new Error('admission-free-bytes must be nonnegative');
  return args;
}

function readMaterials(path, policy) {
  const materials = new Map();
  for (const { line, number } of jsonLines(path)) {
    const row = JSON.parse(line);
    if (Object.keys(row).sort().join('\0') !== materialKeys.join('\0') || materialKeys.some(key => typeof row[key] !== 'string' || !row[key])) throw new Error(`material line ${number} has unknown, missing, or empty fields`);
    if (row.information_policy_class !== policy) throw new Error(`material line ${number} violates requested information policy`);
    row.body_path = strictPath(row.body_path);
    const actual = digest(readFileSync(row.body_path));
    if (actual !== row.content_sha256.replace(/^sha256:/, '')) throw new Error(`material line ${number} body digest does not match its ForeignTwin receipt`);
    row.content_sha256 = actual;
    if (materials.has(row.candidate_identity)) throw new Error(`material manifest repeats candidate ${row.candidate_identity}`);
    materials.set(row.candidate_identity, row);
  }
  if (!materials.size) throw new Error('material manifest is empty');
  return materials;
}

function readFrontier(args) {
  const address = new Database(strictPath(args['address-db']), { readonly: true });
  try {
    if (args['rank-db']) {
      const rank = new Database(strictPath(args['rank-db']), { readonly: true });
      try {
        const run = rank.query('SELECT gate_state,candidate_count FROM bitnet_rank_run WHERE run_id=?').get(args['run-id']);
        if (!run) throw new Error('unknown BitNet rank run');
        if (run.gate_state !== 'ADMIT' && !(args['shadow-diagnostic'] && run.gate_state === 'SHADOW')) throw new Error('a SHADOW/DEFER BitNet run may not feed executable context');
        const rows = rank.query('SELECT rank,candidate_identity,candidate_kind,owner_ref,exact_verifier,lowerer_ref FROM ranked_candidate_envelope WHERE run_id=? ORDER BY rank').all(args['run-id']);
        if (rows.length !== run.candidate_count) throw new Error('ranked candidate envelope is incomplete');
        const ranked = rows.map(row => ({ rank: row.rank, identity: row.candidate_identity, candidate_kind: row.candidate_kind, owner_ref: row.owner_ref, exact_verifier: row.exact_verifier, lowerer_ref: row.lowerer_ref }));
        const current = address.query('SELECT candidate_kind,owner_ref,exact_verifier,lowerer_ref FROM candidate_advertisement WHERE identity=?');
        for (const candidate of ranked) {
          const row = current.get(candidate.identity);
          if (!row || !sameTuple([row.candidate_kind, row.owner_ref, row.exact_verifier, row.lowerer_ref], [candidate.candidate_kind, candidate.owner_ref, candidate.exact_verifier, candidate.lowerer_ref])) throw new Error(`AddressPlane advertisement drifted for ${candidate.identity}`);
        }
        return { state: run.gate_state, size: run.candidate_count, ranked, identity: { kind: 'bitnet_rank', run_id: args['run-id'] } };
      } finally { rank.close(); }
    }
    const provider = new Database(strictPath(args['provider-db']), { readonly: true });
    try {
      const run = provider.query("SELECT representation_id FROM provider_embedding_run WHERE run_id=? AND status='complete' AND requested_count>0 AND stored_count=requested_count AND failed_count=0 AND dimensions>0").get(args['provider-run-id']);
      const query = provider.query('SELECT candidate_count,top_k FROM provider_embedding_query WHERE query_id=? AND run_id=? AND candidate_count>0 AND top_k>0').get(args['provider-query-id'], args['provider-run-id']);
      if (!run || !query) throw new Error('provider frontier requires a complete embedding run and matching rank query');
      const rows = provider.query('SELECT rank,subject_id FROM provider_embedding_rank WHERE query_id=? ORDER BY rank').all(args['provider-query-id']);
      if (rows.length !== query.top_k || rows.some((row, i) => row.rank !== i + 1 || typeof row.subject_id !== 'string' || !row.subject_id)) throw new Error('provider rank frontier is incomplete or non-deterministically ordered');
      const current = address.query('SELECT candidate_kind,owner_ref,exact_verifier,lowerer_ref FROM candidate_advertisement WHERE identity=?');
      const ranked = rows.map(row => {
        const owner = current.get(row.subject_id);
        if (!owner || owner.candidate_kind !== 'external_occurrence' || owner.exact_verifier !== 'ForeignTwin:admission') throw new Error(`provider handle no longer names an exact ForeignTwin external occurrence: ${row.subject_id}`);
        return { rank: row.rank, identity: row.subject_id, representation_id: run.representation_id, candidate_kind: owner.candidate_kind, owner_ref: owner.owner_ref, exact_verifier: owner.exact_verifier, lowerer_ref: owner.lowerer_ref };
      });
      return { state: 'SHADOW', size: query.candidate_count, ranked, identity: { kind: 'provider_embedding_rank', run_id: args['provider-run-id'], query_id: args['provider-query-id'] } };
    } finally { provider.close(); }
  } finally { address.close(); }
}

function checkProviderEvidence(path, materials, runId, queryId) {
  const evidence = new Map(), identities = new Set();
  for (const { line, number } of jsonLines(path)) {
    const row = JSON.parse(line);
    if (row.schema !== 'bonfyre-provider-exact-owner-evidence.v1' || row.run_id !== runId || row.query_id !== queryId) continue;
    if (row.query_state !== 'queried' || row.candidate_kind !== 'external_occurrence' || row.exact_verifier_ref !== 'ForeignTwin:admission' || !['observed', 'unobserved'].includes(row.outcome)) throw new Error(`provider exact-owner evidence line ${number} is invalid`);
    if (row.outcome === 'unobserved') { if (row.receipt_id !== null) throw new Error(`unobserved evidence line ${number} has a receipt`); continue; }
    for (const key of ['candidate_identity', 'receipt_id', 'source_digest', 'owner_ref', 'lowerer_ref', 'representation_id']) if (typeof row[key] !== 'string' || !row[key]) throw new Error(`provider exact-owner evidence line ${number} is incomplete`);
    const hash = digest(Buffer.from(line));
    if (evidence.has(hash) || identities.has(row.candidate_identity)) throw new Error('provider exact-owner evidence repeats a digest or candidate');
    evidence.set(hash, row); identities.add(row.candidate_identity);
  }
  for (const [identity, material] of materials) {
    const row = evidence.get(material.evidence_ref.replace(/^sha256:/, ''));
    if (!row || row.candidate_identity !== identity || row.receipt_id !== material.receipt_digest) throw new Error(`material ${identity} is not bound to the current provider exact-owner ledger`);
  }
}

function admission(args, verb, grant) {
  const command = [strictPath(args['admission-bin']), verb, '--db', resolve(args['admission-db'])];
  if (verb === 'request') {
    command.push('--plane', args['admission-plane'], '--kind', 'gigatoken_context_materialization', '--bytes', String(args['admission-estimated-bytes']), '--volume', args['admission-volume'], '--detail', args['tokenizer-passport']);
    if (args['admission-free-bytes'] !== undefined) command.push('--free-bytes', args['admission-free-bytes']);
  } else command.push('--grant', String(grant));
  const result = Bun.spawnSync(command);
  if (result.exitCode !== 0) throw new Error(`ResourceAdmission ${verb} failed: ${new TextDecoder().decode(result.stderr).trim()}`);
  const receipt = JSON.parse(new TextDecoder().decode(result.stdout));
  if (verb === 'request') {
    if (receipt.verdict !== 'admit' || !Number.isSafeInteger(receipt.grant_id) || receipt.grant_id < 1) throw new Error(`ResourceAdmission refused GigaToken context materialization: ${receipt.reason ?? 'unknown'}`);
    return receipt.grant_id;
  }
  if (receipt.released_grant_id !== grant) throw new Error('ResourceAdmission did not release the GigaToken grant');
}

function tokenBodies(args, byDigest, tokenizerDigest) {
  const cachePath = resolve(args['token-cache-db']);
  mkdirSync(dirname(cachePath), { recursive: true });
  const db = new Database(cachePath);
  db.exec('PRAGMA journal_mode=WAL');
  db.exec('CREATE TABLE IF NOT EXISTS gigatoken_body (content_sha256 TEXT NOT NULL, tokenizer_sha256 TEXT NOT NULL, tokenizer_passport TEXT NOT NULL, token_count INTEGER NOT NULL, token_blob BLOB NOT NULL, created_at INTEGER NOT NULL, PRIMARY KEY(content_sha256,tokenizer_sha256,tokenizer_passport))');
  try {
    const lookup = db.query('SELECT token_count,token_blob FROM gigatoken_body WHERE content_sha256=? AND tokenizer_sha256=? AND tokenizer_passport=?');
    const blobs = new Map(), lengths = new Map(), misses = [];
    let hits = 0, nativeBatches = 0, nativeBodyBytes = 0;
    for (const [cid, path] of byDigest) {
      const row = lookup.get(cid, tokenizerDigest, args['tokenizer-passport']);
      if (row) {
        if (!Number.isSafeInteger(row.token_count) || row.token_count < 1 || row.token_blob.byteLength !== row.token_count * 4) throw new Error('cached GigaToken body has an invalid token shape');
        blobs.set(cid, Buffer.from(row.token_blob)); lengths.set(cid, row.token_count); hits++;
      } else misses.push([cid, path]);
    }
    if (misses.length) {
      nativeBodyBytes = misses.reduce((bytes, [, path]) => bytes + statSync(path).size, 0);
      const result = Bun.spawnSync([strictPath(args['gigatoken-bin']), strictPath(args.tokenizer), ...misses.map(([, path]) => path)], { maxBuffer: 512 * 1024 * 1024 });
      nativeBatches++;
      if (result.exitCode !== 0) throw new Error(`native GigaToken failed: ${new TextDecoder().decode(result.stderr).trim()}`);
      const output = Buffer.from(result.stdout);
      if (output.byteLength < 4 || output.readUInt32LE(0) !== misses.length) throw new Error('native GigaToken returned an incomplete token batch');
      let offset = 4;
      db.transaction(() => {
        const insert = db.query('INSERT INTO gigatoken_body VALUES(?,?,?,?,?,?) ON CONFLICT(content_sha256,tokenizer_sha256,tokenizer_passport) DO UPDATE SET token_count=excluded.token_count,token_blob=excluded.token_blob,created_at=excluded.created_at');
        for (const [cid] of misses) {
          if (offset + 4 > output.byteLength) throw new Error('native GigaToken returned an invalid token frame');
          const count = output.readUInt32LE(offset); offset += 4;
          if (!count || offset + count * 4 > output.byteLength) throw new Error('native GigaToken returned invalid token ids');
          const blob = Buffer.from(output.subarray(offset, offset + count * 4)); offset += count * 4;
          insert.run(cid, tokenizerDigest, args['tokenizer-passport'], count, blob, Math.floor(Date.now() / 1000));
          blobs.set(cid, blob); lengths.set(cid, count);
        }
      })();
      if (offset !== output.byteLength) throw new Error('native GigaToken returned trailing bytes');
    }
    return { blobs, lengths, hits, misses: misses.length, nativeBatches, nativeBodyBytes };
  } finally { db.close(); }
}

function candidateBodies(candidates, materials, known = new Map()) {
  const bodies = new Map();
  for (const candidate of candidates) {
    const material = materials.get(candidate.identity);
    if (material && !known.has(material.content_sha256) && !bodies.has(material.content_sha256))
      bodies.set(material.content_sha256, material.body_path);
  }
  return bodies;
}

function atomicWrite(path, bytes) {
  mkdirSync(dirname(path), { recursive: true });
  const temporary = `${path}.${randomUUID()}.tmp`;
  const descriptor = openSync(temporary, 'wx', 0o600);
  try { writeFileSync(descriptor, bytes); fsyncSync(descriptor); closeSync(descriptor); renameSync(temporary, path); }
  catch (error) { try { closeSync(descriptor); } catch {} try { unlinkSync(temporary); } catch {} throw error; }
}

export function compileCut(args) {
  const tokenizerDigest = digest(readFileSync(strictPath(args.tokenizer)));
  const materials = readMaterials(strictPath(args['material-manifest']), args['information-policy-class']);
  const frontier = readFrontier(args);
  if (args['provider-db']) checkProviderEvidence(strictPath(args['provider-exact-owner-evidence']), materials, args['provider-run-id'], args['provider-query-id']);
  let grant = admission(args, 'request');
  try {
    // The first max-candidates ranked rows are sufficient whenever they fill
    // the cut. Encode the rest only if a missing or over-budget row makes it
    // reachable. This is exact pruning, not a learned relevance judgment.
    const prefix = frontier.ranked.slice(0, args['max-candidates']);
    const tokens = tokenBodies(args, candidateBodies(prefix, materials), tokenizerDigest);
    const selected = [], parts = [], tokenParts = [], tokenSpans = [], consumed = new Set();
    let selectedTokens = 0, omittedNotMaterialized = 0, omittedOverBudget = 0;
    let fallbackScans = 0;
    for (const [index, candidate] of frontier.ranked.entries()) {
      const material = materials.get(candidate.identity);
      if (!material) { omittedNotMaterialized++; continue; }
      if (!tokens.lengths.has(material.content_sha256)) {
        const later = tokenBodies(args, candidateBodies(frontier.ranked.slice(index), materials, tokens.lengths), tokenizerDigest);
        for (const [cid, blob] of later.blobs) tokens.blobs.set(cid, blob);
        for (const [cid, count] of later.lengths) tokens.lengths.set(cid, count);
        tokens.hits += later.hits;
        tokens.misses += later.misses;
        tokens.nativeBatches += later.nativeBatches;
        tokens.nativeBodyBytes += later.nativeBodyBytes;
        fallbackScans++;
      }
      const cid = material.content_sha256, count = tokens.lengths.get(cid);
      const increment = consumed.has(cid) ? 0 : count;
      if (selectedTokens + increment > args['token-budget']) { omittedOverBudget++; continue; }
      if (!consumed.has(cid)) {
        const body = new TextDecoder('utf-8', { fatal: false }).decode(readFileSync(material.body_path));
        parts.push(`<bonfyre-material cid="sha256:${cid}" receipt="${material.receipt_digest}" evidence="${material.evidence_ref}">\n${body}\n</bonfyre-material>\n`);
        tokenSpans.push({ content_sha256: `sha256:${cid}`, offset_tokens: selectedTokens, token_count: count });
        tokenParts.push(tokens.blobs.get(cid));
        consumed.add(cid); selectedTokens += count;
      }
      selected.push({ ...candidate, content_sha256: `sha256:${cid}`, receipt_digest: material.receipt_digest, evidence_ref: material.evidence_ref, temporal_frontier: material.temporal_frontier });
      if (selected.length >= args['max-candidates']) break;
    }
    if (!selected.length) throw new Error('no exactly admitted material fits the context budget');
    const out = resolve(args.out), contextPath = out.replace(/\.[^.\/]+$/, '') + '.context', tokenPath = out.replace(/\.[^.\/]+$/, '') + '.tokens.u32le';
    const context = Buffer.from(parts.join(''), 'utf8'), body = Buffer.concat(tokenParts);
    admission(args, 'release', grant); grant = 0;
    atomicWrite(contextPath, context); atomicWrite(tokenPath, body);
    const diagnostic = frontier.state === 'SHADOW';
    const manifest = {
      schema: 'bonfyre.context-cut.v1', run_id: frontier.identity.run_id, frontier: frontier.identity, rank_gate_state: frontier.state,
      purpose: args.purpose,
      source_rank_frontier: frontier.size, information_policy_class: args['information-policy-class'],
      tokenizer_passport: { identity: args['tokenizer-passport'], tokenizer_sha256: `sha256:${tokenizerDigest}`, implementation: 'GigaToken' },
      context_body: contextPath, context_sha256: tagged(context), token_body: tokenPath, token_sha256: tagged(body), token_encoding: 'u32le', token_spans: tokenSpans,
      token_budget: args['token-budget'], selected_tokens: selectedTokens, selected_candidates: selected, unique_bodies: consumed.size,
      deduplicated_body_references: selected.length - consumed.size,
      token_cache: { identity: 'CID×tokenizer-passport', hits: tokens.hits, misses: tokens.misses,
        native_batches: tokens.nativeBatches, native_body_bytes: tokens.nativeBodyBytes,
        exact_pruned_bodies: new Set([...materials.values()].map(row => row.content_sha256)).size - tokens.lengths.size,
        fallback_scans: fallbackScans },
      resource_admission: { plane: args['admission-plane'], grant_released: true },
      omitted_not_materialized_by_exact_owner: omittedNotMaterialized, omitted_over_budget: omittedOverBudget,
      truth: diagnostic ? 'diagnostic_receipted_context_only' : 'purpose_bound_execution_context_only',
      forbidden_inference: ['a ranked candidate is selected, lawful, proven, or true', 'a retained ForeignTwin body is an admitted institutional fact', 'a context cut contains all evidence needed by a mission', 'a context cut authorizes an external effect', 'a SHADOW diagnostic context may be projected to an executing model, lowerer, or carrier'],
    };
    const materialPath = strictPath(args['material-manifest']);
    const materialDigest = tagged(readFileSync(materialPath));
    manifest.decision_frontier = { schema: 'bonfyre.decision-frontier.v1', state: 'reconstructable',
      owner: 'ContextCut', material_manifest: materialPath,
      material_manifest_sha256: materialDigest, selected_bodies: selected.length,
      recipe_sha256: tagged(Buffer.from(JSON.stringify({ context: manifest.context_sha256,
        materials: materialDigest, purpose: args.purpose,
        candidates: selected.map(candidate => [candidate.identity, candidate.content_sha256,
          candidate.receipt_digest]) }))),
      resident_case_pack_bytes: 0, provider_calls: 0, action: 'none' };
    atomicWrite(out, Buffer.from(JSON.stringify(manifest) + '\n'));
    return { schema: manifest.schema, state: diagnostic ? 'diagnostic' : 'compiled', selected_candidates: selected.length, selected_tokens: selectedTokens, unique_bodies: consumed.size, context_sha256: manifest.context_sha256, truth: manifest.truth };
  } finally { if (grant) admission(args, 'release', grant); }
}

if (import.meta.main) {
  try { console.log(JSON.stringify(compileCut(argsOf(process.argv.slice(2))))); }
  catch (error) { console.error(`context-cut: ${error.message}`); process.exitCode = 1; }
}
