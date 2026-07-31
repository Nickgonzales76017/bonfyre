import { createHash, randomUUID } from 'node:crypto';
import { spawn } from 'node:child_process';
import { createReadStream } from 'node:fs';
import { readdir, stat, readFile } from 'node:fs/promises';
import { join, basename, dirname, isAbsolute, resolve, relative } from 'node:path';
import { createNativeGenerationClient } from '../wordpress/native_generation.mjs';
import { createLlamaGenerationClient } from './llama_generation.mjs';

export const TOOL_RECEIPT_SCHEMA = 'bonfyre.native.tool.receipt.v1';
export const BATCH_RECEIPT_SCHEMA = 'bonfyre.native.generation.batch.v1';
export const PROFILE_SCHEMA = 'bonfyre.native.generation.profile.v1';

const CAPABILITY_HINTS = [
  ['qwen-fpq', ['generation', 'local-model', 'code']],
  ['fpqx', ['generation', 'compression', 'local-model']],
  ['fpq', ['generation', 'compression', 'local-model']],
  ['moe', ['generation', 'mixture-of-experts', 'local-model']],
  ['embed', ['embedding', 'multimodal']],
  ['vec', ['vector-search']],
  ['transcribe', ['transcription', 'audio']],
  ['narrate', ['speech', 'audio']],
  ['gen', ['generation']],
  ['model', ['model-control']],
];

function text(value) {
  return typeof value === 'string' ? value.trim() : '';
}

export function discoverGenerationProfiles({ env = process.env } = {}) {
  const raw = text(env.BONFYRE_MODEL_PROFILES);
  if (!raw) return [];
  let parsed;
  try { parsed = JSON.parse(raw); } catch (error) {
    throw new Error(`BONFYRE_MODEL_PROFILES must be valid JSON: ${error.message}`);
  }
  if (!Array.isArray(parsed)) throw new Error('BONFYRE_MODEL_PROFILES must be a JSON array');
  return parsed.map((profile, index) => {
    if (!profile || !text(profile.id) || !text(profile.binary) || !text(profile.modelPack)) {
      throw new Error(`BONFYRE_MODEL_PROFILES[${index}] requires id, binary, and modelPack`);
    }
    if (profile.runtime !== 'llama-cpp' && !text(profile.tokenizer)) {
      throw new Error(`BONFYRE_MODEL_PROFILES[${index}] requires tokenizer unless runtime is llama-cpp`);
    }
    return { ...profile, id: text(profile.id), schema_version: PROFILE_SCHEMA };
  });
}

function capabilitiesFor(name) {
  const lower = name.toLowerCase();
  return [...new Set(CAPABILITY_HINTS.filter(([hint]) => lower.includes(hint)).flatMap(([, caps]) => caps))];
}

async function executable(path) {
  try {
    const info = await stat(path);
    return info.isFile() && (info.mode & 0o111) !== 0;
  } catch {
    return false;
  }
}

export async function discoverNativeTools({ root = process.cwd(), binDir = join(root, 'bin'), commandRoot = join(root, 'cmd') } = {}) {
  const paths = new Map();
  for (const dir of [binDir, commandRoot]) {
    let entries = [];
    try { entries = await readdir(dir, { withFileTypes: true }); } catch { continue; }
    for (const entry of entries) {
      const path = join(dir, entry.name);
      if (entry.isDirectory()) {
        let nested = [];
        try { nested = await readdir(path, { withFileTypes: true }); } catch { continue; }
        for (const child of nested) {
          const childPath = join(path, child.name);
          if (child.name.startsWith('bonfyre-') && await executable(childPath)) paths.set(child.name, childPath);
        }
      } else if (entry.name.startsWith('bonfyre-') && await executable(path)) {
        paths.set(entry.name, path);
      }
    }
  }
  return [...paths.entries()].sort(([a], [b]) => a.localeCompare(b)).map(([name, path]) => ({
    id: name,
    name,
    path,
    source: path.includes(`${join(root, 'bin')}/`) ? 'bin' : 'cmd',
    capabilities: capabilitiesFor(name),
    available: true,
  }));
}

export async function discoverLocalModelProfiles({ modelRoot = '/Users/nickgonzales/BonfyreModels', binary, llamaBinary = process.env.BONFYRE_LLAMA_CPP_BINARY || '/opt/homebrew/bin/llama-cli', tokenizerRoot } = {}) {
  /* Direct discovery is also used outside createGenerationFabric(), where the
   * native-tool inventory is not available. Resolve Bonfyre's built Qwen
   * runtime here so verified .fpq sidecars become executable profiles rather
   * than inert catalog entries. */
  const nativeBinary = binary || process.env.BONFYRE_QWEN_FPQ_BINARY
    || join(process.cwd(), 'bin', 'bonfyre-qwen-fpq');
  const packRoot = join(modelRoot, 'fpq');
  const llamaCpuOnly = ['1', 'true', 'yes'].includes(String(process.env.BONFYRE_LLAMA_CPU_ONLY || '').toLowerCase());
  const tokenizerBase = tokenizerRoot || join(modelRoot, 'tokenizers');
  let entries = [];
  try { entries = await readdir(packRoot, { withFileTypes: true }); } catch { entries = []; }
  const profiles = [];
  for (const entry of entries) {
    if (!entry.isFile() || !/\.fpq2?-pack\.json$/.test(entry.name)) continue;
    const modelPack = join(packRoot, entry.name);
    let manifest;
    try { manifest = JSON.parse(await readFile(modelPack, 'utf8')); } catch { continue; }
    const model = String(manifest.model_repo || entry.name.replace(/\.fpq2?-pack\.json$/, ''))
      .split('/').pop().toLowerCase();
    const tokenizerCandidates = [
      join(tokenizerBase, model, 'tokenizer.json'),
      join(modelRoot, 'small', model, 'tokenizer.json'),
      join(modelRoot, 'raw', model, 'tokenizer.json'),
    ];
    const partsDir = manifest.parts_dir || join(packRoot, entry.name.replace(/\.fpq2?-pack\.json$/, '.parts'));
    const parts = Array.isArray(manifest.parts) ? manifest.parts : [];
    const qualityGate = text(manifest.quality_gate) || 'unverified';
    const partPaths = parts.map((part) => part.path || part.fpq_path || part.fpq_part || part.fpq2_part)
      .filter(Boolean).map((path) => path.startsWith('/') ? path : join(partsDir, path));
    const tokenizerChecks = await Promise.all(tokenizerCandidates.map((path) => stat(path).then(() => path).catch(() => null)));
    const tokenizer = tokenizerChecks.find(Boolean) || tokenizerCandidates[0];
    const checks = await Promise.all([
      executable(nativeBinary),
      stat(tokenizer).then(() => true).catch(() => false),
      stat(partsDir).then(() => true).catch(() => false),
      ...partPaths.map((path) => stat(path).then(() => true).catch(() => false)),
    ]);
    const missing = [];
    if (!checks[0]) missing.push('binary');
    if (!checks[1]) missing.push('tokenizer');
    if (!checks[2] || (partPaths.length && checks.slice(3).some((present) => !present))) missing.push('model-parts');
    if (qualityGate !== 'passed') missing.push(`quality-gate:${qualityGate}`);
    const fpq2 = entry.name.endsWith('.fpq2-pack.json')
      || String(manifest.schema || '').toLowerCase().includes('fpq2')
      || parts.some((part) => text(part.fpq2_part));
    profiles.push({
      id: `${model}--${entry.name.replace(/\.fpq2?-pack\.json$/, '')}`,
      model,
      binary: nativeBinary,
      modelPack,
      tokenizer,
      backend: 'cpu_neon',
      runtime: 'fpq-native',
      compression: fpq2 ? 'fpq2' : 'fpq',
      runtimeEnv: fpq2
        ? { BONFYRE_QWEN_KEEP_PREFILL_PREPARED: '1' }
        : {},
      defaultTimeoutMs: fpq2 ? 900_000 : 120_000,
      quality_gate: qualityGate,
      generation_quality: text(manifest.generation_quality) || null,
      available: missing.length === 0,
      missing,
      pack_schema: manifest.schema || null,
      parameter_count: manifest.parameter_count || manifest.total_parameters || null,
    });
  }
  for (const entry of entries) {
    if (!entry.isFile() || !entry.name.endsWith('.fpq.config.json')) continue;
    const sidecarPath = join(packRoot, entry.name);
    const modelPack = join(packRoot, entry.name.slice(0, -'.config.json'.length));
    let manifest;
    try { manifest = JSON.parse(await readFile(sidecarPath, 'utf8')); } catch { continue; }
    const model = String(manifest.model_repo || manifest.model_id || basename(modelPack, '.fpq'))
      .split('/').pop().toLowerCase();
    const tokenizerCandidates = [
      text(manifest.tokenizer_path),
      join(tokenizerBase, model, 'tokenizer.json'),
      join(modelRoot, 'small', model, 'tokenizer.json'),
      join(modelRoot, 'raw', model, 'tokenizer.json'),
    ].filter(Boolean);
    const tokenizerChecks = await Promise.all(tokenizerCandidates.map((path) => stat(path).then(() => path).catch(() => null)));
    const tokenizer = tokenizerChecks.find(Boolean) || tokenizerCandidates[0] || null;
    const qualityGate = text(manifest.quality_gate) || 'unverified';
    const [binaryReady, modelReady] = await Promise.all([
      executable(nativeBinary),
      stat(modelPack).then((info) => info.isFile()).catch(() => false),
    ]);
    const missing = [];
    if (!binaryReady) missing.push('binary');
    if (!tokenizer) missing.push('tokenizer');
    if (!modelReady) missing.push('model-pack');
    if (qualityGate !== 'passed') missing.push(`quality-gate:${qualityGate}`);
    profiles.push({
      id: `${model}--${basename(modelPack, '.fpq')}`,
      model,
      binary: nativeBinary,
      modelPack,
      tokenizer,
      backend: 'cpu_neon',
      runtime: 'fpq-native',
      compression: text(manifest.compression) || 'fpq-native',
      runtimeEnv: manifest.runtime_env && typeof manifest.runtime_env === 'object' ? manifest.runtime_env : {},
      defaultTimeoutMs: Number(manifest.default_timeout_ms) || 900_000,
      quality_gate: qualityGate,
      generation_quality: text(manifest.generation_quality) || null,
      available: missing.length === 0,
      missing,
      pack_schema: manifest.schema || 'bonfyre.native.fpq.single-file.v1',
      parameter_count: manifest.parameter_count || null,
    });
  }
  const nativeGroups = new Map();
  for (const profile of profiles) {
    const group = nativeGroups.get(profile.model) || [];
    group.push(profile);
    nativeGroups.set(profile.model, group);
  }
  for (const [model, group] of nativeGroups) {
    group.sort((a, b) => Number(b.available) - Number(a.available)
      || Number(b.compression === 'fpq2') - Number(a.compression === 'fpq2')
      || a.modelPack.localeCompare(b.modelPack));
    group.forEach((profile, index) => {
      profile.id = index === 0 ? model : `${model}-${basename(profile.modelPack).replace(/\.fpq2?-pack\.json$/, '')}`;
    });
  }
  const seenIds = new Set(profiles.map((profile) => profile.id));
  const ggufRoot = join(modelRoot, 'gguf');
  const llamaAvailable = await executable(llamaBinary);
  let ggufGroups = [];
  try { ggufGroups = await readdir(ggufRoot, { withFileTypes: true }); } catch { ggufGroups = []; }
  for (const group of ggufGroups) {
    const groupPath = join(ggufRoot, group.name);
    const candidates = group.isDirectory()
      ? await readdir(groupPath, { withFileTypes: true }).catch(() => [])
      : [group];
    for (const candidate of candidates) {
      if (!candidate.isFile() || !candidate.name.endsWith('.gguf')) continue;
      const modelPath = group.isDirectory() ? join(groupPath, candidate.name) : groupPath;
      const id = `llama-${candidate.name.replace(/\.gguf$/, '').toLowerCase().replace(/[^a-z0-9]+/g, '-')}`;
      if (seenIds.has(id)) continue;
      seenIds.add(id);
      profiles.push({
        id,
        model: candidate.name.replace(/\.gguf$/, ''),
        binary: llamaBinary,
        modelPack: modelPath,
        modelPath,
        runtime: 'llama-cpp',
        backend: llamaCpuOnly ? 'cpu' : 'metal',
        ...(llamaCpuOnly ? { device: 'none', gpuLayers: 0, opOffload: false, fit: false } : {}),
        available: llamaAvailable,
        missing: llamaAvailable ? [] : ['llama-cli'],
      });
    }
  }
  return profiles.sort((a, b) => a.id.localeCompare(b.id));
}

function createGenerationClient(profile, spawnImpl) {
  return profile.runtime === 'llama-cpp'
    ? createLlamaGenerationClient({ ...profile, spawnImpl })
    : createNativeGenerationClient({ ...profile, spawnImpl });
}

function fileSha256(path) {
  return new Promise((resolve, reject) => {
    const hash = createHash('sha256');
    const source = createReadStream(path);
    source.on('data', (chunk) => hash.update(chunk));
    source.once('error', reject);
    source.once('end', () => resolve(hash.digest('hex')));
  });
}

function within(root, path) {
  const child = relative(root, path);
  return child === '' || (!child.startsWith('..') && !isAbsolute(child));
}

export async function verifyModelPack({ modelPack, verifyHashes = true, allowedRoot = process.env.BONFYRE_MODEL_ROOT || '/Users/nickgonzales/BonfyreModels', maxParts = 128, maxPartBytes = 4 * 1024 * 1024 * 1024 } = {}) {
  if (!text(modelPack)) throw new Error('modelPack is required');
  const root = resolve(allowedRoot);
  const resolvedPack = resolve(modelPack);
  if (!within(root, resolvedPack)) throw new Error('modelPack must be within the allowed model root');
  const manifest = JSON.parse(await readFile(resolvedPack, 'utf8'));
  const declaredPartsDir = text(manifest.parts_dir);
  const partsDir = resolve(declaredPartsDir ? (isAbsolute(declaredPartsDir) ? declaredPartsDir : join(dirname(resolvedPack), declaredPartsDir)) : dirname(resolvedPack));
  const parts = Array.isArray(manifest.parts) ? manifest.parts : [];
  if (parts.length === 0 || parts.length > maxParts) throw new Error(`model pack must declare 1-${maxParts} parts`);
  const verifiedParts = await Promise.all(parts.map(async (part, index) => {
    const referenced = part.path || part.fpq_path || part.fpq_part || part.fpq2_part;
    const path = resolve(referenced?.startsWith('/') ? referenced : join(partsDir, referenced || ''));
    const expectedSha256 = text(part.sha256);
    try {
      if (!within(root, path)) throw new Error('part is outside the allowed model root');
      const info = await stat(path);
      if (!info.isFile()) throw new Error('not a file');
      if (info.size > maxPartBytes) throw new Error('part exceeds verification size limit');
      const actualSha256 = verifyHashes ? await fileSha256(path) : null;
      const hashRecorded = Boolean(expectedSha256);
      const verified = verifyHashes ? hashRecorded && actualSha256 === expectedSha256 : true;
      return {
        index,
        path,
        present: true,
        bytes: info.size,
        expected_sha256: expectedSha256 || null,
        actual_sha256: actualSha256,
        hash_recorded: hashRecorded,
        verified,
      };
    } catch {
      return { index, path, present: false, bytes: 0, expected_sha256: expectedSha256 || null, actual_sha256: null, verified: false };
    }
  }));
  const missing = verifiedParts.filter((part) => !part.present).length;
  const hashMismatches = verifiedParts.filter((part) => part.present && part.hash_recorded && !part.verified).length;
  const unverified = verifiedParts.filter((part) => part.present && !part.verified).length;
  const body = {
    schema_version: 'bonfyre.model.recovery.verify.v1',
    model_pack: resolvedPack,
    model_repo: manifest.model_repo || null,
    part_count: verifiedParts.length,
    missing_parts: missing,
    hash_mismatches: hashMismatches,
    unverified_parts: unverified,
    verification_required: verifyHashes,
    ready: verifyHashes && missing === 0 && unverified === 0 && verifiedParts.length > 0,
    parts: verifiedParts,
  };
  return { ...body, receipt_sha256: createHash('sha256').update(JSON.stringify(body)).digest('hex') };
}

function runTool(path, args, { spawnImpl, input = '', timeoutMs = 30_000, maxOutputBytes = 1_000_000 } = {}) {
  return new Promise((resolve, reject) => {
    const child = spawnImpl(path, args, { stdio: ['pipe', 'pipe', 'pipe'] });
    let stdout = '';
    let stderr = '';
    let timedOut = false;
    let settled = false;
    const append = (target, chunk) => target.length >= maxOutputBytes ? target : `${target}${chunk.toString()}`.slice(0, maxOutputBytes);
    child.stdout?.on('data', (chunk) => { stdout = append(stdout, chunk); });
    child.stderr?.on('data', (chunk) => { stderr = append(stderr, chunk); });
    const timer = setTimeout(() => {
      timedOut = true;
      child.kill('SIGKILL');
      settled = true;
      resolve({ code: null, signal: 'SIGKILL', stdout, stderr, timed_out: true });
    }, timeoutMs);
    child.once('error', (error) => { if (settled) return; settled = true; clearTimeout(timer); reject(error); });
    child.once('close', (code, signal) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      resolve({ code, signal, stdout, stderr, timed_out: timedOut });
    });
    if (input && child.stdin) child.stdin.end(input);
    else child.stdin?.end();
  });
}

export async function probeNativeTools({ tools, root = process.cwd(), timeoutMs = 3_000, maxConcurrency = 4, spawnImpl = spawn } = {}) {
  const catalog = tools || await discoverNativeTools({ root });
  const results = [];
  let cursor = 0;
  async function worker() {
    while (cursor < catalog.length) {
      const index = cursor++;
      const tool = catalog[index];
      const started = Date.now();
      try {
        const execution = await runTool(tool.path, ['--help'], { spawnImpl, timeoutMs, maxOutputBytes: 12_000 });
        results[index] = {
          ...tool,
          health: execution.timed_out ? 'timed_out' : execution.code === 0 ? 'passed' : 'failed',
          exit_code: execution.code,
          stderr: execution.stderr.slice(-2_000),
          duration_ms: Date.now() - started,
        };
      } catch (error) {
        results[index] = { ...tool, health: 'error', error: error.message, duration_ms: Date.now() - started };
      }
    }
  }
  await Promise.all(Array.from({ length: Math.min(maxConcurrency, Math.max(1, catalog.length)) }, worker));
  return {
    schema_version: 'bonfyre.native.tool.health.v1',
    count: results.length,
    passed: results.filter((tool) => tool.health === 'passed').length,
    failed: results.filter((tool) => tool.health !== 'passed').length,
    tools: results,
  };
}

export function toLedgerRecord(receipt) {
  return {
    schema_version: TOOL_RECEIPT_SCHEMA,
    event: 'native.tool.completed',
    receipt_id: receipt.receipt_id,
    tool_id: receipt.tool_id,
    profile_id: receipt.profile_id || null,
    status: receipt.status,
    duration_ms: receipt.duration_ms,
    output_sha256: receipt.output_sha256,
    created_at: receipt.created_at,
  };
}

export function toAnalyticsEvent(receipt) {
  return {
    schema_version: 'bonfyre.native.analytics.event.v1',
    event_name: 'native_generation.completed',
    dimensions: { tool_id: receipt.tool_id, profile_id: receipt.profile_id || 'tool' },
    measures: { duration_ms: receipt.duration_ms, output_bytes: receipt.output_bytes, success: receipt.status === 'passed' ? 1 : 0 },
    receipt_id: receipt.receipt_id,
    created_at: receipt.created_at,
  };
}

function toBatchLedgerRecord(batch) {
  return {
    schema_version: BATCH_RECEIPT_SCHEMA,
    event: 'native.generation.batch.completed',
    batch_id: batch.batch_id,
    count: batch.count,
    passed: batch.passed,
    failed: batch.failed,
    created_at: batch.created_at,
  };
}

function toBatchAnalyticsEvent(batch) {
  return {
    schema_version: 'bonfyre.native.analytics.event.v1',
    event_name: 'native_generation.batch_completed',
    measures: { count: batch.count, passed: batch.passed, failed: batch.failed },
    batch_id: batch.batch_id,
    created_at: batch.created_at,
  };
}

export function createEstateGenerationFabric({ root = process.cwd(), modelRoot, modelBinary, llamaBinary, tools, profiles = discoverGenerationProfiles(), spawnImpl = spawn, maxConcurrency = 2 } = {}) {
  const profileClients = new Map(profiles.map((profile) => [profile.id, {
    profile,
    client: createGenerationClient(profile, spawnImpl),
  }]));

  async function inventory() {
    return tools || discoverNativeTools({ root });
  }

  async function modelCatalog() {
    const nativeTools = await inventory();
    const nativeGenerator = modelBinary
      || profiles.find((profile) => profile.binary)?.binary
      || nativeTools.find((tool) => tool.id === 'bonfyre-qwen-fpq' || tool.name === 'bonfyre-qwen-fpq')?.path;
    return discoverLocalModelProfiles({
      modelRoot: modelRoot || process.env.BONFYRE_MODEL_ROOT || '/Users/nickgonzales/BonfyreModels',
      binary: nativeGenerator,
      llamaBinary,
    });
  }

  async function verifyModelPackRecovery(options = {}) {
    return verifyModelPack({ ...options, verifyHashes: true, allowedRoot: modelRoot || process.env.BONFYRE_MODEL_ROOT || '/Users/nickgonzales/BonfyreModels' });
  }

  function profileInventory() {
    return [...profileClients.values()].map(({ profile }) => ({
      schema_version: PROFILE_SCHEMA,
      id: profile.id,
      tool_id: profile.tool_id || basename(profile.binary),
      backend: profile.backend || 'cpu_neon',
      runtime: profile.runtime || 'fpq-native',
      compression: profile.compression || null,
      runtime_env: profile.runtimeEnv || {},
      default_timeout_ms: profile.defaultTimeoutMs || 120_000,
      model: profile.model || basename(profile.modelPack),
      model_pack: profile.modelPack,
      tokenizer: profile.tokenizer,
      available: profile.available !== false,
      missing: profile.missing || [],
    }));
  }

  async function invoke({ toolId, args = [], input = '', timeoutMs = 30_000 } = {}) {
    const catalog = await inventory();
    const tool = catalog.find((candidate) => candidate.id === toolId || candidate.name === toolId);
    if (!tool) throw new Error(`Unknown native tool: ${toolId}`);
    const started = Date.now();
    const execution = await runTool(tool.path, args.map(String), { spawnImpl, input, timeoutMs });
    const output = execution.stdout;
    const status = execution.timed_out ? 'timed_out' : execution.code === 0 ? 'passed' : 'failed';
    const receipt = {
      schema_version: TOOL_RECEIPT_SCHEMA,
      receipt_id: randomUUID(),
      tool_id: tool.id,
      status,
      exit_code: execution.code,
      signal: execution.signal,
      timed_out: execution.timed_out,
      stdout: output,
      stderr: execution.stderr,
      output_sha256: createHash('sha256').update(output).digest('hex'),
      output_bytes: Buffer.byteLength(output),
      duration_ms: Date.now() - started,
      created_at: new Date().toISOString(),
    };
    return { ...receipt, ledger: toLedgerRecord(receipt), analytics: toAnalyticsEvent(receipt) };
  }

  async function generate({ profileId, prompt, maxNewTokens = 20, greedy = true, env = {}, timeoutMs } = {}) {
    let selected = profileClients.get(profileId);
    if (!selected) {
      const discovered = (await modelCatalog()).find((profile) => profile.id === profileId);
      if (discovered) {
        selected = { profile: discovered, client: createGenerationClient(discovered, spawnImpl) };
        profileClients.set(profileId, selected);
      }
    }
    if (!selected) throw new Error(`Unknown generation profile: ${profileId}`);
    if (selected.profile.available === false) throw new Error(`Generation profile is unavailable: ${(selected.profile.missing || []).join(', ')}`);
    const started = Date.now();
    const result = await selected.client.generate({ prompt, maxNewTokens, greedy, env, timeoutMs });
    const receipt = {
      ...result,
      receipt_id: randomUUID(),
      tool_id: selected.profile.tool_id || basename(selected.profile.binary),
      profile_id: profileId,
      status: 'passed',
      duration_ms: Date.now() - started,
      output_bytes: Buffer.byteLength(result.output),
    };
    return { ...receipt, ledger: toLedgerRecord(receipt), analytics: toAnalyticsEvent(receipt) };
  }

  async function batchGenerate(requests = []) {
    if (!Array.isArray(requests) || requests.length > 32) throw new Error('batch is limited to 32 requests');
    const results = [];
    let cursor = 0;
    async function worker() {
      while (cursor < requests.length) {
        const index = cursor++;
        try { results[index] = await generate(requests[index]); }
        catch (error) { results[index] = { status: 'failed', error: error.message, profile_id: requests[index]?.profileId || null }; }
      }
    }
    await Promise.all(Array.from({ length: Math.min(maxConcurrency, Math.max(1, requests.length)) }, worker));
    const batch = {
      schema_version: BATCH_RECEIPT_SCHEMA,
      batch_id: randomUUID(),
      count: results.length,
      passed: results.filter((result) => result.status === 'passed').length,
      failed: results.filter((result) => result.status !== 'passed').length,
      results,
      created_at: new Date().toISOString(),
    };
    return { ...batch, ledger: toBatchLedgerRecord(batch), analytics: toBatchAnalyticsEvent(batch) };
  }

  return { inventory, modelCatalog, verifyModelPackRecovery, probeNativeTools: async (options = {}) => probeNativeTools({ ...options, root, ...(tools ? { tools } : {}), spawnImpl }), profileInventory, invoke, generate, batchGenerate };
}
