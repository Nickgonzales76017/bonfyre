import { createHash, randomUUID } from 'node:crypto';
import { spawn } from 'node:child_process';
import { readdir, stat } from 'node:fs/promises';
import { join, basename } from 'node:path';
import { createNativeGenerationClient } from '../wordpress/native_generation.mjs';

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
    if (!profile || !text(profile.id) || !text(profile.binary) || !text(profile.modelPack) || !text(profile.tokenizer)) {
      throw new Error(`BONFYRE_MODEL_PROFILES[${index}] requires id, binary, modelPack, and tokenizer`);
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

function runTool(path, args, { spawnImpl, input = '', timeoutMs = 30_000, maxOutputBytes = 1_000_000 } = {}) {
  return new Promise((resolve, reject) => {
    const child = spawnImpl(path, args, { stdio: ['pipe', 'pipe', 'pipe'] });
    let stdout = '';
    let stderr = '';
    let timedOut = false;
    const append = (target, chunk) => target.length >= maxOutputBytes ? target : `${target}${chunk.toString()}`.slice(0, maxOutputBytes);
    child.stdout?.on('data', (chunk) => { stdout = append(stdout, chunk); });
    child.stderr?.on('data', (chunk) => { stderr = append(stderr, chunk); });
    const timer = setTimeout(() => {
      timedOut = true;
      child.kill('SIGTERM');
    }, timeoutMs);
    child.once('error', (error) => { clearTimeout(timer); reject(error); });
    child.once('close', (code, signal) => {
      clearTimeout(timer);
      resolve({ code, signal, stdout, stderr, timed_out: timedOut });
    });
    if (input && child.stdin) child.stdin.end(input);
    else child.stdin?.end();
  });
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

export function createEstateGenerationFabric({ root = process.cwd(), tools, profiles = discoverGenerationProfiles(), spawnImpl = spawn, maxConcurrency = 2 } = {}) {
  const profileClients = new Map(profiles.map((profile) => [profile.id, {
    profile,
    client: createNativeGenerationClient({ ...profile, spawnImpl }),
  }]));

  async function inventory() {
    return tools || discoverNativeTools({ root });
  }

  function profileInventory() {
    return [...profileClients.values()].map(({ profile }) => ({
      schema_version: PROFILE_SCHEMA,
      id: profile.id,
      tool_id: profile.tool_id || basename(profile.binary),
      backend: profile.backend || 'cpu_neon',
      model: profile.model || basename(profile.modelPack),
      model_pack: profile.modelPack,
      tokenizer: profile.tokenizer,
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

  async function generate({ profileId, prompt, maxNewTokens = 20, greedy = true, env = {} } = {}) {
    const selected = profileClients.get(profileId);
    if (!selected) throw new Error(`Unknown generation profile: ${profileId}`);
    const started = Date.now();
    const result = await selected.client.generate({ prompt, maxNewTokens, greedy, env });
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

  return { inventory, profileInventory, invoke, generate, batchGenerate };
}
