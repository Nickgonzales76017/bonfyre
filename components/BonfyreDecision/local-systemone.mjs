// Owned, local SystemOne transport. It can execute a pinned trained artifact,
// but is not itself a model or a permission to enact a proposed decision.
import { createHash, randomBytes } from 'node:crypto';
import { spawn } from 'node:child_process';
import { constants, readFileSync } from 'node:fs';
import { mkdtemp, open, lstat, unlink, rmdir, writeFile } from 'node:fs/promises';
import { createServer } from 'node:net';
import { tmpdir } from 'node:os';
import { isAbsolute, join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { compileQuestionSchema } from './typed-decision.mjs';
import { validateDecisionResponse } from './decision-plane.mjs';
import nativeLock from './native/clef-runtime.lock.json' with { type: 'json' };

// ggml-org/Clef-Flash-GGUF is derived from the merged, post-trained Cloudflare
// Clef-Flash checkpoint. The GGUF includes decision.* head tensors. A base
// Qwen3.5 GGUF or an ordinary llama-server is NOT a substitute.
export const CLEF_FLASH_Q4 = Object.freeze({
  repository: nativeLock.model.repository,
  revision: nativeLock.model.revision,
  upstream_revision: nativeLock.model.source_revision,
  sha256: nativeLock.model.sha256,
  bytes: nativeLock.model.bytes,
  native_commit: nativeLock.runtime.revision,
  native_binary_sha256: nativeLock.runtime.local_build.sha256,
  native_binary_bytes: nativeLock.runtime.local_build.bytes,
  confidence_semantics: nativeLock.execution.confidence,
  alias: 'bonfyre-clef-flash-q4',
});
const execution = Object.freeze(structuredClone(nativeLock.execution));
if (execution.device !== 'none' || execution.gpu_layers !== 0 ||
    execution.op_offload !== false || execution.kv_offload !== false ||
    execution.repack_weights !== true)
  throw new Error('Clef-Flash requires the proven CPU placement and patched GET_ROWS repack layout');
const adapter_source_sha256 = createHash('sha256').update(readFileSync(new URL(import.meta.url))).digest('hex');
const defaultLimits = Object.freeze({ startupMs: 120000, requestMs: 120000,
  maxRequestBytes: 65536, maxResponseBytes: 262144 });

function effectiveLimits(overrides = {}) {
  const limits = { ...defaultLimits, ...overrides };
  if (!Number.isSafeInteger(limits.startupMs) || limits.startupMs < 1 || limits.startupMs > 300000 ||
      !Number.isSafeInteger(limits.requestMs) || limits.requestMs < 1 || limits.requestMs > 120000 ||
      !Number.isSafeInteger(limits.maxRequestBytes) || limits.maxRequestBytes < 1 || limits.maxRequestBytes > 1048576 ||
      !Number.isSafeInteger(limits.maxResponseBytes) || limits.maxResponseBytes < 1 || limits.maxResponseBytes > 1048576)
    throw new Error('invalid bounded SystemOne execution limits');
  return Object.freeze(limits);
}

export function clefFlashOwnerIdentity(limits = {}) {
  const bounded = effectiveLimits(limits);
  // This small local source hash is available to a lazy planning descriptor;
  // no model-file read, native process or paid provider call occurs here.
  const implementation_digest = createHash('sha256').update(JSON.stringify({
    schema: 'bonfyre.systemone-adapter.v1', adapter_source_sha256,
    execution, limits: bounded,
  })).digest('hex');
  const artifact_digest = createHash('sha256').update(JSON.stringify({
    protocol: 'bonfyre-clef-flash-systemone-v1', native_commit: CLEF_FLASH_Q4.native_commit,
    native_binary_sha256: CLEF_FLASH_Q4.native_binary_sha256,
    model_sha256: CLEF_FLASH_Q4.sha256, model_revision: CLEF_FLASH_Q4.revision,
    implementation_digest,
  })).digest('hex');
  return Object.freeze({ model: CLEF_FLASH_Q4.alias, artifact_digest,
    model_sha256: CLEF_FLASH_Q4.sha256,
    native_binary_sha256: CLEF_FLASH_Q4.native_binary_sha256,
    question_semantics: 'joint_schema', confidence_semantics: CLEF_FLASH_Q4.confidence_semantics,
    deterministic: false, implementation_digest, adapter_source_sha256 });
}

export const CLEF_FLASH_OWNER_IDENTITY = clefFlashOwnerIdentity();

const hexDigest = value => typeof value === 'string' && /^[a-f0-9]{64}$/.test(value);
const plain = value => value !== null && typeof value === 'object' &&
  (Object.getPrototypeOf(value) === Object.prototype || Object.getPrototypeOf(value) === null);
const mediaKeys = new Set(['image', 'images', 'image_url', 'video', 'videos', 'video_url',
  'audio', 'audios', 'audio_url', 'data_url', 'media', 'media_url', 'mime_type']);
const questionKeys = new Set(['type', 'instructions', 'criteria']);

function assertTextOnly(value, budget, rejectMediaKeys, depth = 0) {
  if (depth > 32) throw new Error('SystemOne request nesting exceeds text-only limit');
  const charge = count => {
    budget.remaining -= count;
    if (budget.remaining < 0) throw new Error('SystemOne request exceeds byte budget; no truncation is permitted');
  };
  if (typeof value === 'string') { charge(Buffer.byteLength(value, 'utf8')); return; }
  if (value === null || typeof value === 'boolean') { charge(5); return; }
  if (typeof value === 'number' && Number.isFinite(value)) { charge(24); return; }
  if (Array.isArray(value)) {
    charge(value.length + 2);
    for (const item of value) assertTextOnly(item, budget, rejectMediaKeys, depth + 1);
    return;
  }
  if (!plain(value)) throw new Error('SystemOne accepts JSON text state only; media and binary values are unsupported');
  charge(2);
  for (const [key, item] of Object.entries(value)) {
    if (rejectMediaKeys && mediaKeys.has(key.toLowerCase()))
      throw new Error('SystemOne text-only runtime refuses media fields');
    charge(Buffer.byteLength(key, 'utf8') + 3);
    assertTextOnly(item, budget, rejectMediaKeys, depth + 1);
  }
}

function requestBody(alias, state, questions, maxRequestBytes) {
  if (!plain(state)) throw new Error('SystemOne requires a non-null JSON object state');
  compileQuestionSchema(questions, { max_questions: 64, max_options: 255 });
  const budget = { remaining: maxRequestBytes };
  assertTextOnly(state, budget, true);
  // Schema IDs and option labels are text, not media-bearing state fields.
  // Keep the same JSON/depth/byte validation without reserving those names.
  assertTextOnly(questions, budget, false);
  for (const [id, question] of Object.entries(questions)) {
    if (Object.keys(question).some(key => !questionKeys.has(key)))
      throw new Error(`${id}: SystemOne refuses unsupported question fields`);
    if (typeof question.instructions !== 'string' || !question.instructions.trim())
      throw new Error(`${id}: SystemOne requires nonempty instructions`);
    if (question.type === 'choice' && Object.values(question.criteria).some(value =>
      typeof value !== 'string' || !value.trim()))
      throw new Error(`${id}: SystemOne requires text choice descriptions`);
    if (question.type === 'noul' && question.criteria !== undefined && question.criteria !== null &&
        (!plain(question.criteria) || Object.keys(question.criteria).some(key => key !== 'true' && key !== 'false')))
      throw new Error(`${id}: SystemOne noul criteria must contain only true/false descriptions`);
    if (question.type === 'score' && question.criteria.length > 10)
      throw new Error(`${id}: native SystemOne supports at most ten score levels`);
    if (question.type === 'score' && question.criteria.some(value =>
      typeof value !== 'string' || !value.trim()))
      throw new Error(`${id}: SystemOne requires text score criteria`);
  }
  const request = { model: alias, state, questions };
  const body = JSON.stringify(request);
  if (body.includes('<<clef:')) throw new Error('SystemOne request contains a reserved Clef segment marker');
  if (Buffer.byteLength(body, 'utf8') > maxRequestBytes)
    throw new Error('SystemOne request exceeds byte budget; no truncation is permitted');
  // Preserve exactly the bytes sent: a caller mutating its objects while the
  // native child works must not rewrite the response-validation contract.
  return { request: JSON.parse(body), body };
}

async function pinnedFile(path, expectedSha256, expectedBytes, signal) {
  if (!isAbsolute(path) || !hexDigest(expectedSha256))
    throw new Error('SystemOne requires an absolute path and exact SHA-256 pin');
  const handle = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
  try {
    const before = await handle.stat({ bigint: true });
    if (!before.isFile() || before.size > BigInt(Number.MAX_SAFE_INTEGER) ||
        (expectedBytes !== undefined && before.size !== BigInt(expectedBytes)))
      throw new Error('SystemOne pinned artifact must be a regular file of the exact size');
    const hash = createHash('sha256'), block = Buffer.allocUnsafe(1024 * 1024);
    let position = 0;
    while (position < Number(before.size)) {
      signal?.throwIfAborted();
      const { bytesRead } = await handle.read(block, 0, Math.min(block.length, Number(before.size) - position), position);
      if (!bytesRead) throw new Error('SystemOne artifact changed during hash');
      hash.update(block.subarray(0, bytesRead)); position += bytesRead;
    }
    const after = await handle.stat({ bigint: true });
    if (before.size !== after.size || before.ino !== after.ino || before.mtimeNs !== after.mtimeNs ||
        before.ctimeNs !== after.ctimeNs || hash.digest('hex') !== expectedSha256)
      throw new Error('SystemOne artifact SHA-256 or file identity mismatch');
    return { handle, identity: after };
  } catch (error) { await handle.close(); throw error; }
}

async function unchangedFile(path, identity) {
  const now = await lstat(path, { bigint: true });
  if (!now.isFile() || now.dev !== identity.dev || now.ino !== identity.ino ||
      now.size !== identity.size || now.mtimeNs !== identity.mtimeNs || now.ctimeNs !== identity.ctimeNs)
    throw new Error('SystemOne pinned artifact changed after startup');
}

async function freeLoopbackPort() {
  const server = createServer();
  let listening = false;
  try {
    await new Promise((resolve, reject) => {
      server.once('error', reject);
      server.listen(0, '127.0.0.1', resolve);
    });
    listening = true;
    return server.address().port;
  } finally { if (listening) await new Promise(resolve => server.close(resolve)); }
}

async function boundedJson(response, maxResponseBytes) {
  if (!response.body) throw new Error('SystemOne response has no body');
  const reader = response.body.getReader(), parts = [];
  let length = 0;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      length += value.byteLength;
      if (length > maxResponseBytes) throw new Error('SystemOne response exceeds byte budget');
      parts.push(Buffer.from(value));
    }
  } finally { await reader.cancel().catch(() => {}); }
  try { return JSON.parse(Buffer.concat(parts, length).toString('utf8')); }
  catch { throw new Error('SystemOne response is not valid JSON'); }
}

const safeNativeErrors = new Set(['Compute error.', 'Invalid input batch.',
  'Context size has been exceeded.', 'unexpected layout of the decision prompt',
  'the instructions and the options of a question must not be empty']);

async function nativeHttpError(response, maxResponseBytes) {
  const prefix = `SystemOne child HTTP ${response.status}`;
  let payload;
  try { payload = await boundedJson(response, Math.min(maxResponseBytes, 8192)); }
  catch { return new Error(prefix); }
  const reported = payload?.error;
  if (!plain(reported)) return new Error(prefix);
  const code = Number.isSafeInteger(reported.code) ? ` code=${reported.code}` : '';
  const type = typeof reported.type === 'string' && /^[a-zA-Z0-9_.-]{1,64}$/.test(reported.type)
    ? ` type=${reported.type}` : '';
  // Native parsers may echo rejected user text. Only exact, source-reviewed
  // constant diagnostics are safe to surface; all other text gets a digest.
  const message = typeof reported.message === 'string'
    ? safeNativeErrors.has(reported.message) ? reported.message
      : `sha256:${createHash('sha256').update(reported.message).digest('hex')}` : '';
  return new Error(`${prefix}${code}${type}${message ? ` message=${message}` : ''}`);
}

function boundedSignal(parent, owner, timeoutMs) {
  return AbortSignal.any([owner, AbortSignal.timeout(timeoutMs), ...(parent ? [parent] : [])]);
}

// Low-level transport has no model passport. Only the strict Clef factory below
// may turn a verified transport into a local decision evaluator. prefixArgs is
// reserved for offline child-process tests; production never passes it.
export async function startOwnedSystemOneTransport({ binaryPath, binarySha256, binaryBytes, modelPath,
  modelSha256, modelBytes, alias, signal, startupMs = defaultLimits.startupMs,
  requestMs = defaultLimits.requestMs, maxRequestBytes = defaultLimits.maxRequestBytes,
  maxResponseBytes = defaultLimits.maxResponseBytes, prefixArgs = [] }) {
  if (!isAbsolute(binaryPath) || !isAbsolute(modelPath) || !hexDigest(binarySha256) ||
      !hexDigest(modelSha256) || !Number.isSafeInteger(modelBytes) || modelBytes < 1 ||
      typeof alias !== 'string' || !/^[a-z0-9][a-z0-9-]{1,79}$/.test(alias) ||
      !Array.isArray(prefixArgs) || prefixArgs.some(value => typeof value !== 'string') ||
      !Number.isSafeInteger(startupMs) || startupMs < 1 || startupMs > 300000 ||
      !Number.isSafeInteger(requestMs) || requestMs < 1 || requestMs > 120000 ||
      !Number.isSafeInteger(maxRequestBytes) || maxRequestBytes < 1 || maxRequestBytes > 1048576 ||
      !Number.isSafeInteger(maxResponseBytes) || maxResponseBytes < 1 || maxResponseBytes > 1048576)
    throw new Error('invalid owned SystemOne transport configuration');
  signal?.throwIfAborted();
  const binary = await pinnedFile(binaryPath, binarySha256, binaryBytes, signal);
  let model, dir, keyPath, child, closed = false, closing;
  const shutdown = new AbortController();
  function close() {
    if (closing) return closing;
    closed = true;
    // Concurrent callers join the same shutdown, and a manager must not start
    // another 6 GB model until this owned child is confirmed stopped.
    closing = Promise.resolve().then(async () => {
      const failures = [];
      shutdown.abort(new Error('SystemOne transport closed'));
      const active = () => child && child.exitCode === null && child.signalCode === null;
      const waitForExit = async () => {
        if (!active()) return;
        await Promise.race([new Promise(resolve => child.once('exit', resolve)), delay(1500)]);
      };
      if (active()) {
        child.kill('SIGTERM');
        await waitForExit();
        if (active()) { child.kill('SIGKILL'); await waitForExit(); }
        if (active()) failures.push(new Error('SystemOne child did not stop after SIGKILL'));
      }
      const handles = await Promise.allSettled([binary.handle.close(), model?.handle.close()]);
      for (const result of handles) if (result.status === 'rejected') failures.push(result.reason);
      for (const [path, remove] of [[keyPath, unlink], [dir, rmdir]]) {
        if (path) try { await remove(path); }
        catch (error) { if (error.code !== 'ENOENT') failures.push(error); }
      }
      if (failures.length) throw new AggregateError(failures, 'SystemOne owned child cleanup failed');
    });
    return closing;
  }
  try {
    model = await pinnedFile(modelPath, modelSha256, modelBytes, signal);
    dir = await mkdtemp(join(tmpdir(), 'bonfyre-systemone-'));
    keyPath = join(dir, 'api-key');
    const secret = randomBytes(32).toString('hex');
    await writeFile(keyPath, secret, { flag: 'wx', mode: 0o600 });
    const port = await freeLoopbackPort();
    const base = `http://127.0.0.1:${port}`;
    child = spawn(binaryPath, [...prefixArgs, '--model', modelPath, '--host', '127.0.0.1',
      '--port', String(port), '--alias', alias, '--api-key-file', keyPath,
      '--ctx-size', String(execution.max_context_tokens), '--batch-size', String(execution.batch_tokens),
      '--ubatch-size', String(execution.microbatch_tokens),
      '--parallel', String(execution.parallel_sequences), '--cache-ram', '0', '--no-context-shift', '--no-webui',
      '--device', execution.device, '--gpu-layers', String(execution.gpu_layers),
      '--no-op-offload', '--no-kv-offload', '--repack',
      '--offline', '--fit', 'off', '--no-agent', '--no-ui-mcp-proxy'], {
      stdio: 'ignore', shell: false,
      env: { PATH: process.env.PATH || '/usr/bin:/bin', LC_ALL: 'C', TMPDIR: dir },
    });
    let childError;
    child.on('error', error => { childError = error; });
    const headers = { Authorization: `Bearer ${secret}`, Accept: 'application/json' };
    async function get(path, deadlineSignal) {
      const response = await fetch(base + path, { headers, redirect: 'error', signal: deadlineSignal });
      return response;
    }
    const readySignal = boundedSignal(signal, shutdown.signal, startupMs);
    while (!readySignal.aborted) {
      if (childError || child.exitCode !== null || child.signalCode !== null)
        throw new Error(`SystemOne child stopped before readiness: ${childError?.message || child.exitCode || child.signalCode}`);
      try {
        const health = await get('/health', readySignal);
        await health.body?.cancel();
        if (health.ok) {
          const props = await get('/props', readySignal);
          if (!props.ok) throw new Error(`SystemOne /props HTTP ${props.status}`);
          const payload = await boundedJson(props, maxResponseBytes);
          if (payload.model_path !== modelPath || payload.model_alias !== alias ||
              payload.total_slots !== execution.parallel_sequences ||
              payload.default_generation_settings?.n_ctx !== execution.max_context_tokens ||
              payload.modalities?.vision !== false)
            throw new Error('SystemOne child did not report the pinned model and execution settings');
          await unchangedFile(binaryPath, binary.identity);
          await unchangedFile(modelPath, model.identity);
          return Object.freeze({
            async request({ state, questions, signal: requestSignal }) {
              if (closed) throw new Error('SystemOne transport is closed');
              const { request, body } = requestBody(alias, state, questions, maxRequestBytes);
              await unchangedFile(binaryPath, binary.identity);
              await unchangedFile(modelPath, model.identity);
              if (child.exitCode !== null || child.signalCode !== null)
                throw new Error('SystemOne child is no longer running');
              const response = await fetch(base + '/v1/systemone', { method: 'POST', redirect: 'error',
                headers: { ...headers, 'Content-Type': 'application/json' }, body,
                signal: boundedSignal(requestSignal, shutdown.signal, requestMs) });
              if (!response.ok) throw await nativeHttpError(response, maxResponseBytes);
              const payload = await boundedJson(response, maxResponseBytes);
              if (payload?.model !== alias) throw new Error('SystemOne child returned a different model identity');
              const answers = validateDecisionResponse(payload, request, { probability_decimals: 4 });
              return { answers };
            },
            close,
          });
        }
      } catch (error) {
        if (error.message.includes('did not report the pinned model and execution settings') ||
            error.message.includes('response exceeds byte budget') || readySignal.aborted) throw error;
      }
      await delay(100, undefined, { signal: readySignal }).catch(() => {});
    }
    throw new Error('SystemOne child readiness timed out or was cancelled');
  } catch (error) { await close(); throw error; }
}

export async function createPinnedClefFlashEvaluator({ binaryPath, binarySha256 = CLEF_FLASH_Q4.native_binary_sha256, modelPath,
  signal, startupMs, requestMs, maxRequestBytes, maxResponseBytes }) {
  if (nativeLock.schema !== 'bonfyre.clef-native-runtime.v1' ||
      binarySha256 !== CLEF_FLASH_Q4.native_binary_sha256)
    throw new Error('Clef-Flash requires the approved pinned native binary SHA-256');
  const identity = clefFlashOwnerIdentity({ startupMs: startupMs ?? defaultLimits.startupMs,
    requestMs: requestMs ?? defaultLimits.requestMs,
    maxRequestBytes: maxRequestBytes ?? defaultLimits.maxRequestBytes,
    maxResponseBytes: maxResponseBytes ?? defaultLimits.maxResponseBytes });
  const transport = await startOwnedSystemOneTransport({ binaryPath, binarySha256,
    binaryBytes: CLEF_FLASH_Q4.native_binary_bytes,
    modelPath, modelSha256: CLEF_FLASH_Q4.sha256, modelBytes: CLEF_FLASH_Q4.bytes,
    alias: CLEF_FLASH_Q4.alias, signal, startupMs, requestMs, maxRequestBytes, maxResponseBytes });
  return Object.freeze({ ...identity,
    // Do not cache based on a file hash. Native determinism must be established
    // separately for the exact implementation, hardware and settings.
    deterministic: false,
    evaluate: ({ state, questions, signal: requestSignal }) => transport.request({ state, questions, signal: requestSignal }),
    close: transport.close });
}
