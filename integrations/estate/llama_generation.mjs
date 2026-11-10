import { createHash } from 'node:crypto';
import { spawn } from 'node:child_process';
import { acquireModelMemory, defaultModelAdmission } from './model_admission.mjs';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { join } from 'node:path';

import { GENERATION_SCHEMA } from '../wordpress/native_generation.mjs';

function text(value) {
  return typeof value === 'string' ? value.trim() : '';
}

function positiveInteger(value, fallback) {
  return Number.isInteger(value) && value > 0 ? value : fallback;
}

function qwenPrompt(prompt, systemPrompt) {
  return `<|im_start|>system\n${systemPrompt}\n<|im_end|>\n<|im_start|>user\n${prompt}\n<|im_end|>\n<|im_start|>assistant\n`;
}

export function extractLlamaResponse(stdout) {
  const marker = '<|im_start|>assistant\n';
  const start = stdout.lastIndexOf(marker);
  const candidate = start >= 0 ? stdout.slice(start + marker.length) : stdout;
  const withoutRuntime = candidate
    .replace(/\n?\[ Prompt:[\s\S]*$/m, '')
    .replace(/\n?Exiting\.\.\.[\s\S]*$/m, '')
    .replace(/^(?:\r?\n)+/, '')
    .replace(/\r?\n+$/, '');
  const fence = withoutRuntime.match(/^```[a-zA-Z0-9_-]*\n([\s\S]*?)(?:\n```)?$/);
  return fence ? fence[1].replace(/\r?\n+$/, '') : withoutRuntime;
}

// The existing llama/MoE protocol is JSON. Bound its bytes while reading, and
// retain cancellation until the body is consumed; response.text() could buffer
// an arbitrarily large response before its caller checked the length.
export async function readLlamaServerPacket(response, maxOutputBytes = 1_000_000) {
  if (!Number.isSafeInteger(maxOutputBytes) || maxOutputBytes < 1 || maxOutputBytes > 8 * 1024 * 1024)
    throw new Error('invalid llama.cpp response bound');
  const declared = response.headers?.get('content-length');
  if (declared !== null && declared !== undefined && (!/^\d+$/.test(declared) || Number(declared) > maxOutputBytes))
    throw new Error('llama.cpp response exceeds its byte bound');
  if (!response.body?.getReader) throw new Error('llama.cpp response body is unavailable');
  const reader = response.body.getReader(), parts = []; let length = 0;
  try {
    for (;;) {
      const {value, done} = await reader.read(); if (done) break;
      if (!(value instanceof Uint8Array) || (length += value.length) > maxOutputBytes)
        throw new Error('llama.cpp response exceeds its byte bound');
      parts.push(value);
    }
    return new TextDecoder('utf-8', {fatal:true}).decode(Buffer.concat(parts, length));
  } finally {
    try { await reader.cancel(); } catch {}
    try { reader.releaseLock(); } catch {}
  }
}

function run(binary, args, { spawnImpl, env, timeoutMs, maxOutputBytes }) {
  return new Promise((resolve, reject) => {
    const child = spawnImpl(binary, args, { cwd: process.cwd(), env, stdio: ['ignore', 'pipe', 'pipe'] });
    let stdout = '';
    let stderr = '';
    let settled = false;
    const capture = (value, chunk) => {
      const remaining = maxOutputBytes - Buffer.byteLength(value);
      if (remaining <= 0) return value;
      return `${value}${chunk.toString().slice(0, remaining)}`;
    };
    const outputLimit = () => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      child.kill('SIGKILL');
      resolve({ code: null, signal: 'SIGKILL', stdout, stderr, timed_out: false, output_limited: true });
    };
    const timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      child.kill('SIGKILL');
      resolve({ code: null, signal: 'SIGKILL', stdout, stderr, timed_out: true });
    }, timeoutMs);
    child.stdout?.on('data', (chunk) => {
      stdout = capture(stdout, chunk);
      if (Buffer.byteLength(stdout) >= maxOutputBytes) outputLimit();
    });
    child.stderr?.on('data', (chunk) => {
      stderr = capture(stderr, chunk);
      if (Buffer.byteLength(stderr) >= maxOutputBytes) outputLimit();
    });
    child.once('error', (error) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      reject(error);
    });
    child.once('close', (code, signal) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      resolve({ code, signal, stdout, stderr, timed_out: false });
    });
  });
}

export function createLlamaGenerationClient({
  binary,
  modelPath,
  modelPack,
  systemPrompt = 'You are Bonfyre\'s precise local coding assistant. Return the requested result directly.',
  gpuLayers = 99,
  contextSize = 2048,
  device,
  opOffload,
  fit,
  backend = 'llama-cpp',
  maxOutputBytes = 1_000_000,
  spawnImpl = spawn,
  tempRoot = '/tmp',
  // A real llama.cpp run loads the whole checkpoint, so it asks the memory
  // ledger first. An injected spawn loads nothing and needs no grant.
  admission = spawnImpl === spawn ? 'default' : null,
} = {}) {
  const model = text(modelPath) || text(modelPack);
  if (!text(binary) || !model) throw new Error('binary and modelPath are required');
  if (typeof spawnImpl !== 'function') throw new Error('spawnImpl is required');

  async function generate({ prompt, maxNewTokens = 64, greedy = true, env = {}, timeoutMs = 120_000 } = {}) {
    const sourcePrompt = typeof prompt === 'string' ? prompt : '';
    if (!sourcePrompt.trim()) throw new Error('prompt is required');
    const limit = positiveInteger(maxNewTokens, 64);
    const started = Date.now();
    const ledger = admission === 'default' ? defaultModelAdmission() : admission;
    const grant = ledger ? acquireModelMemory({ modelPath: model, contextSize, plane: 'llama-cli', admission: ledger }) : null;
    const tempDir = await mkdtemp(join(tempRoot, 'bonfyre-llama-generation-'));
    const promptPath = join(tempDir, 'prompt.txt');
    try {
      await writeFile(promptPath, qwenPrompt(sourcePrompt, systemPrompt), 'utf8');
      const args = [
        ...(text(device) ? ['--device', text(device)] : []),
        ...(opOffload === false ? ['--no-op-offload'] : []),
        ...(fit === false ? ['--fit', 'off'] : []),
        '-m', model,
        '-ngl', String(gpuLayers),
        '-c', String(contextSize),
        '-n', String(limit),
        '--temp', greedy ? '0' : '0.7',
        '--no-warmup',
        '--single-turn',
        '--simple-io',
        '-f', promptPath,
      ];
      const execution = await run(binary, args, {
        spawnImpl,
        env: { ...process.env, ...env },
        timeoutMs,
        maxOutputBytes,
      });
      if (execution.timed_out) throw new Error(`llama.cpp generation timed out after ${timeoutMs}ms`);
      if (execution.output_limited) throw new Error(`llama.cpp generation exceeded ${maxOutputBytes} output bytes`);
      if (execution.code !== 0) throw new Error(`llama.cpp generation failed (${execution.code ?? execution.signal}): ${execution.stderr.trim()}`);
      const output = extractLlamaResponse(execution.stdout);
      if (!output.trim()) throw new Error('llama.cpp generation returned empty output');
      return {
        schema_version: GENERATION_SCHEMA,
        generated_at: new Date().toISOString(),
        model: { name: 'llama.cpp', binary, pack: model, runtime: 'llama-cpp' },
        backend,
        request: { prompt: sourcePrompt, max_new_tokens: limit, greedy },
        prompt_sha256: createHash('sha256').update(sourcePrompt).digest('hex'),
        output,
        output_sha256: createHash('sha256').update(output).digest('hex'),
        stderr: execution.stderr,
        stdout: execution.stdout,
        elapsed_ms: Date.now() - started,
      };
    } finally {
      grant?.release();
      await rm(tempDir, { recursive: true, force: true });
    }
  }

  return { generate };
}

/**
 * Use the llama-server behind `bonfyre-moe serve` rather than reloading a GGUF
 * for every Estate request. That server loads the model on demand after a
 * memory grant and unloads it when idle; a 503 here means admission refused the
 * load, and the response says why. Same generation receipt shape as above.
 */
export function createLlamaServerGenerationClient({
  serverUrl,
  model,
  modelPath,
  modelPack,
  systemPrompt = 'You are Bonfyre\'s precise local coding assistant. Return the requested result directly.',
  backend = 'llama-cpp-server',
  maxOutputBytes = 1_000_000,
  fetchImpl = globalThis.fetch,
} = {}) {
  const endpoint = text(serverUrl).replace(/\/$/, '');
  const modelName = text(model) || text(modelPath) || text(modelPack);
  if (!/^https?:\/\//.test(endpoint)) throw new Error('serverUrl must be an http(s) URL');
  if (!modelName) throw new Error('model, modelPath, or modelPack is required');
  if (typeof fetchImpl !== 'function') throw new Error('fetchImpl is required');

  async function generate({ prompt, maxNewTokens = 64, greedy = true, env: _env = {}, timeoutMs = 120_000, signal } = {}) {
    const sourcePrompt = typeof prompt === 'string' ? prompt : '';
    if (!sourcePrompt.trim()) throw new Error('prompt is required');
    const limit = positiveInteger(maxNewTokens, 64);
    const started = Date.now();
    const controller = new AbortController();
    if (signal !== undefined && !(signal instanceof AbortSignal)) throw new Error('invalid generation cancellation signal');
    const abort = () => controller.abort(signal.reason);
    signal?.addEventListener('abort', abort, {once:true});
    if (signal?.aborted) abort();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const response = await fetchImpl(`${endpoint}/v1/chat/completions`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          model: modelName,
          messages: [
            { role: 'system', content: systemPrompt },
            { role: 'user', content: sourcePrompt },
          ],
          max_tokens: limit,
          temperature: greedy ? 0 : 0.7,
          stream: false,
        }),
        signal: controller.signal,
        redirect: 'error',
      });
      const raw = await readLlamaServerPacket(response, maxOutputBytes);
      if (!response.ok) throw new Error(`llama.cpp server generation failed (${response.status}): ${raw.slice(-4_000)}`);
      let payload;
      try { payload = JSON.parse(raw); } catch { throw new Error('llama.cpp server returned invalid JSON'); }
      const output = payload?.choices?.[0]?.message?.content;
      if (typeof output !== 'string' || !output.trim()) throw new Error('llama.cpp server returned empty output');
      return {
        schema_version: GENERATION_SCHEMA,
        generated_at: new Date().toISOString(),
        model: { name: 'llama.cpp', binary: endpoint, pack: modelName, runtime: 'llama-cpp-server' },
        backend,
        request: { prompt: sourcePrompt, max_new_tokens: limit, greedy },
        prompt_sha256: createHash('sha256').update(sourcePrompt).digest('hex'),
        output,
        output_sha256: createHash('sha256').update(output).digest('hex'),
        stderr: '',
        stdout: raw,
        elapsed_ms: Date.now() - started,
        usage: payload.usage && ['prompt_tokens','completion_tokens','total_tokens'].every(key =>
          Number.isSafeInteger(payload.usage[key]) && payload.usage[key] >= 0)
          ? {prompt_tokens:payload.usage.prompt_tokens,completion_tokens:payload.usage.completion_tokens,total_tokens:payload.usage.total_tokens}
          : null,
      };
    } catch (error) {
      if (signal?.aborted) throw new Error('llama.cpp server request cancelled; native completion was not observed');
      if (error?.name === 'AbortError') throw new Error(`llama.cpp server generation timed out after ${timeoutMs}ms`);
      throw error;
    } finally {
      clearTimeout(timer);
      signal?.removeEventListener('abort', abort);
    }
  }

  return { generate };
}
