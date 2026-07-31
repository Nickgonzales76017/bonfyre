import { createHash } from 'node:crypto';
import { spawn } from 'node:child_process';
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
} = {}) {
  const model = text(modelPath) || text(modelPack);
  if (!text(binary) || !model) throw new Error('binary and modelPath are required');
  if (typeof spawnImpl !== 'function') throw new Error('spawnImpl is required');

  async function generate({ prompt, maxNewTokens = 64, greedy = true, env = {}, timeoutMs = 120_000 } = {}) {
    const sourcePrompt = typeof prompt === 'string' ? prompt : '';
    if (!sourcePrompt.trim()) throw new Error('prompt is required');
    const limit = positiveInteger(maxNewTokens, 64);
    const started = Date.now();
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
      await rm(tempDir, { recursive: true, force: true });
    }
  }

  return { generate };
}
