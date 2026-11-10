// Bounded subprocess boundary shared by every local project owner. No shell
// interpolation, truncated success, interpreter fallback or detached work.
import { spawn } from 'node:child_process';
import { basename } from 'node:path';

export async function projectRun(argv, { cwd, env = process.env, input,
  timeoutMs = 30_000, maxOutputBytes = 16 * 1024 * 1024, signal: abortSignal } = {}) {
  if (!Array.isArray(argv) || !argv.length || argv.some(x => typeof x !== 'string' || x.includes('\0')))
    throw new Error('argv must be nonempty NUL-free strings');
  if (/^(?:python(?:\d+(?:\.\d+)*)?|pypy\d*)$/i.test(basename(argv[0])))
    throw new Error('Python execution is retired');
  if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 3_600_000 ||
      !Number.isSafeInteger(maxOutputBytes) || maxOutputBytes < 1 || maxOutputBytes > 32 * 1024 * 1024)
    throw new Error('invalid process resource bound');
  if (input !== undefined && (typeof input !== 'string' || Buffer.byteLength(input) > 8 * 1024 * 1024))
    throw new Error('invalid or oversized subprocess input');
  if (abortSignal !== undefined && !(abortSignal instanceof AbortSignal)) throw new Error('invalid abort signal');
  if (abortSignal?.aborted) return { argv, ...(cwd ? { cwd } : {}), exit_code: 130, cancelled: true, stdout: '', stderr: '', duration_ms: 0 };
  const started = performance.now();
  return await new Promise(resolve => {
    const child = spawn(argv[0], argv.slice(1), { cwd, env, shell: false,
      detached: process.platform !== 'win32', stdio: ['pipe', 'pipe', 'pipe'] });
    const chunks = { stdout: [], stderr: [] }, sizes = { stdout: 0, stderr: 0 };
    let timedOut = false, overflow = false, cancelled = false, spawnError, hardStop;
    const terminate = signal => {
      if (!child.pid) return;
      try {
        if (process.platform === 'win32') child.kill(signal);
        else process.kill(-child.pid, signal);
      } catch (error) { if (error.code !== 'ESRCH') spawnError ??= error; }
    };
    const stop = () => {
      terminate('SIGTERM');
      hardStop ??= setTimeout(() => terminate('SIGKILL'), 250);
    };
    const timer = setTimeout(() => { timedOut = true; stop(); }, timeoutMs);
    const abort = () => { cancelled = true; stop(); };
    abortSignal?.addEventListener('abort', abort, { once: true });
    if (abortSignal?.aborted) abort();
    for (const name of ['stdout', 'stderr']) child[name].on('data', bytes => {
      const maximum = name === 'stdout' ? maxOutputBytes : Math.min(maxOutputBytes, 1024 * 1024);
      const remaining = maximum - sizes[name];
      if (remaining > 0) chunks[name].push(bytes.subarray(0, remaining));
      sizes[name] += bytes.length;
      if (sizes[name] > maximum && !overflow) { overflow = true; stop(); }
    });
    child.on('error', error => { spawnError = error; });
    // A child may refuse input early. EPIPE is an execution outcome, not an
    // uncaught server exception; the exit status still decides success.
    child.stdin.on('error', error => { if (error.code !== 'EPIPE') { spawnError = error; stop(); } });
    child.stdin.end(input);
    child.on('close', (code, signal) => {
      if (timedOut || overflow || cancelled) terminate('SIGKILL');
      abortSignal?.removeEventListener('abort', abort);
      clearTimeout(timer); clearTimeout(hardStop);
      let stdout, stderr, invalidUtf8 = false;
      try {
        const decoder = new TextDecoder('utf-8', { fatal: !overflow });
        stdout = decoder.decode(Buffer.concat(chunks.stdout));
        stderr = decoder.decode(Buffer.concat(chunks.stderr));
      } catch { invalidUtf8 = true; stdout = ''; stderr = 'invalid UTF-8 subprocess output'; }
      resolve({ argv, ...(cwd ? { cwd } : {}),
        exit_code: cancelled ? 130 : timedOut ? 124 : overflow ? 125 : spawnError ? 127 : invalidUtf8 ? 126 : code ?? 128,
        stdout,
        stderr: spawnError ? `${spawnError.code || 'spawn'}: process could not complete` : stderr,
        duration_ms: Math.round(performance.now() - started),
        ...(timedOut ? { timeout: true } : {}), ...(overflow ? { overflow: true } : {}),
        ...(cancelled ? { cancelled: true } : {}),
        ...(invalidUtf8 ? { invalid_utf8: true } : {}),
        ...(signal ? { signal } : {}) });
    });
  });
}
