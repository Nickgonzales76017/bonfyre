// Shared admission and bounded evidence for local read-only CLI probes. Binary
// presence/version success is not authorization or semantic capability proof.
import { createHash } from 'node:crypto';
import { constants, openSync, closeSync, readSync, fstatSync, lstatSync, realpathSync } from 'node:fs';
import { basename, dirname, isAbsolute, join, resolve, delimiter } from 'node:path';
import { readSpinesBytes } from './spines_store.mjs';
import { projectRun } from './project_runtime.mjs';
import { inspectCanaryProviderExecutable } from './canary_fixtures.mjs';

export function probeExecutable(path) {
  // Inspect the target but preserve the invocation name. Multicall binaries
  // (including Node toolchain shims) select behavior from argv[0].
  try { const actual = realpathSync(path), info = lstatSync(actual); return info.isFile() && info.mode & 0o111 ? resolve(path) : null; }
  catch { return null; }
}
export function findProbeExecutable(name, env = process.env) {
  if (typeof name !== 'string' || !/^[A-Za-z0-9_.+-]{1,128}$/.test(name)) throw new Error('invalid probe command name');
  for (const path of (env.PATH || '').split(delimiter).filter(Boolean)) {
    const found = probeExecutable(join(path, name)); if (found) return found;
  }
  return null;
}
export function isNativeProbeExecutable(path) {
  let fd;
  try {
    fd = openSync(realpathSync(path), constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
    const info = fstatSync(fd); if (!info.isFile() || !(info.mode & 0o111)) return false;
    const bytes = Buffer.alloc(4); if (readSync(fd, bytes, 0, 4, 0) !== 4) return false;
    return bytes.equals(Buffer.from([127, 69, 76, 70])) ||
      [0xfeedface, 0xfeedfacf, 0xcefaedfe, 0xcffaedfe, 0xcafebabe, 0xbebafeca].includes(bytes.readUInt32BE(0));
  } catch { return false; }
  finally { if (fd !== undefined) closeSync(fd); }
}
function plan(path, env) {
  if (isNativeProbeExecutable(path)) return { argv: [path] };
  try {
    const first = readSpinesBytes(realpathSync(path), 2 * 1024 * 1024).toString('utf8').split(/\r?\n/, 1)[0];
    const match = /^#!\s*(?:\/usr\/bin\/env\s+(node|bun)|([^\s]+\/(?:node|bun)))\s*$/.exec(first);
    if (match) {
      const interpreter = match[1] ? findProbeExecutable(match[1], env) : probeExecutable(match[2]);
      return interpreter && isNativeProbeExecutable(interpreter) ? { argv: [interpreter, path] } : null;
    }
    // Reuse the existing exact-byte review owner; neither a shell shebang nor
    // an executable bit grants admission. Bernstein's Python exception remains
    // scoped to its canary owner and is deliberately not inherited here.
    const reviewed = inspectCanaryProviderExecutable(basename(path), dirname(path));
    return reviewed.kind === 'reviewed_script' && reviewed.path === realpathSync(path) ? {
      argv: [path], boundary: { kind: 'reviewed_script', path, sha256: reviewed.sha256, runtime: reviewed.runtime,
        scope: 'exact reviewed entrypoint; transitive dependency behavior is not certified' },
    } : null;
  } catch { return null; }
}
const sha = value => createHash('sha256').update(value).digest('hex');
// Admission shared by semantic execution owners which need bounded raw output.
// This is executable admission, not command authorization or capability proof.
export function nativeExecutionPlan(command, { env = process.env } = {}) {
  const binary = isAbsolute(command) ? probeExecutable(command) : findProbeExecutable(command, env);
  return binary ? plan(binary, env) : null;
}
export function nativeProbeReceipt(argv, result) {
  const stdout = String(result.stdout || ''), stderr = String(result.stderr || '');
  return { argv, exit_code: Number.isSafeInteger(result.exit_code) ? result.exit_code : 127,
    stdout: '', stderr: '', stdout_sha256: sha(stdout), stderr_sha256: sha(stderr),
    stdout_bytes: Buffer.byteLength(stdout), stderr_bytes: Buffer.byteLength(stderr), output_redacted: true,
    timed_out: Boolean(result.timeout || result.timed_out), overflow: Boolean(result.overflow), cancelled: Boolean(result.cancelled),
    duration_ms: Number.isFinite(result.duration_ms) ? Math.max(0, result.duration_ms) : 0 };
}
export const nativeProbePassed = value => Boolean(value && value.exit_code === 0 && !value.timed_out && !value.overflow && !value.cancelled && !value.refused);

export function createNativeProbe({ run = projectRun, env = process.env, cwd, checkStop = () => {} } = {}) {
  const check = signal => { signal?.throwIfAborted(); checkStop(); };
  return async (argv, { cwd: directory = cwd, timeoutMs = 15000, signal } = {}) => {
    check(signal);
    if (!Array.isArray(argv) || !argv.length || argv.length > 128 || argv.some(value => typeof value !== 'string' || value.includes('\0') || Buffer.byteLength(value) > 16384))
      throw new Error('invalid native probe argv');
    const prefix = nativeExecutionPlan(argv[0], { env });
    if (!prefix) return { ...nativeProbeReceipt(argv, {}), refused: 'native-or-node-bun-executable-required', execution_state: 'blocked' };
    const actual = [...prefix.argv, ...argv.slice(1)];
    try {
      check(signal);
      const result = await run(actual, { cwd: directory, env, timeoutMs, maxOutputBytes: 2 * 1024 * 1024, signal });
      check(signal); return { ...nativeProbeReceipt(actual, result), ...(prefix.boundary ? { runtime_boundary: prefix.boundary } : {}) };
    } catch (error) {
      check(signal);
      return { ...nativeProbeReceipt(actual, {}), error_class: error?.name === 'AbortError' ? 'AbortError' : 'ExecutionError', execution_state: 'unknown' };
    }
  };
}
