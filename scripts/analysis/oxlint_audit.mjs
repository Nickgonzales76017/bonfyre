#!/usr/bin/env bun
// Explicit-scope lint evidence: an installer exit code is not an analysis receipt.
import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { constants, openSync, closeSync, fstatSync, lstatSync, readSync, realpathSync } from 'node:fs';
import { isAbsolute, relative, resolve, sep } from 'node:path';

const VERSION = '1.86.0';
const MAX_INPUT = 8 * 1024 * 1024;
const MAX_OUTPUT = 8 * 1024 * 1024;
const DEFAULT_ARGV = Object.freeze(['bunx', '--bun', `oxlint@${VERSION}`]);
const digest = bytes => createHash('sha256').update(bytes).digest('hex');
const record = value => value !== null && typeof value === 'object' && !Array.isArray(value);
const defaultRunner = (binary, args, options) => spawnSync(binary, args, {
  ...options, encoding: null, windowsHide: true,
});

function text(value) {
  const bytes = Buffer.isBuffer(value) ? value : Buffer.from(value ?? '');
  if (bytes.length > MAX_OUTPUT) throw new Error('analyzer output exceeds audit budget');
  return new TextDecoder('utf-8', { fatal: true }).decode(bytes);
}
function invoke(run, argv, args, root) {
  const result = run(argv[0], [...argv.slice(1), ...args], {
    cwd: root, timeout: 120000, maxBuffer: MAX_OUTPUT,
  });
  if (!result || result.error || result.signal || !Number.isInteger(result.status))
    throw new Error('analyzer failed to complete normally');
  return { status: result.status, stdout: text(result.stdout) };
}
function snapshot(root, path) {
  let current = root;
  for (const part of path.split(sep)) {
    current = resolve(current, part);
    if (lstatSync(current).isSymbolicLink()) throw new Error(`symlink input refused: ${path}`);
  }
  const fd = openSync(current, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
  try {
    const before = fstatSync(fd, { bigint: true });
    if (!before.isFile() || before.size > BigInt(MAX_INPUT))
      throw new Error(`input is not a bounded regular file: ${path}`);
    // A bounded buffer prevents a growing input from causing unbounded allocation.
    const bytes = Buffer.alloc(Number(before.size) + 1);
    let length = 0;
    while (length < bytes.length) {
      const count = readSync(fd, bytes, length, bytes.length - length, null);
      if (!count) break;
      length += count;
    }
    const after = fstatSync(fd, { bigint: true });
    const named = lstatSync(current, { bigint: true });
    const identity = stat => [stat.dev, stat.ino, stat.size, stat.mtimeNs, stat.ctimeNs].join(':');
    if (length !== Number(before.size) || identity(before) !== identity(after) ||
        identity(after) !== identity(named) || !named.isFile())
      throw new Error(`input changed during audit: ${path}`);
    return { path, bytes: length, sha256: digest(bytes.subarray(0, length)), identity: identity(after) };
  } finally { closeSync(fd); }
}

export function auditOxlint({ root = resolve(import.meta.dir, '../..'), files,
  run = defaultRunner, analyzerArgv, env = process.env } = {}) {
  const scope = { schema: 'bonfyre.oxlint-audit.v1' };
  try {
    root = realpathSync(root);
    if (!Array.isArray(files) || !files.length || files.length > 256)
      throw new Error('provide 1–256 explicit input files');
    const paths = files.map(file => {
      if (typeof file !== 'string' || !file || file.includes('\0')) throw new Error('invalid input path');
      const path = relative(root, resolve(root, file));
      if (!path || path === '..' || path.startsWith(`..${sep}`) || isAbsolute(path))
        throw new Error('input must be a file within the audit root');
      return path;
    });
    if (new Set(paths).size !== paths.length) throw new Error('duplicate input paths');
    const inputs = paths.map(path => snapshot(root, path));
    if (inputs.reduce((sum, input) => sum + input.bytes, 0) > 64 * 1024 * 1024)
      throw new Error('combined input budget exceeded');
    const argv = analyzerArgv ?? (env.BONFYRE_OXLINT_ARGV ? JSON.parse(env.BONFYRE_OXLINT_ARGV) : DEFAULT_ARGV);
    if (!Array.isArray(argv) || !argv.length || argv.length > 32 ||
        argv.some(arg => typeof arg !== 'string' || !arg || arg.includes('\0') || arg.length > 8192))
      throw new Error('analyzer argv must be a bounded JSON array of strings');
    scope.analyzer_argv = [...argv];
    const version = invoke(run, argv, ['--version'], root);
    if (version.status !== 0 || version.stdout.trim() !== `Version: ${VERSION}`)
      throw new Error(`analyzer must report pinned Oxlint ${VERSION}`);
    const checked = invoke(run, argv, ['--threads=1', '--no-ignore', '--deny-warnings', '--format=json', '--', ...paths], root);
    if (checked.status !== 0 && checked.status !== 1) throw new Error(`analyzer exited ${checked.status}`);
    let parsed;
    try { parsed = JSON.parse(checked.stdout); }
    catch { throw new Error('analyzer did not return JSON analysis evidence'); }
    if (!record(parsed) || parsed.number_of_files !== inputs.length ||
        !Number.isSafeInteger(parsed.number_of_rules) || parsed.number_of_rules <= 0)
      throw new Error('analyzer file/rule counts do not prove the requested scope');
    if (!Array.isArray(parsed.diagnostics) || parsed.diagnostics.some(item => !record(item) ||
        typeof item.message !== 'string' || !item.message || typeof item.code !== 'string' || !item.code ||
        !['error', 'warning', 'advice'].includes(item.severity) || typeof item.filename !== 'string' ||
        !paths.includes(relative(root, resolve(root, item.filename))) || !Array.isArray(item.labels) ||
        item.labels.some(label => !record(label) || !record(label.span) ||
          !Number.isSafeInteger(label.span.offset) || label.span.offset < 0 ||
          !Number.isSafeInteger(label.span.length) || label.span.length < 0)))
      throw new Error('analyzer returned invalid diagnostics');
    if ((checked.status === 0) !== (parsed.diagnostics.length === 0))
      throw new Error('analyzer exit status disagrees with diagnostics');
    for (const input of inputs) {
      const after = snapshot(root, input.path);
      if (after.sha256 !== input.sha256 || after.identity !== input.identity)
        throw new Error(`input changed during audit: ${input.path}`);
    }
    return { report: { ...scope, status: checked.status ? 'diagnostics' : 'pass',
      tool: { name: 'Oxlint', version: VERSION }, files_analyzed: parsed.number_of_files,
      rules_analyzed: parsed.number_of_rules,
      inputs: inputs.map(({ path, bytes, sha256 }) => ({ path, bytes, sha256 })),
      scope_sha256: digest(JSON.stringify(inputs.map(({ path, sha256 }) => [path, sha256]))),
      diagnostics: parsed.diagnostics }, exitCode: checked.status };
  } catch (error) {
    return { report: { ...scope, status: 'audit_failed', error: error.message }, exitCode: 2 };
  }
}

if (import.meta.main) {
  const files = process.argv.slice(2);
  if (files[0] === '--') files.shift();
  const { report, exitCode } = auditOxlint({ files });
  console.log(JSON.stringify(report));
  process.exitCode = exitCode;
}
