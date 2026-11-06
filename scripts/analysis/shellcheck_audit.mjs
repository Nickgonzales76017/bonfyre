#!/usr/bin/env bun
// Read-only, reproducible shell audit. Unsupported shells remain visible but
// are not misreported as parse errors or successfully analyzed files.
import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { lstatSync, readFileSync } from 'node:fs';
import { join, resolve } from 'node:path';

const ROOTS = Object.freeze(['scripts', 'cmd', 'components', 'architecture', 'integrations',
  'services', 'deploy', 'engine', 'mcp']);
const GLOBS = Object.freeze(['*.sh', '!**/vendor/**', '!**/node_modules/**']);
const MAX_OUTPUT = 8 * 1024 * 1024;
const byBytes = (a, b) => Buffer.compare(Buffer.from(a), Buffer.from(b));
const digest = bytes => createHash('sha256').update(bytes).digest('hex');
const defaultRunner = (binary, args, options) => spawnSync(binary, args, {
  cwd: options.cwd, encoding: null, timeout: options.timeout, maxBuffer: options.maxBuffer,
  windowsHide: true,
});
const failure = (kind, message, scope = {}) => ({ report: {
  schema: 'bonfyre.shellcheck-audit.v1', status: kind, error: message, ...scope,
}, exitCode: 2 });

function outputText(value) { return Buffer.isBuffer(value) ? value.toString('utf8') : String(value ?? ''); }
function invoke(run, binary, args, cwd, timeout) {
  const result = run(binary, args, { cwd, timeout, maxBuffer: MAX_OUTPUT });
  if (result.error?.code === 'ENOENT') throw Object.assign(new Error(`${binary} is unavailable`), { kind: 'tool_unavailable' });
  if (result.error) throw new Error(`${binary} failed: ${result.error.code || result.error.message}`);
  if (result.signal) throw new Error(`${binary} stopped by ${result.signal}`);
  return result;
}

export function auditShellScripts({ root = resolve(import.meta.dir, '../..'), run = defaultRunner,
  roots = ROOTS, batchSize = 64 } = {}) {
  if (!Number.isSafeInteger(batchSize) || batchSize < 1 || batchSize > 128)
    throw new Error('invalid ShellCheck batch size');
  const discoveryArgs = ['--files', '-0', ...GLOBS.flatMap(glob => ['-g', glob]), ...roots];
  const scope = { discovery: { command: 'rg', args: discoveryArgs }, roots: [...roots], globs: [...GLOBS] };
  try {
    const discovered = invoke(run, 'rg', discoveryArgs, root, 10000);
    if (discovered.status !== 0) throw new Error(`rg discovery exited ${discovered.status}: ${outputText(discovered.stderr).trim()}`);
    const raw = Buffer.isBuffer(discovered.stdout) ? discovered.stdout : Buffer.from(discovered.stdout ?? '');
    const files = raw.toString('utf8').split('\0').filter(Boolean).sort(byBytes);
    if (!files.length || new Set(files).size !== files.length)
      throw new Error('shell discovery was empty or contained duplicate paths');
    const inputs = files.map(path => {
      const full = join(root, path);
      const stat = lstatSync(full);
      if (!stat.isFile() || stat.size > 8 * 1024 * 1024)
        throw new Error(`shell input is not a bounded regular file: ${path}`);
      return { path, sha256: digest(readFileSync(full)) };
    });
    const scopeSha256 = digest(inputs.map(({ path, sha256 }) => `${path}\0${sha256}\n`).join(''));
    const inventory = { ...scope, scope_sha256: scopeSha256, inputs };
    const version = invoke(run, 'shellcheck', ['--version'], root, 10000);
    if (version.status !== 0) throw new Error(`shellcheck --version exited ${version.status}`);
    const match = /^version:\s*(\S+)/m.exec(outputText(version.stdout));
    if (!match) throw new Error('shellcheck did not report a parseable version');

    const diagnostics = [];
    for (let offset = 0; offset < files.length; offset += batchSize) {
      const batch = files.slice(offset, offset + batchSize);
      const checked = invoke(run, 'shellcheck', ['--format=json', '--', ...batch], root, 30000);
      if (checked.status !== 0 && checked.status !== 1)
        throw new Error(`shellcheck exited ${checked.status}: ${outputText(checked.stderr).trim()}`);
      let parsed;
      try { parsed = JSON.parse(outputText(checked.stdout)); }
      catch { throw new Error('shellcheck returned malformed JSON diagnostics'); }
      if (!Array.isArray(parsed) || parsed.some(item => !item || typeof item.file !== 'string' ||
          !['error', 'warning', 'info', 'style'].includes(item.level) || !Number.isInteger(item.code) ||
          !batch.includes(item.file)))
        throw new Error('shellcheck returned an invalid diagnostic shape');
      if (checked.status === 1 && !parsed.length)
        throw new Error('shellcheck failed without a diagnostic');
      diagnostics.push(...parsed);
    }
    for (const input of inputs) {
      const full = join(root, input.path);
      const stat = lstatSync(full);
      if (!stat.isFile() || stat.size > 8 * 1024 * 1024 ||
          digest(readFileSync(full)) !== input.sha256)
        throw new Error(`shell input changed during audit: ${input.path}`);
    }
    diagnostics.sort((a, b) => byBytes(a.file, b.file) || a.line - b.line ||
      a.column - b.column || a.code - b.code || byBytes(a.message || '', b.message || ''));
    const unsupported = diagnostics.filter(item => item.code === 1071);
    const analyzed = diagnostics.filter(item => item.code !== 1071);
    const unsupportedFiles = [...new Set(unsupported.map(item => item.file))].sort(byBytes);
    const counts = Object.fromEntries(['error', 'warning', 'info', 'style']
      .map(level => [level, analyzed.filter(item => item.level === level).length]));
    const status = counts.error ? 'errors' : unsupportedFiles.length ? 'partial_unsupported' : 'pass';
    return { report: { schema: 'bonfyre.shellcheck-audit.v1', status,
      tool: { name: 'ShellCheck', version: match[1] }, ...inventory,
      files_discovered: files.length, files_analyzed: files.length - unsupportedFiles.length,
      unsupported_files: unsupportedFiles, counts, diagnostics }, exitCode: counts.error ? 1 : 0 };
  } catch (error) {
    return failure(error.kind || 'audit_failed', error.message, scope);
  }
}

if (import.meta.main) {
  const { report, exitCode } = auditShellScripts();
  console.log(JSON.stringify(report));
  process.exitCode = exitCode;
}
