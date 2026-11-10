import { test, expect } from 'bun:test';
import { createHash } from 'node:crypto';
import { mkdtempSync, mkdirSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { auditShellScripts } from '../analysis/shellcheck_audit.mjs';

const digest = text => createHash('sha256').update(text).digest('hex');
function fixture(check) {
  const root = mkdtempSync(join(tmpdir(), 'bonfyre-shellcheck-audit-'));
  try {
    mkdirSync(join(root, 'scripts'));
    writeFileSync(join(root, 'scripts', 'alpha.sh'), '#!/bin/sh\necho alpha\n');
    writeFileSync(join(root, 'scripts', 'beta.sh'), '#!/bin/zsh\necho beta\n');
    return check(root);
  } finally { rmSync(root, { recursive: true, force: true }); }
}
function runnerFor(diagnostics, { missing = false } = {}) {
  const calls = [];
  const run = (binary, args, options) => {
    calls.push({ binary, args, options });
    if (binary === 'rg') return { status: 0,
      stdout: Buffer.from('scripts/beta.sh\0scripts/alpha.sh\0'), stderr: Buffer.alloc(0) };
    if (missing) return { error: Object.assign(new Error('missing'), { code: 'ENOENT' }) };
    if (args[0] === '--version') return { status: 0,
      stdout: Buffer.from('ShellCheck - shell script analysis tool\nversion: 0.11.0\n'), stderr: Buffer.alloc(0) };
    return { status: diagnostics.length ? 1 : 0,
      stdout: Buffer.from(JSON.stringify(diagnostics)), stderr: Buffer.alloc(0) };
  };
  return { run, calls };
}

test('errors fail with full reproducible scope and warnings remain visible', () => fixture(root => {
  const warning = { file: 'scripts/alpha.sh', line: 2, column: 1, level: 'warning', code: 2086, message: 'quote it' };
  const error = { file: 'scripts/beta.sh', line: 1, column: 1, level: 'error', code: 1072, message: 'bad parse' };
  const { run, calls } = runnerFor([error, warning]);
  const { report, exitCode } = auditShellScripts({ root, roots: ['scripts'], run });
  expect(exitCode).toBe(1);
  expect(report.status).toBe('errors');
  expect(report.counts).toEqual({ error: 1, warning: 1, info: 0, style: 0 });
  expect(report.diagnostics).toEqual([warning, error]);
  expect(report.tool.version).toBe('0.11.0');
  expect(report.inputs).toEqual([
    { path: 'scripts/alpha.sh', sha256: digest('#!/bin/sh\necho alpha\n') },
    { path: 'scripts/beta.sh', sha256: digest('#!/bin/zsh\necho beta\n') },
  ]);
  expect(report.scope_sha256).toMatch(/^[a-f0-9]{64}$/);
  expect(calls.map(call => call.binary)).toEqual(['rg', 'shellcheck', 'shellcheck']);
  expect(calls[2].args).toEqual(['--format=json', '--', 'scripts/alpha.sh', 'scripts/beta.sh']);
  expect(calls.every(call => call.options.timeout <= 30000 && call.options.maxBuffer <= 8 * 1024 * 1024)).toBe(true);
}));

test('SC1071 remains unsupported rather than an actual error or a passed file', () => fixture(root => {
  const unsupported = { file: 'scripts/beta.sh', line: 1, column: 1, level: 'error', code: 1071,
    message: 'ShellCheck only supports sh/bash/dash/ksh scripts' };
  const { report, exitCode } = auditShellScripts({ root, roots: ['scripts'],
    run: runnerFor([unsupported]).run });
  expect(exitCode).toBe(0);
  expect(report.status).toBe('partial_unsupported');
  expect(report.files_discovered).toBe(2);
  expect(report.files_analyzed).toBe(1);
  expect(report.unsupported_files).toEqual(['scripts/beta.sh']);
  expect(report.counts.error).toBe(0);
  expect(report.diagnostics).toEqual([unsupported]);
}));

test('missing installed ShellCheck fails instead of producing a clean audit', () => fixture(root => {
  const { report, exitCode } = auditShellScripts({ root, roots: ['scripts'],
    run: runnerFor([], { missing: true }).run });
  expect(exitCode).toBe(2);
  expect(report.status).toBe('tool_unavailable');
  expect(report.error).toContain('shellcheck is unavailable');
  expect(report.tool).toBeUndefined();
}));

test('a file changed while ShellCheck runs cannot retain its old input digest', () => fixture(root => {
  const base = runnerFor([]);
  const run = (binary, args, options) => {
    const result = base.run(binary, args, options);
    if (binary === 'shellcheck' && args[0] === '--format=json')
      writeFileSync(join(root, 'scripts', 'alpha.sh'), '#!/bin/sh\necho changed\n');
    return result;
  };
  const { report, exitCode } = auditShellScripts({ root, roots: ['scripts'], run });
  expect(exitCode).toBe(2);
  expect(report.status).toBe('audit_failed');
  expect(report.error).toContain('changed during audit');
}));

test('unknown ShellCheck severity cannot be silently counted as a pass', () => fixture(root => {
  const diagnostic = { file: 'scripts/alpha.sh', line: 1, column: 1,
    level: 'critical', code: 9999, message: 'unknown severity' };
  const { report, exitCode } = auditShellScripts({ root, roots: ['scripts'],
    run: runnerFor([diagnostic]).run });
  expect(exitCode).toBe(2);
  expect(report.status).toBe('audit_failed');
  expect(report.error).toContain('invalid diagnostic shape');
}));
