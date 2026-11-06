// What this does not prove:
// - a dependency is replaced by an object built in this test, so the real one is not exercised.
import { test, expect } from 'bun:test';
import { mkdtempSync, rmSync, writeFileSync, symlinkSync, mkdirSync, renameSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { auditOxlint } from '../analysis/oxlint_audit.mjs';

function fixture(check) {
  const root = mkdtempSync(join(tmpdir(), 'bonfyre-oxlint-audit-'));
  try {
    writeFileSync(join(root, 'a.mjs'), 'export const a = 1;\n');
    writeFileSync(join(root, 'b.mjs'), 'export const b = 2;\n');
    return check(root);
  } finally { rmSync(root, { recursive: true, force: true }); }
}
const evidence = overrides => ({ diagnostics: [], number_of_files: 2, number_of_rules: 96, ...overrides });
function mock({ payload = evidence(), status = 0, version = 'Version: 1.86.0', after, receipt } = {}) {
  const calls = [];
  return { calls, run(binary, args, options) {
    calls.push({ binary, args, options });
    if (args.at(-1) === '--version') return { status: 0, stdout: version };
    after?.();
    return receipt ?? { status, stdout: typeof payload === 'string' ? payload : JSON.stringify(payload) };
  } };
}
const audit = (root, run, options = {}) => auditOxlint({ root, files: ['a.mjs', 'b.mjs'], env: {}, run, ...options });

test('pinned explicit scope produces hashes and actual file/rule counts', () => fixture(root => {
  const { run, calls } = mock();
  const result = audit(root, run);
  expect(result.exitCode).toBe(0);
  expect(result.report.status).toBe('pass');
  expect(result.report.files_analyzed).toBe(2);
  expect(result.report.rules_analyzed).toBe(96);
  expect(result.report.inputs.map(input => input.path)).toEqual(['a.mjs', 'b.mjs']);
  expect(result.report.inputs.every(input => /^[a-f0-9]{64}$/.test(input.sha256))).toBe(true);
  expect(result.report.scope_sha256).toMatch(/^[a-f0-9]{64}$/);
  expect(calls[0].binary).toBe('bunx');
  expect(calls[0].args).toEqual(['--bun', 'oxlint@1.86.0', '--version']);
  expect(calls[1].args).toEqual(['--bun', 'oxlint@1.86.0', '--threads=1', '--no-ignore',
    '--deny-warnings', '--format=json', '--', 'a.mjs', 'b.mjs']);
  expect(calls.every(call => call.options.maxBuffer === 8388608 && call.options.timeout === 120000)).toBe(true);
}));

test('silent install-only output, malformed JSON and incorrect coverage fail closed', () => fixture(root => {
  for (const payload of ['', 'Installed oxlint', '{', 'null', evidence({ number_of_files: 0 }),
    evidence({ number_of_files: 1 }), evidence({ number_of_files: 3 }),
    evidence({ number_of_rules: 0 }), evidence({ number_of_rules: '96' })]) {
    expect(audit(root, mock({ payload }).run).exitCode).toBe(2);
  }
}));

test('bad version, failed exits, spawn errors, signals and unproven status refuse', () => fixture(root => {
  for (const options of [{ version: 'Version: 1.85.0' }, { version: 'Installed 1.86.0' },
    { status: 2 }, { status: 1 }, { receipt: { status: 0, signal: 'SIGTERM' } },
    { receipt: { error: { code: 'ENOENT' } } }, { receipt: { stdout: '{}' } }]) {
    expect(audit(root, mock(options).run).exitCode).toBe(2);
  }
}));

test('diagnostics must be valid, scoped and consistent with exit status', () => fixture(root => {
  const diagnostic = { filename: 'a.mjs', message: 'Unexpected console statement.',
    code: 'eslint(no-console)', severity: 'warning', labels: [{ span: { offset: 0, length: 1 } }] };
  const payload = evidence({ diagnostics: [diagnostic] });
  const result = audit(root, mock({ payload, status: 1 }).run);
  expect(result.exitCode).toBe(1);
  expect(result.report.diagnostics).toEqual([diagnostic]);
  expect(audit(root, mock({ payload }).run).exitCode).toBe(2);
  for (const bad of [null, {}, { ...diagnostic, filename: '../outside.mjs' },
    { ...diagnostic, severity: 'critical' }, { ...diagnostic, labels: [null] },
    { ...diagnostic, labels: [{ span: { offset: -1, length: 1 } }] }]) {
    expect(audit(root, mock({ payload: evidence({ diagnostics: [bad] }), status: 1 }).run).exitCode).toBe(2);
  }
}));

test('empty scope, directories, aliases and symlink inputs refuse before analyzer starts', () => fixture(root => {
  symlinkSync(join(root, 'a.mjs'), join(root, 'link.mjs'));
  mkdirSync(join(root, 'dir'));
  symlinkSync(root, join(root, 'indirect'));
  for (const files of [[], ['a.mjs', './a.mjs'], ['link.mjs'], ['indirect/a.mjs'], ['dir'], ['../escape.mjs']]) {
    const { run, calls } = mock();
    expect(audit(root, run, { files }).exitCode).toBe(2);
    expect(calls).toHaveLength(0);
  }
}));

test('oversized inputs refuse before analyzer starts', () => fixture(root => {
  writeFileSync(join(root, 'large.mjs'), Buffer.alloc(8388609));
  const { run, calls } = mock();
  expect(audit(root, run, { files: ['large.mjs'] }).exitCode).toBe(2);
  expect(calls).toHaveLength(0);
}));

test('content changes and same-content inode replacements invalidate evidence', () => fixture(root => {
  const changed = mock({ after: () => writeFileSync(join(root, 'a.mjs'), 'export const a = 9;\n') });
  expect(audit(root, changed.run).report.error).toContain('changed during audit');
  const replaced = mock({ after: () => {
    writeFileSync(join(root, 'replacement.mjs'), 'export const a = 9;\n');
    renameSync(join(root, 'replacement.mjs'), join(root, 'a.mjs'));
  } });
  expect(audit(root, replaced.run).report.error).toContain('changed during audit');
}));

test('explicit analyzer JSON argv is passed without a shell or host-specific defaults', () => fixture(root => {
  const { run, calls } = mock();
  const argv = ['env', 'NAPI_RS_NATIVE_LIBRARY_PATH=/fixture/binding.node', 'node', '/fixture/oxlint'];
  expect(audit(root, run, { env: { BONFYRE_OXLINT_ARGV: JSON.stringify(argv) } }).exitCode).toBe(0);
  expect(calls[0].binary).toBe('env');
  expect(calls[0].args).toEqual([...argv.slice(1), '--version']);
  for (const value of ['node oxlint', '{}', '[]', '["node",null]']) {
    expect(audit(root, mock().run, { env: { BONFYRE_OXLINT_ARGV: value } }).exitCode).toBe(2);
  }
}));
