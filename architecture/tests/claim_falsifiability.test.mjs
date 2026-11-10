import { test, expect } from 'bun:test';
import { mkdtempSync, writeFileSync, rmSync } from 'node:fs';
import { join, resolve, dirname } from 'node:path';
import { tmpdir } from 'node:os';
import { fileURLToPath } from 'node:url';

const checker = resolve(dirname(fileURLToPath(import.meta.url)), '../check-falsifiable-claims.mjs');

function scan(source) {
  const dir = mkdtempSync(join(tmpdir(), 'bonfyre-claim-check-'));
  try {
    writeFileSync(join(dir, 'suite.sh'), source);
    const result = Bun.spawnSync([process.execPath, checker, `--root=${dir}`]);
    return { code: result.exitCode, text: new TextDecoder().decode(result.stdout) };
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}

test('a failure counter hidden by the summary exit is a hard failure', () => {
  const result = scan('fail=0\nfail=$((fail + 1))\necho "$fail failed"\n');
  expect(result.code).toBe(1);
  expect(result.text).toContain('counted-not-returned  (1)');
});

test('a suite returning the failure count remains valid', () => {
  const result = scan('fail=0\nfail=$((fail + 1))\necho "$fail failed"\nexit $fail\n');
  expect(result.code).toBe(0);
  expect(result.text).toContain('no unfalsifiable-claim patterns found');
});

test('literal status remains advisory, never a hard failure', () => {
  const result = scan('value={"status":"ready"}\n');
  expect(result.code).toBe(0);
  expect(result.text).toContain('literal-status-ok  (1)');
  expect(result.text).toContain('cannot infer: a flagged line -> a defect');
});
