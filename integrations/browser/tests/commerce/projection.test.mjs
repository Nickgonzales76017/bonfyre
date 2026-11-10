import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, mkdirSync, readFileSync, writeFileSync, rmSync, symlinkSync, statSync, unlinkSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { projectCommerce } from '../../../../scripts/project-recurring-commerce-browser.mjs';

function fixture(work) {
  const root = mkdtempSync(join(tmpdir(), 'bf-commerce-projection-test-'));
  const source = join(root, 'scripts/recurring-commerce'), target = join(root, 'integrations/browser/extension');
  mkdirSync(source, { recursive: true }); mkdirSync(target, { recursive: true });
  writeFileSync(join(source, 'draft.mjs'), 'export const words = "日本語";\n');
  writeFileSync(join(source, 'catalog.json'), '{ "templates": [] }\n');
  writeFileSync(join(source, 'contents.mjs'), 'export const selection = true;\n');
  writeFileSync(join(target, 'recurring-commerce.mjs'), 'old compiler\n');
  writeFileSync(join(target, 'catalog.json'), 'old catalogue\n');
  try { work({ root, source, target }); } finally { rmSync(root, { recursive: true, force: true }); }
}

test('check identifies drift without changing either projection', () => fixture(({ root, target }) => {
  assert(projectCommerce({ root }).every(row => row.changed));
  assert.equal(readFileSync(join(target, 'recurring-commerce.mjs'), 'utf8'), 'old compiler\n');
  assert.equal(readFileSync(join(target, 'catalog.json'), 'utf8'), 'old catalogue\n');
}));
test('write creates exact bytes and unchanged regeneration keeps inode and time', () => fixture(({ root, source, target }) => {
  assert(projectCommerce({ root, write: true }).every(row => row.changed));
  assert.deepEqual(readFileSync(join(target, 'recurring-commerce.mjs')), readFileSync(join(source, 'draft.mjs')));
  assert.deepEqual(readFileSync(join(target, 'catalog.json')), readFileSync(join(source, 'catalog.json')));
  assert.deepEqual(readFileSync(join(target, 'contents.mjs')), readFileSync(join(source, 'contents.mjs')));
  const before = statSync(join(target, 'catalog.json'));
  assert(projectCommerce({ root, write: true }).every(row => !row.changed));
  const after = statSync(join(target, 'catalog.json'));
  assert.equal(before.ino, after.ino); assert.equal(before.mtimeMs, after.mtimeMs);
}));
test('source changes are detected without a new fixed digest', () => fixture(({ root, source }) => {
  projectCommerce({ root, write: true });
  writeFileSync(join(source, 'draft.mjs'), 'export const words = "revised";\n');
  const rows = projectCommerce({ root });
  assert.equal(rows[0].changed, true); assert.equal(rows[1].changed, false);
}));
test('missing second input cannot partly replace the first projection', () => fixture(({ root, source, target }) => {
  unlinkSync(join(source, 'catalog.json'));
  assert.throws(() => projectCommerce({ root, write: true }), /ENOENT/);
  assert.equal(readFileSync(join(target, 'recurring-commerce.mjs'), 'utf8'), 'old compiler\n');
}));
test('a symlink target cannot redirect projection writes', () => fixture(({ root, target }) => {
  const outside = join(root, 'unrelated.txt'); writeFileSync(outside, 'keep');
  unlinkSync(join(target, 'catalog.json')); symlinkSync(outside, join(target, 'catalog.json'));
  assert.throws(() => projectCommerce({ root, write: true }), /symlink refused/);
  assert.equal(readFileSync(outside, 'utf8'), 'keep');
  assert.equal(readFileSync(join(target, 'recurring-commerce.mjs'), 'utf8'), 'old compiler\n');
}));
