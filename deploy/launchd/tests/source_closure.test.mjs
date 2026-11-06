import { afterEach, expect, test } from 'bun:test';
import { mkdtempSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { deploymentSourceClosure } from '../source_closure.mjs';
const fixtures = [];
afterEach(() => { for (const path of fixtures.splice(0)) rmSync(path, { recursive: true, force: true }); });
function fixture() { const root = mkdtempSync(join(tmpdir(), 'bonfyre-source-closure-')); fixtures.push(root); return root; }
test('cyclic and shared imports pin once; declared sidecars join the same source identity', () => {
  const root = fixture();
  writeFileSync(join(root, 'a.mjs'), '#!/usr/bin/env bun\nimport "./b.mjs"; import "node:fs"; export const value=1;');
  writeFileSync(join(root, 'b.mjs'), 'export {value} from "./a.mjs";');
  writeFileSync(join(root, 'schema.sql'), 'CREATE TABLE example(id);');
  const input = { root, entries: ['a.mjs'], sidecars: ['schema.sql'] };
  const first = deploymentSourceClosure(input);
  expect(first.map(item => item.name)).toEqual(['a.mjs', 'b.mjs', 'schema.sql']);
  writeFileSync(join(root, 'b.mjs'), 'export {value} from "./a.mjs"; export const changed=2;');
  expect(deploymentSourceClosure(input)[1].sha256).not.toBe(first[1].sha256);
});
test('unowned package dependencies, escapes and symlinked sources refuse promotion input construction', () => {
  const root = fixture(); writeFileSync(join(root, 'a.mjs'), 'import "unexpected-package";');
  expect(() => deploymentSourceClosure({ root, entries: ['a.mjs'] })).toThrow('unowned package');
  expect(() => deploymentSourceClosure({ root, entries: ['../outside.mjs'] })).toThrow('escapes');
  symlinkSync(join(root, 'a.mjs'), join(root, 'alias.mjs'));
  expect(() => deploymentSourceClosure({ root, entries: ['alias.mjs'] })).toThrow('symlink');
});
