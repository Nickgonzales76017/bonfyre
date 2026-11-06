import { test, expect } from 'bun:test';
import { readFileSync, mkdtempSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';

const cmd = join(import.meta.dir, '..');
const source = readFileSync(join(cmd, 'src/main.c'), 'utf8');
const binary = join(cmd, 'bonfyre-cms');
const schemas = join(cmd, 'content-types');
const scratch = mkdtempSync(join(tmpdir(), 'bonfyre-cms-dispatch-'));
const db = join(scratch, 'cms.db');
const run = (...args) => Bun.spawnSync([binary, ...args, '--db', db, '--schemas', schemas], { stdout: 'pipe', stderr: 'pipe' });
const runBare = (...args) => Bun.spawnSync([binary, ...args], { stdout: 'pipe', stderr: 'pipe', env: { ...process.env, BONFYRE_CMS_DB: db, BONFYRE_CMS_SCHEMAS: schemas } });

test('one sorted command vocabulary covers all second-level predicates', () => {
  const table = source.slice(source.indexOf('#define CMS_SUBCOMMANDS(X)'), source.indexOf('enum {', source.indexOf('#define CMS_SUBCOMMANDS(X)')));
  const names = [...table.matchAll(/X\("([^"]+)", CS_[A-Z_]+\)/g)].map(match => match[1]);
  expect(names.length).toBe(51);
  expect(names.filter(name=>name==='source-publication')).toEqual(['source-publication']);
  expect(names.filter(name=>name==='community-work')).toEqual(['community-work']);
  expect(names).toEqual([...new Set(names)].sort());
  expect(source.match(/CMS_SUB_IS\(CS_[A-Z_]+\)/g)?.length).toBe(68);
  expect(source).not.toMatch(/strcmp\(argv\[2\],/);
});

test('schema and entry subcommands retain syntax, output and unknown-verb codes', () => {
  try {
    const schema = run('schema', 'list');
    expect(schema.exitCode).toBe(0);
    expect(schema.stdout.toString()).toContain('NAME');
    const entry = run('entry', 'list', 'frappe_app');
    expect(entry.exitCode).toBe(0);
    expect(JSON.parse(entry.stdout.toString())).toEqual({ data: [], meta: { total: 0 } });
    for (const family of ['schema', 'entry', 'ops', 'tensor']) {
      const unknown = run(family, 'unknown-second-verb');
      expect(unknown.exitCode).toBe(1);
      expect(unknown.stderr.toString()).toContain(`Unknown ${family} command`);
    }
    const missing = runBare('schema', 'apply');
    expect(missing.exitCode).toBe(1);
    expect(missing.stderr.toString()).toContain('schema apply <file.json>');
  } finally { rmSync(scratch, { recursive: true, force: true }); }
});
