import { test, expect } from 'bun:test';
import { readFileSync, mkdtempSync, mkdirSync, copyFileSync, existsSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { Database } from 'bun:sqlite';
import { compileSpec } from './compile_foundry_behavior.mjs';
import { compileRoles, SpecError } from './compile_role_program.mjs';

const root = resolve(import.meta.dir, '../../..');
const foundry = JSON.parse(readFileSync(join(root, 'schemas/control/foundry-behavior-specs.json'), 'utf8'));
const roles = JSON.parse(readFileSync(join(root, 'schemas/control/role-program-specs.json'), 'utf8'));
test('roles choose exact existing actions without creating Actor kinds or grants', () => {
  const result = compileRoles(roles, compileSpec(foundry));
  const programs = Object.fromEntries(result.programs.map(x => [x.role_id, x]));
  expect(new Set(Object.keys(programs))).toEqual(new Set(['brand-operator', 'go-to-market-engineer', 'internet-operator']));
  expect(programs['brand-operator'].exact_operations).toEqual([]);
  expect(new Set(programs['internet-operator'].exact_operations)).toEqual(new Set(['Community.join', 'Occurrence.follow', 'Project.support']));
  expect(programs['internet-operator'].unresolved_operations).toContain('Community.publish');
  expect(programs['internet-operator'].unresolved_operations).toContain('Occurrence.host');
  expect(programs['internet-operator'].authority_requirements).toContain('community:moderate');
  expect(result.cannot_infer).toContain('effect authority');
  expect(Object.values(programs['brand-operator'].surface_scene).every(x => !x.includes('authority'))).toBe(true);
});
test('native CMS stores all three compiled RolePrograms through its generic entry owner', () => {
  const cms = join(root, 'cmd/BonfyreCMS/bonfyre-cms');
  if (!existsSync(cms)) return;
  const tmp = mkdtempSync(join(tmpdir(), 'bonfyre-role-program-'));
  try {
    const schemas = join(tmp, 'schemas'); mkdirSync(schemas);
    copyFileSync(join(root, 'cmd/BonfyreCMS/content-types/role_program.json'), join(schemas, 'role_program.json'));
    const dbPath = join(tmp, 'roles.db');
    const run = argv => { const done = Bun.spawnSync(argv, { stdout: 'pipe', stderr: 'pipe' }); expect(done.exitCode).toBe(0); };
    run([cms, 'schema', 'migrate', '--db', dbPath, '--schemas', schemas]);
    for (const program of compileRoles(roles, compileSpec(foundry)).programs) run([cms, 'entry', 'create', 'role_program', JSON.stringify(program), '--ns', 'root', '--db', dbPath, '--schemas', schemas]);
    const db = new Database(dbPath, { readonly: true });
    expect(db.query("SELECT count(*) AS n FROM role_program WHERE status='published'").get().n).toBe(3);
    db.close();
  } finally { rmSync(tmp, { recursive: true, force: true }); }
});
test('role spec cannot silently omit work/authority fields', () => {
  const bad = structuredClone(roles); delete bad.roles[0].authority_requirements;
  expect(() => compileRoles(bad, compileSpec(foundry))).toThrow(SpecError);
});
