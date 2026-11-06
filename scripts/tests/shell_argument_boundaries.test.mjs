// What this does not prove:
// - a script written in this test stands where a program would run, so it shows how the caller handles that program's interface and nothing about the program.
import { test, expect } from 'bun:test';
import { strict as assert } from 'node:assert';
import { cpSync, existsSync, mkdtempSync, mkdirSync, readFileSync, readdirSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

const scripts = fileURLToPath(new URL('../', import.meta.url));
const fixture = work => {
  const base = mkdtempSync(join(tmpdir(), 'bonfyre-shell-arguments-'));
  const root = join(base, 'space ;$() [literal]');
  mkdirSync(root);
  try { work(root); }
  finally { rmSync(base, { recursive: true, force: true }); }
};
const run = (script, args, cwd, env = {}) => Bun.spawnSync(['bash', script, ...args], {
  cwd, env: { ...process.env, ...env }, stdout: 'pipe', stderr: 'pipe',
});

test('artifact dry-run keeps mapped spaces and metacharacters in one path', () => fixture(root => {
  const demo = join(root, 'site', 'demos', 'demo space;[x]');
  const sourceFile = 'file one;[*].txt';
  const sourceDir = 'directory $name [x]';
  mkdirSync(join(demo, sourceDir), { recursive: true });
  writeFileSync(join(demo, sourceFile), 'demo');
  writeFileSync(join(demo, sourceDir, '.hidden'), 'hidden');
  const projects = join(root, 'projects with spaces');
  const repo = join(projects, 'pages repo;[x]');
  mkdirSync(join(repo, 'site'), { recursive: true });
  const map = join(root, 'mapping name;[x].json');
  writeFileSync(map, JSON.stringify({ mappings: [{ demo_dir: demo,
    remote: 'https://example.invalid/pages repo;[x].git', paths: [sourceFile, sourceDir] }] }));
  const result = run(join(scripts, 'move_artifacts_to_site.sh'), ['--map', map, '--dry-run'], root,
    { BONFYRE_PAGES_PROJECTS_DIR: projects });
  assert.equal(result.exitCode, 0, result.stderr.toString());
  const output = result.stdout.toString();
  expect(output).toContain(`would copy file ${join(demo, sourceFile)} -> ${join(repo, 'site', sourceFile)}`);
  expect(output).toContain(`would copy dir ${join(demo, sourceDir)} -> ${join(repo, 'site', sourceDir)}`);
  expect(readdirSync(join(repo, 'site'))).toEqual([]);
  const missing = run(join(scripts, 'move_artifacts_to_site.sh'), ['--map'], root);
  expect(missing.exitCode).toBe(2);
}));

test('E2E discovery preserves an executable filename with spaces and metacharacters', () => fixture(root => {
  const bin = join(root, 'cmd', 'group', 'bonfyre-status file;[x]');
  const capture = join(root, 'called.txt');
  mkdirSync(join(root, 'cmd', 'group'), { recursive: true });
  writeFileSync(bin, '#!/bin/sh\nprintf "%s\\n" "$0" >> "$CAPTURE"\n[ "$1" = status ]\n', { mode: 0o755 });
  const result = run(join(scripts, 'e2e_test_live_apps.sh'), [], root, { CAPTURE: capture });
  assert.equal(result.exitCode, 0, result.stderr.toString());
  expect(readFileSync(capture, 'utf8').trim()).toBe('./cmd/group/bonfyre-status file;[x]');
  expect(readFileSync(join(root, 'site', 'demos', 'e2e-results', 'bonfyre-status file;[x]', 'summary.txt'), 'utf8'))
    .toContain('status OK');
}));

test('demo runner passes special-character source paths as one argv element to fake binaries only', () => fixture(root => {
  mkdirSync(join(root, 'scripts'), { recursive: true });
  mkdirSync(join(root, 'site', 'demos'), { recursive: true });
  writeFileSync(join(root, 'site', 'demos', 'test_a.wav'), 'fixture');
  cpSync(join(scripts, 'run_live_app_demos.sh'), join(root, 'scripts', 'run_live_app_demos.sh'));
  const fake = join(root, 'cmd', 'BonfyreBrief', 'BonfyreBrief');
  mkdirSync(join(root, 'cmd', 'BonfyreBrief'), { recursive: true });
  writeFileSync(fake, '#!/bin/sh\nprintf "%s\\0" "$@" >> "$CAPTURE_ARGS"\n', { mode: 0o755 });
  const capture = join(root, 'received-argv.bin');
  const result = run(join(root, 'scripts', 'run_live_app_demos.sh'), [], root,
    { CAPTURE_ARGS: capture });
  assert.equal(result.exitCode, 0, result.stderr.toString());
  expect(existsSync(capture)).toBe(true);
  const args = readFileSync(capture, 'utf8').split('\0');
  expect(args).toContain(join(root, 'site', 'demos', 'test_a.wav'));
  expect(args).toContain(join(root, 'site', 'demos', 'shift_handoff_brief.json'));
  expect(readFileSync(join(root, 'site', 'demos', 'BonfyreBrief', 'run.log'), 'utf8'))
    .toContain('--- running BonfyreBrief');
}));
