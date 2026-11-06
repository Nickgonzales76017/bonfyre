import { expect, test } from 'bun:test';
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

const helper = fileURLToPath(new URL('../pages-runtime-metadata.mjs', import.meta.url));

function fixture(callback) {
  const consumerRepo = mkdtempSync(join(tmpdir(), 'bonfyre-pages-metadata-'));
  const changedFile = join(consumerRepo, 'changed.txt');
  writeFileSync(changedFile, 'input/é.txt\ninput/a.mp3\n');
  const env = { ...process.env,
    APP_NAME: 'Café 🪼',
    GITHUB_REPOSITORY: 'owner/app',
    GITHUB_REF_NAME: 'main',
    GITHUB_SHA: 'a'.repeat(40),
    GITHUB_RUN_ID: '12345',
    GITHUB_RUN_ATTEMPT: '2',
    CHANGED_COUNT: '2',
    CHANGED_FILES_FILE: changedFile,
    DEPLOY_PATH: './site/output/',
  };
  const run = (changed = {}, remove = []) => {
    const childEnv = { ...env, ...changed };
    for (const name of remove) delete childEnv[name];
    return Bun.spawnSync([process.execPath, helper], { cwd: consumerRepo, env: childEnv });
  };
  try { callback({ consumerRepo, changedFile, run }); }
  finally { rmSync(consumerRepo, { recursive: true, force: true }); }
}

test('the actual CLI writes Python-compatible job and summary into the consumer repo', () => fixture(({ consumerRepo, run }) => {
  const result = run();
  expect(result.exitCode).toBe(0);
  const jobBytes = readFileSync(join(consumerRepo, 'site/output/job.json'), 'utf8');
  const summaryBytes = readFileSync(join(consumerRepo, 'site/output/summary.json'), 'utf8');
  expect(jobBytes.endsWith('\n')).toBe(true);
  expect(summaryBytes.endsWith('\n')).toBe(true);
  expect(jobBytes).toContain('"app_name": "Caf\\u00e9 \\ud83e\\udebc"');
  expect(jobBytes).toContain('"input/\\u00e9.txt"');
  const job = JSON.parse(jobBytes), summary = JSON.parse(summaryBytes);
  expect(job).toMatchObject({
    app_name: 'Café 🪼', repository: 'owner/app', ref: 'main', sha: 'a'.repeat(40),
    run_id: '12345', run_attempt: '2', changed_count: 2,
    changed_files: ['input/é.txt', 'input/a.mp3'],
  });
  expect(job.generated_at).toMatch(/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{6}Z$/);
  expect(summary).toEqual({ app_name: 'Café 🪼', status: 'processed', changed_count: 2,
    changed_files: ['input/é.txt', 'input/a.mp3'], deploy_path: 'site/output',
    artifact_manifest: 'site/output/artifact.json' });
  expect(existsSync(join(consumerRepo, 'scripts/pages-runtime-metadata.mjs'))).toBe(false);
}));

test('the CLI refuses missing, malformed, mismatched, and unreadable inputs before writing', () => {
  const cases = [
    { remove: ['APP_NAME'], message: 'APP_NAME' },
    { values: { GITHUB_REPOSITORY: 'not-a-repository' }, message: 'GITHUB_REPOSITORY' },
    { values: { GITHUB_SHA: 'short' }, message: 'GITHUB_SHA' },
    { values: { GITHUB_RUN_ATTEMPT: '0' }, message: 'GITHUB_RUN_ATTEMPT' },
    { values: { CHANGED_COUNT: '2.0' }, message: 'CHANGED_COUNT' },
    { values: { CHANGED_COUNT: '3' }, message: 'differs' },
    { values: { CHANGED_FILES_FILE: '/nonexistent/changed.txt' }, message: 'CHANGED_FILES_FILE' },
    { values: { DEPLOY_PATH: '' }, message: 'DEPLOY_PATH' },
  ];
  for (const { values = {}, remove = [], message } of cases) fixture(({ consumerRepo, run }) => {
    const result = run(values, remove);
    expect(result.exitCode).toBe(2);
    expect(result.stderr.toString()).toContain(message);
    expect(existsSync(join(consumerRepo, 'site'))).toBe(false);
  });
  fixture(({ consumerRepo, changedFile, run }) => {
    writeFileSync(changedFile, Buffer.from([0xff]));
    const result = run();
    expect(result.exitCode).toBe(2);
    expect(result.stderr.toString()).toContain('valid UTF-8');
    expect(existsSync(join(consumerRepo, 'site'))).toBe(false);
  });
  fixture(({ consumerRepo, changedFile, run }) => {
    writeFileSync(changedFile, 'input/a.txt\n\ninput/b.txt\n');
    const result = run({ CHANGED_COUNT: '3' });
    expect(result.exitCode).toBe(2);
    expect(result.stderr.toString()).toContain('empty path');
    expect(existsSync(join(consumerRepo, 'site'))).toBe(false);
  });
});

test('deploy path metadata keeps pathlib lexical .. components', () => fixture(({ consumerRepo, run }) => {
  const result = run({ DEPLOY_PATH: './site/../docs/' });
  expect(result.exitCode).toBe(0);
  const summary = JSON.parse(readFileSync(join(consumerRepo, 'docs/summary.json'), 'utf8'));
  expect(summary.deploy_path).toBe('site/../docs');
  expect(summary.artifact_manifest).toBe('site/../docs/artifact.json');
}));
