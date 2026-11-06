import { afterEach, expect, test } from 'bun:test';
import { createHash } from 'node:crypto';
import { mkdtempSync, rmSync, writeFileSync, mkdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { canonicalJson } from '../canonical_json.mjs';
import { createHypervisorSources } from '../hypervisor_sources.mjs';
import { createHypervisorStore } from '../hypervisor_store.mjs';
import { nativeExecutionPlan } from '../native_probe.mjs';
import { projectRun } from '../project_runtime.mjs';

const fixtures = [];
afterEach(() => { for (const { home, store } of fixtures.splice(0)) { store?.close(); rmSync(home, { recursive: true, force: true }); } });
const sha = value => createHash('sha256').update(value).digest('hex');
const git = nativeExecutionPlan('git')?.argv[0];
function localGit(args, cwd) {
  const result = Bun.spawnSync({ cmd: [git, ...args], cwd, stdout: 'pipe', stderr: 'pipe',
    env: { ...process.env, GIT_CONFIG_NOSYSTEM: '1', GIT_CONFIG_GLOBAL: '/dev/null', GIT_TERMINAL_PROMPT: '0' } });
  if (result.exitCode !== 0) throw new Error(`local Git fixture failed: ${new TextDecoder().decode(result.stderr)}`);
  return new TextDecoder().decode(result.stdout).trim();
}
function fixture() {
  const home = mkdtempSync(join(tmpdir(), 'bonfyre-hypervisor-source-'));
  const registryPath = join(home, 'ecosystem_registry.json');
  writeFileSync(registryPath, JSON.stringify({ capability_contracts: ['agent.executor'], projects: [{
    id: 'sample', repo: 'fixture/sample', homepage: 'https://example.test/sample', license: 'MIT',
    integration: 'native', default_mode: 'candidate', detect: ['sample'], capabilities: ['agent.executor'],
  }] }));
  const origin = join(home, 'origin'); mkdirSync(origin);
  localGit(['init', '-q'], origin);
  writeFileSync(join(origin, 'README.md'), 'A local Git fixture\n');
  writeFileSync(join(origin, 'LICENSE'), 'MIT License\n');
  writeFileSync(join(origin, 'package.json'), JSON.stringify({ name: 'sample', bin: { sample: './cli.js' } }));
  localGit(['add', '.'], origin);
  localGit(['-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.test', 'commit', '-q', '-m', 'initial'], origin);
  const first = localGit(['rev-parse', 'HEAD'], origin);
  const store = createHypervisorStore({ home, registryPath, now: () => 1791000000 });
  store.loadRegistry(); fixtures.push({ home, store });
  const run = (argv, options) => {
    const changed = argv.map(item => item === 'https://github.com/fixture/sample.git' ? origin : item);
    return projectRun(changed, { ...options, env: { ...options.env, GIT_ALLOW_PROTOCOL: 'file' } });
  };
  return { home, store, origin, first, run, make: options => createHypervisorSources({ store, run, ...options }) };
}

test.skipIf(!git)('source ingest uses real local Git fixture, preserves Python manifest hash, and verifies reuse', async () => {
  const f = fixture(), owner = f.make();
  const result = await owner.ingestSource('https://github.com/fixture/sample.git');
  expect(result).toMatchObject({ state: 'inspected', repo: 'fixture/sample', resolved_ref: f.first, license: 'MIT' });
  const row = f.store.getSource(result.source_id);
  const material = Object.fromEntries(['repo', 'resolved_ref', 'license', 'manifests', 'commands', 'files'].map(key => [key, row.manifest[key]]));
  expect(result.digest).toBe(sha(canonicalJson(material, { spaced: true })));
  expect(row.manifest.commands).toContain('sample');
  expect(f.store.provider('sample').source_ref).toBe(f.first);
  expect(f.store.verifyReceiptChain().verified).toBe(true);
  const reused = await owner.ingestSource('fixture/sample');
  expect(reused.reused).toBe(true);
  expect(f.store.summary().sources).toBe(1);
  writeFileSync(join(result.checkout, 'README.md'), 'changed\n');
  await expect(owner.ingestSource('fixture/sample')).rejects.toThrow('not clean');
});

test.skipIf(!git)('update check persists candidate once without promoting or downloading remote code', async () => {
  const f = fixture(), owner = f.make();
  await owner.ingestSource('fixture/sample');
  writeFileSync(join(f.origin, 'README.md'), 'second commit\n');
  localGit(['add', '.'], f.origin);
  localGit(['-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.test', 'commit', '-q', '-m', 'second'], f.origin);
  const second = localGit(['rev-parse', 'HEAD'], f.origin);
  const first = await owner.sourceUpdateCheck({ provider_id: 'sample' });
  expect(first.updates[0]).toMatchObject({ state: 'candidate', old_ref: f.first, new_ref: second });
  expect(f.store.provider('sample').source_ref).toBe(f.first);
  const repeated = await owner.sourceUpdateCheck({ provider_id: 'sample' });
  expect(repeated.updates[0].candidate.reused).toBe(true);
  expect(f.store.verifyReceiptChain().verified).toBe(true);
});

test.skipIf(!git)('registry gate, ref admission, test-execution refusal and interrupted attempt prevent effects', async () => {
  const f = fixture(), owner = f.make();
  let subprocesses = 0;
  const noRun = f.make({ run: async () => { subprocesses++; throw new Error('unexpected subprocess'); } });
  await expect(noRun.ingestSource('other/repo')).rejects.toThrow('not declared');
  await expect(noRun.ingestSource('fixture/sample', { ref: '--upload-pack=evil' })).rejects.toThrow('ref');
  await expect(noRun.ingestSource('fixture/sample', { execute_tests: true })).rejects.toThrow('sandboxed');
  expect(subprocesses).toBe(0);
  const id = `source-${sha('fixture/sample\0HEAD').slice(0, 24)}`;
  f.store.beginSource({ sourceId: id, repo: 'fixture/sample', requestedRef: null, checkoutPath: join(f.store.home, 'sources', id) });
  await expect(owner.ingestSource('fixture/sample')).rejects.toThrow('reconciliation');
  expect(f.store.getSource(id).state).toBe('cloning');
  expect(f.store.verifyReceiptChain().verified).toBe(true);
});

test.skipIf(!git)('failed clone is a durable failed attempt with no provider binding or automatic retry', async () => {
  const f = fixture();
  const owner = f.make({ run: async () => ({ exit_code: 1, stdout: '', stderr: 'fixture failure', duration_ms: 1 }) });
  await expect(owner.ingestSource('fixture/sample')).rejects.toThrow('clone failed');
  const sourceId = `source-${sha('fixture/sample\0HEAD').slice(0, 24)}`;
  expect(f.store.getSource(sourceId).state).toBe('failed');
  expect(f.store.provider('sample').source_ref).toBeNull();
  expect(f.store.verifyReceiptChain().verified).toBe(true);
  await expect(owner.ingestSource('fixture/sample')).rejects.toThrow('reconciliation');
});
