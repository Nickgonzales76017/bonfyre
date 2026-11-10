// What this does not prove:
// - some checks run in embedded Python, which this estate has retired; they say nothing about a native path.
import { afterEach, expect, test } from 'bun:test';
import { mkdtempSync, mkdirSync, rmSync, writeFileSync, copyFileSync, chmodSync, realpathSync, symlinkSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { createHash } from 'node:crypto';
import { Database } from 'bun:sqlite';
import { createHypervisorDiscovery } from '../hypervisor_discovery.mjs';
import { createHypervisorStore } from '../hypervisor_store.mjs';
import { projectRun } from '../project_runtime.mjs';

const homes = [], stores = [];
afterEach(() => { for (const store of stores.splice(0)) store.close(); for (const home of homes.splice(0)) rmSync(home, { recursive: true, force: true }); });
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
function fixture(count = 5) {
  const home = realpathSync(mkdtempSync(join(tmpdir(), 'bonfyre-hypervisor-discovery-'))); homes.push(home);
  const bin = join(home, 'bin'); mkdirSync(bin);
  const projects = Array.from({ length: count }, (_, i) => ({ id: `provider-${i}`, detect: [`probe-${i}`], repo: `fixture/provider-${i}`,
    homepage: 'https://example.test', license: 'MIT', integration: 'native', default_mode: 'candidate', capabilities: ['fixture.read'] }));
  const registry = { projects, capability_contracts: ['fixture.read'] }, commits = [], calls = [];
  const store = { registry: () => registry, recordDiscovery: value => commits.push(structuredClone(value)) };
  const env = { PATH: bin };
  const native = name => { const path = join(bin, name); copyFileSync('/bin/echo', path); chmodSync(path, 0o700); return path; };
  const script = (name, body) => { const path = join(bin, name); writeFileSync(path, body); chmodSync(path, 0o700); return path; };
  return { home, bin, env, registry, projects, commits, calls, store, native, script };
}
test('constructors are lazy and discovery snapshots registry with stable order and bounded parallelism', async () => {
  const f = fixture(9); for (const project of f.projects) f.native(project.detect[0]);
  let active = 0, peak = 0, completed = 0;
  const owner = createHypervisorDiscovery({ store: f.store, root: f.home, env: f.env, now: () => 42,
    run: async (argv, options) => {
      active++; peak = Math.max(peak, active); f.calls.push({ argv, options });
      expect(f.commits).toHaveLength(0); await sleep(15); active--; completed++;
      return { exit_code: 0, stdout: 'credential=private-version-line', stderr: 'private-error', duration_ms: 15 };
    } });
  expect(f.calls).toHaveLength(0); expect(f.commits).toHaveLength(0);
  const pending = owner.discover();
  await expect(owner.discover()).rejects.toThrow('already running');
  const result = await pending;
  expect(peak).toBe(4); expect(completed).toBe(9); expect(f.commits).toHaveLength(1); expect(f.commits[0].observed_at).toBe(42);
  expect(result.providers.map(item => item.provider)).toEqual(f.projects.map(item => item.id));
  expect(f.calls.every(item => item.options.timeoutMs === 12000 && item.options.maxOutputBytes === 2097152)).toBe(true);
  expect(JSON.stringify(result)).not.toContain('private-version-line'); expect(JSON.stringify(result)).not.toContain('private-error');
  expect(result.providers.every(item => item.version === 'reachable')).toBe(true);
  expect(result.providers[0].evidence[0].stdout_sha256).toBe(createHash('sha256').update('credential=private-version-line').digest('hex'));
});
test('fallback probe order remains per-provider; missing, refused, timed-out and overflow are not reachable', async () => {
  const f = fixture(5); f.native('probe-0'); f.native('probe-1'); f.native('probe-2');
  f.script('probe-3', '#!/usr/bin/env python3\n');
  const owner = createHypervisorDiscovery({ store: f.store, root: f.home, env: f.env, run: async argv => {
    f.calls.push(argv); if (argv[0].endsWith('probe-0')) return { exit_code: argv[1] === '--help' ? 0 : 2 };
    if (argv[0].endsWith('probe-1')) return { exit_code: 0, overflow: true };
    return { exit_code: 0, timeout: true };
  } });
  const result = await owner.discover();
  expect(f.calls.filter(argv => argv[0].endsWith('probe-0')).map(argv => argv[1])).toEqual(['--version', 'version', '--help']);
  expect(result.providers.map(item => item.health)).toEqual(['reachable', 'degraded', 'degraded', 'degraded', 'missing']);
  expect(result.providers[3].evidence[0].refused).toBe('native-or-node-bun-executable-required');
  expect(f.calls).toHaveLength(5); expect(f.commits).toHaveLength(1);
});
test('unknown shell and Python trampolines refuse; direct Bun script is admitted', async () => {
  const f = fixture(3);
  f.script('probe-0', '#!/bin/sh\nexec python3 tool.py\n');
  f.script('probe-1', '#!/usr/bin/env python3\n');
  f.script('probe-2', `#!${process.execPath}\nconsole.log("ok");\n`);
  const owner = createHypervisorDiscovery({ store: f.store, root: f.home, env: f.env, run: async argv => { f.calls.push(argv); return { exit_code: 0 }; } });
  const result = await owner.discover();
  expect(f.calls).toHaveLength(1); expect(f.calls[0][0]).toBe(realpathSync(process.execPath));
  expect(result.providers.map(item => item.health)).toEqual(['degraded', 'degraded', 'reachable']);
});
test('native symlink admission preserves argv0 instead of changing multicall command semantics', async () => {
  const f = fixture(1), actual = f.native('multicall');
  symlinkSync(actual, join(f.bin, 'probe-0'));
  const owner = createHypervisorDiscovery({ store: f.store, root: f.home, env: f.env, run: async argv => { f.calls.push(argv); return { exit_code: 0 }; } });
  expect((await owner.discover()).providers[0].health).toBe('reachable');
  expect(f.calls[0][0]).toBe(join(f.bin, 'probe-0'));
  expect(f.calls[0][0]).not.toBe(actual);
});
test('existing exact reviewed wrapper is admitted but changed bytes are refused', async () => {
  const f = fixture(1); f.projects[0].detect = ['bonfyre-cavekit-provider'];
  // Historical reviewed bytes are intentionally exact, including the old root;
  // the runner is mocked, so this never reads or executes that live installation.
  const bytes = [ '#!/bin/sh', 'set -eu', 'ROOT="/Users/nickgonzales/Documents/Bonfyre"', 'found=0',
    'for candidate in   "$ROOT/.claude/skills/spec/SKILL.md"   "$ROOT/.claude/skills/build/SKILL.md"   "$ROOT/.agents/skills/spec/SKILL.md"   "$ROOT/.agents/skills/build/SKILL.md"   "$HOME/.claude/skills/spec/SKILL.md"',
    'do', '  if [ -f "$candidate" ]; then', '    found=1', '    break', '  fi', 'done', '[ "$found" -eq 1 ] || {',
    '  echo "cavekit provider is not installed" >&2', '  exit 1', '}', 'case "${1:-status}" in', '  --version|version)',
    '    echo "bonfyre-cavekit-provider 1"', '    ;;', '  status)', '    echo \'{"status":"ok","provider":"cavekit","durable_spec":true}\'',
    '    ;;', '  --help|-h)', '    echo "usage: bonfyre-cavekit-provider [status|--version]"', '    ;;', '  *)',
    '    echo "unknown command: $1" >&2', '    exit 2', '    ;;', 'esac', '' ].join('\n');
  const path = f.script('bonfyre-cavekit-provider', bytes);
  const owner = createHypervisorDiscovery({ store: f.store, root: f.home, env: f.env, run: async argv => { f.calls.push(argv); return { exit_code: 0 }; } });
  const result = await owner.discover(); expect(result.providers[0].health).toBe('reachable');
  expect(result.providers[0].evidence[0].runtime_boundary.kind).toBe('reviewed_script');
  expect(result.providers[0].evidence[0].runtime_boundary.sha256).toBe('442010338ae429bd0b08a13d70f4a7815a35100527cdcbd8f1043881576fae43');
  writeFileSync(path, `${bytes}# changed\n`);
  expect((await owner.discover()).providers[0].health).toBe('degraded'); expect(f.calls).toHaveLength(1);
});
test('STOP during parallel subprocesses cancels and joins all lanes without partial persistence', async () => {
  const f = fixture(8); for (const project of f.projects) f.native(project.detect[0]);
  let stopped = false, active = 0, exited = 0;
  const owner = createHypervisorDiscovery({ store: f.store, root: f.home, env: f.env, checkStop: () => { if (stopped) throw Error('STOP'); },
    run: async (_argv, { signal }) => { active++; await new Promise(resolve => signal.addEventListener('abort', resolve, { once: true })); active--; exited++; return { exit_code: 130, cancelled: true }; } });
  const pending = owner.discover(); setTimeout(() => { stopped = true; }, 15);
  await expect(pending).rejects.toThrow('STOP'); expect(active).toBe(0); expect(exited).toBe(4); expect(f.commits).toHaveLength(0);
});
test('invalid registry and pre-abort perform no probes or writes', async () => {
  const f = fixture(1), owner = createHypervisorDiscovery({ store: f.store, env: f.env, run: async argv => f.calls.push(argv) });
  const controller = new AbortController(); controller.abort();
  await expect(owner.discover({ signal: controller.signal })).rejects.toThrow();
  f.projects[0].detect = ['--help/../../python']; await expect(owner.discover()).rejects.toThrow('commands');
  expect(f.calls).toHaveLength(0); expect(f.commits).toHaveLength(0);
});
test('real subprocess witnesses overlap while complete observations commit once into real SQLite', async () => {
  const f = fixture(6), registryPath = join(f.home, 'registry.json'), storeHome = join(f.home, 'state');
  writeFileSync(registryPath, JSON.stringify(f.registry));
  for (const project of f.projects) f.script(project.detect[0], `#!${process.execPath}\nawait Bun.sleep(60); console.log("private fixture output");\n`);
  const store = createHypervisorStore({ home: storeHome, registryPath, now: () => 1700000000 }); stores.push(store);
  let active = 0, peak = 0;
  const owner = createHypervisorDiscovery({ store, root: f.home, env: f.env, run: async (argv, options) => {
    active++; peak = Math.max(peak, active); try { return await projectRun(argv, options); } finally { active--; }
  } });
  const result = await owner.discover(); expect(peak).toBe(4); expect(active).toBe(0);
  expect(result.providers.every(item => item.health === 'reachable')).toBe(true);
  const db = new Database(store.dbPath, { readonly: true });
  try {
    expect(db.query('SELECT count(*) AS n FROM providers WHERE installed=1').get().n).toBe(6);
    expect(db.query("SELECT count(*) AS n FROM events WHERE kind='ecosystem.discovered'").get().n).toBe(1);
    expect(JSON.stringify(db.query('SELECT * FROM events').all())).not.toContain('private fixture output');
  } finally { db.close(); }
});
test('paired real-process fixture benchmark compares one and four lanes without a production speed claim', async () => {
  const f = fixture(8);
  for (const project of f.projects) f.script(project.detect[0], `#!${process.execPath}\nawait Bun.sleep(60); console.log("fixture-version");\n`);
  const timings = [], outputs = [];
  for (const concurrency of [1, 4]) {
    const owner = createHypervisorDiscovery({ store: f.store, root: f.home, env: f.env, run: projectRun, concurrency });
    const started = performance.now(); outputs.push(await owner.discover());
    timings.push({ concurrency, elapsed_ms: Math.round((performance.now() - started) * 100) / 100 });
  }
  const semantic = result => result.providers.map(item => ({ provider: item.provider, installed: item.installed, health: item.health,
    outputs: item.evidence.map(probe => [probe.exit_code, probe.stdout_sha256, probe.stderr_sha256]) }));
  expect(semantic(outputs[0])).toEqual(semantic(outputs[1]));
  expect(timings.every(item => item.elapsed_ms > 0)).toBe(true);
  process.stdout.write(`${JSON.stringify({ schema: 'bonfyre.discovery-fixture-benchmark.v1', providers: 8, subprocess: 'Bun sleep(60ms) then fixed output',
    samples_per_configuration: 1, semantic_outputs_equal: true, production_speed_claim: false, timings })}\n`);
});
