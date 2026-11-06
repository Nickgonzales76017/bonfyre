#!/usr/bin/env bun
// Synthetic latency-isolation benchmark; never discovers installed providers.
import { createHash } from 'node:crypto';
import { closeSync, fstatSync, lstatSync, mkdtempSync, mkdirSync, openSync, readSync, realpathSync, rmSync, writeFileSync, chmodSync } from 'node:fs';
import { tmpdir, cpus, platform, release } from 'node:os';
import { dirname, isAbsolute, join, resolve, relative } from 'node:path';
import { fileURLToPath } from 'node:url';
import { isDeepStrictEqual } from 'node:util';
import { createHypervisorDiscovery } from '../hypervisor_discovery.mjs';
import { findProbeExecutable, isNativeProbeExecutable } from '../native_probe.mjs';
import { projectRun } from '../project_runtime.mjs';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '../../..');
const SOURCES = ['integrations/mcp/native_probe.mjs', 'integrations/mcp/hypervisor_discovery.mjs',
  'integrations/mcp/project_runtime.mjs', 'integrations/mcp/spines_store.mjs',
  'integrations/mcp/canary_fixtures.mjs', 'integrations/mcp/canary_reviewed_scripts.json',
  'integrations/mcp/tests/hypervisor_discovery_benchmark.mjs'];
function fingerprint(path) {
  const fd = openSync(path, 'r');
  try {
    const before = fstatSync(fd, { bigint: true });
    if (!before.isFile() || before.size > 256n * 1024n * 1024n) throw new Error('benchmark input is not a bounded regular file');
    const hash = createHash('sha256'), block = Buffer.alloc(1024 * 1024); let count = 0, length;
    while ((length = readSync(fd, block, 0, block.length, null))) { count += length; if (count > Number(before.size)) throw new Error('benchmark input grew'); hash.update(block.subarray(0, length)); }
    const after = fstatSync(fd, { bigint: true }), named = lstatSync(path, { bigint: true });
    if (count !== Number(before.size) || ['dev', 'ino', 'size', 'mtimeNs', 'ctimeNs'].some(key => before[key] !== after[key] || after[key] !== named[key])) throw new Error('benchmark input changed');
    return { bytes: count, sha256: hash.digest('hex') };
  } finally { closeSync(fd); }
}
const median = values => [...values].sort((a, b) => a - b)[Math.floor(values.length / 2)];
const percentile95 = values => [...values].sort((a, b) => a - b)[Math.ceil(values.length * .95) - 1];
function withoutDurations(result) {
  return { providers: result.providers.map(provider => ({ ...provider, evidence: provider.evidence.map(probe => {
    const { duration_ms: _duration, ...rest } = probe; return rest;
  }) })) };
}
export async function benchmarkDiscovery({ pairs = 7, providers = 8, delayMs = 60, nodePath = findProbeExecutable('node') } = {}) {
  if (!Number.isSafeInteger(pairs) || pairs < 7 || pairs > 31 || !Number.isSafeInteger(providers) || providers < 4 || providers > 32 ||
      !Number.isSafeInteger(delayMs) || delayMs < 1 || delayMs > 1000 || !nodePath || !isNativeProbeExecutable(nodePath)) throw new Error('invalid benchmark parameters or native Node executable');
  const resolverPath = resolve(nodePath), resolverExecutable = realpathSync(resolverPath), resolver = fingerprint(resolverExecutable);
  const resolved = await projectRun([resolverPath, '-p', 'process.execPath'], { cwd: ROOT, env: { PATH: dirname(resolverPath) }, timeoutMs: 5000, maxOutputBytes: 4096 });
  if (resolved.exit_code !== 0 || !isAbsolute(resolved.stdout.trim()) || /[\r\n]/.test(resolved.stdout.trim()) || !isNativeProbeExecutable(resolved.stdout.trim()))
    throw new Error('native Node identity resolution failed');
  // Pin and execute the actual Node runtime, not a toolchain shim that might
  // select a different runtime on a subsequent child invocation.
  nodePath = realpathSync(resolved.stdout.trim());
  const nodeExecutable = realpathSync(nodePath);
  const sources = Object.fromEntries(SOURCES.map(path => [path, fingerprint(join(ROOT, path))])), node = fingerprint(nodeExecutable);
  const version = await projectRun([nodePath, '--version'], { cwd: ROOT, env: { PATH: dirname(nodePath) }, timeoutMs: 5000, maxOutputBytes: 1024 });
  if (version.exit_code !== 0 || !/^v\d+\.\d+\.\d+\s*$/.test(version.stdout)) throw new Error('native Node version probe failed');
  const home = realpathSync(mkdtempSync(join(tmpdir(), 'bonfyre-discovery-benchmark-')));
  const bin = join(home, 'bin'); mkdirSync(bin);
  const script = `#!${nodePath}\nsetTimeout(() => process.stdout.write("discovery-benchmark-fixture-v1\\n"), ${delayMs});\n`;
  const projects = Array.from({ length: providers }, (_, index) => ({ id: `fixture-${index}`, detect: [`fixture-${index}`] }));
  const registry = { projects }, env = { PATH: bin, LANG: 'C', LC_ALL: 'C' };
  let expected, expectedDigest;
  const samples = [];
  try {
    for (const project of projects) { const path = join(bin, project.detect[0]); writeFileSync(path, script, { mode: 0o700 }); chmodSync(path, 0o700); }
    for (let pair = 0; pair < pairs; pair++) {
      const order = pair % 2 === 0 ? [1, 4] : [4, 1];
      for (const concurrency of order) {
        let active = 0, peak = 0, calls = 0, commits = 0;
        const store = { registry: () => registry, recordDiscovery: value => {
          if (active || value.providers.length !== providers) throw new Error('partial discovery publication'); commits++;
        } };
        const owner = createHypervisorDiscovery({ store, root: home, env, concurrency, now: () => 1700000000,
          run: async (argv, options) => {
            calls++; active++; peak = Math.max(peak, active);
            try { return await projectRun(argv, options); } finally { active--; }
          } });
        const start = performance.now(), result = await owner.discover(), elapsed_ms = performance.now() - start;
        if (calls !== providers || commits !== 1 || active || peak !== concurrency || result.providers.some(item => item.health !== 'reachable')) throw new Error('benchmark execution contract failed');
        const semantic = withoutDurations(result);
        if (!expected) { expected = semantic; expectedDigest = createHash('sha256').update(JSON.stringify(semantic)).digest('hex'); }
        else if (!isDeepStrictEqual(semantic, expected)) throw new Error('benchmark semantic outputs differ');
        samples.push({ pair, order, concurrency, elapsed_ms, peak_in_flight: peak, subprocesses: calls, atomic_publications: commits, semantic_digest: expectedDigest });
      }
    }
    for (const [path, identity] of Object.entries(sources)) if (!isDeepStrictEqual(identity, fingerprint(join(ROOT, path)))) throw new Error('benchmark implementation changed during measurements');
    if (realpathSync(nodePath) !== nodeExecutable || !isDeepStrictEqual(node, fingerprint(nodeExecutable))) throw new Error('benchmark Node executable changed');
    if (realpathSync(resolverPath) !== resolverExecutable || !isDeepStrictEqual(resolver, fingerprint(resolverExecutable))) throw new Error('benchmark runtime resolver changed');
    const serial = samples.filter(sample => sample.concurrency === 1).map(sample => sample.elapsed_ms);
    const parallel = samples.filter(sample => sample.concurrency === 4).map(sample => sample.elapsed_ms);
    return { schema: 'bonfyre.hypervisor-discovery-benchmark.v1', state: 'passed', generated_at: new Date().toISOString(),
      command: 'bun integrations/mcp/tests/hypervisor_discovery_benchmark.mjs --output integrations/mcp/tests/hypervisor_discovery_benchmark.json',
      scope: `${providers} disposable native-Node children per scan; each waits ${delayMs}ms then prints a fixed line. Measures bounded process overlap, not production provider latency or capability.`,
      limitations: ['Ordinary development host; no CPU affinity or exclusive host control.', 'Repeated fixture commands do not establish latency, reachability, or correctness of actual providers.'],
      production_speed_claim: false, pairs, providers, child_delay_ms: delayMs, samples_per_configuration: pairs,
      excluded_semantic_fields: ['providers[].evidence[].duration_ms'], semantic_outputs_equal: true, sources_unchanged: true,
      sources, runtime: { bun: Bun.version, node: { invocation_path: nodePath, executable_path: nodeExecutable, version: version.stdout.trim(), ...node },
        node_resolver: { invocation_path: resolverPath, executable_path: resolverExecutable, ...resolver },
        os: platform(), release: release(), arch: process.arch, cpu: cpus()[0]?.model ?? null },
      fixture: { path: home, retained: false, script_sha256: createHash('sha256').update(script).digest('hex'),
        provider_registry_sha256: createHash('sha256').update(JSON.stringify(registry)).digest('hex') },
      summary: { serial_p50_ms: median(serial), serial_p95_ms: percentile95(serial), parallel_p50_ms: median(parallel),
        parallel_p95_ms: percentile95(parallel), p50_ratio: median(serial) / median(parallel) }, samples };
  } finally { rmSync(home, { recursive: true, force: true }); }
}
if (import.meta.main) {
  const args = process.argv.slice(2);
  if (args.length !== 0 && (args.length !== 2 || args[0] !== '--output')) throw new Error('usage: benchmark [--output report.json]');
  const report = await benchmarkDiscovery();
  if (args.length) {
    const path = resolve(args[1]), rel = relative(ROOT, path);
    if (!rel || rel.startsWith('..') || rel.startsWith('/')) throw new Error('benchmark report must be inside workspace');
    writeFileSync(path, `${JSON.stringify(report, null, 2)}\n`, { mode: 0o600 });
  }
  console.log(JSON.stringify({ state: report.state, pairs: report.pairs, summary: report.summary, sources_unchanged: report.sources_unchanged }));
}
