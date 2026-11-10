#!/usr/bin/env bun
// Same resident archive and verifier; compare event-loop isolation, not remote scale.
import assert from 'node:assert/strict';
import { readFileSync, realpathSync, statSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { dirname } from 'node:path';
import { createOwnedWorkerPool } from '../owned_worker_pool.mjs';
import { createV6Bfp } from '../v6_bfp.mjs';
const archive = realpathSync(process.argv[2] || ''), root = dirname(archive);
const pool = createOwnedWorkerPool({ workerUrl: new URL('../v6_bfp_worker.mjs', import.meta.url), maxWorkers: 2, timeoutMs: 120000 });
const direct = createV6Bfp({ root, home: root });
const job = { operation: 'verify', root, home: root, path: archive };
const sourcePaths = ['../owned_worker_pool.mjs', '../v6_bfp_worker.mjs', '../v6_bfp.mjs', '../canonical_json.mjs', './v6_bfp_worker_benchmark.mjs'];
const sourceDigests = () => Object.fromEntries(sourcePaths.map(path => [path, createHash('sha256').update(readFileSync(new URL(path, import.meta.url))).digest('hex')]));
const sources = sourceDigests();
const results = { direct: [], worker: [] };
async function sample(fn) {
  let last = performance.now(), maximum = 0, ticks = 0;
  const timer = setInterval(() => { const now = performance.now(); maximum = Math.max(maximum, now - last); last = now; ticks++; }, 2);
  const started = performance.now();
  try {
    const value = await fn(), elapsed = performance.now() - started;
    await new Promise(resolve => setTimeout(resolve, 4));
    return { value, elapsed_ms: elapsed, maximum_timer_gap_ms: maximum, timer_ticks: ticks };
  } finally { clearInterval(timer); }
}
try {
  const baseline = await direct.verify(archive), warm = await pool.run(job);
  assert.equal(warm.sha256, baseline.sha256); assert.deepEqual(warm.reconstructed, baseline.reconstructed);
  for (let i = 0; i < 3; i++) for (const method of i % 2 ? ['worker', 'direct'] : ['direct', 'worker']) {
    const { value, ...measurement } = await sample(() => method === 'worker' ? pool.run(job) : direct.verify(archive));
    assert.equal(value.verified, true); assert.equal(value.sha256, baseline.sha256);
    assert.deepEqual(value.reconstructed, baseline.reconstructed); results[method].push(measurement);
  }
  const summary = Object.fromEntries(Object.entries(results).map(([method, samples]) => [method, {
    p50_ms: samples.map(s => s.elapsed_ms).sort((a, b) => a - b)[1],
    maximum_timer_gap_ms: Math.max(...samples.map(s => s.maximum_timer_gap_ms)), samples,
  }]));
  assert.deepEqual(sourceDigests(), sources, 'benchmark source changed during measurement');
  console.log(JSON.stringify({ schema: 'bonfyre.bfp-worker-isolation-benchmark.v1', measured_at: new Date().toISOString(), sources,
    runtime: 'bun', node_compat_version: process.version, bun: Bun.version, platform: `${process.platform}-${process.arch}`, archive_bytes: statSync(archive).size,
    archive_sha256: baseline.sha256, verified_sections: baseline.reconstructed.length, warmup_pairs: 1, measured_pairs: 3, summary,
    scope: 'Local warmed same-file same-verifier comparison. Timer gap measures main-event-loop interference, not distributed throughput or compression gain.' }, null, 2));
} finally { await pool.close(); }
