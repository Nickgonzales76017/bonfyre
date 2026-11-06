#!/usr/bin/env bun
// Read-only source-bound comparison; the installed database is copied and never opened for mutation.
import { createHash } from 'node:crypto';
import { copyFileSync, existsSync, mkdtempSync, readFileSync, realpathSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createHypervisorStore } from '../hypervisor_store.mjs';

const digest = path => createHash('sha256').update(readFileSync(path)).digest('hex');
const median = values => [...values].sort((a, b) => a - b)[Math.floor(values.length / 2)];
export function benchmark({ home, iterations = 100, capabilities = 12 } = {}) {
  if (!home) throw new Error('pass --home PATH');
  home = realpathSync(home);
  const dbPath = join(home, 'hypervisor.sqlite3'), sourceHash = digest(dbPath);
  const implementationPaths = {
    hypervisor_store: new URL('../hypervisor_store.mjs', import.meta.url).pathname,
    canonical_json: new URL('../canonical_json.mjs', import.meta.url).pathname,
    benchmark: new URL(import.meta.url).pathname,
  };
  const implementationHashes = Object.fromEntries(Object.entries(implementationPaths).map(([name, path]) => [name, digest(path)]));
  const measuredAt = new Date().toISOString();
  if (['-wal', '-shm', '-journal'].some(suffix => existsSync(`${dbPath}${suffix}`))) throw new Error('active source database refused');
  const scratch = realpathSync(mkdtempSync(join(tmpdir(), 'bonfyre-hypervisor-route-bench-')));
  let store;
  try {
    copyFileSync(dbPath, join(scratch, 'hypervisor.sqlite3'));
    copyFileSync(join(home, 'ecosystem_registry.json'), join(scratch, 'ecosystem_registry.json'));
    store = createHypervisorStore({ home: scratch });
    store.loadRegistry(); // Only the disposable copy is writable; source stays untouched.
    const ids = store.registry().capability_contracts.slice(0, capabilities);
    if (!ids.length) throw new Error('no source capabilities');
    const singles = () => Object.fromEntries(ids.map(id => [id, store.route(id, { include_shadow: true })]));
    const batch = () => store.routeMany(ids, { include_shadow: true });
    if (JSON.stringify(singles()) !== JSON.stringify(batch())) throw new Error('batch route differs from repeated single route');
    for (let i = 0; i < 20; i++) { singles(); batch(); }
    const samples = { repeated_ms: [], batch_ms: [] }, before = store.routeQueryCount();
    for (let pair = 0; pair < 7; pair++) {
      for (const [name, call] of pair % 2 ? [['batch_ms', batch], ['repeated_ms', singles]] : [['repeated_ms', singles], ['batch_ms', batch]]) {
        const start = performance.now(); for (let i = 0; i < iterations; i++) call();
        samples[name].push((performance.now() - start) / iterations);
      }
    }
    const queryCount = store.routeQueryCount() - before;
    if (queryCount !== 7 * iterations * (ids.length + 1)) throw new Error('route statement count changed');
    if (digest(dbPath) !== sourceHash) throw new Error('source database changed during benchmark');
    for (const [name, path] of Object.entries(implementationPaths))
      if (digest(path) !== implementationHashes[name]) throw new Error('implementation changed during benchmark');
    return { schema: 'bonfyre.hypervisor-route-benchmark.v1', source_database_sha256: sourceHash,
      implementation_sha256: implementationHashes, measured_at_utc: measuredAt,
      capabilities: ids.length, iterations_per_pair: iterations, pairs: 7,
      repeated_queries_per_call: ids.length, batch_queries_per_call: 1,
      raw_pair_ms_per_call: samples,
      repeated_p50_ms: median(samples.repeated_ms), batch_p50_ms: median(samples.batch_ms),
      result_equal: true, source_unchanged: true, interpretation: 'Local copied-state timing only; no production latency or distributed scaling claim.' };
  } finally { try { store?.close(); } catch { /* disposable owner */ } rmSync(scratch, { recursive: true, force: true }); }
}
if (import.meta.main) {
  const index = process.argv.indexOf('--home');
  if (index < 0 || !process.argv[index + 1]) throw new Error('pass --home PATH');
  process.stdout.write(`${JSON.stringify(benchmark({ home: process.argv[index + 1] }))}\n`);
}
