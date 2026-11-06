import { existsSync, mkdtempSync, rmSync } from 'node:fs';
import { homedir, tmpdir } from 'node:os';
import { join } from 'node:path';
import { createSpinesStore } from '../spines_store.mjs';
import { createReasoningEngine } from '../spines_reasoning.mjs';
import { createSpinesWasm, SPINES_HELIX_WASM_SHA256 } from '../spines_wasm.mjs';

const native = join(homedir(), '.local/bin/bonfyre-helix-vm');
if (!existsSync(native)) throw new Error('actual installed native Helix VM required for benchmark');
const term = 'mul(add(1,2),sub(9,4))', fuel = 100;
const wasm = createSpinesWasm();
const started = performance.now();
const cold = await wasm.reduce(term, fuel);
const coldMs = performance.now() - started;
if (cold.exit_code !== 0 || JSON.parse(cold.stdout).result !== 15) throw new Error('cold Wasm witness failed');

const kernel = { native: [], wasm: [] };
for (let i = 0; i < 500; i++) for (const engine of ['native', 'wasm']) {
  const start = performance.now();
  const outcome = engine === 'native'
    ? Bun.spawnSync({ cmd: [native, 'reduce', '--term', term, '--fuel', String(fuel)], stdout: 'pipe', stderr: 'pipe' })
    : await wasm.reduce(term, fuel);
  if ((engine === 'native' ? outcome.exitCode : outcome.exit_code) !== 0) throw new Error(`${engine} kernel failed`);
  kernel[engine].push(performance.now() - start);
}

const home = mkdtempSync(join(tmpdir(), 'bonfyre-spines-benchmark-'));
let nativeStore, wasmStore;
try {
  const common = { projectRoot: home, deltaKernel: '/unused/delta', helixVm: native, kernelProbe: '/unused/probe' };
  nativeStore = createSpinesStore({ ...common, home: join(home, 'native') });
  wasmStore = createSpinesStore({ ...common, home: join(home, 'wasm') });
  const nativeOwner = createReasoningEngine(nativeStore, { wasmPath: join(home, 'artifact-absent.wasm') });
  const wasmOwner = createReasoningEngine(wasmStore);
  const nativeId = nativeOwner.compileHelix({ term, fuel }).helix_id;
  const wasmId = wasmOwner.compileHelix({ term, fuel }).helix_id;
  const full = { native: [], wasm: [] };
  for (let i = 0; i < 30; i++) for (const engine of ['native', 'wasm']) {
    const start = performance.now();
    const outcome = engine === 'native' ? await nativeOwner.reduce({ helix_id: nativeId }) : await wasmOwner.reduce({ helix_id: wasmId });
    if (outcome.state !== 'passed' || outcome.witness.result !== 15) throw new Error(`${engine} owner failed`);
    full[engine].push(performance.now() - start);
  }
  const stats = values => {
    values.sort((a, b) => a - b);
    return { p50_ms: values[Math.floor(values.length * .5)], p95_ms: values[Math.floor(values.length * .95)] };
  };
  console.log(JSON.stringify({ schema: 'bonfyre.spines-wasm-benchmark.v1', scope: 'one Mac, local pure Helix; not throughput or distributed scaling',
    platform: process.platform, arch: process.arch, bun: Bun.version, native, wasm_sha256: SPINES_HELIX_WASM_SHA256,
    samples: { kernel: 500, full_owner: 30 }, wasm_cold_ms: coldMs,
    kernel: { native: stats(kernel.native), wasm: stats(kernel.wasm) },
    full_owner: { native: stats(full.native), wasm: stats(full.wasm) },
    native_receipts_verified: nativeStore.verifyReceipts().verified,
    wasm_receipts_verified: wasmStore.verifyReceipts().verified }, null, 2));
} finally {
  nativeStore?.close(); wasmStore?.close();
  rmSync(home, { recursive: true, force: true });
}
