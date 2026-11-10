import { afterEach, expect, test } from 'bun:test';
import { createHash } from 'node:crypto';
import { existsSync, mkdtempSync, readFileSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import { homedir, tmpdir } from 'node:os';
import { join } from 'node:path';
import { createSpinesStore } from '../spines_store.mjs';
import { createReasoningEngine } from '../spines_reasoning.mjs';
import { createSpinesWasm, denySpinesWasiImports, SPINES_HELIX_WASM_PATH, SPINES_HELIX_WASM_SHA256 } from '../spines_wasm.mjs';
import { createSpinesWasmCore } from '../spines_wasm_core.mjs';

const homes = [];
afterEach(() => { for (const { home, store } of homes.splice(0)) { store?.close(); rmSync(home, { recursive: true, force: true }); } });
const hash = text => createHash('sha256').update(text).digest('hex');
const cases = [
  ['add(1,2)', 100, 3], ['mul(add(1,2),sub(9,4))', 100, 15], ['sqrt(9)', 100, 3],
  ['and(gt(4,3),lt(2,5))', 100, 1], ['neg(div(7,2))', 100, -3.5], ['true', 100, 1],
  ['div(1,0)', 100, null], ['sqrt(-1)', 100, null], ['add(1,2)', 1, null],
  ['add(1)', 100, null], ['mystery(1,2)', 100, null], ['add(1,2)junk', 100, null],
];

test('pinned artifact and source lock verify without compiling or executing effects', () => {
  const result = Bun.spawnSync({ cmd: [process.execPath, 'integrations/mcp/native/build_spines_wasm.mjs', '--verify'],
    cwd: process.cwd(), stdout: 'pipe', stderr: 'pipe' });
  expect(result.exitCode).toBe(0);
  expect(JSON.parse(new TextDecoder().decode(result.stdout)).sha256).toBe(SPINES_HELIX_WASM_SHA256);
  expect(hash(readFileSync(SPINES_HELIX_WASM_PATH))).toBe(SPINES_HELIX_WASM_SHA256);
});

test('in-process Wasm runs same pure result/error/fuel cases and reuses one pinned module', async () => {
  const wasm = createSpinesWasm();
  for (const [term, fuel, result] of cases) {
    const probe = await wasm.reduce(term, fuel);
    const witness = JSON.parse(probe.stdout);
    expect(probe.execution_kind).toBe('in_process_wasm');
    expect(probe.argv).toBeUndefined();
    expect(probe.module_sha256).toBe(SPINES_HELIX_WASM_SHA256);
    expect(witness.term_sha256).toBe(hash(term));
    expect(witness.steps + witness.fuel_remaining).toBe(fuel);
    expect([probe.exit_code, witness.state, witness.result ?? null])
      .toEqual(result === null ? [3, 'failed', null] : [0, 'passed', result]);
  }
  expect(JSON.parse((await wasm.reduce('3', 100)).stdout).result).toBe(3);
});

test('platform-neutral core accepts pinned bytes directly without local-file or process owner', async () => {
  const bytes = readFileSync(SPINES_HELIX_WASM_PATH);
  const core = await createSpinesWasmCore(bytes, { expectedSha256: SPINES_HELIX_WASM_SHA256, expectedBytes: bytes.length });
  const result = await core.reduce('add(1,2)', 100);
  expect(result.execution_kind).toBe('in_process_wasm');
  expect(JSON.parse(result.stdout).result).toBe(3);
  const corrupt = new Uint8Array(bytes); corrupt[corrupt.length - 1] ^= 1;
  await expect(createSpinesWasmCore(corrupt, { expectedSha256: SPINES_HELIX_WASM_SHA256, expectedBytes: bytes.length }))
    .rejects.toThrow('digest mismatch');
});

test('present modified module refuses execution; no fallback to untrusted bytes', async () => {
  const home = mkdtempSync(join(tmpdir(), 'bonfyre-spines-wasm-')); homes.push({ home });
  const path = join(home, 'corrupt.wasm'), bytes = readFileSync(SPINES_HELIX_WASM_PATH);
  bytes[bytes.length - 1] ^= 1; writeFileSync(path, bytes);
  await expect(createSpinesWasm({ artifactPath: path }).reduce('add(1,2)', 100)).rejects.toThrow('digest mismatch');
});

test('pinned WASI imports have no rights and trap if invoked', async () => {
  const module = await WebAssembly.compile(readFileSync(SPINES_HELIX_WASM_PATH));
  expect(WebAssembly.Module.exports(module).find(item => item.name === 'memory').type.maximum).toBe(512);
  const imports = denySpinesWasiImports(module).wasi_snapshot_preview1;
  expect(Object.keys(imports).sort()).toEqual(['fd_close', 'fd_fdstat_get', 'fd_prestat_get', 'fd_prestat_dir_name',
    'fd_seek', 'fd_write', 'proc_exit', 'random_get'].sort());
  for (const [name, operation] of Object.entries(imports)) expect(() => operation()).toThrow(`WASI effect refused: ${name}`);
});

test('ordinary reduction refuses a present corrupt module without invoking native fallback', async () => {
  const home = mkdtempSync(join(tmpdir(), 'bonfyre-spines-wasm-')), path = join(home, 'corrupt.wasm');
  const bytes = readFileSync(SPINES_HELIX_WASM_PATH); bytes[bytes.length - 1] ^= 1; writeFileSync(path, bytes);
  let nativeCalls = 0;
  const store = createSpinesStore({ home, projectRoot: process.cwd(), deltaKernel: '/native/delta', helixVm: '/native/helix',
    kernelProbe: '/native/probe', env: { PATH: '' }, run: async () => { nativeCalls++; throw new Error('native must not run'); } });
  homes.push({ home, store });
  const reasoning = createReasoningEngine(store, { wasmPath: path }), compiled = reasoning.compileHelix({ term: 'add(1,2)' });
  await expect(reasoning.reduce({ helix_id: compiled.helix_id })).rejects.toThrow('digest mismatch');
  expect(nativeCalls).toBe(0);
});

test('dangling symlink is present corruption, not native-fallback absence', async () => {
  const home = mkdtempSync(join(tmpdir(), 'bonfyre-spines-wasm-')), path = join(home, 'dangling.wasm');
  symlinkSync(join(home, 'missing-target.wasm'), path);
  let nativeCalls = 0;
  const store = createSpinesStore({ home, projectRoot: process.cwd(), deltaKernel: '/native/delta', helixVm: '/native/helix',
    kernelProbe: '/native/probe', env: { PATH: '' }, run: async () => { nativeCalls++; throw new Error('native must not run'); } });
  homes.push({ home, store });
  const reasoning = createReasoningEngine(store, { wasmPath: path }), compiled = reasoning.compileHelix({ term: 'add(1,2)' });
  await expect(reasoning.reduce({ helix_id: compiled.helix_id })).rejects.toThrow();
  expect(nativeCalls).toBe(0);
});

test('ordinary R6 reduction selects verified Wasm by default, preserves receipt truth and no fake argv', async () => {
  const home = mkdtempSync(join(tmpdir(), 'bonfyre-spines-wasm-')); let nativeCalls = 0;
  const store = createSpinesStore({ home, projectRoot: process.cwd(), deltaKernel: '/native/delta', helixVm: '/native/helix',
    kernelProbe: '/native/probe', now: () => 1234, env: { PATH: '' }, run: async () => { nativeCalls++; throw new Error('native must not run'); } });
  homes.push({ home, store });
  const reasoning = createReasoningEngine(store), compiled = reasoning.compileHelix({ term: 'add(1,2)' });
  const result = await reasoning.reduce({ helix_id: compiled.helix_id });
  expect(result.state).toBe('passed');
  expect(result.backend).toBe('bonfyre-helix-wasm-r7');
  expect(result.wasm_artifact_sha256).toBe(SPINES_HELIX_WASM_SHA256);
  expect(result.probe.execution_kind).toBe('in_process_wasm');
  expect(result.probe.argv).toBeUndefined();
  expect(result.witness.result).toBe(3);
  expect(nativeCalls).toBe(0);
  expect(store.verifyReceipts().verified).toBe(true);
});

test.skipIf(!existsSync(join(homedir(), '.local/bin/bonfyre-helix-vm')))('installed native Helix VM agrees on exact bounded witness outputs', async () => {
  const wasm = createSpinesWasm(), native = join(homedir(), '.local/bin/bonfyre-helix-vm');
  for (const [term, fuel] of cases) {
    const wasmProbe = await wasm.reduce(term, fuel);
    const nativeProbe = Bun.spawnSync({ cmd: [native, 'reduce', '--term', term, '--fuel', String(fuel)], stdout: 'pipe', stderr: 'pipe' });
    expect(wasmProbe.exit_code).toBe(nativeProbe.exitCode);
    expect(JSON.parse(wasmProbe.stdout)).toEqual(JSON.parse(new TextDecoder().decode(nativeProbe.stdout)));
  }
});
