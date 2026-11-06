// Platform-neutral PURE Helix executor: no filesystem, process, network, or Bun APIs.
const ALLOWED_IMPORTS = new Set(['fd_close', 'fd_fdstat_get', 'fd_prestat_get', 'fd_prestat_dir_name',
  'fd_seek', 'fd_write', 'proc_exit', 'random_get']);
const encoder = new TextEncoder();
const MAX_MEMORY = 32 * 1024 * 1024;
const hex = bytes => [...bytes].map(byte => byte.toString(16).padStart(2, '0')).join('');
async function sha256(bytes) { return hex(new Uint8Array(await crypto.subtle.digest('SHA-256', bytes))); }

export function denySpinesWasiImports(module) {
  const descriptors = WebAssembly.Module.imports(module);
  // A module may ask for fewer of the allowed imports, never for another one:
  // an optimiser that proves an import unused removes it, and asking the host
  // for less is not a reason to refuse. Every import is denied when called.
  if (descriptors.length > ALLOWED_IMPORTS.size || new Set(descriptors.map(item => item.name)).size !== descriptors.length ||
    descriptors.some(item => item.module !== 'wasi_snapshot_preview1' ||
    item.kind !== 'function' || !ALLOWED_IMPORTS.has(item.name))) throw new Error('unexpected Wasm import contract');
  const imports = { wasi_snapshot_preview1: {} };
  for (const item of descriptors) imports.wasi_snapshot_preview1[item.name] = () => { throw new Error(`WASI effect refused: ${item.name}`); };
  return imports;
}

export async function createSpinesWasmCore(moduleBytes, { expectedSha256, expectedBytes } = {}) {
  if (!(moduleBytes instanceof Uint8Array) || !Number.isSafeInteger(expectedBytes) || expectedBytes < 1 ||
    expectedBytes > 2 * 1024 * 1024 || moduleBytes.byteLength !== expectedBytes ||
    typeof expectedSha256 !== 'string' || !/^[a-f0-9]{64}$/.test(expectedSha256)) throw new Error('Wasm artifact bounds mismatch');
  const pinned = new Uint8Array(moduleBytes);
  if (await sha256(pinned) !== expectedSha256) throw new Error('Wasm artifact digest mismatch');
  const module = await WebAssembly.compile(pinned);
  const instance = await WebAssembly.instantiate(module, denySpinesWasiImports(module));
  const { memory, malloc, free, bf_reduce } = instance.exports;
  if (!(memory instanceof WebAssembly.Memory) || typeof malloc !== 'function' || typeof free !== 'function' ||
    typeof bf_reduce !== 'function' || memory.buffer.byteLength > MAX_MEMORY) throw new Error('unexpected Wasm export contract');

  return Object.freeze({
    async reduce(term, fuel) {
      if (typeof term !== 'string' || term.includes('\0')) throw new Error('invalid Wasm term');
      const bytes = encoder.encode(term);
      if (!bytes.length || bytes.length > 65536 || !Number.isSafeInteger(fuel) || fuel < 1 || fuel > 1000000)
        throw new Error('Wasm input budget exceeded');
      const started = performance.now();
      let termPtr = 0, outputPtr = 0;
      try {
        termPtr = malloc(bytes.length + 1);
        outputPtr = malloc(16);
        if (!termPtr || !outputPtr) throw new Error('Wasm allocation failed');
        const view = new Uint8Array(memory.buffer);
        if (termPtr + bytes.length + 1 > view.length || outputPtr + 16 > view.length) throw new Error('Wasm memory bounds exceeded');
        view.set(bytes, termPtr);
        view[termPtr + bytes.length] = 0;
        const code = bf_reduce(termPtr, bytes.length, fuel, outputPtr, outputPtr + 8, outputPtr + 12);
        const output = new DataView(memory.buffer);
        const value = output.getFloat64(outputPtr, true), steps = output.getInt32(outputPtr + 8, true);
        const remaining = output.getInt32(outputPtr + 12, true);
        if (![0, 3].includes(code) || !Number.isSafeInteger(steps) || steps < 1 || steps > fuel || remaining !== fuel - steps)
          throw new Error('Wasm reduction contract mismatch');
        const passed = code === 0 && Number.isFinite(value);
        const witness = { schema: 'bonfyre.helix-witness.v1', state: passed ? 'passed' : 'failed',
          term_sha256: await sha256(bytes), steps, fuel_remaining: remaining, ...(passed ? { result: value } : {}) };
        if (memory.buffer.byteLength > MAX_MEMORY) throw new Error('Wasm memory budget exceeded');
        return { exit_code: passed ? 0 : 3, stdout: `${JSON.stringify(witness)}\n`, stderr: '',
          duration_ms: Math.max(0, Math.floor(performance.now() - started)), timed_out: false,
          execution_kind: 'in_process_wasm', module_sha256: expectedSha256 };
      } finally {
        const view = new Uint8Array(memory.buffer);
        if (termPtr && termPtr + bytes.length + 1 <= view.length) view.fill(0, termPtr, termPtr + bytes.length + 1);
        if (outputPtr && outputPtr + 16 <= view.length) view.fill(0, outputPtr, outputPtr + 16);
        if (termPtr) free(termPtr);
        if (outputPtr) free(outputPtr);
      }
    },
  });
}
