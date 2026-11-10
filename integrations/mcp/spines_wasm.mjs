import { constants, closeSync, fstatSync, lstatSync, openSync, readSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { createSpinesWasmCore } from './spines_wasm_core.mjs';

export { denySpinesWasiImports } from './spines_wasm_core.mjs';
export const SPINES_HELIX_WASM_SHA256 = '5dd6f057e49c7c6637b9cf3b5b86190eb2ae9fc93bfaf6a146a03cf53cf48e52';
export const SPINES_HELIX_WASM_BYTES = 30454;
export const SPINES_HELIX_WASM_PATH = fileURLToPath(new URL('./native/spines_helix.wasm', import.meta.url));

function readPinned(path) {
  let fd;
  try {
    fd = openSync(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
    const before = fstatSync(fd, { bigint: true });
    if (!before.isFile() || before.size !== BigInt(SPINES_HELIX_WASM_BYTES)) throw new Error('Wasm artifact size mismatch');
    const bytes = Buffer.alloc(SPINES_HELIX_WASM_BYTES); let used = 0;
    while (used < bytes.length) { const count = readSync(fd, bytes, used, bytes.length - used, null); if (!count) break; used += count; }
    const after = fstatSync(fd, { bigint: true }), named = lstatSync(path, { bigint: true });
    const identity = value => [value.dev, value.ino, value.size, value.mtimeNs, value.ctimeNs].join(':');
    if (used !== bytes.length || !named.isFile() || identity(before) !== identity(after) || identity(after) !== identity(named))
      throw new Error('Wasm artifact changed while reading');
    return bytes;
  } finally { if (fd !== undefined) closeSync(fd); }
}

export function createSpinesWasm({ artifactPath = SPINES_HELIX_WASM_PATH } = {}) {
  let ready;
  return {
    async reduce(term, fuel) {
      ready ||= createSpinesWasmCore(readPinned(artifactPath), {
        expectedSha256: SPINES_HELIX_WASM_SHA256, expectedBytes: SPINES_HELIX_WASM_BYTES,
      });
      return (await ready).reduce(term, fuel);
    },
  };
}
