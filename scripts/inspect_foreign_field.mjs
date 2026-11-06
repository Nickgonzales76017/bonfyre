#!/usr/bin/env bun
// Read the exact uncompressed int32 Zarr field used by the ForeignTwin witness.
import { readFileSync } from "node:fs";
import { join } from "node:path";

export function inspect(root) {
  const array = JSON.parse(readFileSync(join(root, ".zarray"), "utf8"));
  const attrs = JSON.parse(readFileSync(join(root, ".zattrs"), "utf8"));
  if (!Array.isArray(array.shape) || array.shape.length !== 2 ||
      !array.shape.every(n => Number.isSafeInteger(n) && n > 0) ||
      array.dtype !== "<i4" || array.compressor !== null)
    throw new Error("expected an uncompressed 2D little-endian int32 field");
  const cells = array.shape[0] * array.shape[1];
  if (!Number.isSafeInteger(cells)) throw new Error("field dimensions overflow");
  const bytes = readFileSync(join(root, "0.0"));
  if (bytes.byteLength !== cells * 4) throw new Error("field chunk length does not match shape");
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  let total = 0;
  for (let i = 0; i < cells; i++) total += view.getInt32(i * 4, true);
  return { shape: array.shape, total, derived_from_digest: attrs["bonfyre:derivedFrom"]?.digest ?? "",
    has_derivation_frontier: Boolean(attrs["bonfyre:derivation"] && attrs["bonfyre:frontier"]) };
}

if (import.meta.main) {
  try {
    if (process.argv.length !== 3) throw new Error("usage: inspect_foreign_field.mjs <zarr-root>");
    process.stdout.write(`${JSON.stringify(inspect(process.argv[2]))}\n`);
  } catch (error) {
    process.stderr.write(`${error.message}\n`);
    process.exitCode = 1;
  }
}
