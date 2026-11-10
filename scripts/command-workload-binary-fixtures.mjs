#!/usr/bin/env bun
// Exact, deliberately small inputs for command-completion workloads.
import { mkdirSync, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

// Captured once from Python's random.Random(7), uniform(-1, 1), packed as
// 64 little-endian float32 values. This is a legacy fixture, not a RNG API.
const seed7CodebookF32LE = Buffer.from(
  "2f65b4bee6c332bf918e9a3ea2e95abf02f9923dd88889bef74d62bf73a7733c5ecd6cbfc6e407bee93b5cbf108e51bfaf951abe2959273f089d40bf9fb30dbfdb7d823e1b3a653f27e81d3e309953beb6d7733f512668bf2e89373faa70d7be332436bfbcb043bf571dc4be50db213fd57723bffd1d273e553f8e3e38aa82beb38fc33d20da5fbff57b61bf948c16bfc5bab83e7c4a14be3450bebe0f3b2f3ebdc1bfbde109cdbee8b8163f32c5cb3ec80503bf756b183ee6684e3d0612403fb5f3ea3ed826d9be7ad9753fe28c43bf39af27befaa7033f152f32bf1dd4b4bc07ed6bbfc740ac3ed575073fa08e153ea13e403ff9b8bebe82fbc73efe44413e",
  "hex",
);
if (seed7CodebookF32LE.length !== 4 * 16 * 4) throw new Error("legacy BQFP codebook bytes are incomplete");

export function bqfpFixture() {
  const body = Buffer.alloc(16 + 256 + 24 + seed7CodebookF32LE.length);
  body.writeUInt32LE(0x50464251, 0); // BQFP magic
  body.writeUInt32LE(0, 4);          // version
  body.writeUInt32LE(1, 8);          // one tensor
  body.writeUInt32LE(8, 12);         // bits
  body.write("workload_tensor", 16, "ascii");
  body.writeBigUInt64LE(16n, 272);  // elements
  body.writeBigUInt64LE(0n, 280);   // no blocks
  body.writeUInt32LE(4, 288);       // effective codebook width
  body.writeUInt32LE(0, 292);       // padding
  seed7CodebookF32LE.copy(body, 296);
  return body;
}

export function vecfFixture() {
  const body = Buffer.alloc(8 + 4 * 4);
  body.writeUInt32LE(0x46434556, 0); // VECF magic
  body.writeUInt32LE(4, 4);          // dimensions
  for (const [index, value] of [3, 4, 0, 0].entries()) body.writeFloatLE(value, 8 + index * 4);
  return body;
}

export function generateWorkloadBinaryFixtures(directory) {
  if (typeof directory !== "string" || !directory) throw new Error("fixture output directory is required");
  const out = resolve(directory);
  mkdirSync(out, { recursive: true });
  const bqfp = join(out, "fixture.bqfp"), vecf = join(out, "fixture.vecf");
  writeFileSync(bqfp, bqfpFixture());
  writeFileSync(vecf, vecfFixture());
  return { bqfp, vecf };
}

if (process.argv[1] && fileURLToPath(import.meta.url) === resolve(process.argv[1])) {
  if (process.argv.length !== 4 || process.argv[2] !== "--out-dir") {
    console.error("usage: bun scripts/command-workload-binary-fixtures.mjs --out-dir DIR");
    process.exitCode = 2;
  } else {
    try { generateWorkloadBinaryFixtures(process.argv[3]); }
    catch (error) { console.error(error.message); process.exitCode = 1; }
  }
}
