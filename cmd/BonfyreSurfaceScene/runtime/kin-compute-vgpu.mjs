#!/usr/bin/env node
/**
 * Second kernel: kin.  Nearest neighbours in the estate's own word space.
 *
 * The spatial kernel can only connect the handful of things that state where
 * they are -- a hundred of seven thousand.  Everything else the estate holds
 * is connected by what it is about, and the latent atlas already places every
 * item in a dense chart.  This computes, for every item, the items nearest to
 * it in that chart, tile by tile, on the same pinned vGPU.
 *
 * It is an acceleration boundary and nothing more.  A high similarity between
 * two items is a statement about two coordinates in one chart computed from
 * words: it is not a claim that either item is about the other, that they
 * share a subject, that one caused or followed the other, or that anyone
 * should read the second because they read the first.
 *
 * Vectors arrive as a raw float32 file (count * dims) with a parallel key
 * list, because a chart of seven thousand items in JSON is a hundred times its
 * own size and this has to run on the estate's own machine.
 *
 *   kin-compute-vgpu.mjs --vectors vectors.f32 --keys keys.json --dims 256
 *       [--k 8] [--tile 1024] [--backend auto|mock|cpu] [--min 0.15]
 *
 * Prints one JSON line: {receipt, nearest:[{key, near:[{key, score}]}]}.
 */
import fs from "node:fs";
import crypto from "node:crypto";
import { fileURLToPath, pathToFileURL } from "node:url";
import path from "node:path";

const args = process.argv.slice(2);
const arg = (name, fallback = "") => {
  const i = args.indexOf(name);
  return i >= 0 && i + 1 < args.length ? args[i + 1] : fallback;
};
const k = Math.max(1, Math.min(32, Number(arg("--k", 8)) || 8));
const tileSize = Math.max(64, Math.min(4096, Number(arg("--tile", 1024)) || 1024));
const dims = Math.max(2, Number(arg("--dims", 256)) || 256);
const backendRequested = arg("--backend", "auto");
const floor = Number(arg("--min", 0.85));
const vectorsPath = arg("--vectors", "");
const keysPath = arg("--keys", "");

function digest(value) {
  return crypto.createHash("sha256").update(JSON.stringify(value)).digest("hex");
}

function readVectors() {
  const raw = fs.readFileSync(vectorsPath);
  const floats = new Float32Array(raw.buffer, raw.byteOffset, Math.floor(raw.byteLength / 4));
  const keys = JSON.parse(fs.readFileSync(keysPath, "utf8"));
  const count = keys.length;
  if (floats.length < count * dims) {
    throw new Error(`vectors hold ${floats.length} floats, ${count} keys of ${dims} dims need ${count * dims}`);
  }
  // Cosine similarity is a dot product once every row has unit length, and a
  // row of zeroes (an item whose words the chart could not place) is left at
  // zero so it matches nothing rather than matching everything.
  const unit = new Float32Array(count * dims);
  for (let i = 0; i < count; i++) {
    let sum = 0;
    for (let d = 0; d < dims; d++) { const v = floats[i * dims + d]; sum += v * v; }
    const norm = Math.sqrt(sum);
    if (norm > 0) for (let d = 0; d < dims; d++) unit[i * dims + d] = floats[i * dims + d] / norm;
  }
  return { keys, unit, count };
}

/* A bounded top-k, kept as a plain insertion sort: k is at most 32. */
function keep(best, index, score) {
  if (score < floor) return;
  if (best.length === k && score <= best[best.length - 1].score) return;
  let at = best.length;
  while (at > 0 && best[at - 1].score < score) at--;
  best.splice(at, 0, { index, score });
  if (best.length > k) best.length = k;
}

function cpuTile(unit, count, from, to) {
  const rows = [];
  for (let i = from; i < to; i++) {
    const best = [];
    for (let j = 0; j < count; j++) {
      if (i === j) continue;
      let dot = 0;
      for (let d = 0; d < dims; d++) dot += unit[i * dims + d] * unit[j * dims + d];
      keep(best, j, dot);
    }
    rows.push(best);
  }
  return rows;
}

function repoVgpu(subpath) {
  const here = path.dirname(fileURLToPath(import.meta.url));
  return pathToFileURL(path.resolve(here, "../../../site/matrix/vgpu/node_modules/vgpu/dist", subpath)).href;
}

/* A dispatch may ask for at most 65,535 workgroups in a dimension, so a tile
 * of a thousand rows against seven thousand items is two orders of magnitude
 * over the limit.  Asking for more does not fail loudly -- it produces a
 * buffer of zeroes, which reads as "nothing resembles anything".  The tile is
 * therefore sized from the work, never from the request alone. */
const MAX_WORKGROUPS = 65535, LANES = 64;
function rowsPerTile(count, wanted) {
  const ceiling = Math.max(1, Math.floor((MAX_WORKGROUPS * LANES) / Math.max(1, count)));
  return Math.max(1, Math.min(wanted, ceiling));
}

async function gpuTiles(unit, count, mode, onTile) {
  const mod = await import(repoVgpu(mode === "mock" ? "mock.js" : "node.js"));
  const { init, compute, storage } = mod;
  const gpu = await init();
  const step = rowsPerTile(count, tileSize);
  let tiles = 0;
  try {
    const vectors = storage(gpu, Math.max(8, count * dims * 4), "read");
    vectors.write(unit);
    for (let from = 0; from < count; from += step) {
      const rows = Math.min(step, count - from);
      const scores = storage(gpu, Math.max(4, rows * count * 4), "read-write");
      const shader = `
        @group(0) @binding(0) var<storage, read> vectors: array<f32>;
        @group(0) @binding(1) var<storage, read_write> scores: array<f32>;
        const N: u32 = ${count}u;
        const D: u32 = ${dims}u;
        const FROM: u32 = ${from}u;
        const ROWS: u32 = ${rows}u;

        @compute @workgroup_size(64)
        fn cs_main(@builtin(global_invocation_id) gid: vec3u) {
          let idx = gid.x;
          if (idx >= ROWS * N) { return; }
          let r = idx / N;
          let j = idx % N;
          let i = FROM + r;
          if (i == j) { scores[idx] = -1.0; return; }
          var dot: f32 = 0.0;
          for (var d: u32 = 0u; d < D; d = d + 1u) {
            dot = dot + vectors[i * D + d] * vectors[j * D + d];
          }
          scores[idx] = dot;
        }
      `;
      const kernel = compute(gpu, shader, {
        label: "bonfyre-kin-word-neighbourhood",
        set: { vectors, scores },
      });
      const groups = Math.ceil((rows * count) / LANES);
      if (groups > MAX_WORKGROUPS) throw new Error(`tile of ${rows} rows needs ${groups} workgroups`);
      kernel.dispatch(groups);
      const block = new Float32Array(await scores.read());
      for (let r = 0; r < rows; r++) {
        const best = [];
        for (let j = 0; j < count; j++) keep(best, j, block[r * count + j]);
        onTile(from + r, best);
      }
      tiles++;
    }
  } finally {
    gpu.dispose();
  }
  return tiles;
}

async function main() {
  if (!vectorsPath || !keysPath) throw new Error("--vectors and --keys are required");
  const { keys, unit, count } = readVectors();
  const started = Date.now();
  const nearest = new Array(count);
  let backend = "cpu", tiles = 0;

  const record = (index, best) => {
    nearest[index] = {
      key: keys[index],
      near: best.map((one) => ({ key: keys[one.index], score: Number(one.score.toFixed(4)) })),
    };
  };

  if (count >= 2 && backendRequested !== "cpu") {
    try {
      tiles = await gpuTiles(unit, count, backendRequested === "mock" ? "mock" : "node", record);
      backend = backendRequested === "mock" ? "vgpu-mock" : "vgpu-node";
    } catch (err) {
      backend = "cpu";
      process.stderr.write("kin: vgpu unavailable (" + String(err && err.message).slice(0, 120) + "); running the same kernel on the deterministic CPU path\n");
    }
  }
  if (backend === "cpu") {
    for (let from = 0; from < count; from += tileSize) {
      const to = Math.min(count, from + tileSize);
      cpuTile(unit, count, from, to).forEach((best, r) => record(from + r, best));
      tiles++;
    }
  }

  const receipt = {
    schema: "bonfyre.kin-compute-receipt.v1",
    requested_backend: backendRequested,
    actual_backend: backend,
    items: count,
    dims,
    k,
    tiles,
    pairs: count * count,
    floor,
    elapsed_ms: Date.now() - started,
    input_digest: digest({ count, dims, keys: keys.length }),
    cannot_infer: "a high score between two items is a statement about two coordinates in one chart "
                + "computed from words; it is not a shared subject, a citation, an influence, an "
                + "endorsement, or a reason anyone should read one because they read the other",
  };
  process.stdout.write(JSON.stringify({ receipt, nearest: nearest.filter(Boolean) }) + "\n");
}

main().catch((err) => {
  process.stderr.write(String(err && err.stack || err) + "\n");
  process.exit(1);
});
