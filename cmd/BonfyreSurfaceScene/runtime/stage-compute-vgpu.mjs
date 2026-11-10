#!/usr/bin/env node
/**
 * Execute high-fanout StageClosure work through the repo's pinned vGPU 0.3.1.
 *
 * First kernel: spatial-neighborhood.  It computes an all-pairs distance field
 * for placed Stage material and returns nearest-neighbour indices/distances.
 * This is an acceleration boundary only: it never invents a Place relation,
 * attendance, access, availability, or scene membership.
 *
 * Modes:
 *   --backend auto  try Dawn-backed vgpu/node, otherwise deterministic CPU
 *   --backend mock  run the exact vGPU compute path against vgpu/mock
 *   --backend cpu   deterministic fallback
 */
import fs from "node:fs";
import crypto from "node:crypto";
import { fileURLToPath, pathToFileURL } from "node:url";
import path from "node:path";

const args = process.argv.slice(2);
function option(name, fallbackValue) {
  const at = args.indexOf(name);
  return at >= 0 && at + 1 < args.length ? args[at + 1] : fallbackValue;
}
const k = Math.max(1, Math.min(32, Number(option("--k", 8)) || 8));
const tileSize = Math.max(64, Math.min(4096, Number(option("--tile", 2048)) || 2048));
const arg = (name, fallback = "") => {
  const i = args.indexOf(name);
  return i >= 0 && i + 1 < args.length ? args[i + 1] : fallback;
};
const backendRequested = arg("--backend", "auto");
const inputPath = arg("--input", "-");

function readInput() {
  const text = inputPath === "-" ? fs.readFileSync(0, "utf8") : fs.readFileSync(inputPath, "utf8");
  return JSON.parse(text);
}
function stable(value) {
  if (Array.isArray(value)) return value.map(stable);
  if (value && typeof value === "object") {
    return Object.fromEntries(Object.keys(value).sort().map((k) => [k, stable(value[k])]));
  }
  return value;
}
function digest(value) {
  return crypto.createHash("sha256").update(JSON.stringify(stable(value))).digest("hex");
}
function placed(closure) {
  // Anything the estate holds that states where it is: a place, an event a
  // calendar put a point on, a photograph with its own coordinates, a wildlife
  // record.  Carrying only two kinds of material was the reason this kernel
  // could never answer "what is around here".
  const material = closure.material ?? {};
  const candidates = Object.keys(material)
    .filter((kind) => Array.isArray(material[kind]))
    .flatMap((kind) => material[kind].map((item) => ({ ...item, kind: item.kind || kind })));
  const out = [];
  const seen = new Set();
  for (const item of candidates) {
    const lon = Number(item.lon), lat = Number(item.lat);
    if (!Number.isFinite(lon) || !Number.isFinite(lat)) continue;
    if (lon === 0 && lat === 0 && !item.placed) continue;
    const key = String(item.key || item.href || item.title || `${lon},${lat}`);
    if (seen.has(key)) continue;
    seen.add(key);
    out.push({ key, source: String(item.source || ""), title: String(item.title || ""), kind: String(item.kind || ""), lon, lat });
  }
  return out;
}
function cpuDistances(points) {
  const n = points.length;
  const out = new Float32Array(n * n);
  const earth = 6371000;
  for (let i = 0; i < n; i++) {
    const a = points[i];
    const lat1 = a.lat * Math.PI / 180;
    for (let j = 0; j < n; j++) {
      if (i === j) { out[i*n+j] = 0; continue; }
      const b = points[j];
      const lat2 = b.lat * Math.PI / 180;
      const x = (b.lon - a.lon) * Math.PI / 180 * Math.cos((lat1 + lat2) * 0.5);
      const y = lat2 - lat1;
      out[i*n+j] = earth * Math.sqrt(x*x + y*y);
    }
  }
  return out;
}
function nearest(points, distances, k) {
  // The k nearest, not merely the nearest: a page asking what is around
  // something needs a neighbourhood, and one neighbour is not one.
  const n = points.length;
  const result = [];
  for (let i = 0; i < n; i++) {
    const near = [];
    for (let j = 0; j < n; j++) {
      if (i === j) continue;
      const d = distances[i*n+j];
      if (!Number.isFinite(d)) continue;
      near.push({ j, d });
    }
    near.sort((a, b) => a.d - b.d);
    const kept = near.slice(0, k);
    result.push({
      key: points[i].key,
      source: points[i].source,
      nearest_key: kept.length ? points[kept[0].j].key : "",
      meters: kept.length ? Math.round(kept[0].d * 10) / 10 : null,
      near: kept.map(({ j, d }) => ({
        key: points[j].key, source: points[j].source, kind: points[j].kind,
        meters: Math.round(d * 10) / 10,
      })),
    });
  }
  return result;
}

/* Beyond one tile, the estate's placed material is cut into overlapping
 * windows by longitude: every point is compared with everything inside its own
 * window, so a neighbourhood near a seam is still found, and no run allocates
 * a quadratic matrix over the whole estate. */
function tiles(points, size, overlapKm) {
  if (points.length <= size) return [points];
  const sorted = [...points].sort((a, b) => a.lon - b.lon || a.lat - b.lat);
  const step = Math.max(1, Math.floor(size / 2));
  const out = [];
  for (let start = 0; start < sorted.length; start += step) {
    const window = sorted.slice(start, start + size);
    if (window.length > 1) out.push(window);
    if (start + size >= sorted.length) break;
  }
  void overlapKm;
  return out;
}
function repoVgpu(subpath) {
  const here = path.dirname(fileURLToPath(import.meta.url));
  return pathToFileURL(path.resolve(here, "../../../site/matrix/vgpu/node_modules/vgpu/dist", subpath)).href;
}
async function gpuDistances(points, mode) {
  const mod = await import(repoVgpu(mode === "mock" ? "mock.js" : "node.js"));
  const { init, compute, storage } = mod;
  const gpu = await init();
  try {
    const n = points.length;
    const input = storage(gpu, Math.max(8, n * 8), "read");
    const output = storage(gpu, Math.max(4, n * n * 4), "read-write");
    const coords = new Float32Array(n * 2);
    for (let i = 0; i < n; i++) {
      coords[i*2] = points[i].lon;
      coords[i*2+1] = points[i].lat;
    }
    input.write(coords);
    const shader = `
      @group(0) @binding(0) var<storage, read> positions: array<vec2f>;
      @group(0) @binding(1) var<storage, read_write> distances: array<f32>;
      const N: u32 = ${n}u;
      const PI: f32 = 3.141592653589793;
      const EARTH: f32 = 6371000.0;

      @compute @workgroup_size(64)
      fn cs_main(@builtin(global_invocation_id) gid: vec3u) {
        let idx = gid.x;
        if (idx >= N * N) { return; }
        let i = idx / N;
        let j = idx % N;
        if (i == j) {
          distances[idx] = 0.0;
          return;
        }
        let a = positions[i];
        let b = positions[j];
        let lat1 = a.y * PI / 180.0;
        let lat2 = b.y * PI / 180.0;
        let x = (b.x - a.x) * PI / 180.0 * cos((lat1 + lat2) * 0.5);
        let y = lat2 - lat1;
        distances[idx] = EARTH * sqrt(x*x + y*y);
      }
    `;
    const kernel = compute(gpu, shader, {
      label: "bonfyre-stage-spatial-neighborhood",
      set: { positions: input, distances: output },
    });
    {
      /* A dispatch may ask for at most 65,535 workgroups, which a window of
       * 2,048 points exceeds by one.  Over the limit the buffer comes back
       * zeroed rather than failing, so the ask is checked before it is made. */
      const groups = Math.ceil((n * n) / 64);
      if (groups > 65535) throw new Error(`window of ${n} points needs ${groups} workgroups; use a smaller --tile`);
      kernel.dispatch(groups);
    }
    // StorageBuffer.read() waits on a copy/readback boundary.
    return new Float32Array(await output.read());
  } finally {
    gpu.dispose();
  }
}

async function main() {
  const closure = readInput();
  if (closure.schema !== "bonfyre.stage-closure.v1") {
    throw new Error("expected bonfyre.stage-closure.v1");
  }
  const points = placed(closure);
  if (points.length < 2) {
    const receipt = {
      schema: "bonfyre.stage-compute-receipt.v1",
      requested_backend: backendRequested,
      actual_backend: "none",
      job: "spatial-neighborhood",
      input_digest: digest(closure),
      points: points.length,
      pairs: points.length * points.length,
      skipped: "fewer than two placed items",
      cannot_infer: "no compute result is a semantic relation or action authority",
    };
    process.stdout.write(JSON.stringify({ receipt, nearest: [] }) + "\n");
    return;
  }
  // An all-pairs matrix is deliberately bounded.  Beyond this, Stage should
  // tile/window its spatial closure rather than allocate quadratic memory.


  const started = performance.now();
  let actual = backendRequested, fallback = "";
  const windows = tiles(points, tileSize, 2);
  const merged = new Map();
  let pairs = 0;
  for (const window of windows) {
    let distances;
    if (backendRequested === "cpu") {
      actual = "cpu";
      distances = cpuDistances(window);
    } else {
      const mode = backendRequested === "mock" ? "mock" : "node";
      try {
        distances = await gpuDistances(window, mode);
        actual = mode === "mock" ? "vgpu-mock" : "vgpu-node";
      } catch (error) {
        if (backendRequested !== "auto") throw error;
        fallback = String(error?.message || error);
        actual = "cpu";
        distances = cpuDistances(window);
      }
    }
    pairs += window.length * window.length;
    for (const row of nearest(window, distances, k)) {
      const held = merged.get(row.key);
      if (!held) { merged.set(row.key, row); continue; }
      /* A point seen in two windows keeps the nearer neighbours of both. */
      const seen = new Set(held.near.map((entry) => entry.key));
      for (const entry of row.near) if (!seen.has(entry.key)) held.near.push(entry);
      held.near.sort((a, b) => a.meters - b.meters);
      held.near = held.near.slice(0, k);
      held.nearest_key = held.near.length ? held.near[0].key : "";
      held.meters = held.near.length ? held.near[0].meters : null;
    }
  }
  const result = [...merged.values()];
  const receipt = {
    schema: "bonfyre.stage-compute-receipt.v1",
    requested_backend: backendRequested,
    actual_backend: actual,
    job: "spatial-neighborhood",
    input_digest: digest(closure),
    output_digest: digest(result),
    points: points.length,
    tiles: windows.length,
    tile_size: tileSize,
    neighbours_each: k,
    pairs,
    workgroups: Math.ceil(pairs / 64),
    elapsed_ms: Math.round((performance.now() - started) * 1000) / 1000,
    fallback_reason: fallback,
    cannot_infer: "distance computation does not establish place identity, access, presence, attendance, availability, recommendation or authority",
  };
  process.stdout.write(JSON.stringify({ receipt, nearest: result }) + "\n");
}
main().catch((error) => {
  process.stderr.write(`stage-compute: ${error?.stack || error}\n`);
  process.exitCode = 2;
});
