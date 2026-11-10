import { test, expect } from "bun:test";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { findChrome } from "../capture/chrome-session.mjs";
import { htmlToPdf, pdfText, runWgsl } from "../engines/chromium-engines.mjs";

const here = Boolean(findChrome());

test.skipIf(!here)("a document written by the browser's PDF writer is read back by its PDF reader", async () => {
  const dir = mkdtempSync(join(tmpdir(), "bonfyre-engines-"));
  try {
    const html = `<!doctype html><title>Notice</title><h1>Room parents: bake sale</h1>
      <p>Friday 3pm in the library. Bring nut-free items.</p><table><tr><td>Tables</td><td>6</td></tr><tr><td>Volunteers</td><td>11</td></tr></table>
      <div style="page-break-before:always"><h2>Second page</h2><p>Cash box is with the treasurer.</p></div>`;
    const written = await htmlToPdf({ html }, join(dir, "notice.pdf"));
    expect(written).toMatchObject({ engine: "skia-pdf", operation: "html_to_pdf" });
    expect(readFileSync(written.path).subarray(0, 5).toString()).toBe("%PDF-");
    const read = await pdfText(written.path);
    expect(read).toMatchObject({ engine: "pdfium", operation: "pdf_to_text", pages: 2, source_sha256: written.sha256 });
    for (const phrase of ["Room parents: bake sale", "Friday 3pm in the library. Bring nut-free items.", "Volunteers", "11", "Second page", "Cash box is with the treasurer."])
      expect(read.text).toContain(phrase);
    // What the page never said is not in what was read.
    expect(read.text).not.toContain("<h1>");
    writeFileSync(join(dir, "not.pdf"), "hello");
    await expect(pdfText(join(dir, "not.pdf"))).rejects.toThrow("not a PDF");
    await expect(htmlToPdf({ url: "http://example.com/" }, join(dir, "x.pdf"))).rejects.toThrow("public https");
  } finally { rmSync(dir, { recursive: true, force: true }); }
}, 120_000);

// The field places every channel by an FNV-1a hash of its identity. The same
// hash as a WGSL program, checked value for value against the reference.
const fnv = `@group(0) @binding(0) var<storage, read_write> data: array<u32>;
@compute @workgroup_size(64)
fn main(@builtin(global_invocation_id) id: vec3<u32>) {
  if (id.x >= arrayLength(&data)) { return; }
  var h: u32 = 2166136261u;
  let v = data[id.x];
  for (var b: u32 = 0u; b < 4u; b = b + 1u) { h = (h ^ ((v >> (8u * b)) & 255u)) * 16777619u; }
  data[id.x] = h;
}`;
const reference = value => { let h = 2166136261 >>> 0; for (let b = 0; b < 4; b++) h = Math.imul(h ^ ((value >>> (8 * b)) & 255), 16777619) >>> 0; return h; };

test.skipIf(!here)("one WGSL program runs on the GPU and matches its reference on every value", async () => {
  const input = Array.from({ length: 100_000 }, (_, i) => (i * 2654435761) >>> 0);
  const result = await runWgsl({ wgsl: fnv, input, kind: "u32", expected: input.map(reference) });
  expect(result).toMatchObject({ engine: "dawn-tint", operation: "wgsl_compute", values: 100_000, mismatches: 0 });
  expect(result.adapter.fallback).toBe(false);
  for (const part of ["compile", "upload", "execute", "readback", "total"]) expect(result.ms[part]).toBeGreaterThanOrEqual(0);
  expect(result.ms.total).toBeGreaterThanOrEqual(result.ms.execute);
  // A wrong reference is counted, not passed.
  const off = await runWgsl({ wgsl: fnv, input: input.slice(0, 256), kind: "u32", expected: input.slice(0, 256).map((v, i) => reference(v) + (i % 2)) });
  expect(off.mismatches).toBe(128);
  await expect(runWgsl({ wgsl: fnv.replace("data[id.x] = h;", "data[id.x] = nothing;"), input: [1, 2, 3], kind: "u32" })).rejects.toThrow("did not compile");
  await expect(runWgsl({ wgsl: "fn main() {}", input: [1] })).rejects.toThrow("compute program");
}, 120_000);

// A workload with real arithmetic in it: for each of 2,048 queries, the
// nearest of 20,000 sixteen-dimensional vectors. Values are small integers so
// every squared distance is exact in a 32-bit float and the answer can be
// checked index for index. Only the answers are read back.
test.skipIf(!here)("a nearest-vector search on the GPU returns the same index as the reference for every query", async () => {
  const N = 20000, Q = 2048, D = 16;
  let seed = 7; const next = () => (seed = (seed * 1103515245 + 12345) & 0x7fffffff) % 16;
  const data = Array.from({ length: N * D }, next), queries = Array.from({ length: Q * D }, next);
  const wgsl = `@group(0) @binding(0) var<storage, read_write> m: array<f32>;
@compute @workgroup_size(64)
fn main(@builtin(global_invocation_id) id: vec3<u32>) {
  let q = id.x;
  if (q >= ${Q}u) { return; }
  var best: f32 = 3.0e38; var at: u32 = 0u;
  for (var n: u32 = 0u; n < ${N}u; n = n + 1u) {
    var sum: f32 = 0.0;
    for (var d: u32 = 0u; d < ${D}u; d = d + 1u) { let diff = m[n * ${D}u + d] - m[${N * D}u + q * ${D}u + d]; sum = sum + diff * diff; }
    if (sum < best) { best = sum; at = n; }
  }
  m[${N * D + Q * D}u + q] = f32(at);
}`;
  const started = performance.now();
  const expected = new Array(Q);
  for (let q = 0; q < Q; q++) {
    let best = Infinity, at = 0;
    for (let n = 0; n < N; n++) { let sum = 0; for (let d = 0; d < D; d++) { const diff = data[n * D + d] - queries[q * D + d]; sum += diff * diff; } if (sum < best) { best = sum; at = n; } }
    expected[q] = at;
  }
  const referenceMs = performance.now() - started;
  const result = await runWgsl({ wgsl, kind: "f32", input: [...data, ...queries, ...new Array(Q).fill(0)], invocations: Q, read: [N * D + Q * D, Q], expected: Object.assign(expected, { tolerance: 0 }) });
  expect(result).toMatchObject({ engine: "dawn-tint", invocations: Q, read_back: Q, mismatches: 0 });
  expect(result.output).toHaveLength(Q);
  console.log(`nearest-vector: reference ${referenceMs.toFixed(0)} ms on the processor; GPU ${JSON.stringify(result.ms)}`);
}, 180_000);

test.skipIf(!here || !Bun.which("cupsfilter") || !Bun.which("sips"))("the reader reads a PDF it did not write, and says a picture of a page has no text", async () => {
  const dir = mkdtempSync(join(tmpdir(), "bonfyre-engines-other-"));
  try {
    writeFileSync(join(dir, "note.txt"), "Quarterly closet reset\nBring three items for alteration.\nPickup is Tuesday.\n");
    const quartz = Bun.spawnSync(["cupsfilter", "-i", "text/plain", "-m", "application/pdf", join(dir, "note.txt")], { stderr: "ignore" });
    writeFileSync(join(dir, "quartz.pdf"), quartz.stdout);
    expect(quartz.stdout.toString("latin1")).toContain("Quartz PDFContext");
    const read = await pdfText(join(dir, "quartz.pdf"));
    expect(read.pages).toBe(1);
    for (const line of ["Quarterly closet reset", "Bring three items for alteration.", "Pickup is Tuesday."]) expect(read.text).toContain(line);
    // A page that is only a picture: one page, no characters, and no invented text.
    const png = Buffer.from("iVBORw0KGgoAAAANSUhEUgAAAAgAAAAICAIAAABLbSncAAAAFElEQVQI12P8//8/AzbAhFWUhhIAU0YDDZ0rQ3kAAAAASUVORK5CYII=", "base64");
    writeFileSync(join(dir, "dot.png"), png);
    Bun.spawnSync(["sips", "-s", "format", "pdf", join(dir, "dot.png"), "--out", join(dir, "picture.pdf")], { stdout: "ignore", stderr: "ignore" });
    const picture = await pdfText(join(dir, "picture.pdf"));
    expect(picture).toMatchObject({ pages: 1, characters: 0, text: "" });
  } finally { rmSync(dir, { recursive: true, force: true }); }
}, 120_000);
