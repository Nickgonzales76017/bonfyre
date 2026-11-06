#!/usr/bin/env bun
// The installed browser as an engine provider. Chromium carries a PDF writer
// (Skia), a PDF reader (PDFium) and a GPU stack (Dawn, with the Tint shader
// compiler). Building any of those as a native dependency is a multi-gigabyte
// project; borrowing them from the browser already on the machine is a process
// start. Each operation here says which engine did the work and what it cost,
// so whether a smaller native dependency would pay is a measurement.
//
//   bun chromium-engines.mjs pdf <url-or-file.html> <out.pdf>
//   bun chromium-engines.mjs text <file.pdf>
//   bun chromium-engines.mjs gpu <program.json>
import { createHash } from "node:crypto";
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { withChrome } from "../capture/chrome-session.mjs";

export const SCHEMA = "bonfyre.chromium-engine.v1";
const sha = bytes => createHash("sha256").update(bytes).digest("hex");
const PDF_VIEWER = "chrome-extension://mhjfbmdgcfjbbpaeojofohoefgiehjai";
const receipt = (engine, operation, more) => ({ schema: SCHEMA, engine, operation, ...more });

// A document written here is served to the browser from this process on the
// loopback interface, so nothing is read from the network to render it.
async function serving(html, work) {
  const server = Bun.serve({ port: 0, hostname: "127.0.0.1", fetch: () => new Response(html, { headers: { "content-type": "text/html; charset=utf-8" } }) });
  try { return await work(`http://127.0.0.1:${server.port}/`); } finally { server.stop(true); }
}

// HTML to PDF, by the browser's own print path (Skia's PDF backend).
export async function htmlToPdf({ url, html }, outPath, { paper = [8.5, 11], background = true } = {}) {
  if ((url == null) === (html == null)) throw new Error("say either a url or the html itself");
  if (url) { const at = new URL(url); if (at.protocol !== "https:" || at.username || at.password) throw new Error("only a public https address is rendered"); }
  const render = address => withChrome(async ({ open }) => {
    const tab = await open();
    const started = performance.now();
    await tab.go(address);
    const loaded = performance.now();
    const pdf = Buffer.from((await tab.page("Page.printToPDF", { printBackground: background, paperWidth: paper[0], paperHeight: paper[1], preferCSSPageSize: true })).data, "base64");
    writeFileSync(outPath, pdf);
    return receipt("skia-pdf", "html_to_pdf", { path: outPath, bytes: pdf.length, sha256: sha(pdf), load_ms: Math.round(loaded - started), print_ms: Math.round(performance.now() - loaded),
      cannot_infer: ["a PDF written -> an accessible or archival (PDF/A) document", "the same bytes on another browser version; the writer stamps its own producer and time"] });
  });
  return url ? render(url) : serving(html, render);
}

// PDF to text, by the browser's own reader (PDFium). The reader is reached
// through the built-in viewer's controller, which is the browser's private
// interface and not a published one: a browser release may move it, and this
// says so plainly when it has.
export async function pdfText(pdfPath) {
  const path = resolve(pdfPath);
  if (!existsSync(path)) throw new Error(`no PDF at ${path}`);
  const bytes = readFileSync(path);
  if (bytes.subarray(0, 5).toString() !== "%PDF-") throw new Error("that file is not a PDF");
  return withChrome(async ({ open, send, deadline }) => {
    const tab = await open();
    const started = performance.now();
    await tab.go("file://" + path);
    let viewer;
    while (!(viewer = (await send("Target.getTargets")).targetInfos.find(info => info.type === "iframe" && info.url.startsWith(PDF_VIEWER)))) {
      if (Date.now() > deadline) throw new Error("the browser's PDF viewer did not open");
      await Bun.sleep(50);
    }
    const { sessionId } = await send("Target.attachToTarget", { targetId: viewer.targetId, flatten: true });
    const answer = await send("Runtime.evaluate", { awaitPromise: true, returnByValue: true, expression: `(async () => {
      // The viewer builds itself after its frame loads, so it is looked for
      // again on every wait rather than once before it exists.
      let element = null;
      for (let waited = 0; waited < 400; waited++) {
        element = document.querySelector("pdf-viewer");
        if (element && element.pluginController_ && element.docLength_ > 0) break;
        await new Promise(r => setTimeout(r, 50));
      }
      if (!element) return { slow: true };
      const controller = element.pluginController_;
      if (!controller || !controller.selectAll || !controller.getSelectedText) return { moved: true };
      if (!(element.docLength_ > 0)) return { slow: true };
      const pages = element.docLength_;
      controller.selectAll();
      await new Promise(r => setTimeout(r, 150));
      const reply = await controller.getSelectedText();
      return { text: reply.selectedText, pages };
    })()` }, sessionId);
    if (answer.result?.value?.slow) throw new Error("the browser's PDF viewer did not finish opening the document in time");
    if (answer.exceptionDetails || answer.result.value?.moved) throw new Error("this browser's PDF viewer no longer exposes the interface this reads through");
    const { text, pages } = answer.result.value;
    return receipt("pdfium", "pdf_to_text", { path, source_sha256: sha(bytes), source_bytes: bytes.length, pages, characters: text.length, text, read_ms: Math.round(performance.now() - started),
      cannot_infer: ["text read from a PDF -> the text a person sees; a scanned page has none and reading order follows the file", "an empty result -> an empty document; it may be a picture of one"] });
  });
}

// A WGSL compute program over one array of 32-bit values, run on the GPU by
// Dawn with the shader compiled by Tint. The program is the shared unit: the
// same WGSL, input and expected output can be handed to a native Dawn build
// or a browser tab. What is measured is the whole trip -- upload, execution
// and readback -- because the shader's own time is the smallest part of it.
export async function runWgsl({ wgsl, entry = "main", input, kind = "u32", workgroup = 64, expected, invocations, read }) {
  if (typeof wgsl !== "string" || !wgsl.includes("@compute")) throw new Error("a WGSL compute program is required");
  if (!["u32", "i32", "f32"].includes(kind)) throw new Error("values are u32, i32 or f32");
  if (!Array.isArray(input) || !input.length || input.length > 4_000_000) throw new Error("input is 1 to 4,000,000 values");
  // A program whose answer is smaller than its input says how many times to
  // run and which slice to bring back, so only the answer crosses back.
  const runs = invocations ?? input.length, from = read?.[0] ?? 0, count = read?.[1] ?? input.length;
  if (!Number.isInteger(runs) || runs < 1 || from < 0 || count < 1 || from + count > input.length) throw new Error("the invocation count or the slice to read is outside the input");
  // WebGPU is offered only to a secure context; loopback is one.
  return serving("<!doctype html><title>gpu</title>", address => withChrome(async ({ open }) => {
    const tab = await open();
    await tab.go(address);
    const program = `(async () => {
      const kinds = { u32: Uint32Array, i32: Int32Array, f32: Float32Array };
      const input = new kinds[${JSON.stringify(kind)}](${JSON.stringify(input)});
      if (!navigator.gpu) return { refused: "this browser offers no WebGPU" };
      const adapter = await navigator.gpu.requestAdapter();
      if (!adapter) return { refused: "no GPU adapter" };
      const device = await adapter.requestDevice();
      const problems = [];
      device.pushErrorScope("validation");
      const t0 = performance.now();
      const module = device.createShaderModule({ code: ${JSON.stringify(wgsl)} });
      const info = await module.getCompilationInfo();
      for (const message of info.messages) if (message.type === "error") problems.push(message.lineNum + ":" + message.linePos + " " + message.message);
      if (problems.length) return { refused: "the shader did not compile", problems };
      const pipeline = await device.createComputePipelineAsync({ layout: "auto", compute: { module, entryPoint: ${JSON.stringify(entry)} } });
      const t1 = performance.now();
      const buffer = device.createBuffer({ size: input.byteLength, usage: GPUBufferUsage.STORAGE | GPUBufferUsage.COPY_SRC | GPUBufferUsage.COPY_DST });
      device.queue.writeBuffer(buffer, 0, input);
      const read = device.createBuffer({ size: ${count} * 4, usage: GPUBufferUsage.MAP_READ | GPUBufferUsage.COPY_DST });
      const bind = device.createBindGroup({ layout: pipeline.getBindGroupLayout(0), entries: [{ binding: 0, resource: { buffer } }] });
      const t2 = performance.now();
      const encoder = device.createCommandEncoder();
      const pass = encoder.beginComputePass();
      pass.setPipeline(pipeline); pass.setBindGroup(0, bind);
      pass.dispatchWorkgroups(Math.ceil(${runs} / ${Number(workgroup)}));
      pass.end();
      encoder.copyBufferToBuffer(buffer, ${from} * 4, read, 0, ${count} * 4);
      device.queue.submit([encoder.finish()]);
      await device.queue.onSubmittedWorkDone();
      const t3 = performance.now();
      await read.mapAsync(GPUMapMode.READ);
      const output = Array.from(new kinds[${JSON.stringify(kind)}](read.getMappedRange().slice(0)));
      read.unmap();
      const t4 = performance.now();
      const failed = await device.popErrorScope();
      if (failed) return { refused: "the GPU refused the program", problems: [failed.message] };
      const about = adapter.info || {};
      return { output, adapter: { vendor: about.vendor, architecture: about.architecture, fallback: Boolean(adapter.isFallbackAdapter ?? about.isFallbackAdapter) },
        ms: { compile: t1 - t0, upload: t2 - t1, execute: t3 - t2, readback: t4 - t3, total: t4 - t0 } };
    })()`;
    const answer = await tab.page("Runtime.evaluate", { expression: program, awaitPromise: true, returnByValue: true });
    if (answer.exceptionDetails) throw new Error("the GPU program threw: " + (answer.exceptionDetails.exception?.description ?? answer.exceptionDetails.text).slice(0, 300));
    const result = answer.result.value;
    if (result.refused) throw new Error(result.refused + (result.problems ? ": " + result.problems.join("; ") : ""));
    const round = Object.fromEntries(Object.entries(result.ms).map(([name, ms]) => [name, Math.round(ms * 100) / 100]));
    let mismatches = null;
    if (expected) {
      if (expected.length !== result.output.length) throw new Error("the expected output is a different length");
      const tolerance = kind === "f32" ? (expected.tolerance ?? 1e-5) : 0;
      mismatches = result.output.reduce((count, value, i) => count + (Math.abs(value - expected[i]) > tolerance ? 1 : 0), 0);
    }
    return receipt("dawn-tint", "wgsl_compute", { values: input.length, invocations: runs, read_back: count, kind, adapter: result.adapter, ms: round, mismatches, output: result.output,
      program_sha256: sha(wgsl), cannot_infer: ["zero mismatches on this adapter -> the same floats on another device", "a fast execute time -> a fast operation; upload and readback are part of it",
        "this run -> a native Dawn build behaving the same; only the browser's was run"] });
  }));
}

if (import.meta.main) {
  const [verb, a, b] = process.argv.slice(2);
  try {
    let out;
    if (verb === "pdf" && a && b) out = await htmlToPdf(/^https:/.test(a) ? { url: a } : { html: readFileSync(a, "utf8") }, b);
    else if (verb === "text" && a) out = await pdfText(a);
    else if (verb === "gpu" && a) { const { output, ...rest } = await runWgsl(JSON.parse(readFileSync(a, "utf8"))); out = { ...rest, output_values: output.length }; }
    else { console.error("usage: chromium-engines.mjs pdf <url|file.html> <out.pdf> | text <file.pdf> | gpu <program.json>"); process.exit(2); }
    console.log(JSON.stringify(out, null, 1));
  } catch (why) { console.error(String(why.message ?? why)); process.exit(1); }
}
