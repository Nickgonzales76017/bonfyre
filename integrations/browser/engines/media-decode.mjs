#!/usr/bin/env bun
// One media operation, two realisations. "Decode this image to pixels" is the
// operation; the browser's WebCodecs decoder and the native FFmpeg libraries
// are two engines that realise it. Both return the same receipt -- frame
// count, size, pixel layout, a digest of the pixels -- so a consumer names the
// operation and takes whichever engine is where the work is, and the two can
// be held against each other.
//
//   bun media-decode.mjs <image> [--engine webcodecs|ffmpeg|both]
import { createHash } from "node:crypto";
import { existsSync, readFileSync } from "node:fs";
import { withChrome } from "../capture/chrome-session.mjs";

export const SCHEMA = "bonfyre.media-decode.v1";
const sha = bytes => createHash("sha256").update(bytes).digest("hex");
const types = { png: "image/png", avif: "image/avif", webp: "image/webp", jpg: "image/jpeg", jpeg: "image/jpeg", gif: "image/gif" };
const typeOf = path => types[path.toLowerCase().split(".").pop()] ?? "";
const receipt = (engine, path, more) => ({ schema: SCHEMA, operation: "decode_image", engine, source_sha256: sha(readFileSync(path)), pixel_layout: "rgba8 straight alpha, rows top to bottom", ...more });

// The browser's own decoder, through WebCodecs. No canvas and no <img>: the
// decoder is asked directly and the frame's pixels are copied out as RGBA.
export async function decodeWithWebCodecs(path) {
  const type = typeOf(path);
  if (!type) throw new Error("not an image type this operation carries");
  const bytes = readFileSync(path);
  const server = Bun.serve({ port: 0, hostname: "127.0.0.1", fetch: request => new URL(request.url).pathname === "/image" ? new Response(bytes, { headers: { "content-type": type } }) : new Response("<!doctype html><title>decode</title>", { headers: { "content-type": "text/html" } }) });
  try {
    return await withChrome(async ({ open }) => {
      const tab = await open();
      await tab.go(`http://127.0.0.1:${server.port}/`);
      const answer = await tab.page("Runtime.evaluate", { awaitPromise: true, returnByValue: true, expression: `(async () => {
        if (typeof ImageDecoder !== "function") return { refused: "this browser has no ImageDecoder" };
        if (!(await ImageDecoder.isTypeSupported(${JSON.stringify(type)}))) return { refused: "this browser does not decode ${type}" };
        const started = performance.now();
        const decoder = new ImageDecoder({ data: await (await fetch("/image")).arrayBuffer(), type: ${JSON.stringify(type)}, colorSpaceConversion: "none" });
        await decoder.tracks.ready;
        const frames = decoder.tracks.selectedTrack.frameCount;
        const { image } = await decoder.decode({ frameIndex: 0 });
        const pixels = new Uint8Array(image.codedWidth * image.codedHeight * 4);
        await image.copyTo(pixels, { format: "RGBA" });
        const out = { width: image.codedWidth, height: image.codedHeight, frames, timestamp_us: image.timestamp, ms: performance.now() - started };
        image.close();
        let binary = ""; for (let i = 0; i < pixels.length; i += 32768) binary += String.fromCharCode.apply(null, pixels.subarray(i, i + 32768));
        out.pixels = btoa(binary);
        return out;
      })()` });
      if (answer.exceptionDetails) throw new Error("the browser decoder threw: " + (answer.exceptionDetails.exception?.description ?? answer.exceptionDetails.text).slice(0, 200));
      const value = answer.result.value;
      if (value.refused) throw new Error(value.refused);
      const pixels = Buffer.from(value.pixels, "base64");
      return { ...receipt("webcodecs", path, { width: value.width, height: value.height, frames: value.frames, pixels_sha256: sha(pixels), decode_ms: Math.round(value.ms * 10) / 10 }), pixels };
    });
  } finally { server.stop(true); }
}

// The native libraries, through FFmpeg.
export async function decodeWithFfmpeg(path) {
  const ffmpeg = Bun.which("ffmpeg"), ffprobe = Bun.which("ffprobe");
  if (!ffmpeg || !ffprobe) throw new Error("FFmpeg is not present");
  const probe = Bun.spawnSync([ffprobe, "-v", "error", "-select_streams", "v:0", "-count_frames", "-show_entries", "stream=width,height,nb_read_frames,codec_name", "-of", "json", path]);
  const stream = JSON.parse(probe.stdout.toString()).streams?.[0];
  if (!stream) throw new Error("FFmpeg found no picture in that file");
  const started = performance.now();
  const out = Bun.spawnSync([ffmpeg, "-v", "error", "-i", path, "-frames:v", "1", "-f", "rawvideo", "-pix_fmt", "rgba", "-"], { maxBuffer: 512 * 1024 * 1024 });
  if (out.exitCode !== 0) throw new Error("FFmpeg did not decode it: " + out.stderr.toString().slice(0, 200));
  const pixels = Buffer.from(out.stdout);
  return { ...receipt("ffmpeg", path, { width: stream.width, height: stream.height, frames: Number(stream.nb_read_frames ?? 1), codec: stream.codec_name, pixels_sha256: sha(pixels), decode_ms: Math.round((performance.now() - started) * 10) / 10 }), pixels };
}

// How far two decodes of the same picture are apart, pixel by pixel.
export function compareDecodes(a, b) {
  if (a.width !== b.width || a.height !== b.height || a.pixels.length !== b.pixels.length) return { same_size: false };
  let differing = 0, worst = 0, total = 0;
  for (let i = 0; i < a.pixels.length; i++) { const d = Math.abs(a.pixels[i] - b.pixels[i]); if (d) { differing++; total += d; if (d > worst) worst = d; } }
  return { same_size: true, identical: differing === 0, differing_channels: differing, worst_channel_difference: worst, mean_difference: differing ? total / a.pixels.length : 0 };
}

if (import.meta.main) {
  const path = process.argv[2];
  const at = process.argv.indexOf("--engine"), engine = at > 0 ? process.argv[at + 1] : "both";
  if (!path || !existsSync(path)) { console.error("usage: media-decode.mjs <image> [--engine webcodecs|ffmpeg|both]"); process.exit(2); }
  const strip = ({ pixels, ...rest }) => rest;
  try {
    const browser = engine !== "ffmpeg" ? await decodeWithWebCodecs(path) : null, native = engine !== "webcodecs" ? await decodeWithFfmpeg(path) : null;
    console.log(JSON.stringify({ webcodecs: browser && strip(browser), ffmpeg: native && strip(native), agreement: browser && native ? compareDecodes(browser, native) : null }, null, 1));
  } catch (why) { console.error(String(why.message ?? why)); process.exit(1); }
}
