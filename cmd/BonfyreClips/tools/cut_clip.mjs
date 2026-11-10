// Direct local-media clip workflow. The owner makes a playable clip, not a
// synthetic brief or browser-only simulation; rights/publication stay separate.
import { existsSync, readFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { exportClip } from "./export_clip.mjs";

const seconds = value => {
  if (typeof value !== "string" || !/^\d+(?::[0-5]?\d){0,2}(?:[.,]\d{1,3})?$/.test(value))
    throw Error("start/end must be seconds or hh:mm:ss.mmm");
  const units = value.replace(",", ".").split(":");
  const number = units.reduce((total, item) => total * 60 + Number(item), 0);
  if (!Number.isFinite(number) || number < 0 || number > 24 * 3600)
    throw Error("clip timestamp outside supported 24-hour range");
  return number;
};
export function parseSrt(file, start, end, maxCues = 100) {
  const text = readFileSync(resolve(file), "utf8").replace(/\r\n/g, "\n").replace(/\r/g, "\n");
  const entries = [];
  for (const block of text.split(/\n{2,}/).filter(Boolean)) {
    const lines = block.split("\n").filter(Boolean);
    const i = lines.findIndex(line => line.includes("-->"));
    if (i < 0) throw Error("caption block is missing a timestamp range");
    const matched = /^\s*(\d{1,2}:\d{2}:\d{2}[.,]\d{1,3})\s*-->\s*(\d{1,2}:\d{2}:\d{2}[.,]\d{1,3})\s*$/.exec(lines[i]);
    if (!matched) throw Error("caption timestamp must use SRT hh:mm:ss,mmm format");
    const a = seconds(matched[1]), b = seconds(matched[2]), caption = lines.slice(i + 1).join(" ").trim();
    if (b <= a || !caption || a < 0) throw Error("invalid caption time or text");
    if (b <= start || a >= end) continue;
    const from = Math.max(0, a - start), to = Math.min(end - start, b - start);
    if (entries.length && from < entries.at(-1).end - 0.0001)
      throw Error("overlapping captions must be reviewed before rendering");
    entries.push({ start: from, end: to, text: caption });
    if (entries.length > maxCues) throw Error("transcript exceeds the permitted synchronized-caption limit");
  }
  return entries;
}
const required = new Set(["source","start","end","out","title","credit"]);
const supported = new Set([...required,"brand","font","captions","source-ref","crop-x"]);
export function parseArgs(argv) {
  const opts = {};
  for (let i = 0; i < argv.length; i += 2) {
    const name = argv[i]?.replace(/^--/, "");
    if (!argv[i]?.startsWith("--") || !supported.has(name) || !argv[i+1] || opts[name] !== undefined)
      throw Error("unknown, duplicate or missing named clip argument: " + (argv[i] ?? ""));
    opts[name] = argv[i+1];
  }
  for (const key of required) if (!opts[key]) throw Error("missing --" + key);
  const start = seconds(opts.start), end = seconds(opts.end);
  if (end <= start || end - start > 180) throw Error("clip must be 0 < duration <= 180 seconds");
  const fonts = [opts.font, process.env.BONFYRE_CLIP_FONT,
    "/System/Library/Fonts/Supplemental/Arial.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"].filter(Boolean);
  const font = fonts.find(file => existsSync(file));
  if (!font) throw Error("no font found; pass --font FILE");
  const ref = opts["source-ref"];
  if (ref && (!/^https?:\/\//.test(ref) || ref.length > 2048 || /[\x00-\x20]/.test(ref)))
    throw Error("source reference must be a bounded HTTP(S) address");
  return {
    output: resolve(opts.out),
    plan: { source: resolve(opts.source), font: resolve(font), start, end,
      title: opts.title, brand: opts.brand || "Aurekai",
      credit: opts.credit, source_reference: ref || null,
      crop_x: opts["crop-x"] === undefined ? undefined : Number(opts["crop-x"]),
      captions: opts.captions ? parseSrt(opts.captions, start, end) : [] }
  };
}
export function cut(argv, options) {
  const { output, plan } = parseArgs(argv);
  const result = exportClip(plan, output, options);
  return { page: join(output, "index.html"), video: result.output,
    source_sha256: result.source_sha256, output_sha256: result.output_sha256,
    caption_count: result.caption_count };
}
if (import.meta.main) {
  try {
    const result = cut(process.argv.slice(2));
    process.stdout.write("Decoded media clip created and independently verified.\n"
      + "Open: " + result.page + "\n" + "Media: " + result.video + "\n"
      + "Source SHA256: " + result.source_sha256 + "\n"
      + "Clip SHA256: " + result.output_sha256 + "\n"
      + "Captions: " + result.caption_count + "\n"
      + "Rights and publication: not authorized by export; separate owner review required.\n");
  } catch (error) {
    console.error("clip export refused: " + error.message);
    process.exitCode = 1;
  }
}
