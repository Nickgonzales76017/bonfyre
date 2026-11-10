// Range-limited ForeignTwin audio sampling, then native Atlas decoding.
import { createHash } from "node:crypto";
import { closeSync, existsSync, mkdtempSync, openSync, readFileSync, readSync, rmSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { observePublic } from "../../BonfyreSourceFrontier/tools/foreign_observation.mjs";

const sampleBytes = 1_200_000;
const sha = value => createHash("sha256").update(value).digest("hex");

export async function playableUrl(item, project, foreign, observeFn = observePublic) {
  if (item.asset) return item.asset;
  if (!String(item.href ?? "").startsWith("https://archive.org/details/")) return "";
  const identifier = item.href.split("/").at(-1);
  if (!identifier) return "";
  const metaUrl = `https://archive.org/metadata/${encodeURIComponent(identifier)}`;
  const meta = await observeFn(project, foreign, `audio-meta-${sha(metaUrl).slice(0, 16)}`, metaUrl,
    { maxBytes: 2_000_000, timeout: 60000 });
  if (meta.receipt.policy_decision !== "allow" || !meta.text) return "";
  let doc;
  try { doc = JSON.parse(meta.text); } catch { return ""; }
  for (const want of ["VBR MP3", "128Kbps MP3", "MP3", "Ogg Vorbis"]) for (const file of doc.files ?? []) {
    if (String(file.format ?? "").toLowerCase().includes(want.toLowerCase()) && file.name)
      return `https://archive.org/download/${encodeURIComponent(identifier)}/${encodeURIComponent(file.name)}`;
  }
  return "";
}

export function decodeSample(data, atlas) {
  const dir = mkdtempSync(join(tmpdir(), "bonfyre-acoustic-"));
  try {
    const raw = join(dir, "sample.bin"), pcm = join(dir, "sample.pcm");
    writeFileSync(raw, data);
    const ffmpeg = Bun.spawnSync(["ffmpeg", "-v", "quiet", "-y", "-i", raw, "-t", "30", "-ac", "1", "-ar", "11025", "-f", "s16le", pcm],
      { stdout: "pipe", stderr: "pipe" });
    if (ffmpeg.exitCode !== 0 || !existsSync(pcm) || statSync(pcm).size < 11025 * 2 * 5) return null;
    const output = Bun.spawnSync([atlas, "acoustic", "--pcm", pcm], { stdout: "pipe", stderr: "pipe" });
    if (output.exitCode !== 0) return null;
    try { return JSON.parse(new TextDecoder().decode(output.stdout)); } catch { return null; }
  } finally { rmSync(dir, { recursive: true, force: true }); }
}

export async function sampleAudio(item, { project, foreign, atlas, start = 0, observeFn = observePublic,
  decodeFn = decodeSample } = {}) {
  const url = await playableUrl(item, project, foreign, observeFn);
  if (!url) return { acoustic: null, reason: "no playable file" };
  const observation = await observeFn(project, foreign, `audio-sample-${sha(`${url}:${start}`).slice(0, 16)}`, url,
    { sampleBytes, rangeStart: start, readText: false, timeout: 90000 });
  const receipt = observation.receipt;
  if (receipt.policy_decision !== "allow") return { acoustic: null, reason: `observation policy: ${receipt.policy_decision ?? "unproven"}`, receipt };
  if (!existsSync(observation.bodyPath) || Number(receipt.http_status ?? 0) < 200 || Number(receipt.http_status ?? 0) >= 300)
    return { acoustic: null, reason: "audio range did not answer", receipt };
  const size = statSync(observation.bodyPath).size;
  if (size <= 0 || size > sampleBytes) return { acoustic: null, reason: "audio range exceeded bound", receipt };
  const fd = openSync(observation.bodyPath, "r");
  let data;
  try { data = Buffer.alloc(size); data = data.subarray(0, readSync(fd, data, 0, size, 0)); }
  finally { closeSync(fd); }
  const acoustic = decodeFn(data, atlas);
  return { acoustic, reason: acoustic ? null : "undecodable sample", receipt,
    sample_sha256: sha(data), sample_bytes: data.length, url };
}
