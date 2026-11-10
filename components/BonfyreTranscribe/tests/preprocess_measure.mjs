#!/usr/bin/env bun
// Does cleaning the signal before recognition lower word error? The same
// spoken passages, at the same noise levels, are recognised as they are and
// after a named ffmpeg audio filter. WebRTC's audio processing module is the
// thing one would want here and it is not packaged on this machine; this
// measures the preprocessing step with what is, so the question "does a
// cleaner input help this recognizer" has a number before any dependency is taken.
//
//   usage: bun preprocess_measure.mjs [transcriber] [--filter "afftdn=nf=-25"]
import { existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";

const bin = process.argv[2] && !process.argv[2].startsWith("--") ? process.argv[2] : "./bonfyre-transcribe";
const at = process.argv.indexOf("--filter");
const filters = at > 0 ? [process.argv[at + 1]] : ["afftdn=nf=-25", "highpass=f=80,lowpass=f=7600,afftdn=nf=-25"];
const model = process.env.BONFYRE_WHISPER_MODEL ?? join(process.env.HOME, ".local/share/whisper/ggml-base.en.bin");
for (const need of ["say", "afconvert", "ffmpeg"]) if (!Bun.which(need)) { console.log(`SKIP: ${need} not present`); process.exit(0); }
if (!existsSync(bin) || !existsSync(model)) { console.log("SKIP: transcriber or model not present"); process.exit(0); }

const passages = [
  "The council meets on Thursday evening. Bring the signed lease and two copies of the budget. The gate code changes at noon.",
  "Forty students signed up for the spring field trip. Permission slips are due by the ninth. The bus leaves the north lot at seven thirty.",
  "The treasurer reported eleven hundred dollars in the repair fund. Three bids came in for the roof. The lowest bid was from a firm in Garland.",
  "Please move the folding tables into the library before the meeting. The projector cable is in the bottom drawer. Lock the side door when you leave.",
];
const noises = [0, 0.06, 0.12, 0.25];
const words = text => text.toLowerCase().replace(/[^a-z0-9' ]+/g, " ").split(/\s+/).filter(Boolean);
function errors(reference, hypothesis) {
  const a = words(reference), b = words(hypothesis);
  let row = Array.from({ length: b.length + 1 }, (_, j) => j);
  for (let i = 1; i <= a.length; i++) { const next = [i]; for (let j = 1; j <= b.length; j++) next[j] = Math.min(row[j] + 1, next[j - 1] + 1, row[j - 1] + (a[i - 1] === b[j - 1] ? 0 : 1)); row = next; }
  return [row[b.length], a.length];
}
const run = async argv => Bun.spawn(argv, { stdout: "ignore", stderr: "ignore" }).exited;
const work = mkdtempSync(join(tmpdir(), "bonfyre-preprocess-"));
const totals = {};
try {
  for (const [p, passage] of passages.entries()) {
    const aiff = join(work, `p${p}.aiff`), clean = join(work, `p${p}.wav`);
    await run(["say", "-o", aiff, passage]);
    await run(["afconvert", "-f", "WAVE", "-d", "LEI16@16000", "-c", "1", aiff, clean]);
    for (const gain of noises) {
      const noisy = gain ? join(work, `p${p}-${gain}.wav`) : clean;
      if (gain) await run(["ffmpeg", "-nostdin", "-loglevel", "error", "-y", "-i", clean, "-f", "lavfi", "-i", `anoisesrc=color=white:amplitude=${gain}:seed=7`,
        "-filter_complex", "amix=inputs=2:duration=first:normalize=0", "-ac", "1", "-ar", "16000", noisy]);
      for (const filter of ["as recorded", ...filters]) {
        let source = noisy;
        if (filter !== "as recorded") {
          source = join(work, `f-${p}-${gain}-${filters.indexOf(filter)}.wav`);
          // A filter this ffmpeg does not have is reported as unavailable, not as a result.
          if (await run(["ffmpeg", "-nostdin", "-loglevel", "error", "-y", "-i", noisy, "-af", filter, "-ac", "1", "-ar", "16000", source]) !== 0) { (totals[`${gain} | ${filter}`] ??= { unavailable: true }); continue; }
        }
        const out = join(work, `o-${p}-${gain}-${filter.replace(/\W+/g, "_")}`);
        if (await run([bin, source, out, "--model", model, "--no-normalize", "--no-gpu", "--output-format", "json", "--no-redecode"]) !== 0) continue;
        const doc = JSON.parse(readFileSync(join(out, `${basename(source, ".wav")}.json`), "utf8"));
        const [wrong, total] = errors(passage, doc.segments.map(s => s.text).join(" "));
        const cell = totals[`${gain} | ${filter}`] ??= { errors: 0, words: 0, passages: 0 };
        cell.errors += wrong; cell.words += total; cell.passages++;
      }
    }
  }
} finally { rmSync(work, { recursive: true, force: true }); }
console.log("noise amplitude | input | passages | word error");
for (const [key, cell] of Object.entries(totals)) console.log(cell.unavailable ? `${key} | filter not available in this ffmpeg` : `${key} | ${cell.passages} | ${(100 * cell.errors / cell.words).toFixed(2)}%`);
console.log("cannot infer: an ffmpeg spectral filter -> WebRTC's audio processing; white noise on a synthetic voice -> a room; four passages -> a population");
