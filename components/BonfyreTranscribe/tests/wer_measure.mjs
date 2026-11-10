#!/usr/bin/env bun
// Does the repair pass lower word error? Known sentences are spoken, noise is
// mixed in at several levels, and the transcript before and after repair is
// scored against the text that was spoken. Reports numbers; asserts nothing
// about which way they fall.
import { existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";

const bin = process.argv[2] ?? "./bonfyre-transcribe";
const model = process.env.BONFYRE_WHISPER_MODEL ?? join(process.env.HOME, ".local/share/whisper/ggml-base.en.bin");
for (const need of ["say", "afconvert", "ffmpeg"]) if (!Bun.which(need)) { console.log(`SKIP: ${need} not present`); process.exit(0); }
if (!existsSync(bin) || !existsSync(model)) { console.log("SKIP: transcriber or model not present"); process.exit(0); }

const passages = [
  "The council meets on Thursday evening. Bring the signed lease and two copies of the budget. The gate code changes at noon.",
  "Forty students signed up for the spring field trip. Permission slips are due by the ninth. The bus leaves the north lot at seven thirty.",
  "The treasurer reported eleven hundred dollars in the repair fund. Three bids came in for the roof. The lowest bid was from a firm in Garland.",
  "Please move the folding tables into the library before the meeting. The projector cable is in the bottom drawer. Lock the side door when you leave.",
];
const noises = [{ label: "clean", gain: 0 }, { label: "noise 0.02", gain: 0.02 }, { label: "noise 0.06", gain: 0.06 }, { label: "noise 0.12", gain: 0.12 }];
const words = text => text.toLowerCase().replace(/[^a-z0-9' ]+/g, " ").split(/\s+/).filter(Boolean);
function errors(reference, hypothesis) {
  const a = words(reference), b = words(hypothesis);
  let row = Array.from({ length: b.length + 1 }, (_, j) => j);
  for (let i = 1; i <= a.length; i++) {
    const next = [i];
    for (let j = 1; j <= b.length; j++) next[j] = Math.min(row[j] + 1, next[j - 1] + 1, row[j - 1] + (a[i - 1] === b[j - 1] ? 0 : 1));
    row = next;
  }
  return { errors: row[b.length], words: a.length };
}
const run = async argv => { const proc = Bun.spawn(argv, { stdout: "ignore", stderr: "ignore" }); return proc.exited; };

const work = mkdtempSync(join(tmpdir(), "bonfyre-wer-"));
const rows = [];
try {
  for (const [p, passage] of passages.entries()) {
    const aiff = join(work, `p${p}.aiff`), clean = join(work, `p${p}.wav`);
    await run(["say", "-o", aiff, passage]);
    await run(["afconvert", "-f", "WAVE", "-d", "LEI16@16000", "-c", "1", aiff, clean]);
    for (const noise of noises) {
      const wav = join(work, `p${p}-${noise.gain}.wav`);
      if (noise.gain) await run(["ffmpeg", "-nostdin", "-loglevel", "error", "-y", "-i", clean, "-f", "lavfi", "-i", `anoisesrc=color=white:amplitude=${noise.gain}:seed=7`,
        "-filter_complex", "amix=inputs=2:duration=first:normalize=0", "-ac", "1", "-ar", "16000", wav]);
      for (const mode of [{ name: "flagged only", flags: [] }, { name: "every segment", flags: ["--redecode-all"] }]) {
        const out = join(work, `out-${p}-${noise.gain}-${mode.name.replace(/ /g, "-")}`);
        const source = noise.gain ? wav : clean;
        if (await run([bin, source, out, "--model", model, "--no-normalize", "--no-gpu", "--output-format", "json", ...mode.flags]) !== 0) continue;
        const doc = JSON.parse(readFileSync(join(out, `${basename(source, ".wav")}.json`), "utf8"));
        const before = doc.segments.map(s => s.text_before_repair ?? s.text).join(" "), after = doc.segments.map(s => s.text).join(" ");
        rows.push({ noise: noise.label, mode: mode.name, flagged: doc.hcp_flagged_segments, redecoded: doc.hcp_redecoded_segments, repaired: doc.hcp_repaired_segments,
          before: errors(passage, before), after: errors(passage, after) });
      }
    }
  }
} finally { rmSync(work, { recursive: true, force: true }); }

const summary = {};
for (const row of rows) {
  const cell = summary[`${row.noise} | ${row.mode}`] ??= { passages: 0, words: 0, before: 0, after: 0, flagged: 0, redecoded: 0, repaired: 0 };
  cell.passages++; cell.words += row.before.words; cell.before += row.before.errors; cell.after += row.after.errors;
  cell.flagged += row.flagged; cell.redecoded += row.redecoded; cell.repaired += row.repaired;
}
const pct = (e, w) => (100 * e / w).toFixed(2);
console.log("condition | repair candidates | passages | flagged | re-decoded | accepted | WER before | WER after");
for (const [key, c] of Object.entries(summary))
  console.log(`${key} | ${c.passages} | ${c.flagged} | ${c.redecoded} | ${c.repaired} | ${pct(c.before, c.words)}% | ${pct(c.after, c.words)}%`);
console.log("cannot infer: synthetic speech with white noise -> human speech in a room; four passages -> a population");
if (process.argv.includes("--json")) console.log(JSON.stringify({ model: basename(model), summary }));
