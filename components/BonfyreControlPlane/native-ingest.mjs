import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { basename, join } from "node:path";
import { LiveSemanticTrack, SemanticEvent } from "./live-semantic-track.mjs";

export class IngestError extends Error {}
const binDir = () => process.env.BONFYRE_BIN ?? join(homedir(), ".bonfyre", "bin");
const segment = () => join(binDir(), "bonfyre-segment");
const clips = () => join(binDir(), "bonfyre-clips");
export const available = () => existsSync(segment()) && existsSync(clips());

// Two programs answer to the name BonfyreTranscribe and do different work. A
// caller names the one it means; neither is a default for the other.
//   managed  cmd/BonfyreTranscribe: recognition, denoise, speech split, vocabulary
//   hcp      components/BonfyreTranscribe: recognition, HCP diagnosis, re-decode repair
const recognizers = {
  managed: { binary: () => process.env.BONFYRE_TRANSCRIBE ?? join(binDir(), "bonfyre-transcribe"),
    argv: (binary, audio, out, { model }) => [binary, audio, out, "--model", model ?? "base", "--media-prep-binary", process.env.BONFYRE_MEDIA_PREP ?? join(binDir(), "bonfyre-media-prep")],
    transcript: () => "transcript.json",
    segment: row => ({ start_ms: row.t0, end_ms: row.t1, quality: row.quality }),
    contributes: ["recognition"] },
  hcp: { binary: () => process.env.BONFYRE_TRANSCRIBE_HCP ?? join(import.meta.dir, "..", "BonfyreTranscribe", "bonfyre-transcribe"),
    argv: (binary, audio, out, { model, redecode = true }) => [binary, audio, out, "--no-normalize", "--output-format", "json",
      ...(model ? ["--model", model] : []), ...(redecode ? [] : ["--no-redecode"])],
    transcript: audio => `${basename(audio).replace(/\.[^.]+$/, "")}.json`,
    segment: row => ({ start_ms: row.t0_ms, end_ms: row.t1_ms, quality: row.quality, hcp_quality: row.hcp_quality, repair: row.repair,
      ...(row.text_before_repair == null ? {} : { text_before_repair: row.text_before_repair }) }),
    contributes: ["recognition", "diagnosis", "repair"] },
};
export const recognizerAvailable = name => Boolean(recognizers[name]) && existsSync(recognizers[name].binary());

// Python's round semantics are half-to-even; preserve the millisecond clock.
export function secToMs(seconds) {
  const value = Number(seconds) * 1000;
  if (!Number.isFinite(value)) throw new IngestError("media-clock offset must be finite");
  const floor = Math.floor(value);
  const fraction = value - floor;
  return fraction === 0.5 ? floor + (floor % 2) : Math.round(value);
}

// A trace, when one is attached, sees every native process this module starts
// and the files handed to it. Nothing is traced unless a caller asks.
let tracer = null;
export function traceWith(trace) { const before = tracer; tracer = trace; return before; }

async function run(argv) {
  if (tracer) return tracer.span(basename(argv[0]), { kind: "process", argv: argv.slice(1).map(arg => arg.length > 120 ? arg.slice(0, 120) : arg), reads: argv.slice(1).filter(arg => arg.startsWith("/")) }, () => spawn(argv));
  return spawn(argv);
}

async function spawn(argv) {
  const proc = Bun.spawn(argv, { stdout: "pipe", stderr: "pipe" });
  const timeout = setTimeout(() => proc.kill(), 120_000);
  const [stdout, stderr, code] = await Promise.all([new Response(proc.stdout).text(), new Response(proc.stderr).text(), proc.exited]);
  clearTimeout(timeout);
  if (code !== 0) throw new IngestError(`${basename(argv[0])} failed (${code}): ${stderr.trim() || stdout.trim()}`);
  return stdout;
}

// Recognise audio with the named program and write the Whisper-shaped
// segments[].start/end/text document the downstream organs read. What the
// recognizer did -- and did not do -- is returned beside the path.
export async function runTranscribe(audio, outDir, { recognizer, ...options } = {}) {
  const chosen = recognizers[recognizer];
  if (!chosen) throw new IngestError(`name a recognizer (${Object.keys(recognizers).join(" or ")}); the two BonfyreTranscribe programs do different work and none is assumed`);
  const binary = chosen.binary();
  if (!existsSync(binary)) throw new IngestError(`${recognizer} recognizer not present at ${binary}`);
  if (!existsSync(audio)) throw new IngestError(`no audio at ${audio}`);
  const raw = join(outDir, "recognition");
  mkdirSync(raw, { recursive: true });
  await run(chosen.argv(binary, audio, raw, options));
  const produced = join(raw, chosen.transcript(audio));
  if (!existsSync(produced)) throw new IngestError(`${recognizer} recognizer wrote no transcript at ${produced}`);
  const doc = JSON.parse(readFileSync(produced, "utf8"));
  if (!Array.isArray(doc.segments)) throw new IngestError(`${produced} carries no segments[]`);
  const segments = doc.segments.map(row => {
    const shaped = chosen.segment(row);
    if (!Number.isFinite(shaped.start_ms) || !Number.isFinite(shaped.end_ms)) throw new IngestError(`${produced} carries a segment without a media-clock span`);
    const { start_ms, end_ms, ...rest } = shaped;
    return { start: start_ms / 1000, end: end_ms / 1000, text: String(row.text ?? ""), ...rest };
  });
  const repaired = recognizer === "hcp";
  const recognition = { recognizer, binary, contributes: chosen.contributes, segment_count: segments.length,
    diagnosed_segments: repaired ? doc.hcp_flagged_segments ?? 0 : null,
    redecoded_segments: repaired ? doc.hcp_redecoded_segments ?? 0 : null,
    repaired_segments: repaired ? doc.hcp_repaired_segments ?? 0 : null,
    cannot_infer: ["a recognised segment -> what was actually said", "a flagged segment -> a wrong one", "an accepted repair -> a correct transcript"] };
  const whisperJson = join(outDir, "whisper.json");
  // BonfyreSegment and BonfyreClips locate the first "segments" key, so the array leads.
  writeFileSync(whisperJson, JSON.stringify({ text: segments.map(row => row.text.trim()).join(" "), segments, recognition }));
  return { whisperJson, recognition };
}

export function ingestTranscript(track, whisperJson, { speaker = "" } = {}) {
  const doc = JSON.parse(readFileSync(whisperJson, "utf8"));
  if (!Array.isArray(doc.segments)) throw new IngestError(`${whisperJson} carries no segments[]; the organs read Whisper JSON with segments[].start/end/text and this is a different shape`);
  for (const segment of doc.segments) {
    const text = String(segment.text ?? "").trim();
    if (text) track.say(secToMs(segment.start), text, speaker);
  }
  return track.byKind("utterance").length;
}

export async function runSegment(whisperJson, outDir) {
  if (!existsSync(segment())) throw new IngestError(`BonfyreSegment not present at ${segment()}`);
  await run([segment(), "all", whisperJson, outDir]);
  const path = join(outDir, "boundaries.json");
  if (!existsSync(path)) throw new IngestError(`BonfyreSegment wrote no boundaries.json in ${outDir}`);
  const doc = JSON.parse(readFileSync(path, "utf8"));
  return (doc.boundaries ?? []).map(boundary => ({ index: boundary.index, label: boundary.label, kind: boundary.type,
    start_ms: secToMs(boundary.start), end_ms: secToMs(boundary.end) }));
}

export function ingestBoundaries(track, boundaries) {
  for (const boundary of boundaries) track.append(new SemanticEvent(boundary.start_ms, "chapter", boundary.label, "", "native:BonfyreSegment"));
  return boundaries.length;
}

export async function runClips(whisperJson, outDir, { top = 5 } = {}) {
  if (!existsSync(clips())) throw new IngestError(`BonfyreClips not present at ${clips()}`);
  await run([clips(), "all", whisperJson, outDir, "--top", String(top)]);
  const path = join(outDir, "clips.json");
  if (!existsSync(path)) throw new IngestError(`BonfyreClips wrote no clips.json in ${outDir}`);
  const doc = JSON.parse(readFileSync(path, "utf8"));
  return (doc.clips ?? []).map(clip => ({ rank: clip.rank, score: Number(clip.score), start_ms: secToMs(clip.start), end_ms: secToMs(clip.end),
    reasons: String(clip.reasons ?? "").split(/\s+/).filter(Boolean), text: String(clip.text ?? "").trim() }));
}

export function ingestClips(track, candidates) {
  for (const clip of candidates) track.append(new SemanticEvent(clip.start_ms, "clip_candidate", `rank ${clip.rank} score ${clip.score}`, "", "native:BonfyreClips"));
  return candidates.length;
}

export async function ingestStream(occurrenceId, whisperJson, outDir, { speaker = "", top = 5 } = {}) {
  const track = new LiveSemanticTrack(occurrenceId);
  const utterances = ingestTranscript(track, whisperJson, { speaker });
  const boundaries = await runSegment(whisperJson, join(outDir, "seg"));
  ingestBoundaries(track, boundaries);
  const candidates = await runClips(whisperJson, join(outDir, "clip"), { top });
  ingestClips(track, candidates);
  return { track, utterances, boundaries, clips: candidates };
}

// Recording -> named recognizer -> timed semantic track. The transcript is
// produced here rather than supplied.
export async function ingestAudio(occurrenceId, audio, outDir, { recognizer, model, redecode, speaker = "", top = 5 } = {}) {
  const { whisperJson, recognition } = await runTranscribe(audio, outDir, { recognizer, model, redecode });
  return { ...(await ingestStream(occurrenceId, whisperJson, outDir, { speaker, top })), recognition, whisperJson };
}

// Audio as it arrives. ffmpeg cuts the source into fixed windows and names each
// one in a list the moment it closes; a window is yielded only then, so nothing
// downstream can read audio that has not happened yet. `input` is ffmpeg's own
// input arguments: a capture device (["-f", "avfoundation", "-i", ":0"]) or a
// recording replayed at its own pace (["-re", "-i", path]).
export async function* captureSegments(input, dir, { seconds = 5 } = {}) {
  const ffmpeg = Bun.which("ffmpeg");
  if (!ffmpeg) throw new IngestError("ffmpeg is not present; no capture source can be cut into windows");
  mkdirSync(dir, { recursive: true });
  const list = join(dir, "windows.csv");
  const proc = Bun.spawn([ffmpeg, "-nostdin", "-loglevel", "error", ...input, "-ac", "1", "-ar", "16000", "-c:a", "pcm_s16le",
    "-f", "segment", "-segment_time", String(seconds), "-segment_list", list, "-segment_list_type", "csv", "-segment_list_flags", "live",
    join(dir, "window-%05d.wav")], { stdout: "ignore", stderr: "pipe" });
  let exited = false, yielded = 0;
  const done = proc.exited.then(code => { exited = true; return code; });
  try {
    for (;;) {
      const wasExited = exited;
      const rows = existsSync(list) ? readFileSync(list, "utf8").split("\n").filter(Boolean) : [];
      for (; yielded < rows.length; yielded++) {
        const [name, start, end] = rows[yielded].split(",");
        yield { index: yielded, path: join(dir, name), start_ms: secToMs(start), end_ms: secToMs(end) };
      }
      if (wasExited) break;
      await Bun.sleep(100);
    }
    const code = await done;
    if (code !== 0) throw new IngestError(`capture ended (${code}): ${(await new Response(proc.stderr).text()).trim()}`);
  } finally { if (!exited) proc.kill(); }
}

// A running occurrence -> named recognizer -> track, window by window. Each
// window's utterances are placed at the occurrence's media clock and reach the
// track only after that window has closed. Chapters and clip candidates are
// read once the capture ends, over everything recognised.
export async function ingestLive(occurrenceId, windows, outDir, { recognizer, model, redecode, speaker = "", top = 5, onWindow } = {}) {
  const track = new LiveSemanticTrack(occurrenceId);
  const segments = [], recognitions = [];
  for await (const window of windows) {
    const { whisperJson, recognition } = await runTranscribe(window.path, join(outDir, `window-${String(window.index).padStart(5, "0")}`), { recognizer, model, redecode });
    let said = 0;
    for (const row of JSON.parse(readFileSync(whisperJson, "utf8")).segments) {
      const text = String(row.text ?? "").trim();
      if (!text) continue;
      const placed = { ...row, start: row.start + window.start_ms / 1000, end: row.end + window.start_ms / 1000 };
      segments.push(placed);
      track.say(secToMs(placed.start), text, speaker);
      said++;
    }
    recognitions.push({ window: window.index, start_ms: window.start_ms, end_ms: window.end_ms, utterances: said, ...recognition });
    await onWindow?.({ window, track, utterances: track.byKind("utterance").length });
  }
  const whisperJson = join(outDir, "whisper.json");
  writeFileSync(whisperJson, JSON.stringify({ text: segments.map(row => row.text.trim()).join(" "), segments, recognitions }));
  let boundaries = [], candidates = [];
  if (segments.length) {
    boundaries = await runSegment(whisperJson, join(outDir, "seg"));
    ingestBoundaries(track, boundaries);
    candidates = await runClips(whisperJson, join(outDir, "clip"), { top });
    ingestClips(track, candidates);
  }
  return { track, utterances: track.byKind("utterance").length, boundaries, clips: candidates, windows: recognitions, whisperJson };
}

export function clipCovering(candidates, at_ms) {
  return candidates.filter(candidate => candidate.start_ms <= at_ms && at_ms < candidate.end_ms).sort((a, b) => a.rank - b.rank)[0] ?? null;
}
