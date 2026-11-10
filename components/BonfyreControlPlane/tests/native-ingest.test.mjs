import { test, expect } from "bun:test";
import { existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { available, captureSegments, clipCovering, ingestAudio, ingestLive, ingestStream, ingestTranscript, IngestError, recognizerAvailable, runTranscribe, secToMs, traceWith } from "../native-ingest.mjs";
import { Trace } from "../trace.mjs";
import { LiveSemanticTrack } from "../live-semantic-track.mjs";

const lines = [
  [0, 4.2, "hey Deep Ellum we're Neon June thanks for being here tonight"],
  [4.2, 9.8, "this next one is off the new record"],
  [9.8, 14.1, "we recorded it right here in Dallas last winter"],
  [30, 34.5, "alright so a couple things before we keep going"],
  [34.5, 39.9, "we're playing Trees again on Friday doors at eight"],
  [39.9, 44, "tickets are in our profile go grab them they move fast"],
  [60, 64.4, "huge thanks to Red Bull for backing this run of shows"],
  [64.4, 68.2, "seriously could not do this without them"],
  [95, 99.6, "okay this is the last one we love you Deep Ellum"],
  [99.6, 104, "see you Friday at Trees"],
];

test("seconds use the same half-even millisecond clock and require Whisper segments", async () => {
  expect(secToMs(14.099999)).toBe(14100);
  expect(secToMs(0.0005)).toBe(0);
  expect(secToMs(0.0015)).toBe(2);
  const directory = mkdtempSync(join(tmpdir(), "bonfyre-ingest-shape-"));
  try {
    const path = join(directory, "bad.json");
    await Bun.write(path, JSON.stringify({ text: "no segments here" }));
    expect(() => ingestTranscript(new LiveSemanticTrack("x"), path)).toThrow(IngestError);
  } finally { rmSync(directory, { recursive: true, force: true }); }
});

test.skipIf(!available())("real Segment and Clips outputs become a track without taxonomy remapping or publication", async () => {
  const directory = mkdtempSync(join(tmpdir(), "bonfyre-ingest-native-"));
  try {
    const whisper = join(directory, "stream.json");
    const segments = lines.map(([start, end, text], id) => ({ id, seek: 0, start, end, text: ` ${text}`, tokens: [], temperature: 0, avg_logprob: -0.28, compression_ratio: 1.4, no_speech_prob: 0.02 }));
    await Bun.write(whisper, JSON.stringify({ text: lines.map(line => line[2]).join(" "), segments, language: "en" }));
    const result = await ingestStream("live:neon-june", whisper, directory, { speaker: "Neon June" });
    expect(result.utterances).toBe(10);
    const utterances = result.track.byKind("utterance");
    expect(utterances.slice(0, 3).map(event => event.at_ms)).toEqual([0, 4200, 9800]);
    expect(utterances.find(event => event.value.includes("Trees again")).at_ms).toBe(34500);
    expect(utterances[0].speaker).toBe("Neon June");
    expect(result.boundaries.map(boundary => boundary.start_ms)).toEqual([0, 30000, 60000, 95000]);
    expect(result.boundaries.map(boundary => boundary.kind)).toEqual(["intro", "idea", "idea", "closing"]);
    expect(result.track.byKind("chapter")).toHaveLength(4);
    expect(result.track.byKind("chapter")[0].source).toBe("native:BonfyreSegment");
    expect(result.clips[0].rank).toBe(1);
    expect(result.clips[0].text).toContain("Red Bull");
    expect(result.clips[0].text).not.toContain("Trees");
    expect(result.clips[0].score).toBeGreaterThan(result.clips[1].score);
    expect(result.clips[0].reasons).toContain("actionable");
    expect(result.clips[0].reasons).toContain("good-pacing");
    expect(result.clips.find(clip => clip.text.includes("Trees again")).rank).toBeGreaterThan(1);
    expect(clipCovering(result.clips, 62_000)?.rank).toBe(1);
    expect(clipCovering(result.clips, 20_000)).toBeNull();
    expect(result.track.byKind("claim")).toEqual([]);
    expect(result.track.byKind("clip_candidate").every(event => event.tier() === "moment")).toBe(true);
    expect(result.track.cool()[0]).toEqual([]);
    expect(result.track.compression().semantic_events).toBe(14 + result.clips.length);
  } finally { rmSync(directory, { recursive: true, force: true }); }
});

// Audio is spoken here rather than fixtured, so these run where a speech
// source, the organs and a recognizer exist, and say so where they do not.
// Where the GPU backend is absent or denied (sandboxes, CI, VMs), run with
// BONFYRE_TRANSCRIBE_NO_GPU=1: both recognizers take --no-gpu and decode on CPU.
const spoken = "The council meets on Thursday evening. Bring the signed lease and two copies of the budget. The gate code changes at noon.";
const speechSource = Boolean(Bun.which("say") && Bun.which("afconvert"));
async function speak(directory) {
  const aiff = join(directory, "speech.aiff"), wav = join(directory, "speech.wav");
  await Bun.spawn(["say", "-o", aiff, spoken]).exited;
  await Bun.spawn(["afconvert", "-f", "WAVE", "-d", "LEI16@16000", "-c", "1", aiff, wav]).exited;
  return wav;
}

test("a recognizer is named, never assumed", async () => {
  await expect(runTranscribe("/nonexistent.wav", "/tmp", {})).rejects.toThrow("name a recognizer");
  await expect(runTranscribe("/nonexistent.wav", "/tmp", { recognizer: "whisper" })).rejects.toThrow("name a recognizer");
});

for (const recognizer of ["managed", "hcp"]) {
  test.skipIf(!speechSource || !available() || !recognizerAvailable(recognizer))(`a recording becomes a timed track through the ${recognizer} recognizer`, async () => {
    const directory = mkdtempSync(join(tmpdir(), `bonfyre-ingest-audio-${recognizer}-`));
    try {
      const wav = await speak(directory);
      const model = recognizer === "hcp" ? join(process.env.HOME, ".local/share/whisper/ggml-base.en.bin") : "base";
      if (recognizer === "hcp" && !existsSync(model)) return;
      const result = await ingestAudio("council-recording", wav, directory, { recognizer, model, speaker: "clerk" });
      expect(result.recognition.recognizer).toBe(recognizer);
      expect(result.utterances).toBeGreaterThan(0);
      const heard = result.track.byKind("utterance").map(event => event.value.toLowerCase()).join(" ");
      expect(heard).toContain("council");
      expect(heard).toContain("lease");
      const written = JSON.parse(readFileSync(result.whisperJson, "utf8"));
      for (const row of written.segments) expect(row.end).toBeGreaterThan(row.start);
      if (recognizer === "hcp") {
        expect(result.recognition.contributes).toEqual(["recognition", "diagnosis", "repair"]);
        expect(result.recognition.redecoded_segments).toBeLessThanOrEqual(result.recognition.diagnosed_segments);
        expect(result.recognition.repaired_segments).toBeLessThanOrEqual(result.recognition.redecoded_segments);
      } else {
        expect(result.recognition.contributes).toEqual(["recognition"]);
        expect(result.recognition.repaired_segments).toBeNull();
      }
    } finally { rmSync(directory, { recursive: true, force: true }); }
  }, 180_000);
}

// A capture is replayed at its own pace here, which exercises the same window
// cutter a device would feed. No microphone is opened by this test.
test.skipIf(!speechSource || !Bun.which("ffmpeg") || !available() || !recognizerAvailable("managed"))("a running occurrence reaches the track window by window, never ahead of the audio", async () => {
  const directory = mkdtempSync(join(tmpdir(), "bonfyre-ingest-live-"));
  try {
    const aiff = join(directory, "long.aiff"), wav = join(directory, "long.wav");
    await Bun.spawn(["say", "-o", aiff, `${spoken} The treasurer reports forty dollars in the fund. The next meeting is in the library on the ninth.`]).exited;
    await Bun.spawn(["afconvert", "-f", "WAVE", "-d", "LEI16@16000", "-c", "1", aiff, wav]).exited;
    const seen = [];
    const started = Date.now();
    const result = await ingestLive("council-live", captureSegments(["-re", "-i", wav], join(directory, "capture"), { seconds: 4 }), directory, {
      recognizer: "managed", model: "base", speaker: "clerk",
      onWindow: ({ window, track, utterances }) => seen.push({ end_ms: window.end_ms, utterances, wall_ms: Date.now() - started,
        latest_ms: Math.max(-1, ...track.byKind("utterance").map(event => event.at_ms)) }),
    });
    expect(result.windows.length).toBeGreaterThanOrEqual(3);
    // Each window was heard only after its audio had been played out.
    for (const moment of seen.slice(0, -1)) expect(moment.wall_ms).toBeGreaterThanOrEqual(moment.end_ms - 500);
    // The track never held an utterance from later than the window just closed.
    for (const moment of seen) expect(moment.latest_ms).toBeLessThan(moment.end_ms);
    // It grew as the occurrence went on, rather than arriving at the end.
    expect(seen[0].utterances).toBeLessThan(seen.at(-1).utterances);
    const heard = result.track.byKind("utterance").map(event => event.value.toLowerCase()).join(" ");
    expect(heard).toContain("council");
    // What a later window says is not asserted word for word: a fixed window
    // cuts sentences, and the recognizer was seen to garble a cut one
    // ("the treasurer reports for for the first time in the last year") and
    // drop a late word about one run in thirty. What must hold is that the
    // later windows were heard at all and placed after the first.
    expect(result.windows.at(-1).utterances + result.windows.at(-2).utterances).toBeGreaterThan(0);
    expect(Math.max(...result.track.byKind("utterance").map(event => event.at_ms))).toBeGreaterThanOrEqual(result.windows[1].start_ms);
    const clock = result.track.byKind("utterance").map(event => event.at_ms);
    expect(clock).toEqual([...clock].sort((a, b) => a - b));
  } finally { rmSync(directory, { recursive: true, force: true }); }
}, 240_000);

test.skipIf(!available())("a trace of the stream shows each native process and the transcript both organs read", async () => {
  const directory = mkdtempSync(join(tmpdir(), "bonfyre-ingest-trace-"));
  const trace = new Trace("native-ingest");
  const before = traceWith(trace);
  try {
    const whisper = join(directory, "whisper.json");
    await Bun.write(whisper, JSON.stringify({ text: lines.map(line => line[2]).join(" "), segments: lines.map(([start, end, text]) => ({ start, end, text })), language: "en" }));
    await ingestStream("traced", whisper, directory);
    const summary = trace.summary();
    expect(summary.processes_started).toBe(2);
    expect(Object.keys(summary.by_operation).sort()).toEqual(["bonfyre-clips", "bonfyre-segment"]);
    // The same transcript is parsed by two processes; the trace names it by its bytes.
    expect(summary.bodies_read_more_than_once).toHaveLength(1);
    expect(summary.bodies_read_more_than_once[0]).toMatchObject({ times: 2, readers: ["bonfyre-segment", "bonfyre-clips"] });
    // The file is in the format Perfetto loads: complete events with microsecond times.
    const written = JSON.parse(readFileSync(trace.write(join(directory, "trace.json")), "utf8"));
    expect(written.traceEvents).toHaveLength(2);
    for (const event of written.traceEvents) { expect(event.ph).toBe("X"); expect(event.dur).toBeGreaterThan(0); expect(Number.isInteger(event.ts)).toBe(true); }
  } finally { traceWith(before); rmSync(directory, { recursive: true, force: true }); }
});
