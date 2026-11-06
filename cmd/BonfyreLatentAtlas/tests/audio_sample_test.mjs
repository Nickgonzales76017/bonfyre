import { test, expect } from "bun:test";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { playableUrl, sampleAudio } from "../tools/audio_sample.mjs";

test("Archive metadata names a playable public file without guessing from title", async () => {
  const observed = [];
  const url = await playableUrl({ href: "https://archive.org/details/record-1" }, "project", "foreign",
    async (_project, _foreign, _twin, address) => {
      observed.push(address);
      return { receipt: { policy_decision: "allow", http_status: 200 },
        text: JSON.stringify({ files: [{ name: "noise.wav", format: "WAVE" }, { name: "song 1.mp3", format: "VBR MP3" }] }) };
    });
  expect(observed).toEqual(["https://archive.org/metadata/record-1"]);
  expect(url).toBe("https://archive.org/download/record-1/song%201.mp3");
});

test("sample uses bounded native observation and carries exact digest into acoustic material", async () => {
  const dir = mkdtempSync(join(tmpdir(), "bonfyre-audio-sample-"));
  try {
    const bodyPath = join(dir, "sample.body"), bytes = Buffer.alloc(220000, 42);
    writeFileSync(bodyPath, bytes);
    let options;
    const result = await sampleAudio({ asset: "https://audio.test/file.mp3" }, { project: "project", foreign: dir, atlas: "atlas",
      observeFn: async (_project, _foreign, _twin, _url, opts) => {
        options = opts;
        return { receipt: { policy_decision: "allow", http_status: 206, observation_kind: "sample_observed" }, bodyPath };
      },
      decodeFn: data => ({ fingerprint: "bits", distribution: [data.length], trajectory: [1] }),
    });
    expect(options).toMatchObject({ sampleBytes: 1_200_000, rangeStart: 0, readText: false });
    expect(result).toMatchObject({ sample_bytes: 220000, acoustic: { fingerprint: "bits" } });
    expect(result.sample_sha256).toHaveLength(64);
  } finally { rmSync(dir, { recursive: true, force: true }); }
});

test("robots refusal never reaches decoding", async () => {
  let decoded = false;
  const result = await sampleAudio({ asset: "https://audio.test/private.mp3" }, { project: "project", foreign: "foreign", atlas: "atlas",
    observeFn: async () => ({ receipt: { policy_decision: "robots_disallow" }, bodyPath: "/missing" }),
    decodeFn: () => { decoded = true; return {}; },
  });
  expect(result.acoustic).toBeNull();
  expect(decoded).toBe(false);
});
