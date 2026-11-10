import { test, expect } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { compile, selectWanted } from "../tools/compile_acoustic_sweep.mjs";

test("audio sources enter the derivative queue round-robin", () => {
  const items = [
    { source: "a", key: "1", kind: "audio" }, { source: "a", key: "2", kind: "audio" },
    { source: "b", key: "1", kind: "audio" }, { source: "a", key: "3", kind: "audio" },
  ];
  expect(selectWanted(items, new Set()).wanted.map(([address]) => address)).toEqual([
    "o/a/1", "o/b/1", "o/a/2", "o/a/3"]);
  expect(selectWanted(items, new Set(["o/a/1"])).wanted[0][0]).toBe("o/a/2");
});

test("acoustic sweep imports native chart contracts before projections", async () => {
  const dir = mkdtempSync(join(tmpdir(), "bonfyre-acoustic-sweep-"));
  try {
    const atlasDb = join(dir, "atlas.db"), items = join(dir, "items.jsonl");
    const db = new Database(atlasDb);
    db.exec("CREATE TABLE projection (address TEXT, chart_id TEXT)"); db.close();
    writeFileSync(items, [{ source: "a", key: "1", kind: "audio", family: "podcast", asset: "https://audio.test/a" },
      { source: "b", key: "1", kind: "audio", family: "music", asset: "https://audio.test/b" }]
      .map(row => JSON.stringify(row)).join("\n") + "\n");
    const calls = [];
    const result = await compile({ items, atlasDb, atlasBin: "atlas", project: "project", foreign: dir, work: dir, perRun: 1 }, {
      sampleFn: async () => ({ acoustic: { fingerprint: "bits", distribution: [1], trajectory: [2] }, sample_sha256: "digest" }),
      runFn: argv => { calls.push(argv); return argv[1] === "refresh" ? { unchanged: 0, parents_recomputed: 1, parents_held: 0 } : {}; },
      sleepFn: async () => {},
    });
    expect(result).toMatchObject({ waiting: 2, sampled: 1, refused: 0, sources_in_round_robin: 2 });
    expect(calls.map(argv => argv[1])).toEqual(["import", "import", "group", "group", "refresh"]);
    expect(readFileSync(join(dir, "acoustic-charts.jsonl"), "utf8")).toContain('"id":"spectrum-24"');
    expect(readFileSync(join(dir, "acoustic.jsonl"), "utf8")).toContain('"source_digest":"digest"');
  } finally { rmSync(dir, { recursive: true, force: true }); }
});
