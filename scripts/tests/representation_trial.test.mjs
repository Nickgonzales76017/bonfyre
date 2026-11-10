import { test, expect } from "bun:test";
import { mkdtempSync, mkdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { load, topK } from "../admitted-frontier.mjs";
import { runTrial } from "../representation-trial.mjs";

test("bounded stable top-k agrees with full ranking, including ties", () => {
  const ids = ["a", "b", "c", "d", "e", "f"];
  const vectors = [0, 2, 2, 1, 3, 2];
  const rep = { vectors, score: (_a, b) => b };
  expect(topK(ids, rep, 0, 3)).toEqual(["e", "b", "c"]);
  for (let qi = 0; qi < ids.length; qi++) {
    const expected = ids.map((id, j) => [id, j, vectors[j]]).filter(row => row[1] !== qi)
      .sort((a, b) => b[2] - a[2]).slice(0, 3).map(row => row[0]);
    expect(topK(ids, rep, qi, 3)).toEqual(expected);
  }
});

test("one trial measures all declared representations against exact", () => {
  const dir = mkdtempSync(join(tmpdir(), "bonfyre-trial-"));
  const corpusPath = join(dir, "vectors");
  try {
    mkdirSync(corpusPath);
    for (const [id, vector] of Object.entries({ a: [1, 0, 0, 0], b: [0.9, 0.1, 0, 0], c: [0.8, 0.2, 0, 0], d: [0, 1, 0, 0], e: [0, 0.9, 0.1, 0], f: [0, 0, 1, 0], g: [0.1, 0, 0.9, 0] })) {
      writeFileSync(join(corpusPath, `${id}.json`), JSON.stringify({ vector }));
    }
    const receipt = runTrial(load(corpusPath), "vectors", 2);
    expect(receipt.trials.map(row => row.representation).sort()).toEqual(["exact-f32", "halved-f32", "int8", "sign-1bit"]);
    expect(receipt.trials.find(row => row.representation === "exact-f32").recall_at_k).toBe(1);
    expect(receipt.trials.every(row => row.false_prunes === row.false_keeps)).toBe(true);
    expect(receipt.cannot_infer).toContain("preservation on this corpus -> preservation on another");
  } finally { rmSync(dir, { recursive: true, force: true }); }
});
