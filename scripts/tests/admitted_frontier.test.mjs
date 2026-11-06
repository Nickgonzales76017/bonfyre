import { test, expect } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdtempSync, mkdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { decide } from "../resource-admission.mjs";
import { load, runFrontier } from "../admitted-frontier.mjs";

test("measured admission executes and reports realised cohort loss separately", () => {
  const dir = mkdtempSync(join(tmpdir(), "bonfyre-frontier-"));
  try {
    const dbPath = join(dir, "cms.db"), corpusPath = join(dir, "vectors");
    mkdirSync(corpusPath);
    const db = new Database(dbPath);
    db.exec("CREATE TABLE representation_profile(representation TEXT, axis TEXT, availability TEXT, bytes_per_body TEXT, envelope TEXT, status TEXT)");
    db.exec("CREATE TABLE compute_site(site TEXT, cores INTEGER, backends TEXT, holds_live_data INTEGER, max_data_scope TEXT, reach_seconds TEXT, status TEXT)");
    db.query("INSERT INTO representation_profile VALUES(?,?,?,?,?,'published')").run("int8", "latent_geometry", "available", "4", "recall@2 0.95; 1 false prunes");
    db.close();
    for (const [id, vector] of Object.entries({ a: [1, 0, 0], b: [0.9, 0.1, 0], c: [0.8, 0.2, 0], d: [0, 1, 0], e: [0, 0.9, 0.1], f: [0, 0, 1], g: [0.1, 0, 0.9] })) {
      writeFileSync(join(corpusPath, `${id}.json`), JSON.stringify({ vector }));
    }
    writeFileSync(join(corpusPath, "bad.json"), "{");
    const decision = decide({ dbPath, k: 2 });
    const receipt = runFrontier(load(corpusPath), decision, { k: 2 });
    expect(decision.admitted).toBe("int8");
    expect(receipt.bodies).toBe(7);
    expect(receipt.frontier_size).toBe(14);
    expect(receipt.predicted_recall).toBe(0.95);
    expect(receipt.realised_false_prunes).toBe(receipt.realised_false_keeps);
    expect(receipt.cannot_infer).toContain("a realised loss on one cohort -> a new envelope");
    expect(receipt.no_loss_observed_on_this_cohort).toBe(receipt.realised_false_prunes === 0 && receipt.realised_false_keeps === 0);
    const strict = decide({ dbPath, k: 2, requireNoLoss: true });
    expect(strict.admitted).toBe("exact-f32");
    const strictReceipt = runFrontier(load(corpusPath), strict, { k: 2 });
    expect(strictReceipt.no_loss_observed_on_this_cohort).toBe(true);
  } finally { rmSync(dir, { recursive: true, force: true }); }
});
