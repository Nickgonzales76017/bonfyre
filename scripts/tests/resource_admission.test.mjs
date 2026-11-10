import { test, expect } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { decide } from "../resource-admission.mjs";

test("only measured same-axis same-k affordable envelopes may be admitted", () => {
  const dir = mkdtempSync(join(tmpdir(), "bonfyre-admission-"));
  const path = join(dir, "cms.db");
  try {
    const db = new Database(path);
    db.exec("CREATE TABLE representation_profile(representation TEXT, axis TEXT, availability TEXT, bytes_per_body TEXT, envelope TEXT, status TEXT)");
    db.exec("CREATE TABLE compute_site(site TEXT, cores INTEGER, backends TEXT, holds_live_data INTEGER, max_data_scope TEXT, reach_seconds TEXT, status TEXT)");
    const add = db.query("INSERT INTO representation_profile VALUES(?,?,?,?,?,'published')");
    add.run("exact-f32", "latent_geometry", "available", "1000", "recall@5 1.0");
    add.run("unmeasured", "latent_geometry", "available", "1", "");
    add.run("wrong-axis", "model_tensor", "available", "2", "recall@5 1.0");
    add.run("wrong-k", "latent_geometry", "available", "3", "recall@10 1.0");
    add.run("too-lossy", "latent_geometry", "available", "4", "recall@5 0.90; 1 false prunes");
    add.run("affordable", "latent_geometry", "available", "5", "recall@5 0.97; 0 false prunes");
    db.query("INSERT INTO compute_site VALUES(?,?,?,?,?,?,'published')").run("public-box", 12, "reference,hvm4", 0, "public", "1");
    db.query("INSERT INTO compute_site VALUES(?,?,?,?,?,?,'published')").run("private-box", 4, "reference", 1, "institution-private", "3");
    db.query("INSERT INTO compute_site VALUES(?,?,?,?,?,?,'published')").run("typo-box", 20, "reference", 0, "institution_private", "1");
    db.close();

    const admission = decide({ dbPath: path, dataScope: "institution-private" });
    expect(admission.admitted).toBe("affordable");
    expect(admission.measured_recall).toBe(0.97);
    expect(admission.placement.site).toBe("private-box");
    expect(admission.placement.considered.find(x => x.site === "typo-box").verdict).toContain("unrecognised clearance");
    expect(admission.considered.map(x => x.verdict)).toEqual([
      "no measured envelope; an envelope is earned by an executed trial",
      "measured on axis model_tensor, not latent_geometry; an envelope does not cross axes",
      "envelope measured at k=10, budget stated at k=5; recall is not monotone in k",
      "preserves 0.9, budget requires 0.95",
      "satisfies the budget at 0.97",
      "reserved as the fallback",
    ]);
    for (const sensitive of ["authority", "money", "identity", "publication", "effect", "legal", "privacy", "fulfillment"]) {
      expect(decide({ dbPath: path, workClass: sensitive }).admitted).toBe("exact-f32");
    }
    expect(decide({ dbPath: path, minRecall: 0.99 }).admitted).toBe("exact-f32");
    expect(decide({ dbPath: path, dataScope: "restricted" }).placement.site).toBeNull();
    expect(decide({ dbPath: path, backend: "hvm4", dataScope: "institution-private" }).placement.site).toBeNull();
    // Strict no-loss mode does not remove the default discovery envelope.
    // Recall of 0.97 is not a proven lossless replacement even when a fixture
    // reports zero prunes; the fallback remains usable.
    const stricter = decide({ dbPath: path, requireNoLoss: true });
    expect(stricter.admitted).toBe("exact-f32");
    expect(stricter.no_loss_required).toBe(true);
    // The fixture writer above was closed deliberately so decide() reads
    // the same persisted profiles through a separate read-only connection.
    // Reopen a fresh writer for later cases; Bun finalizes prepared queries on close.
    const update = new Database(path);
    try {
      const addStrict = update.query("INSERT INTO representation_profile VALUES(?,?,?,?,?,'published')");
      addStrict.run("unreported-prunes", "latent_geometry", "available", "1", "recall@5 1.0");
      addStrict.run("observed-prune", "latent_geometry", "available", "2", "recall@5 1.0; 1 false prunes");
      addStrict.run("lossless", "latent_geometry", "available", "3", "recall@5 1.0; 0 false prunes");
    } finally { update.close(); }
    const exactSafe = decide({ dbPath: path, requireNoLoss: true });
    expect(exactSafe.admitted).toBe("lossless");
    expect(exactSafe.considered.find(x => x.representation === "unreported-prunes").verdict).toContain("missing is not zero");
    expect(exactSafe.considered.find(x => x.representation === "observed-prune").verdict).toContain("zero false prunes");
    expect(decide({ dbPath: path, minRecall: 0.95 }).admitted).toBe("unreported-prunes");
  } finally { rmSync(dir, { recursive: true, force: true }); }
});
