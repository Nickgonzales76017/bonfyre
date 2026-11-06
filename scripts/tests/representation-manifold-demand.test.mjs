import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { existsSync, mkdtempSync, readFileSync, rmSync, symlinkSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { apply, portfolio } from "../representation-manifold-shift.mjs";

test("demand lowers only its exact source cut, retaining full compilation as default", async () => {
  const dir = mkdtempSync(join(tmpdir(), "bonfyre-manifold-demand-"));
  try {
    const cms = new Database(join(dir, "cms.db"));
    cms.exec("CREATE TABLE internet_source(source TEXT); INSERT INTO internet_source VALUES ('selected')");
    cms.exec("CREATE TABLE other_collection(code TEXT); INSERT INTO other_collection VALUES ('not-selected')");
    cms.close();
    const items = new Database(join(dir, "items.db"));
    items.exec("CREATE TABLE item(source TEXT, key TEXT, kind TEXT); INSERT INTO item VALUES ('s','k','body')");
    items.close();
    const cmsBytes = readFileSync(join(dir, "cms.db"));
    const itemBytes = readFileSync(join(dir, "items.db"));
    const first = await apply(dir, join(dir, "no-substrates"), { kinds: ["cms.internet_source"] });
    expect(first.demand.kinds).toEqual(["cms.internet_source"]);
    expect(first.demand.materialized).toBe(1);
    expect(first.manifest.map(r => r.kind)).toEqual(["cms.internet_source"]);
    expect(existsSync(join(dir, "representations/manifest.json"))).toBe(false);
    expect(existsSync(join(dir, "representation-portfolio.json"))).toBe(false);
    expect(existsSync(join(dir, "representations/parquet/cms/other_collection.parquet"))).toBe(false);
    expect(existsSync(join(dir, "representations/parquet/items/item.parquet"))).toBe(false);
    const ledgerPath = join(dir, "representation-manifold.db");
    const ledger = new Database(ledgerPath);
    expect(ledger.query("SELECT semantic_kind FROM source_binding").all().map(r => r.semantic_kind))
      .toEqual(["cms.internet_source"]);
    expect(ledger.query("SELECT COUNT(*) n FROM representation_target").get().n)
      .toBe(Object.values(portfolio.classes).flat().length);
    expect(ledger.query("SELECT COUNT(*) n FROM physical_realization").get().n).toBe(1);
    // Other owners may already have supplied valid realization proofs.
    ledger.query("UPDATE representation_target SET status='physical',reason='verified synthetic Zarr receipt' WHERE semantic_kind=? AND representation_kind='zarr'")
      .run("cms.internet_source");
    ledger.close();
    const again = await apply(dir, join(dir, "no-substrates"), { kinds: ["cms.internet_source"] });
    expect(again.demand.materialized).toBe(1);
    const readback = new Database(ledgerPath, { readonly: true, create: false });
    const zarr = readback.query("SELECT status,reason FROM representation_target WHERE semantic_kind=? AND representation_kind='zarr'")
      .get("cms.internet_source");
    expect(zarr).toEqual({ status: "physical", reason: "verified synthetic Zarr receipt" });
    expect(readback.query("SELECT COUNT(*) n FROM source_binding").get().n).toBe(1);
    readback.close();
    expect(readFileSync(join(dir, "cms.db")).equals(cmsBytes)).toBe(true);
    expect(readFileSync(join(dir, "items.db")).equals(itemBytes)).toBe(true);
    const full = await apply(dir, join(dir, "no-substrates"));
    expect(full.manifest.filter(row => row.state === "physical").length).toBe(3);
    expect(existsSync(join(dir, "representations/manifest.json"))).toBe(true);
    expect(existsSync(join(dir, "representation-portfolio.json"))).toBe(true);
  } finally { rmSync(dir, { recursive: true, force: true }); }
});

test("invalid and symlinked demand refuse before creating a manifold", async () => {
  const dir = mkdtempSync(join(tmpdir(), "bonfyre-manifold-demand-refuse-"));
  try {
    const db = new Database(join(dir, "cms.db"));
    db.exec("CREATE TABLE internet_source(source TEXT)");
    db.close();
    const original = readFileSync(join(dir, "cms.db"));
    await expect(apply(dir, join(dir, "no-substrates"), { kinds: ["cms.not-real"] }))
      .rejects.toThrow("demanded table unavailable");
    await expect(apply(dir, join(dir, "no-substrates"), { kinds: ["cms../not-real"] }))
      .rejects.toThrow("invalid demanded semantic kind");
    symlinkSync(join(dir, "cms.db"), join(dir, "foreign.db"));
    await expect(apply(dir, join(dir, "no-substrates"), { kinds: ["foreign.internet_source"] }))
      .rejects.toThrow("demand source unavailable");
    expect(existsSync(join(dir, "representation-manifold.db"))).toBe(false);
    expect(readFileSync(join(dir, "cms.db")).equals(original)).toBe(true);
  } finally { rmSync(dir, { recursive: true, force: true }); }
});
