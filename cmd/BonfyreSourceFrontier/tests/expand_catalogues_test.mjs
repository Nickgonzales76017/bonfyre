import { test, expect } from "bun:test";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { dcatRows, expand } from "../tools/expand_catalogues.mjs";

test("DCAT distributions remain virtual source claims with rights", () => {
  const seed = { name: "County Data", kind: "dcat", url: "https://county.test/data.json", place: "County" };
  const text = JSON.stringify({ dataset: [{ title: "Parcels", license: "CC0", distribution: [
    { downloadURL: "https://county.test/parcels.csv", format: "text/csv" }] }] });
  expect(dcatRows(text, seed, seed.url, 5)[0]).toMatchObject({ address: "https://county.test/parcels.csv",
    state: "candidate", virtual: true, rights: "CC0", material: "dataset" });
});

test("catalogue observation emits separate eligible catalogue and virtual members", async () => {
  const dir = mkdtempSync(join(tmpdir(), "bonfyre-catalogue-expansion-"));
  try {
    const extra = join(dir, "seeds.json"), out = join(dir, "rows.jsonl"), evidence = join(dir, "evidence.jsonl");
    writeFileSync(extra, JSON.stringify([{ name: "County Data", kind: "dcat", url: "https://county.test/data.json" }]));
    const requested = [];
    const result = await expand({ project: "project", foreign: dir, out, evidence, seeds: extra, only: "county.test", perCatalogue: 10 }, {
      observeFn: async (_project, _foreign, _twin, url) => {
        requested.push(url);
        return { receipt: { policy_decision: "allow", http_status: 200 }, receiptPath: "receipt-fixture",
          text: JSON.stringify({ dataset: [{ title: "Parcels", landingPage: "https://county.test/parcels" }] }) };
      }, sleepFn: async () => {},
    });
    expect(requested).toEqual(["https://county.test/data.json"]);
    expect(result).toEqual({ catalogues: 1, virtual_channels: 1, by_catalogue: { "County Data": 1 } });
    const rows = readFileSync(out, "utf8").trim().split("\n").map(JSON.parse);
    expect(rows[0]).toMatchObject({ address: "https://county.test/data.json", material: "catalogue", state: "eligible" });
    expect(rows[1]).toMatchObject({ address: "https://county.test/parcels", state: "candidate", virtual: true });
    expect(readFileSync(evidence, "utf8")).toContain("receipt-fixture");
  } finally { rmSync(dir, { recursive: true, force: true }); }
});

test("policy refusal produces no virtual members", async () => {
  const dir = mkdtempSync(join(tmpdir(), "bonfyre-catalogue-refusal-"));
  try {
    const extra = join(dir, "seeds.json"), out = join(dir, "rows.jsonl"), evidence = join(dir, "evidence.jsonl");
    writeFileSync(extra, JSON.stringify([{ name: "Refused", kind: "dcat", url: "https://refused.test/data.json" }]));
    const result = await expand({ project: "project", foreign: dir, out, evidence, seeds: extra, only: "refused.test", perCatalogue: 10 }, {
      observeFn: async () => ({ receipt: { policy_decision: "robots_disallow", http_status: 0 }, receiptPath: "refused-receipt", text: "" }),
    });
    expect(result.virtual_channels).toBe(0);
    const rows = readFileSync(out, "utf8").trim().split("\n").map(JSON.parse);
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({ state: "candidate", refusal: "the catalogue answered nothing this run" });
  } finally { rmSync(dir, { recursive: true, force: true }); }
});
