import { test, expect } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { compile, listingRow, protocolOf, rowsFor } from "../tools/compile_inventories.mjs";

test("protocol guesses do not become rights or admission", () => {
  expect(protocolOf("https://data.test/FeatureServer")).toBe("arcgis");
  expect(listingRow("https://data.test/FeatureServer")).toEqual({
    address: "https://data.test/FeatureServer", protocol: "arcgis", material: "dataset",
    discovered_from: "address_conventions_are_candidate_protocol_hints_not_observed_capability_or_admission",
  });
  expect(listingRow("https://data.test/FeatureServer", { protocol: "arcgis" }).discovered_from).toBeUndefined();
  expect(listingRow("https://data.test/opaque", { protocol: "__proto__" }).material).toBeUndefined();
  expect(listingRow("https://x.test/feed.xml", { state: "eligible", refusal: "robots: deny" })).toMatchObject({
    protocol: "rss", material: "publication", state: "eligible", refusal: "robots: deny" });
});

test("separate inventories retain overlap and native import labels", () => {
  const dir = mkdtempSync(join(tmpdir(), "bonfyre-inventories-"));
  try {
    const v4Jsonl = join(dir, "v4.jsonl"), advertisements = join(dir, "ads.sqlite"), liveSources = join(dir, "live.json");
    writeFileSync(v4Jsonl, JSON.stringify({ id: "ext-01234", observed_name: "Community", primary_url: "https://same.test/feed.xml",
      source_kind: "rss", projection: { candidate_affordances: [] } }) + "\n");
    const db = new Database(advertisements);
    db.exec("CREATE TABLE candidate_advertisement (identity TEXT, candidate_kind TEXT)");
    db.query("INSERT INTO candidate_advertisement VALUES (?, ?)").run("advertisement-for-ext_01234", "candidate");
    db.close();
    writeFileSync(liveSources, JSON.stringify({ rows: [{ url: "https://same.test/feed.xml", family: "local-voices", source: "native-source" }] }));
    const imports = [];
    const result = compile({ db: join(dir, "frontier.sqlite"), frontier: "frontier", out: join(dir, "out"), v4Jsonl, advertisements, liveSources },
      argv => { imports.push(argv); return { code: 0, stdout: JSON.stringify({ label: argv[argv.indexOf("--label") + 1] ?? "init" }), stderr: "" }; });
    expect(imports.map(argv => argv[1])).toEqual(["init", "import", "import", "import"]);
    expect(imports.slice(1).map(argv => argv[argv.indexOf("--label") + 1])).toEqual(["EXTERNAL_ESTATE_V4", "ADDRESS_PLANE", "LIVE_SOURCE_DISCOVERY"]);
    expect(result.ADDRESS_PLANE.label).toBe("ADDRESS_PLANE");
    const v4 = readFileSync(join(dir, "out", "external_estate_v4.v4_jsonl.jsonl"), "utf8").trim().split("\n").map(JSON.parse);
    const ads = readFileSync(join(dir, "out", "address_plane.advertisements.jsonl"), "utf8").trim().split("\n").map(JSON.parse);
    expect(v4[0].address).toBe(ads[0].address);
    expect(v4[0].listing_id).toBe("ext-01234");
    expect(ads[0].state).toBeUndefined();
    expect(rowsFor("liveSources", liveSources)[0]).toMatchObject({ census_source: "native-source", state: "eligible" });
  } finally { rmSync(dir, { recursive: true, force: true }); }
});
