import { test, expect } from "bun:test";
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { best, candidates, instanceLike, ordinary, point, policy, resolveEntities } from "../resolve-entities.mjs";

const binding = (name, qid, links, kind, coord = "") => ({ name: { value: name }, item: { value: `http://www.wikidata.org/entity/${qid}` },
  n: { value: String(links) }, kindLabel: { value: kind }, d: { value: "description" }, coord: { value: coord } });

test("identity admission distinguishes names, classes, ordinary prose and thin single words", async () => {
  expect(ordinary("The Best")).toBe(true);
  expect(ordinary("The Guardian")).toBe(false);
  expect(instanceLike("type of city")).toBe(false);
  expect(instanceLike("state of the United States")).toBe(true);
  expect(point("Point(-97.3 32.8)")).toEqual([-97.3, 32.8]);
  expect(point("not a point")).toEqual([null, null]);
  const stronger = best([binding("fort worth", "Q1", 12, "city"), binding("Fort Worth", "Q2", 41, "city")], ["Fort Worth"]);
  expect(stronger.get("Fort Worth").qid).toBe("Q2");
  const dir = mkdtempSync(join(tmpdir(), "bonfyre-entity-"));
  try {
    const rows = [{ name: "Fort Worth", mentions: 15, publishers: 4 }, { name: "Gaza", mentions: 13, publishers: 3 },
      { name: "The Best", mentions: 12, publishers: 3 }, { name: "Apple", mentions: 11, publishers: 3 },
      { name: "Welcome", mentions: 10, publishers: 3 }];
    const requests = [];
    const fetcher = async url => {
      const query = new URL(url).searchParams.get("query");
      requests.push(query);
      const all = [binding("fort worth", "Q2", 41, "city"), binding("Gaza", "Q3", 70, "state"),
        binding("The Best", "Q4", 23, "single"), binding("Apple", "Q5", 100, "fruit"),
        binding("Welcome", "Q6", 28, "city")];
      return { ok: true, json: async () => ({ results: { bindings: query.includes("SELECT ?item ?coord") ? [
        { item: { value: "http://www.wikidata.org/entity/Q2" }, coord: { value: "Point(-97.3 32.8)" } } ] : [
        ...all.filter(row => query.toLowerCase().includes(`${JSON.stringify(row.name.value.toLowerCase())}@en`)) ] } }) };
    };
    const result = await resolveEntities({ catalog: dir, candidateRows: rows, batch: 3, fetcher, pause: async () => {} });
    expect(result.asked).toBe(5);
    expect(result.state).toBe("complete");
    expect(result.resolved).toBe(5);
    expect(result.kept).toBe(2);
    expect(result.refused_as_ordinary_english).toBe(1);
    expect(result.refused_single_word_class).toBe(1);
    expect(result.refused_single_word_thinly_linked).toBe(1);
    expect(result.placed).toBe(1);
    expect(requests.length).toBe(4);
    const entities = readFileSync(join(dir, "entity.jsonl"), "utf8").trim().split("\n").map(JSON.parse);
    expect(entities.map(row => row.name)).toEqual(["Fort Worth", "Gaza"]);
    expect(entities[0]).toMatchObject({ qid: "Q2", lon: -97.3, lat: 32.8, publishers: 4 });
    expect(result.cannot_infer).toContain("Mention is not aboutness");
  } finally { rmSync(dir, { recursive: true, force: true }); }
});

test("provider refusal preserves the prior complete identity cut", async () => {
  const dir = mkdtempSync(join(tmpdir(), "bonfyre-entity-refusal-"));
  const prior = join(dir, "entity.jsonl");
  writeFileSync(prior, '{"name":"prior"}\n');
  try {
    const report = await resolveEntities({ catalog: dir, candidateRows: [{ name: "Fort Worth", mentions: 2, publishers: 2 }],
      fetcher: async () => { throw new Error("provider refused"); }, pause: async () => {} });
    expect(report.state).toBe("incomplete");
    expect(report.prior_cut_preserved).toBe(true);
    expect(report.wrote).toBeNull();
    expect(report.refused_batches).toHaveLength(1);
    expect(readFileSync(prior, "utf8")).toBe('{"name":"prior"}\n');
  } finally { rmSync(dir, { recursive: true, force: true }); }
});

test("candidate names are compiled from retained Parquet, not invented by the resolver", () => {
  const dir = mkdtempSync(join(tmpdir(), "bonfyre-entity-parquet-"));
  try {
    const path = join(dir, "material.parquet").replaceAll("'", "''");
    const built = Bun.spawnSync(["duckdb", "-c", `COPY (SELECT * FROM (VALUES ('Fort Worth weekly','publisher-one'),('Fort Worth today','publisher-two'),('January Sale','publisher-three')) AS v(title,publisher)) TO '${path}' (FORMAT PARQUET)`]);
    expect(built.exitCode).toBe(0);
    const rows = candidates(dir, 2, 20);
    expect(rows.some(row => row.name === "Fort Worth" && row.publishers === 2)).toBe(true);
    expect(rows.some(row => row.name === "January Sale")).toBe(false);
    expect(candidates(dir, 3, 20)).toEqual([]);
    const stage = join(dir, "stage");
    const installed = Bun.spawnSync(["make", "-C", join(import.meta.dir, ".."), "install-entity-resolution", `MEMBRANE_ROOT=${stage}`]);
    expect(installed.exitCode).toBe(0);
    expect(existsSync(join(stage, "scripts/entity-resolution.cue"))).toBe(true);
    expect(policy.single_word_sitelink_floor).toBe(60);
  } finally { rmSync(dir, { recursive: true, force: true }); }
});
