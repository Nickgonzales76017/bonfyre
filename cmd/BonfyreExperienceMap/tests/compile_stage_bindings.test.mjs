import { test, expect } from "bun:test";
import { mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { appStageHints, compileStageBindings, presentationStageBindings, stageBindings } from "../tools/compile_stage_bindings.mjs";

test("lexical misses and reference-only apps emit no stage edge", () => {
  const chains = new Map([["proof", { stages: ["field-or-herd", "weather", "harvest-or-output", "market", "sale"] }]]);
  expect(stageBindings(["proof"], chains, ["unrelated-capability"])).toEqual({});
  expect(presentationStageBindings(["proof"], chains)).toEqual({});
  expect(stageBindings(["proof"], chains, ["weather"])).toEqual({ proof: ["weather"] });
  expect(appStageHints("zzzzzz")).toEqual(["OBSERVE"]);
});

test("compiles exact external, ghost and app stage records with digest/check parity", () => {
  const dir = mkdtempSync(join(tmpdir(), "bonfyre-stage-bindings-"));
  const source = join(dir, "observed.jsonl"), definitions = join(dir, "chains.tsv");
  const rows = (name, entries) => writeFileSync(join(dir, name), entries.map(JSON.stringify).join("\n") + "\n");
  writeFileSync(definitions, "id\tterritories\tentry_intents\tstages\nproof\tlife\tinspect\tfield-or-herd|weather|harvest-or-output|market|sale\n");
  rows("observed.jsonl", [{ id: "external:weather", projection: { candidate_affordances: ["weather"],
    estate_roles: [], direct_domains: ["agriculture"] }, truth_level: "observed_candidate" }]);
  rows("external_bindings.jsonl", [{ record_kind: "external", id: "external:weather", name: "Weather",
    human_chains: ["proof"] }]);
  rows("ghost_bindings.jsonl", [{ record_kind: "ghost", id: "ghost:one", name: "Ghost",
    human_chains: ["proof"], categories: [], presentation_families: [], renderer_mixes: [] }]);
  rows("app_bindings.jsonl", [{ record_kind: "app", id: "app:one", name: "Weather App",
    human_chains: ["proof"], reference_only: true, source_ref: "donor" }]);
  const { manifest, records } = compileStageBindings({ dir, source, definitions, write: true, check: true });
  expect(manifest.binding_records).toBe(3);
  expect(manifest.records_with_stage_bindings).toBe(1);
  expect(records[0].chain_bindings).toEqual({ proof: ["weather"] });
  expect(records[1].chain_bindings).toEqual({});
  expect(records[2].chain_bindings).toEqual({});
  expect(records[2].cannot_infer).toContain("interaction reference -> executable stage");
  expect(readFileSync(join(dir, "stage_bindings.jsonl"), "utf8").trim().split("\n")).toHaveLength(3);
  writeFileSync(join(dir, "stage_manifest.json"), JSON.stringify({ digest: "stale" }));
  expect(() => compileStageBindings({ dir, source, definitions, check: true })).toThrow("stale");
});
