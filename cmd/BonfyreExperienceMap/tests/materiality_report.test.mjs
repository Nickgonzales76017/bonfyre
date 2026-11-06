import { test, expect } from "bun:test";
import { mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { materialityReport, materialityScore, writeMateriality } from "../tools/materiality_report.mjs";

test("reports weak generic bindings without treating them as admitted material", () => {
  const dir = mkdtempSync(join(tmpdir(), "bonfyre-materiality-"));
  const writeRows = (name, rows) => writeFileSync(join(dir, `${name}.jsonl`), rows.map(JSON.stringify).join("\n") + "\n");
  writeRows("external_bindings", [
    { id: "external:school", name: "School", source_kind: "institution", protocol_chains: ["rss-atom"],
      human_chains: ["learning"], home_blueprints: ["school"], territories: ["local"],
      reagent_roles: ["material_source"], direct_domains: ["education"], slug_roots: ["school"] },
    { id: "external:ghost", name: "Ghost", source_kind: "person", protocol_chains: ["machine-generic"],
      human_chains: ["discover"], home_blueprints: ["person"], territories: ["local"] },
  ]);
  writeRows("ghost_bindings", [{ human_chains: ["discover"] }]);
  writeRows("app_bindings", [{ human_chains: ["learning"] }]);
  writeRows("chains", [{ id: "learning" }, { id: "discover" }]);
  writeRows("protocols", [{ id: "rss-atom" }, { id: "machine-generic" }]);
  writeRows("home_blueprints", [{ id: "school" }, { id: "person" }]);
  writeRows("chain_reference_lattice", [
    { chain_id: "learning", reference_count: 1, interaction_reference_count: 1 },
    { chain_id: "discover", reference_count: 1, interaction_reference_count: 1 },
  ]);
  writeFileSync(join(dir, "manifest.json"), JSON.stringify({ build_digest: "fixture" }));
  writeFileSync(join(dir, "stage_manifest.json"), JSON.stringify({ binding_records: 4, digest: "stage" }));
  const result = materialityReport(dir);
  expect(result.report.external.specific_protocol).toBe(1);
  expect(result.report.external.generic_protocol_only).toBe(1);
  expect(result.report.external.discover_only).toBe(1);
  expect(result.report.external.missing_reagent_role).toBe(1);
  expect(result.report.stage_projection.stale).toBe(false);
  expect(result.report.structural_green).toBe(true);
  expect(result.report.material_green).toBe(false);
  expect(result.report.cannot_infer).toContain("materiality score -> source admission");
  expect(materialityScore({ protocol_chains: ["machine-generic"], human_chains: ["discover"] })).toBe(0);
  writeMateriality(dir, result);
  expect(JSON.parse(readFileSync(join(dir, "materiality_manifest.json"), "utf8"))).toEqual(result.report);
  expect(readFileSync(join(dir, "materiality_gaps.jsonl"), "utf8")).toContain('"gap": "generic_protocol_only"');
});
