import { test, expect } from "bun:test";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { auditExperienceMap } from "../tools/audit_experience_map.mjs";

const fixture = (external, ghosts, apps) => {
  const dir = mkdtempSync(join(tmpdir(), "bonfyre-experience-audit-"));
  for (const [name, rows] of Object.entries({ external_bindings: external, ghost_bindings: ghosts, app_bindings: apps }))
    writeFileSync(join(dir, `${name}.jsonl`), rows.map(row => JSON.stringify(row)).join("\n") + "\n");
  return dir;
};

test("distinguishes specific external protocols from generic discovery", () => {
  const dir = fixture([
    { id: "school", name: "School", source_kind: "institution", protocol_chains: ["rss-atom"],
      human_chains: ["learning"], home_blueprints: ["school"], territories: ["local"] },
    { id: "ghost", name: "Ghost", source_kind: "person", protocol_chains: ["machine-generic"],
      human_chains: ["discover"], home_blueprints: ["person"], territories: ["local"] },
  ], [{ human_chains: ["discover"] }, { human_chains: [] }], [{ human_chains: [] }]);
  const { report, complete } = auditExperienceMap(dir);
  expect(complete).toBe(true);
  expect(report.external_specific_protocol).toBe(1);
  expect(report.external_machine_generic_only).toBe(1);
  expect(report.external_discover_only).toBe(1);
  expect(report.ghost_without_human_chain).toBe(1);
  expect(report.app_reference_without_human_chain).toBe(1);
  expect(report.machine_generic_only_by_source_kind).toEqual({ person: 1 });
});

test("missing chains, homes, and territories fail the audit", () => {
  const dir = fixture([{ id: "incomplete" }], [], []);
  const { report, complete } = auditExperienceMap(dir);
  expect(complete).toBe(false);
  expect(report.external_without_protocol).toBe(1);
  expect(report.external_without_human_chain).toBe(1);
  expect(report.external_without_home_blueprint).toBe(1);
  expect(report.external_without_territory).toBe(1);
});
