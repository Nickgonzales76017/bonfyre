#!/usr/bin/env bun
// Check whether external, ghost, and app bindings have real experience depth.
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import policy from "./experience-audit.generated.json" with { type: "json" };
import { prettyCanonical } from "./json_contract.mjs";

const here = dirname(fileURLToPath(import.meta.url));
export { policy };
const load = (dir, name) => readFileSync(join(dir, name), "utf8").split(/\r?\n/)
  .filter(line => line.trim()).map(line => JSON.parse(line));
const count = (rows, key) => {
  const counts = new Map();
  for (const row of rows) for (const value of row[key] ?? []) counts.set(value, (counts.get(value) ?? 0) + 1);
  return Object.fromEntries([...counts].sort((a, b) => b[1] - a[1]));
};
const genericOnly = (values, generic) => values.length > 0 && values.every(value => generic.has(value));
const example = row => ({ id: row.id ?? null, name: row.name ?? null, source_kind: row.source_kind ?? null });

export function auditExperienceMap(dir = join(here, "..", "generated")) {
  const external = load(dir, "external_bindings.jsonl");
  const ghosts = load(dir, "ghost_bindings.jsonl");
  const apps = load(dir, "app_bindings.jsonl");
  const genericProtocol = new Set(policy.generic_protocol_chains);
  const genericHuman = new Set(policy.generic_human_chains);
  const protocolGenericOnly = external.filter(row => genericOnly(row.protocol_chains ?? [], genericProtocol));
  const humanGenericOnly = external.filter(row => genericOnly(row.human_chains ?? [], genericHuman));
  const missingProtocol = external.filter(row => !(row.protocol_chains?.length));
  const missingHuman = external.filter(row => !(row.human_chains?.length));
  const missingHome = external.filter(row => !(row.home_blueprints?.length));
  const missingTerritory = external.filter(row => !(row.territories?.length));
  const sourceKind = new Map();
  for (const row of protocolGenericOnly) {
    const kind = row.source_kind ?? "";
    sourceKind.set(kind, (sourceKind.get(kind) ?? 0) + 1);
  }
  const report = {
    schema: policy.schema,
    external_total: external.length,
    external_specific_protocol: external.length - missingProtocol.length - protocolGenericOnly.length,
    external_machine_generic_only: protocolGenericOnly.length,
    machine_generic_only_by_source_kind: Object.fromEntries([...sourceKind].sort(([a], [b]) => a.localeCompare(b))),
    external_without_protocol: missingProtocol.length,
    external_discover_only: humanGenericOnly.length,
    external_without_human_chain: missingHuman.length,
    external_without_home_blueprint: missingHome.length,
    external_without_territory: missingTerritory.length,
    ghost_total: ghosts.length,
    ghost_without_human_chain: ghosts.filter(row => !(row.human_chains?.length)).length,
    app_reference_total: apps.length,
    app_reference_without_human_chain: apps.filter(row => !(row.human_chains?.length)).length,
    protocol_binding_counts: count(external, "protocol_chains"),
    human_binding_counts: count(external, "human_chains"),
    home_binding_counts: count(external, "home_blueprints"),
    generic_protocol_examples: protocolGenericOnly.slice(0, policy.example_limit).map(example),
    discover_only_examples: humanGenericOnly.slice(0, policy.example_limit).map(example),
  };
  return { report, complete: !(missingProtocol.length || missingHuman.length || missingHome.length || missingTerritory.length) };
}

if (import.meta.main) {
  try {
    const dir = process.argv[2] ?? join(here, "..", "generated");
    const { report, complete } = auditExperienceMap(dir);
    console.log(prettyCanonical(report));
    if (!complete) process.exitCode = 1;
  } catch (error) { console.error(error.message); process.exitCode = 2; }
}
