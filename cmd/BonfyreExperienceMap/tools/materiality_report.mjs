#!/usr/bin/env bun
// Derived diagnostics over the compiled ExperienceMap; never an admission decision.
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import auditPolicy from "./experience-audit.generated.json" with { type: "json" };
import policy from "./experience-materiality.generated.json" with { type: "json" };
import { prettyCanonical, pythonFlat } from "./json_contract.mjs";

const generated = join(dirname(fileURLToPath(import.meta.url)), "..", "generated");
const readJson = path => existsSync(path) ? JSON.parse(readFileSync(path, "utf8")) : {};
const readJsonl = (dir, name) => {
  const path = join(dir, name);
  return existsSync(path) ? readFileSync(path, "utf8").split(/\r?\n/).filter(line => line.trim()).map(JSON.parse) : [];
};
const values = (row, key) => row[key] ?? [];
const ids = rows => new Set(rows.map(row => row.id).filter(Boolean));
const difference = (left, right) => [...left].filter(id => !right.has(id)).sort();
const counter = () => new Map();
const increment = (map, value) => map.set(value, (map.get(value) ?? 0) + 1);
const addAll = (map, items) => { for (const item of items) increment(map, item); };
const counted = map => Object.fromEntries(map);
const countRows = (rows, key) => { const map = counter(); for (const row of rows) addAll(map, values(row, key)); return map; };
const classNames = roles => Object.entries(policy.role_classes)
  .filter(([, members]) => members.some(member => roles.includes(member))).map(([name]) => name);
const genericOnly = (items, generic) => items.length > 0 && items.every(item => generic.has(item));

export function materialityScore(row) {
  const protocol = new Set(values(row, "protocol_chains"));
  const human = new Set(values(row, "human_chains"));
  const genericProtocol = new Set(auditPolicy.generic_protocol_chains);
  const genericHuman = new Set(auditPolicy.generic_human_chains);
  return Number(protocol.size > 0 && !genericOnly([...protocol], genericProtocol)) +
    Number(human.size > 0 && !genericOnly([...human], genericHuman)) +
    ["reagent_roles", "direct_domains", "home_blueprints", "slug_roots"]
      .filter(key => Boolean(row[key]?.length)).length;
}

export function materialityReport(dir = generated) {
  const external = readJsonl(dir, "external_bindings.jsonl");
  const ghosts = readJsonl(dir, "ghost_bindings.jsonl");
  const apps = readJsonl(dir, "app_bindings.jsonl");
  const chains = readJsonl(dir, "chains.jsonl");
  const protocols = readJsonl(dir, "protocols.jsonl");
  const homes = readJsonl(dir, "home_blueprints.jsonl");
  const leaders = readJsonl(dir, "leaders.jsonl");
  const interactionEvidence = readJsonl(dir, "interaction_evidence.jsonl");
  const lattice = readJsonl(dir, "chain_reference_lattice.jsonl");
  const typedPrograms = readJsonl(dir, "typed_application_programs.jsonl");
  const typedNodes = readJsonl(dir, "typed_application_nodes.jsonl");
  const urls = readJsonl(dir, "url_grammars.jsonl");
  const primary = readJson(join(dir, "manifest.json"));
  const stage = readJson(join(dir, "stage_manifest.json"));
  const expectedStage = external.length + ghosts.length + apps.length;
  const actualStage = Number(stage.binding_records || 0);
  const stale = actualStage !== expectedStage;
  const genericProtocol = new Set(auditPolicy.generic_protocol_chains);
  const genericHuman = new Set(auditPolicy.generic_human_chains);
  const protocolGeneric = [], humanGeneric = [], missingProtocol = [], missingHuman = [];
  const missingHome = [], missingTerritory = [], missingRoles = [], weak = [], gaps = [];
  const counts = Object.fromEntries(["human", "protocol", "home", "territory", "slug", "role", "roleClass", "score", "sourceKind"]
    .map(key => [key, counter()]));
  const gap = (kind, row, extra = {}) => gaps.push({ gap: kind, id: row.id ?? null, name: row.name ?? null, ...extra });

  for (const row of external) {
    const protocol = [...new Set(values(row, "protocol_chains"))];
    const human = [...new Set(values(row, "human_chains"))];
    if (!protocol.length) { missingProtocol.push(row); gap("missing_protocol", row); }
    else if (genericOnly(protocol, genericProtocol)) {
      protocolGeneric.push(row);
      increment(counts.sourceKind, row.source_kind ?? "unknown");
      gap("generic_protocol_only", row, { source_kind: row.source_kind ?? null, protocol_chains: protocol.sort() });
    }
    if (!human.length) { missingHuman.push(row); gap("missing_human_chain", row); }
    else if (genericOnly(human, genericHuman)) {
      humanGeneric.push(row); gap("discover_only", row, { source_kind: row.source_kind ?? null });
    }
    if (!row.home_blueprints?.length) { missingHome.push(row); gap("missing_home_blueprint", row); }
    if (!row.territories?.length) { missingTerritory.push(row); gap("missing_territory", row); }
    if (!row.reagent_roles?.length) {
      missingRoles.push(row); gap("missing_reagent_role", row, { source_kind: row.source_kind ?? null });
    }
    const score = materialityScore(row);
    increment(counts.score, score);
    if (score <= 2) { weak.push(row); gap("weak_materiality", row, { score, source_kind: row.source_kind ?? null }); }
    addAll(counts.human, values(row, "human_chains"));
    addAll(counts.protocol, values(row, "protocol_chains"));
    addAll(counts.home, values(row, "home_blueprints"));
    addAll(counts.territory, values(row, "territories"));
    addAll(counts.slug, values(row, "slug_roots"));
    addAll(counts.role, values(row, "reagent_roles"));
    addAll(counts.roleClass, classNames(values(row, "reagent_roles")));
  }

  const chainIds = ids(chains), protocolIds = ids(protocols), homeIds = ids(homes);
  const leaderCounts = counter();
  for (const row of leaders) {
    const raw = values(row, "chain_ids");
    addAll(leaderCounts, typeof raw === "string" ? raw.split(/\s+/).filter(Boolean) : raw);
  }
  const latticeByChain = new Map(lattice.map(row => [row.chain_id, row]));
  const withoutInteraction = [...chainIds].filter(id => !Number(latticeByChain.get(id)?.interaction_reference_count || 0)).sort();
  const withoutReference = [...chainIds].filter(id => !Number(latticeByChain.get(id)?.reference_count || 0)).sort();
  const provenance = counter();
  for (const row of interactionEvidence) increment(provenance, row.provenance ?? "unknown");
  const appKinds = counter();
  for (const row of apps) increment(appKinds, row.record_kind ?? "unknown");
  const selectedNodes = typedNodes.filter(row => row.selection_state === "selected").length;
  const report = {
    schema: policy.schema,
    truth_level: policy.truth_level,
    cannot_infer: policy.cannot_infer,
    primary_build_digest: primary.build_digest ?? null,
    external: {
      total: external.length,
      specific_protocol: external.length - protocolGeneric.length - missingProtocol.length,
      generic_protocol_only: protocolGeneric.length,
      generic_protocol_only_by_source_kind: Object.fromEntries([...counts.sourceKind].sort(([a], [b]) => a.localeCompare(b))),
      missing_protocol: missingProtocol.length,
      specific_human: external.length - humanGeneric.length - missingHuman.length,
      discover_only: humanGeneric.length,
      missing_human: missingHuman.length,
      missing_home: missingHome.length,
      missing_territory: missingTerritory.length,
      missing_reagent_role: missingRoles.length,
      weak_materiality_score_le_2: weak.length,
      materiality_score_distribution: counted(counts.score),
      reagent_role_counts: counted(counts.role),
      reagent_role_class_counts: counted(counts.roleClass),
      human_chain_counts: counted(counts.human),
      protocol_chain_counts: counted(counts.protocol),
      home_blueprint_counts: counted(counts.home),
      territory_counts: counted(counts.territory),
      slug_root_counts: counted(counts.slug),
    },
    reference_lattice: {
      selection_boundary: "none; all rows are evidence-only until a SiteProgram selects a compatible renderer",
      market_leaders: leaders.length,
      human_chains: chainIds.size,
      chains_with_market_reference: leaderCounts.size,
      chains_without_market_reference: difference(chainIds, new Set(leaderCounts.keys())),
      market_reference_chain_counts: counted(leaderCounts),
      interaction_evidence_total: interactionEvidence.length,
      interaction_evidence_by_provenance: counted(provenance),
      chains_with_interaction_reference: chainIds.size - withoutInteraction.length,
      chains_without_interaction_reference: withoutInteraction,
      chains_without_any_reference_evidence: withoutReference,
      ghost_references: ghosts.length,
      ghost_chain_counts: counted(countRows(ghosts, "human_chains")),
      app_reference_records: apps.length,
      app_reference_kind_counts: counted(appKinds),
      app_reference_chain_counts: counted(countRows(apps, "human_chains")),
    },
    typed_application_estate: {
      programs: typedPrograms.length,
      nodes: typedNodes.length,
      programs_without_nodes: difference(chainIds, new Set(typedNodes.map(row => row.experience_id))),
      selected_nodes: selectedNodes,
      note: "typed declarations are not runtime selections; SiteProgram still supplies lawful material and renderer selection",
    },
    occupancy: {
      human_chains_unpopulated_by_external: difference(chainIds, new Set(counts.human.keys())),
      protocol_chains_unpopulated_by_external: difference(protocolIds, new Set(counts.protocol.keys())),
      home_blueprints_unpopulated_by_external: difference(homeIds, new Set(counts.home.keys())),
      url_grammars: urls.length,
    },
    stage_projection: {
      expected_records_from_current_inputs: expectedStage,
      actual_records_in_stage_manifest: actualStage,
      stale,
      delta: expectedStage - actualStage,
      stage_manifest_digest: stage.digest ?? null,
    },
    partner_inventory: primary.partner_inventory ?? {},
    structural_green: !(missingProtocol.length || missingHuman.length || missingHome.length || missingTerritory.length),
    material_green: !(protocolGeneric.length || humanGeneric.length || missingRoles.length || weak.length || withoutInteraction.length || stale),
  };
  return { report, gaps };
}

export function writeMateriality(dir, { report, gaps }) {
  writeFileSync(join(dir, "materiality_manifest.json"), `${prettyCanonical(report)}\n`);
  writeFileSync(join(dir, "materiality_gaps.jsonl"), gaps.map(gap => pythonFlat(gap)).join("\n") + (gaps.length ? "\n" : ""));
}

if (import.meta.main) {
  try {
    const args = process.argv.slice(2), dirIndex = args.indexOf("--generated");
    const dir = dirIndex < 0 ? generated : args[dirIndex + 1];
    if (!dir) throw new Error("--generated requires a directory");
    const result = materialityReport(dir);
    if (args.includes("--write")) writeMateriality(dir, result);
    console.log(prettyCanonical(result.report));
    if (args.includes("--strict") && (!result.report.structural_green || result.report.stage_projection.stale)) process.exitCode = 1;
  } catch (error) { console.error(error.message); process.exitCode = 2; }
}
