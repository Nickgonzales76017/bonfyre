#!/usr/bin/env bun
// Exact stage projection over current candidate/reference records.
import { createHash } from "node:crypto";
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import law from "./stage-law.generated.json" with { type: "json" };
import { canonicalAscii, prettyCanonical, pythonFlat } from "./json_contract.mjs";

const here = dirname(fileURLToPath(import.meta.url));
const root = join(here, "..", "..", "..");
const generated = join(here, "..", "generated");
const externalPath = join(root, "estate", "generated", "external-estate-v4", "observed-external-estate.jsonl");
const chainPath = join(here, "..", "data", "experience_chains.tsv");
const readJsonl = path => readFileSync(path, "utf8").split(/\r?\n/)
  .filter(line => line.trim()).map(line => JSON.parse(line));
const words = value => value.split(/\s+/).filter(Boolean);
const uniq = values => [...new Set(values.filter(Boolean))];

function chainSets(path) {
  const [header, ...lines] = readFileSync(path, "utf8").trimEnd().split(/\r?\n/);
  const names = header.split("\t"), chains = new Map();
  for (const line of lines) {
    const cells = line.split("\t");
    if (cells.length !== names.length) throw new Error(`invalid experience chain TSV row: ${line}`);
    const row = Object.fromEntries(names.map((name, index) => [name, cells[index]]));
    const stages = row.stages.split("|");
    if (stages.length < 5 || stages.length > 20) throw new Error(`human chain ${row.id} has ${stages.length} stages`);
    chains.set(row.id, { ...row, territories: words(row.territories), entry_intents: words(row.entry_intents), stages });
  }
  return chains;
}

export function stageBindings(chainIds, chains, hints) {
  const expanded = [];
  for (const hint of hints) {
    expanded.push(...words(law.affordance_stage_hints[hint] ?? ""));
    expanded.push(...words(law.role_stage_hints[hint] ?? ""));
    expanded.push(hint.toLowerCase());
  }
  const terms = new Set(expanded), substrings = [...terms].filter(term => term.length >= 4), out = {};
  for (const id of chainIds) {
    const stages = chains.get(id)?.stages;
    if (!stages) throw new Error(`unknown human chain ${id}`);
    const chosen = stages.filter(stage => {
      const lower = stage.toLowerCase(), parts = lower.split(/[^a-z0-9]+/).filter(Boolean);
      return parts.some(part => terms.has(part)) || substrings.some(term => lower.includes(term));
    });
    if (chosen.length) out[id] = chosen.slice(0, law.max_stage_bindings_per_chain);
  }
  return out;
}

export function presentationStageBindings(chainIds, chains) {
  const terms = new Set(words(law.presentation_terms)), out = {};
  for (const id of chainIds) {
    const stages = chains.get(id)?.stages;
    if (!stages) throw new Error(`unknown human chain ${id}`);
    const chosen = stages.filter(stage => [...terms].some(term => stage.toLowerCase().includes(term)));
    if (chosen.length) out[id] = chosen.slice(0, law.max_stage_bindings_per_chain);
  }
  return out;
}

export function appStageHints(name) {
  const lower = name.toLowerCase(), hints = [];
  for (const [affordance, terms] of Object.entries(law.affordance_stage_hints))
    if (words(terms).some(term => lower.includes(term))) hints.push(affordance);
  for (const override of law.app_name_overrides)
    if (override.terms.some(term => lower.includes(term))) hints.push(...override.affordances);
  return uniq(hints.length ? hints : [law.app_fallback_affordance]);
}

function validBindings(record, chains) {
  const allowed = new Set(record.human_chains);
  for (const [id, stages] of Object.entries(record.chain_bindings)) {
    if (!allowed.has(id)) throw new Error(`${record.id} has a stage edge outside its candidate experience references`);
    const legal = new Set(chains.get(id)?.stages ?? []);
    if (!stages.length || stages.some(stage => !legal.has(stage)))
      throw new Error(`${record.id} has illegal/empty stages for ${id}: ${JSON.stringify(stages)}`);
  }
  return record;
}

export function buildStageRecords(dir = generated, source = externalPath, definitions = chainPath) {
  const chains = chainSets(definitions);
  const raw = new Map(readJsonl(source).map(row => [row.id, row]));
  const external = readJsonl(join(dir, "external_bindings.jsonl"));
  const ghosts = readJsonl(join(dir, "ghost_bindings.jsonl"));
  const apps = readJsonl(join(dir, "app_bindings.jsonl"));
  const records = [];
  for (const row of external) {
    const src = raw.get(row.id);
    if (!src) throw new Error(`missing observed external occurrence ${row.id}`);
    const projection = src.projection ?? {};
    const hints = uniq([...(projection.candidate_affordances ?? []), ...(projection.estate_roles ?? [])]);
    records.push(validBindings({ record_kind: row.record_kind, id: row.id, name: row.name,
      human_chains: row.human_chains, chain_bindings: stageBindings(row.human_chains, chains, hints),
      basis: { candidate_affordances: projection.candidate_affordances ?? [],
        reagent_roles: projection.estate_roles ?? [], direct_domains: projection.direct_domains ?? [],
        truth_level: src.truth_level ?? "observed_candidate" },
      cannot_infer: ["stage fit -> source admission", "stage fit -> authority", "stage fit -> effect"] }, chains));
  }
  for (const row of ghosts) records.push(validBindings({ record_kind: row.record_kind,
    id: row.id, name: row.name, human_chains: row.human_chains,
    chain_bindings: presentationStageBindings(row.human_chains, chains),
    basis: { categories: row.categories, presentation_families: row.presentation_families,
      renderer_mixes: row.renderer_mixes },
    cannot_infer: ["presentation stage fit -> semantic ownership", "reference -> copied theme code"] }, chains));
  for (const row of apps) {
    const referenceOnly = Boolean(row.reference_only);
    records.push(validBindings({ record_kind: row.record_kind, id: row.id, name: row.name,
      human_chains: row.human_chains,
      chain_bindings: referenceOnly ? {} : stageBindings(row.human_chains, chains, appStageHints(row.name)),
      basis: { source_ref: row.source_ref ?? "", mapping: referenceOnly
        ? "declared interaction evidence" : "name+declared estate role", reference_only: referenceOnly },
      cannot_infer: ["tool stage fit -> semantic ownership", "tool stage fit -> authority",
        referenceOnly ? "interaction reference -> executable stage" : ""] }, chains));
  }
  return { records, chains, expected: external.length + ghosts.length + apps.length };
}

export function stageManifest({ records, chains, expected }) {
  const counts = new Map(), hash = createHash("sha256");
  for (const row of records) {
    counts.set(row.record_kind, (counts.get(row.record_kind) ?? 0) + 1);
    hash.update(canonicalAscii(row));
    hash.update("\n");
  }
  if (records.length !== expected) throw new Error(`expected ${expected} records, got ${records.length}`);
  return { schema: "bonfyre.experience-stage-map.v1", binding_records: records.length,
    records_with_stage_bindings: records.filter(row => Object.keys(row.chain_bindings).length).length,
    chain_stage_edges: records.reduce((total, row) => total + Object.values(row.chain_bindings)
      .reduce((sum, stages) => sum + stages.length, 0), 0),
    unbound_experience_references: records.reduce((total, row) => total +
      new Set(row.human_chains.filter(id => !Object.hasOwn(row.chain_bindings, id))).size, 0),
    human_chain_count: chains.size, record_kind_counts: Object.fromEntries([...counts].sort(([a], [b]) => a.localeCompare(b))),
    digest: hash.digest("hex"), expected_binding_records: expected, coverage: true };
}

export function compileStageBindings({ dir = generated, source = externalPath, definitions = chainPath, write = false, check = false } = {}) {
  const built = buildStageRecords(dir, source, definitions), manifest = stageManifest(built);
  if (write) {
    writeFileSync(join(dir, "stage_bindings.jsonl"), built.records.map(pythonFlat).join("\n") + "\n");
    writeFileSync(join(dir, "stage_manifest.json"), `${prettyCanonical(manifest)}\n`);
  }
  if (check) {
    const path = join(dir, "stage_manifest.json");
    if (!existsSync(path) || JSON.parse(readFileSync(path, "utf8")).digest !== manifest.digest)
      throw new Error("generated stage map is absent or stale");
  }
  return { manifest, records: built.records };
}

if (import.meta.main) {
  try {
    const args = process.argv.slice(2), idx = args.indexOf("--generated");
    const dir = idx < 0 ? generated : args[idx + 1];
    if (!dir) throw new Error("--generated requires a directory");
    const { manifest } = compileStageBindings({ dir, write: args.includes("--write"), check: args.includes("--check") });
    console.log(pythonFlat(manifest));
  } catch (error) { console.error(error.message); process.exitCode = 2; }
}
