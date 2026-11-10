#!/usr/bin/env bun
// Compiled candidate/reference map. It cannot admit sources, select renderers,
// authorize effects, or infer a relationship from shared presentation grammar.
import { createHash } from "node:crypto";
import { existsSync, mkdirSync, readFileSync, readdirSync, statSync, writeFileSync } from "node:fs";
import { dirname, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";
import { canonicalAscii, prettyCanonical, pythonFlat } from "./json_contract.mjs";
import { encodeDelimited, readTsv } from "./tsv_contract.mjs";
import { atlasBindings, sourceClassification } from "./atlas_bindings.mjs";

const here = dirname(fileURLToPath(import.meta.url));
const ATLAS_ORGAN = join(here, "../../BonfyreAtlas");
// The verified Atlas index, checked by the native atlas (cmd/BonfyreAtlas).
function nativeAtlas(argv, root) {
  const bin = process.env.BONFYRE_ATLAS_BIN || join(ATLAS_ORGAN, 'bonfyre-atlas');
  if (!existsSync(bin)) spawnSync('make', ['-s', '-C', ATLAS_ORGAN]);
  const r = spawnSync(bin, argv, { encoding: 'utf8', maxBuffer: 64 << 20, env: { ...process.env, BONFYRE_ATLAS_DIR: join(root, 'architecture') } });
  if (r.status !== 0) throw new Error((r.stderr || r.stdout || 'bonfyre-atlas failed').trim());
  return r.stdout;
}
function loadVerifiedAtlas(root) {
  const index = JSON.parse(readFileSync(join(root, 'architecture', 'atlas.index.json'), 'utf8'));
  const actual = JSON.parse(nativeAtlas(['digest'], root)), recorded = index.provenance ?? {};
  if (actual.declaration_digest !== recorded.declaration_digest || actual.declaration_files !== recorded.declaration_files)
    throw new Error('Atlas index is stale: run bun architecture/atlas.mjs export before using this projection');
  return index;
}
const cmd = resolve(here, "..");
const root = resolve(cmd, "../..");
const data = join(cmd, "data");
const output = join(cmd, "generated");
const externalPath = join(root, "estate/generated/external-estate-v4/observed-external-estate.jsonl");
const externalManifestPath = join(root, "estate/generated/external-estate-v4/manifest.json");
const ghostPath = join(root, "cmd/BonfyreCMS/data/ghost-marketplace-2026-09-06.tsv");
const law = JSON.parse(readFileSync(join(here, "experience-classification.generated.json"), "utf8"));

const words = value => String(value ?? "").split(/\s+/).filter(Boolean);
const uniq = values => [...new Set(values.filter(Boolean))];
const slug = value => value.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "") || "item";
const count = values => {
  const out = {};
  for (const value of values) out[value] = (out[value] ?? 0) + 1;
  return out;
};
const sortedObject = obj => Object.fromEntries(Object.entries(obj).sort(([a], [b]) => a.localeCompare(b)));
const readJsonl = path => readFileSync(path, "utf8").split(/\r?\n/).filter(line => line.trim()).map(line => JSON.parse(line));
const sourceRef = path => relative(root, path);

export function chainSets(rows) {
  const out = new Map();
  for (const original of rows) {
    const stages = original.stages.split("|");
    if (stages.length < 5 || stages.length > 20) throw new Error(`human chain ${original.id} has ${stages.length} stages`);
    out.set(original.id, { ...original, territories: words(original.territories),
      entry_intents: words(original.entry_intents), stages });
  }
  return out;
}

function protocolSets(rows, chains) {
  const out = new Map();
  for (const row of rows) {
    const stages = row.stages.split("|");
    if (stages.length < 5 || stages.length > 20) throw new Error(`protocol ${row.id} has ${stages.length} stages`);
    const human = words(row.human_chains);
    const missing = human.filter(id => !chains.has(id));
    if (missing.length) throw new Error(`protocol ${row.id} references missing chains ${JSON.stringify(missing)}`);
    out.set(row.id, { ...row, match_terms: row.match_terms.split("|"), human_chains: human, stages });
  }
  return out;
}

function homeSets(rows, chains) {
  const out = new Map();
  for (const row of rows) {
    const sections = row.sections.split("|").map(raw => {
      const offset = raw.indexOf(":");
      if (offset < 0) throw new Error(`home ${row.id} has malformed section ${raw}`);
      const chain = raw.slice(0, offset);
      if (!chains.has(chain)) throw new Error(`home ${row.id} references missing chain ${chain}`);
      return { chain, role: raw.slice(offset + 1) };
    });
    if (sections.length < 10) throw new Error(`home ${row.id} is too shallow: ${sections.length} sections`);
    out.set(row.id, { id: row.id, audience: row.audience,
      territory_or_subject: row.territory_or_subject, sections });
  }
  return out;
}

function interactionReferences(chains) {
  const rows = readTsv(join(data, "interaction_references.tsv"));
  const seen = new Set();
  const out = [];
  for (const row of rows) {
    const id = row.id ?? "";
    if (!id || seen.has(id)) throw new Error(`interaction reference has missing/duplicate id ${JSON.stringify(id)}`);
    seen.add(id);
    if (!["donor", "frappe"].includes(row.provenance))
      throw new Error(`interaction reference ${id} has invalid provenance ${JSON.stringify(row.provenance)}`);
    const chainIds = words(row.chain_ids);
    const missing = chainIds.filter(chain => !chains.has(chain));
    if (missing.length || !chainIds.length) throw new Error(`interaction reference ${id} has invalid chain ids ${JSON.stringify(missing)}`);
    const required = ["name", "interaction_grammar", "application_kinds", "affordances", "status", "source_ref"];
    if (required.some(key => !row[key]?.trim())) throw new Error(`interaction reference ${id} is incomplete`);
    if (row.status !== "declared_reference") throw new Error(`interaction reference ${id} cannot claim runtime selection`);
    out.push({ ...row, chain_ids: chainIds, application_kinds: words(row.application_kinds), affordances: words(row.affordances) });
  }
  const actual = count(out.map(row => row.provenance));
  for (const [provenance, expected] of Object.entries(law.expected_interaction_provenance))
    if (actual[provenance] !== expected) throw new Error(`interaction reference estate incomplete: ${provenance}=${actual[provenance]} expected ${expected}`);
  if (Object.keys(actual).length !== Object.keys(law.expected_interaction_provenance).length)
    throw new Error("interaction reference estate has unexpected provenance");
  return out;
}

function typedApplicationPrograms(chains) {
  const rows = readTsv(join(data, "typed_application_programs.tsv"));
  const seen = new Set();
  const out = [];
  const allowed = new Set(Object.keys(JSON.parse(readFileSync(join(here, "stage-law.generated.json"), "utf8")).affordance_stage_hints));
  for (const row of rows) {
    const id = row.experience_id ?? "";
    if (!chains.has(id) || seen.has(id)) throw new Error(`typed application program has invalid/duplicate experience ${JSON.stringify(id)}`);
    seen.add(id);
    const applications = words(row.applications);
    const materialRoles = words(row.material_roles);
    const actions = words(row.action_families);
    const renderers = words(row.renderer_families);
    if (applications.length < 6 || applications.length !== new Set(applications).size)
      throw new Error(`typed application program ${id} needs six distinct application kinds`);
    if (!materialRoles.length || !renderers.length || actions.some(action => !allowed.has(action)))
      throw new Error(`typed application program ${id} has invalid roles/actions/renderers`);
    if (!row.empty_projection?.trim() || !row.refused_projection?.trim())
      throw new Error(`typed application program ${id} has no honest empty/refusal state`);
    out.push({ ...row, applications, material_roles: materialRoles,
      action_families: actions, renderer_families: renderers });
  }
  if (seen.size !== chains.size || [...chains.keys()].some(id => !seen.has(id)))
    throw new Error("typed application program coverage mismatch");
  return out;
}

function typedApplicationNodes(programs) {
  return programs.flatMap(program => program.applications.map(application => ({
    schema: "bonfyre.typed-application-node.v1",
    node_id: `${program.experience_id}:${slug(application)}`,
    experience_id: program.experience_id, application_kind: application,
    consumes: [...program.material_roles], produces: ["SurfaceIR"],
    action_families: [...program.action_families], renderer_requirements: [...program.renderer_families],
    empty_projection: program.empty_projection, refused_projection: program.refused_projection,
    selection_state: "unselected_declaration", edges: [],
    cannot_infer: ["typed application -> admitted material", "typed application -> selected renderer",
      "action family -> authority or effect", "same program -> executable transition"],
  })));
}

const homeRefs = (ids, homes) => {
  const wanted = new Set(ids);
  return [...homes].filter(([, home]) => home.sections.some(section => wanted.has(section.chain))).map(([id]) => id);
};
const territories = (ids, chains) => [...new Set(ids.flatMap(id => chains.get(id).territories))].sort();
const presentation = ids => uniq(ids.flatMap(id => law.presentation[id] ?? [`deep-${id}`]));
const slugRoots = ids => uniq(ids.map(id => law.slug_root[id] ?? `/${id}`));

function nameChains(name, fallback = "software-work") {
  const lower = name.toLowerCase();
  const found = uniq(law.app_hints.filter(hint => lower.includes(hint.term)).flatMap(hint => words(hint.chains)));
  return found.length ? found : [fallback];
}

function externalChains(row, chains) {
  const projection = row.projection ?? {};
  const domains = projection.direct_domains?.length ? projection.direct_domains : projection.topic_domains ?? [];
  const unknown = domains.filter(domain => !(domain in law.domain_to_chains));
  if (unknown.length) throw new Error(`${row.id} has unmapped direct domains ${JSON.stringify(unknown)}`);
  let human = uniq(domains.flatMap(domain => words(law.domain_to_chains[domain])));
  const lower = `${row.observed_name ?? ""} ${row.section ?? ""}`.toLowerCase();
  for (const hint of law.external_name_hints)
    if (hint.terms.some(term => lower.includes(term))) human = uniq([hint.chain, ...human]);
  if (!human.length) human = ["discover"];
  const missing = human.filter(id => !chains.has(id));
  if (missing.length) throw new Error(`${row.id} resolved unknown chains ${JSON.stringify(missing)}`);
  return human;
}

function protocolRefs(row, protocols) {
  const sourceKind = row.source_kind ?? "";
  const text = ["observed_name", "primary_url", "raw_source_text", "section"].map(key => String(row[key] ?? "")).join(" ").toLowerCase();
  const refs = [];
  if (law.source_kind_protocol[sourceKind]) refs.push(law.source_kind_protocol[sourceKind]);
  for (const [id, spec] of protocols) {
    if (id === "machine-generic" || id === "http-api") continue;
    if (spec.match_terms.some(term => text.includes(term.toLowerCase()))) refs.push(id);
  }
  if (sourceKind === "machine_protocol" && !refs.length) refs.push("machine-generic");
  if (!(sourceKind in law.source_kind_support)) throw new Error(`${row.id} has unknown source kind ${JSON.stringify(sourceKind)}`);
  return uniq(refs);
}

function bindExternal(rows, chains, protocols, homes) {
  return rows.map(row => {
    const human = externalChains(row, chains);
    const projection = row.projection ?? {};
    return {
      record_kind: "external", id: row.id, name: row.observed_name ?? "", source_kind: row.source_kind,
      url: row.primary_url ?? "", truth_level: row.truth_level ?? "observed_candidate",
      human_chains: human, support_chains: law.source_kind_support[row.source_kind],
      protocol_chains: protocolRefs(row, protocols), territories: territories(human, chains),
      home_blueprints: homeRefs(human, homes), presentation_modes: presentation(human), slug_roots: slugRoots(human),
      reagent_roles: projection.estate_roles ?? [], direct_domains: projection.direct_domains ?? [],
      candidate_worlds: projection.candidate_worlds ?? [],
      cannot_infer: ["binding -> source admission", "home eligibility -> publication", "protocol fit -> authority or effect"],
    };
  });
}

function bindGhost(chains, homes) {
  return readTsv(ghostPath).map(row => {
    const categories = row.categories.split(/[,;/]/).map(value => value.trim()).filter(Boolean);
    const unknown = categories.filter(category => !(category in law.ghost_category));
    if (unknown.length) throw new Error(`Ghost ${row.name} has unknown categories ${JSON.stringify(unknown)}`);
    const human = uniq(categories.flatMap(category => words(law.ghost_category[category].chains)));
    const families = uniq(categories.map(category => law.ghost_category[category].family));
    const mixes = uniq(categories.map(category => law.ghost_category[category].mix));
    return {
      record_kind: "ghost", id: `ghost:${slug(row.name)}`, name: row.name, categories,
      presentation_families: families, renderer_mixes: mixes, human_chains: human, protocol_chains: [],
      territories: territories(human, chains), home_blueprints: homeRefs(human, homes),
      presentation_modes: families.map(family => `ghost:${family}`), slug_roots: slugRoots(human),
      source_ref: sourceRef(ghostPath),
      cannot_infer: ["reference theme -> copied Ghost code", "presentation fit -> semantic ownership"],
    };
  });
}

function appRecord(kind, name, chainIds, chains, homes, source, { referenceOnly = false, interactionEvidence } = {}) {
  const human = uniq(chainIds);
  const result = {
    record_kind: kind, id: `${kind}:${slug(name)}`, name, human_chains: human, protocol_chains: [],
    territories: territories(human, chains), home_blueprints: homeRefs(human, homes),
    presentation_modes: presentation(human), slug_roots: slugRoots(human), source_ref: source,
    cannot_infer: ["available app/tool -> semantic owner for every matching chain", "reference -> authority"],
  };
  if (referenceOnly) {
    result.reference_only = true;
    result.cannot_infer.push("interaction reference -> selected renderer or executable stage");
  }
  if (interactionEvidence) result.interaction_evidence = interactionEvidence;
  return result;
}

function sortedPaths(dir, filter = () => true) {
  return readdirSync(dir).sort().map(name => join(dir, name)).filter(filter);
}

function bindApps(chains, homes, leaders, protocols, interactions, atlas) {
  const out = [];
  const bindings = atlasBindings(atlas, chains, source => existsSync(join(root, source)));
  for (const path of sortedPaths(join(root, "cmd"), path => statSync(path).isDirectory())) {
    const name = path.slice(path.lastIndexOf("/") + 1);
    out.push(appRecord("command", name, nameChains(name), chains, homes, sourceRef(path)));
  }
  for (const path of sortedPaths(join(root, "components"), path => statSync(path).isDirectory() && !path.split("/").at(-1).startsWith("."))) {
    const name = path.slice(path.lastIndexOf("/") + 1);
    out.push(appRecord("component", name, nameChains(name), chains, homes, sourceRef(path)));
  }
  for (const [dir, kind, fallback] of [
    ["cmd/BonfyreCMS/content-types", "cms_type", "knowledge"],
    ["site/matrix/surfaces", "surface", "discover"],
  ]) for (const path of sortedPaths(join(root, dir), path => path.endsWith(".json") && statSync(path).isFile())) {
    const name = path.split("/").at(-1).slice(0, -5);
    const classification = sourceClassification(bindings, sourceRef(path), nameChains(name, fallback));
    out.push({...appRecord(kind, name, classification.human_chains, chains, homes, sourceRef(path)), classification});
  }
  for (const {id, record} of bindings.architectures) out.push({
    ...appRecord('architecture', id, record.experience_chains, chains, homes, `architecture:${id}`),
    canonical_name: record.canonical_name, maturity: record.maturity,
    source_paths: record.source_paths, witnesses: record.witnesses, known_gaps: record.known_gaps,
    interaction_refs: Object.entries(atlas.interactions ?? {})
      .filter(([, interaction]) => interaction.source === id || interaction.destination === id)
      .map(([interactionId, interaction]) => ({id: interactionId, ...interaction})),
    classification: {basis: 'atlas-declaration', architecture_ids: [id]},
    cannot_infer: [...record.forbidden_inferences, 'workflow reference -> authorized executable chain'],
  });
  for (const row of interactions) {
    const kind = row.provenance === "frappe" ? "frappe_app" : "donor_reference";
    out.push(appRecord(kind, row.name, row.chain_ids, chains, homes, row.source_ref, {
      referenceOnly: true, interactionEvidence: {
        reference_id: row.id, provenance: row.provenance, grammar: row.interaction_grammar,
        application_kinds: row.application_kinds, affordances: row.affordances, status: row.status,
      },
    }));
  }
  for (const name of law.observed_partner_names)
    out.push(appRecord("partner_observed", name, nameChains(name), chains, homes, "packs/aurekai/session-reuse-census.yaff"));
  for (const row of leaders)
    out.push(appRecord("market_reference", row.id, words(row.chain_ids), chains, homes, row.source_url, {
      referenceOnly: true, interactionEvidence: {
        reference_id: row.id, provenance: "market", grammar: row.persistent_mechanics,
        application_kinds: [], affordances: [], status: "declared_reference",
      },
    }));
  for (const [id, protocol] of protocols)
    out.push(appRecord("protocol_reference", id, protocol.human_chains, chains, homes, "data/protocol_chains.tsv"));
  return out;
}

function referenceLattice(chains, interactions, leaders, ghosts) {
  const evidence = [];
  for (const row of interactions) evidence.push({
    record_kind: "interaction_reference", id: `${row.provenance}:${row.id}`, name: row.name,
    provenance: row.provenance, human_chains: row.chain_ids, interaction_grammar: row.interaction_grammar,
    application_kinds: row.application_kinds, affordances: row.affordances,
    selection_state: "reference_only", source_ref: row.source_ref,
    cannot_infer: ["reference -> selected renderer", "reference -> executable application", "reference -> authority"],
  });
  for (const row of leaders) evidence.push({
    record_kind: "market_interaction_reference", id: `market:${row.id}`, name: row.id,
    provenance: "market", human_chains: words(row.chain_ids), interaction_grammar: row.persistent_mechanics,
    application_kinds: [], affordances: [], selection_state: "reference_only", source_ref: row.source_url,
    cannot_infer: ["market reference -> copied product", "market reference -> selected renderer", "reference -> authority"],
  });
  for (const row of ghosts) evidence.push({
    record_kind: "ghost_art_direction_candidate", id: row.id, name: row.name,
    provenance: "ghost", human_chains: row.human_chains, interaction_grammar: "art-direction-candidate",
    application_kinds: [], affordances: [], selection_state: "unobserved_candidate", source_ref: row.source_ref,
    cannot_infer: ["unique generated vector -> observed structural profile", "art direction -> interaction grammar", "candidate -> selected profile"],
  });
  const byChain = new Map([...chains.keys()].map(id => [id, []]));
  for (const row of evidence) for (const id of row.human_chains) byChain.get(id).push(row);
  const lattice = [...chains.keys()].map(id => {
    const rows = byChain.get(id);
    const provenance = count(rows.map(row => row.provenance));
    return {
      chain_id: id, evidence_ids: rows.map(row => row.id), reference_count: rows.length,
      provenance_counts: sortedObject(provenance),
      interaction_reference_count: rows.filter(row => ["donor", "frappe", "market"].includes(row.provenance)).length,
      ghost_art_direction_candidate_count: provenance.ghost ?? 0, selection_state: "evidence_only",
      cannot_infer: ["reference coverage -> renderer selection", "reference coverage -> runtime material", "reference coverage -> executable edge"],
    };
  });
  return [evidence, lattice];
}

function canonicalDigest(groups) {
  const hash = createHash("sha256");
  for (const [name, rows] of groups) {
    hash.update(name).update("\n");
    for (const row of rows) hash.update(canonicalAscii(row)).update("\n");
  }
  return hash.digest("hex");
}

export async function build({ write = false, out = output } = {}) {
  const atlas = loadVerifiedAtlas(root);
  const chains = chainSets(readTsv(join(data, "experience_chains.tsv")));
  const protocols = protocolSets(readTsv(join(data, "protocol_chains.tsv")), chains);
  const homes = homeSets(readTsv(join(data, "home_blueprints.tsv")), chains);
  const interactions = interactionReferences(chains);
  const programs = typedApplicationPrograms(chains);
  const nodes = typedApplicationNodes(programs);
  const leaders = readTsv(join(data, "market_leaders.tsv"));
  for (const row of leaders) {
    const missing = words(row.chain_ids).filter(id => !chains.has(id));
    if (missing.length) throw new Error(`leader ${row.id} references missing chains ${JSON.stringify(missing)}`);
  }
  const urls = readTsv(join(data, "url_grammars.tsv"));
  const externalRows = readJsonl(externalPath);
  const externalManifest = JSON.parse(readFileSync(externalManifestPath, "utf8"));
  const expected = externalManifest.counts.external_occurrences;
  if (externalRows.length !== expected) throw new Error(`ExternalEstate manifest says ${expected}, read ${externalRows.length}`);
  const external = bindExternal(externalRows, chains, protocols, homes);
  const ghost = bindGhost(chains, homes);
  const apps = bindApps(chains, homes, leaders, protocols, interactions, atlas);
  const [interactionEvidence, chainLattice] = referenceLattice(chains, interactions, leaders, ghost);
  if (ghost.length !== law.expected_ghost_references) throw new Error(`expected ${law.expected_ghost_references} Ghost references, got ${ghost.length}`);
  if ([...external, ...ghost, ...apps].some(row => !row.human_chains.length)) throw new Error("unmapped record exists");
  const chainRows = [...chains.values()], protocolRows = [...protocols.values()], homeRows = [...homes.values()];
  const ownerGraph = {
    schema: 'bonfyre.experience-owner-graph.v1',
    atlas_declaration_digest: atlas.provenance.declaration_digest,
    owners: Object.entries(atlas.architectures).map(([id, record]) => ({id, ...record})),
    interactions: Object.entries(atlas.interactions).map(([id, record]) => ({id, ...record})),
    workflows: chainRows,
  };
  const groups = [
    ["chains", chainRows], ["typed-application-programs", programs], ["typed-application-nodes", nodes],
    ["protocols", protocolRows], ["interaction-references", interactions], ["leaders", leaders],
    ["homes", homeRows], ["urls", urls], ["external", external], ["ghost", ghost], ["apps", apps],
    ["interaction-evidence", interactionEvidence], ["chain-lattice", chainLattice],
    ["owner-graph", [ownerGraph]],
  ];
  const kindCounts = count([...external, ...ghost, ...apps].map(row => row.record_kind));
  const sourceKindCounts = count(external.map(row => row.source_kind));
  const protocolBound = external.filter(row => row.protocol_chains.length).length;
  const manifest = {
    schema: "bonfyre.experience-map.v1", truth_level: "compiled_projection",
    build_digest: canonicalDigest(groups), human_chain_count: chainRows.length,
    atlas_declaration_digest: atlas.provenance.declaration_digest,
    owner_graph: {owners: ownerGraph.owners.length, interactions: ownerGraph.interactions.length},
    owner_graph_digest: canonicalDigest([['owner-graph', [ownerGraph]]]),
    protocol_chain_count: protocolRows.length, market_leader_count: leaders.length,
    home_blueprint_count: homeRows.length, url_grammar_count: urls.length,
    external_occurrences: external.length, external_mapped: external.length,
    ghost_references: ghost.length, ghost_mapped: ghost.length, app_reference_records: apps.length,
    record_kind_counts: sortedObject(kindCounts), source_kind_counts: sortedObject(sourceKindCounts),
    external_with_protocol_chain: protocolBound,
    external_manifest_digest: externalManifest.input.master_sha256 ?? "",
    interaction_reference_lattice: {
      total_evidence: interactionEvidence.length, market_references: leaders.length,
      donor_interaction_references: interactions.filter(row => row.provenance === "donor").length,
      frappe_interaction_references: interactions.filter(row => row.provenance === "frappe").length,
      ghost_art_direction_candidates: ghost.length,
      chains_with_interaction_reference: chainLattice.filter(row => row.interaction_reference_count > 0).length,
      chains_with_any_reference_evidence: chainLattice.filter(row => row.reference_count > 0).length,
      market_reference_is_not_selection_boundary: true, all_references_are_evidence_only: true,
    },
    typed_application_estate: {
      experience_programs: programs.length, application_nodes: nodes.length,
      minimum_nodes_per_program: Math.min(...programs.map(row => row.applications.length)),
      all_nodes_unselected_until_site_program: true, prose_stage_input_used: false,
    },
    coverage: {
      external: true, ghost: true, local_commands_components: true, cms_types: true,
      surface_descriptors: true, frappe_apps: true, donor_references: true,
      frappe_interaction_grammars: true, reference_lattice: true,
      protocol_occurrences: protocolBound === external.length, partner_shipped_repo_names: false,
    },
    partner_inventory: {
      declared_shipped_repos: law.declared_shipped_partner_repos,
      names_provable_in_checkout: law.observed_partner_names.length,
      note: "session-reuse-census proves 41 shipped repos but does not enumerate all 41 names inside this project root",
    },
    cannot_infer: ["experience binding -> provider admission", "home blueprint -> current publication",
      "reference market leader -> copied ontology or code", "chain fit -> authority to execute"],
  };
  if (write) {
    mkdirSync(out, { recursive: true });
    for (const [name, rows] of [
      ["chains.jsonl", chainRows], ["protocols.jsonl", protocolRows], ["leaders.jsonl", leaders],
      ["home_blueprints.jsonl", homeRows], ["url_grammars.jsonl", urls],
      ["external_bindings.jsonl", external], ["ghost_bindings.jsonl", ghost],
      ["app_bindings.jsonl", apps], ["typed_application_programs.jsonl", programs],
      ["typed_application_nodes.jsonl", nodes], ["interaction_evidence.jsonl", interactionEvidence],
      ["chain_reference_lattice.jsonl", chainLattice],
    ]) writeFileSync(join(out, name), rows.map(row => pythonFlat(row)).join("\n") + (rows.length ? "\n" : ""));
    const tsvRows = [["record_kind", "id", "name", "human_chains", "protocol_chains", "territories", "home_blueprints", "source"]];
    for (const row of [...external, ...ghost, ...apps]) tsvRows.push([
      row.record_kind, row.id, row.name, row.human_chains.join(" "),
      row.protocol_chains.join(" "), row.territories.join(" "), row.home_blueprints.join(" "),
      row.url || row.source_ref || "",
    ]);
    writeFileSync(join(out, "all_bindings.tsv"), encodeDelimited(tsvRows));
    writeFileSync(join(out, "manifest.json"), `${prettyCanonical(manifest)}\n`);
    writeFileSync(join(out, "owner_graph.json"), `${prettyCanonical(ownerGraph)}\n`);
  }
  return manifest;
}

export async function main(args = process.argv.slice(2)) {
  const write = args.includes("--write");
  const check = args.includes("--check");
  const outputIndex = args.indexOf("--output");
  const out = outputIndex >= 0 ? resolve(args[outputIndex + 1]) : output;
  if (args.some(arg => arg.startsWith("-") && !["--write", "--check", "--output"].includes(arg)) || !out)
    throw new Error("usage: compile_experience_map.mjs [--write] [--check] [--output DIR]");
  const manifest = await build({ write, out });
  if (check) {
    const path = join(out, "manifest.json");
    if (!existsSync(path)) throw new Error("generated manifest is absent; run --write");
    const retained = JSON.parse(readFileSync(path, "utf8"));
    if (retained.build_digest !== manifest.build_digest || retained.atlas_declaration_digest !== manifest.atlas_declaration_digest)
      throw new Error("generated experience map is stale; run --write");
    const graph = JSON.parse(readFileSync(join(out, 'owner_graph.json'), 'utf8'));
    if (canonicalDigest([['owner-graph', [graph]]]) !== manifest.owner_graph_digest)
      throw new Error('generated owner graph is stale or damaged; run --write');
  }
  process.stdout.write(`${canonicalAscii(manifest)}\n`);
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) await main();
