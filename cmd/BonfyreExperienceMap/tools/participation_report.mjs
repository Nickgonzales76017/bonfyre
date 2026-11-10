#!/usr/bin/env bun
// Candidate participation and occurrence-keyed realization are separate axes.
import { existsSync, readFileSync, readdirSync, statSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { prettyCanonical, pythonFlat } from "./json_contract.mjs";

const here = dirname(fileURLToPath(import.meta.url));
const defaultGenerated = join(here, "../generated");
export const law = JSON.parse(readFileSync(join(here, "participation-law.generated.json"), "utf8"));
const axes = law.axes;
const intersects = (values, candidates) => candidates.some(value => values.has(value));
const difference = (values, excluded) => new Set(values.filter(value => !excluded.includes(value)));
const pairKey = (source, target) => JSON.stringify([source, target]);
const sortedPairs = pairs => [...pairs].map(pair => JSON.parse(pair))
  .sort(([a, b], [c, d]) => a.localeCompare(c) || b.localeCompare(d));
const count = values => {
  const result = {};
  for (const value of values) result[value] = (result[value] ?? 0) + 1;
  return result;
};

function files(path) {
  if (!existsSync(path)) return [];
  if (!statSync(path).isDirectory()) return [path];
  const out = [];
  const visit = current => {
    for (const name of readdirSync(current).sort()) {
      const entry = join(current, name);
      if (statSync(entry).isDirectory()) visit(entry);
      else if (/\.json/.test(name)) out.push(entry);
    }
  };
  visit(path);
  return out.sort();
}

export function loadJsonl(path) {
  const rows = [];
  for (const file of files(path)) {
    for (const line of readFileSync(file, "utf8").split(/\r?\n/)) {
      if (!line.trim()) continue;
      const row = JSON.parse(line);
      if (row && typeof row === "object" && !Array.isArray(row)) {
        if (row._source_path === undefined) row._source_path = file;
        rows.push(row);
      }
    }
  }
  return rows;
}

export function candidateProfile(row) {
  const roles = new Set(row.reagent_roles ?? []);
  const roleClasses = Object.entries(law.role_classes)
    .filter(([, members]) => intersects(roles, members)).map(([name]) => name).sort();
  const human = difference(row.human_chains ?? [], law.generic_human);
  const protocols = difference(row.protocol_chains ?? [], law.generic_protocol);
  const direct = new Set(row.direct_domains ?? []);
  const support = new Set(row.support_chains ?? []);
  const chains = new Set([...human, ...support]);
  const economicTerms = new Set([...direct, ...chains, ...protocols]);
  const families = [], covered = new Set();
  for (const [name, members] of Object.entries(law.join_families)) {
    if (intersects(chains, members)) {
      families.push(name);
      for (const chain of chains) if (members.includes(chain)) covered.add(chain);
    }
  }
  const residual = [...chains].filter(chain => !covered.has(chain)).sort();
  if (residual.length) families.push("other");
  families.sort();
  const planes = {
    discovery: true,
    observation: Boolean(row.url),
    semantic: Boolean(direct.size),
    protocol: Boolean(protocols.size),
    capability: intersects(roles, law.capability_roles),
    economic: intersects(economicTerms, law.economic_terms),
    round_trip: intersects(roles, law.round_trip_roles)
      || intersects(human, law.round_trip_human) || intersects(protocols, law.round_trip_protocol),
  };
  return {
    candidate_planes: planes, candidate_plane_count: Object.values(planes).filter(Boolean).length,
    chain_span: chains.size, join_families: families, join_power: families.length,
    residual_chains: residual, role_classes: roleClasses, protocol_carriers: [...protocols].sort(),
    direct_domains: [...direct].sort(), support_chains: [...support].sort(), human_chains: [...human].sort(),
  };
}

const externalId = value => typeof value === "string" && value.startsWith("external:");
const digest = value => typeof value === "string" && /^[0-9a-f]{64}$/.test(value);

export function normalizeFact(raw) {
  if (raw.schema === "bonfyre-domain-binding.v1") {
    if (raw.binding_validated !== true || raw.owner !== "DomainProfile" || raw.state !== "profile_bound") return null;
    return { occurrence_id: raw.occurrence_id, axis: "semantic", owner: "DomainProfile", state: "profile_bound",
      receipt: raw, from_owner: "ForeignTwin", to_owner: "DomainProfile" };
  }
  if (raw.schema === "bonfyre-protocol-binding.v1") {
    if (raw.binding_validated !== true || raw.owner !== "ProtocolContract" || raw.state !== "compiled"
      || raw.from_owner !== "DomainProfile" || raw.to_owner !== "ProtocolContract") return null;
    if (["semantic_record_sha256", "domain_binding_sha256", "native_registration_receipt_sha256"]
      .some(field => typeof raw[field] !== "string" || raw[field].length !== 64)) return null;
    if (typeof raw.contract !== "string" || !raw.contract || !Number.isInteger(raw.programs) || raw.programs < 1) return null;
    return { occurrence_id: raw.occurrence_id, axis: "protocol", owner: "ProtocolContract", state: "compiled",
      receipt: raw, from_owner: "DomainProfile", to_owner: "ProtocolContract" };
  }
  if (raw.schema === "bonfyre-foreign-observation.v1") {
    if (!externalId(raw.twin_id)) return null;
    let state = raw.observation_kind || raw.policy_decision || "receipted";
    if (state === "source_retained") state = "source_observed";
    if (state === "source_observed" && raw.body_path) {
      try { if (statSync(raw.body_path).isFile() && statSync(raw.body_path).size > 0) state = "source_retained"; }
      catch { /* missing bytes cannot earn retention */ }
    }
    return { occurrence_id: raw.twin_id, axis: "observation", owner: "ForeignTwin", state,
      receipt: raw._source_path || raw.content_sha256, from_owner: "ExternalEstate", to_owner: "ForeignTwin" };
  }
  if (raw.schema === "bonfyre-protocol-enactment.v1" || raw.schema === "bonfyre-owner-participation.v1") {
    const { axis, state } = raw;
    if (!law.axis_ladders[axis]?.[state] || !externalId(raw.occurrence_id)) return null;
    if (!["basis_sha256", "evidence_sha256"].every(field => digest(raw[field]))) return null;
    if (raw.schema === "bonfyre-protocol-enactment.v1") {
      if (typeof raw.contract !== "string" || !raw.contract) return null;
      return { occurrence_id: raw.occurrence_id, axis, owner: raw.owner || "ProtocolContract", state,
        receipt: raw.evidence_sha256, from_owner: raw.from_owner, to_owner: raw.to_owner };
    }
    if (!["owner", "from_owner", "to_owner", "produced_by"]
      .every(field => typeof raw[field] === "string" && raw[field])) return null;
    return { occurrence_id: raw.occurrence_id, axis, owner: raw.owner, state,
      receipt: raw.evidence_sha256, from_owner: raw.from_owner, to_owner: raw.to_owner };
  }
  return null;
}

export function evidenceIndex(rows) {
  const byOccurrence = new Map(), transitions = new Map();
  for (const raw of rows) {
    const fact = normalizeFact(raw);
    if (!fact || !fact.occurrence_id || !axes.includes(fact.axis) || !fact.owner || !fact.state) continue;
    if (!byOccurrence.has(fact.occurrence_id)) byOccurrence.set(fact.occurrence_id, []);
    byOccurrence.get(fact.occurrence_id).push(fact);
    if (fact.from_owner && fact.to_owner && fact.from_owner !== fact.to_owner) {
      if (!transitions.has(fact.occurrence_id)) transitions.set(fact.occurrence_id, new Set());
      transitions.get(fact.occurrence_id).add(pairKey(fact.from_owner, fact.to_owner));
    }
  }
  return [byOccurrence, transitions];
}

function stateDepth(axis, facts) {
  if (!facts.length) return [0, "unknown"];
  const ladder = law.axis_ladders[axis] ?? {};
  const recognized = facts.map(fact => ladder[fact.state]).filter(Boolean);
  if (axis === "observation" && facts.filter(fact => ["source_observed", "source_retained"].includes(fact.state)).length >= 2)
    recognized.push(ladder.repeat_observed);
  if (!recognized.length) return [1, "owner-fact"];
  const best = recognized.reduce((a, b) => a.depth >= b.depth ? a : b);
  return [best.depth, best.name];
}

function longestOwnerPath(transitions) {
  const graph = new Map();
  for (const [source, target] of sortedPairs(transitions)) {
    if (!graph.has(source)) graph.set(source, new Set());
    graph.get(source).add(target);
  }
  let best = ["ExternalEstate"];
  const walk = (node, path) => {
    if (path.length > best.length) best = path;
    for (const target of [...(graph.get(node) ?? [])].sort())
      if (!path.includes(target)) walk(target, [...path, target]);
  };
  walk("ExternalEstate", ["ExternalEstate"]);
  return [best.length - 1, best];
}

function braidShape(transitions) {
  const outgoing = new Map(), incoming = new Map();
  for (const [source, target] of sortedPairs(transitions)) {
    if (!outgoing.has(source)) outgoing.set(source, new Set());
    if (!incoming.has(target)) incoming.set(target, new Set());
    outgoing.get(source).add(target);
    incoming.get(target).add(source);
  }
  const owners = new Set([...outgoing.keys(), ...incoming.keys()]);
  return {
    distinct_owners: owners.size, owner_transitions: transitions.size,
    longest_owner_path: longestOwnerPath(transitions)[0],
    branch_points: [...outgoing].filter(([, targets]) => targets.size > 1).map(([owner]) => owner).sort(),
    rejoin_points: [...incoming].filter(([, sources]) => sources.size > 1).map(([owner]) => owner).sort(),
    braid_excess: Math.max(0, transitions.size - Math.max(0, owners.size - 1)),
    truth: "diagnostic_projection_over_supplied_evidence_not_a_claim_any_owner_was_right",
  };
}

function realizedProfile(id, facts, transitions) {
  const byAxis = new Map();
  for (const fact of facts) {
    if (!byAxis.has(fact.axis)) byAxis.set(fact.axis, []);
    byAxis.get(fact.axis).push(fact);
  }
  const resultAxes = {
    discovery: { status: "measured", state: "indexed", depth: 1, depth_name: "indexed",
      owner: "ExternalEstate", evidence: id },
  };
  const owners = new Set(["ExternalEstate"]);
  for (const axis of axes.slice(1)) {
    const measured = byAxis.get(axis) ?? [];
    if (!measured.length) {
      resultAxes[axis] = { status: "unknown", depth: 0, depth_name: "unknown",
        reason: "no occurrence-keyed owner evidence supplied" };
      continue;
    }
    const [depth, depthName] = stateDepth(axis, measured);
    for (const fact of measured) owners.add(fact.owner);
    resultAxes[axis] = { status: "measured", depth, depth_name: depthName,
      facts: measured.map(fact => ({ owner: fact.owner, state: fact.state,
        ...(fact.receipt ? { receipt: fact.receipt } : {}) })) };
  }
  const [braidDepth, braidPath] = longestOwnerPath(transitions);
  return {
    axes: resultAxes, braid_shape: braidShape(transitions), distinct_owners: [...owners].sort(),
    distinct_owner_count: owners.size, known_owner_transitions: transitions.size,
    transition_pairs: sortedPairs(transitions), braid_depth: braidDepth, braid_path: braidPath,
    measurement_complete: axes.every(axis => resultAxes[axis].status === "measured"),
  };
}

export function buildProfile(row, facts = [], transitions = new Set()) {
  const candidate = candidateProfile(row);
  const realized = realizedProfile(row.id, facts, transitions);
  const unknownAxes = axes.filter(axis => realized.axes[axis].status === "unknown");
  const weights = law.investigation_score;
  const investigationPriority = candidate.join_power * weights.join_power_weight
    + Math.min(candidate.chain_span, weights.chain_span_cap)
    + candidate.candidate_plane_count * weights.candidate_plane_weight
    + unknownAxes.length - realized.braid_depth * weights.braid_depth_weight
    - realized.braid_shape.branch_points.length * weights.branch_weight
    - realized.braid_shape.rejoin_points.length * weights.rejoin_weight
    - realized.braid_shape.braid_excess * weights.braid_excess_weight;
  return {
    schema: "bonfyre.external-participation-profile.v2", truth_level: "compiled_diagnostic_projection",
    id: row.id ?? null, name: row.name ?? null, source_kind: row.source_kind ?? null, url: row.url ?? null,
    candidate, realized, unknown_axes: unknownAxes, investigation_priority: investigationPriority,
    cannot_infer: law.cannot_infer,
  };
}

export function build({ generated = defaultGenerated, evidence = join(generated, "participation_evidence.jsonl"),
  frontierLimit = 100, write = false } = {}) {
  const external = loadJsonl(join(generated, "external_bindings.jsonl"));
  const receipts = loadJsonl(evidence);
  const [byOccurrence, transitions] = evidenceIndex(receipts);
  const profiles = external.map(row => buildProfile(row, byOccurrence.get(row.id) ?? [], transitions.get(row.id) ?? new Set()));
  const candidateAxes = {}, measuredAxes = {}, joinPower = {}, braidDepth = {};
  for (const profile of profiles) {
    for (const [axis, value] of Object.entries(profile.candidate.candidate_planes))
      if (value) candidateAxes[axis] = (candidateAxes[axis] ?? 0) + 1;
    for (const [axis, value] of Object.entries(profile.realized.axes))
      if (value.status === "measured") measuredAxes[axis] = (measuredAxes[axis] ?? 0) + 1;
    const power = profile.candidate.join_power, depth = profile.realized.braid_depth;
    joinPower[power] = (joinPower[power] ?? 0) + 1;
    braidDepth[depth] = (braidDepth[depth] ?? 0) + 1;
  }
  const frontier = [...profiles].sort((a, b) => b.investigation_priority - a.investigation_priority
    || b.candidate.join_power - a.candidate.join_power
    || b.candidate.chain_span - a.candidate.chain_span || a.id.localeCompare(b.id))
    .slice(0, Math.max(1, frontierLimit));
  const manifest = {
    schema: "bonfyre.external-participation-manifest.v2", truth_level: "compiled_diagnostic_projection",
    total_occurrences: profiles.length, owner_evidence_rows: receipts.length,
    candidate_axis_counts: candidateAxes, measured_axis_counts: measuredAxes,
    join_power_distribution: joinPower, braid_depth_distribution: braidDepth,
    frontier_limit: frontier.length,
    cannot_infer: ["unknown realized depth -> zero realized depth",
      "high investigation priority -> source admission, authority or value",
      "latent join power -> semantic compatibility or permission to join data"],
  };
  if (write) {
    writeFileSync(join(generated, "participation_manifest.json"), `${prettyCanonical(manifest)}\n`);
    writeFileSync(join(generated, "participation_profiles.jsonl"), profiles.map(pythonFlat).join("\n") + (profiles.length ? "\n" : ""));
    writeFileSync(join(generated, "participation_frontier.jsonl"), frontier.map(pythonFlat).join("\n") + (frontier.length ? "\n" : ""));
  }
  return { manifest, profiles, frontier };
}

export function main(args = process.argv.slice(2)) {
  const evidenceIndex = args.indexOf("--evidence"), frontierIndex = args.indexOf("--frontier-limit");
  const generatedIndex = args.indexOf("--generated");
  const generated = generatedIndex < 0 ? defaultGenerated : resolve(args[generatedIndex + 1]);
  const evidence = evidenceIndex < 0 ? join(generated, "participation_evidence.jsonl") : resolve(args[evidenceIndex + 1]);
  const frontierLimit = frontierIndex < 0 ? 100 : Number(args[frontierIndex + 1]);
  if (!Number.isInteger(frontierLimit) || args.some(arg => arg.startsWith("-")
    && !["--evidence", "--frontier-limit", "--generated", "--write"].includes(arg)))
    throw new Error("usage: participation_report.mjs [--write] [--evidence PATH] [--frontier-limit N] [--generated DIR]");
  const { manifest } = build({ generated, evidence, frontierLimit, write: args.includes("--write") });
  process.stdout.write(`${prettyCanonical(manifest)}\n`);
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) main();
