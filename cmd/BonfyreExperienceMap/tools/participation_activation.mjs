#!/usr/bin/env bun
// Diagnostic projection only: ranks observed candidates without admitting a
// source, granting authority, or claiming an owner transition occurred.
import { existsSync, readFileSync, statSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { prettyCanonical, pythonFlat } from "./json_contract.mjs";

const here = dirname(fileURLToPath(import.meta.url));
const defaultGenerated = join(here, "../generated");
export const law = JSON.parse(readFileSync(join(here, "activation-law.generated.json"), "utf8"));

const count = values => {
  const out = {};
  for (const value of values) out[value] = (out[value] ?? 0) + 1;
  return out;
};

const hasAny = (values, wanted) => wanted?.some(value => values.has(value)) ?? false;

export function leverageModes(profile) {
  const candidate = profile.candidate;
  const roles = new Set(candidate.role_classes ?? []);
  const protocols = new Set(candidate.protocol_carriers ?? []);
  const source = profile.source_kind;
  const modes = law.modes.filter(rule => {
    const sourceMatch = rule.source_kind_any?.includes(source) ?? false;
    const roleMatch = hasAny(roles, rule.roles_any);
    const protocolMatch = hasAny(protocols, rule.protocol_any);
    const sourceOrProtocol = rule.source_or_protocol && (sourceMatch || protocolMatch);
    const sourceOrRole = rule.source_or_role && (sourceMatch || roleMatch);
    if (rule.source_or_protocol && !sourceOrProtocol) return false;
    if (rule.source_or_role && !sourceOrRole) return false;
    if (rule.roles_any && !rule.source_or_role && !roleMatch) return false;
    if (rule.roles_none && hasAny(roles, rule.roles_none)) return false;
    if (rule.min_join_power && (candidate.join_power ?? 0) < rule.min_join_power) return false;
    if (rule.candidate_plane && !candidate.candidate_planes[rule.candidate_plane]) return false;
    return true;
  }).map(rule => rule.name);
  return modes.length ? modes : [law.fallback_mode];
}

export function nextActivation(profile) {
  const candidate = profile.candidate.candidate_planes;
  const axes = profile.realized.axes;
  const match = law.next.find(rule => candidate[rule.candidate_plane]
    && (rule.status === undefined || axes[rule.axis].status === rule.status)
    && (rule.depth_lt === undefined || (axes[rule.axis].depth ?? 0) < rule.depth_lt));
  const { owner, action, why } = match ?? law.fallback_next;
  return { owner, action, why };
}

export function activationScore(profile, modes) {
  const score = law.score;
  return (profile.investigation_priority ?? 0)
    + modes.reduce((sum, mode) => sum + (score.bonuses[mode] ?? 0), 0)
    + (profile.candidate.join_power ?? 0) * score.join_power_weight
    + Math.min(profile.candidate.chain_span ?? 0, score.chain_span_cap)
    - (profile.realized.braid_depth ?? 0) * score.braid_depth_weight;
}

export function protocolDerivationWitness(path, bridgeSource) {
  if (!existsSync(path) || !existsSync(bridgeSource)) return false;
  try { if (!statSync(path).isFile() || !statSync(bridgeSource).isFile()) return false; }
  catch { return false; }
  let row;
  try { row = JSON.parse(readFileSync(path, "utf8")); } catch { return false; }
  return row?.schema === "bonfyre-protocol-derivation-bridge.v1"
    && row.registered === true && typeof row.contract === "string" && row.contract.length > 0
    && Number.isInteger(row.enactments) && row.enactments >= 2
    && Number.isInteger(row.programs) && row.programs >= 2
    && row.scoped === true && row.targeted_invalidation === true && row.atomic === true;
}

export function disturbanceAcceptance(rows) {
  const policy = law.disturbance;
  const failures = [];
  const byId = new Map();
  const disturbanceIds = new Set();
  for (const [index, row] of rows.entries()) {
    const id = row.event_id;
    if (row.schema !== policy.schema) failures.push(`row ${index + 1}: wrong disturbance schema`);
    if (typeof id !== "string" || !id) {
      failures.push(`row ${index + 1}: missing event_id`);
      continue;
    }
    if (byId.has(id)) failures.push(`duplicate event_id ${id}`);
    byId.set(id, row);
    for (const field of ["owner", "disturbance_id", "kind", "subject"])
      if (typeof row[field] !== "string" || !row[field]) failures.push(`${id}: missing ${field}`);
    if (typeof row.disturbance_id === "string" && row.disturbance_id) disturbanceIds.add(row.disturbance_id);
    if (typeof (row.evidence_ref || row.receipt) !== "string") failures.push(`${id}: missing owner evidence reference`);
    const parents = row.causal_parents ?? [];
    if (!Array.isArray(parents) || parents.some(parent => typeof parent !== "string" || !parent))
      failures.push(`${id}: invalid causal_parents`);
  }
  if (!rows.length) failures.push("no disturbance trace supplied");
  if (disturbanceIds.size !== 1) failures.push(`expected one disturbance_id, got ${disturbanceIds.size}`);

  const roots = [];
  const ownerEdges = new Set();
  for (const [id, row] of byId) {
    const parents = Array.isArray(row.causal_parents) ? row.causal_parents : [];
    if (!parents.length) roots.push(id);
    for (const parent of parents) {
      if (!byId.has(parent)) { failures.push(`${id}: missing causal parent ${parent}`); continue; }
      const source = byId.get(parent).owner;
      const target = row.owner;
      if (source && target && source !== target) ownerEdges.add(JSON.stringify([source, target]));
    }
  }
  if (roots.length !== 1) failures.push(`expected one root disturbance, got ${roots.length}`);

  const visiting = new Set();
  const depths = new Map();
  function depth(id) {
    if (depths.has(id)) return depths.get(id);
    if (visiting.has(id)) { failures.push("causal graph contains a cycle"); return [0, 0]; }
    visiting.add(id);
    const row = byId.get(id);
    let bestEvent = 1;
    let bestOwner = 0;
    for (const parent of Array.isArray(row.causal_parents) ? row.causal_parents : []) {
      if (!byId.has(parent)) continue;
      const [parentEvent, parentOwner] = depth(parent);
      bestEvent = Math.max(bestEvent, parentEvent + 1);
      bestOwner = Math.max(bestOwner, parentOwner + Number(byId.get(parent).owner !== row.owner));
    }
    visiting.delete(id);
    const result = [bestEvent, bestOwner];
    depths.set(id, result);
    return result;
  }
  for (const id of byId.keys()) depth(id);

  const owners = new Set(rows.map(row => row.owner).filter(Boolean));
  const kinds = count(rows.map(row => String(row.kind ?? "")));
  const grouped = Object.fromEntries(Object.entries(policy.kind_groups)
    .map(([name, members]) => [name, members.reduce((sum, kind) => sum + (kinds[kind] ?? 0), 0)]));
  const verdicts = count(rows.filter(row => row.owner === "RoundTripTwin" && typeof row.verdict === "string")
    .map(row => row.verdict));
  const sortedEdges = [...ownerEdges].map(edge => JSON.parse(edge)).sort(([a, b], [c, d]) => a.localeCompare(c) || b.localeCompare(d));
  const measured = {
    trace_events: rows.length,
    distinct_owners: owners.size,
    owners: [...owners].sort(),
    owner_transition_kinds: ownerEdges.size,
    owner_transition_pairs: sortedEdges,
    max_event_depth: Math.max(0, ...[...depths.values()].map(pair => pair[0])),
    max_owner_braid_depth: Math.max(0, ...[...depths.values()].map(pair => pair[1])),
    ...grouped,
    round_trip: Object.fromEntries(Object.entries(verdicts).sort(([a], [b]) => a.localeCompare(b))),
    round_trip_total: Object.values(verdicts).reduce((sum, value) => sum + value, 0),
  };
  for (const owner of policy.required_owners.filter(owner => !owners.has(owner)).sort()) failures.push(`missing required owner ${owner}`);
  if (!policy.economic_owners.some(owner => owners.has(owner))) failures.push("missing economic owner");
  for (const [metric, floor] of Object.entries(policy.thresholds))
    if ((measured[metric] ?? 0) < floor) failures.push(`${metric}=${measured[metric] ?? 0} below ${floor}`);
  if (measured.retractions + measured.compensations < 1) failures.push("no retraction or compensation");
  if (measured.improper_erases) failures.push("history was erased instead of compensated/retracted");
  if ((measured.round_trip.drifted ?? 0) > measured.corrections) failures.push("round-trip drift lacks correction work");
  return {
    schema: "bonfyre.disturbance-acceptance.v1", accepted: !failures.length,
    metrics: measured, failures,
    cannot_infer: ["event count -> propagation depth", "sequence/step number -> causality",
      "owner label -> owner receipt", "completed protocol -> discharged commitment"],
  };
}

const readJsonl = path => existsSync(path)
  ? readFileSync(path, "utf8").split(/\r?\n/).filter(line => line.trim()).map(line => JSON.parse(line)) : [];

export function build({ generated = defaultGenerated, limit = 250 } = {}) {
  const profiles = readJsonl(join(generated, "participation_profiles.jsonl"));
  const rows = [];
  const byMode = new Map();
  const modeCounts = {};
  const nextOwnerCounts = {};
  for (const profile of profiles) {
    const modes = leverageModes(profile);
    const next = nextActivation(profile);
    const row = {
      schema: "bonfyre.external-activation-candidate.v1", id: profile.id, name: profile.name,
      source_kind: profile.source_kind, url: profile.url ?? null, leverage_modes: modes,
      latent_join_power: profile.candidate.join_power, chain_span: profile.candidate.chain_span,
      candidate_plane_count: profile.candidate.candidate_plane_count,
      realized_braid_depth: profile.realized.braid_depth,
      next_activation: next, activation_score: activationScore(profile, modes),
      cannot_infer: law.cannot_infer,
    };
    rows.push(row);
    nextOwnerCounts[next.owner] = (nextOwnerCounts[next.owner] ?? 0) + 1;
    for (const mode of modes) {
      modeCounts[mode] = (modeCounts[mode] ?? 0) + 1;
      if (!byMode.has(mode)) byMode.set(mode, []);
      byMode.get(mode).push(row);
    }
  }
  const rank = (a, b) => b.activation_score - a.activation_score || a.id.localeCompare(b.id);
  rows.sort(rank);
  const selected = rows.slice(0, Math.max(1, limit));
  const cohortHeads = {};
  for (const mode of [...byMode.keys()].sort()) {
    const candidates = byMode.get(mode).sort(rank);
    cohortHeads[mode] = candidates.slice(0, 10).map(row => ({
      id: row.id, name: row.name, score: row.activation_score, next_owner: row.next_activation.owner,
    }));
  }
  const disturbance = disturbanceAcceptance(readJsonl(join(generated, "disturbance_trace.jsonl")));
  const sensorReceipts = readJsonl(join(generated, "disturbance_sensor_receipt.jsonl"));
  disturbance.sensor_boundary = {
    attempts: sensorReceipts.length,
    states: count(sensorReceipts.map(row => row.observation_kind ?? "unknown")),
    body_observed: sensorReceipts.some(row => row.observation_kind === "source_observed"),
    policy_decisions: count(sensorReceipts.map(row => row.policy_decision ?? "unknown")),
    cannot_infer: ["documented public feed -> lawful observation by this agent"],
  };

  const evidence = readJsonl(join(generated, "participation_evidence.jsonl"));
  const domainBindings = evidence.filter(row => row.schema === "bonfyre-domain-binding.v1" && row.binding_validated === true);
  disturbance.owner_bridge_evidence = {
    "ForeignTwinGraph->DomainProfile": {
      receipt_ingress_observed: Boolean(domainBindings.length),
      validated_bindings: domainBindings.length,
      blocked_candidates: nextOwnerCounts.DomainProfile ?? 0,
      cannot_infer: ["validated occurrence bindings -> all future DomainProfile handoffs are ready"],
    },
    "ProtocolContract->SemanticDerivationGraph": {
      native_graph_registration: protocolDerivationWitness(join(generated, "protocol_derivation_bridge_receipt.json"),
        join(here, "../src/protocol_derivation_bridge.c")),
      cannot_infer: ["native registration witness -> every contract has a valid lowering"],
    },
  };
  const manifest = {
    schema: "bonfyre.external-activation-manifest.v1", truth_level: "compiled_diagnostic_projection",
    total_occurrences: profiles.length, frontier_rows: selected.length,
    leverage_mode_counts: modeCounts, next_owner_counts: nextOwnerCounts,
    cohort_heads: cohortHeads, disturbance_acceptance: disturbance,
    cannot_infer: ["rank -> source admission, authority, budget or value",
      "cohort membership -> semantic equivalence between members"],
  };
  return { manifest, disturbance, selected };
}

export function main(args = process.argv.slice(2)) {
  const generatedIndex = args.indexOf("--generated");
  const limitIndex = args.indexOf("--limit");
  const generated = generatedIndex >= 0 ? resolve(args[generatedIndex + 1]) : defaultGenerated;
  const limit = limitIndex >= 0 ? Number(args[limitIndex + 1]) : 250;
  if (!Number.isInteger(limit) || args.some(arg => arg.startsWith("-") && !["--generated", "--limit", "--write"].includes(arg)))
    throw new Error("usage: participation_activation.mjs [--write] [--limit N] [--generated DIR]");
  const { manifest, disturbance, selected } = build({ generated, limit });
  if (args.includes("--write")) {
    writeFileSync(join(generated, "participation_activation_manifest.json"), `${prettyCanonical(manifest)}\n`);
    writeFileSync(join(generated, "disturbance_acceptance_manifest.json"), `${prettyCanonical(disturbance)}\n`);
    writeFileSync(join(generated, "participation_activation_frontier.jsonl"), selected.map(row => pythonFlat(row)).join("\n") + (selected.length ? "\n" : ""));
  }
  process.stdout.write(`${prettyCanonical(manifest)}\n`);
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) main();
