import { createHash } from "node:crypto";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { evaluatePolicyFamily } from "./policy-family-evaluator.mjs";
import { calibrateResidual } from "./residual-calibrator.mjs";
import { buildTrajectoryCalculus } from "../kernel/src/trajectory-calculus.mjs";

function normalizeKey(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

function stableHash(payload) {
  return createHash("sha256").update(JSON.stringify(payload)).digest("hex");
}

function nowIso() {
  return new Date().toISOString();
}

function slugifyTarget(value) {
  return normalizeKey(value).slice(0, 96) || "unknown-target";
}

function inferIngressDepth(surface) {
  if (surface === "hard-fork") return "sovereign_fork";
  if (surface === "soft-fork") return "shim";
  if (surface === "adapter") return "adapter";
  if (surface === "observer") return "observer";
  return "coprocessor";
}

function inferTrustLevel(surface) {
  if (surface === "hard-fork" || surface === "state-machine") return "protocol-critical";
  if (surface === "observer") return "attestation-critical";
  if (surface === "soft-fork") return "execution-critical";
  return "advisory";
}

function inferRequiredKernelLayers(surface) {
  const common = ["representation", "commitment", "trajectory", "transport", "claims"];
  if (surface === "adapter") return ["representation", "commitment", "claims"];
  if (surface === "observer") return [...common, "observer"];
  if (surface === "soft-fork") return [...common, "policy"];
  return [...common, "policy", "lineage"];
}

function inferChartFamily(category, target, surface) {
  const key = normalizeKey(category);
  const targetKey = normalizeKey(target);
  if (key === "transport" || key === "transport-continuity" || key === "transport_continuity") return "bridge_packet";
  if (key === "execution" || key === "execution-continuity" || key === "execution_continuity") return "execution_batch";
  if (key === "rollup-pipeline" || key === "rollup_pipeline" || key === "execution-pipeline-continuity" || key === "execution_pipeline_continuity") return "execution_batch";
  if (key === "availability" || key === "availability-persistence-continuity" || key === "availability_persistence_continuity") return "availability_root";
  if (key === "financial-topology" || key === "financial_topology" || key === "financial-topology-continuity" || key === "financial_topology_continuity") return "liquidity_topology";
  if (key === "privacy-succinctness" || key === "privacy_succinctness" || key === "privacy-succinctness-continuity" || key === "privacy_succinctness_continuity") return "identity_claim";
  if (key === "runtime-shim") return "workflow_state";
  if (targetKey.includes("ibc") || targetKey.includes("packet") || targetKey.includes("layerzero") || targetKey.includes("wormhole") || targetKey.includes("hyperlane") || targetKey.includes("stargate")) return "bridge_packet";
  if (targetKey.includes("oracle") || targetKey.includes("chainlink") || targetKey.includes("pyth")) return "oracle_round";
  if (surface === "soft-fork") return "workflow_state";
  return "generic";
}

function inferPolicyFamily(category, target, surface) {
  const key = normalizeKey(category);
  const targetKey = normalizeKey(target);
  if (key.includes("availability")) return "availability";
  if (key.includes("financial")) return targetKey.includes("restak") ? "restaking" : "liquidity";
  if (key.includes("privacy")) return "privacy";
  if (key.includes("transport") || targetKey.includes("ibc") || targetKey.includes("wormhole") || targetKey.includes("layerzero")) return "settlement";
  if (targetKey.includes("oracle") || targetKey.includes("chainlink") || targetKey.includes("pyth")) return "oracle";
  if (surface === "soft-fork") return "workflow";
  return "finality";
}

function inferEdgeFamily(category, target, surface) {
  const key = normalizeKey(category);
  const targetKey = normalizeKey(target);
  if (key.includes("transport")) return "transport-edge";
  if (key.includes("execution") || key.includes("pipeline")) return "execution-edge";
  if (key.includes("financial")) return targetKey.includes("collateral") ? "collateral-edge" : "liquidity-edge";
  if (key.includes("availability")) return "availability-edge";
  if (key.includes("privacy")) return "identity-edge";
  if (targetKey.includes("oracle") || targetKey.includes("chainlink") || targetKey.includes("pyth")) return "oracle-edge";
  if (surface === "soft-fork") return "fulfillment-edge";
  if (surface === "observer") return "trust-edge";
  return "execution-edge";
}

function readRegistry(repoRoot) {
  const path = join(repoRoot, "registry", "integrations.json");
  return JSON.parse(readFileSync(path, "utf8"));
}

function readMutationBoundaries(repoRoot) {
  const path = join(repoRoot, "registry", "protocol-mutation-boundaries.json");
  if (!existsSync(path)) return [];
  const payload = JSON.parse(readFileSync(path, "utf8"));
  return Array.isArray(payload?.boundaries) ? payload.boundaries : [];
}

function tokenizeProtocolLabel(value) {
  return normalizeKey(value)
    .split("-")
    .map((item) => item.trim())
    .filter((item) => item && item.length > 2);
}

function scoreBoundary(descriptor, boundary) {
  const targetKey = normalizeKey(descriptor.target);
  const protocolKey = normalizeKey(boundary?.protocol);
  const policyFamily = inferPolicyFamily(descriptor.category, descriptor.target, descriptor.surface);

  let score = 0;
  if (!targetKey || !protocolKey) return score;
  if (targetKey === protocolKey) score += 6;
  if (descriptor.chartFamily && boundary?.chart_family === descriptor.chartFamily) score += 3;
  if (policyFamily && boundary?.policy_family === policyFamily) score += 1;

  const tokens = tokenizeProtocolLabel(boundary?.protocol);
  const shared = tokens.filter((token) => targetKey.includes(token));
  score += shared.length;
  return score;
}

function hasBoundaryAffinity(descriptor, boundary) {
  const targetKey = normalizeKey(descriptor.target);
  const protocolKey = normalizeKey(boundary?.protocol);
  if (!targetKey || !protocolKey) return false;
  if (targetKey === protocolKey) return true;
  const tokens = tokenizeProtocolLabel(boundary?.protocol);
  return tokens.some((token) => targetKey.includes(token));
}

function resolveMutationBoundary(repoRoot, descriptor) {
  const boundaries = readMutationBoundaries(repoRoot);
  let winner = null;
  let winnerScore = 0;

  for (const boundary of boundaries) {
    const score = scoreBoundary(descriptor, boundary);
    if (score > winnerScore) {
      winner = boundary;
      winnerScore = score;
    }
  }

  if (!winner || winnerScore < 4) return null;
  if (!hasBoundaryAffinity(descriptor, winner)) return null;
  return winner;
}

function pushTarget(index, target, descriptor, namespace = null) {
  const keys = new Set();
  keys.add(normalizeKey(target));
  keys.add(normalizeKey(target.replace(/\//g, " ")));
  keys.add(normalizeKey(target.replace(/-style/g, "")));
  keys.add(normalizeKey(target.replace(/\s+style\s+/gi, " ")));

  if (namespace) {
    for (const key of Array.from(keys)) {
      if (!key) continue;
      index.set(`${namespace}:${key}`, descriptor);
    }
  }

  for (const key of keys) {
    if (!key) continue;
    if (!index.has(key)) index.set(key, descriptor);
  }
}

function containsTarget(list, target) {
  const key = normalizeKey(target);
  return list.some((item) => normalizeKey(item) === key);
}

function buildStateMachineMembership(registry, target) {
  const memberships = [];
  const groups = registry.state_machine_integrations || {};
  for (const [group, targets] of Object.entries(groups)) {
    if (Array.isArray(targets) && containsTarget(targets, target)) {
      memberships.push(group);
    }
  }
  return memberships;
}

function buildTargetIndex(registry) {
  const index = new Map();

  const hardCategories = registry.hard_fork_targets?.categories || {};
  for (const [category, targets] of Object.entries(hardCategories)) {
    for (const target of targets) {
      const descriptor = {
        target,
        surface: "hard-fork",
        category,
        runtimeDepth: "protocol-native",
        ingressDepth: inferIngressDepth("hard-fork"),
        trustLevel: inferTrustLevel("hard-fork"),
        requiredKernelLayers: inferRequiredKernelLayers("hard-fork"),
        chartFamily: inferChartFamily(category, target, "hard-fork"),
      };
      descriptor.stateMachineTypes = buildStateMachineMembership(registry, target);
      pushTarget(index, target, descriptor);
    }
  }

  for (const target of registry.soft_fork_targets?.targets || []) {
    const descriptor = {
      target,
      surface: "soft-fork",
      category: "runtime-shim",
      runtimeDepth: "execution-loop-mediation",
      ingressDepth: inferIngressDepth("soft-fork"),
      trustLevel: inferTrustLevel("soft-fork"),
      requiredKernelLayers: inferRequiredKernelLayers("soft-fork"),
      chartFamily: inferChartFamily("runtime-shim", target, "soft-fork"),
    };
    descriptor.stateMachineTypes = buildStateMachineMembership(registry, target);
    pushTarget(index, target, descriptor);
  }

  for (const target of registry.ingress_adapters?.thin_kernel_projectors || []) {
    const descriptor = {
      target,
      surface: "adapter",
      category: "thin-kernel-projector",
      runtimeDepth: "semantic-projection",
      ingressDepth: inferIngressDepth("adapter"),
      trustLevel: inferTrustLevel("adapter"),
      requiredKernelLayers: inferRequiredKernelLayers("adapter"),
      chartFamily: inferChartFamily("thin-kernel-projector", target, "adapter"),
    };
    descriptor.stateMachineTypes = buildStateMachineMembership(registry, target);
    pushTarget(index, target, descriptor);
  }

  for (const target of registry.ingress_adapters?.protocol_observer_layers || []) {
    const descriptor = {
      target,
      surface: "observer",
      category: "protocol-observer-layer",
      runtimeDepth: "observer-coprocessor",
      ingressDepth: inferIngressDepth("observer"),
      trustLevel: inferTrustLevel("observer"),
      requiredKernelLayers: inferRequiredKernelLayers("observer"),
      chartFamily: inferChartFamily("protocol-observer-layer", target, "observer"),
    };
    descriptor.stateMachineTypes = buildStateMachineMembership(registry, target);
    pushTarget(index, target, descriptor);
  }

  const stateMachine = registry.state_machine_integrations || {};
  for (const [group, targets] of Object.entries(stateMachine)) {
    for (const target of targets) {
      const descriptor = {
        target,
        surface: "state-machine",
        category: group,
        runtimeDepth: "continuity-native",
        ingressDepth: inferIngressDepth("state-machine"),
        trustLevel: inferTrustLevel("state-machine"),
        requiredKernelLayers: inferRequiredKernelLayers("state-machine"),
        chartFamily: inferChartFamily(group, target, "state-machine"),
      };
      descriptor.stateMachineTypes = buildStateMachineMembership(registry, target);
      pushTarget(index, target, descriptor, `state-machine:${normalizeKey(group)}`);
    }
  }

  return index;
}

function toTransitionType(surface) {
  if (surface === "hard-fork") return "protocol-mutation";
  if (surface === "soft-fork") return "host-loop-mediation";
  if (surface === "adapter") return "semantic-projection";
  if (surface === "state-machine") return "continuity-projection";
  return "observer-emission";
}

function buildProtocolProfile(descriptor) {
  const policyFamily = inferPolicyFamily(descriptor.category, descriptor.target, descriptor.surface);
  const base = {
    maps_into: [
      "committed_state",
      "transitions",
      "policies",
      "lineage_edges",
      "export_projections",
    ],
    witness_kind: "hash-witness",
    lineage_mode: "append-only-replay",
    continuity_mode: "classify-and-allow",
    policy_family: policyFamily,
    chart_family: descriptor.chartFamily || "generic",
    ingress_depth: descriptor.ingressDepth,
    trust_level: descriptor.trustLevel,
    required_kernel_layers: descriptor.requiredKernelLayers,
    mutation_path: ["ingress", "normalize", "project", "commit", "export"],
    monitor_signals: ["deformation_score", "witness_skew"],
  };

  if (descriptor.surface === "hard-fork") {
    return {
      ...base,
      continuity_mode: "protocol-native-gating",
      mutation_path: ["protocol-pre", "mutation-path", "state-commit", "witness-emit", "lineage-append"],
      monitor_signals: ["deformation_score", "witness_skew", "transition_density"],
      witness_kind: "mutation-path-witness",
    };
  }

  if (descriptor.surface === "soft-fork") {
    return {
      ...base,
      continuity_mode: "host-loop-gating",
      mutation_path: ["loop-enter", "state-translate", "policy-evaluate", "loop-exit"],
      monitor_signals: ["deformation_score", "policy_churn"],
      execution_loop_hooks: ["before_step", "after_step", "on_error"],
      witness_kind: "loop-step-witness",
    };
  }

  if (descriptor.surface === "adapter") {
    return {
      ...base,
      continuity_mode: "projection-only",
      mutation_path: ["ingress", "kernel-projection", "commit", "export"],
      monitor_signals: ["projection_loss", "deformation_score"],
      witness_kind: "projection-witness",
    };
  }

  if (descriptor.surface === "observer") {
    return {
      ...base,
      continuity_mode: "observer-gating",
      mutation_path: ["observe", "project", "commit", "lineage-append"],
      monitor_signals: ["source_lag", "deformation_score", "witness_skew"],
      witness_kind: "observer-witness",
    };
  }

  return {
    ...base,
    continuity_mode: "state-machine-continuity",
    mutation_path: ["state-read", "transition-project", "continuity-check", "state-commit", "lineage-append"],
    monitor_signals: ["continuity_gap", "deformation_score", "witness_skew"],
    witness_kind: "continuity-witness",
  };
}

function toFiniteNumber(value, fallback) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function makeTransportSemantics(descriptor, event, mutationBoundary, policyDecision, transitions, runContext) {
  const family = descriptor.chartFamily;
  const transportLike = family === "bridge_packet" || family === "oracle_round" || family === "availability_root" || event?.transport === true;
  if (!transportLike) return null;

  const sourceDomain = String(event?.source_domain || event?.source_chain || "domain:unknown");
  const targetDomain = String(event?.target_domain || event?.target_chain || "domain:unknown");
  const relayPath = String(event?.relay_path || mutationBoundary?.protocol || descriptor.target);
  const routeLatencyMs = toFiniteNumber(event?.route_latency_ms, 120);
  const routeHops = toFiniteNumber(event?.route_hops, 2);
  const packetLossRate = clamp(toFiniteNumber(event?.packet_loss_rate, 0), 0, 1);
  const retransmitCount = Math.max(0, toFiniteNumber(event?.retransmit_count, 0));
  const replayNonce = String(event?.replay_nonce || `${runContext.runId}:${retransmitCount}`);

  const routeScoreRaw = 1 - ((routeLatencyMs / 1200) + (routeHops * 0.08) + (packetLossRate * 0.6));
  const routeScore = Number(clamp(routeScoreRaw, 0, 1).toFixed(4));
  const ackStatus = event?.ack === false || event?.acknowledged === false ? "missing" : "present";

  return {
    schema_version: "aurekai.transport_semantics.v1",
    envelope_id: stableHash({ sourceDomain, targetDomain, relayPath, replayNonce, run: runContext.runId }),
    domain_envelope: {
      source_domain: sourceDomain,
      target_domain: targetDomain,
      relay_path: relayPath,
      continuity_hops: Math.max(1, Math.round(routeHops)),
    },
    projection_modes: {
      public_mode: String(event?.public_projection_mode || "transport-summary"),
      private_mode: String(event?.private_projection_mode || "operator-trace"),
    },
    witness_bundle: {
      witness_boundary: mutationBoundary?.witness_boundary || "transport_witness",
      witness_items: transitions.map((item) => ({
        transition_id: item.transition_id,
        witness_hash: item.witness_hash,
        phase: item.phase,
      })),
    },
    replay_semantics: {
      mode: String(event?.replay_mode || "bounded-replay"),
      retransmission_allowed: event?.retransmission_allowed !== false,
      retransmit_count: retransmitCount,
      replay_nonce: replayNonce,
    },
    topology_scoring: {
      route_latency_ms: routeLatencyMs,
      route_hops: routeHops,
      packet_loss_rate: packetLossRate,
      route_score: routeScore,
    },
    domain_semantics: {
      bridge_packet_lineage: family === "bridge_packet",
      oracle_update_relay: family === "oracle_round",
      settlement_ack_semantics: ackStatus,
      decision: policyDecision?.decision || "allow",
    },
  };
}

function buildIngressDoctrineReport(descriptor, profile, mutationBoundary, policyDecision, artifacts) {
  const checks = [];
  const addCheck = (code, ok, details) => checks.push({ code, ok, details });

  addCheck("maps_into_committed_state", Boolean(artifacts?.committedState?.state_hash), "committed state hash present");
  addCheck("maps_into_transitions", Array.isArray(artifacts?.transitions) && artifacts.transitions.length > 0, "at least one transition emitted");
  addCheck("maps_into_policies", Boolean(policyDecision?.policy_id), "policy decision emitted");
  addCheck("maps_into_lineage_edges", Array.isArray(artifacts?.lineageEdges) && artifacts.lineageEdges.length > 0, "lineage edges emitted");
  addCheck("maps_into_export_projections", Array.isArray(artifacts?.exportProjections) && artifacts.exportProjections.length > 0, "export projections emitted");
  addCheck("maps_into_functional_claims", Array.isArray(artifacts?.functionalClaims) && artifacts.functionalClaims.length > 0, "functional claims emitted");

  const protocolCritical = descriptor.trustLevel === "protocol-critical" || descriptor.surface === "hard-fork" || descriptor.surface === "state-machine";
  addCheck("protocol_boundary_required", !protocolCritical || Boolean(mutationBoundary), "protocol-critical surfaces require mutation boundaries");

  const witnessReq = policyDecision?.continuity_context?.witness_requirements;
  addCheck("witness_requirements_declared", Array.isArray(witnessReq) && witnessReq.length > 0, "policy declares witness requirements");

  addCheck(
    "required_kernel_layers_present",
    Array.isArray(descriptor.requiredKernelLayers) && descriptor.requiredKernelLayers.length >= 3,
    "kernel layer doctrine metadata present",
  );

  const failed = checks.filter((item) => !item.ok);
  return {
    schema_version: "aurekai.ingress_doctrine_report.v1",
    profile: {
      ingress_depth: descriptor.ingressDepth,
      trust_level: descriptor.trustLevel,
      required_kernel_layers: descriptor.requiredKernelLayers,
      policy_family: profile.policy_family,
      chart_family: profile.chart_family,
    },
    map_targets: profile.maps_into,
    compliant: failed.length === 0,
    checks,
    failed_checks: failed.map((item) => item.code),
  };
}

function makeSpecializedClaims(descriptor, event, committedState, policyDecision, trajectory, runContext) {
  const family = String(policyDecision?.policy_family || "finality");
  const decisionPass = policyDecision?.decision === "allow";
  const claims = [];

  if (family === "settlement") {
    claims.push({
      schema_version: "aurekai.functional_claim.v1",
      claim_id: `${slugifyTarget(descriptor.target)}:${runContext.runId}:settlement-ack-claim`,
      claim_type: "settlement_ack_bound",
      predicate: "settlement.ack == present",
      target_commitment: committedState.state_hash,
      proof_ref: trajectory.folded_witness,
      opening_policy: "operator-private",
      evaluation_result: event?.ack === false || event?.acknowledged === false ? "fail" : "pass",
      context: { policy_family: family, chart_family: descriptor.chartFamily },
    });
  }

  if (family === "oracle") {
    const maxDrift = toFiniteNumber(event?.oracle_drift_max, 0.2);
    const observed = toFiniteNumber(event?.oracle_drift, toFiniteNumber(event?.witness_skew, 0));
    claims.push({
      schema_version: "aurekai.functional_claim.v1",
      claim_id: `${slugifyTarget(descriptor.target)}:${runContext.runId}:oracle-drift-claim`,
      claim_type: "oracle_drift_bounded",
      predicate: `oracle.drift <= ${maxDrift}`,
      target_commitment: committedState.state_hash,
      proof_ref: trajectory.folded_witness,
      opening_policy: "operator-private",
      evaluation_result: observed <= maxDrift ? "pass" : "fail",
      context: { observed_drift: observed, max_drift: maxDrift, policy_family: family },
    });
  }

  if (family === "collateral") {
    const ratio = toFiniteNumber(event?.collateral_ratio, 2);
    const min = toFiniteNumber(event?.min_collateral_ratio, 1);
    claims.push({
      schema_version: "aurekai.functional_claim.v1",
      claim_id: `${slugifyTarget(descriptor.target)}:${runContext.runId}:collateralization-claim`,
      claim_type: "collateralization_bound",
      predicate: `collateral.ratio >= ${min}`,
      target_commitment: committedState.state_hash,
      proof_ref: trajectory.folded_witness,
      opening_policy: "operator-private",
      evaluation_result: ratio >= min ? "pass" : "fail",
      context: { collateral_ratio: ratio, min_collateral_ratio: min, policy_family: family },
    });
  }

  if (family === "workflow") {
    claims.push({
      schema_version: "aurekai.functional_claim.v1",
      claim_id: `${slugifyTarget(descriptor.target)}:${runContext.runId}:workflow-compliance-claim`,
      claim_type: "workflow_policy_compliant",
      predicate: "workflow.step_policy_break == false && workflow.unhandled_branch == false",
      target_commitment: committedState.state_hash,
      proof_ref: trajectory.folded_witness,
      opening_policy: "operator-private",
      evaluation_result: decisionPass ? "pass" : "fail",
      context: { policy_family: family, decision: policyDecision.decision },
    });
  }

  return claims;
}

function makeContinuityPolicy(repoRoot, descriptor, event, residualAssessment, committedState, transitionWitnessHash) {
  const profile = buildProtocolProfile(descriptor);
  const evaluation = evaluatePolicyFamily(repoRoot, {
    policyFamily: profile.policy_family,
    chartFamily: descriptor.chartFamily,
    event,
    residual: residualAssessment,
    surface: descriptor.surface,
  });

  return {
    schema_version: "aurekai.policy_decision.v1",
    policy_id: `${slugifyTarget(descriptor.target)}:continuity:v1`,
    surface: descriptor.surface,
    category: descriptor.category,
    policy_family: evaluation.family_id,
    decision: evaluation.decision,
    confidence: evaluation.confidence,
    continuity_mode: profile.continuity_mode,
    reason_codes: evaluation.reason_codes,
    thresholds: {
      gate_at: evaluation.thresholds.gate,
      halt_at: evaluation.thresholds.halt,
    },
    continuity_context: {
      committed_state_hash: committedState.state_hash,
      witness_hash: transitionWitnessHash,
      deformation_window: event?.window || "current",
      chart_transition: evaluation.chart_transition,
      chart_transition_valid: evaluation.chart_transition_valid,
      triggered_fail_conditions: evaluation.triggered_fail_conditions,
      witness_requirements: evaluation.witness_requirements,
      projection_defaults: evaluation.projection_defaults,
      replay_expectations: evaluation.replay_expectations,
      residual_assessment: evaluation.residual,
    },
  };
}

function makeCommittedState(descriptor, event, residualAssessment, mutationBoundary, runContext) {
  const profile = buildProtocolProfile(descriptor);
  const statePayload = {
    target: descriptor.target,
    surface: descriptor.surface,
    category: descriptor.category,
    runtime_depth: descriptor.runtimeDepth,
    ingress_depth: descriptor.ingressDepth,
    trust_level: descriptor.trustLevel,
    required_kernel_layers: descriptor.requiredKernelLayers,
    chart_family: descriptor.chartFamily,
    residual_assessment: residualAssessment,
    mutation_boundary: mutationBoundary,
    state_machine_types: descriptor.stateMachineTypes,
    host_event: event,
    protocol_profile: profile,
    integration_run_id: runContext.runId,
    committed_at: runContext.timestamp,
  };

  return {
    schema_version: "aurekai.committed_state.v1",
    state_id: `${slugifyTarget(descriptor.target)}:${runContext.runId}`,
    state_hash: stableHash(statePayload),
    target: descriptor.target,
    surface: descriptor.surface,
    category: descriptor.category,
    runtime_depth: descriptor.runtimeDepth,
    ingress_depth: descriptor.ingressDepth,
    trust_level: descriptor.trustLevel,
    required_kernel_layers: descriptor.requiredKernelLayers,
    chart_family: descriptor.chartFamily,
    residual_assessment: residualAssessment,
    mutation_boundary: mutationBoundary,
    state_machine_types: descriptor.stateMachineTypes,
    committed_at: runContext.timestamp,
    host_event: event,
    canonical_state: statePayload,
  };
}

function makeTransitions(descriptor, event, committedState, mutationBoundary, runContext) {
  const profile = buildProtocolProfile(descriptor);
  const action = String(event?.action || "observe");
  const stage = String(event?.stage || profile.mutation_path[1] || "mutation-path");
  const canonicalPath = String(mutationBoundary?.canonical_transition_function || "");
  const canonicalSteps = canonicalPath
    .split("->")
    .map((item) => item.trim())
    .filter(Boolean);
  const stepSequence = canonicalSteps.length > 0 ? canonicalSteps : ["prepare", "commit"];

  const transitionCore = {
    run_id: runContext.runId,
    target: descriptor.target,
    action,
    stage,
    surface: descriptor.surface,
  };

  const transitions = [];
  for (let i = 0; i < stepSequence.length; i += 1) {
    const step = stepSequence[i];
    const isLast = i === stepSequence.length - 1;
    const phase = normalizeKey(step) || `step-${i + 1}`;
    const witnessHash = stableHash({ transitionCore, step, index: i, event, committedStateHash: committedState.state_hash });

    transitions.push({
      schema_version: "aurekai.transition.v1",
      transition_id: `${slugifyTarget(descriptor.target)}:${runContext.runId}:${phase}`,
      transition_type: toTransitionType(descriptor.surface),
      phase,
      witness_hash: witnessHash,
      predecessor_state_hash: committedState.state_hash,
      successor_state_hash: stableHash({ transitionCore, step, phase: `${phase}-successor`, committedState }),
      emitted_at: runContext.timestamp,
      metadata: {
        event_id: event?.event_id || null,
        source: event?.source || "unknown",
        chart_family: descriptor.chartFamily,
        witness_kind: profile.witness_kind,
        mutation_path: profile.mutation_path,
        canonical_step: step,
        step_index: i,
        final_step: isLast,
        witness_boundary: mutationBoundary?.witness_boundary || null,
        replay_granularity: mutationBoundary?.replay_granularity || null,
      },
    });
  }

  const witnessHash = transitions.length > 0
    ? transitions[transitions.length - 1].witness_hash
    : stableHash({ transitionCore, event, committedStateHash: committedState.state_hash });

  return { transitions, witnessHash };
}

function makeLineageEdges(descriptor, event, committedState, transitions, mutationBoundary, runContext) {
  const profile = buildProtocolProfile(descriptor);
  const edgeFamily = mutationBoundary?.lineage_edge_type || inferEdgeFamily(descriptor.category, descriptor.target, descriptor.surface);
  const sourceNode = event?.event_id || `event:${runContext.runId}`;
  const targetNode = committedState.state_id;
  const firstTransition = transitions[0];
  const lastTransition = transitions[transitions.length - 1] || firstTransition;

  const base = {
    schema_version: "aurekai.lineage_edge.v1",
    surface: descriptor.surface,
    category: descriptor.category,
    edge_family: edgeFamily,
    replay_granularity: mutationBoundary?.replay_granularity || (descriptor.surface === "state-machine" ? "transition" : "step"),
    emitted_at: runContext.timestamp,
  };

  return [
    {
      ...base,
      edge_id: `${slugifyTarget(descriptor.target)}:${runContext.runId}:event-to-state`,
      edge_type: "event_to_committed_state",
      from: sourceNode,
      to: targetNode,
      witness_hash: firstTransition?.witness_hash || committedState.state_hash,
    },
    {
      ...base,
      edge_id: `${slugifyTarget(descriptor.target)}:${runContext.runId}:state-to-transition`,
      edge_type: "committed_state_to_transition",
      from: targetNode,
      to: lastTransition?.transition_id || `${targetNode}:transition:none`,
      witness_hash: lastTransition?.witness_hash || committedState.state_hash,
    },
    {
      ...base,
      edge_id: `${slugifyTarget(descriptor.target)}:${runContext.runId}:replay-anchor`,
      edge_type: "replay_anchor",
      from: lastTransition?.transition_id || `${targetNode}:transition:none`,
      to: `${targetNode}:replay:${profile.lineage_mode}`,
      witness_hash: lastTransition?.witness_hash || committedState.state_hash,
    },
  ];
}

function makeExportProjections(descriptor, event, committedState, transitions, lineageEdges, policyDecision, transportSemantics, runContext) {
  const profile = buildProtocolProfile(descriptor);
  const lastTransition = transitions[transitions.length - 1];
  const projections = [
    {
      schema_version: "aurekai.export_projection.v1",
      projection_id: `${slugifyTarget(descriptor.target)}:${runContext.runId}:json`,
      format: "application/json",
      intent: "kernel-state-projection",
      payload: {
        maps_into: profile.maps_into,
        committed_state: committedState,
        transitions,
        lineage_edges: lineageEdges,
        policy: policyDecision,
      },
    },
    {
      schema_version: "aurekai.export_projection.v1",
      projection_id: `${slugifyTarget(descriptor.target)}:${runContext.runId}:ledger-row`,
      format: "text/csv",
      intent: "ops-ledger-projection",
      payload: {
        run_id: runContext.runId,
        target: descriptor.target,
        surface: descriptor.surface,
        event_id: event?.event_id || null,
        committed_state_hash: committedState.state_hash,
        transition_witness_hash: lastTransition?.witness_hash || null,
        policy_decision: policyDecision.decision,
      },
    },
  ];

  if (transportSemantics) {
    projections.push({
      schema_version: "aurekai.export_projection.v1",
      projection_id: `${slugifyTarget(descriptor.target)}:${runContext.runId}:transport-public`,
      format: "application/json",
      intent: "transport-public-projection",
      payload: {
        mode: transportSemantics.projection_modes.public_mode,
        domain_envelope: transportSemantics.domain_envelope,
        topology_scoring: transportSemantics.topology_scoring,
      },
    });

    projections.push({
      schema_version: "aurekai.export_projection.v1",
      projection_id: `${slugifyTarget(descriptor.target)}:${runContext.runId}:transport-private`,
      format: "application/json",
      intent: "transport-private-projection",
      payload: {
        mode: transportSemantics.projection_modes.private_mode,
        witness_bundle: transportSemantics.witness_bundle,
        replay_semantics: transportSemantics.replay_semantics,
      },
    });
  }

  return projections;
}

function makeTrajectory(descriptor, transitions, lineageEdges, mutationBoundary, runContext) {
  const trajectoryRoot = stableHash({ target: descriptor.target, run_id: runContext.runId, transitions });
  const historyAccumulator = stableHash({ trajectoryRoot, lineageEdges });
  const foldedWitness = stableHash({ historyAccumulator, surface: descriptor.surface, category: descriptor.category });
  const calculus = buildTrajectoryCalculus({
    transitions,
    lineageEdges,
    defaultQueries: [
      { type: "edge-family-count" },
      { type: "replay-slice", mode: "phase-window" },
      { type: "branch", edge_family: inferEdgeFamily(descriptor.category, descriptor.target, descriptor.surface) },
    ],
  });

  return {
    schema_version: "aurekai.trajectory.v1",
    trajectory_id: `${slugifyTarget(descriptor.target)}:${runContext.runId}:trajectory`,
    trajectory_root: trajectoryRoot,
    history_accumulator: historyAccumulator,
    folded_witness: foldedWitness,
    step_count: transitions.length,
    edge_count: lineageEdges.length,
    edge_families: calculus.edge_families,
    branch_count: calculus.branch_count,
    branches: calculus.branches,
    replay_slices: calculus.replay_slices,
    branch_compression: calculus.branch_compression,
    continuity_queries: calculus.continuity_queries,
    replay_mode: descriptor.surface === "state-machine" ? "state-machine-unfold" : "lineage-unfold",
    continuity_summary: {
      ingress_depth: descriptor.ingressDepth,
      edge_family: mutationBoundary?.lineage_edge_type || inferEdgeFamily(descriptor.category, descriptor.target, descriptor.surface),
      policy_family: inferPolicyFamily(descriptor.category, descriptor.target, descriptor.surface),
      canonical_state_object: mutationBoundary?.canonical_state_object || null,
      canonical_transition_function: mutationBoundary?.canonical_transition_function || null,
      witness_boundary: mutationBoundary?.witness_boundary || null,
      replay_granularity: mutationBoundary?.replay_granularity || null,
    },
  };
}

function isCommerceDescriptor(descriptor, event) {
  const key = normalizeKey(descriptor?.category);
  const family = normalizeKey(descriptor?.chartFamily);
  if (event?.commerce === true || Boolean(event?.commerce_mode)) return true;
  return key.includes("commerce") || family === "fulfillment-state" || family === "treasury-state";
}

function extractTriggeredFailConditions(policyDecision) {
  const raw = policyDecision?.continuity_context?.triggered_fail_conditions;
  if (Array.isArray(raw)) {
    return raw.map((item) => String(item || "").trim()).filter(Boolean);
  }

  const reason = Array.isArray(policyDecision?.reason_codes)
    ? policyDecision.reason_codes.find((code) => String(code).startsWith("fail_conditions:"))
    : null;
  if (!reason) return [];

  const payload = String(reason).split(":").slice(1).join(":");
  if (!payload || payload === "none") return [];
  return payload.split(",").map((item) => item.trim()).filter(Boolean);
}

function evaluateCommerceChargeability(policyDecision, event) {
  const policyFamily = String(policyDecision?.policy_family || "");
  const triggered = extractTriggeredFailConditions(policyDecision);
  const blockedByFamily = new Map([
    ["fulfillment", new Set(["delivery_gap", "handoff_break"])],
    ["workflow", new Set(["step_policy_break", "unhandled_branch"])],
  ]);

  const familyBlockers = blockedByFamily.get(policyFamily) || new Set();
  const matchedBlockers = triggered.filter((condition) => familyBlockers.has(condition));

  const explicitSlaBreach = event?.sla_breach === true || event?.sla_violation === true;
  const explicitHandoffBreak = event?.handoff_break === true;
  const hasPolicyBlock = matchedBlockers.length > 0;

  const shouldFail = policyDecision?.decision === "halt" || hasPolicyBlock || explicitSlaBreach || explicitHandoffBreak;
  return {
    policy_family: policyFamily,
    triggered_fail_conditions: triggered,
    matched_blockers: matchedBlockers,
    explicit_sla_breach: explicitSlaBreach,
    explicit_handoff_break: explicitHandoffBreak,
    evaluation_result: shouldFail ? "fail" : "pass",
    violation_reasons: [
      ...matchedBlockers,
      ...(explicitSlaBreach ? ["sla_breach"] : []),
      ...(explicitHandoffBreak ? ["handoff_break"] : []),
    ],
  };
}

function makeCommerceContinuityBundle(descriptor, event, residualAssessment, committedState, transitions, policyDecision, runContext) {
  if (!isCommerceDescriptor(descriptor, event)) return null;

  const serviceStep = String(event?.service_step || event?.action || "deliver");
  const priorCommitment = String(event?.prior_commitment || stableHash({ target: descriptor.target, prior: runContext.runId }));
  const nextCommitment = String(event?.next_commitment || committedState.state_hash);
  const deliveryWitnessRef = stableHash({ target: descriptor.target, serviceStep, nextCommitment, at: runContext.timestamp });
  const chargeableUnits = Number(event?.chargeable_units ?? event?.usage_units ?? 1);
  const residualDelta = Number(residualAssessment?.composite_severity || 0);
  const usageCommitment = stableHash({ target: descriptor.target, usage: chargeableUnits, run: runContext.runId });
  const invoiceId = `${slugifyTarget(descriptor.target)}:${runContext.runId}:invoice`;
  const proofBundleRef = stableHash({ invoiceId, usageCommitment, deliveryWitnessRef, policy: policyDecision.decision });
  const finalTransition = transitions[transitions.length - 1];
  const chargeabilityEvaluation = evaluateCommerceChargeability(policyDecision, event);

  const fulfillmentTransition = {
    schema_version: "aurekai.fulfillment_transition.v1",
    transition_id: `${slugifyTarget(descriptor.target)}:${runContext.runId}:fulfillment`,
    service_step: serviceStep,
    prior_commitment: priorCommitment,
    next_commitment: nextCommitment,
    delivery_witness: deliveryWitnessRef,
    residual_delta: residualDelta,
    chargeable_units: Math.max(0, chargeableUnits),
    continuity_verdict: policyDecision.decision,
  };

  const deliveryWitness = {
    schema_version: "aurekai.delivery_witness.v1",
    witness_id: `${slugifyTarget(descriptor.target)}:${runContext.runId}:delivery-witness`,
    service_step: serviceStep,
    artifact_refs: [
      committedState.state_hash,
      finalTransition?.witness_hash || committedState.state_hash,
    ],
    commitment_ref: nextCommitment,
    issued_at: runContext.timestamp,
    signature_ref: `sig:${deliveryWitnessRef.slice(0, 16)}`,
  };

  const continuityBudget = Number(event?.continuity_budget ?? 1);
  const slaContinuity = {
    schema_version: "aurekai.sla.continuity.v1",
    service_id: `${slugifyTarget(descriptor.target)}:service`,
    continuity_budget: Math.max(0, continuityBudget),
    deformation_thresholds: {
      gate: Number(policyDecision?.thresholds?.gate_at ?? 0.7),
      halt: Number(policyDecision?.thresholds?.halt_at ?? 0.9),
    },
    handoff_rules: [
      "delivery_witness_required",
      "usage_commitment_required",
      "policy_decision_must_not_halt",
      "policy_fail_conditions_must_not_include_delivery_or_handoff_break",
    ],
    violation_proofs: chargeabilityEvaluation.evaluation_result === "fail"
      ? [deliveryWitnessRef, ...chargeabilityEvaluation.violation_reasons]
      : [],
  };

  const invoiceProof = {
    schema_version: "aurekai.invoice.proof.v1",
    invoice_id: invoiceId,
    usage_commitments: [usageCommitment],
    fulfillment_refs: [fulfillmentTransition.transition_id],
    meter_refs: [stableHash({ invoiceId, meter: "continuity-meter" })],
    continuity_summary: {
      decision: policyDecision.decision,
      confidence: policyDecision.confidence,
      residual_taxonomy: residualAssessment?.taxonomy || "unknown",
      residual_profile: residualAssessment?.profile_id || "generic",
      policy_family: chargeabilityEvaluation.policy_family,
      triggered_fail_conditions: chargeabilityEvaluation.triggered_fail_conditions,
      matched_blockers: chargeabilityEvaluation.matched_blockers,
      chargeability_result: chargeabilityEvaluation.evaluation_result,
    },
    proof_bundle_ref: proofBundleRef,
  };

  const chargeabilityClaim = {
    schema_version: "aurekai.chargeability_claim.v1",
    claim_id: `${slugifyTarget(descriptor.target)}:${runContext.runId}:chargeability`,
    invoice_id: invoiceId,
    usage_commitment_ref: usageCommitment,
    fulfillment_ref: fulfillmentTransition.transition_id,
    predicate: `continuity.verdict != halt && policy.blockers == none && usage.units >= 0`,
    proof_ref: proofBundleRef,
    evaluation_result: chargeabilityEvaluation.evaluation_result,
  };

  return {
    fulfillment_transition: fulfillmentTransition,
    delivery_witness: deliveryWitness,
    sla_continuity: slaContinuity,
    invoice_proof: invoiceProof,
    chargeability_claim: chargeabilityClaim,
  };
}

function makeFunctionalClaims(descriptor, event, residualAssessment, committedState, policyDecision, trajectory, commerceContinuity, runContext) {
  const threshold = Number(policyDecision?.thresholds?.gate_at ?? 0.7);
  const evaluation = policyDecision?.decision === "allow";
  const predicate = `continuity.calibrated_severity <= ${threshold.toFixed(4)}`;

  const claims = [
    {
      schema_version: "aurekai.functional_claim.v1",
      claim_id: `${slugifyTarget(descriptor.target)}:${runContext.runId}:continuity-claim`,
      claim_type: "continuity_bound",
      predicate,
      target_commitment: committedState.state_hash,
      proof_ref: trajectory.folded_witness,
      opening_policy: descriptor.surface === "hard-fork" ? "minimal-public" : "operator-private",
      evaluation_result: evaluation ? "pass" : "fail",
      context: {
        policy_family: inferPolicyFamily(descriptor.category, descriptor.target, descriptor.surface),
        chart_family: descriptor.chartFamily,
        decision: policyDecision.decision,
        residual_taxonomy: residualAssessment?.taxonomy || "unknown",
        residual_profile: residualAssessment?.profile_id || "generic",
      },
    },
  ];

  if (commerceContinuity) {
    claims.push({
      schema_version: "aurekai.functional_claim.v1",
      claim_id: `${slugifyTarget(descriptor.target)}:${runContext.runId}:invoice-backing-claim`,
      claim_type: "invoice_backed_by_usage_state",
      predicate: "invoice.usage_commitments.length > 0 && chargeability.evaluation_result == pass",
      target_commitment: committedState.state_hash,
      proof_ref: commerceContinuity.invoice_proof.proof_bundle_ref,
      opening_policy: "operator-private",
      evaluation_result: commerceContinuity.chargeability_claim.evaluation_result === "pass" ? "pass" : "fail",
      context: {
        invoice_id: commerceContinuity.invoice_proof.invoice_id,
        chargeability_claim_id: commerceContinuity.chargeability_claim.claim_id,
        fulfillment_ref: commerceContinuity.fulfillment_transition.transition_id,
      },
    });
  }

  const specialized = makeSpecializedClaims(descriptor, event, committedState, policyDecision, trajectory, runContext);
  claims.push(...specialized);

  return claims;
}

function makeDeformationMonitor(descriptor, event, residualAssessment, transitions) {
  const profile = buildProtocolProfile(descriptor);
  const drift = Number(event?.deformation_score || 0);
  const witnessSkew = Number(event?.witness_skew || 0);
  const continuityGap = Number(event?.continuity_gap || 0);
  const severity = Number(residualAssessment?.composite_severity || Math.max(drift, witnessSkew));

  return {
    schema_version: "aurekai.deformation_monitor.v1",
    target: descriptor.target,
    surface: descriptor.surface,
    severity,
    status: severity >= 0.8 ? "critical" : severity >= 0.6 ? "warning" : "normal",
    indicators: {
      deformation_score: drift,
      witness_skew: witnessSkew,
      continuity_gap: continuityGap,
      calibrated_deformation_score: residualAssessment?.weighted?.deformation_score || 0,
      calibrated_witness_skew: residualAssessment?.weighted?.witness_skew || 0,
      calibrated_continuity_gap: residualAssessment?.weighted?.continuity_gap || 0,
      calibrated_risk_score: residualAssessment?.weighted?.risk_score || 0,
      residual_taxonomy: residualAssessment?.taxonomy || "unknown",
      residual_semantics: residualAssessment?.semantics || "generalized deformation",
      residual_profile: residualAssessment?.profile_id || "generic",
      transition_count: transitions.length,
      monitor_signals: profile.monitor_signals,
    },
  };
}

function persistExecution(repoRoot, descriptor, envelope) {
  const outputDir = join(repoRoot, "output", "integration-runs");
  if (!existsSync(outputDir)) mkdirSync(outputDir, { recursive: true });

  const targetSlug = slugifyTarget(descriptor.target);
  const outPath = join(outputDir, `${targetSlug}.jsonl`);
  const row = `${JSON.stringify(envelope)}\n`;
  writeFileSync(outPath, row, { encoding: "utf8", flag: "a" });

  return outPath;
}

function makeRunContext(runId) {
  return {
    runId,
    timestamp: nowIso(),
  };
}

function resolveRunId(target, event) {
  if (event?.run_id) return String(event.run_id);
  const source = {
    target,
    event_id: event?.event_id || null,
    nonce: event?.nonce || nowIso(),
  };
  return stableHash(source).slice(0, 16);
}

export function listIntegrationTargets(repoRoot, options = {}) {
  const registry = readRegistry(repoRoot);
  const index = buildTargetIndex(registry);
  const rows = [];

  for (const descriptor of index.values()) {
    rows.push({
      target: descriptor.target,
      surface: descriptor.surface,
      category: descriptor.category,
      runtime_depth: descriptor.runtimeDepth,
      ingress_depth: descriptor.ingressDepth,
      trust_level: descriptor.trustLevel,
      required_kernel_layers: descriptor.requiredKernelLayers,
      chart_family: descriptor.chartFamily,
      state_machine_types: descriptor.stateMachineTypes,
    });
  }

  const dedup = new Map();
  for (const row of rows) {
    const key = `${row.target}:${row.surface}:${row.category}`;
    if (!dedup.has(key)) dedup.set(key, row);
  }

  let result = Array.from(dedup.values()).sort((a, b) => a.target.localeCompare(b.target));
  if (options.surface) result = result.filter((row) => row.surface === options.surface);
  if (options.group) result = result.filter((row) => row.category === options.group);

  return result;
}

function executeWithDescriptor(repoRoot, descriptor, input) {
  const event = input.event || {};
  const runId = resolveRunId(descriptor.target, event);
  const runContext = makeRunContext(runId);
  const profile = buildProtocolProfile(descriptor);
  const mutationBoundary = resolveMutationBoundary(repoRoot, descriptor);
  const residualAssessment = calibrateResidual(repoRoot, {
    chartFamily: descriptor.chartFamily,
    surface: descriptor.surface,
    target: descriptor.target,
    event,
  });

  const committedState = makeCommittedState(descriptor, event, residualAssessment, mutationBoundary, runContext);
  const { transitions, witnessHash } = makeTransitions(descriptor, event, committedState, mutationBoundary, runContext);
  const policyDecision = makeContinuityPolicy(repoRoot, descriptor, event, residualAssessment, committedState, witnessHash);
  const lineageEdges = makeLineageEdges(descriptor, event, committedState, transitions, mutationBoundary, runContext);
  const trajectory = makeTrajectory(descriptor, transitions, lineageEdges, mutationBoundary, runContext);
  const transportSemantics = makeTransportSemantics(descriptor, event, mutationBoundary, policyDecision, transitions, runContext);
  const commerceContinuity = makeCommerceContinuityBundle(
    descriptor,
    event,
    residualAssessment,
    committedState,
    transitions,
    policyDecision,
    runContext,
  );
  const functionalClaims = makeFunctionalClaims(
    descriptor,
    event,
    residualAssessment,
    committedState,
    policyDecision,
    trajectory,
    commerceContinuity,
    runContext,
  );
  const exportProjections = makeExportProjections(
    descriptor,
    event,
    committedState,
    transitions,
    lineageEdges,
    policyDecision,
    transportSemantics,
    runContext,
  );
  const deformationMonitor = makeDeformationMonitor(descriptor, event, residualAssessment, transitions);
  const doctrineReport = buildIngressDoctrineReport(descriptor, profile, mutationBoundary, policyDecision, {
    committedState,
    transitions,
    lineageEdges,
    exportProjections,
    functionalClaims,
  });

  if (input.enforceDoctrine && !doctrineReport.compliant) {
    throw new Error(`Ingress doctrine enforcement failed for ${descriptor.target}: ${doctrineReport.failed_checks.join(", ")}`);
  }

  const envelope = {
    schema_version: "aurekai.integration_execution.v1",
    run_id: runContext.runId,
    timestamp: runContext.timestamp,
    target: descriptor.target,
    surface: descriptor.surface,
    category: descriptor.category,
    runtime_depth: descriptor.runtimeDepth,
    state_machine_types: descriptor.stateMachineTypes,
    committed_state: committedState,
    transitions,
    policies: [policyDecision],
    lineage_edges: lineageEdges,
    trajectory,
    functional_claims: functionalClaims,
    export_projections: exportProjections,
    deformation_monitor: deformationMonitor,
    ingress_depth: descriptor.ingressDepth,
    trust_level: descriptor.trustLevel,
    required_kernel_layers: descriptor.requiredKernelLayers,
    chart_family: descriptor.chartFamily,
    residual_assessment: residualAssessment,
    mutation_boundary: mutationBoundary,
    transport_semantics: transportSemantics,
    commerce_continuity: commerceContinuity,
    ingress_doctrine: doctrineReport,
  };

  let persistedTo = null;
  if (input.persist) {
    persistedTo = persistExecution(repoRoot, descriptor, envelope);
  }

  return {
    envelope,
    persistedTo,
  };
}

export function executeIntegration(repoRoot, input) {
  const registry = readRegistry(repoRoot);
  const index = buildTargetIndex(registry);
  const key = normalizeKey(input.target);
  const descriptor = index.get(key);

  if (!descriptor) {
    const known = listIntegrationTargets(repoRoot).map((entry) => entry.target);
    throw new Error(`Unknown integration target: ${input.target}. Known targets: ${known.join(", ")}`);
  }

  return executeWithDescriptor(repoRoot, descriptor, input);
}

export function executeSurfaceBatch(repoRoot, input) {
  const targets = listIntegrationTargets(repoRoot, { surface: input.surface });
  const executions = [];
  for (const item of targets) {
    const result = executeIntegration(repoRoot, {
      target: item.target,
      event: input.event,
      persist: input.persist,
      enforceDoctrine: input.enforceDoctrine,
    });
    executions.push({
      target: item.target,
      surface: item.surface,
      category: item.category,
      run_id: result.envelope.run_id,
      committed_state_hash: result.envelope.committed_state.state_hash,
      policy_decision: result.envelope.policies[0].decision,
      persisted_to: result.persistedTo,
    });
  }

  return {
    schema_version: "aurekai.integration_batch_execution.v1",
    surface: input.surface,
    count: executions.length,
    executions,
  };
}

export function executeStateMachineBatch(repoRoot, input) {
  const registry = readRegistry(repoRoot);
  const groups = registry.state_machine_integrations || {};
  const type = input.type;

  if (!type) {
    throw new Error("Missing state-machine type.");
  }

  const targets = groups[type];
  if (!Array.isArray(targets)) {
    throw new Error(`Unknown state-machine type: ${type}`);
  }

  const index = buildTargetIndex(registry);
  const executions = [];

  for (const target of targets) {
    const descriptor = index.get(normalizeKey(target)) || {
      target,
      surface: "state-machine",
      category: type,
      runtimeDepth: "continuity-native",
      ingressDepth: inferIngressDepth("state-machine"),
      trustLevel: inferTrustLevel("state-machine"),
      requiredKernelLayers: inferRequiredKernelLayers("state-machine"),
      chartFamily: inferChartFamily(type, target, "state-machine"),
      stateMachineTypes: [type],
    };

    const result = executeWithDescriptor(repoRoot, descriptor, {
      event: input.event,
      persist: input.persist,
      enforceDoctrine: input.enforceDoctrine,
    });

    executions.push({
      target: descriptor.target,
      surface: descriptor.surface,
      category: descriptor.category,
      run_id: result.envelope.run_id,
      committed_state_hash: result.envelope.committed_state.state_hash,
      policy_decision: result.envelope.policies[0].decision,
      persisted_to: result.persistedTo,
    });
  }

  return {
    schema_version: "aurekai.state_machine_batch_execution.v1",
    state_machine_type: type,
    count: executions.length,
    executions,
  };
}
