import { readFileSync } from "node:fs";
import { join } from "node:path";

function toNumber(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function getPath(payload, path) {
  const keys = String(path || "").split(".").filter(Boolean);
  let cur = payload;
  for (const key of keys) {
    if (!cur || typeof cur !== "object" || !(key in cur)) return undefined;
    cur = cur[key];
  }
  return cur;
}

function readPolicyFamilies(repoRoot) {
  const path = join(repoRoot, "registry", "continuity-policy-families.json");
  try {
    const parsed = JSON.parse(readFileSync(path, "utf8"));
    return Array.isArray(parsed?.families) ? parsed.families : [];
  } catch {
    return [];
  }
}

function evaluateFailCondition(condition, event, thresholds, residual) {
  if (condition === "missing_ack") {
    return event?.ack === false || event?.acknowledged === false || event?.payload?.ack === false;
  }
  if (condition === "missing_settlement_ack") {
    return event?.settlement_ack === false || event?.settled === false;
  }
  if (condition === "retransmission_limit_exceeded") {
    return toNumber(event?.retransmit_count, 0) > toNumber(event?.retransmit_limit, 3);
  }
  if (condition === "residual_delta_exceeded") {
    return toNumber(residual?.composite_severity, toNumber(event?.deformation_score, 0)) > toNumber(thresholds?.halt, 1);
  }
  if (condition === "non_final_state") {
    const status = String(event?.status || event?.finality_status || "").toLowerCase();
    return event?.finalized === false || status === "pending" || status === "unfinalized";
  }
  if (condition === "stale_round") {
    const age = toNumber(event?.round_age_s, toNumber(getPath(event, "payload.round.age_s"), 0));
    return age > 120;
  }
  if (condition === "attestation_gap") {
    return event?.attested === false || event?.attestation_ok === false;
  }
  if (condition === "retrieval_failure") {
    return event?.retrieval_ok === false || event?.retrieved === false;
  }
  if (condition === "root_mismatch") {
    return event?.root_match === false || event?.commitment_match === false;
  }
  if (condition === "liquidity_gap") {
    return toNumber(event?.liquidity_gap, 0) > 0;
  }
  if (condition === "unsafe_slippage") {
    return toNumber(event?.slippage_bps, 0) > 100;
  }
  if (condition === "collateral_ratio_breach") {
    const min = toNumber(event?.min_collateral_ratio, 1);
    return toNumber(event?.collateral_ratio, min) < min;
  }
  if (condition === "liquidation_boundary_cross") {
    return event?.liquidation_boundary_cross === true;
  }
  if (condition === "slash_event_unbound") {
    return event?.slash_event_unbound === true;
  }
  if (condition === "delegation_gap") {
    return event?.delegation_gap === true;
  }
  if (condition === "quorum_loss") {
    return event?.quorum_met === false;
  }
  if (condition === "invalid_vote_transition") {
    return event?.vote_transition_valid === false;
  }
  if (condition === "claim_invalid") {
    return event?.claim_valid === false;
  }
  if (condition === "privacy_budget_exceeded") {
    return event?.privacy_budget_exceeded === true;
  }
  if (condition === "delivery_gap") {
    return event?.delivery_gap === true;
  }
  if (condition === "handoff_break") {
    return event?.handoff_break === true;
  }
  if (condition === "step_policy_break") {
    return event?.step_policy_break === true;
  }
  if (condition === "unhandled_branch") {
    return event?.unhandled_branch === true;
  }
  if (condition === "identity_claim_break") {
    return event?.identity_claim_break === true;
  }
  return false;
}

export function evaluatePolicyFamily(repoRoot, input) {
  const families = readPolicyFamilies(repoRoot);
  const family = families.find((item) => item?.id === input.policyFamily) || {
    id: input.policyFamily || "default",
    residual_thresholds: { gate: 0.7, halt: 0.9 },
    fail_conditions: [],
    witness_requirements: ["transition_witness"],
    projection_defaults: ["public_summary"],
    replay_expectations: ["lineage_unfold"],
    allowed_chart_transitions: [],
  };

  const event = input.event || {};
  const thresholds = {
    gate: toNumber(family?.residual_thresholds?.gate, 0.7),
    halt: toNumber(family?.residual_thresholds?.halt, 0.9),
  };

  const chartTransition = event?.chart_transition || `${input.chartFamily || "generic"}->${input.chartFamily || "generic"}`;
  const allowedTransitions = Array.isArray(family.allowed_chart_transitions) ? family.allowed_chart_transitions : [];
  const chartTransitionValid = allowedTransitions.length === 0 || allowedTransitions.includes(chartTransition);

  const residual = input.residual || null;
  const failConditions = Array.isArray(family.fail_conditions) ? family.fail_conditions : [];
  const triggered = failConditions.filter((condition) => evaluateFailCondition(condition, event, thresholds, residual));
  if (!chartTransitionValid) triggered.push("chart_transition_disallowed");

  const severity = toNumber(
    residual?.composite_severity,
    Math.max(
      toNumber(event?.risk_score, 0),
      toNumber(event?.deformation_score, 0),
      toNumber(event?.witness_skew, 0),
      toNumber(event?.continuity_gap, 0),
    ),
  );

  let decision = "allow";
  if (triggered.length > 0) decision = "halt";
  else if (severity >= thresholds.halt) decision = "halt";
  else if (severity >= thresholds.gate) decision = "gate";

  const confidence = decision === "allow" ? 0.92 : decision === "gate" ? 0.95 : 0.98;
  const specialization = family.id === "settlement"
    ? "transport-ack"
    : family.id === "oracle"
      ? "oracle-round"
      : family.id === "collateral"
        ? "collateral-health"
        : family.id === "workflow"
          ? "workflow-branch"
          : "general";
  const reasonCodes = [
    `policy_family:${family.id}`,
    `policy_specialization:${specialization}`,
    `chart_transition:${chartTransitionValid ? "allowed" : "disallowed"}`,
    residual ? `residual_taxonomy:${residual.taxonomy}` : "residual_taxonomy:unavailable",
    residual ? `residual_profile:${residual.profile_id}` : "residual_profile:unavailable",
    triggered.length ? `fail_conditions:${triggered.join(",")}` : "fail_conditions:none",
    `severity:${severity.toFixed(4)}`,
  ];

  return {
    family_id: family.id,
    decision,
    confidence,
    reason_codes: reasonCodes,
    thresholds,
    triggered_fail_conditions: triggered,
    chart_transition: chartTransition,
    chart_transition_valid: chartTransitionValid,
    witness_requirements: Array.isArray(family.witness_requirements) ? family.witness_requirements : [],
    projection_defaults: Array.isArray(family.projection_defaults) ? family.projection_defaults : [],
    replay_expectations: Array.isArray(family.replay_expectations) ? family.replay_expectations : [],
    residual,
  };
}
