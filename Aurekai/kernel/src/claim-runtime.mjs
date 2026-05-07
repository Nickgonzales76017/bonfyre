export function createFunctionalClaim(input = {}) {
  return {
    schema_version: "aurekai.functional_claim.v1",
    claim_id: input.claim_id || "claim:unknown",
    claim_type: input.claim_type || "continuity_bound",
    predicate: input.predicate || "true",
    target_commitment: input.target_commitment || "",
    proof_ref: input.proof_ref || "",
    opening_policy: input.opening_policy || "operator-private",
    evaluation_result: input.evaluation_result || "unknown",
    context: input.context || {},
  };
}

export function createContinuityClaim(input = {}) {
  const threshold = Number(input.threshold ?? 0.7);
  const value = Number(input.value ?? 0);
  return createFunctionalClaim({
    ...input,
    claim_type: "continuity_bound",
    predicate: `continuity.calibrated_severity <= ${threshold.toFixed(4)}`,
    evaluation_result: value <= threshold ? "pass" : "fail",
  });
}

export function createInvoiceBackingClaim(input = {}) {
  const usageCount = Number(input.usage_commitment_count ?? 0);
  const chargeability = String(input.chargeability_result || "unknown");
  return createFunctionalClaim({
    ...input,
    claim_type: "invoice_backed_by_usage_state",
    predicate: "invoice.usage_commitments.length > 0 && chargeability.evaluation_result == pass",
    evaluation_result: usageCount > 0 && chargeability === "pass" ? "pass" : "fail",
  });
}
