sourceStarvationPolicy: {
  schema: "bonfyre.source-starvation-policy.v1"
  pressure_threshold: 4
  severity_order: ["high", "medium", "low"]
  diagnostic_truth: "engineering_diagnostic_not_semantic_failure_verdict"
  scene_truth: "scene_dependency_diagnostic_not_material_failure_verdict"
  summary_truth: "engineering_diagnostic_not_semantic_failure_verdict"
  cannot_infer: [
    "no Scene edge -> source has no value",
    "candidate pressure -> source should be admitted",
    "open gap -> permission to acquire/contact/pay/combine",
    "diagnostic severity -> semantic rank or provider preference",
  ]
  rules: [
    {
      code: "ADMITTED_NO_CLOSURE_IMPACT"
      severity: "high"
      all: [{field: "admitted", op: "gt", value: 0}, {field: "scene_edges", op: "eq", value: 0}, {field: "impact_closures", op: "eq", value: 0}]
      evidence: {admitted_channels: "admitted", active_channels: "active"}
      investigate: "is demand binding, owner lowering, identity joining, or Scene dependency propagation missing?"
    },
    {
      code: "PRESSURED_WITHOUT_OBSERVED_IMPACT"
      severity: "high"
      all: [{field: "candidate_occurrences", op: "gte", compare_field: "pressure_threshold"}, {field: "impact_closures", op: "eq", value: 0}]
      evidence: {candidate_occurrences: "candidate_occurrences", candidate_closure_reuse: "candidate_closure_reuse"}
      investigate: "why does repeated ScopedDemand survival not reach a ForeignTwin-backed reverse dependency?"
    },
    {
      code: "CLOSURE_IMPACT_WITHOUT_SCENE_EDGE"
      severity: "high"
      all: [{field: "impact_closures", op: "gt", value: 0}, {field: "scene_edges", op: "eq", value: 0}]
      evidence: {affected_closures: "impact_closures", affected_subjects: "impact_subjects"}
      investigate: "are affected closures absent from persisted SurfaceScene or failing Scene dependency compilation?"
    },
    {
      code: "SCENE_DEPENDS_ON_NONACTIVE_ROOT"
      severity: "medium"
      all: [{field: "scene_edges", op: "gt", value: 0}, {field: "active", op: "eq", value: 0}]
      evidence: {scene_edges: "scene_edges", admitted_channels: "admitted"}
      investigate: "is this cold historical material, a candidate-only path, or stale admission state?"
    },
    {
      code: "IMPACTFUL_ROOT_HAS_OPEN_GAPS"
      severity: "medium"
      all: [{field: "open_gaps", op: "gt", value: 0}]
      any: [{field: "impact_closures", op: "gt", value: 0}, {field: "scene_edges", op: "gt", value: 0}]
      evidence: {gap_kinds: "gap_kinds", affected_closures: "impact_closures", scene_edges: "scene_edges"}
      investigate: "which gap closure most reduces uncertainty or unlocks an exact capability for already-dependent Scenes?"
    },
    {
      code: "HOT_FROM_PRESSURE_ONLY"
      severity: "medium"
      all: [{field: "attention_temperature", op: "eq", value: "hot"}, {field: "impact_closures", op: "eq", value: 0}]
      evidence: {attention_basis: "attention_basis", candidate_occurrences: "candidate_occurrences"}
      investigate: "should this stay hot until bounded exact-owner observation proves reuse, then cool if it does not?"
    },
    {
      code: "ACTIVE_BUT_UNREFERENCED"
      severity: "low"
      all: [{field: "active", op: "gt", value: 0}, {field: "candidate_occurrences", op: "eq", value: 0}, {field: "impact_closures", op: "eq", value: 0}, {field: "scene_edges", op: "eq", value: 0}]
      evidence: {active_channels: "active"}
      investigate: "is this intentionally latent, or has no Pack/World/Scene learned how to ask for it?"
    },
  ]
}
