sourceAttentionPolicy: {
  schema: "bonfyre.source-attention-policy.v1"
  hot_pressure_closures: 8
  hot_pressure_occurrences: 32
  hot_impact_closures: 4
  hot_impact_scenes: 4
  truth: "attention_projection_only_not_admission_fetch_authority_or_value"
  sync: {
    plan_schema: "bonfyre.source-attention-plan.v1"
    output_schema: "bonfyre.source-attention-sync.v1"
    temperatures: ["hot", "warm", "cool", "cold"]
    atom_pattern: "^[!-~]+$"
    detail_prefix: "SourceFabric"
    truth: "internal_attention_projection_only_not_fetch_admission_authority_or_effect"
    cannot_infer: [
      "attention watch -> permission to observe",
      "hot -> semantic importance or source quality",
      "scheduled watch -> acquisition, payment, contact or external effect",
    ]
  }
}
