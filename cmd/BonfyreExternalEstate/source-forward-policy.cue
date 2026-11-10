sourceForwardPolicy: {
  schema: "bonfyre.source-forward-policy.v1"
  benefit_weights: {
    subject_reuse: 1
    scene_reuse: 1.5
    predicate_reuse: 1.25
    action_unlock: 2
    calculation_simplification: 2
    freshness_need: 0.75
    economic_potential: 1
  }
  cost_weights: {
    acquisition_cost: 1
    maintenance_cost: 1
    rights_risk: 1.5
    fragility: 1
  }
  measurement_fields: ["candidate_branches_before", "candidate_branches_after", "queries_avoided"]
  relationship_classes: ["AFFILIATE", "PARTNER", "OUTREACH"]
  identity_disclosure_classes: ["OUTREACH"]
  truth: "a measured closure dividend or available resolution class does not authorize acquisition, contact, provider selection, payment or effect"
}
