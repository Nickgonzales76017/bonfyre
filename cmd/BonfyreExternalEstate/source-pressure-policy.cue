sourcePressurePolicy: {
  schema: "bonfyre.source-pressure-policy.v1"
  identity_fields: ["id", "identity", "ext_id", "passport", "source", "source_ref", "publisher_ident", "qid"]
  external_prefix: "EXT-"
  external_alias_prefix: "external:"
  candidate_paths: [
    {basis: "pack_demand", parent: "pack_demands", group: "candidate_groups", leaf: "candidates", demand_id: "identity", source: "source"},
    {basis: "world_demand", parent: "world_demands", group: "dependencies", leaf: "candidates", demand_id: "identity", source: "dependency"},
  ]
  truth: "candidate_pressure_only_exact_owner_recheck_required"
  cannot_infer: [
    "candidate pressure -> source contribution",
    "candidate pressure -> admission, availability or freshness",
    "candidate pressure -> authority, rights or permission",
    "candidate pressure -> closure dividend or economic value",
  ]
}
