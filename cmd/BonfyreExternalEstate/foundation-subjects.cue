foundationSubjects: {
  schema: "bonfyre.foundation-subject-fabric.v1"
  territories: ["life", "live", "shop", "work"]
  subject_prefix: "institution:aurekai:"
  scope: "public"
  basis: "source_frontier_admitted_estate_closure"
  reader_states: ["admitted", "active"]
  required_tables: ["subject_bundle", "subject_reader"]
  derived_tables: [
    "subject_route_item", "subject_route_region", "subject_route_reader",
    "subject_route_phase", "subject_route", "subject_material_member",
    "subject_material", "subject_query", "subject_scene", "subject_action",
    "subject_dependency", "subject_root", "subject_candidate", "subject_reader",
    "subject_bundle",
  ]
  truth: "derived_public_reader_closure_over_admitted_source_frontier"
  cannot_infer: [
    "admitted source -> current source body or freshness",
    "foundation reader membership -> route visibility, ranking, endorsement or truth",
    "derived public closure -> authority, payment, private access or external effect",
  ]
}
