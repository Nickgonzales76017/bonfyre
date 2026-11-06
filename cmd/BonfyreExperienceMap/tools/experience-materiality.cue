package experience

experienceMateriality: {
  schema: "bonfyre.experience-materiality.v1"
  truth_level: "compiled_diagnostic_projection"
  role_classes: {
    material: ["material_source", "media_service"]
    action: ["action_adapter", "authoring_adapter"]
    interaction_reference: ["interaction_reference"]
    design_reference: ["design_reference"]
    compiler: ["compiler_toolchain"]
    carrier: ["transport_carrier", "publication_carrier"]
    verification: ["verification_tool"]
    service: ["external_service_candidate"]
    standard: ["reference_standard"]
  }
  cannot_infer: [
    "materiality score -> source admission",
    "chain fit -> semantic identity",
    "protocol specificity -> authority or permission",
    "market reference -> copied ontology, UI, code, or product ownership",
    "reagent role -> provider availability",
  ]
}
