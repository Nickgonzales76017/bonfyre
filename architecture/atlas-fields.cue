package atlas

// The parser's field contract. Unknown keys remain visible in `fields` rather
// than disappearing, while aliases preserve authored meaning.
list_fields: [
  "node_types", "edge_types", "state_types", "internal_calculus", "child",
  "invariant", "forbidden_inference", "interacts", "computational_form",
  "native_format", "command", "app", "service", "external_binding", "surface",
  "source_path", "witness", "known_gap", "cooling_path", "reheat_condition",
  "source_lineage", "historical_name", "supersedes", "superseded_by",
  "preserves", "drops", "authority", "evidence", "invalidates_on", "binding",
  "expands_to", "consumes", "produces", "placement_constraint", "specialized_by",
  "owns", "publishes", "subscribes", "derived",
  "experience_chain",
]
scalar_fields: [
  "canonical_name", "family", "domain", "layer", "semantic_question", "purpose",
  "maturity", "relation", "source", "destination", "roundtrip_probe",
  "generation", "authority", "absorbs", "statement", "contract",
  "native_header", "native_source", "schema", "record", "field",
]
aliases: cannot_infer: "forbidden_inference"
maturity: ["architectural", "declared", "implemented", "measured", "proven"]
block_kinds: ["architecture", "view", "interaction", "phenotype", "edge_class"]
