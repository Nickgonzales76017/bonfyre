package observation

domainObservation: {
  schema: "bonfyre-domain-binding.v1"
  observation_schema: "bonfyre-foreign-observation.v1"
  parser: "html-metadata-v1"
  profile: "reference-standard"
  owner: "DomainProfile"
  fields: ["authority", "description", "name", "source_url", "updated_at"]
  evidence_requirements: ["foreign_observation_receipt", "ingest_artifact"]
  cannot_infer: [
    "profile binding -> external standard conformance",
    "semantic extraction -> source authority",
    "retained bytes -> provider admission or callable capability",
  ]
}
