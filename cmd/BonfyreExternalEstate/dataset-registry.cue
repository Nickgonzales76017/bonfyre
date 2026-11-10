datasetRegistry: {
  agent: "BonfyreProject/1.0"
  probe_timeout_ms: 15000
  robots_max_bytes: 512000
  host_pace_ms: 250
  registry_name: "awesome-public-datasets"
  passport: {
    roles: ["REFERENCE"]
    topology: ["CORPUS"]
    identifiers: ["url"]
    spatial: "global"
    temporal: "snapshot"
    refresh: "irregular"
    revision: "none"
    capabilities: ["supply-dataset"]
    reuse: {
      allows: ["observe"]
      requires: []
      unknown: ["retain", "display_metadata", "redistribute", "transform", "commercial_use"]
    }
    scale: "unmeasured"
    mirror: "none"
    evidence: "claimed"
    cannot_infer: "a listed dataset is a pointer somebody curated, not a reachable, lawful or current source"
  }
  candidate_verifier: "SourcePassport:registry-candidate"
  candidate_lowerer: "SourcePassport:unlowered"
  catalogue_cannot_infer: "a catalogued dataset is a pointer; nothing here says it was read, is reachable, or may be republished"
  cannot_infer: [
    "a registry entry -> a fetched body, lawful reuse or admission",
    "a robots answer -> a provider relationship or effect authority",
  ]
}
