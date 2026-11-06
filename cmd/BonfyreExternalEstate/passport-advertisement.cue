passportAdvertisement: {
  candidate_kind: "external_source"
  identity_prefix: "external_source:passport:"
  owner_prefix: "SourcePassport:"
  census_prefix: "InternetSource:"
  admitted_state: "ADMITTED"
  admitted_verifier: "ForeignTwin:receipt"
  declared_verifier: "SourcePassport:declared"
  admitted_lowerer: "InternetSource:census"
  declared_lowerer: "SourcePassport:unlowered"
  term_arrays: [
    {path: "capabilities", prefix: "affordance:"},
    {path: "roles", prefix: "source_kind:"},
    {path: "access", prefix: "carrier:"},
    {path: "topology", prefix: "material:"},
    {path: "subject_kinds", prefix: "subject_kind:"},
    {path: "identifiers", prefix: "field:"},
    {path: "reuse.allows", prefix: "reuse:allows-"},
    {path: "reuse.unknown", prefix: "reuse:unknown-"},
    {path: "reuse.requires", prefix: "requirement:"},
  ]
  term_scalars: [
    {path: "passport", prefix: "source:"},
    {path: "state", prefix: "admission:"},
    {path: "refresh", prefix: "refresh:"},
    {path: "temporal", prefix: "temporal:"},
    {path: "spatial", prefix: "spatial:"},
    {path: "evidence", prefix: "evidence:"},
    {path: "mirror", prefix: "regime:mirror-"},
    {path: "carrier.robots", prefix: "regime:robots-", fallback: "unobserved"},
  ]
  credential_prefixes: ["requirement:credential-", "authority:credential-"]
  varies_per_item_term: "reuse:varies-per-item"
  cannot_infer: [
    "capability advertisement -> source admission or a working endpoint",
    "passport reuse posture -> legal permission or provider authority",
  ]
}
