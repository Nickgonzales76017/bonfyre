protocolObservation: {
  schema: "bonfyre-protocol-binding.v1"
  domain_schema: "bonfyre-domain-binding.v1"
  native_schema: "bonfyre-protocol-observation-bridge.v1"
  domain_owner: "DomainProfile"
  protocol_owner: "ProtocolContract"
  contract: "ogc-api-common-observation"
  programs: 3
  cannot_infer: [
    "compiled contract -> OGC conformance",
    "compiled contract -> enacted request",
    "compiled contract -> provider admission, authority or effect",
  ]
}
