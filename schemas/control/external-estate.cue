// External estate descriptors classify observed sources for composition. They
// are not provider admissions, credentials, liveness checks, or authority.
package control

#ExternalAffordance: "LOCATE" | "IDENTIFY" | "OBSERVE" | "MEASURE" | "VERIFY" |
    "PREDICT" | "COMPARE" | "DISCOVER" | "MATCH" | "ROUTE" | "SCHEDULE" |
    "RESERVE" | "QUOTE" | "PURCHASE" | "PAY" | "SETTLE" | "INSURE" |
    "FINANCE" | "SHIP" | "STORE" | "CONTROL" | "COMMUNICATE" | "TRANSLATE" |
    "PUBLISH" | "STREAM" | "AUTHENTICATE" | "AUTHORIZE" | "SIGN" | "CERTIFY" |
    "FILE" | "REPORT" | "NOTIFY" | "ALERT" | "SIMULATE" | "OPTIMIZE" |
    "REPAIR" | "INSPECT" | "CLASSIFY" | "SEARCH" | "NEGOTIATE" | "CONTRACT" |
    "HIRE" | "TEACH" | "CARE"

#ExternalDescriptor: {
    id: string & != ""
    observedName: string & != ""
    sourceRef: string & != ""
    primaryURL: =~"^https?://"
    sourceKind: "rss" | "public_api" | "x402" | "mcp" | "machine_protocol" | "x402_spec"
    estateRoles: ["material_source" | "media_service" | "action_adapter" | "interaction_reference" | "design_reference" | "authoring_adapter" | "compiler_toolchain" | "transport_carrier" | "publication_carrier" | "verification_tool" | "external_service_candidate" | "reference_standard", ...string]
    topicDomains: [...string]
    domains: [string & != "", string & != "", ...string]
    semanticOutputs: [string & != "", ...string]
    affordances: [#ExternalAffordance, ...#ExternalAffordance]
    carrier: "http" | "feed" | "mcp" | "a2a" | "x402" | "protocol" | "file" | "unknown"
    economics: "unknown" | "free" | "paid" | "quote" | "settlement"
    temporal: "static" | "historic" | "live" | "forecast" | "mixed" | "unknown"
    spatial: "global" | "regional" | "local" | "asset" | "mixed" | "unknown"
    actors: [string & != "", ...string]
    worldPhases: ["discover" | "plan" | "execute" | "observe" | "settle" | "remember", ...string]
    interactionFits: ["stage" | "board" | "thread" | "spatial" | "market" | "work" | "discover" | "inspector", ...string]
    truthLevel: "observed_candidate"
    limit: string & != ""
}
