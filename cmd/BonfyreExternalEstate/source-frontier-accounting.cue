sourceFrontierAccounting: {
  schema: "bonfyre.source-frontier.v1"
  truth_level: "compiled_accounting_not_admission"
  input_arrays: ["rows", "items", "data", "sources", "entries", "candidates"]
  root_identity_fields: ["publisher_ident", "source", "source_ref", "ext_id", "passport", "id", "observed_name", "name"]
  channel_identity_fields: ["source", "source_ref", "ext_id", "passport", "id", "observed_name", "name"]
  publisher_identity_fields: ["publisher_ident", "publisher_id", "qid", "source_identity", "institution"]
  family_fields: ["channel_family", "phenotype", "acquisition", "source_kind", "kind", "access"]
  state_fields: ["state", "admission", "truth_level", "status", "review_state", "policy_decision"]
  rights_fields: ["licence", "license", "rights", "terms_url"]
  credential_fields: ["credential_state", "credentials", "auth_state"]
  economic_fields: ["economic_interface", "economic_interfaces", "commercial", "relationship"]
  term_fields: ["terms", "capabilities", "roles", "access", "topology", "domains", "functions"]
  projection_term_fields: ["candidate_affordances", "candidate_domains", "candidate_worlds", "carrier_default_domains", "direct_domains", "estate_roles", "interaction_fits", "topic_domains", "world_phases"]
  credential_hints: ["oauth", "api-key", "apikey", "credential", "auth:"]
  transaction_hints: ["buy", "book", "reserve", "quote", "pay", "order", "ticket", "hold", "checkout"]
  structured_families: ["RSS", "ATOM", "RDF", "JSON_FEED", "SITEMAP", "OPML", "XML_DOCUMENT", "ICS", "GTFS", "GTFS_RT", "GBFS", "SPARQL", "JSON_API", "GRAPHQL", "OPENAPI", "ARCGIS_LAYER", "SOCRATA_DATASET", "OGC_COLLECTION", "STAC_CATALOG", "WARC_INDEX", "MEDIA_COLLECTION", "WEBHOOK", "WEBSUB", "ACTIVITYPUB", "MCP", "A2A", "BECKN", "X402"]
  active_states: ["ACTIVE", "HOT", "WARM", "LIVE"]
  admitted_states: ["ACTIVE", "HOT", "WARM", "LIVE", "ADMITTED", "OBSERVED", "PUBLISHED"]
  refused_states: ["REFUSED", "BLOCKED", "DENIED", "REJECTED", "POLICY_EXCLUDED"]
  family_rules: [
    {family: "GTFS_RT", terms: ["gtfs-rt", "gtfs_rt", "tripupdates", "vehiclepositions"]},
    {family: "GTFS", terms: ["gtfs"]}, {family: "GBFS", terms: ["gbfs"]},
    {family: "JSON_FEED", terms: ["jsonfeed", "json_feed", "feed.json"]},
    {family: "ATOM", terms: ["atom", "atom.xml", "application/atom+xml"]},
    {family: "RDF", terms: ["application/rdf+xml", "/feed/rdf", "rdf.xml"]},
    {family: "RSS", terms: ["rss", ".rss", "rss.xml", "/feed/", "application/rss+xml"]},
    {family: "OPML", terms: ["opml", ".opml"]},
    {family: "SITEMAP", terms: ["sitemap", "wp-sitemap"]},
    {family: "XML_DOCUMENT", terms: ["application/xml", "text/xml", "xml_document"]},
    {family: "ICS", terms: [".ics", "ical", "calendar"]},
    {family: "SPARQL", terms: ["sparql"]}, {family: "GRAPHQL", terms: ["graphql"]},
    {family: "OPENAPI", terms: ["openapi", "swagger"]},
    {family: "SOCRATA_DATASET", terms: ["socrata", "soda"]},
    {family: "ARCGIS_LAYER", terms: ["featureserver", "mapserver", "arcgis"]},
    {family: "STAC_CATALOG", terms: ["stac"]},
    {family: "OGC_COLLECTION", terms: ["ogc", "wfs", "wms"]},
    {family: "WARC_INDEX", terms: ["warc", "cdx"]},
    {family: "WEBSUB", terms: ["websub"]}, {family: "ACTIVITYPUB", terms: ["activitypub"]},
    {family: "WEBHOOK", terms: ["webhook"]}, {family: "MCP", terms: ["mcp"]},
    {family: "A2A", terms: ["a2a"]}, {family: "BECKN", terms: ["beckn"]},
    {family: "X402", terms: ["x402"]},
    {family: "JSON_API", terms: ["public_api", "json_api", "json-api", "/api/", " api "]},
  ]
  gap_rules: [
    {kind: "OBSERVATION", predicate: "no_admitted_channels", routes: ["PUBLIC_BACKFILL", "DISCOVER"], reason: "no exact channel is admitted/observed"},
    {kind: "STRUCTURED_CHANNEL", predicate: "no_structured_family", routes: ["DISCOVER", "OUTREACH", "PARTNER"], reason: "no structured channel is materialized"},
    {kind: "RIGHTS", predicate: "unknown_rights", routes: ["PUBLIC_BACKFILL", "HUMAN_VERIFY", "PARTNER"], reason: "reuse posture remains unknown"},
    {kind: "CREDENTIAL", predicate: "required_credential", routes: ["CREDENTIAL", "PARTNER"], reason: "an access route needs credentials without evidenced readiness"},
    {kind: "IDENTITY", predicate: "no_publisher_identity", routes: ["PUBLIC_BACKFILL", "DISCOVER", "HUMAN_VERIFY"], reason: "no canonical publisher/source identity is attached"},
    {kind: "ECONOMIC_INTERFACE", predicate: "unrelated_transaction", routes: ["AFFILIATE", "PARTNER", "OUTREACH"], reason: "action-capable source has no commercial-relationship posture"},
  ]
  cannot_infer: [
    "a SourceRoot -> an admitted or active Channel",
    "a ChannelFamily -> a materialized endpoint",
    "a declared virtual-channel count -> observed or retained material",
    "an open SourceGap -> permission to contact, purchase, combine or act",
  ]
}
