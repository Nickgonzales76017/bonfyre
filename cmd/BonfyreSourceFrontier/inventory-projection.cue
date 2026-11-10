sourceInventoryProjection: {
  schema: "bonfyre.source-inventory-projection.v1"
  truth: "address_conventions_are_candidate_protocol_hints_not_observed_capability_or_admission"
  protocol_hints: [
    {pattern: "\\.ics(\\?|$)|/ical|icalendar", protocol: "ics"},
    {pattern: "/rss|rss\\.xml|feed\\.xml|\\.rss(\\?|$)|rssfeed\\.aspx", protocol: "rss"},
    {pattern: "/atom|atom\\.xml", protocol: "atom"},
    {pattern: "/feed/?$|/feeds?/", protocol: "rss"},
    {pattern: "/feed\\.json|\\.json_feed", protocol: "json_feed"},
    {pattern: "/api/views/|/resource/[a-z0-9]{4}-[a-z0-9]{4}/", protocol: "socrata"},
    {pattern: "/api/3/action/|/dataset\\.json", protocol: "ckan"},
    {pattern: "/arcgis/rest/|/featureserver|/mapserver", protocol: "arcgis"},
    {pattern: "/openapi\\.json|swagger\\.json|/v3/api-docs", protocol: "openapi"},
    {pattern: "/graphql", protocol: "graphql"},
    {pattern: "/gtfs.*\\.zip|/gtfs-rt|/vehiclepositions|/tripupdates", protocol: "gtfs"},
    {pattern: "/gbfs\\.json|/gbfs/", protocol: "gbfs"},
    {pattern: "/stac|catalog\\.json", protocol: "stac"},
    {pattern: "/oai2?\\?|/oai-pmh", protocol: "oai_pmh"},
    {pattern: "/sparql", protocol: "sparql"},
    {pattern: "/\\.well-known/mcp|/mcp(\\?|$)", protocol: "mcp"},
    {pattern: "/\\.well-known/agent(-card)?\\.json|/a2a", protocol: "a2a"},
    {pattern: "/beckn|/bap|/bpp", protocol: "beckn"},
    {pattern: "/x402", protocol: "x402"},
    {pattern: "/sitemap.*\\.xml", protocol: "sitemap"},
    {pattern: "\\.wsdl|/soap", protocol: "soap"},
    {pattern: "/api(/|$)", protocol: "json_api"},
  ]
  material_by_protocol: {
    ics: "event"
    socrata: "dataset"
    ckan: "dataset"
    arcgis: "dataset"
    stac: "dataset"
    gtfs: "mobility"
    gbfs: "mobility"
    oai_pmh: "research"
    rss: "publication"
    atom: "publication"
    json_feed: "publication"
  }
  source_kind_protocol: {
    rss: "rss"
    mcp: "mcp"
    x402: "x402"
    x402_spec: "x402"
  }
  live_family_material: {
    podcasts: "audio"
    "local-voices": "publication"
  }
  inventories: [
    {arg: "--v4-jsonl", key: "v4Jsonl", label: "EXTERNAL_ESTATE_V4", file: "v4_jsonl"},
    {arg: "--v4-sqlite", key: "v4Sqlite", label: "EXTERNAL_ESTATE_V4", file: "v4_sqlite"},
    {arg: "--advertisements", key: "advertisements", label: "ADDRESS_PLANE", file: "advertisements"},
    {arg: "--feeds", key: "feeds", label: "PUBLICATION_FEEDS", file: "feeds"},
    {arg: "--discovery", key: "discovery", label: "DISCOVERY_CANDIDATES", file: "discovery"},
    {arg: "--dataset-registry", key: "datasetRegistry", label: "PUBLIC_DATASET_REGISTRY", file: "dataset_registry"},
    {arg: "--live-sources", key: "liveSources", label: "LIVE_SOURCE_DISCOVERY", file: "live_sources"},
    {arg: "--passports", key: "passports", label: "PUBLIC_COMMONS_PASSPORT", file: "passports"},
  ]
}
