priceObservation: {
  schema: "observed_price"
  max_parallel_sources: 3
  max_json_bytes: 16777216
  user_agent: "Aurekai/1.0 (price observation; honours robots.txt; contact via aurekai.live)"
  cannot_infer: "an observed price is what a publisher stated on the date recorded. It is not a price this estate has been quoted, not one it will be charged, not evidence that anyone paid it, and not a rate: rates live in the meter, apply only to usage this estate performed, and are admitted separately. A publisher that states no price is absent here rather than assumed to be free."
  sources: {
    openrouter: {url: "https://openrouter.ai/api/v1/models", source: "openrouter/models", kind: "inference"}
    azure: {url: "https://prices.azure.com/api/retail/prices?$top=1000", source: "azure/retail-prices", kind: "compute"}
    usaspending: {url: "https://api.usaspending.gov/api/v2/search/spending_by_award/", source: "usaspending/spending_by_award", kind: "award"}
    openprices: {url: "https://prices.openfoodfacts.org/api/v1/prices", source: "openfoodfacts/open-prices", kind: "consumer"}
    nadac: {url: "https://data.medicaid.gov/api/1/datastore/query/dfa2ab14-06c2-457a-9e36-5cb6d80f8d93/0", source: "medicaid/nadac", kind: "medicine"}
    wikidata: {url: "https://qlever.dev/api/wikidata", source: "wikidata/P2284", kind: "catalogued"}
  }
}
