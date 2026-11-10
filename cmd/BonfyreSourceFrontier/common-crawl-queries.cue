commonCrawlQueries: {
  agent: "BonfyreSourceFrontier/1.0 (source census; aurekai.life)"
  collection_index: "https://index.commoncrawl.org/collinfo.json"
  collection_timeout_ms: 60000
  query_timeout_ms: 120000
  default_per_dork: 400
  default_pause_ms: 1500
  dorks: [
    {domain: "*.gov/*", wants: ["rss", "feed", "atom"]},
    {domain: "*.us/*", wants: ["rss", "feed", "ics"]},
    {domain: "*.edu/*", wants: ["rss", "feed", "atom"]},
    {domain: "*.org/*", wants: ["feed.xml", "rss.xml", "atom.xml"]},
    {domain: "*.museum/*", wants: ["rss", "feed"]},
    {domain: "*.k12.tx.us/*", wants: ["rss", "feed"]},
    {domain: "*.tx.us/*", wants: ["rss", "feed", "ics", "calendar"]},
    {domain: "*.ca.gov/*", wants: ["rss", "feed"]},
    {domain: "*.ny.us/*", wants: ["rss", "feed"]},
    {domain: "*.lib.tx.us/*", wants: ["rss", "feed"]},
  ]
  feedish: "rss|atom|feed|\\.ics|\\.xml|podcast"
  excluded: "\\.jpg|\\.png|\\.gif|\\.pdf|\\.css|\\.js|/wp-admin/|/login|\\.zip"
  observed_statuses: ["200", "301", "302"]
  candidate_truth: "observed_index_candidate"
  cannot_infer: "an address found in a web index is an address, not a channel this estate reads; the index says a crawler once saw it, never that it answers now"
}
