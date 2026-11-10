publisherGrammarPolicy: {
  schema: "bonfyre.publisher-grammar-policy.v1"
  truth: "publisher_grammar_addresses_are_candidates_not_observed_endpoints_or_admission"
  grammars: [
    {
      id: "wordpress"
      markers: ["/wp-content/", "/wp-includes/", "wp-json"]
      sweep: [{path: "/feed/", protocol: "rss"}, {path: "/comments/feed/", protocol: "rss"}, {path: "/wp-json/wp/v2/posts", protocol: "json_api"}]
      retained: [
        {path: "/feed/", protocol: "rss", material: "publication"},
        {path: "/feed/atom/", protocol: "atom", material: "publication"},
        {path: "/comments/feed/", protocol: "rss", material: "conversation"},
        {path: "/wp-json/wp/v2/posts", protocol: "wp_json", material: "publication"},
        {path: "/wp-json/wp/v2/pages", protocol: "wp_json", material: "reference"},
        {path: "/wp-json/wp/v2/categories", protocol: "wp_json", material: "taxonomy"},
        {path: "/wp-json/wp/v2/media", protocol: "wp_json", material: "photo"},
        {path: "/wp-sitemap.xml", protocol: "sitemap", material: "index"},
        {path: "/events/feed/", protocol: "rss", material: "event"},
      ]
    },
    {
      id: "drupal"
      markers: ["/sites/default/files/", "drupal-settings-json", "/core/misc/"]
      sweep: [{path: "/rss.xml", protocol: "rss"}]
      retained: [{path: "/rss.xml", protocol: "rss", material: "publication"}, {path: "/sitemap.xml", protocol: "sitemap", material: "index"}, {path: "/calendar/ical", protocol: "ics", material: "event"}]
    },
    {
      id: "ghost"
      markers: ["/assets/built/", "ghost-sdk", "content/images/size/"]
      sweep: [{path: "/rss/", protocol: "rss"}]
      retained: [{path: "/rss/", protocol: "rss", material: "publication"}, {path: "/sitemap.xml", protocol: "sitemap", material: "index"}]
    },
    {
      id: "squarespace"
      markers: ["squarespace.com", "static1.squarespace"]
      sweep: [{path: "?format=rss", protocol: "rss"}]
      retained: [{path: "?format=rss", protocol: "rss", material: "publication"}, {path: "/sitemap.xml", protocol: "sitemap", material: "index"}]
    },
    {id: "wix", markers: ["wix.com", "wixstatic.com"], sweep: [{path: "/blog-feed.xml", protocol: "rss"}], retained: []},
    {
      id: "blogger"
      markers: ["blogspot.com", "blogger.com/static"]
      sweep: [{path: "/feeds/posts/default", protocol: "atom"}, {path: "/feeds/posts/default?alt=rss", protocol: "rss"}]
      retained: [{path: "/feeds/posts/default", protocol: "atom", material: "publication"}]
    },
    {id: "substack", markers: ["substackcdn.com", "substack.com/api"], sweep: [{path: "/feed", protocol: "rss"}], retained: [{path: "/feed", protocol: "rss", material: "publication"}]},
    {
      id: "discourse"
      markers: ["discourse_theme", "/stylesheets/desktop_"]
      sweep: [{path: "/latest.rss", protocol: "rss"}, {path: "/top.rss", protocol: "rss"}]
      retained: [{path: "/latest.rss", protocol: "rss", material: "conversation"}, {path: "/sitemap.xml", protocol: "sitemap", material: "index"}]
    },
    {id: "mediawiki", markers: ["/load.php?", "mw-parser-output"], sweep: [{path: "/index.php?title=Special:RecentChanges&feed=rss", protocol: "rss"}], retained: []},
    {id: "hugo", markers: ["generator\" content=\"Hugo"], sweep: [{path: "/index.xml", protocol: "rss"}], retained: []},
    {id: "civicplus", markers: ["civicplus.com", "/Common/"], sweep: [{path: "/RSSFeed.aspx", protocol: "rss"}], retained: [
      {path: "/RSSFeed.aspx?ModID=1&CID=All-newsflash.xml", protocol: "rss", material: "publication"},
      {path: "/RSSFeed.aspx?ModID=58&CID=All-calendar.xml", protocol: "rss", material: "event"},
      {path: "/common/modules/iCalendar/iCalendar.aspx?feed=calendar", protocol: "ics", material: "event"},
    ]},
    {id: "joomla", markers: ["/media/jui/", "joomla"], sweep: [{path: "/index.php?format=feed&type=rss", protocol: "rss"}], retained: []},
    {id: "jekyll", markers: [], sweep: [{path: "/feed.xml", protocol: "rss"}], retained: []},
    {id: "typo3", markers: [], sweep: [{path: "/rss.xml", protocol: "rss"}], retained: []},
  ]
  feed_types: {
    "application/rss+xml": "rss"
    "application/atom+xml": "atom"
    "application/rdf+xml": "rdf"
    "application/feed+json": "json_feed"
    "application/json+feed": "json_feed"
    "text/calendar": "ics"
  }
  retained_link_types: [
    {token: "application/rss+xml", family: "rss"},
    {token: "application/atom+xml", family: "atom"},
    {token: "application/feed+json", family: "json_feed"},
    {token: "application/json+feed", family: "json_feed"},
    {token: "text/calendar", family: "ics"},
    {token: "application/activity+json", family: "activitypub"},
    {token: "application/ld+json", family: "json_ld"},
    {token: "application/opensearchdescription+xml", family: "opensearch"},
  ]
  retained_url_hints: [
    {pattern: "\\.ics(\\?|$)", family: "ics"},
    {pattern: "/\\.well-known/openapi\\.json|openapi\\.json|swagger\\.json|/v3/api-docs", family: "openapi"},
    {pattern: "/\\.well-known/(mcp|mcp\\.json)/", family: "mcp"},
    {pattern: "/\\.well-known/agent(-card)?\\.json", family: "a2a"},
    {pattern: "/\\.well-known/api-catalog/", family: "api_catalog"},
    {pattern: "/\\.well-known/gbfs\\.json|/gbfs/", family: "gbfs"},
    {pattern: "sitemap[^/]*\\.xml", family: "sitemap"},
    {pattern: "/api/views/[a-z0-9]{4}-[a-z0-9]{4}|/resource/[a-z0-9]{4}-[a-z0-9]{4}/", family: "socrata"},
    {pattern: "/api/3/action/package_search|/api/3/action/package_list", family: "ckan"},
    {pattern: "/arcgis/rest/services|/featureserver|/mapserver", family: "arcgis"},
    {pattern: "/oai-pmh|verb=listrecords", family: "oai_pmh"},
    {pattern: "gtfs.*\\.zip|/gtfs-rt/|tripupdates|vehiclepositions", family: "gtfs"},
    {pattern: "/beckn|/bap/|/bpp/", family: "beckn"},
    {pattern: "x402", family: "x402"},
    {pattern: "/graphql", family: "graphql"},
  ]
  retained_material: {
    ics: "event"
    socrata: "dataset"
    ckan: "dataset"
    arcgis: "dataset"
    gbfs: "mobility"
    gtfs: "mobility"
    oai_pmh: "research"
    rss: "publication"
    atom: "publication"
    json_feed: "publication"
  }
  recursive_profile: {
    link_hints: [
      {pattern: "application/rss\\+xml", family: "RSS"},
      {pattern: "application/atom\\+xml", family: "ATOM"},
      {pattern: "application/rdf\\+xml|/feed/rdf|rdf\\.xml", family: "RDF"},
      {pattern: "application/feed\\+json", family: "JSON_FEED"},
      {pattern: "text/calendar|\\.ics(?:[?\\\"']|$)", family: "ICS"},
      {pattern: "rel=[\\\"']?hub|websub", family: "WEBSUB"},
      {pattern: "application/activity\\+json|activitypub", family: "ACTIVITYPUB"},
      {pattern: "openapi|swagger", family: "OPENAPI"},
      {pattern: "(?:^|[/._-])mcp(?:[/._?-]|$)", family: "MCP"},
      {pattern: "(?:^|[/._-])a2a(?:[/._?-]|$)", family: "A2A"},
      {pattern: "beckn", family: "BECKN"},
      {pattern: "x402", family: "X402"},
    ]
    json_api_path: "/api/|\\.(json|geojson)$"
    sitemap_path: "sitemap"
    wordpress_markers: ["wp-content/", "wp-includes/", "https://api.w.org/", "/wp-json"]
    wordpress_archive_path: "/(category|tag|author)/[^?#\\\"']+/?$"
    sitemap_index_marker: "<sitemapindex"
    sitemap_location: "<loc>\\s*(https?://[^<\\s]+)\\s*</loc>"
    wordpress_routes: [
      {path: "feed/", family: "RSS", role: "site_feed"},
      {path: "feed/atom/", family: "ATOM", role: "site_atom_feed"},
      {path: "feed/rdf/", family: "RDF", role: "site_rdf_feed"},
      {path: "comments/feed/", family: "RSS", role: "comments_feed"},
      {path: "wp-json/", family: "JSON_API", role: "rest_index"},
      {path: "wp-json/wp/v2/posts", family: "JSON_API", role: "posts_collection"},
      {path: "wp-json/wp/v2/pages", family: "JSON_API", role: "pages_collection"},
      {path: "wp-json/wp/v2/categories", family: "JSON_API", role: "categories_collection"},
      {path: "wp-json/wp/v2/tags", family: "JSON_API", role: "tags_collection"},
      {path: "wp-json/wp/v2/media", family: "JSON_API", role: "media_collection"},
      {path: "wp-sitemap.xml", family: "SITEMAP", role: "sitemap_index"},
    ]
    cannot_infer: [
      "observed link -> reachable channel",
      "channel candidate -> admission or current content",
      "same root -> same semantic owner",
      "protocol-shaped URL -> capability, authority or permission",
    ]
  }
  archive_grammars: ["wordpress", "ghost", "drupal"]
  universal: [
    {path: "/feed", protocol: "rss"}, {path: "/rss", protocol: "rss"},
    {path: "/feed.xml", protocol: "rss"}, {path: "/atom.xml", protocol: "atom"},
    {path: "/index.xml", protocol: "rss"}, {path: "/rss.xml", protocol: "rss"},
    {path: "/feeds/all.atom.xml", protocol: "atom"}, {path: "/sitemap.xml", protocol: "sitemap"},
  ]
}
