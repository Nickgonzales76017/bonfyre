package control

// PublicEstatePassport: a public estate's phenotype, legible to the machine.
//
// It carries the descriptor-level #SourcePassport (external-descriptor.cue) as
// its descriptor, so robots admission and best carrier are stated once and
// constrained to agree with the route this estate actually takes.
//
// The open internet's datasets, indexes, archives, fingerprints and mirrors
// are not one kind of thing.  An identity registry answers "what existing
// thing is this?", an archive "what exact material existed?", an observation
// network "what is happening?", a reference estate "what facts constrain
// this?", a relation graph "how is it connected?" and a fingerprint corpus "is
// this the same material?".  A passport says which of those a source can do,
// through what access, under what reuse posture, by what lawful route, with
// what credential, and what it cannot be read to imply -- so a task asks for a
// capability, not for a source's name.
//
// A passport is not an admission to read (that is a census row with a robots
// preflight and a receipt), not a licence (terms are the source's, per item
// where they vary), and not evidence that anything a source says is true.
// cmd/BonfyreWebAddress/tools/compile_source_passports.py lowers passports
// into AddressPlane candidate advertisements (kind external_source) so
// `bonfyre-external-estate forward --kind external_source --need
// affordance:identify-recording` answers with eligible estates.

#PassportRole: "IDENTITY" | "OBSERVATION" | "MATERIAL" | "REFERENCE" | "ARCHIVE" | "RELATION_GRAPH" | "FINGERPRINT" | "REGISTRY"
#PassportAccess: "API" | "RSS" | "BULK" | "WARC" | "SPARQL" | "FILE" | "STREAM" | "CDX" | "TILES" | "EMBED"
#PassportTopology: "ENTITY" | "GRAPH" | "TIMESERIES" | "GEO" | "MEDIA" | "CORPUS" | "FINGERPRINT" | "CATALOGUE"

// What a person or the estate may do with what a source publishes.  "unknown"
// is a real value: a source that does not state terms is not thereby free.
#ReuseAct: "observe" | "index" | "cache" | "retain" | "display_metadata" | "display_thumbnail" |
           "stream" | "redistribute" | "transform" | "commercial_use"
#ReusePosture: {
    allows: [...#ReuseAct]
    requires: [...("attribution" | "share_alike" | "non_commercial" | "no_derivatives" | "rate_limit" | "contact_identification")]
    // Terms differ item by item; the item's own terms govern it.
    varies_per_item: *false | bool
    unknown: [...#ReuseAct]
    terms_url: *"" | =~"^https://"
}

// The route this estate takes.  "admitted" means robots rules admit
// BonfyreProject/1.0 on the exact path, or the host publishes none.  A refused
// path is recorded with the lawful route that replaces it, never re-reached.
#PassportCarrier: {
    route: string & != ""
    robots: "admitted" | "no-rules" | "refused" | "unobserved"
    replaces: *"" | string
    rate: *"" | string
}

#PublicEstatePassport: {
    passport: string & =~"^[a-z0-9-]+$"
    name: string & != ""
    roles: [#PassportRole, ...#PassportRole]
    access: [#PassportAccess, ...#PassportAccess]
    topology: [#PassportTopology, ...#PassportTopology]
    identifiers: [...string & =~"^[a-z0-9-]+$"]
    subject_kinds: [...string & =~"^[a-z0-9-]+$"]
    spatial: "global" | "regional" | "local" | "none"
    temporal: "live" | "historic" | "snapshot" | "periodic" | "none"
    refresh: "live" | "hourly" | "daily" | "weekly" | "monthly" | "twice-weekly" | "irregular" | "immutable"
    revision: "overwrite" | "versioned" | "append" | "archived-revisions" | "none"
    // Things this estate can ask of the source, as verb-object terms.
    capabilities: [string & =~"^[a-z]+-[a-z0-9-]+$", ...string & =~"^[a-z]+-[a-z0-9-]+$"]
    reuse: #ReusePosture
    attribution: *"" | string
    scale: string & != ""
    mirror: "none" | "sample" | "partial" | "full" | "refused"
    evidence: "authoritative" | "curated" | "community" | "crowd" | "derived" | "claimed"
    carrier: #PassportCarrier
    descriptor: #SourcePassport & {
        robots_allows_us: carrier.robots == "admitted" || carrier.robots == "no-rules"
        protocol_descriptors: *[] | [...#ProtocolDescriptor]
    }
    // A provider on the private credentials page (provider-credentials.cue).
    credential: *"" | =~"^[a-z0-9-]+$"
    state: "DECLARED" | "SHADOW" | "ADMITTED" | "REFUSED"
    // Census rows that realize an admitted passport.
    census: [...string & =~"^[a-z0-9-]+$"]
    cannot_infer: string & != ""
}

nativeSourcePassports: [...#PublicEstatePassport] & [
    {
        passport: "wikipedia-feeds", name: "Wikipedia daily feeds"
        roles: ["OBSERVATION", "REFERENCE"], access: ["API"], topology: ["ENTITY", "CORPUS"]
        identifiers: ["wikidata-qid", "url"], subject_kinds: ["article", "event", "person", "place"]
        spatial: "global", temporal: "periodic", refresh: "daily", revision: "archived-revisions"
        capabilities: ["observe-attention", "recall-anniversaries", "summarize-subject", "resolve-article"]
        reuse: {allows: ["observe", "index", "cache", "retain", "display_metadata", "display_thumbnail", "redistribute", "transform", "commercial_use"], requires: ["attribution", "share_alike"], varies_per_item: true, terms_url: "https://foundation.wikimedia.org/wiki/Policy:Terms_of_Use"}
        attribution: "Wikipedia contributors, CC BY-SA 4.0; pictures carry their own terms"
        scale: "one featured feed a day per language", mirror: "sample", evidence: "community"
        descriptor: {root: "https://api.wikimedia.org", best_carrier: "structured-metadata"}
        carrier: {route: "https://api.wikimedia.org/feed/v1/wikipedia/en/featured/{date}", robots: "no-rules"}
        state: "ADMITTED", census: ["wikipedia-most-read", "wikipedia-featured-article", "wikimedia-picture-of-the-day", "wikipedia-on-this-day"]
        cannot_infer: "a page people read or an event recorded for a date is not a verified account or this estate's choice"
    },
    {
        passport: "wikimedia-commons-structured-data", name: "Wikimedia Commons structured data"
        roles: ["IDENTITY", "MATERIAL", "RELATION_GRAPH"], access: ["SPARQL"], topology: ["GRAPH", "MEDIA", "GEO"]
        identifiers: ["commons-mediainfo", "wikidata-qid", "file-name"], subject_kinds: ["picture", "video", "recording", "place", "creator"]
        spatial: "global", temporal: "historic", refresh: "weekly", revision: "archived-revisions"
        capabilities: ["supply-free-pictures", "supply-free-video", "supply-free-audio", "credit-media", "locate-photographs", "depict-subjects", "rate-pictures"]
        reuse: {allows: ["observe", "index", "cache", "display_metadata", "display_thumbnail", "stream"], requires: ["attribution"], varies_per_item: true, unknown: ["redistribute", "transform", "commercial_use"], terms_url: "https://commons.wikimedia.org/wiki/Commons:Reusing_content_outside_Wikimedia"}
        attribution: "each file's author and licence, as its Commons page states"
        scale: "over 100 million files", mirror: "none", evidence: "community"
        descriptor: {root: "https://qlever.dev", best_carrier: "structured-metadata"}
        carrier: {route: "https://qlever.dev/api/wikimedia-commons", robots: "no-rules", replaces: "https://commons.wikimedia.org/w/api.php (robots refuses /w/)"}
        state: "ADMITTED", census: ["commons-featured-pictures", "commons-dfw-rated"]
        cannot_infer: "a rating, a depicted thing or a recorded location is a community statement about a file, not a survey, an endorsement or a licence granted here"
    },
    {
        passport: "wikidata", name: "Wikidata"
        roles: ["IDENTITY", "REFERENCE", "RELATION_GRAPH"], access: ["SPARQL"], topology: ["ENTITY", "GRAPH", "GEO"]
        identifiers: ["wikidata-qid", "musicbrainz-mbid", "isni", "viaf", "geonames", "osm-relation"], subject_kinds: ["person", "place", "work", "film", "recording", "organization", "event"]
        spatial: "global", temporal: "historic", refresh: "weekly", revision: "archived-revisions"
        capabilities: ["resolve-entity", "resolve-place", "join-identifiers", "supply-public-domain-films", "supply-recorded-works"]
        reuse: {allows: ["observe", "index", "cache", "retain", "display_metadata", "redistribute", "transform", "commercial_use"], requires: [], terms_url: "https://www.wikidata.org/wiki/Wikidata:Licensing"}
        attribution: "Wikidata (CC0)"
        scale: "over 110 million items", mirror: "none", evidence: "community"
        descriptor: {root: "https://qlever.dev", best_carrier: "structured-metadata"}
        carrier: {route: "https://qlever.dev/api/wikidata", robots: "no-rules", replaces: "https://query.wikidata.org/sparql (robots refuses /sparql)"}
        state: "ADMITTED", census: ["wikidata-places-dfw", "wikidata-public-domain-films", "wikidata-recorded-music"]
        cannot_infer: "an identifier joined in Wikidata is a community assertion of sameness, not a legal or evidentiary identity"
    },
    {
        passport: "internet-archive-items", name: "Internet Archive"
        roles: ["ARCHIVE", "MATERIAL"], access: ["API", "EMBED"], topology: ["MEDIA", "CORPUS"]
        identifiers: ["internet-archive-id", "url"], subject_kinds: ["film", "recording", "live-performance", "broadcast", "book"]
        spatial: "global", temporal: "historic", refresh: "daily", revision: "versioned"
        capabilities: ["supply-archived-films", "supply-live-recordings", "supply-historic-audio", "embed-player"]
        reuse: {allows: ["observe", "index", "display_metadata", "display_thumbnail", "stream"], requires: ["attribution"], varies_per_item: true, unknown: ["redistribute", "transform", "commercial_use"], terms_url: "https://archive.org/about/terms.php"}
        attribution: "the item's creator and its stated licence, as the Archive's item page records"
        scale: "millions of films, recordings and broadcasts", mirror: "none", evidence: "claimed"
        descriptor: {root: "https://archive.org", best_carrier: "structured-metadata"}
        carrier: {route: "https://archive.org/advancedsearch.php", robots: "admitted", rate: "one query per source per day"}
        state: "ADMITTED", census: ["archive-feature-films", "archive-dallas-live-music", "archive-great-78"]
        cannot_infer: "an item hosted by the Archive is not thereby public domain; hosting is not a statement of legal status"
    },
    {
        passport: "wayback-cdx", name: "Wayback Machine capture index"
        roles: ["ARCHIVE"], access: ["CDX"], topology: ["TIMESERIES", "CORPUS"]
        identifiers: ["url", "content-digest"], subject_kinds: ["web-page"]
        spatial: "global", temporal: "historic", refresh: "live", revision: "append"
        capabilities: ["replay-url", "history-url-changes"]
        reuse: {allows: ["observe", "index", "display_metadata"], requires: ["attribution"], unknown: ["redistribute", "transform", "commercial_use"], terms_url: "https://archive.org/about/terms.php"}
        attribution: "Internet Archive Wayback Machine"
        scale: "over 900 billion captures", mirror: "none", evidence: "authoritative"
        descriptor: {root: "https://web.archive.org", best_carrier: "structured-metadata"}
        carrier: {route: "https://web.archive.org/cdx/search/cdx?url={url}&output=json&collapse=digest", robots: "no-rules", rate: "on a person's request, once a day per address"}
        state: "ADMITTED"
        cannot_infer: "a capture shows what an address answered to the Archive's crawler at that moment, not what every visitor saw or what is true"
    },
    {
        passport: "common-crawl", name: "Common Crawl"
        roles: ["ARCHIVE", "OBSERVATION"], access: ["WARC", "CDX", "BULK"], topology: ["CORPUS", "TIMESERIES"]
        identifiers: ["url", "warc-record", "content-digest"], subject_kinds: ["web-page"]
        spatial: "global", temporal: "historic", refresh: "monthly", revision: "append"
        capabilities: ["replay-url", "history-url-changes", "sample-web-corpus"]
        reuse: {allows: ["observe", "index"], requires: [], unknown: ["retain", "display_metadata", "redistribute", "transform", "commercial_use"], terms_url: "https://commoncrawl.org/terms-of-use"}
        scale: "billions of pages per crawl", mirror: "refused", evidence: "authoritative"
        descriptor: {root: "https://index.commoncrawl.org", best_carrier: "none"}
        carrier: {route: "https://index.commoncrawl.org/{crawl}-index?url={url}", robots: "refused", replaces: "data.commoncrawl.org/cc-index is refused too; Wayback CDX answers the same history question"}
        state: "REFUSED"
        cannot_infer: "availability of a capture proves observation, never a right to republish it"
    },
    {
        passport: "musicbrainz", name: "MusicBrainz"
        roles: ["IDENTITY", "RELATION_GRAPH"], access: ["API", "BULK"], topology: ["ENTITY", "GRAPH"]
        identifiers: ["musicbrainz-mbid", "isrc", "iswc", "wikidata-qid"], subject_kinds: ["artist", "recording", "work", "release", "label", "event", "area"]
        spatial: "global", temporal: "historic", refresh: "twice-weekly", revision: "versioned"
        capabilities: ["identify-recording", "resolve-artist", "relate-works", "list-releases"]
        reuse: {allows: ["observe", "index", "cache", "retain", "display_metadata", "redistribute", "transform", "commercial_use"], requires: ["contact_identification", "rate_limit"], terms_url: "https://musicbrainz.org/doc/About/Data_License"}
        attribution: "MusicBrainz (core data CC0)"
        scale: "tens of millions of recordings", mirror: "partial", evidence: "community"
        descriptor: {root: "https://data.metabrainz.org", best_carrier: "conditional-http"}
        carrier: {route: "https://data.metabrainz.org/pub/musicbrainz/data/fullexport/ (twice-weekly CC0 dumps); Wikidata MBID joins", robots: "no-rules", replaces: "https://musicbrainz.org/ws/2 (robots refuses the web service to crawlers)"}
        state: "DECLARED"
        cannot_infer: "a matched MBID says two descriptions name one recording, not that a performance is authentic or licensed"
    },
    {
        passport: "acoustid", name: "AcoustID"
        roles: ["FINGERPRINT", "IDENTITY"], access: ["API"], topology: ["FINGERPRINT"]
        identifiers: ["acoustid", "chromaprint", "musicbrainz-mbid"], subject_kinds: ["recording"]
        spatial: "none", temporal: "none", refresh: "live", revision: "append"
        capabilities: ["identify-recording", "fingerprint-audio"]
        reuse: {allows: ["observe", "index", "display_metadata"], requires: ["non_commercial", "rate_limit"], unknown: ["redistribute", "commercial_use"], terms_url: "https://acoustid.org/webservice"}
        attribution: "AcoustID"
        scale: "tens of millions of fingerprints", mirror: "none", evidence: "crowd"
        descriptor: {root: "https://api.acoustid.org", best_carrier: "structured-metadata"}
        carrier: {route: "https://api.acoustid.org/v2/lookup", robots: "admitted", rate: "three requests a second"}
        credential: "acoustid", state: "DECLARED"
        cannot_infer: "a fingerprint match is acoustic similarity, not proof of the same performance or of rights"
    },
    {
        passport: "podcast-index", name: "Podcast Index"
        roles: ["OBSERVATION", "IDENTITY"], access: ["API"], topology: ["ENTITY", "MEDIA"]
        identifiers: ["podcast-guid", "feed-url", "episode-guid"], subject_kinds: ["podcast", "episode", "person"]
        spatial: "global", temporal: "live", refresh: "live", revision: "overwrite"
        capabilities: ["discover-podcasts", "resolve-podcast-guid", "observe-new-episodes"]
        reuse: {allows: ["observe", "index", "cache", "display_metadata", "display_thumbnail"], requires: ["attribution"], unknown: ["redistribute", "commercial_use"], terms_url: "https://podcastindex.org/"}
        attribution: "Podcast Index"
        scale: "over four million feeds", mirror: "none", evidence: "claimed"
        descriptor: {root: "https://api.podcastindex.org", best_carrier: "structured-metadata"}
        carrier: {route: "https://api.podcastindex.org/api/1.0", robots: "admitted"}
        credential: "podcast-index", state: "DECLARED"
        cannot_infer: "a listed feed is a claim by its publisher, not a verified show"
    },
    {
        passport: "podcasting-2-feeds", name: "Podcasting 2.0 feeds"
        roles: ["OBSERVATION", "MATERIAL"], access: ["RSS"], topology: ["MEDIA", "ENTITY"]
        identifiers: ["podcast-guid", "episode-guid", "url"], subject_kinds: ["episode", "person", "place"]
        spatial: "global", temporal: "live", refresh: "daily", revision: "overwrite"
        capabilities: ["supply-episode-audio", "supply-chapters", "supply-transcripts", "name-people", "link-discussion"]
        reuse: {allows: ["observe", "index", "display_metadata", "display_thumbnail", "stream"], requires: ["attribution"], varies_per_item: true, unknown: ["redistribute", "transform", "commercial_use"]}
        attribution: "each show, as its feed names it"
        scale: "one feed per show", mirror: "none", evidence: "claimed"
        descriptor: {root: "https://feeds.podcastindex.org", best_carrier: "feed"}
        carrier: {route: "the show's own feed address", robots: "admitted"}
        state: "ADMITTED", census: ["podcasting-2-0"]
        cannot_infer: "a person named in an episode or a place it mentions is the show's statement, not an identity or a fact"
    },
    {
        passport: "fyyd", name: "fyyd podcast search"
        roles: ["OBSERVATION"], access: ["API"], topology: ["CATALOGUE"]
        identifiers: ["feed-url"], subject_kinds: ["podcast", "episode"]
        spatial: "global", temporal: "live", refresh: "live", revision: "overwrite"
        capabilities: ["discover-podcasts"]
        reuse: {allows: ["observe"], requires: [], unknown: ["retain", "display_metadata"]}
        scale: "millions of episodes", mirror: "none", evidence: "claimed"
        descriptor: {root: "https://api.fyyd.de", best_carrier: "none"}
        carrier: {route: "https://api.fyyd.de/0.2", robots: "refused", replaces: "Podcast Index with a key, or shows' own feeds"}
        state: "REFUSED"
        cannot_infer: "a refusal by robots is a standing rule the host wrote, not a statement about this estate"
    },
    {
        passport: "free-music-archive-dataset", name: "Free Music Archive dataset"
        roles: ["MATERIAL", "FINGERPRINT", "REFERENCE"], access: ["BULK"], topology: ["CORPUS", "MEDIA", "FINGERPRINT"]
        identifiers: ["fma-track-id"], subject_kinds: ["recording", "artist", "album", "genre"]
        spatial: "none", temporal: "snapshot", refresh: "immutable", revision: "none"
        capabilities: ["benchmark-audio-similarity", "supply-audio-features", "train-fingerprint-neighbourhoods"]
        reuse: {allows: ["observe", "index", "retain", "transform"], requires: ["attribution"], varies_per_item: true, unknown: ["redistribute", "commercial_use"], terms_url: "https://github.com/mdeff/fma"}
        attribution: "each track's artist and Creative Commons licence"
        scale: "106,574 tracks, about 900 GiB in full; metadata about 342 MiB", mirror: "sample", evidence: "curated"
        descriptor: {root: "https://os.unil.cloud.switch.ch", best_carrier: "conditional-http"}
        carrier: {route: "https://os.unil.cloud.switch.ch/fma/fma_metadata.zip", robots: "no-rules"}
        state: "DECLARED"
        cannot_infer: "a dataset licence per track is the uploader's statement at collection time"
    },
    {
        passport: "gbif-inaturalist", name: "iNaturalist research-grade observations through GBIF"
        roles: ["OBSERVATION"], access: ["API"], topology: ["GEO", "TIMESERIES", "MEDIA"]
        identifiers: ["gbif-occurrence", "gbif-taxon", "inaturalist-observation"], subject_kinds: ["species-sighting", "taxon", "place"]
        spatial: "local", temporal: "live", refresh: "daily", revision: "overwrite"
        capabilities: ["observe-species", "locate-sightings", "supply-licensed-nature-photos"]
        reuse: {allows: ["observe", "index", "cache", "display_metadata", "display_thumbnail"], requires: ["attribution"], varies_per_item: true, unknown: ["redistribute", "transform", "commercial_use"], terms_url: "https://www.gbif.org/terms"}
        attribution: "each observer, with the licence they chose, via iNaturalist and GBIF"
        scale: "tens of thousands of sightings a month in North Texas", mirror: "none", evidence: "crowd"
        descriptor: {root: "https://api.gbif.org", best_carrier: "structured-metadata"}
        carrier: {route: "https://api.gbif.org/v1/occurrence/search", robots: "admitted", replaces: "https://api.inaturalist.org (robots refuses crawlers)"}
        state: "ADMITTED", census: ["gbif-inaturalist-dfw"]
        cannot_infer: "a research-grade sighting is a community identification at a place and time, not a survey or a range"
    },
    {
        passport: "dbnomics", name: "DBnomics"
        roles: ["REFERENCE"], access: ["API"], topology: ["TIMESERIES"]
        identifiers: ["dbnomics-series"], subject_kinds: ["economic-series", "region", "industry"]
        spatial: "global", temporal: "periodic", refresh: "daily", revision: "archived-revisions"
        capabilities: ["supply-economic-series", "compare-regions"]
        reuse: {allows: ["observe"], requires: ["attribution"], varies_per_item: true, unknown: ["retain", "display_metadata", "commercial_use"], terms_url: "https://db.nomics.world/"}
        scale: "over a billion observations from 80 providers", mirror: "none", evidence: "authoritative"
        descriptor: {root: "https://api.db.nomics.world", best_carrier: "none"}
        carrier: {route: "https://api.db.nomics.world/v22", robots: "refused", replaces: "the underlying providers' own published series"}
        state: "REFUSED"
        cannot_infer: "an economic series describes an area, never one property or person"
    },
    {
        passport: "fred", name: "FRED economic data"
        roles: ["REFERENCE"], access: ["API"], topology: ["TIMESERIES"]
        identifiers: ["fred-series"], subject_kinds: ["economic-series", "metro-area"]
        spatial: "regional", temporal: "periodic", refresh: "daily", revision: "archived-revisions"
        capabilities: ["supply-economic-series", "supply-metro-employment"]
        reuse: {allows: ["observe", "index", "display_metadata"], requires: ["attribution"], varies_per_item: true, unknown: ["redistribute", "commercial_use"], terms_url: "https://fred.stlouisfed.org/legal/"}
        attribution: "Federal Reserve Bank of St. Louis, and each series' source"
        scale: "over 800,000 series", mirror: "none", evidence: "authoritative"
        descriptor: {root: "https://api.stlouisfed.org", best_carrier: "structured-metadata"}
        carrier: {route: "https://api.stlouisfed.org/fred/series/observations", robots: "admitted"}
        credential: "fred", state: "DECLARED"
        cannot_infer: "a metro series is not a statement about any one property"
    },
    {
        passport: "ooni", name: "OONI network measurements"
        roles: ["OBSERVATION"], access: ["API", "BULK"], topology: ["TIMESERIES", "GEO"]
        identifiers: ["ooni-measurement", "url", "asn", "country"], subject_kinds: ["web-page", "network"]
        spatial: "global", temporal: "live", refresh: "hourly", revision: "append"
        capabilities: ["observe-reachability", "compare-regional-access"]
        reuse: {allows: ["observe", "index", "retain", "display_metadata", "redistribute", "transform", "commercial_use"], requires: ["attribution"], terms_url: "https://ooni.org/about/data-policy/"}
        attribution: "OONI (CC BY 4.0)"
        scale: "over two billion measurements", mirror: "none", evidence: "crowd"
        descriptor: {root: "https://ooni-data-eu-fra.s3.eu-central-1.amazonaws.com", best_carrier: "conditional-http"}
        carrier: {route: "https://ooni-data-eu-fra.s3.eu-central-1.amazonaws.com (bulk)", robots: "no-rules", replaces: "https://api.ooni.io and api.ooni.org (robots refuses crawlers)"}
        state: "DECLARED"
        cannot_infer: "a failed measurement is what one probe saw from one network, not proof of censorship"
    },
    {
        passport: "opencorporates", name: "OpenCorporates"
        roles: ["IDENTITY", "REFERENCE", "RELATION_GRAPH"], access: ["API"], topology: ["ENTITY", "GRAPH"]
        identifiers: ["opencorporates-company", "jurisdiction-company-number", "lei"], subject_kinds: ["company", "officer", "filing"]
        spatial: "global", temporal: "historic", refresh: "daily", revision: "versioned"
        capabilities: ["resolve-company", "list-officers", "list-filings"]
        reuse: {allows: ["observe", "index", "display_metadata"], requires: ["attribution", "share_alike"], unknown: ["commercial_use", "redistribute"], terms_url: "https://opencorporates.com/legal/licence"}
        attribution: "OpenCorporates, with each record's source registry"
        scale: "over 200 million companies", mirror: "none", evidence: "authoritative"
        descriptor: {root: "https://api.opencorporates.com", best_carrier: "structured-metadata"}
        carrier: {route: "https://api.opencorporates.com/v0.4", robots: "unobserved"}
        credential: "opencorporates", state: "DECLARED"
        cannot_infer: "a registered company record is not a statement that the company operates, owns anything here or can be contacted"
    },
    {
        passport: "openstreetmap-tiles", name: "OpenStreetMap standard tiles"
        roles: ["MATERIAL", "REFERENCE"], access: ["TILES"], topology: ["GEO"]
        identifiers: ["tile-zxy"], subject_kinds: ["street-map"]
        spatial: "global", temporal: "live", refresh: "live", revision: "overwrite"
        capabilities: ["draw-street-map"]
        reuse: {allows: ["display_thumbnail"], requires: ["attribution", "rate_limit"], terms_url: "https://operations.osmfoundation.org/policies/tiles/"}
        attribution: "Map © OpenStreetMap contributors"
        scale: "the planet at 19 zoom levels", mirror: "none", evidence: "community"
        descriptor: {root: "https://tile.openstreetmap.org", best_carrier: "conditional-http"}
        carrier: {route: "https://tile.openstreetmap.org/{z}/{x}/{y}.png, loaded by the reader's browser", robots: "admitted", rate: "light use only"}
        state: "ADMITTED"
        cannot_infer: "a map tile shows streets, not boundaries, ownership or access rights"
    },
    {
        passport: "cover-art-archive", name: "Cover Art Archive"
        roles: ["MATERIAL"], access: ["API"], topology: ["MEDIA"]
        identifiers: ["musicbrainz-mbid"], subject_kinds: ["release-cover"]
        spatial: "none", temporal: "historic", refresh: "daily", revision: "versioned"
        capabilities: ["supply-release-covers"]
        reuse: {allows: ["observe", "index"], requires: [], unknown: ["display_thumbnail", "redistribute", "transform", "commercial_use"], terms_url: "https://coverartarchive.org/"}
        scale: "millions of covers", mirror: "none", evidence: "community"
        descriptor: {root: "https://coverartarchive.org", best_carrier: "structured-metadata"}
        carrier: {route: "https://coverartarchive.org/release-group/{mbid}", robots: "admitted"}
        state: "REFUSED"
        cannot_infer: "most release art is copyrighted by its label; the estate does not show pictures whose terms do not travel"
    },
    {
        passport: "awesome-public-datasets", name: "awesome-public-datasets registry"
        roles: ["REGISTRY"], access: ["FILE"], topology: ["CATALOGUE"]
        identifiers: ["url", "doi"], subject_kinds: ["dataset"]
        spatial: "global", temporal: "snapshot", refresh: "irregular", revision: "versioned"
        capabilities: ["discover-datasets", "propose-sources"]
        reuse: {allows: ["observe", "index", "retain", "display_metadata", "redistribute", "transform", "commercial_use"], requires: ["attribution"], terms_url: "https://github.com/awesomedata/awesome-public-datasets"}
        attribution: "awesomedata/awesome-public-datasets contributors"
        scale: "about a thousand dataset entries", mirror: "full", evidence: "curated"
        descriptor: {root: "https://raw.githubusercontent.com", best_carrier: "conditional-http"}
        carrier: {route: "https://raw.githubusercontent.com/awesomedata/awesome-public-datasets/master/README.rst", robots: "no-rules"}
        state: "ADMITTED"
        cannot_infer: "a listed dataset is a pointer somebody curated, not a reachable, lawful or current source"
    },
    {
        passport: "tarrant-appraisal-roll", name: "Tarrant Appraisal District roll"
        roles: ["REFERENCE", "IDENTITY"], access: ["BULK"], topology: ["ENTITY", "GEO"]
        identifiers: ["tad-account", "street-address"], subject_kinds: ["property", "owner-of-record"]
        spatial: "local", temporal: "periodic", refresh: "weekly", revision: "archived-revisions"
        capabilities: ["resolve-property", "observe-ownership", "compare-street-properties"]
        reuse: {allows: ["observe", "index", "cache", "retain", "display_metadata"], requires: [], unknown: ["redistribute", "commercial_use"]}
        attribution: "Tarrant Appraisal District"
        scale: "about 6.5 million observations", mirror: "full", evidence: "authoritative"
        descriptor: {root: "https://www.tad.org", best_carrier: "conditional-http"}
        carrier: {route: "the retained columnar estate (estate-lite.db, estate.parquet)", robots: "admitted"}
        state: "ADMITTED"
        cannot_infer: "an owner of record and an assessed value are not a title search, an appraisal or a right to act"
    },
]
