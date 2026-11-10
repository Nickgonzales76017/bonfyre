package control

// Census rows for Wikimedia's free knowledge, read only through carriers
// whose robots rules admit a retention crawler.
//
// commons.wikimedia.org/w/ and query.wikidata.org/sparql refuse crawlers, and
// the estate honours that.  The same public knowledge is published elsewhere
// under open terms: Wikimedia's own feed gateway (api.wikimedia.org, whose
// robots address is a documentation page with no rules) and QLever
// (qlever.dev, University of Freiburg), which indexes the Wikidata and
// Wikimedia Commons dumps and answers SPARQL with no robots rules at all.
// Reading a dump's index is not reading Wikimedia's servers; the files a page
// shows are still loaded by the reader's own browser from Wikimedia, with the
// credit and terms Commons states.
//
// A row is presentation input only: it names where to read and how to read
// what came back.  It is not a licence, an endorsement or a claim that what a
// source says is true.  Rows are written into the census through the CMS
// binary by cmd/BonfyreWebAddress/tools/apply_census_sources.py.

#CensusReading: {
    extract: string & != ""
    title_field: string & != ""
    meta_field: *"" | string
    link_field: *"" | string
    link_base: *"" | string
    image_field: *"" | string
    image_base: *"" | string
    sub_field: *"" | string
    summary_field: *"" | string
    maker_field: *"" | string
    rights_field: *"" | string
    rights_link_field: *"" | string
    media_field: *"" | string
    ident_field: *"" | string
    // The identity a thing is about or depicts, which is not its own identity.
    about_field: *"" | string
    place_field: *"" | string
    // The provider's own record id: the item key when two records share a page or a title.
    key_field: *"" | string
}

#CensusSource: {
    source: string & =~"^[a-z0-9-]+$"
    family: string & =~"^[a-z0-9-]+$"
    kind: "text" | "image" | "video" | "audio"
    label: string & != ""
    says: string & != ""
    twin: string & =~"^[a-z0-9-]+$"
    tier: "HOT" | "WARM" | "COLD"
    cadence_seconds: int & >=3600
    front_door: *"yes" | "no"
    // A provider declared in provider-credentials.cue whose credential the read needs.
    credential?: string & =~"^[a-z0-9-]+$"
    // A feed row names its address.  A query row names a QLever dataset and
    // its SPARQL; the address is rendered from them.
    url?: string & =~"^https://"
    dataset?: "wikidata" | "wikimedia-commons"
    query?: string & != ""
    reading: #CensusReading
    note: string & != ""
}

// Licence items as Commons structured data names them, in words and deeds.
_licenceWords: """
    VALUES (?lic ?lw ?ld) {
      (wd:Q6938433 "CC0" "https://creativecommons.org/publicdomain/zero/1.0/")
      (wd:Q18199165 "CC BY-SA 4.0" "https://creativecommons.org/licenses/by-sa/4.0/")
      (wd:Q20007257 "CC BY 4.0" "https://creativecommons.org/licenses/by/4.0/")
      (wd:Q14946043 "CC BY-SA 3.0" "https://creativecommons.org/licenses/by-sa/3.0/")
      (wd:Q14947546 "CC BY 3.0" "https://creativecommons.org/licenses/by/3.0/")
      (wd:Q19068220 "CC BY-SA 2.0" "https://creativecommons.org/licenses/by-sa/2.0/")
      (wd:Q19125117 "CC BY 2.0" "https://creativecommons.org/licenses/by/2.0/")
      (wd:Q18810333 "CC BY-SA 2.5" "https://creativecommons.org/licenses/by-sa/2.5/")
      (wd:Q18810341 "CC BY 2.5" "https://creativecommons.org/licenses/by/2.5/")
    }
    """

_prefixes: "PREFIX wd: <http://www.wikidata.org/entity/> PREFIX wdt: <http://www.wikidata.org/prop/direct/> PREFIX p: <http://www.wikidata.org/prop/> PREFIX pq: <http://www.wikidata.org/prop/qualifier/> PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> PREFIX schema: <http://schema.org/> PREFIX wikibase: <http://wikiba.se/ontology#> PREFIX geof: <http://www.opengis.net/def/function/geosparql/>"

// Today's feed carries today's article, picture and anniversaries from the
// first minutes of the day; its most-read list arrives only once the day's
// counts exist, so most-read is read from yesterday's complete feed.
_featured: "https://api.wikimedia.org/feed/v1/wikipedia/en/featured/{today}"
_featuredComplete: "https://api.wikimedia.org/feed/v1/wikipedia/en/featured/{yesterday}"

nativeWikimediaSources: [...#CensusSource] & [
    {
        // The row that already exists: its reading grows to the picture, the
        // link, the article's first paragraph, its identity and its place.
        source: "wikipedia-most-read", family: "attention", kind: "text"
        label: "hot web", says: "what the world is reading right now"
        twin: "wikipedia-most-read", tier: "WARM", cadence_seconds: 21600
        url: _featuredComplete
        reading: {
            extract: "mostread.articles", title_field: "normalizedtitle", meta_field: "views"
            link_field: "content_urls.desktop.page", image_field: "thumbnail.source"
            summary_field: "extract", ident_field: "wikibase_item", place_field: "coordinates"
        }
        note: "a page many people read is not an important or a true one"
    },
    {
        source: "wikipedia-featured-article", family: "history", kind: "text"
        label: "editors' choice", says: "the article Wikipedia's editors chose for today"
        twin: "wikipedia-featured-today", tier: "WARM", cadence_seconds: 21600
        url: _featured
        reading: {
            extract: "tfa", title_field: "normalizedtitle", link_field: "content_urls.desktop.page"
            image_field: "thumbnail.source", summary_field: "extract"
            ident_field: "wikibase_item", place_field: "coordinates"
        }
        note: "a featured article meets Wikipedia's own standard; it is not this estate's recommendation"
    },
    {
        source: "wikimedia-picture-of-the-day", family: "commons", kind: "image"
        label: "editors' choice", says: "the picture Wikimedia Commons chose for today"
        twin: "wikipedia-featured-today", tier: "WARM", cadence_seconds: 21600
        url: _featured
        reading: {
            extract: "image", title_field: "title", link_field: "file_page", image_field: "thumbnail.source"
            summary_field: "description.text|description.html", maker_field: "artist.text|artist.html"
            rights_field: "license.type", rights_link_field: "license.url"
        }
        note: "the terms shown are the ones Commons states for the file; reusing it is between the reuser and its author"
    },
    {
        source: "wikipedia-on-this-day", family: "history", kind: "text"
        label: "almanac", says: "what happened on this date in other years"
        twin: "wikipedia-featured-today", tier: "WARM", cadence_seconds: 21600
        url: _featured
        reading: {
            extract: "onthisday", title_field: "text", maker_field: "year"
            link_field: "pages.0.content_urls.desktop.page", image_field: "pages.0.thumbnail.source"
            summary_field: "pages.0.extract", ident_field: "pages.0.wikibase_item", place_field: "pages.0.coordinates"
        }
        note: "an event recorded for a date is an encyclopedia's account, not a verified one"
    },
    {
        source: "commons-featured-pictures", family: "commons", kind: "image"
        label: "photographers", says: "pictures the Commons community voted its best, newest first"
        twin: "commons-featured-pictures", tier: "COLD", cadence_seconds: 86400
        dataset: "wikimedia-commons"
        query: """
            \(_prefixes)
            SELECT ?file (SAMPLE(?cap) AS ?caption) (SAMPLE(?nm) AS ?name) (SAMPLE(?pic) AS ?picture) (SAMPLE(?pg) AS ?page)
                   (SAMPLE(?au) AS ?author) (SAMPLE(?un) AS ?user) (SAMPLE(?dep) AS ?depicts) (SAMPLE(?lw) AS ?license) (SAMPLE(?ld) AS ?deed) (MAX(?m) AS ?modified) WHERE {
              ?file wdt:P6731 wd:Q63348049 ; schema:contentUrl ?u ; schema:dateModified ?m ; schema:encodingFormat "image/jpeg" .
              BIND(REPLACE(IF(CONTAINS(STR(?u), "?"), STRBEFORE(STR(?u), "?"), STR(?u)), "^.*/", "") AS ?nm)
              BIND(CONCAT("https://commons.wikimedia.org/wiki/Special:FilePath/", ?nm, "?width=500") AS ?pic)
              BIND(CONCAT("https://commons.wikimedia.org/wiki/File:", ?nm) AS ?pg)
              OPTIONAL { ?file rdfs:label ?cap . FILTER(LANG(?cap) = "en") }
              OPTIONAL { ?file p:P170 ?cs . ?cs pq:P2093 ?au }
              OPTIONAL { ?file p:P170 ?cu . ?cu pq:P4174 ?un }
              OPTIONAL { ?file wdt:P180 ?dep }
              OPTIONAL { ?file wdt:P275 ?lic . \(_licenceWords) }
            } GROUP BY ?file ORDER BY DESC(?modified) LIMIT 48
            """
        reading: {
            extract: "results.bindings", title_field: "caption.value|name.value", link_field: "page.value"
            image_field: "picture.value", maker_field: "author.value|user.value"
            rights_field: "license.value", rights_link_field: "deed.value", about_field: "depicts.value"
        }
        note: "a featured picture is the Commons community's choice; the terms are what Commons states for each file"
    },
    {
        source: "commons-dfw-rated", family: "local-photos", kind: "image"
        label: "photographers", says: "photographs of Fort Worth and Dallas that the Commons community rated for quality, newest first"
        twin: "commons-dfw-rated", tier: "COLD", cadence_seconds: 86400
        dataset: "wikimedia-commons"
        query: """
            \(_prefixes)
            SELECT ?file (SAMPLE(?cap) AS ?caption) (SAMPLE(?nm) AS ?name) (SAMPLE(?pic) AS ?picture) (SAMPLE(?pg) AS ?page)
                   (SAMPLE(?au) AS ?author) (SAMPLE(?un) AS ?user) (SAMPLE(?dep) AS ?depicts) (SAMPLE(?lw) AS ?license) (SAMPLE(?ld) AS ?deed) (SAMPLE(?c) AS ?geometry) (MAX(?t) AS ?taken) WHERE {
              VALUES ?rated { wd:Q63348049 wd:Q63348069 wd:Q63348040 }
              ?file wdt:P6731 ?rated ; wdt:P1259 ?c ; schema:contentUrl ?u ; schema:encodingFormat "image/jpeg" .
              OPTIONAL { ?file wdt:P571 ?t }
              BIND(geof:longitude(?c) AS ?lon) BIND(geof:latitude(?c) AS ?lat)
              FILTER(?lon > -97.55 && ?lon < -96.55 && ?lat > 32.55 && ?lat < 33.10)
              BIND(REPLACE(IF(CONTAINS(STR(?u), "?"), STRBEFORE(STR(?u), "?"), STR(?u)), "^.*/", "") AS ?nm)
              BIND(CONCAT("https://commons.wikimedia.org/wiki/Special:FilePath/", ?nm, "?width=500") AS ?pic)
              BIND(CONCAT("https://commons.wikimedia.org/wiki/File:", ?nm) AS ?pg)
              OPTIONAL { ?file rdfs:label ?cap . FILTER(LANG(?cap) = "en") }
              OPTIONAL { ?file p:P170 ?cs . ?cs pq:P2093 ?au }
              OPTIONAL { ?file p:P170 ?cu . ?cu pq:P4174 ?un }
              OPTIONAL { ?file wdt:P180 ?dep }
              OPTIONAL { ?file wdt:P275 ?lic . \(_licenceWords) }
            } GROUP BY ?file ORDER BY DESC(?taken) LIMIT 60
            """
        reading: {
            extract: "results.bindings", title_field: "caption.value|name.value", meta_field: "taken.value"
            link_field: "page.value", image_field: "picture.value", maker_field: "author.value|user.value"
            rights_field: "license.value", rights_link_field: "deed.value", about_field: "depicts.value", place_field: "geometry.value"
        }
        note: "a photograph's place is where its camera said it was; it does not show who or what is in it or that it is current"
    },
    {
        source: "wikidata-places-dfw", family: "places", kind: "image"
        label: "gazetteer", says: "landmarks and neighbourhoods of Tarrant and Dallas counties, most written-about first"
        twin: "wikidata-places-dfw", tier: "COLD", cadence_seconds: 604800
        dataset: "wikidata"
        query: """
            \(_prefixes)
            SELECT ?place (SAMPLE(?l) AS ?name) (SAMPLE(?d) AS ?about) (SAMPLE(?pic) AS ?picture) (SAMPLE(?g) AS ?geometry)
                   (SAMPLE(?link) AS ?page) (MAX(?n) AS ?links) WHERE {
              VALUES ?county { wd:Q110412 wd:Q111168 }
              ?place wdt:P131+ ?county ; wdt:P625 ?g ; wdt:P18 ?i ; wikibase:sitelinks ?n ; rdfs:label ?l . FILTER(LANG(?l) = "en")
              FILTER NOT EXISTS { ?place wdt:P31 wd:Q5 }
              BIND(CONCAT(REPLACE(STR(?i), "^http:", "https:"), "?width=500") AS ?pic)
              OPTIONAL { ?place schema:description ?d . FILTER(LANG(?d) = "en") }
              OPTIONAL { ?w schema:about ?place ; schema:isPartOf <https://en.wikipedia.org/> }
              BIND(COALESCE(STR(?w), CONCAT("https://www.wikidata.org/wiki/", STRAFTER(STR(?place), "entity/"))) AS ?link)
            } GROUP BY ?place ORDER BY DESC(?links) LIMIT 60
            """
        reading: {
            extract: "results.bindings", title_field: "name.value", link_field: "page.value", image_field: "picture.value"
            summary_field: "about.value", ident_field: "place.value", place_field: "geometry.value"
        }
        note: "a place here is what Wikidata records about it; it is not a boundary, a property record, an address or a recommendation to visit"
    },
    {
        source: "wikidata-public-domain-films", family: "films", kind: "video"
        label: "film archive", says: "films old enough to belong to everyone, whole, newest first"
        twin: "wikidata-public-domain-films", tier: "COLD", cadence_seconds: 604800
        dataset: "wikidata"
        query: """
            \(_prefixes)
            SELECT ?film (SAMPLE(?t) AS ?title) (SAMPLE(?made) AS ?maker) (SAMPLE(?fd) AS ?about) (SAMPLE(?v) AS ?video) (SAMPLE(?poster) AS ?picture)
                   (SAMPLE(?link) AS ?page) (SAMPLE(?pd) AS ?terms) (MAX(?n) AS ?links) WHERE {
              BIND("Public domain" AS ?pd)
              ?film wdt:P31 wd:Q11424 ; wdt:P10 ?file ; wdt:P6216 wd:Q19652 ; wikibase:sitelinks ?n ; rdfs:label ?t . FILTER(LANG(?t) = "en")
              FILTER(!CONTAINS(LCASE(STR(?file)), "trailer"))
              BIND(REPLACE(STR(?file), "^http:", "https:") AS ?v)
              BIND(CONCAT(?v, "?width=500") AS ?poster)
              OPTIONAL { ?film wdt:P577 ?date } BIND(YEAR(?date) AS ?year)
              OPTIONAL { ?film wdt:P57 ?dir . ?dir rdfs:label ?dl . FILTER(LANG(?dl) = "en") }
              BIND(COALESCE(?dl, "") AS ?made)
              OPTIONAL { ?film schema:description ?fd . FILTER(LANG(?fd) = "en") }
              OPTIONAL { ?w schema:about ?film ; schema:isPartOf <https://en.wikipedia.org/> }
              BIND(COALESCE(STR(?w), CONCAT("https://www.wikidata.org/wiki/", STRAFTER(STR(?film), "entity/"))) AS ?link)
            } GROUP BY ?film HAVING(MAX(?n) >= 20) ORDER BY DESC(MAX(?year)) LIMIT 40
            """
        reading: {
            extract: "results.bindings", title_field: "title.value", link_field: "page.value", image_field: "picture.value"
            maker_field: "maker.value", summary_field: "about.value", media_field: "video.value", ident_field: "film.value"
            rights_field: "terms.value"
        }
        note: "a film's public-domain status is what Wikidata records for the United States; it plays from Wikimedia Commons and is not hosted here"
    },
    {
        source: "wikidata-recorded-music", family: "music", kind: "audio"
        label: "music archive", says: "recorded music, from symphonies to songs, that anyone may hear"
        twin: "wikidata-recorded-music", tier: "COLD", cadence_seconds: 604800
        dataset: "wikidata"
        query: """
            \(_prefixes)
            SELECT ?work (SAMPLE(?t) AS ?title) (SAMPLE(?c) AS ?composer) (SAMPLE(?a) AS ?audio) (SAMPLE(?link) AS ?page) (MAX(?n) AS ?links) WHERE {
              ?work wdt:P51 ?file ; wdt:P86 ?cm ; wikibase:sitelinks ?n ; rdfs:label ?t . FILTER(LANG(?t) = "en")
              FILTER NOT EXISTS { ?work wdt:P31 wd:Q23691 }
              ?cm rdfs:label ?c . FILTER(LANG(?c) = "en")
              BIND(REPLACE(STR(?file), "^http:", "https:") AS ?a)
              OPTIONAL { ?w schema:about ?work ; schema:isPartOf <https://en.wikipedia.org/> }
              BIND(COALESCE(STR(?w), CONCAT("https://www.wikidata.org/wiki/", STRAFTER(STR(?work), "entity/"))) AS ?link)
            } GROUP BY ?work ORDER BY DESC(?links) LIMIT 48
            """
        reading: {
            extract: "results.bindings", title_field: "title.value", link_field: "page.value"
            maker_field: "composer.value", media_field: "audio.value", ident_field: "work.value"
        }
        note: "a recording plays from Wikimedia Commons under the terms its file page states; the composer is who Wikidata records, not the performer"
    },
]
