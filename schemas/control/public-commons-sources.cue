package control

// Census rows that realize admitted public-estate passports beyond Wikimedia
// (source-passport.cue).  Same contract, same apply tool, same law: each row
// reads through a route its host's robots rules admit, and every item keeps
// the terms its source states.

_archiveSearch: "https://archive.org/advancedsearch.php?output=json&rows=48&fl%5B%5D=identifier&fl%5B%5D=title&fl%5B%5D=creator&fl%5B%5D=date&fl%5B%5D=year&fl%5B%5D=description&fl%5B%5D=licenseurl&fl%5B%5D=venue"

_archiveReading: #CensusReading & {
    extract: "response.docs", title_field: "title", link_field: "identifier", link_base: "https://archive.org/details/"
    image_field: "identifier", image_base: "https://archive.org/services/img/", maker_field: "creator"
    rights_link_field: "licenseurl"
}

nativePublicCommonsSources: [...#CensusSource] & [
    {
        source: "archive-feature-films", family: "films", kind: "video"
        label: "film archive", says: "feature films the Internet Archive holds, most watched first"
        twin: "archive-feature-films", tier: "COLD", cadence_seconds: 604800
        url: _archiveSearch + "&sort%5B%5D=downloads+desc&q=collection%3Afeature_films+AND+mediatype%3Amovies+AND+year%3A%5B1920+TO+1970%5D+AND+NOT+subject%3A%28exploitation+OR+sexploitation+OR+nudity%29"
        reading: _archiveReading & {summary_field: "description"}
        note: "an item the Archive hosts is not thereby public domain; its stated licence, where it has one, is shown as stated"
    },
    {
        source: "archive-dallas-live-music", family: "live-music", kind: "audio"
        label: "live music archive", says: "concerts recorded in Dallas and shared by tapers, newest first"
        twin: "archive-dallas-live-music", tier: "COLD", cadence_seconds: 86400
        url: _archiveSearch + "&sort%5B%5D=date+desc&q=collection%3Aetree+AND+coverage%3A%28Dallas%29"
        reading: _archiveReading & {meta_field: "date", summary_field: "venue"}
        note: "a live recording is shared under the band's taping policy as the Archive records it; it is not a release or a licence"
    },
    {
        source: "archive-great-78", family: "music", kind: "audio"
        label: "78 rpm archive", says: "blues, jazz, country and folk records digitized from 78s"
        twin: "archive-great-78", tier: "COLD", cadence_seconds: 604800
        url: _archiveSearch + "&sort%5B%5D=downloads+desc&q=collection%3Ageorgeblood+AND+mediatype%3Aaudio+AND+subject%3A%28blues+OR+jazz+OR+country+OR+folk%29"
        reading: _archiveReading & {summary_field: "year"}
        note: "a digitized 78 is preserved by the Archive for research; its rights in any use are the reuser's to establish"
    },
    {
        source: "gbif-inaturalist-dfw", family: "local-nature", kind: "image"
        label: "species sightings", says: "plants and animals people photographed around Fort Worth and Dallas in the last month"
        twin: "gbif-inaturalist-dfw", tier: "COLD", cadence_seconds: 86400
        url: "https://api.gbif.org/v1/occurrence/search?datasetKey=50c9509d-22c7-4a22-a47d-8c48425ef4a7&decimalLatitude=32.55,33.10&decimalLongitude=-97.55,-96.55&mediaType=StillImage&eventDate={month-ago-iso},{today-iso}&limit=60"
        reading: {
            extract: "results", title_field: "vernacularName|species|genus|family|scientificName", meta_field: "eventDate"
            link_field: "references", image_field: "media.0.identifier", maker_field: "recordedBy|rightsHolder"
            summary_field: "scientificName", rights_link_field: "media.0.license|license"
        }
        note: "a research-grade sighting is a community identification at a place and time; it is not a survey, a range or a hazard"
    },
    {
        source: "podcasting-2-0", family: "podcasts", kind: "audio"
        label: "podcast feed", says: "the show about podcasting's open namespace, with chapters, transcripts and guests"
        twin: "podcasting-2-0", tier: "COLD", cadence_seconds: 86400
        url: "https://feeds.podcastindex.org/pc20.xml"
        reading: {extract: "rss:item", title_field: "title"}
        note: "a person or place an episode names is the show's statement; a transcript is the show's, not a verified record"
    },
    {
        source: "podcast-index-recent", family: "podcasts", kind: "audio"
        label: "podcast index", says: "episodes just published across open podcast feeds"
        twin: "podcast-index-recent", tier: "COLD", cadence_seconds: 21600
        url: "https://api.podcastindex.org/api/1.0/recent/episodes?max=40&lang=en"
        credential: "podcast-index"
        reading: {
            extract: "items", title_field: "title", meta_field: "datePublished", link_field: "link|feedUrl"
            image_field: "image|feedImage", maker_field: "feedTitle", summary_field: "description", media_field: "enclosureUrl"
        }
        note: "an episode listed by the index is its publisher's claim; reading it needs the operator's own Podcast Index key"
    },
    {
        source: "buzzcast", family: "podcasts", kind: "audio"
        label: "podcast feed", says: "a show about making podcasts, with chapters and transcripts"
        twin: "buzzcast", tier: "COLD", cadence_seconds: 86400
        url: "https://feeds.buzzsprout.com/231452.rss"
        reading: {extract: "rss:item", title_field: "title"}
        note: "a person or place an episode names is the show's statement; a transcript is the show's, not a verified record"
    },
]
