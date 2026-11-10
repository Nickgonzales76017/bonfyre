package control

// Open, keyless content sources across the families people come for: what is
// on, what to cook, what to read, play, hear and look at, what science and
// government published, and what was recalled.  Same contract and apply tool
// as the commons rows; every mapping below was read from the provider's own
// response (2026-10-18), and each row's key field is the provider's record id,
// so records that share a landing page stay distinct.

nativeOpenContentSources: [...#CensusSource] & [
    // Watch
    {
        source: "tvmaze-web-schedule", family: "video", kind: "video"
        label: "streaming schedule", says: "episodes premiering on streaming services today"
        twin: "tvmaze-web-schedule", tier: "WARM", cadence_seconds: 21600
        url: "https://api.tvmaze.com/schedule/web?date={today-iso}"
        reading: {extract: ".", title_field: "_embedded.show.name", summary_field: "name", link_field: "url", meta_field: "airstamp",
            image_field: "_embedded.show.image.medium|image.medium", maker_field: "_embedded.show.webChannel.name|_embedded.show.network.name", key_field: "id"}
        note: "TVmaze lists what is scheduled; an episode listed is not one that aired"
    },
    // Food
    for letter in ["b", "c", "p", "s"] {
        source: "themealdb-\(letter)", family: "food", kind: "image"
        label: "recipes", says: "recipes from TheMealDB, with their cuisine and method"
        twin: "themealdb-\(letter)", tier: "COLD", cadence_seconds: 604800
        url: "https://www.themealdb.com/api/json/v1/1/search.php?f=\(letter)"
        reading: {extract: "meals", title_field: "strMeal", link_field: "idMeal", link_base: "https://www.themealdb.com/meal/",
            image_field: "strMealThumb", summary_field: "strInstructions", sub_field: "strArea", key_field: "idMeal"}
        note: "TheMealDB's test key reads its public recipe set"
    },
    for letter in ["g", "m", "r"] {
        source: "thecocktaildb-\(letter)", family: "food", kind: "image"
        label: "drinks", says: "drinks from TheCocktailDB, with glass and method"
        twin: "thecocktaildb-\(letter)", tier: "COLD", cadence_seconds: 604800
        url: "https://www.thecocktaildb.com/api/json/v1/1/search.php?f=\(letter)"
        reading: {extract: "drinks", title_field: "strDrink", link_field: "idDrink", link_base: "https://www.thecocktaildb.com/drink/",
            image_field: "strDrinkThumb", summary_field: "strInstructions", sub_field: "strCategory", key_field: "idDrink"}
        note: "TheCocktailDB's test key reads its public drink set"
    },
    // Read
    for slug, subject in {fiction: "fiction", cooking: "cooking", "science-fiction": "science_fiction"} {
        source: "openlibrary-new-\(slug)", family: "books", kind: "image"
        label: "new books", says: "newly catalogued \(slug) at Open Library"
        twin: "openlibrary-new-\(slug)", tier: "COLD", cadence_seconds: 86400
        url: "https://openlibrary.org/search.json?q=subject:\(subject)&sort=new&limit=40&fields=key,title,author_name,cover_i,first_publish_year"
        reading: {extract: "docs", title_field: "title", link_field: "key", link_base: "https://openlibrary.org",
            image_field: "cover_i", image_base: "https://covers.openlibrary.org/b/id/|-M.jpg", maker_field: "author_name", key_field: "key"}
        note: "Open Library's catalogue record, not a copy of the book"
    },
    // Listen
    {
        source: "radio-browser-top", family: "music", kind: "audio"
        label: "radio stations", says: "the stations people tune to most, playing live"
        twin: "radio-browser-top", tier: "WARM", cadence_seconds: 86400
        url: "https://de1.api.radio-browser.info/json/stations/topclick/60"
        reading: {extract: ".", title_field: "name", link_field: "homepage", media_field: "url_resolved", image_field: "favicon",
            sub_field: "country", summary_field: "tags", key_field: "stationuuid"}
        note: "a station's stream is the broadcaster's; Radio Browser lists where it plays"
    },
    // Play
    {
        source: "scryfall-sets", family: "gaming", kind: "image"
        label: "card sets", says: "Magic: The Gathering sets, newest first"
        twin: "scryfall-sets", tier: "COLD", cadence_seconds: 86400
        url: "https://api.scryfall.com/sets"
        reading: {extract: "data", title_field: "name", link_field: "scryfall_uri", image_field: "icon_svg_uri", meta_field: "released_at",
            sub_field: "set_type", key_field: "id"}
        note: "Scryfall's catalogue of sets; card images and text belong to their publisher"
    },
    {
        source: "cheapshark-deals", family: "gaming", kind: "image"
        label: "game deals", says: "PC game prices on sale across stores right now"
        twin: "cheapshark-deals", tier: "WARM", cadence_seconds: 21600
        url: "https://www.cheapshark.com/api/1.0/deals?pageSize=60&sortBy=Recent"
        reading: {extract: ".", title_field: "title", link_field: "dealID", link_base: "https://www.cheapshark.com/redirect?dealID=",
            image_field: "thumb", summary_field: "salePrice", sub_field: "steamRatingText", key_field: "dealID"}
        note: "a listed price is the store's as CheapShark last read it, not a quote"
    },
    // Sport
    {
        source: "openf1-meetings", family: "sports", kind: "image"
        label: "Formula 1 weekends", says: "this season's Formula 1 race weekends"
        twin: "openf1-meetings", tier: "COLD", cadence_seconds: 86400
        url: "https://api.openf1.org/v1/meetings?year=2026"
        reading: {extract: ".", title_field: "meeting_official_name", meta_field: "date_start", image_field: "circuit_image",
            sub_field: "location", summary_field: "circuit_short_name", key_field: "meeting_key"}
        note: "OpenF1's unofficial timing record of the season"
    },
    // Nature
    {
        source: "inaturalist-texas", family: "nature", kind: "image"
        label: "Texas sightings", says: "plants and animals people photographed in Texas today"
        twin: "inaturalist-texas", tier: "WARM", cadence_seconds: 21600
        url: "https://api.inaturalist.org/v1/observations?place_id=18&photos=true&per_page=60&order_by=observed_on&quality_grade=research"
        reading: {extract: "results", title_field: "taxon.preferred_common_name|taxon.name|species_guess", link_field: "uri",
            image_field: "photos.0.url", meta_field: "time_observed_at", maker_field: "user.login", rights_field: "photos.0.license_code", key_field: "id"}
        note: "an observation is what someone saw and how the community identified it, not a survey"
    },
    {
        source: "gbif-texas", family: "nature", kind: "image"
        label: "Texas biodiversity", says: "occurrence records with pictures from Texas collections"
        twin: "gbif-texas", tier: "COLD", cadence_seconds: 86400
        url: "https://api.gbif.org/v1/occurrence/search?country=US&stateProvince=Texas&mediaType=StillImage&limit=60"
        reading: {extract: "results", title_field: "species|scientificName", link_field: "key", link_base: "https://www.gbif.org/occurrence/",
            image_field: "media.0.identifier", meta_field: "eventDate", maker_field: "recordedBy|institutionCode", rights_field: "license", key_field: "key"}
        note: "a GBIF occurrence is its publishing institution's record"
    },
    // Hazards
    {
        source: "nws-alerts-texas", family: "hazards", kind: "text"
        label: "weather alerts", says: "active National Weather Service alerts in Texas"
        twin: "nws-alerts-texas", tier: "HOT", cadence_seconds: 3600
        url: "https://api.weather.gov/alerts/active?area=TX"
        reading: {extract: "features", title_field: "properties.headline|properties.event", link_field: "id", summary_field: "properties.description",
            meta_field: "properties.effective", sub_field: "properties.areaDesc", key_field: "properties.id"}
        note: "the alert as the National Weather Service issued it; it expires on its stated time"
    },
    {
        source: "usgs-quakes-week", family: "hazards", kind: "text"
        label: "earthquakes", says: "earthquakes of magnitude 2.5 or more this week"
        twin: "usgs-quakes-week", tier: "HOT", cadence_seconds: 3600
        url: "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_week.geojson"
        reading: {extract: "features", title_field: "properties.title", link_field: "properties.url", sub_field: "properties.place", key_field: "id"}
        note: "the USGS event as reviewed so far; magnitudes are revised"
    },
    // Talk
    {
        source: "hn-new", family: "hacker-attention", kind: "text"
        label: "Hacker News", says: "the newest stories on Hacker News"
        twin: "hn-new", tier: "WARM", cadence_seconds: 3600
        url: "https://hn.algolia.com/api/v1/search_by_date?tags=story&hitsPerPage=60"
        reading: {extract: "hits", title_field: "title", link_field: "url", meta_field: "created_at", maker_field: "author", key_field: "objectID"}
        note: "a story as submitted; its title is the submitter's"
    },
    {
        source: "lobsters-hottest", family: "forums", kind: "text"
        label: "Lobsters", says: "what programmers are discussing on Lobsters"
        twin: "lobsters-hottest", tier: "WARM", cadence_seconds: 3600
        url: "https://lobste.rs/hottest.json"
        reading: {extract: ".", title_field: "title", link_field: "url|comments_url", summary_field: "description_plain", meta_field: "created_at",
            maker_field: "submitter_user", key_field: "short_id"}
        note: "a story and its discussion as Lobsters lists them"
    },
    // Look
    {
        source: "artic-artworks", family: "museums", kind: "image"
        label: "Art Institute of Chicago", says: "artworks in the Art Institute of Chicago's collection"
        twin: "artic-artworks", tier: "COLD", cadence_seconds: 604800
        url: "https://api.artic.edu/api/v1/artworks?limit=60&fields=id,title,artist_display,image_id,date_display&query[term][is_public_domain]=true"
        reading: {extract: "data", title_field: "title", link_field: "id", link_base: "https://www.artic.edu/artworks/", image_field: "image_id",
            image_base: "https://www.artic.edu/iiif/2/|/full/843,/0/default.jpg", maker_field: "artist_display", summary_field: "date_display", key_field: "id"}
        note: "public-domain works as the museum publishes them"
    },
    {
        source: "cleveland-artworks", family: "museums", kind: "image"
        label: "Cleveland Museum of Art", says: "open-access artworks at the Cleveland Museum of Art"
        twin: "cleveland-artworks", tier: "COLD", cadence_seconds: 604800
        url: "https://openaccess-api.clevelandart.org/api/artworks/?limit=60&has_image=1&cc0=1"
        reading: {extract: "data", title_field: "title", link_field: "url", image_field: "images.web.url", maker_field: "creators.0.description",
            summary_field: "tombstone", rights_field: "share_license_status", key_field: "id"}
        note: "CC0 works as the museum publishes them"
    },
    // Science
    {
        source: "openalex-recent", family: "science", kind: "text"
        label: "new research", says: "open-access research published this week"
        twin: "openalex-recent", tier: "WARM", cadence_seconds: 86400
        url: "https://api.openalex.org/works?sort=publication_date:desc&per-page=60&filter=is_oa:true,from_publication_date:{week-ago-iso},to_publication_date:{today-iso}"
        reading: {extract: "results", title_field: "display_name", link_field: "doi|id", meta_field: "publication_date",
            maker_field: "primary_location.source.display_name", key_field: "id"}
        note: "OpenAlex's record of a work; the work itself stays with its publisher"
    },
    {
        source: "europepmc-recent", family: "science", kind: "text"
        label: "life-science papers", says: "open-access life-science papers, newest first"
        twin: "europepmc-recent", tier: "WARM", cadence_seconds: 86400
        url: "https://www.ebi.ac.uk/europepmc/webservices/rest/search?query=OPEN_ACCESS:y%20AND%20FIRST_PDATE:[{week-ago-iso}%20TO%20{today-iso}]&format=json&pageSize=60"
        reading: {extract: "resultList.result", title_field: "title", link_field: "doi", link_base: "https://doi.org/", maker_field: "authorString",
            summary_field: "journalTitle", meta_field: "firstPublicationDate", key_field: "id"}
        note: "Europe PMC's index entry for a paper"
    },
    // Government
    {
        source: "federal-register-new", family: "rulemaking", kind: "text"
        label: "Federal Register", says: "rules, notices and proposals published today"
        twin: "federal-register-new", tier: "WARM", cadence_seconds: 21600
        url: "https://www.federalregister.gov/api/v1/documents.json?order=newest&per_page=60"
        reading: {extract: "results", title_field: "title", link_field: "html_url", summary_field: "abstract", meta_field: "publication_date",
            maker_field: "agencies.0.name", sub_field: "type", key_field: "document_number"}
        note: "the document as the Federal Register published it"
    },
    // Recalls
    {
        source: "cpsc-recalls", family: "recalls", kind: "image"
        label: "product recalls", says: "consumer products recalled this month"
        twin: "cpsc-recalls", tier: "WARM", cadence_seconds: 21600
        url: "https://www.saferproducts.gov/RestWebServices/Recall?format=json&RecallDateStart={month-ago-iso}"
        reading: {extract: ".", title_field: "Title", link_field: "URL", summary_field: "Description", image_field: "Images.0.URL",
            meta_field: "RecallDate", key_field: "RecallID"}
        note: "the recall as the CPSC announced it"
    },
    {
        source: "openfda-food-recalls", family: "recalls", kind: "text"
        label: "food recalls", says: "food recalls the FDA reported most recently"
        twin: "openfda-food-recalls", tier: "WARM", cadence_seconds: 21600
        url: "https://api.fda.gov/food/enforcement.json?sort=report_date:desc&limit=60"
        reading: {extract: "results", title_field: "product_description", summary_field: "reason_for_recall", maker_field: "recalling_firm",
            sub_field: "classification", key_field: "recall_number"}
        note: "the FDA's enforcement report entry"
    },
]
