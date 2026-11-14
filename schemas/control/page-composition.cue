package control

// A subject's page is composed from the material it actually has.  Each region
// is a reading rhythm people already know from a mature product; a region
// exists only when material of its kind is present, and nothing on the page
// names the machinery that assembled it.
//
// This is presentation only.  A region cannot choose a source, admit
// material, change a subject, or offer an effect: material still comes from
// its canonical owner, and the World's published fragments are the only edges
// from a page to the subjects it shows.
#PageRegion: {
    // Internal coordinate.  Never rendered.
    role: "lead" | "watch" | "photos" | "places" | "latest" | "topics" | "listen" | "talk" | "more" |
          "feed" | "wall" | "grid" | "communities" | "table" | "results" | "playlist" |
          "happening" | "soon" | "stage" | "parts" | "named" | "channels"
    // The words a person sees.  An empty label means the region needs no
    // heading because its content already says what it is.
    label: string
    // Which normalized material stream the region consumes.  Regions that
    // share a stream consume it in order: the lead takes the newest story, the
    // index takes the next ones, and the compact list takes what remains.
    // "mix" is every retained item of the page in one time order, whatever its
    // kind, for rhythms that interleave pictures, video and headlines.
    // "event" is something that happens at a time and usually a place: it is
    // read from what its organizer publishes, and it is not a publication
    // about an event.
    //
    // Three kinds are relations rather than things published: "passage" is a
    // part of one piece as its publisher divided it, "organization" is
    // someone a piece names and links, and "channel" is a way a publisher puts
    // things out.  A page composed from what it relates to is not thin just
    // because it holds one story.
    material: "publication" | "video" | "photo" | "place" | "audio" | "post" | "event" | "mix" |
              "passage" | "organization" | "channel"
    // The DOM topology.  Two regions with different topologies may not be
    // lowered through the same markup with a different class.
    topology: "lead" | "shelf" | "field" | "map" | "index" | "tracks" | "thread" | "list" |
              "feed" | "wall" | "grid" | "communities" | "table" | "results" | "agenda"
    // Where the reading rhythm was studied.  Provenance only; no donor code
    // runs and no donor vocabulary reaches the page.
    language: string & != ""
    min_items: int & >=1
    max_items: int & >=min_items & <=50
}

// A World fragment's subject address names a subject in another owner.  The
// scheme decides which canonical reader normalizes it, so C never matches an
// application or grammar name to find media or conversation.
#PageSubjectScheme: {
    scheme: "community" | "recording" | "program"
    material: "post" | "audio" | "video"
}

// A source's census row says what kind of thing it publishes.  An item from a
// video or image source that carries a picture joins that rhythm; the same
// item without a picture is only a headline, and every other kind is a story.
// A recording joins the listening rhythm only when it carries a file that
// plays; without one it is only a headline.
#PageSourceKind: {
    kind: "video" | "image" | "audio" | "event"
    material: "video" | "photo" | "audio" | "event"
}

// A source family is an acquisition and routing fact, not a statement that
// every item it carries is itself a release, advisory, or rule.  These narrow
// contracts are the additional evidence a high-claim reading route requires
// before it may call an item one of those things.  `host_suffix` is a source
// publisher/address contract; it is never inferred from an item title alone.
// An empty `title_terms` means the named publisher's feed is itself the
// asserted form.  Otherwise at least one declared term must occur in the
// item's title as well as the source contract holding.
#PageRouteSourceContract: {
    route: "/" | "/releases" | "/advisories"
    territory: "any" | "work" | "shop"
    host_suffix: string & =~"^[a-z0-9.-]+$"
    title_terms: [...string & =~"^[a-z0-9][a-z0-9 .:+_-]*$"]
}

// This deliberately starts smaller than the source families.  A forum thread
// about a release and a generic IT discussion may remain in Discover or its
// source ledger, but neither is a release/advisory occurrence merely because
// the channel was filed under gaming or vulnerabilities.
nativePageRouteSourceContracts: [...#PageRouteSourceContract] & [
    {route: "/releases", territory: "any", host_suffix: "gamespress.com", title_terms: []},
    {route: "/releases", territory: "any", host_suffix: "stonemaiergames.com", title_terms: ["release", "available", "launch"]},
    {route: "/advisories", territory: "any", host_suffix: "threatfilter.dev", title_terms: []},
    {route: "/advisories", territory: "any", host_suffix: "linuxsecurity.com", title_terms: []},
    {route: "/advisories", territory: "any", host_suffix: "oss-security.openwall.org", title_terms: []},
    {route: "/advisories", territory: "any", host_suffix: "mend.io", title_terms: []},
    {route: "/advisories", territory: "any", host_suffix: "tenable.com", title_terms: []},
    // Work's lead is an entry into these two verified reading cuts, never a
    // license for a generic vulnerabilities-family source to pose as one.
    {route: "/", territory: "work", host_suffix: "threatfilter.dev", title_terms: []},
    {route: "/", territory: "work", host_suffix: "linuxsecurity.com", title_terms: []},
    {route: "/", territory: "work", host_suffix: "oss-security.openwall.org", title_terms: []},
    {route: "/", territory: "work", host_suffix: "mend.io", title_terms: []},
    {route: "/", territory: "work", host_suffix: "tenable.com", title_terms: []},
]

nativePageSourceKinds: [...#PageSourceKind] & [
    {kind: "video", material: "video"},
    {kind: "image", material: "photo"},
    {kind: "audio", material: "audio"},
    {kind: "event", material: "event"},
]

// What a person calls a published way into the same subject.  These are the
// page's tabs.  A grammar with no label here is not linked: an internal name
// is never a navigation word.
#PageGrammarLabel: {
    grammar: string & =~"^[a-z][a-z0-9-]*$"
    label: string & != ""
}

nativePageRegions: [...#PageRegion] & [
    {role: "lead", label: "", material: "publication", topology: "lead", language: "ghost-broadsheet", min_items: 1, max_items: 1},
    {role: "watch", label: "Watch", material: "video", topology: "shelf", language: "youtube-shelf", min_items: 1, max_items: 12},
    {role: "photos", label: "Photos", material: "photo", topology: "field", language: "pixelfed-grid", min_items: 2, max_items: 24},
    {role: "places", label: "Where", material: "place", topology: "map", language: "maplibre-points", min_items: 3, max_items: 50},
    {role: "latest", label: "Latest", material: "publication", topology: "index", language: "character", min_items: 1, max_items: 12},
    {role: "stage", label: "", material: "event", topology: "lead", language: "stage", min_items: 1, max_items: 1},
    {role: "happening", label: "What's on", material: "event", topology: "agenda", language: "calendar-agenda", min_items: 1, max_items: 40},
    {role: "soon", label: "Starting soon", material: "event", topology: "agenda", language: "calendar-agenda", min_items: 1, max_items: 8},
    {role: "topics", label: "", material: "publication", topology: "thread", language: "zulip-streams", min_items: 1, max_items: 48},
    {role: "listen", label: "Listen", material: "audio", topology: "tracks", language: "spotify-tracklist", min_items: 1, max_items: 20},
    {role: "talk", label: "What people are saying", material: "post", topology: "thread", language: "zulip-topic", min_items: 1, max_items: 20},
    {role: "more", label: "More", material: "publication", topology: "list", language: "ghost-wire", min_items: 1, max_items: 30},
    {role: "feed", label: "", material: "mix", topology: "feed", language: "pinterest-home-feed", min_items: 1, max_items: 50},
    {role: "wall", label: "", material: "photo", topology: "wall", language: "pinry-masonry", min_items: 1, max_items: 50},
    {role: "grid", label: "", material: "video", topology: "grid", language: "youtube-browse-grid", min_items: 1, max_items: 50},
    {role: "communities", label: "", material: "publication", topology: "communities", language: "lemmy-community-list", min_items: 1, max_items: 50},
    {role: "table", label: "", material: "publication", topology: "table", language: "linear-issue-list", min_items: 1, max_items: 50},
    {role: "results", label: "", material: "mix", topology: "results", language: "search-results", min_items: 1, max_items: 50},
    {role: "playlist", label: "", material: "audio", topology: "tracks", language: "spotify-tracklist", min_items: 1, max_items: 50},
    // Relations, appended so every earlier region keeps its index.
    {role: "parts", label: "In the piece", material: "passage", topology: "index", language: "ghost-table-of-contents", min_items: 1, max_items: 12},
    {role: "named", label: "Named in it", material: "organization", topology: "index", language: "wikipedia-infobox-links", min_items: 1, max_items: 24},
    {role: "channels", label: "Where they publish", material: "channel", topology: "table", language: "linear-issue-list", min_items: 1, max_items: 12},
]

nativePageSubjectSchemes: [...#PageSubjectScheme] & [
    {scheme: "community", material: "post"},
    {scheme: "recording", material: "audio"},
    {scheme: "program", material: "video"},
]

nativePageGrammarLabels: [...#PageGrammarLabel] & [
    {grammar: "discover", label: "Discover"},
    {grammar: "stage", label: "Schedule"},
    {grammar: "spatial", label: "Map"},
    {grammar: "board", label: "Boards"},
    {grammar: "community", label: "Community"},
    {grammar: "channels", label: "Channels"},
    {grammar: "listings", label: "Listings"},
    {grammar: "parcels", label: "Properties"},
    {grammar: "observations", label: "Updates"},
    {grammar: "territory", label: "Area"},
    {grammar: "watch", label: "Watch"},
    {grammar: "listen", label: "Listen"},
    {grammar: "market", label: "Shop"},
    {grammar: "inbox", label: "Inbox"},
    {grammar: "worlds", label: "Pages"},
    {grammar: "packs", label: "Packs"},
    {grammar: "link", label: "Accounts"},
    {grammar: "studio", label: "Studio"},
    {grammar: "console", label: "Console"},
]

// Some publishers speak through a relay or API host whose domain would name
// the relay instead of them.  These hosts are authored, exact, and only ever
// replace a byline; they never select or admit material.
// A host that answers for several publishers names each by its path.
#PagePublisherHost: { host: string & =~"^[a-z0-9.-]+$", path: *"" | =~"^/[A-Za-z0-9/._-]+$", publisher: string & != "" }
nativePagePublisherHosts: [...#PagePublisherHost] & [
    {host: "hn.algolia.com", publisher: "Hacker News"},
    {host: "hacker-news.firebaseio.com", publisher: "Hacker News"},
    {host: "feeds.bbci.co.uk", publisher: "BBC"},
    {host: "api.wikimedia.org", publisher: "Wikipedia"},
    {host: "wikimedia.org", publisher: "Wikipedia"},
    {host: "public.api.bsky.app", publisher: "Bluesky"},
    {host: "sepiasearch.org", publisher: "PeerTube"},
    {host: "store.steampowered.com", publisher: "Steam"},
    {host: "en.wikipedia.org", publisher: "Wikipedia"},
    {host: "www.wikidata.org", publisher: "Wikidata"},
    {host: "commons.wikimedia.org", publisher: "Wikimedia Commons"},
    {host: "archive.org", publisher: "Internet Archive"},
    {host: "www.inaturalist.org", publisher: "iNaturalist"},
    {host: "api.gbif.org", publisher: "iNaturalist via GBIF"},
    {host: "feeds.podcastindex.org", publisher: "Podcasting 2.0"},
    {host: "qlever.dev", path: "/api/wikidata", publisher: "Wikidata"},
    {host: "qlever.dev", path: "/api/wikimedia-commons", publisher: "Wikimedia Commons"},
]

// Where a picture or a playable file is kept, and the page there that states
// who made it and the terms it may be reused under.  A file held by a
// commons is credited to that commons with a link to its own page, whichever
// source carried it here; the page never states terms the file's page does
// not.  "hashed" paths carry two directory levels before the file name, and a
// "sized" address asks the commons itself for a picture at a width, which it
// never enlarges past the original.
#PagePictureOrigin: {
    host: string & =~"^[a-z0-9.-]+$"
    path: string & =~"^/"
    hashed: bool
    name: string & != ""
    page: string & =~"^https://"
    // {name} is the file's name, {width} the width asked for, {size} the
    // word the host uses for the nearest size in "sizes" ("500:medium"), and
    // {ext} the original file's extension.
    sized: string & =~"^https://.*[{]name[}].*[{](width|size)[}]"
    sizes: *"" | =~"^[0-9]+:[a-z]+(,[0-9]+:[a-z]+)*$"
    // A host whose given addresses are full-size originals is asked for a
    // sized picture in every slot, not only the large ones.
    everywhere: *false | bool
}
nativePagePictureOrigins: [...#PagePictureOrigin] & [
    {host: "upload.wikimedia.org", path: "/wikipedia/commons/", hashed: true, name: "Wikimedia Commons", page: "https://commons.wikimedia.org/wiki/File:", sized: "https://commons.wikimedia.org/wiki/Special:FilePath/{name}?width={width}"},
    {host: "thumb.wikimedia.org", path: "/wikipedia/commons/", hashed: true, name: "Wikimedia Commons", page: "https://commons.wikimedia.org/wiki/File:", sized: "https://commons.wikimedia.org/wiki/Special:FilePath/{name}?width={width}"},
    {host: "commons.wikimedia.org", path: "/wiki/Special:FilePath/", hashed: false, name: "Wikimedia Commons", page: "https://commons.wikimedia.org/wiki/File:", sized: "https://commons.wikimedia.org/wiki/Special:FilePath/{name}?width={width}"},
    {host: "inaturalist-open-data.s3.amazonaws.com", path: "/photos/", hashed: false, name: "iNaturalist", page: "https://www.inaturalist.org/photos/", sized: "https://inaturalist-open-data.s3.amazonaws.com/photos/{name}/{size}.{ext}", sizes: "75:square,240:small,500:medium,1024:large", everywhere: true},
]

// What a stated licence lets a person do, in the words people use for it.
// "match" is a lower-case fragment of the licence's address or words; the
// first match wins, so narrower licences come first.  A licence nobody
// recognises is shown as its source wrote it and promises nothing.
#PageLicence: {
    match: string & =~"^[a-z0-9 ./-]+$"
    words: string & != ""
    commercial: bool
    adapt: bool
    credit: bool
    share_alike: bool
}
nativePageLicences: [...#PageLicence] & [
    {match: "publicdomain/zero", words: "CC0", commercial: true, adapt: true, credit: false, share_alike: false},
    {match: "cc0", words: "CC0", commercial: true, adapt: true, credit: false, share_alike: false},
    {match: "publicdomain/mark", words: "Public domain", commercial: true, adapt: true, credit: false, share_alike: false},
    {match: "licenses/publicdomain", words: "Public domain", commercial: true, adapt: true, credit: false, share_alike: false},
    {match: "public domain", words: "Public domain", commercial: true, adapt: true, credit: false, share_alike: false},
    {match: "by-nc-nd", words: "CC BY-NC-ND", commercial: false, adapt: false, credit: true, share_alike: false},
    {match: "by-nc-sa", words: "CC BY-NC-SA", commercial: false, adapt: true, credit: true, share_alike: true},
    {match: "by-nc", words: "CC BY-NC", commercial: false, adapt: true, credit: true, share_alike: false},
    {match: "by-nd", words: "CC BY-ND", commercial: true, adapt: false, credit: true, share_alike: false},
    {match: "by-sa", words: "CC BY-SA", commercial: true, adapt: true, credit: true, share_alike: true},
    {match: "licenses/by/", words: "CC BY", commercial: true, adapt: true, credit: true, share_alike: false},
    {match: "cc by ", words: "CC BY", commercial: true, adapt: true, credit: true, share_alike: false},
]

// A page address a host can also play inside another page, through the
// player the host itself publishes for that purpose.
#PageEmbedHost: {
    host: string & =~"^[a-z0-9.-]+$"
    path: string & =~"^/"
    embed: string & =~"^https://.*[{]id[}]"
}
nativePageEmbedHosts: [...#PageEmbedHost] & [
    {host: "archive.org", path: "/details/", embed: "https://archive.org/embed/{id}"},
]

// Pictures whose terms do not travel with them.  A Wikipedia's own uploads
// are largely non-free material used under a claim that holds only on the
// article that uses it, so a picture kept there is not shown on a page here,
// whatever carried its address.
#PageWithheldPicture: { host: string & =~"^[a-z0-9.-]+$", path: string & =~"^/", why: string & != "" }
nativePageWithheldPictures: [...#PageWithheldPicture] & [
    {host: "upload.wikimedia.org", path: "/wikipedia/en/", why: "an English Wikipedia upload is often non-free and usable only on its own article"},
    {host: "thumb.wikimedia.org", path: "/wikipedia/en/", why: "an English Wikipedia upload is often non-free and usable only on its own article"},
]

// An addressed World page uses these rhythms, in this order.  Topics is a
// main-route rhythm: on a World it would take the stories More should keep.
nativePageWorldRegions: [...#PageRegion.role] & ["lead", "watch", "photos", "places", "latest", "listen", "talk", "more"]

// A main public route is a composed page over the source families it names.
// The families are an authored selection, like a World's sources; nothing
// matches a title, a domain in the URL or a source's own claims to choose.
#PageProgram: {
    // "/" is a territory front page and answers only on that territory's own
    // apex (aurekai.<territory> or www.aurekai.<territory>); an institution
    // or subdomain host keeps its own home.
    route: string & =~"^/([a-z][a-z-]*)?$"
    territory: *"any" | "live" | "life" | "shop" | "work"
    title: string & != ""
    lede: string & != ""
    character: "zine" | "scrapbook" | "dossier" | "ticker" | "almanac" | "journal" | "atlas"
    families: [string, ...string]
    source_limit: int & >=1 & <=96
    // Which rhythms this page uses, in order.  Absent roles do not render.
    regions: [#PageRegion.role, ...#PageRegion.role]
    // A front page's sections: other page programs previewed in order.
    sections: *[] | [...string & =~"^/[a-z][a-z-]*$"]
    cannot_infer: string & != ""
}

// ---------------------------------------------------------------- scenes
//
// A scene is not an authored page.  The estate holds a subject's material --
// events, places, pictures, recordings, video, publications, conversations --
// and the *shape* of that material decides which reading rhythms appear and in
// what order.  Three placed things make a map worth drawing; two future events
// make an agenda; six pictures make a gallery; three recordings that play make
// a tracklist.  Below those thresholds the grammar does not appear at all,
// which is why a thin subject stays honest instead of growing empty headings.
//
// Each grammar names the region it lowers through, so a scene still renders
// through the same executor and cannot invent markup of its own.  "weight" is
// how strongly the material's own shape argues for that rhythm: the compiler
// orders a scene by weight, so the same compiler produces a map-led place, an
// agenda-led venue and a tracklist-led artist without any of the three being
// written down.
#SceneGrammar: {
    name:      string & != ""
    region:    string & != ""      // the declared region it lowers through
    material:  "publication" | "video" | "photo" | "place" | "audio" | "post" | "event" |
               "passage" | "organization" | "channel"
    // How much material of that kind before the rhythm is worth showing, and
    // how much it can carry.
    at_least:  int & >=1
    take:      int & >=1 & <=50
    weight:    int & >=1 & <=100
    // What a person reads above it.  Empty means the rhythm needs no heading.
    label:     string
    says:      string & != ""      // why this rhythm, in the estate's own words
}

nativeSceneGrammars: [...#SceneGrammar] & [
    {name: "stage", region: "stage", material: "event", at_least: 1, take: 1, weight: 95, label: "",
     says: "one thing is happening now, so it leads"},
    {name: "agenda", region: "happening", material: "event", at_least: 2, take: 24, weight: 90, label: "What's on",
     says: "several things happen at stated times, which is a day to read forwards"},
    {name: "map", region: "places", material: "place", at_least: 3, take: 40, weight: 80, label: "Where",
     says: "several things carry a point, which is a place to see at once"},
    {name: "field", region: "photos", material: "photo", at_least: 6, take: 24, weight: 72, label: "Pictures",
     says: "many pictures of one subject read as a field seen at once"},
    {name: "wall", region: "wall", material: "photo", at_least: 1, take: 12, weight: 66, label: "Pictures",
     says: "even one picture of a subject belongs beside its words"},
    {name: "tracklist", region: "playlist", material: "audio", at_least: 2, take: 20, weight: 75, label: "Listen",
     says: "recordings that play belong in a list you can play from"},
    {name: "shelf", region: "watch", material: "video", at_least: 2, take: 12, weight: 65, label: "Watch",
     says: "video with pictures reads as a shelf"},
    {name: "threads", region: "topics", material: "post", at_least: 3, take: 30, weight: 55, label: "Talk",
     says: "public conversation is threads, not headlines"},
    {name: "broadsheet", region: "lead", material: "publication", at_least: 1, take: 1, weight: 50, label: "",
     says: "the newest thing published about it leads when nothing is happening"},
    {name: "index", region: "latest", material: "publication", at_least: 2, take: 12, weight: 45, label: "Latest",
     says: "what has been published about it, newest first"},
    {name: "more", region: "more", material: "publication", at_least: 4, take: 24, weight: 20, label: "More",
     says: "everything else it has, compactly"},
    {name: "contents", region: "parts", material: "passage", at_least: 2, take: 12, weight: 62, label: "In the piece",
     says: "a piece its publisher divided into parts reads as those parts"},
    {name: "cast", region: "named", material: "organization", at_least: 2, take: 24, weight: 40, label: "Named in it",
     says: "a piece that names several organizations is read beside who they are"},
    {name: "masthead", region: "channels", material: "channel", at_least: 1, take: 12, weight: 30, label: "Where they publish",
     says: "who published it, and every way they put things out, is part of reading it"},
]

// A front page is allowed to be a different page at eight in the morning and
// at midnight, because the day is.  The hour a phase starts is read in this
// estate's own zone; the sections are the routes below, in the order that hour
// makes sense of.  This is derived composition, not personalization: every
// visitor at the same hour sees the same arrangement, and a phase can only
// reorder and rename what the territory already publishes.
#HomePhase: {
    territory: "life" | "live" | "shop" | "work"
    from_hour: int & >=0 & <=23
    title: string & != ""
    lede: string & != ""
    sections: [...string & =~"^/[a-z][a-z-]*$"]
}

nativeHomePhases: [...#HomePhase] & [
    {
        territory: "life", from_hour: 5, title: "This morning"
        lede: "Dallas this morning, something to cook, and what to read."
        sections: ["/local", "/food", "/health", "/whats-on", "/ideas", "/home", "/travel", "/books"]
    },
    {
        territory: "life", from_hour: 11, title: "Today"
        lede: "Pictures, video, stories and the conversations around them, from the people and institutions publishing today."
        sections: ["/local", "/food", "/whats-on", "/ideas", "/home", "/travel", "/books", "/health"]
    },
    {
        territory: "life", from_hour: 17, title: "Tonight"
        lede: "What is on tonight, what to cook, and what Dallas is talking about."
        sections: ["/whats-on", "/food", "/local", "/ideas", "/books", "/home", "/travel", "/health"]
    },
    {
        territory: "life", from_hour: 23, title: "Still open"
        lede: "What is still happening at this hour, what plays without anyone else awake, and what changed while the city went quiet."
        sections: ["/ideas", "/books", "/food", "/local", "/travel", "/home", "/health", "/whats-on"]
    },
]

nativePagePrograms: [...#PageProgram] & [
    // Front pages index the routes below as sections, each in that route's own
    // rhythm, so a front page is a way in rather than one more feed.
    {
        route: "/", territory: "life", title: "Today", character: "scrapbook"
        lede: "Dallas, food, home and the ideas worth your evening."
        families: ["local-news"], source_limit: 1, regions: ["lead"]
        sections: ["/local", "/food", "/whats-on", "/ideas", "/home", "/health", "/travel", "/books"]
        cannot_infer: "a front page is an arrangement by time, not a ranking of importance or an endorsement"
    },
    {
        route: "/", territory: "live", title: "Live", character: "journal"
        lede: "What's on, and what everyone is watching, hearing and playing."
        families: ["entertainment"], source_limit: 1, regions: ["lead"]
        sections: ["/whats-on", "/film", "/music", "/sports", "/art", "/watch", "/listen", "/stage"]
        cannot_infer: "a report shown here is what its source published, not a warning, advice or a verified event"
    },
    {
        route: "/", territory: "shop", title: "Shop", character: "ticker"
        lede: "Deals worth your time, things worth buying, and what has been recalled."
        families: ["shopping"], source_limit: 1, regions: ["lead"]
        sections: ["/deals", "/picks", "/style", "/recalls", "/retail"]
        cannot_infer: "a release listed here is not an offer, a price, a licence or a recommendation to buy"
    },
    {
        route: "/", territory: "work", title: "Work", character: "dossier"
        lede: "Jobs, running a business, money, and the day's news that touches them."
        families: ["today"], source_limit: 1, regions: ["lead"]
        sections: ["/today", "/jobs", "/business", "/money"]
        cannot_infer: "an advisory or rule listed here does not say whether it applies to you; nothing here is legal, security or financial advice"
    },
    {route: "/deals", title: "Deals", character: "ticker", lede: "Prices worth acting on, as the deal hunters post them."
        families: ["deals"], source_limit: 8, regions: ["table", "more"], cannot_infer: "a deal is the store's price when posted"},
    // The shop's market and new listings read as a trade index: numbered
    // lots, the price in each listing's own words, one way to open each.
    {route: "/market", territory: "shop", title: "Market", character: "ticker", lede: "Everything listed for sale across the shop, with the price each listing states."
        families: ["deals", "drops"], source_limit: 12, regions: ["table", "more"], cannot_infer: "a listed price is the seller's when posted, not a quote, a stock check or a recommendation"},
    {route: "/listings", territory: "shop", title: "New listings", character: "ticker", lede: "Releases, launches and drops as the stores list them."
        families: ["drops"], source_limit: 12, regions: ["table", "more"], cannot_infer: "a listing is the store's; it is not stock, a reservation or a price promise"},
    {route: "/listings", territory: "work", title: "Listings", character: "dossier", lede: "Open roles and contracts, as the people hiring list them."
        families: ["jobs"], source_limit: 12, regions: ["table", "more"], cannot_infer: "a listing is the employer's; it is not an offer of work"},
    {route: "/picks", title: "Worth buying", character: "scrapbook", lede: "What reviewers and editors think is worth the money."
        families: ["shopping", "drops"], source_limit: 12, regions: ["lead", "wall", "latest", "more"], cannot_infer: "a pick is the publisher's opinion; a drop is the store's listing"},
    {route: "/style", title: "Style", character: "scrapbook", lede: "What people are wearing and buying."
        families: ["style"], source_limit: 8, regions: ["lead", "wall", "latest", "more"], cannot_infer: "style coverage is the publisher's"},
    {route: "/recalls", title: "Recalls", character: "dossier", lede: "Products and food pulled from shelves."
        families: ["recalls"], source_limit: 8, regions: ["table", "more"], cannot_infer: "a recall is as the agency announced it"},
    {route: "/retail", title: "Retail", character: "journal", lede: "How stores and brands are changing."
        families: ["retail"], source_limit: 8, regions: ["table", "more"], cannot_infer: "retail news is the publisher's"},
    {route: "/film", title: "Film & TV", character: "journal", lede: "What is opening, streaming and worth watching."
        families: ["film", "entertainment"], source_limit: 12, regions: ["lead", "wall", "latest", "more"], cannot_infer: "coverage is the publisher's"},
    {route: "/music", title: "Music", character: "journal", lede: "New records, tours and the people making them."
        families: ["music"], source_limit: 12, regions: ["lead", "wall", "latest", "more"], cannot_infer: "coverage is the publisher's"},
    {route: "/sports", title: "Sports", character: "ticker", lede: "Scores, trades and the stories behind them."
        families: ["sports"], source_limit: 12, regions: ["lead", "latest", "more"], cannot_infer: "coverage is the publisher's"},
    {route: "/art", title: "Art", character: "scrapbook", lede: "Exhibitions, artists and work worth seeing."
        families: ["art"], source_limit: 8, regions: ["wall", "latest", "more"], cannot_infer: "coverage is the publisher's"},
    {route: "/local", title: "Dallas", character: "journal", lede: "What is happening in Dallas and across Texas, and where to go."
        families: ["local-news", "going-out"], source_limit: 12, regions: ["lead", "latest", "wall", "more"], cannot_infer: "local coverage is the publisher's"},
    {route: "/food", title: "Food", character: "scrapbook", lede: "Something to cook tonight, and where to eat."
        families: ["food"], source_limit: 12, regions: ["lead", "wall", "latest", "more"], cannot_infer: "a recipe is its creator's"},
    {route: "/ideas", title: "Ideas", character: "journal", lede: "The essays and arguments people are reading."
        families: ["culture"], source_limit: 8, regions: ["lead", "latest", "more"], cannot_infer: "an essay is its author's"},
    {route: "/home", title: "Home", character: "scrapbook", lede: "Rooms, gardens and making a place your own."
        families: ["home"], source_limit: 8, regions: ["wall", "latest", "more"], cannot_infer: "coverage is the publisher's"},
    {route: "/health", title: "Health", character: "journal", lede: "Living well, explained."
        families: ["health"], source_limit: 8, regions: ["latest", "more"], cannot_infer: "health coverage is not medical advice"},
    {route: "/travel", title: "Travel", character: "scrapbook", lede: "Places worth the trip."
        families: ["travel"], source_limit: 8, regions: ["lead", "wall", "latest", "more"], cannot_infer: "coverage is the publisher's"},
    {route: "/books", title: "Books", character: "journal", lede: "What to read next."
        families: ["books"], source_limit: 8, regions: ["latest", "wall", "more"], cannot_infer: "a review is its critic's"},
    {route: "/today", title: "The day's news", character: "ticker", lede: "What happened today, briefly."
        families: ["today"], source_limit: 8, regions: ["lead", "latest", "more"], cannot_infer: "news is the publisher's"},
    {route: "/jobs", title: "Jobs", character: "dossier", lede: "Open roles you can apply for now."
        families: ["jobs"], source_limit: 8, regions: ["table", "more"], cannot_infer: "a listing is the employer's"},
    {route: "/business", title: "Small business", character: "journal", lede: "Starting, running and growing one."
        families: ["small-business", "retail"], source_limit: 12, regions: ["lead", "latest", "more"], cannot_infer: "coverage is the publisher's"},
    {route: "/money", title: "Money", character: "journal", lede: "Saving, spending and what it costs."
        families: ["money"], source_limit: 8, regions: ["latest", "more"], cannot_infer: "money coverage is not financial advice"},
    {
        route: "/discover", title: "Discover", character: "scrapbook"
        lede: "Stories, the day's most-read pages, books and the sky, in one stream as they arrive."
        // Discover is the estate-wide reading route.  Each family here has
        // retained public material and therefore has an honest way into the
        // public walk; a source with no retained item remains absent rather
        // than being made to look like it published one.
        families: ["news", "attention", "wikimedia", "books", "space", "history", "institutions", "datasets", "live-music", "commerce", "gaming", "vulnerabilities", "food", "science", "sports"]
        // A Discover length visibly carries at least eight sources across
        // its reading rhythms.  Selecting ninety-six and then advancing the
        // offset skipped the unseen majority, so its window is the page's
        // actual public carrying capacity; the bounded route walk supplies
        // the remaining admitted publishers as later lengths.
        source_limit: 8, regions: ["lead", "wall", "watch", "listen", "topics", "latest", "table", "more", "feed"]
        cannot_infer: "a story shown here is not endorsed, verified or ranked by importance; order is by time"
    },
    {
        route: "/watch", title: "Watch", character: "journal"
        lede: "Video published on people's own servers, and films old enough to belong to everyone, playing right here."
        families: ["video", "films"]
        source_limit: 64, regions: ["grid", "watch", "wall", "latest", "more"]
        cannot_infer: "a video listed here is not hosted, licensed or endorsed by this estate; it plays from its own server"
    },
    {
        route: "/people", title: "People", character: "dossier"
        lede: "Everyone this estate has read: the names publishers printed beside the things they published, and what else each one made."
        families: ["news", "attention", "podcasts", "commons"]
        source_limit: 8, regions: ["latest"]
        cannot_infer: "a name here is what a publisher printed, never an account on this estate, a verified identity, or proof that two identical names are one person"
    },
    {
        route: "/organisations", title: "Institutions", character: "dossier"
        lede: "The publishers whose channels this estate actually reads, and how much of each one it has taken in."
        families: ["news", "institutions", "commons"]
        source_limit: 8, regions: ["latest"]
        cannot_infer: "being listed is this estate's own decision to read a publisher; it is never a relationship either party agreed to, nor an endorsement"
    },
    {
        route: "/channels", title: "Channels", character: "journal"
        lede: "Public threads, grouped by where they are happening. Pick a place to read its topics."
        families: ["forums", "hacker-attention"]
        source_limit: 64, regions: ["topics", "latest", "more", "feed"]
        cannot_infer: "a public thread is not local speech, membership or moderation by this estate"
    },
    {
        route: "/community", title: "Community", character: "scrapbook"
        lede: "People and communities posting in the open, gathered by who is posting."
        families: ["fediverse", "atproto"]
        source_limit: 64, regions: ["communities", "talk", "latest", "more", "feed"]
        cannot_infer: "a post from another community is not a post in this one"
    },
    {
        route: "/board", title: "Boards", character: "scrapbook"
        lede: "Pictures from museums, agencies and the photographers of Wikimedia Commons, pinned wall to wall. Open one to see it large and to use it."
        families: ["commons", "museums", "images"]
        source_limit: 96, regions: ["wall", "photos", "grid", "latest", "more"]
        cannot_infer: "a picture shown here is not a copy, a licence or a claim about who is depicted"
    },
    {
        route: "/parcels", title: "Map", character: "atlas"
        lede: "Where things are being reported: earthquakes, species sightings and bike networks, placed where their sources say."
        families: ["hazards", "nature", "mobility"]
        source_limit: 16, regions: ["places", "photos", "latest", "more"]
        cannot_infer: "a point is where a source placed a report, not a verified location, a warning or advice"
    },
    {
        route: "/releases", title: "Releases", character: "ticker"
        lede: "What registries, studios and model hubs just published, one line each."
        families: ["packages", "ai-models", "gaming", "github"]
        source_limit: 20, regions: ["table", "latest", "more", "feed"]
        cannot_infer: "a release listed here is not reviewed, safe to install or endorsed"
    },
    {
        route: "/advisories", title: "Advisories", character: "dossier"
        lede: "Vulnerabilities, security writing, standards and rules as they were filed."
        families: ["vulnerabilities", "security", "standards", "rulemaking", "recalls"]
        source_limit: 16, regions: ["lead", "table", "latest", "more"]
        cannot_infer: "an advisory does not say whether you are affected; nothing here is security or legal advice"
    },
    {
        route: "/research", title: "Research", character: "journal"
        lede: "Papers, preprints and datasets registered in the last hours, with where each can be cited from."
        families: ["literature", "government-spending", "local-government"]
        source_limit: 18, regions: ["latest", "table", "more", "feed"]
        cannot_infer: "a preprint listed here is not peer reviewed unless its index says so"
    },
    {
        route: "/listen", title: "Listen", character: "journal"
        lede: "Music anyone may hear and recordings the area's own publishers put out \u2014 symphonies, 78 rpm blues, concerts taped in Dallas, talk and reporting \u2014 playing here one after another, with where each is kept."
        families: ["music", "live-music", "podcasts"]
        source_limit: 96, regions: ["playlist", "listen", "wall", "latest", "more"]
        cannot_infer: "a recording listed here is not hosted, licensed or endorsed by this estate; it plays from Wikimedia Commons under the terms its own page states"
    },
    {
        route: "/podcasts", title: "Podcasts", character: "journal"
        lede: "Shows that publish their chapters, transcripts and guests, playing right here."
        families: ["podcasts"]
        source_limit: 16, regions: ["playlist", "listen", "latest", "more"]
        cannot_infer: "an episode is its show's own statement; a guest named or a place mentioned is not verified here"
    },
    {
        route: "/on-this-day", title: "On this day", character: "almanac"
        lede: "The article Wikipedia's editors chose for today, the picture of the day, what happened on this date in other years, and what people are reading."
        families: ["history", "commons", "attention"]
        source_limit: 12, regions: ["lead", "photos", "wall", "latest", "more"]
        cannot_infer: "an event listed for this date is what an encyclopedia records, not a verified account; the picture of the day is Wikimedia's choice, not this estate's"
    },
    {
        route: "/whats-on", title: "What's on", character: "almanac"
        lede: "Events the organizers themselves publish: what starts soon, what is on today, and what is coming, with where each one is."
        families: ["events"], source_limit: 96, regions: ["soon", "happening", "photos", "latest", "more"]
        // No preview sections: the Stitch What's on screen is the agenda itself.
        cannot_infer: "an event is listed as its organizer published it: a time, a place or a ticket may have changed since, and nothing here is a reservation"
    },
    {
        route: "/stage", title: "Stage", character: "journal"
        lede: "What is happening around this event now: where it is and what its organizers have published."
        families: ["events"], source_limit: 96, regions: ["stage", "happening", "soon"]
        cannot_infer: "a stage is a live composition around organizer-published events, not proof of attendance, capacity, availability, a reservation or an invitation"
    },
    {
        route: "/nearby", title: "Around Fort Worth and Dallas", character: "atlas"
        lede: "Landmarks, neighbourhoods and the month's wildlife sightings on the map, the best photographs of the two cities, and what their cities, schools and institutions are saying."
        families: ["places", "events", "local-voices", "local-photos", "local-nature"]
        source_limit: 96, regions: ["places", "soon", "photos", "wall", "latest", "more", "feed"]
        cannot_infer: "a place on this map is where Wikidata or a photographer placed it, not a survey, a boundary, a property record or a recommendation to visit"
    },
    {
        route: "/search", title: "Search", character: "journal"
        lede: "Everything Aurekai is reading right now, searched by what it says and who made it."
        families: ["news", "attention", "wikimedia", "books", "space", "video", "forums", "hacker-attention", "fediverse", "atproto", "museums", "images", "hazards", "nature", "packages", "ai-models", "gaming", "vulnerabilities", "security", "literature", "history", "films", "music", "commons", "places", "local-photos", "live-music", "podcasts", "local-nature", "local-voices", "institutions", "events", "food", "science", "sports", "recalls", "rulemaking"]
        source_limit: 64, regions: ["results"]
        cannot_infer: "a search result is a match on retained public text, not a ranking by truth or importance"
    },
]

// The same few words lead everywhere a person can go from a composed page.
// A nav item may address any page this estate serves, including a subject room.
// The pattern allowed one segment only, which is why no room could ever be
// linked from the bar however many of them existed: the constraint, not the
// intent, kept the front door shut. Query strings stay out, because a nav item
// is an address rather than a search.
#PageNavItem: { route: string & =~"^/[a-z-]+(/[a-z0-9.-]+){0,2}$", label: string & != "" }
// The bar lists the sections that hold material. Channels, Community, Map,
// Pages and Who publishes still answer at their addresses; they return to the
// bar when there is something in them to open.
nativePageNav: [...#PageNavItem] & [
    {route: "/discover", label: "Discover"},
    {route: "/whats-on", label: "What's on"},
    {route: "/watch", label: "Watch"},
    {route: "/listen", label: "Listen"},
    // "Boards" was the donor's word; a reader calls a wall of kept pictures
    // a collection.
    {route: "/board", label: "Collections"},
    // "Needs doing" and "What things cost" left the bar on 2026-11-04. They
    // are estate rooms, not sections: the first answered "No project is
    // published here" on every public host, the second opens on counts of
    // prices. Both still answer at their addresses and are linked from Discover.
]

// One export feeds the native generator, so the three tables cannot drift
// between separate compile steps.
nativePageComposition: {
    regions: nativePageRegions
    subject_schemes: nativePageSubjectSchemes
    source_kinds: nativePageSourceKinds
    route_source_contracts: nativePageRouteSourceContracts
    world_regions: nativePageWorldRegions
    publisher_hosts: nativePagePublisherHosts
    picture_origins: nativePagePictureOrigins
    licences: nativePageLicences
    embed_hosts: nativePageEmbedHosts
    withheld_pictures: nativePageWithheldPictures
    programs: nativePagePrograms
    home_phases: nativeHomePhases
    scene_grammars: nativeSceneGrammars
    nav: nativePageNav
    grammar_labels: nativePageGrammarLabels
}
