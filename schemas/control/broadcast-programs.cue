package control

// Broadcast programs: produced audio composed from the estate's own sources.
//
// A program is a clock of segments.  A desk segment reads the freshest items
// of one item family aloud (headline, publisher, date), each line citing the
// item it came from.  A music or feature segment plays an excerpt of a
// recording whose source states open terms (public domain, CC0, CC BY,
// CC BY-SA) at the moment it is fetched; anything else is skipped and the
// skip is recorded.  bonfyre-broadcast reads the exported JSON
// (cue export ./schemas/control -e broadcast --out json); it never
// invents an item, a licence or a voice.

// Engines are discovered, not listed.  bonfyre-broadcast engines finds:
//   neural    a speech runtime bound below whose model bodies the model estate
//             (bonfyre model) holds and verifies; it loads only under a
//             memory grant from the admission ledger, and every line it
//             renders is read back by whisper and kept only above min_recall
//   system    the operating system's installed voices for the locales asked,
//             less the exclusions below
//   provider  a /v1/audio/speech endpoint, live only when its key is present
//             (or, for a loopback server, when it answers)
#SpeechRuntime: {
    id: string & =~"^[a-z0-9-]+$"
    runtime: "llama-tts"
    model: string & =~"^[a-z0-9.-]+$"   // bonfyre model id
    vocoder: string & =~"^[a-z0-9.-]+$" // bonfyre model id
    licence: string & != ""
    min_recall: number & >0 & <=1 | *0.6
}

#Provider: {
    id: string & =~"^[a-z0-9-]+$"
    url: string & =~"^https?://"
    model: string & != ""
    voice: string & != ""
    key_env?: string & =~"^[A-Z0-9_]+$"
    licence: string & != ""
}

#Role: {
    prefer: [...("neural" | "system" | "provider")] & [_, ...]
    locale: *"en" | string
}

#Pool: {
    id: string & =~"^[a-z0-9-]+$"
    // An Internet Archive advanced search; the licence filter is in the query
    // and is checked again per item against open terms.
    query: string & != ""
    rows: int & >=1 & <=200 | *50
    says: string & != ""
}

#Segment: {
    kind: "ident" | "desk" | "music" | "feature" | "outro"
    title: string & != ""
    // desk
    family?: string & =~"^[a-z0-9-]+$"
    families?: [...string & =~"^[a-z0-9-]+$"] // read together, scored as one desk
    census?: string // only sources of one declared census (see broadcast.censuses)
    count?: int & >=1 & <=20
    max_age_days?: int & >=1 | *60
    intro?: string
    voices?: [...string] // cast roles, rotated line by line
    // music / feature
    pool?: string
    seconds?: int & >=5 & <=600
    // ident / outro / feature lead-in
    say?: string
    voice?: string // a cast role
    bed?: string // a pool played softly under the spoken words
}

#Program: {
    id: string & =~"^[a-z0-9-]+$"
    title: string & != ""
    station: string & != ""
    says: string & != ""
    // Roles the clock speaks in; plan casts each to a distinct discovered
    // engine where enough exist.  fallback names the role that speaks a line
    // its own engine could not.
    cast: [string]: #Role
    fallback: string
    clock: [...#Segment] & [_, ...]
}

_netlabelOpen: "collection:netlabels AND mediatype:audio AND licenseurl:(\"http://creativecommons.org/licenses/by/3.0/\" OR \"http://creativecommons.org/licenses/by-sa/3.0/\" OR \"http://creativecommons.org/licenses/by/4.0/\" OR \"http://creativecommons.org/licenses/by-sa/4.0/\" OR \"http://creativecommons.org/licenses/by/2.5/\" OR \"http://creativecommons.org/licenses/by-sa/2.5/\")"

broadcast: {
    // Desks may read only the sources a census declares; the list is the census itself.
    censuses: {
        mainstream: [for s in nativeMainstreamSources {s.source}]
        "public-commons": [for s in nativePublicCommonsSources {s.source}]
        "open-content": [for s in nativeOpenContentSources {s.source}]
    }
    runtimes: [...#SpeechRuntime] & [
        {id: "outetts", runtime: "llama-tts", model: "outetts-0.3-500m-q4km", vocoder: "wavtokenizer-large-75-f16",
         licence: "OuteTTS 0.3 500M, CC BY-SA 4.0; WavTokenizer, MIT", min_recall: 0.6},
    ]
    // Whisper models tried, in order, for neural read-back.
    readback: ["~/.local/share/whisper/ggml-base.en.bin", "~/.local/share/whisper/ggml-tiny.en.bin"]
    // Novelty and Eloquence voices are not broadcast voices.
    system_exclude: ["Albert", "Bad News", "Bahh", "Bells", "Boing", "Bubbles", "Cellos", "Wobble", "Jester",
        "Good News", "Organ", "Superstar", "Trinoids", "Whisper", "Zarvox", "Ralph", "Fred", "Junior", "Kathy",
        "Eddy", "Flo", "Grandma", "Grandpa", "Reed", "Rocko", "Sandy", "Shelley"]
    providers: [...#Provider] & [
        {id: "openai-speech", url: "https://api.openai.com/v1/audio/speech", model: "gpt-4o-mini-tts", voice: "alloy",
         key_env: "BONFYRE_OPENAI_TTS_KEY", licence: "OpenAI usage terms; disclose the voice is AI-generated"},
        {id: "kokoro-loopback", url: "http://127.0.0.1:8880/v1/audio/speech", model: "kokoro", voice: "af_heart",
         licence: "Kokoro-82M, Apache 2.0"},
    ]
    pools: [...#Pool] & [
        {id: "netlabel-open", query: _netlabelOpen, rows: 80,
         says: "netlabel releases under CC BY or CC BY-SA"},
        {id: "nasa-archive", query: "collection:nasaaudiocollection AND licenseurl:*publicdomain*", rows: 60,
         says: "NASA mission audio, public domain"},
        {id: "librivox-poetry", query: "collection:librivoxaudio AND subject:poetry AND licenseurl:*publicdomain*", rows: 80,
         says: "LibriVox poetry readings, public domain"},
        {id: "librivox-stories", query: "collection:librivoxaudio AND subject:(short stories) AND licenseurl:*publicdomain*", rows: 80,
         says: "LibriVox short story readings, public domain"},
    ]
    programs: [...#Program] & [
        {
            id: "estate-hour", title: "The Estate Hour", station: "Aurekai Radio"
            says: "what the estate's sources are reporting, read by its desks, with open-licence music between"
            cast: {
                anchor: {prefer: ["neural", "provider", "system"]}
                co: {prefer: ["system"], locale: "en_GB"}
                local: {prefer: ["system"], locale: "en_US"}
                weather: {prefer: ["system"], locale: "en_AU"}
                recalls: {prefer: ["system"], locale: "en_IE"}
                security: {prefer: ["system"], locale: "en_ZA"}
                archive: {prefer: ["system"], locale: "en_IN"}
            }
            fallback: "local"
            clock: [
                {kind: "ident", title: "Open", voice: "anchor", bed: "netlabel-open",
                 say: "This is The Estate Hour on Aurekai Radio. Every story you hear was published by the source we name, and every song is shared under open terms."},
                {kind: "desk", title: "Headlines", census: "mainstream", families: ["news", "today", "money", "health", "local-news"], count: 10, intro: "The headlines.", voices: ["anchor", "co"]},
                {kind: "music", title: "Interlude", pool: "netlabel-open", seconds: 75},
                {kind: "desk", title: "North Texas", families: ["local-news"], count: 6, intro: "Around North Texas.", voices: ["local"]},
                {kind: "desk", title: "Weather and hazards", family: "hazards", count: 4, intro: "Weather and hazard notices.", voices: ["weather"]},
                {kind: "desk", title: "Recalls", family: "recalls", count: 4, intro: "Product recalls.", voices: ["recalls"]},
                {kind: "music", title: "Interlude", pool: "netlabel-open", seconds: 60},
                {kind: "desk", title: "Security desk", family: "vulnerabilities", count: 4, intro: "From the security desk.", voices: ["security"]},
                {kind: "desk", title: "Books", family: "books", count: 3, intro: "On the books page.", voices: ["co"]},
                {kind: "feature", title: "From the mission archive", pool: "nasa-archive", seconds: 40, voice: "archive",
                 say: "From NASA's public archive."},
                {kind: "feature", title: "A poem", pool: "librivox-poetry", seconds: 90, voice: "anchor",
                 say: "A reading from the LibriVox volunteers."},
                {kind: "outro", title: "Close", voice: "anchor", bed: "netlabel-open",
                 say: "That was The Estate Hour. The sources, licences and links for everything you heard are in the show notes."},
            ]
        },
        {
            id: "night-library", title: "Night Library", station: "Aurekai Radio"
            says: "public-domain stories and poems read by LibriVox volunteers, with quiet open-licence music between"
            cast: {
                host: {prefer: ["neural", "provider", "system"]}
                reader: {prefer: ["system"], locale: "en_IE"}
                poet: {prefer: ["system"], locale: "en_GB"}
            }
            fallback: "reader"
            clock: [
                {kind: "ident", title: "Open", voice: "host", bed: "netlabel-open",
                 say: "Night Library, on Aurekai Radio. Stories and poems from the public domain."},
                {kind: "feature", title: "A story", pool: "librivox-stories", seconds: 300, voice: "reader", say: "Tonight's first story."},
                {kind: "music", title: "Between readings", pool: "netlabel-open", seconds: 60},
                {kind: "feature", title: "A poem", pool: "librivox-poetry", seconds: 120, voice: "poet", say: "A poem."},
                {kind: "music", title: "Between readings", pool: "netlabel-open", seconds: 60},
                {kind: "feature", title: "Another story", pool: "librivox-stories", seconds: 300, voice: "reader", say: "One more."},
                {kind: "outro", title: "Close", voice: "host", bed: "netlabel-open",
                 say: "Night Library is read by LibriVox volunteers. Their recordings are in the public domain."},
            ]
        },
    ]
}
