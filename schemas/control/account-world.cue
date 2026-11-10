package control

import "strings"


#AccountWorldKey: string & =~"^[A-Za-z0-9._:/-]+$" & strings.MaxRunes(256)
#AccountWorldBinding: {
    world_id: int & >0
    subject: #AccountWorldKey
    namespace: #AccountWorldKey & strings.MaxRunes(127)
}
#AccountWorldSpace: {
    world_id: int & >0
    space_id: #AccountWorldKey & strings.MaxRunes(64)
    title: string & != "" & !~"^[ \t\r\n]+$" & !~#"[\x00-\x1f\x7f]"#
    if len(title) > 256 { _|_ }
    renderer: "2d" | "3d"
    template_ref: #AccountWorldKey
    layout_ref: #AccountWorldKey
    revision: int & >0
}

#AccountWorldSlug: string & =~"^[a-z0-9][a-z0-9-]*$" & strings.MaxRunes(32)
#AccountWorldSpaceStyle: {
    world_id: int & >0
    space_id: #AccountWorldKey & strings.MaxRunes(64)
    medium: #AccountWorldSlug
    character: "zine" | "scrapbook" | "dossier" | "ticker" | "almanac" | "journal"
    lede: string & strings.MaxRunes(600)
}
#AccountWorldItem: {
    world_id: int & >0
    space_id: #AccountWorldKey & strings.MaxRunes(64)
    position: int & >=1 & <=24
    ref: =~"^([a-z0-9._-]{1,95}/[0-9a-f]{24}|space:[A-Za-z0-9._:/-]{1,64})$"
    role: #AccountWorldSlug
    note: string & strings.MaxRunes(280)
}
#AccountWorldRelease: {
    world_id: int & >0
    space_id: #AccountWorldKey & strings.MaxRunes(64)
    state: "public" | "withdrawn"
    revision: int & >0
}

// account-world-cine
#AccountWorldFilmRef: =~"^[a-z0-9._-]{1,95}/[0-9a-f]{24}$"
#AccountWorldScreening: {
    world_id: int & >0
    space_id: #AccountWorldKey & strings.MaxRunes(64)
    screening_id: string & =~"^[a-z0-9][a-z0-9-]*$" & strings.MaxRunes(64)
    starts: =~"^[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])T([01][0-9]|2[0-3]):[0-5][0-9]$"
    venue: string & != "" & strings.MaxRunes(120)
    ref: #AccountWorldFilmRef
    note: string & strings.MaxRunes(280)
    state: "draft" | "announced" | "cancelled"
}
#AccountWorldClub: {
    world_id: int & >0
    space_id: #AccountWorldKey & strings.MaxRunes(64)
    state: "open" | "closed"
}
#AccountWorldClubMember: {
    member: #AccountWorldKey
    state: "member" | "left" | "removed"
}
#AccountWorldClubPick: {
    pick_id: int & >0
    ref: #AccountWorldFilmRef
    note: string & strings.MaxRunes(280)
    proposed_by: #AccountWorldKey
    state: "open" | "hidden"
}
#AccountWorldClubPost: {
    post_id: int & >0
    author: #AccountWorldKey
    ref: #AccountWorldFilmRef | ""
    body: string & strings.MaxRunes(1000)
    state: "shown" | "hidden" | "retracted"
    if state == "retracted" { body: "" }
}
#AccountWorldNote: {
    note_id: int & >0
    world_id: int & >0
    ref: #AccountWorldFilmRef
    watched: =~"^[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])$"
    rating: int & >=0 & <=10
    rewatch: 0 | 1
    body: string & strings.MaxRunes(2000)
    visibility: "private" | "public"
}

#AccountWorldPiece: =~"^[a-z0-9._-]{1,95}/[0-9a-f]{24}$"
#AccountWorldSound: {
    world_id: int & >0
    space_id: #AccountWorldKey & strings.MaxRunes(64)
    released: 0 | 1
    kind: "station" | "show"
    timezone: "UTC" | =~"^[A-Za-z0-9_+-]+(/[A-Za-z0-9_+-]+)+$" & strings.MaxRunes(64)
    language: =~"^[A-Za-z-]{2,16}$"
    author: string & != "" & strings.MaxRunes(160)
    category: string & != "" & strings.MaxRunes(64)
    explicit: 0 | 1
    stream_ref: "" | #AccountWorldPiece
}
#AccountWorldSlot: {
    world_id: int & >0
    space_id: #AccountWorldKey & strings.MaxRunes(64)
    released: 0 | 1
    slot: int & >=1 & <=48
    weekday: int & >=1 & <=7
    start_minute: int & >=0 & <=1439
    minutes: int & >=1 & <=1440
    programme: string & != "" & strings.MaxRunes(160)
    host: string & strings.MaxRunes(160)
    ref: "" | #AccountWorldPiece | =~"^space:[A-Za-z0-9._:/-]{1,64}$"
}
#AccountWorldEpisode: {
    world_id: int & >0
    space_id: #AccountWorldKey & strings.MaxRunes(64)
    released: 0 | 1
    ref: #AccountWorldPiece
    season: int & >=0 & <=999
    number: int & >=0 & <=9999
    published: =~"^[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])$"
}
#AccountWorldMark: {
    world_id: int & >0
    space_id: #AccountWorldKey & strings.MaxRunes(64)
    released: 0 | 1
    ref: #AccountWorldPiece
    kind: "track" | "chapter" | "cue" | "clip" | "person" | "source"
    start_ms: int & >=0 & <=86400000
    end_ms: int & >=0 & <=86400000
    if kind == "cue" || kind == "clip" { end_ms: >start_ms }
    title: string & != "" & strings.MaxRunes(600)
    credit: string & strings.MaxRunes(160)
    href: "" | #AccountWorldPiece | =~"^https://[^\\s\"'<>`]+$" & strings.MaxRunes(512)
    checked: 0 | 1
}
