package control

// A SiteProgram names a human application; the native web lowerer may only
// execute a presentation declared here.  This is deliberately a renderer
// registry, not an owner query: material remains with its typed owner.
#SiteApplicationLowerer: {
    application_kind: #ApplicationKind
    // This is a renderer program, not an application name.  C receives the
    // compiled structural layout and executes it over a normalized stream
    // returned by the canonical material owner.
    layout: "river" | "state" | "thread" | "board" | "audio" | "video"
    // A closed presentation hook accompanies the structural layout.  It is
    // renderer data, never an application-name branch in native CSS.
    presentation_class: "publication" | "state" | "community" | "board" | "listen" | "watch"
    // Structural SurfaceIR. The sequence is presentation only: it can expose
    // no owner query, authority, carrier, or effect. Native lowering executes
    // these generic operations over the material stream supplied by the
    // canonical owner named below.
    ops: [...("header" | "lead" | "stream" | "thread" | "board" | "media" | "transcript")]
    material_owner: "PublicationOccurrence" | "WeatherObservation" | "HumanCommunityBody" | "WorldFragment" | "MusicRecording" | "MediaProgram"
    // Reader realization is physical lowering data, never another semantic
    // owner.  The closed mapping below makes owner/reader drift a CUE error
    // before Zig emits a native descriptor.
    reader: #ReaderByOwner[material_owner]
    // The selector is a coordinate into a binding's typed material_query,
    // except source_collection which is its own typed relation.  C never
    // names these coordinates per application at query time.
    query_key: "source_collection" | "community" | "world" | "recording" | "program" | "none"
    style_asset?: string & =~"^/assets/[a-z0-9._-]+\\.css$"
    empty_character: string & != ""
}

#ReaderByOwner: {
    PublicationOccurrence: "publication"
    WeatherObservation: "none"
    HumanCommunityBody: "community"
    WorldFragment: "world_fragment"
    MusicRecording: "recording"
    MediaProgram: "media_program"
}

nativeSiteApplicationLowerers: [
    {
        application_kind: "PublicationFeed"
        layout: "river"
        presentation_class: "publication"
        ops: ["header", "lead", "stream"]
        material_owner: "PublicationOccurrence"
        reader: "publication"
        query_key: "source_collection"
        style_asset: "/assets/donor-pixelfed.css"
        empty_character: "No admitted publication is available for this part of the World."
    },
    {
        application_kind: "Newsroom"
        layout: "river"
        presentation_class: "publication"
        ops: ["header", "lead", "stream"]
        material_owner: "PublicationOccurrence"
        reader: "publication"
        query_key: "source_collection"
        style_asset: "/assets/donor-pixelfed.css"
        empty_character: "No admitted reporting is available for this part of the World."
    },
    {
        application_kind: "Weather"
        layout: "state"
        presentation_class: "state"
        ops: []
        material_owner: "WeatherObservation"
        reader: "none"
        query_key: "none"
        empty_character: "No admitted weather observation is available for this part of the World."
    },
    {
        application_kind: "Community"
        layout: "thread"
        presentation_class: "community"
        ops: ["header", "thread"]
        material_owner: "HumanCommunityBody"
        reader: "community"
        query_key: "community"
        style_asset: "/assets/donor-lemmy.css"
        empty_character: "No published human community body is available for this part of the World."
    },
    {
        application_kind: "Board"
        layout: "board"
        presentation_class: "board"
        ops: ["header", "board"]
        material_owner: "WorldFragment"
        reader: "world_fragment"
        query_key: "world"
        style_asset: "/assets/donor-pinry.css"
        empty_character: "No published Board arrangement is available for this World."
    },
    {
        application_kind: "Listen"
        layout: "audio"
        presentation_class: "listen"
        ops: ["header", "media", "transcript"]
        material_owner: "MusicRecording"
        reader: "recording"
        query_key: "recording"
        style_asset: "/assets/donor-pixelfed.css"
        empty_character: "No published recording is available for listening in this part of the World."
    },
    {
        application_kind: "Watch"
        layout: "video"
        presentation_class: "watch"
        ops: ["header", "media", "transcript"]
        material_owner: "MediaProgram"
        reader: "media_program"
        query_key: "program"
        style_asset: "/assets/donor-pixelfed.css"
        empty_character: "No published video program is available for this part of the World."
    },
]
