package control

// Native SurfaceProgram source. This file owns wording, slot order and which
// generic operation is requested. It deliberately contains neither HTML nor
// owner SQL: SurfacePatterns/owners answer data and the native renderer only
// executes these typed instructions against an already-resolved World.
#SurfaceOp: {
    op: "territory_nav" | "world_nav" | "world_link" | "world_fragments" |
        "community_posts" | "community_title" | "community_lede" |
        "community_standing" | "community_limit" |
        "offer_status" | "offer_empty" | "night_subjects" |
        "publications" | "pack_demands" | "capability_programs" | "world_catalogue" |
        "subject_apertures" |
        "text" | "date"
    text?: string
    // A label is never a routing instruction.  This makes the destination an
    // explicit authored part of a link operation instead of a C comparison on
    // its visible wording.
    cut?: string
}
#SurfaceSlot: { slot: int & >=0, ops: [...#SurfaceOp] }
#NativeSurfaceProgram: {
    program: "discover" | "stage" | "channels" | "community" | "board" | "spatial" |
        "listings" | "inbox" | "territory" | "observations" | "worlds" | "packs" |
        "link" | "studio" | "console"
    renderer: "pixelfed_stream" | "mobilizon_stage" | "zulip_channels" | "lemmy_community" |
        "pinry_board" | "maplibre_spatial" | "commerce_market" | "frappe_inbox" |
        "territory_inspector" | "observation_stream" | "world_shelf" | "pack_graph" |
        "link_inspector" | "studio_workspace" | "operator_console"
    route: string & =~"^/[a-z][a-z-]*$"
    // The donor template that supplies only its observed CSS/art direction.
    // It never supplies a browser-owned renderer for a native document.
    template: string & =~"^[a-z][a-z-]*$"
    document: "slots" | "native"
    world_route: bool
    world_navigation: bool
    slots: [#SurfaceSlot, ...#SurfaceSlot]
}

// Donor interaction posture is portable presentation data.  A C lowerer may
// execute one of these structural variants, but it must never discover the
// variant by comparing a renderer name at request time.
#RendererPresentation: {
    renderer: "pixelfed_stream" | "mobilizon_stage" | "zulip_channels" | "lemmy_community" |
        "pinry_board" | "maplibre_spatial" | "commerce_market" | "frappe_inbox" |
        "territory_inspector" | "observation_stream" | "world_shelf" | "pack_graph" |
        "link_inspector" | "studio_workspace" | "operator_console"
    territory_nav: "plain" | "stream"
    world_nav: "plain" | "stream" | "community" | "board" | "market"
    world_link: "stream" | "stage" | "community" | "board" | "market"
    subjects: "stream" | "stage"
}

nativeSurfaceRendererPresentations: [
    {renderer:"pixelfed_stream", territory_nav:"stream", world_nav:"stream", world_link:"stream", subjects:"stream"},
    {renderer:"mobilizon_stage", territory_nav:"plain", world_nav:"plain", world_link:"stage", subjects:"stage"},
    {renderer:"lemmy_community", territory_nav:"plain", world_nav:"community", world_link:"community", subjects:"stream"},
    {renderer:"pinry_board", territory_nav:"plain", world_nav:"board", world_link:"board", subjects:"stream"},
    {renderer:"commerce_market", territory_nav:"plain", world_nav:"market", world_link:"market", subjects:"stream"},
    {renderer:"zulip_channels", territory_nav:"plain", world_nav:"plain", world_link:"stream", subjects:"stream"},
    {renderer:"maplibre_spatial", territory_nav:"plain", world_nav:"plain", world_link:"stream", subjects:"stream"},
    {renderer:"frappe_inbox", territory_nav:"plain", world_nav:"plain", world_link:"stream", subjects:"stream"},
    {renderer:"territory_inspector", territory_nav:"plain", world_nav:"plain", world_link:"stream", subjects:"stream"},
    {renderer:"observation_stream", territory_nav:"stream", world_nav:"stream", world_link:"stream", subjects:"stream"},
    {renderer:"world_shelf", territory_nav:"plain", world_nav:"plain", world_link:"stream", subjects:"stream"},
    {renderer:"pack_graph", territory_nav:"plain", world_nav:"plain", world_link:"stream", subjects:"stream"},
    {renderer:"link_inspector", territory_nav:"plain", world_nav:"plain", world_link:"stream", subjects:"stream"},
    {renderer:"studio_workspace", territory_nav:"plain", world_nav:"plain", world_link:"stream", subjects:"stream"},
    {renderer:"operator_console", territory_nav:"plain", world_nav:"plain", world_link:"stream", subjects:"stream"},
]

// Territory chooses an already-owned subject field for presentation.  This is
// a field-selection policy, not a change in subject truth or an instruction
// to infer an offer, operation, or live occurrence.
#TerritoryPrimary: {
    territory: "life" | "live" | "shop" | "work" | "independent"
    field: "summary" | "tonight" | "offer" | "operations"
}
nativeSurfaceTerritoryPrimary: [
    {territory:"life", field:"summary"},
    {territory:"live", field:"tonight"},
    {territory:"shop", field:"offer"},
    {territory:"work", field:"operations"},
    {territory:"independent", field:"summary"},
]

// These are admissible renderer/extent vocabularies for CMS World fragments.
// The executor validates membership in these compiled sets; it does not carry
// a second handwritten list that can drift from authored SurfaceIR.
nativeSurfaceRendererFamilies: [
    "vertical", "stage", "thread", "board", "community", "market", "search",
    "spatial", "work", "inspector", "watch", "listen", "library", "analysis",
]
nativeSurfaceExtents: ["chip", "card", "section", "page", "workspace", "immersive"]

nativeSurfacePrograms: [
    {
        program: "discover"
        renderer: "pixelfed_stream"
        route: "/discover"
        template: "discover"
        document: "slots"
        world_route: false
        world_navigation: true
        slots: [
            {slot: 0, ops: [{op:"territory_nav"}]},
            {slot: 1, ops: [{op:"world_link", text:"Open the page"}, {op:"world_nav"}]},
            {slot: 2, ops: [{op:"night_subjects"}, {op:"publications"}]},
            {slot: 3, ops: [{op:"subject_apertures"}]},
        ]
    },
    {
        program: "stage"
        renderer: "mobilizon_stage"
        route: "/stage"
        template: "stage"
        document: "slots"
        world_route: true
        world_navigation: true
        slots: [
            {slot: 0, ops: [{op:"territory_nav"}]},
            {slot: 1, ops: [{op:"world_nav"}]},
            {slot: 2, ops: [{op:"night_subjects"}]},
            {slot: 3, ops: [{op:"text", text:""}]},
            {slot: 4, ops: [{op:"date", text:"weekday"}]},
            {slot: 5, ops: [{op:"date", text:"day"}]},
            {slot: 6, ops: [{op:"date", text:"month"}]},
            {slot: 7, ops: [{op:"date", text:"year"}]},
            {slot: 8, ops: [{op:"text", text:"Tonight in this city"}]},
            {slot: 9, ops: [{op:"text", text:"What is happening around here."}]},
            {slot: 10, ops: [{op:"text", text:"Notices, not bookings"}]},
            {slot: 11, ops: [{op:"world_link", text:"Open the page"}, {op:"world_nav"}]},
            {slot: 12, ops: [{op:"text", text:"No event is on the schedule yet. What follows are notices that have been posted; they are not performances, bookings or attendance."}]},
            {slot: 13, ops: [{op:"text", text:"A room status, a capacity note, an offer or a safety note is not an event, a ticket or permission to attend."}]},
            {slot: 14, ops: [{op:"text", text:"Notices"}]},
            {slot: 15, ops: [{op:"night_subjects"}]},
            {slot: 16, ops: [{op:"text", text:"The schedule appears once an event is published."}]},
            {slot: 17, ops: [{op:"subject_apertures"}]},
        ]
    },
    {
        program: "community"
        renderer: "lemmy_community"
        route: "/community"
        template: "community"
        document: "slots"
        world_route: true
        world_navigation: true
        slots: [
            {slot: 0, ops: [{op:"world_link", text:"AUREKAI"}]},
            {slot: 1, ops: [{op:"world_nav"}]},
            // A community's name, its own description of itself and its
            // standing are read from the published community_body, not
            // authored here. Hard-coding them made this page agree with the
            // record by coincidence and disagree with it silently whenever a
            // different community was asked for.
            {slot: 2, ops: [{op:"community_title", text:"Community"}]},
            {slot: 3, ops: [{op:"community_lede", text:"Conversations people have started here. Posts from other networks are not brought in as if they were written here."}]},
            {slot: 4, ops: [{op:"world_link", text:"Open the page"}]},
            {slot: 5, ops: [{op:"community_posts"}]},
            {slot: 6, ops: [{op:"subject_apertures"}]},
            // Who is actually here. A community page that cannot say this is
            // describing a forum in general rather than this one.
            {slot: 7, ops: [{op:"community_standing"}]},
            {slot: 8, ops: [{op:"community_limit", text:"Posts from other networks are shown on their own pages."}]},
        ]
    },
    {
        program: "board"
        renderer: "pinry_board"
        route: "/board"
        template: "board"
        document: "slots"
        world_route: true
        world_navigation: true
        slots: [
            {slot: 0, ops: [{op:"world_link", text:"AUREKAI / WORLD"}]},
            {slot: 1, ops: [{op:"world_nav"}]},
            {slot: 2, ops: [{op:"world_nav"}]},
            {slot: 3, ops: [{op:"world_fragments"}]},
            {slot: 4, ops: [{op:"subject_apertures"}]},
        ]
    },
    {
        program: "listings"
        renderer: "commerce_market"
        route: "/listings"
        template: "listings"
        document: "slots"
        world_route: true
        world_navigation: true
        slots: [
            {slot: 0, ops: [{op:"world_link", text:"AUREKAI"}]},
            {slot: 1, ops: [{op:"world_nav"}]},
            {slot: 2, ops: [{op:"text", text:"What is on offer here. "}, {op:"text", text:"An offer shows once it is published. "}, {op:"text", text:"A note about capacity or a supplier is not a listing, a price or a reservation."}]},
            {slot: 3, ops: [{op:"world_link", text:"This address"}, {op:"world_link", text:"Boards", cut:"board"}]},
            {slot: 4, ops: [{op:"offer_status"}]},
            {slot: 5, ops: [{op:"text", text:"Offers"}]},
            {slot: 6, ops: [{op:"text", text:"Nothing is on offer here yet."}]},
            {slot: 7, ops: [{op:"offer_empty"}, {op:"subject_apertures"}]},
            {slot: 8, ops: [{op:"text", text:"Availability"}, {op:"text", text:"Price"}, {op:"text", text:"Terms"}]},
        ]
    },
    {
        program: "channels"
        renderer: "zulip_channels"
        route: "/channels"
        template: "channels"
        document: "native"
        world_route: true
        world_navigation: true
        slots: [
            {slot: 0, ops: [{op:"territory_nav"}, {op:"world_nav"}]},
            {slot: 1, ops: [{op:"subject_apertures"}]},
            {slot: 2, ops: [{op:"community_posts"}]},
        ]
    },
    {
        program: "spatial"
        renderer: "maplibre_spatial"
        route: "/parcels"
        template: "parcels"
        document: "native"
        world_route: true
        world_navigation: true
        slots: [
            {slot: 0, ops: [{op:"territory_nav"}, {op:"world_nav"}]},
            {slot: 1, ops: [{op:"night_subjects"}]},
            {slot: 2, ops: [{op:"subject_apertures"}]},
        ]
    },
    {
        program: "inbox"
        renderer: "frappe_inbox"
        route: "/inbox"
        template: "inbox"
        document: "native"
        world_route: true
        world_navigation: true
        slots: [
            {slot: 0, ops: [{op:"territory_nav"}, {op:"world_nav"}]},
            {slot: 1, ops: [{op:"capability_programs"}]},
            {slot: 2, ops: [{op:"subject_apertures"}]},
        ]
    },
    {
        program: "territory"
        renderer: "territory_inspector"
        route: "/territory"
        template: "territory"
        document: "native"
        world_route: false
        world_navigation: false
        slots: [
            {slot: 0, ops: [{op:"territory_nav"}, {op:"world_link", text:"Open current World"}]},
            {slot: 1, ops: [{op:"subject_apertures"}]},
        ]
    },
    {
        program: "observations"
        renderer: "observation_stream"
        route: "/sources"
        template: "sources"
        document: "native"
        world_route: true
        world_navigation: true
        slots: [
            {slot: 0, ops: [{op:"territory_nav"}, {op:"world_nav"}]},
            {slot: 1, ops: [{op:"publications"}]},
            {slot: 2, ops: [{op:"subject_apertures"}]},
        ]
    },
    {
        program: "worlds"
        renderer: "world_shelf"
        route: "/w"
        template: "world"
        document: "native"
        world_route: false
        world_navigation: false
        slots: [
            {slot: 0, ops: [{op:"territory_nav"}]},
            {slot: 1, ops: [{op:"world_catalogue"}]},
        ]
    },
    {
        program: "packs"
        renderer: "pack_graph"
        route: "/packs"
        template: "packs"
        document: "native"
        world_route: true
        world_navigation: true
        slots: [
            {slot: 0, ops: [{op:"territory_nav"}, {op:"world_nav"}]},
            {slot: 1, ops: [{op:"pack_demands"}]},
            {slot: 2, ops: [{op:"subject_apertures"}]},
        ]
    },
    {
        program: "link"
        renderer: "link_inspector"
        route: "/link"
        template: "link"
        document: "native"
        world_route: false
        world_navigation: false
        slots: [
            {slot: 0, ops: [{op:"territory_nav"}, {op:"world_link", text:"Current World"}]},
            {slot: 1, ops: [{op:"capability_programs"}]},
            {slot: 2, ops: [{op:"subject_apertures"}]},
        ]
    },
    {
        program: "studio"
        renderer: "studio_workspace"
        route: "/studio"
        template: "studio"
        document: "native"
        world_route: false
        world_navigation: false
        slots: [
            {slot: 0, ops: [{op:"territory_nav"}]},
            {slot: 1, ops: [{op:"world_catalogue"}, {op:"pack_demands"}, {op:"capability_programs"}]},
            {slot: 2, ops: [{op:"subject_apertures"}]},
        ]
    },
    {
        program: "console"
        renderer: "operator_console"
        route: "/console"
        template: "console"
        document: "native"
        world_route: false
        world_navigation: false
        slots: [
            {slot: 0, ops: [{op:"territory_nav"}]},
            {slot: 1, ops: [{op:"world_catalogue"}, {op:"pack_demands"}, {op:"capability_programs"}, {op:"publications"}]},
            {slot: 2, ops: [{op:"subject_apertures"}]},
        ]
    },
]

// The compiler input is intentionally one closed declaration.  Program rows
// cannot name a renderer that lacks a separately-authored presentation
// posture, and native C receives pointer/index data rather than renderer
// names it must branch over.
nativeSurfaceProgramCompilerInput: {
    programs: nativeSurfacePrograms
    renderers: nativeSurfaceRendererPresentations
    territory_primary: nativeSurfaceTerritoryPrimary
    renderer_families: nativeSurfaceRendererFamilies
    extents: nativeSurfaceExtents
}

// Machine carriers are declared here too. A carrier name and its schema are
// projection metadata, not a hand-maintained C route table.
#NativeMachineSurface: {
    name: "mcp" | "a2a" | "api"
    schema: string & =~"^bonfyre-(api|mcp-resources|a2a-resources)-v1$"
}

nativeMachineSurfaces: [
    {name: "api", schema: "bonfyre-api-v1"},
    {name: "mcp", schema: "bonfyre-mcp-resources-v1"},
    {name: "a2a", schema: "bonfyre-a2a-resources-v1"},
]

// Shared document-shell slots are one program family, not three unrelated
// string substitutions in WebAddress. The operation remains native because it
// consumes resolved address state and CMS navigation, while the declaration
// owns which template may request it.
#NativeTemplateSlot: {
    template: "home" | "matrix_index"
    slot: int & >=0
    op: "resolved_identity" | "authored_navigation" | "matrix_context"
}

nativeTemplateSlots: [
    {template: "home", slot: 0, op: "resolved_identity"},
    {template: "home", slot: 1, op: "authored_navigation"},
    {template: "matrix_index", slot: 0, op: "matrix_context"},
]
