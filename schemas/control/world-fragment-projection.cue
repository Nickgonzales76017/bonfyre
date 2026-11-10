package control

// World-specific presentation choices extend grammarPresentation; renderer
// families come from that canonical grammar except for historical aliases.
worldFragmentLaw: {
    source_projection_prefix: "/_surface/world-material"
    media_posture: "native"
    temporal_posture: "retained_observation"
    visible_sections: "material,provenance,why_here"
    provenance: "compiled from World.grammars; source bodies remain owned by ForeignTwin receipts"
    cannot_infer: "a rendered World fragment is not source truth, actor authority, provider admission, payment authority, or an effect grant"
    profiles: {
        discover: {variant: "faceted_stream", extent: "page"}
        stage: {variant: "calendar_timeline", extent: "page"}
        channels: {variant: "stream_topics", extent: "section"}
        community: {variant: "activity_body", extent: "section"}
        board: {variant: "masonry_collection", extent: "page"}
        spatial: {variant: "map_layers", extent: "page"}
        listings: {variant: "availability_compare", extent: "page"}
        search: {canonical: "discover", family: "search", variant: "query_results", extent: "page"}
        inbox: {variant: "obligation_timeline", extent: "workspace"}
        territory: {variant: "address_inspector", extent: "page"}
        observations: {variant: "observation_timeline", extent: "section"}
        worlds: {variant: "world_shelf", extent: "page"}
        packs: {variant: "dependency_kanban", extent: "workspace"}
        link: {variant: "relationship_inspector", extent: "page"}
        studio: {variant: "composition_kanban", extent: "workspace"}
        console: {variant: "observatory", extent: "workspace"}
    }
    historical: {
        vertical: {canonical: "discover", family: "vertical", variant: "faceted_stream", extent: "page"}
        thread: {canonical: "channels", family: "thread", variant: "stream_topics", extent: "section"}
        market: {canonical: "listings", family: "market", variant: "availability_compare", extent: "page"}
        work: {canonical: "inbox", family: "work", variant: "obligation_timeline", extent: "workspace"}
        inspector: {canonical: "territory", family: "inspector", variant: "address_inspector", extent: "page"}
    }
    canonical_aliases: {
        search: "discover"
    }
    forbidden_inference: "a fragment projection -> new World identity, source ownership, actor authority or effect grant"
}
