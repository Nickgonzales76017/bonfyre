package control

import "list"

// The presentation matrix is authored data, not a browser role switch.  The
// `axis:value` form is deliberately compiler-readable: it requests candidate
// material from the public reservoir but cannot identify an actor, admit a
// provider, disclose a field, or perform an effect.

#GrammarPresentation: {
    question: string & != ""
    visible_sections: [string & != "", ...string]
    preferred_renderers: [string & != "", ...string]
    // Portable interaction physics is authored once per grammar.  Native
    // lowering reads this generated descriptor instead of a route-name ladder.
    renderer_family: "vertical" | "stage" | "thread" | "community" | "board" | "spatial" | "market" | "work" | "inspector"
    renderer_variant: string & != ""
    route: string & =~"^/[a-z][a-z/-]*$"
    interaction_density: "low" | "medium" | "high" | "very_high"
    allowed_affordance_families: [string & != "", ...string]
    external_demand_roles: [string & != "", ...string]
    empty_character: string & != ""
    refusal_character: string & != ""
    extent_defaults: "chip" | "card" | "section" | "page" | "workspace"
    navigation_neighbors: [#SurfaceGrammar, ...#SurfaceGrammar]
    requires: [...("relation" | "pack" | "habitat" | "work_scope" | "institution" | "information_policy")]
}

#ClassPresentation: {
    question_suffix: string & != ""
    experience_character: string & != ""
    visible_sections: [string & != "", ...string]
    allowed_affordance_families: [string & != "", ...string]
    empty_character: string & != ""
    refusal_character: string & != ""
    interaction_density: "low" | "medium" | "high" | "very_high"
    requires: [...("relation" | "pack" | "habitat" | "work_scope" | "institution" | "information_policy")]
}

grammarPresentation: close({
    discover: {
        question: "What lawful possibilities intersect this addressed situation?"
        visible_sections: ["candidate horizon", "why this may be shown", "next context"]
        preferred_renderers: ["faceted-discovery", "comparison-tray"]
        renderer_family: "vertical"
        renderer_variant: "faceted_stream"
        route: "/discover"
        interaction_density: "medium"
        allowed_affordance_families: ["inspect", "compare", "open_context"]
        external_demand_roles: ["affordance:DISCOVER", "affordance:SEARCH", "affordance:MATCH", "domain:community", "domain:travel", "domain:education", "source_kind:public_api", "source_kind:rss", "source_kind:mcp"]
        empty_character: "No observed candidate material answers this shape yet."
        refusal_character: "This address cannot widen into a private or institutional candidate closure."
        extent_defaults: "section"
        navigation_neighbors: ["worlds", "board", "territory"]
        requires: []
    }
    stage: {
        question: "What is progressing in this occurrence, and what is lawful to do now?"
        visible_sections: ["temporal focus", "current observations", "participation context"]
        preferred_renderers: ["temporal-stage", "event-rail"]
        renderer_family: "stage"
        renderer_variant: "timeline"
        route: "/stage"
        interaction_density: "high"
        allowed_affordance_families: ["inspect", "follow", "open_context"]
        external_demand_roles: ["affordance:OBSERVE", "affordance:STREAM", "affordance:SCHEDULE", "affordance:ALERT", "domain:sports", "domain:weather", "domain:transport", "domain:media", "source_kind:public_api", "source_kind:rss"]
        empty_character: "No lawful live observation is available for this occurrence."
        refusal_character: "A stage does not disclose backstage state merely because an occurrence is public."
        extent_defaults: "page"
        navigation_neighbors: ["observations", "spatial", "channels"]
        requires: []
    }
    channels: {
        question: "Which conversations and notices belong in this context?"
        visible_sections: ["threads", "notices", "conversation boundaries"]
        preferred_renderers: ["thread-stream", "notice-rail"]
        renderer_family: "thread"
        renderer_variant: "threaded_stream"
        route: "/community"
        interaction_density: "high"
        allowed_affordance_families: ["read", "reply", "follow"]
        external_demand_roles: ["affordance:COMMUNICATE", "affordance:TRANSLATE", "affordance:ALERT", "affordance:PUBLISH", "domain:community", "domain:media", "domain:education", "source_kind:mcp", "source_kind:rss"]
        empty_character: "There is no conversation admitted to this context."
        refusal_character: "A channel may not infer membership from topical similarity."
        extent_defaults: "page"
        navigation_neighbors: ["community", "inbox", "stage"]
        requires: []
    }
    community: {
        question: "Who and what are gathered here under this community's terms?"
        visible_sections: ["body", "shared work", "public norms"]
        preferred_renderers: ["community-atlas", "member-constellation"]
        renderer_family: "community"
        renderer_variant: "community_profile"
        route: "/community"
        interaction_density: "medium"
        allowed_affordance_families: ["inspect", "join_request", "open_context"]
        external_demand_roles: ["affordance:IDENTIFY", "affordance:COMMUNICATE", "affordance:PUBLISH", "domain:community", "domain:people", "domain:government", "source_kind:public_api", "source_kind:mcp"]
        empty_character: "No public body has been composed for this community."
        refusal_character: "A community surface never turns an observed network into membership."
        extent_defaults: "page"
        navigation_neighbors: ["channels", "worlds", "board"]
        requires: []
    }
    board: {
        question: "What heterogeneous material helps this situation move forward?"
        visible_sections: ["arrangement", "references", "working alternatives"]
        preferred_renderers: ["heterogeneous-board", "comparison-tray"]
        renderer_family: "board"
        renderer_variant: "masonry_board"
        route: "/board"
        interaction_density: "medium"
        allowed_affordance_families: ["inspect", "arrange", "compare"]
        external_demand_roles: ["affordance:COMPARE", "affordance:SEARCH", "affordance:DISCOVER", "affordance:OPTIMIZE", "domain:research", "domain:art", "domain:construction", "domain:food", "source_kind:public_api", "source_kind:mcp"]
        empty_character: "Nothing has been arranged here yet."
        refusal_character: "A board cannot convert a reference into an approved plan."
        extent_defaults: "workspace"
        navigation_neighbors: ["discover", "worlds", "studio"]
        requires: []
    }
    spatial: {
        question: "How is this world arranged across place, route and capacity?"
        visible_sections: ["map", "layers", "coverage and route"]
        preferred_renderers: ["spatial-field", "route-inspector"]
        renderer_family: "spatial"
        renderer_variant: "map"
        route: "/spatial"
        interaction_density: "high"
        allowed_affordance_families: ["inspect", "locate", "route"]
        external_demand_roles: ["affordance:LOCATE", "affordance:ROUTE", "affordance:MEASURE", "affordance:OBSERVE", "domain:weather", "domain:transport", "domain:water", "domain:energy", "domain:telecommunications", "source_kind:public_api", "source_kind:machine_protocol"]
        empty_character: "No observed spatial layer is available at this address."
        refusal_character: "A map marker is not proof of possession, presence or authority."
        extent_defaults: "page"
        navigation_neighbors: ["stage", "observations", "territory"]
        requires: []
    }
    listings: {
        question: "Which declared capacity can be compared here without claiming a completed exchange?"
        visible_sections: ["availability", "terms", "comparison"]
        preferred_renderers: ["market-grid", "quote-comparison"]
        renderer_family: "market"
        renderer_variant: "listing_grid"
        route: "/listings"
        interaction_density: "medium"
        allowed_affordance_families: ["inspect", "compare", "quote_request"]
        external_demand_roles: ["affordance:QUOTE", "affordance:RESERVE", "affordance:PURCHASE", "affordance:COMPARE", "domain:trade", "domain:hospitality", "domain:labor", "domain:construction", "source_kind:public_api", "source_kind:x402", "source_kind:mcp"]
        empty_character: "No declared availability meets this shape."
        refusal_character: "Availability is not a quote, commitment, payment or settlement."
        extent_defaults: "page"
        navigation_neighbors: ["discover", "inbox", "territory"]
        requires: []
    }
    inbox: {
        question: "What is waiting for a lawful response in this context?"
        visible_sections: ["waiting items", "timing", "response boundary"]
        preferred_renderers: ["inbox-queue", "exception-rail"]
        renderer_family: "work"
        renderer_variant: "priority_queue"
        route: "/inbox"
        interaction_density: "high"
        allowed_affordance_families: ["inspect", "acknowledge", "open_context"]
        external_demand_roles: ["affordance:ALERT", "affordance:NOTIFY", "affordance:SCHEDULE", "domain:community", "domain:transport", "domain:weather", "domain:medicine", "source_kind:rss", "source_kind:public_api", "source_kind:mcp"]
        empty_character: "Nothing is waiting at this scope."
        refusal_character: "An incoming signal cannot assign an obligation without its owner."
        extent_defaults: "workspace"
        navigation_neighbors: ["console", "channels", "observations"]
        requires: []
    }
    territory: {
        question: "Why is this material here, and which boundary governs it?"
        visible_sections: ["address", "provenance", "withholding"]
        preferred_renderers: ["territory-inspector", "lineage-panel"]
        renderer_family: "inspector"
        renderer_variant: "provenance_panel"
        route: "/territory"
        interaction_density: "low"
        allowed_affordance_families: ["inspect", "trace", "open_context"]
        external_demand_roles: ["affordance:VERIFY", "affordance:INSPECT", "affordance:REPORT", "affordance:IDENTIFY", "domain:government", "domain:law", "domain:land", "domain:research", "source_kind:machine_protocol", "source_kind:public_api"]
        empty_character: "This address has no additional public provenance material."
        refusal_character: "Territory explains a boundary; it never grants passage through it."
        extent_defaults: "section"
        navigation_neighbors: ["observations", "link", "worlds"]
        requires: ["information_policy"]
    }
    observations: {
        question: "What did the outside world report, when, and under which evidence boundary?"
        visible_sections: ["observation stream", "freshness", "provenance"]
        preferred_renderers: ["evidence-stream", "freshness-panel"]
        renderer_family: "inspector"
        renderer_variant: "evidence_ledger"
        route: "/observations"
        interaction_density: "high"
        allowed_affordance_families: ["inspect", "compare", "trace"]
        external_demand_roles: ["affordance:OBSERVE", "affordance:MEASURE", "affordance:VERIFY", "affordance:ALERT", "domain:weather", "domain:science", "domain:medicine", "domain:energy", "domain:water", "source_kind:public_api", "source_kind:rss", "source_kind:machine_protocol"]
        empty_character: "No admitted observation answers this address yet."
        refusal_character: "An external candidate is not an admitted observation or evidence."
        extent_defaults: "page"
        navigation_neighbors: ["stage", "spatial", "territory"]
        requires: ["information_policy"]
    }
    worlds: {
        question: "Which composed situations can make this material useful together?"
        visible_sections: ["world composition", "demands", "related contexts"]
        preferred_renderers: ["world-atlas", "composition-graph"]
        renderer_family: "board"
        renderer_variant: "world_gallery"
        route: "/w"
        interaction_density: "medium"
        allowed_affordance_families: ["inspect", "open_context", "compare"]
        external_demand_roles: ["affordance:MATCH", "affordance:DISCOVER", "affordance:SIMULATE", "affordance:PREDICT", "domain:community", "domain:family", "domain:household", "domain:farms", "domain:buildings", "source_kind:public_api", "source_kind:mcp"]
        empty_character: "No World declaration currently asks for this material."
        refusal_character: "A shared source does not make two Worlds the same World."
        extent_defaults: "page"
        navigation_neighbors: ["packs", "discover", "board"]
        requires: ["pack"]
    }
    packs: {
        question: "Which reusable appetites can compose lawful outside-world support?"
        visible_sections: ["demand closure", "candidate families", "lineage"]
        preferred_renderers: ["pack-closure", "capability-lattice"]
        renderer_family: "work"
        renderer_variant: "dependency_graph"
        route: "/packs"
        interaction_density: "medium"
        allowed_affordance_families: ["inspect", "compare", "open_context"]
        external_demand_roles: ["affordance:DISCOVER", "affordance:COMPARE", "affordance:OPTIMIZE", "affordance:SEARCH", "domain:computers", "domain:agents", "domain:telecommunications", "domain:trade", "domain:research", "source_kind:mcp", "source_kind:machine_protocol", "source_kind:public_api"]
        empty_character: "No Pack demand is available at this scope."
        refusal_character: "A Pack requests a capability; it does not admit a provider."
        extent_defaults: "page"
        navigation_neighbors: ["worlds", "link", "studio"]
        requires: ["pack"]
    }
    link: {
        question: "What external relationship can be inspected without treating it as an admitted connection?"
        visible_sections: ["external identity", "carrier", "binding boundary"]
        preferred_renderers: ["connection-inspector", "carrier-map"]
        renderer_family: "inspector"
        renderer_variant: "relationship_inspector"
        route: "/link"
        interaction_density: "medium"
        allowed_affordance_families: ["inspect", "verify", "open_context"]
        external_demand_roles: ["affordance:IDENTIFY", "affordance:VERIFY", "affordance:AUTHENTICATE", "affordance:INSPECT", "domain:computers", "domain:agents", "domain:telecommunications", "domain:government", "source_kind:machine_protocol", "source_kind:mcp", "source_kind:public_api"]
        empty_character: "No external relationship has been declared here."
        refusal_character: "A listed carrier is not a configured or authorized binding."
        extent_defaults: "section"
        navigation_neighbors: ["territory", "packs", "console"]
        requires: []
    }
    studio: {
        question: "Which declared composition can be shaped here before publication?"
        visible_sections: ["draft composition", "references", "publication boundary"]
        preferred_renderers: ["composition-studio", "reference-board"]
        renderer_family: "work"
        renderer_variant: "studio_canvas"
        route: "/studio"
        interaction_density: "high"
        allowed_affordance_families: ["inspect", "draft", "compare"]
        external_demand_roles: ["affordance:PUBLISH", "affordance:COMPARE", "affordance:SEARCH", "affordance:OPTIMIZE", "domain:art", "domain:media", "domain:film", "domain:music", "domain:research", "source_kind:mcp", "source_kind:public_api", "source_kind:rss"]
        empty_character: "There is no authored composition in this studio scope."
        refusal_character: "Drafting a composition does not publish it or authorize its effects."
        extent_defaults: "workspace"
        navigation_neighbors: ["board", "packs", "console"]
        requires: []
    }
    console: {
        question: "What is the estate doing, withholding, waiting on, or failing to compose?"
        visible_sections: ["source weather", "demand pressure", "exceptions"]
        preferred_renderers: ["operational-observatory", "exception-matrix"]
        renderer_family: "inspector"
        renderer_variant: "observatory"
        route: "/console"
        interaction_density: "very_high"
        allowed_affordance_families: ["inspect", "trace", "open_context"]
        external_demand_roles: ["affordance:INSPECT", "affordance:OPTIMIZE", "affordance:REPORT", "affordance:VERIFY", "domain:computers", "domain:agents", "domain:finance", "domain:government", "domain:telecommunications", "source_kind:machine_protocol", "source_kind:mcp", "source_kind:public_api"]
        empty_character: "No operational residue is exposed at this scope."
        refusal_character: "Visibility into an exception does not authorize its resolution."
        extent_defaults: "workspace"
        navigation_neighbors: ["packs", "worlds", "territory"]
        requires: ["institution", "information_policy"]
    }
})

classPresentation: close({
    anonymous: {
        question_suffix: "What can a public visitor understand without an account assumption?"
        experience_character: "public orientation over source-derived material"
        visible_sections: ["public boundary"]
        allowed_affordance_families: ["open", "inspect"]
        empty_character: "Nothing public answers this address yet."
        refusal_character: "This public view does not imply a private relationship or permission."
        interaction_density: "low"
        requires: ["information_policy"]
    }
    personal: {
        question_suffix: "What intersects the actor's real relations, Habitat and permitted Packs?"
        experience_character: "relation, Habitat and permitted-Pack aware projection"
        visible_sections: ["personal context", "kept relations"]
        allowed_affordance_families: ["save", "join_request", "go"]
        empty_character: "Nothing admitted to this personal context answers yet."
        refusal_character: "A personal presentation cannot invent a relation, Habitat or Pack closure."
        interaction_density: "medium"
        requires: ["relation", "pack", "habitat", "information_policy"]
    }
    member_worker: {
        question_suffix: "What intersects the real community, mission or work closure?"
        experience_character: "scoped member, project and work projection"
        visible_sections: ["shared context", "work boundary"]
        allowed_affordance_families: ["participate", "handoff", "open_context"]
        empty_character: "Nothing admitted to this member or work scope is waiting."
        refusal_character: "This presentation cannot infer membership, assignment or authority."
        interaction_density: "high"
        requires: ["relation", "pack", "work_scope", "institution", "information_policy"]
    }
    operator: {
        question_suffix: "Which controls and exceptions are visible around the already-resolved closure?"
        experience_character: "operational observability over real owner boundaries"
        visible_sections: ["operational context", "owner boundaries"]
        allowed_affordance_families: ["inspect", "trace", "request_operation"]
        empty_character: "No operational residue is visible at this resolved closure."
        refusal_character: "A visible operator control is not authority to admit, spend, publish or effect."
        interaction_density: "very_high"
        requires: ["institution", "information_policy"]
    }
})

surfacePresentationCellSet: [
    for g, gp in grammarPresentation
    for pc, cp in classPresentation {
    #SurfacePresentationCell & {
        grammar: g
        presentation_class: pc
        question: "\(gp.question) \(cp.question_suffix)"
        experience_character: cp.experience_character
        visible_sections: list.Concat([gp.visible_sections, cp.visible_sections])
        preferred_renderers: gp.preferred_renderers
        renderer_family: gp.renderer_family
        renderer_variant: gp.renderer_variant
        route: gp.route
        interaction_density: cp.interaction_density
        allowed_affordance_families: list.Concat([gp.allowed_affordance_families, cp.allowed_affordance_families])
        external_demand_roles: gp.external_demand_roles
        empty_character: cp.empty_character
        refusal_character: cp.refusal_character
        extent_defaults: gp.extent_defaults
        navigation_neighbors: gp.navigation_neighbors
        requires: list.Concat([gp.requires, cp.requires])
    }
    }
]
