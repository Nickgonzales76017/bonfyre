package control

#SurfaceGrammar: "discover" | "stage" | "channels" | "community" | "board" |
    "spatial" | "listings" | "inbox" | "territory" | "observations" |
    "worlds" | "packs" | "link" | "studio" | "console"

#PresentationClass: "anonymous" | "personal" | "member_worker" | "operator"

#SurfacePresentationCell: {
    grammar: #SurfaceGrammar
    presentation_class: #PresentationClass
    question: string & != ""
    experience_character: string & != ""
    visible_sections: [string & != "", ...string]
    preferred_renderers: [string & != "", ...string]
    renderer_family: string & != ""
    renderer_variant: string & != ""
    route: string & != ""
    interaction_density: "low" | "medium" | "high" | "very_high"
    allowed_affordance_families: [string & != "", ...string]
    external_demand_roles: [string & != "", ...string]
    empty_character: string & != ""
    refusal_character: string & != ""
    extent_defaults: "chip" | "card" | "section" | "page" | "workspace"
    navigation_neighbors: [string & != "", ...string]
    requires: [...("relation" | "pack" | "habitat" | "work_scope" | "institution" | "information_policy")]
}
