package control

#SiteTerritory: "life" | "live" | "shop" | "work"
#SiteStatus: "draft" | "published" | "retired"
#SlotExtent: "chip" | "card" | "section" | "page" | "workspace" | "immersive"
#SemanticSlotName: "hero" | "lead" | "feature" | "secondary" | "ticker" | "rail" | "gallery" | "media" | "body" | "sidebar" | "footer_feature"
#MaterialQuality: "any" | "standard" | "high"

#ApplicationKind: "PublicationFeed" | "Newsroom" | "Article" | "Gallery" | "Watch" | "Listen" | "Podcast" | "Thread" | "Community" | "Board" | "LiveEvent" | "Calendar" | "Scoreboard" | "Standings" | "Ticker" | "MarketTape" | "QuoteBook" | "Product" | "Marketplace" | "Checkout" | "Ticketing" | "Map" | "Route" | "Property" | "HousingSearch" | "Restaurant" | "Reservation" | "Ride" | "Shipment" | "Tracking" | "Weather" | "Incident" | "Alert" | "ResearchPaper" | "Dataset" | "Experiment" | "Repository" | "IssueTracker" | "BuildStatus" | "Documentation" | "SupportCase" | "ServiceRequest" | "Grant" | "FundingOpportunity" | "Job" | "Applicant" | "Course" | "Lesson" | "Quiz" | "ProviderDirectory" | "Appointment" | "Policy" | "Claim" | "Portfolio" | "Asset" | "Vehicle" | "Maintenance" | "Facility" | "UtilityStatus"

#SitePresentation: {
    site_id: string & != ""
    host: string & =~"^[a-z0-9.-]+\\.aurekai\\.(life|live|shop|work)$"
    subject_address: string & != ""
    profile: string & =~"^ghost:"
    territory: #SiteTerritory
    navigation: [...string]
    status: #SiteStatus
}

#SemanticSlot: {
    slot_id: string & != ""
    site_id: string & != ""
    slot_name: #SemanticSlotName
    ordinal: int & >=0
    extent: #SlotExtent
    accepts: [string & != "", ...string]
    min_material_quality: #MaterialQuality
    max_items: int & >0
    htmx_boundary?: string
    status: #SiteStatus
}

#SourceCollection: {
    collection_id: string & != ""
    subject_address: string & != ""
    source_selector: {...}
    material_kind: [string & != "", ...string]
    refresh_policy: string & != ""
    status: #SiteStatus
}

#ApplicationBinding: {
    binding_id: string & != ""
    site_id: string & != ""
    slot_id: string & != ""
    application_kind: #ApplicationKind
    material_query: {...}
    source_collection?: string
    action_aperture?: string
    continuity_key?: string
    status: #SiteStatus
}

#CapabilityProgram: {
    program_id: string & != ""
    application_kind: #ApplicationKind
    action: string & != ""
    consumes: [string & != "", ...string]
    produces: [string & != "", ...string]
    carrier_candidates: [string & != "", ...string]
    capability_requirements: [string & != "", ...string]
    authority_requirement: string & != ""
    transition_conditions: {...}
    failure_projection: string & != ""
    status: #SiteStatus
}

#ExperienceStage: {
    stage_id: string & != ""
    experience: string & != ""
    application_kind: #ApplicationKind
    consumes: [string & != "", ...string]
    produces: [string & != "", ...string]
    material_queries: {...}
    renderer: string & != ""
    extent: #SlotExtent
    actions: [string & != "", ...string]
    next_stage_rules: {...}
    empty_projection: string & != ""
    refused_projection: string & != ""
    status: #SiteStatus
}

#SiteProgram: {
    program_id: string & != ""
    site_id: string & != ""
    experience_programs: [string & != "", ...string]
    application_bindings: [string & != "", ...string]
    renderer_preferences: {...}
    source_collections: [string & != "", ...string]
    capability_programs: [string & != "", ...string]
    actor_presentations: {...}
    status: #SiteStatus
}
