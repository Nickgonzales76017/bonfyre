package control

#PatternExtent: "chip" | "card" | "section" | "page" | "workspace"
#PatternSource: "cms_type" | "native_organ"
#SurfacePattern: {
    name: string & != ""
    territory: #Territory
    reads: #PatternSource
    source: string & != ""
    extent: #PatternExtent
    actions: [...#StandingAction] | *[]
    authority?: string
    lineage?: string
    if len(actions) > 0 {
        extent: "page" | "workspace"
        authority: string & != ""
    }
}
#ReferenceUse: "donor" | "implementation" | "reference"
#ReferenceSource: {
    project: string & != ""
    license: string & != ""
    use: #ReferenceUse
    solves: string & != ""
}

permissiveLicenses: ["MIT", "BSD-2-Clause", "BSD-3-Clause", "Apache-2.0", "ISC"]
restrictedLicenses: {
    "AGPL-3.0": "network copyleft would extend to this estate"
    "GPL-3.0": "copyleft would extend to this estate"
    FSL: "field-of-use restriction forbids competing use until it converts"
    mixed: "licensing varies per file and needs file-level review first"
}

// Authored surface commons. Views reuse these semantic owners; no cloned backend.
surfacePatterns: [...#SurfacePattern] & [
    {name:"VerticalDiscovery", territory:"live", reads:"native_organ", source:"StandingAddress", extent:"card", lineage:"short-video feeds"},
    {name:"LiveCreatorRoom", territory:"live", reads:"native_organ", source:"LiveSemanticTrack", extent:"workspace", actions:["WATCH","FOLLOW","JOIN","GO"], authority:"participate:live", lineage:"live streaming"},
    {name:"EphemeralBoard", territory:"live", reads:"cms_type", source:"Post", extent:"section", lineage:"imageboards"},
    {name:"LiveMoment", territory:"live", reads:"native_organ", source:"LiveSemanticTrack", extent:"chip"},
    {name:"LiveSemanticOverlay", territory:"live", reads:"native_organ", source:"LiveSemanticTrack", extent:"section"},
    {name:"LiveTimeline", territory:"live", reads:"native_organ", source:"LiveSemanticTrack", extent:"page", actions:["SAVE"], authority:"save:moment"},
    {name:"AudienceHandoff", territory:"live", reads:"native_organ", source:"StandingAddress", extent:"section", lineage:"live streaming"},
    {name:"ActionCard", territory:"live", reads:"native_organ", source:"StandingAddress", extent:"card"},
    {name:"PersonalWorld", territory:"life", reads:"cms_type", source:"Presence", extent:"page", actions:["PLAN","SHARE"], authority:"operate:self"},
    {name:"CollectionBoard", territory:"life", reads:"cms_type", source:"Collection", extent:"page", actions:["SAVE","SHARE"], authority:"curate:collection", lineage:"Pinterest"},
    {name:"HabitatMap", territory:"life", reads:"native_organ", source:"AddressPlane", extent:"workspace", actions:["GO","SAVE","PLAN"], authority:"operate:habitat", lineage:"Google Maps"},
    {name:"PlacePage", territory:"life", reads:"native_organ", source:"AddressPlane", extent:"page", actions:["GO","SAVE"], authority:"participate:place"},
    {name:"NeighborhoodPulse", territory:"life", reads:"cms_type", source:"Post", extent:"page", actions:["HELP","JOIN"], authority:"participate:neighborhood", lineage:"Nextdoor"},
    {name:"InterestCommunity", territory:"life", reads:"cms_type", source:"Circle", extent:"page", actions:["JOIN","FOLLOW"], authority:"participate:circle", lineage:"Reddit"},
    {name:"MemoryTimeline", territory:"life", reads:"cms_type", source:"Memory", extent:"page", actions:["REMEMBER","SHARE"], authority:"operate:self", lineage:"Immich"},
    {name:"PlanBoard", territory:"life", reads:"cms_type", source:"Plan", extent:"page", actions:["PLAN","SAVE"], authority:"operate:self"},
    {name:"PeopleGraph", territory:"life", reads:"native_organ", source:"RelationshipGraph", extent:"page", actions:["MEET","FOLLOW"], authority:"operate:relationships"},
    {name:"HomeHabitat", territory:"life", reads:"native_organ", source:"AddressPlane", extent:"workspace", actions:["MOVE","PLAN"], authority:"operate:habitat", lineage:"Home Assistant"},
    {name:"MerchantPresence", territory:"shop", reads:"cms_type", source:"Presence", extent:"page", actions:["BUY","SAVE"], authority:"transact:merchant", lineage:"Shopify"},
    {name:"ProductDiscovery", territory:"shop", reads:"native_organ", source:"StandingAddress", extent:"card"},
    {name:"ListingSearch", territory:"shop", reads:"native_organ", source:"ForeignTwinGraph", extent:"section", lineage:"eBay"},
    {name:"ProductDetail", territory:"shop", reads:"cms_type", source:"Product", extent:"page", actions:["BUY","COMPARE","SAVE"], authority:"transact:merchant"},
    {name:"OfferCompare", territory:"shop", reads:"native_organ", source:"MoneyGraph", extent:"page", actions:["COMPARE","BUY"], authority:"transact:merchant"},
    {name:"LocalAvailability", territory:"shop", reads:"native_organ", source:"ForeignTwinGraph", extent:"section", lineage:"Walmart"},
    {name:"CartWorkspace", territory:"shop", reads:"cms_type", source:"Cart", extent:"workspace", actions:["BUY","SAVE","COMPARE"], authority:"transact:cart"},
    {name:"Checkout", territory:"shop", reads:"native_organ", source:"CommitmentGraph", extent:"page", actions:["BUY"], authority:"commit:purchase"},
    {name:"OrderTimeline", territory:"shop", reads:"native_organ", source:"OccurrenceSpine", extent:"page", actions:["REQUEST"], authority:"transact:order"},
    {name:"GroupBuy", territory:"shop", reads:"native_organ", source:"CommitmentGraph", extent:"page", actions:["JOIN","CONTRIBUTE"], authority:"commit:collective"},
    {name:"SellFlow", territory:"shop", reads:"cms_type", source:"Listing", extent:"workspace", actions:["SELL","OFFER"], authority:"operate:seller"},
    {name:"ProfessionalPresence", territory:"work", reads:"cms_type", source:"Presence", extent:"page", actions:["CONTACT","APPLY"], authority:"engage:professional", lineage:"LinkedIn"},
    {name:"WorkInbox", territory:"work", reads:"native_organ", source:"WorkGraph", extent:"workspace", actions:["DO","CLAIM","ESCALATE"], authority:"operate:work"},
    {name:"ChannelThread", territory:"work", reads:"cms_type", source:"Message", extent:"page", actions:["RESPOND"], authority:"participate:channel", lineage:"Zulip"},
    {name:"IssueBoard", territory:"work", reads:"native_organ", source:"WorkGraph", extent:"workspace", actions:["CLAIM","ASSIGN","DO"], authority:"operate:work", lineage:"Linear"},
    {name:"ProjectRoadmap", territory:"work", reads:"native_organ", source:"WorkGraph", extent:"page", actions:["ASSIGN"], authority:"operate:work"},
    {name:"KnowledgePage", territory:"work", reads:"cms_type", source:"Document", extent:"page", actions:["REVIEW"], authority:"author:knowledge", lineage:"Notion"},
    {name:"DecisionPage", territory:"work", reads:"native_organ", source:"EvidenceGraph", extent:"page", actions:["APPROVE","REVIEW"], authority:"decide:proposal"},
    {name:"CRMRecord", territory:"work", reads:"cms_type", source:"Account", extent:"page", actions:["CONTACT"], authority:"operate:relationships", lineage:"Salesforce"},
    {name:"CalendarWorkspace", territory:"work", reads:"cms_type", source:"Meeting", extent:"workspace", actions:["BOOK","RESPOND"], authority:"operate:schedule", lineage:"Cal.com"},
    {name:"ApprovalQueue", territory:"work", reads:"native_organ", source:"AuthorityGraph", extent:"workspace", actions:["APPROVE","ESCALATE"], authority:"decide:approval"},
    {name:"AgentWorkmate", territory:"work", reads:"native_organ", source:"WorkGraph", extent:"workspace", actions:["ASSIGN","REVIEW"], authority:"delegate:agent"},
    {name:"ActivityStream", territory:"work", reads:"native_organ", source:"OccurrenceSpine", extent:"section"},
]

referenceSources: [...#ReferenceSource] & [
    {project:"Pinry", license:"BSD-2-Clause", use:"donor", solves:"collecting intent: boards, pins, tags"},
    {project:"MapLibre GL JS", license:"BSD-3-Clause", use:"donor", solves:"the spatial canvas"},
    {project:"Home Assistant frontend", license:"Apache-2.0", use:"donor", solves:"home, devices, rooms, control"},
    {project:"Fedify", license:"MIT", use:"implementation", solves:"ActivityPub, WebFinger, HTTP signatures"},
    {project:"Lemmy", license:"AGPL-3.0", use:"reference", solves:"interest communities"},
    {project:"Pixelfed", license:"AGPL-3.0", use:"reference", solves:"federated photo interaction"},
    {project:"Immich", license:"AGPL-3.0", use:"reference", solves:"personal memory timeline and search"},
    {project:"Mobilizon", license:"AGPL-3.0", use:"reference", solves:"federated event and group vocabulary"},
    {project:"Spree Storefront", license:"MIT", use:"donor", solves:"production storefront and one-page checkout"},
    {project:"Next.js Commerce", license:"MIT", use:"donor", solves:"provider-adapter separation"},
    {project:"Hydrogen", license:"MIT", use:"donor", solves:"headless commerce interaction"},
    {project:"Bagisto", license:"MIT", use:"donor", solves:"marketplace and multitenant patterns"},
    {project:"Saleor storefront", license:"FSL", use:"reference", solves:"premium storefront design"},
    {project:"Zulip", license:"Apache-2.0", use:"donor", solves:"topic-within-stream conversation"},
    {project:"Mattermost webapp", license:"mixed", use:"reference", solves:"team conversation shell"},
    {project:"Plane", license:"AGPL-3.0", use:"reference", solves:"issue and cycle interaction"},
    {project:"AppFlowy", license:"AGPL-3.0", use:"reference", solves:"block-based knowledge pages"},
    {project:"Twenty", license:"AGPL-3.0", use:"reference", solves:"CRM relationship model"},
    {project:"Cal.com", license:"AGPL-3.0", use:"reference", solves:"scheduling and availability"},
]
