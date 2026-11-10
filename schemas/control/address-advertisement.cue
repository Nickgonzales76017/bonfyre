package control

// CandidateAdvertisement is the portable compiled forwarding descriptor.  It
// is derived from an owner or compiler's canonical declaration and is
// disposable: it cannot replace any owner record or effect-time check.
#CandidateKind: "surface_renderer" | "site_presentation" | "site_program" | "semantic_slot" | "application_binding" | "world_publication" | "source_collection" | "external_source" | "capability_program" | "experience_stage" | "pack" | "pack_demand" | "relation" | "social_presence" | "information_policy" | "spatial_observation" | "coverage_volume" | "mcp_projection" | "protocol_pattern" | "protocol_chain" | "carrier" | "provider" | "compute" | "transformation" | "target_obligation" | "external_occurrence" | "media_library" | "creator_channel" | "interaction_grammar" | "projection"

// The axis vocabulary is declared here once and mirrored by the native
// forwarding plane; `bonfyre-external-estate kinds --axes` prints what the
// binary accepts and the estate witness proves the two sets are identical.
// Two independently maintained copies of this vocabulary is how a plane starts
// answering a question with terms that no owner meant.
//
// An axis says which question a value answers, never that the answer is
// available, permitted, affordable or true.  Every axis below is forwarding
// eligibility only.
#AddressAxis:
	// what a row is addressed as
	"cms" | "subject" | "subject_kind" | "institution" | "world" | "site" | "library" | "territory" | "domain" | "source_kind" | "affordance" | "interaction" |
	// how it is presented
	"presentation" | "profile" | "renderer" | "grammar" | "extent" | "slot" | "density" | "projection" | "experience" | "application" | "binding" |
	// what it draws on
	"source" | "source_collection" | "material" | "refresh" |
	// Temporal is its own forwarding coordinate. It selects only publication
	// phenotypes the temporal owner classified; it is neither refresh policy
	// nor a claim that an occurrence is currently true.
	"temporal" |
	// what it does
	"operation" | "consumes" | "produces" | "transition" | "condition" |
	// what it needs before it could be done -- a requirement, never a grant
	"carrier" | "protocol" | "requirement" | "authority" | "scope" | "purpose" | "target" | "admission" |
	// who it concerns -- a declared relation, never a disclosure decision
	"audience" | "community" | "member" | "role" | "state" | "surface" | "disclosure" | "field" | "evidence" |
	// behaviour published over it
	"pack" | "capability_program" | "demand" | "regime" | "reheat" |
	// where it is, coarsely. These narrow a population before an exact spatial
	// closure runs; they are not geometry and cannot answer a spatial question.
	// `mode` carries how a position was arrived at, so an interpolated display
	// fix can never be selected by asking for what a source observed.
	"spatial" | "mobility" | "scale" | "region" | "cell" | "geometry_kind" | "mode" | "derivation" |
	// what runs it
	"provider" | "compute" | "representation" | "transform" | "geometry" | "version" | "exactness" | "measurement" | "mcp" |
	// what a public estate's stated terms allow or require -- the source's terms, never a grant
	"reuse"

#AddressTerm: =~"^(cms|subject|subject_kind|institution|world|site|library|territory|domain|source_kind|affordance|interaction|presentation|profile|renderer|grammar|extent|slot|density|projection|experience|application|binding|source|source_collection|material|refresh|temporal|operation|consumes|produces|transition|condition|carrier|protocol|requirement|authority|scope|purpose|target|admission|audience|community|member|role|state|surface|disclosure|field|evidence|pack|capability_program|demand|regime|reheat|spatial|mobility|scale|region|cell|geometry_kind|mode|derivation|provider|compute|representation|transform|geometry|version|exactness|measurement|mcp|reuse):[a-z0-9-]+$"

#CandidateAdvertisement: {
    identity: string & != ""
    candidate_kind: #CandidateKind

    // References, not copied state.  Exact verification is mandatory after
    // mask/SID/ranking selection, and lowerer_ref is a compiler endpoint.
    owner_ref: string & != ""
    exact_verifier: string & != ""
    lowerer_ref: string & != ""
    validity_frontier?: string

    // Coordinates are only forwarding eligibility.  They cannot express a
    // grant, proof, provider admission, relation, payment or effect right.
    terms: [...#AddressTerm]
    dependencies: [...string & != ""]
}

// A compiled generation is validated as a whole population, not one sampled
// record: a schema that only ever sees a hand-written example is a schema the
// emitters are not held to.
#CandidateAdvertisementSet: [...#CandidateAdvertisement]
