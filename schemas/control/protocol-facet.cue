package control

// The generalization: a protocol integration is a facet, not a vertical
// adapter, and facets are compiled from the phenotype of the thing crossing
// the fabric rather than from its domain.
//
// Compiling from domains degenerates into `if earthquake then FDSN`, which is
// 739 integrations. Compiling from phenotypes makes the same 739 a library of
// typed transition grammars, and one object may carry several phenotypes at
// once.

// Finite and architectural. A facet that adds none of these adds nothing.
#RelationKind:
	"DISCOVER" | "IDENTIFY" | "AUTHENTICATE" | "AUTHORIZE" |
	"OBSERVE" | "SYNCHRONIZE" | "DESCRIBE" | "QUERY" | "FILTER" |
	"SUBSCRIBE" | "NOTIFY" | "TRANSFER" | "STREAM" | "PUBLISH" | "FEDERATE" |
	"TASK" | "COMMAND" | "NEGOTIATE" | "QUOTE" | "COMMIT" | "PAY" | "SETTLE" |
	"PROVE" | "VERIFY" | "ROUND_TRIP"

// What the thing crossing the fabric is, structurally. Not exclusive: a
// property is a SPATIAL_RESOURCE and a VERSIONED_RECORD and an ECONOMIC_OFFER
// at the same time, and each phenotype opens different lawful facets.
#Phenotype:
	"VERSIONED_RECORD" | "EVENT" | "LIVE_ENTITY" | "MEASUREMENT_STREAM" |
	"FIELD_COVERAGE" | "DATASET" | "MEDIA_ASSET" | "COMMANDABLE_RESOURCE" |
	"ECONOMIC_OFFER" | "SUPPLY_CHAIN_OBJECT" | "IDENTITY_CREDENTIAL" |
	"SERVICE_CAPABILITY" | "DOCUMENT" | "SPATIAL_RESOURCE" | "OBLIGATION"

// The five states, and collapsing any adjacent pair is the failure mode.
#FacetMaturity:
	"candidate_match" |          // a phenotype and a protocol could meet here
	"contract_compiled" |        // a ProtocolContract exists for this facet
	"enacted" |                  // the exchange actually ran
	"obligations_closed" |       // what each side still owed is discharged
	"effect_authorized"          // and only then may an effect follow

// AddressPlane coordinates for facets are affordances, never protocol names.
// `supports_gtfs` becomes another taxonomy swamp; a live moving public entity
// and a private physiological measurement share `temporal:live` and are split
// by InformationPolicy and Authority rather than by routing vocabulary.
#AddressAffordance:
	"semantic:event" | "semantic:dataset" | "semantic:measurement" | "semantic:offer" |
	"semantic:record" | "semantic:document" | "semantic:capability" | "semantic:credential" |
	"temporal:live" | "temporal:historical" | "temporal:versioned" |
	"spatial:point" | "spatial:trajectory" | "spatial:coverage" |
	"interaction:read" | "interaction:subscribe" | "interaction:command" |
	"interaction:commit" | "interaction:settle" |
	"resource:compute" | "resource:network" | "resource:physical" | "resource:human" |
	"privacy:public" | "privacy:restricted" |
	"effect:none" | "effect:reversible" | "effect:physical" | "effect:financial"

// Everything the compiler is allowed to see about one occurrence. Absent
// fields are absent, not empty: no actor context means no actor-scoped facet
// may be compiled, rather than one compiled for a generic actor.
#OccurrenceBraidInput: {
	occurrence_id: string & != ""

	subjects: [...string & != ""]
	owner_receipts: [...string & != ""]
	domain_profiles: [...string & != ""]
	phenotypes: [...#Phenotype]

	valid_frontier?: string & != ""
	knowledge_frontier?: string & != ""

	rights_profile?: string & != ""
	information_policy?: string & != ""

	exact_owner_refs: [...string & != ""]
	representation_passports: [...string & != ""]

	observed_capabilities: [...string & != ""]
	protocol_descriptors: [...string & != ""]

	world_demand?: string & != ""
	actor_context?: string & != ""
	authority_cut?: string & != ""
}

// What a protocol contributes at one junction, and what it must not be read to
// imply. `relation_kind` is the whole point: a facet that adds no previously
// unavailable relation is not mature, however well it parses.
#ProtocolFacetContract: {
	protocol_family: string & != ""
	relation_kind: #RelationKind

	from_owner: string & != ""
	to_owner: string & != ""

	consumes: [...string & != ""]
	produces: [...string & != ""]

	evidence_requirements: [...string & != ""]
	semantic_requirements: [...string & != ""]

	authority_requirement?: string & != ""
	information_policy_requirement?: string & != ""

	representation_requirements: [...string & != ""]
	carrier_candidates: [...string & != ""]

	invalidated_by: [...string & != ""]
	roundtrip_probe?: string & != ""

	// Which phenotype admitted this relation, and on which tier of evidence.
	// Added to the declared shape because a facet that cannot name the
	// phenotype it came from is a domain match wearing a phenotype label, and
	// because shape evidence and domain evidence are not the same strength:
	// `shape` means the thing itself said so, `domain` means the
	// neighbourhood it sits in usually does.
	via_phenotype?: #Phenotype
	phenotype_derivation?: "shape" | "domain"

	// Never defaulted upward. A compiled facet claims candidate_match until
	// something witnesses the next rung.
	maturity: #FacetMaturity

	// A facet may not take ownership of a fact already owned elsewhere; this
	// names the owner it defers to for every fact it touches.
	defers_to: [...string & != ""]
}

// Density measured as shape, not as length. A straight line of eight owners is
// not denser than six branches that rejoin, and the longest path scores the
// second one lower.
#RealizedBraidDensity: {
	distinct_owners: int & >=0
	owner_transition_count: int & >=0

	longest_owner_path: int & >=0

	branch_points: int & >=0
	rejoin_points: int & >=0

	protocol_contracts_compiled: int & >=0
	protocol_contracts_enacted: int & >=0
	obligations_closed: int & >=0

	semantic_profiles_used: int & >=0
	exact_owner_queries: int & >=0

	representations_used: int & >=0
	carrier_families_used: int & >=0

	evidence_edges: int & >=0
	rights_edges: int & >=0
	authority_edges: int & >=0

	invalidation_fanout: int & >=0
	recompute_cut_size: int & >=0

	human_decisions: int & >=0
	machine_decisions: int & >=0

	commitments: int & >=0
	effects: int & >=0
	compensations: int & >=0

	roundtrips: int & >=0
	faithful_roundtrips: int & >=0
	drifted_roundtrips: int & >=0
}
