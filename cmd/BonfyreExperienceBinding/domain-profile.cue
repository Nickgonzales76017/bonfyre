package experiencebinding

#DomainAudience: "self" | "household" | "group" | "community" | "public" | "market" | "institution"

#DomainField: {
	name: string & != ""
	audiences: [#DomainAudience, ...#DomainAudience]
}

#CarrierField: {
	semantic: string & != ""
	carrier: string & != ""
}

#CarrierProjection: {
	carrier: string & != ""
	standard: string & != ""
	fields: [#CarrierField, ...#CarrierField]
}

#NativeDomainProfile: {
	name: string & != ""
	evidence_requirements: [string & != "", ...string]
	fields: [#DomainField, ...#DomainField]
	projections: [#CarrierProjection, ...#CarrierProjection]
}

// This is the semantic owner used by the retained-reference observation chain.
// It does not claim that the observed page conforms to the external standard;
// it only declares which extracted fields may become a Bonfyre semantic record.
nativeDomainProfiles: [#NativeDomainProfile & {
	name: "reference-standard"
	evidence_requirements: ["foreign_observation_receipt", "ingest_artifact"]
	fields: [
		{name:"authority", audiences:["self", "household", "group", "community", "public", "market", "institution"]},
		{name:"description", audiences:["self", "household", "group", "community", "public", "market", "institution"]},
		{name:"name", audiences:["self", "household", "group", "community", "public", "market", "institution"]},
		{name:"source_url", audiences:["self", "household", "group", "community", "public", "market", "institution"]},
		{name:"updated_at", audiences:["self", "household", "group", "community", "public", "market", "institution"]},
	]
	projections: [{
		carrier: "semantic-json"
		standard: "Bonfyre semantic record"
		fields: [
			{semantic:"authority", carrier:"authority"}, {semantic:"description", carrier:"description"},
			{semantic:"name", carrier:"name"}, {semantic:"source_url", carrier:"source_url"},
			{semantic:"updated_at", carrier:"updated_at"},
		]
	}]
}]
