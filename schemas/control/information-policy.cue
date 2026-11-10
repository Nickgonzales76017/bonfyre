package control

// Closed authored input for InformationPolicy. This is deliberately a
// declaration validator rather than a second policy engine: native projection
// owners will evaluate the exact rules below, while YAFF retains architecture
// and C owns the eventual durable decision boundary.
#DisclosureAudience: "self" | "household" | "group" | "community" | "public" | "market" | "institution"

#FieldDisclosure: {
	field_name: string & !=""
	audiences: [...#DisclosureAudience]
	reason?: string & !=""
}

#AggregationFloor: {
	statistic: string & !=""
	minimum_population: int & >=2
	audiences?: [...#DisclosureAudience]
}

#InformationPolicy: {
	subject_kind: string & !=""
	fields: [...#FieldDisclosure]
	floors?: [...#AggregationFloor]
	evidence_release?: [...string & !=""]
}

// Concrete ABI order. Audiences are exact labels, never a permission ladder.
informationPolicyBinding: {
	audiences: ["self", "household", "group", "community", "public", "market", "institution"]
	aggregate_default_audiences: ["public", "market", "institution"]
}
