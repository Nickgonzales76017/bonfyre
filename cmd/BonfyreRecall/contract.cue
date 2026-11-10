// BonfyreRecall owns learned candidate geometry, not canonical semantics.
package recall

#Geometry: {
	model:  "nomic-embed-text-onnx"
	dims:   768
	metric: "cosine"
}

#RecallRequest: {
	store: string & != ""
	text:  string & != ""
	top:   int & >=1 & <=1024
	geometry: #Geometry
}

#Candidate: {
	id: string & != ""
	distance: number
	cosine_similarity?: number & >=-1 & <=1
	source?: string
	type?: string
	text?: string
}

#RecallResult: {
	schema: "bonfyre.semantic-recall.v1"
	geometry: #Geometry
	truth: "candidate_only"
	result: {
		results: [...#Candidate]
		top_k: int & >=1
		mode?: string
		scanned?: int & >=0
	}
	cannot_infer: [
		"semantic proximity -> exact identity",
		"candidate -> publication or visibility",
		"candidate -> evidence, authority or effect",
	]
}

#IndexRequest: {
	store: string & != ""
	apply: bool
	geometry: #Geometry
	canonical_owner: string & != ""
}

#IndexResult: {
	truth: "candidate_store"
	geometry: #Geometry
	publication_effect: false
	indexable_count: int & >=0
	vector_count?: int & >=0
	verified_complete?: bool
}

#OwnerRecheck: {
	candidate_id: string & != ""
	owner: string & != ""
	required: [...("identity" | "status" | "visibility" | "assertion" | "membership" | "authority" | "evidence")]
	truth_after_recheck: "owner_admitted" | "refused"
}

// Candidate stores are rebuildable derivatives. A vector-store rebuild may
// change ranking without changing canonical state. Surface/runtime caches that
// depend on learned ranking must therefore include the candidate-store frontier
// in their dependency cut or bypass cache reuse for searched projections.
#CacheRule: {
	uses_learned_ranking: bool
	candidate_store_dependency_declared: bool
	may_reuse_projection_cache: !uses_learned_ranking | candidate_store_dependency_declared
}
