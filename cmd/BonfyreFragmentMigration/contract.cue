package fragmentmigration

fragmentMigrationContract: {
	schema: "bonfyre.fragment-migration.contract.v1"
	compiler: {
		input: ["bonfyre.surface.literal-candidates.v2", "bonfyre.surface.source-map.v1"]
		outputs: ["drafts.jsonl", "review.jsonl", "manifest.json"]
		exact_equivalence: "normalized UTF-8 byte identity"
		semantic_similarity_owner: "BonfyreRecall"
		publication_effect: false
	}
	admission: {
		read_mode: "canonical CMS SQLite; PRAGMA query_only=ON"
		write_owner: "BonfyreCMS"
		write_path: "entry import-jsonl site_fragment"
		status_required: "draft"
		duplicate_input_coordinates: "refuse"
		existing_authored_coordinates: "skip; never overwrite"
		transactionality: "BonfyreCMS batch transaction"
		publication_effect: false
	}
	cannot_infer: [
		"literal candidate -> authored fragment",
		"lexical or learned similarity -> exact equivalence",
		"draft admission -> publication",
		"draft admission -> authority",
		"migration candidate -> permission to remove compiled fallback",
	]
	promotion_requires: [
		"exact SurfaceIR projection witness",
		"separate publication effect",
		"verified source patch before compiled fallback removal",
	]
}
