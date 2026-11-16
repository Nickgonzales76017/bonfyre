package stores

// cmd/BonfyreConnectionCatalog reads two cold Parquet files and keeps no hot
// copy: the connector facets and the source entries they were cut from. Each
// declared column is one the catalogue query reads; the owner refuses a file
// that lacks any of them before it runs the query.

store_schemas: "cmd/BonfyreConnectionCatalog": stores: {
	connection_facets: {
		kind:       "columnar"
		macro:      "CONNECTION_FACETS"
		written_by: "the connection studio hunt (workboard-connector-facets-*.parquet)"
		columnar: {
			format:    "parquet"
			cold_glob: "argument: list --facets <file.parquet>"
			columns: [
				{name: "catalogue", type: "utf8"},
				{name: "raw_key", type: "utf8"},
				{name: "source_url_candidate", type: "utf8"},
				{name: "source_goal_id", type: "utf8"},
				{name: "source_goal_key", type: "utf8"},
				{name: "original_ordinal", type: "int32"},
				{name: "row_sha256", type: "utf8"},
				{name: "source_entry", type: "utf8"},
				{name: "status", type: "utf8"},
			]
			promote: "never: the cold files are the catalogue; nothing is promoted to a hot table"
		}
	}
	connection_entries: {
		kind:       "columnar"
		macro:      "CONNECTION_ENTRIES"
		written_by: "the connection studio hunt (workboard-source-entries-*.parquet)"
		columnar: {
			format:    "parquet"
			cold_glob: "argument: list --entries <file.parquet>"
			columns: [
				{name: "source_goal_id", type: "utf8"},
				{name: "source_goal_key", type: "utf8"},
				{name: "original_ordinal", type: "int32"},
				{name: "source_entry", type: "utf8"},
				{name: "source_url_candidate", type: "utf8"},
				{name: "row_sha256", type: "utf8"},
				{name: "raw_line", type: "utf8"},
				{name: "goal_source", type: "utf8"},
			]
			promote: "never: the cold files are the catalogue; nothing is promoted to a hot table"
		}
	}
}
