package stores

// cmd/BonfyreFragmentMigration: store DDL, byte-for-byte what the owner executes.
// Compiled to cmd/BonfyreFragmentMigration/generated/store_schema.h by scripts/compile-store-schemas.sh.
store_schemas: "cmd/BonfyreFragmentMigration": {
	fragments: [
		{
			macro:  "BF_STORE_FRAGMENT_MIGRATION_EQUIVALENCE"
			source: "cmd/BonfyreFragmentMigration/src/compile.c"
			statements: [
				{kind: "table", name: "equivalence", sql: "CREATE TABLE equivalence(hash TEXT PRIMARY KEY,n INTEGER NOT NULL)"},
			]
		},
		{
			macro:  "BF_STORE_FRAGMENT_MIGRATION_SEEN"
			source: "cmd/BonfyreFragmentMigration/src/admit.c"
			statements: [
				{kind: "table", name: "seen", sql: "CREATE TABLE seen(coordinate TEXT PRIMARY KEY)"},
			]
		},
	]
}
