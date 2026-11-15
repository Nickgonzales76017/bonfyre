package stores

// cmd/BonfyreDiscipl: store DDL, byte-for-byte what the owner executes.
// Compiled to cmd/BonfyreDiscipl/generated/store_schema.h by scripts/compile-store-schemas.sh.
store_schemas: "cmd/BonfyreDiscipl": {
	fragments: [
		{
			macro:  "BF_STORE_DISCIPL_DISCIPL_INVOCATION"
			source: "cmd/BonfyreDiscipl/src/main.c"
			statements: [
				{kind: "table", name: "discipl_invocation", sql: "CREATE TABLE IF NOT EXISTS discipl_invocation( family TEXT PRIMARY KEY, binary TEXT, subcommand TEXT, wants_outdir INTEGER, verified_at TEXT, fixture TEXT)"},
			]
		},
	]
}
