package stores

// cmd/BonfyreExperienceStage: store DDL, byte-for-byte what the owner executes.
// Compiled to cmd/BonfyreExperienceStage/generated/store_schema.h by scripts/compile-store-schemas.sh.
store_schemas: "cmd/BonfyreExperienceStage": {
	fragments: [
		{
			macro:  "BF_STORE_EXPERIENCE_STAGE_RAW_EXTERNAL"
			source: "cmd/BonfyreExperienceStage/src/compiler.c"
			statements: [
				{kind: "table", name: "raw_external", sql: "CREATE TABLE raw_external(id TEXT PRIMARY KEY,json TEXT NOT NULL)"},
			]
		},
	]
}
