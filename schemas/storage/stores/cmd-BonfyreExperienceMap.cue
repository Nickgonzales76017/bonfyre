package stores

// cmd/BonfyreExperienceMap: store DDL, byte-for-byte what the owner executes.
// Compiled to cmd/BonfyreExperienceMap/generated/store_schema.h by scripts/compile-store-schemas.sh.
store_schemas: "cmd/BonfyreExperienceMap": {
	fragments: [
		{
			macro:  "BF_STORE_EXPERIENCE_MAP_GRAPH"
			source: "cmd/BonfyreExperienceMap/src/owner_resolution.c"
			statements: [
				{kind: "table", name: "graph", sql: "CREATE TABLE graph(body TEXT NOT NULL);"},
				{kind: "table", name: "owners", sql: "CREATE TABLE owners(id TEXT PRIMARY KEY NOT NULL CHECK(length(id)>0),body TEXT NOT NULL);"},
				{kind: "table", name: "edges", sql: "CREATE TABLE edges(id TEXT PRIMARY KEY NOT NULL,source TEXT NOT NULL,destination TEXT NOT NULL,body TEXT NOT NULL);"},
				{kind: "table", name: "workflows", sql: "CREATE TABLE workflows(id TEXT PRIMARY KEY NOT NULL,body TEXT NOT NULL);"},
			]
		},
	]
}
