package stores

// cmd/BonfyreShowcaseCrosswalk: store DDL, byte-for-byte what the owner executes.
// Compiled to cmd/BonfyreShowcaseCrosswalk/generated/store_schema.h by scripts/compile-store-schemas.sh.
store_schemas: "cmd/BonfyreShowcaseCrosswalk": {
	fragments: [
		{
			macro:  "BF_STORE_SHOWCASE_CROSSWALK_ESTATE"
			source: "cmd/BonfyreShowcaseCrosswalk/src/main.c"
			statements: [
				{kind: "table", name: "estate", sql: "CREATE TABLE estate(id TEXT PRIMARY KEY,name TEXT,url TEXT,host TEXT,kind TEXT);"},
				{kind: "index", name: "estate_url", sql: "CREATE INDEX estate_url ON estate(url);"},
				{kind: "index", name: "estate_host", sql: "CREATE INDEX estate_host ON estate(host);"},
				{kind: "index", name: "estate_name", sql: "CREATE INDEX estate_name ON estate(name);"},
				{kind: "table", name: "showcase", sql: "CREATE TABLE showcase(id TEXT PRIMARY KEY,label TEXT,authority TEXT,url TEXT,host TEXT,carrier TEXT,auth TEXT,protocols TEXT,worlds INTEGER);"},
			]
		},
	]
}
