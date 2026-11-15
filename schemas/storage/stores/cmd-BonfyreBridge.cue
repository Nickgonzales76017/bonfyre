package stores

// cmd/BonfyreBridge: store DDL, byte-for-byte what the owner executes.
// Compiled to cmd/BonfyreBridge/generated/store_schema.h by scripts/compile-store-schemas.sh.
store_schemas: "cmd/BonfyreBridge": {
	fragments: [
		{
			macro:  "BF_STORE_BRIDGE_S"
			source: "cmd/BonfyreBridge/src/transform.c"
			statements: [
				{kind: "table", name: "%s", sql: "CREATE TABLE \"%s\" (\n"},
			]
		},
		{
			macro:  "BF_STORE_BRIDGE_S_2"
			source: "cmd/BonfyreBridge/src/transform.c"
			statements: [
				{kind: "index", name: "%s", sql: "CREATE INDEX \"%s\" ON \"%s\" ("},
			]
		},
	]
}
