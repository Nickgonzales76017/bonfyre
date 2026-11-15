package stores

// cmd/BonfyreMediaDelivery: store DDL, byte-for-byte what the owner executes.
// Compiled to cmd/BonfyreMediaDelivery/generated/store_schema.h by scripts/compile-store-schemas.sh.
store_schemas: "cmd/BonfyreMediaDelivery": {
	fragments: [
		{
			macro:  "BF_STORE_MEDIA_DELIVERY_MEDIA_DELIVERY_RIGHT"
			source: "cmd/BonfyreMediaDelivery/src/main.c"
			statements: [
				{kind: "other", name: "", sql: "PRAGMA journal_mode=WAL;"},
				{kind: "table", name: "media_delivery_right", sql: "CREATE TABLE IF NOT EXISTS media_delivery_right(right_id TEXT PRIMARY KEY,library_id TEXT NOT NULL,subject_ref TEXT NOT NULL,representation_id TEXT NOT NULL,actor_subject TEXT NOT NULL,protocol TEXT NOT NULL CHECK(protocol IN('hls','dash','moqt')),starts_at INTEGER NOT NULL,ends_at INTEGER NOT NULL,state TEXT NOT NULL CHECK(state IN('active','revoked')),evidence_ref TEXT NOT NULL,granted_by TEXT NOT NULL,created_at INTEGER NOT NULL);"},
				{kind: "index", name: "media_delivery_right_lookup", sql: "CREATE INDEX IF NOT EXISTS media_delivery_right_lookup ON media_delivery_right(library_id,subject_ref,representation_id,actor_subject,protocol,state,starts_at,ends_at);"},
				{kind: "table", name: "media_transport_lease", sql: "CREATE TABLE IF NOT EXISTS media_transport_lease(lease_id TEXT PRIMARY KEY,right_id TEXT NOT NULL,library_id TEXT NOT NULL,subject_ref TEXT NOT NULL,representation_id TEXT NOT NULL,actor_subject TEXT NOT NULL,protocol TEXT NOT NULL,issued_at INTEGER NOT NULL,expires_at INTEGER NOT NULL,state TEXT NOT NULL CHECK(state IN('active','revoked','expired')),created_at INTEGER NOT NULL);"},
			]
		},
	]
}
