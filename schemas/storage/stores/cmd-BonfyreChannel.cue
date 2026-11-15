package stores

// cmd/BonfyreChannel: store DDL, byte-for-byte what the owner executes.
// Compiled to cmd/BonfyreChannel/generated/store_schema.h by scripts/compile-store-schemas.sh.
store_schemas: "cmd/BonfyreChannel": {
	fragments: [
		{
			macro:  "BF_STORE_CHANNEL_CREATOR_CHANNEL"
			source: "cmd/BonfyreChannel/src/main.c"
			statements: [
				{kind: "other", name: "", sql: "PRAGMA journal_mode=WAL;"},
				{kind: "table", name: "creator_channel", sql: "CREATE TABLE IF NOT EXISTS creator_channel(channel_id TEXT PRIMARY KEY,owner_subject TEXT NOT NULL,display_name TEXT NOT NULL,handle TEXT NOT NULL UNIQUE,visibility TEXT NOT NULL CHECK(visibility IN('public','unlisted','private')),community_ref TEXT NOT NULL DEFAULT '',state TEXT NOT NULL CHECK(state IN('active','retired')),updated_at INTEGER NOT NULL);"},
				{kind: "table", name: "channel_publication", sql: "CREATE TABLE IF NOT EXISTS channel_publication(channel_id TEXT NOT NULL REFERENCES creator_channel(channel_id),subject_ref TEXT NOT NULL,media_program_ref TEXT NOT NULL,program_kind TEXT NOT NULL CHECK(program_kind IN('video','short','live')),state TEXT NOT NULL CHECK(state IN('published','removed')),published_at INTEGER NOT NULL,PRIMARY KEY(channel_id,subject_ref,media_program_ref));"},
				{kind: "table", name: "channel_subscription", sql: "CREATE TABLE IF NOT EXISTS channel_subscription(channel_id TEXT NOT NULL REFERENCES creator_channel(channel_id),actor_subject TEXT NOT NULL,state TEXT NOT NULL CHECK(state IN('active','unsubscribed')),updated_at INTEGER NOT NULL,PRIMARY KEY(channel_id,actor_subject));"},
				{kind: "index", name: "channel_publication_surface", sql: "CREATE INDEX IF NOT EXISTS channel_publication_surface ON channel_publication(channel_id,state,published_at DESC);"},
				{kind: "index", name: "channel_subscription_actor", sql: "CREATE INDEX IF NOT EXISTS channel_subscription_actor ON channel_subscription(actor_subject,state,updated_at DESC);"},
			]
		},
	]
}
