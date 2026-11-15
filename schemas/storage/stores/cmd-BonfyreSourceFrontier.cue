package stores

// cmd/BonfyreSourceFrontier: store DDL, byte-for-byte what the owner executes.
// Compiled to cmd/BonfyreSourceFrontier/generated/store_schema.h by scripts/compile-store-schemas.sh.
store_schemas: "cmd/BonfyreSourceFrontier": {
	fragments: [
		{
			macro:  "BF_STORE_SOURCE_FRONTIER_INVENTORY"
			source: "cmd/BonfyreSourceFrontier/src/main.c"
			statements: [
				{kind: "other", name: "", sql: "PRAGMA journal_mode=DELETE;"},
				{kind: "table", name: "inventory", sql: "CREATE TABLE IF NOT EXISTS inventory(label TEXT PRIMARY KEY, first_seen TEXT, last_seen TEXT, rows INTEGER DEFAULT 0, refused INTEGER DEFAULT 0);"},
				{kind: "table", name: "root", sql: "CREATE TABLE IF NOT EXISTS root(root_id TEXT PRIMARY KEY, host TEXT NOT NULL, name TEXT DEFAULT '', identity TEXT DEFAULT '', kind TEXT DEFAULT '', robots TEXT DEFAULT 'unknown', economic TEXT DEFAULT '', first_seen TEXT, last_seen TEXT);"},
				{kind: "table", name: "root_origin", sql: "CREATE TABLE IF NOT EXISTS root_origin(root_id TEXT, label TEXT, listings INTEGER DEFAULT 0, PRIMARY KEY(root_id,label));"},
				{kind: "table", name: "family", sql: "CREATE TABLE IF NOT EXISTS family(family_id TEXT PRIMARY KEY, root_id TEXT NOT NULL, protocol TEXT NOT NULL, endpoint TEXT DEFAULT '', discovered_from TEXT DEFAULT '', first_seen TEXT);"},
				{kind: "table", name: "channel", sql: "CREATE TABLE IF NOT EXISTS channel(channel_id TEXT PRIMARY KEY, family_id TEXT NOT NULL, root_id TEXT NOT NULL, address TEXT NOT NULL, exact_address TEXT NOT NULL, title TEXT DEFAULT '', material TEXT DEFAULT '', protocol TEXT DEFAULT 'unknown', state TEXT NOT NULL DEFAULT 'candidate', truth_level TEXT DEFAULT 'listed', refusal TEXT DEFAULT '', virtual INTEGER DEFAULT 0, census_source TEXT DEFAULT '', credential TEXT DEFAULT '', rights TEXT DEFAULT '', cadence TEXT DEFAULT '', discovered_from TEXT DEFAULT '', body_sha256 TEXT DEFAULT '', identity TEXT DEFAULT '', first_seen TEXT, last_seen TEXT, last_contributed TEXT DEFAULT '', contributions INTEGER DEFAULT 0);"},
				{kind: "table", name: "channel_origin", sql: "CREATE TABLE IF NOT EXISTS channel_origin(channel_id TEXT, label TEXT, PRIMARY KEY(channel_id,label));"},
				{kind: "table", name: "channel_census", sql: "CREATE TABLE IF NOT EXISTS channel_census(channel_id TEXT, census_source TEXT, material TEXT DEFAULT '', PRIMARY KEY(channel_id,census_source));"},
				{kind: "table", name: "contribution", sql: "CREATE TABLE IF NOT EXISTS contribution(route TEXT, channel_id TEXT, day TEXT, items INTEGER DEFAULT 0, PRIMARY KEY(route,channel_id,day));"},
				{kind: "table", name: "gap", sql: "CREATE TABLE IF NOT EXISTS gap(gap_id TEXT PRIMARY KEY, root_id TEXT NOT NULL, channel_id TEXT DEFAULT '', kind TEXT NOT NULL, state TEXT NOT NULL, resolutions TEXT NOT NULL, dependents INTEGER DEFAULT 0, measured TEXT DEFAULT '', status TEXT DEFAULT 'open', first_seen TEXT, last_seen TEXT);"},
				{kind: "table", name: "attention", sql: "CREATE TABLE IF NOT EXISTS attention(root_id TEXT PRIMARY KEY, level TEXT NOT NULL, reason TEXT DEFAULT '', updated TEXT);"},
				{kind: "index", name: "channel_root", sql: "CREATE INDEX IF NOT EXISTS channel_root ON channel(root_id);"},
				{kind: "index", name: "channel_family", sql: "CREATE INDEX IF NOT EXISTS channel_family ON channel(family_id);"},
				{kind: "index", name: "family_root", sql: "CREATE INDEX IF NOT EXISTS family_root ON family(root_id);"},
				{kind: "index", name: "channel_exact_address", sql: "CREATE INDEX IF NOT EXISTS channel_exact_address ON channel(exact_address);"},
				{kind: "index", name: "root_host", sql: "CREATE INDEX IF NOT EXISTS root_host ON root(host);"},
				{kind: "index", name: "channel_state", sql: "CREATE INDEX IF NOT EXISTS channel_state ON channel(state);"},
				{kind: "index", name: "channel_by_census", sql: "CREATE INDEX IF NOT EXISTS channel_by_census ON channel(census_source);"},
				{kind: "index", name: "contribution_channel", sql: "CREATE INDEX IF NOT EXISTS contribution_channel ON contribution(channel_id);"},
			]
		},
		{
			macro:  "BF_STORE_SOURCE_FRONTIER_CHANNEL_PUBLIC_SELECTION"
			source: "cmd/BonfyreSourceFrontier/src/main.c"
			statements: [
				{kind: "index", name: "channel_public_selection", sql: "CREATE INDEX IF NOT EXISTS channel_public_selection ON channel(state,material,carries,census_source,root_id,protocol,last_contributed,contributions,first_seen)"},
			]
		},
	]
}
