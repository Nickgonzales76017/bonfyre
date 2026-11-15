package stores

// cmd/BonfyreSwarm: store DDL, byte-for-byte what the owner executes.
// Compiled to cmd/BonfyreSwarm/generated/store_schema.h by scripts/compile-store-schemas.sh.
store_schemas: "cmd/BonfyreSwarm": {
	fragments: [
		{
			macro:  "BF_STORE_SWARM_SWARM_SCOPED_TRANSFERS"
			source: "cmd/BonfyreSwarm/src/scoped_transfer.h"
			statements: [
				{kind: "table", name: "swarm_scoped_transfers", sql: "CREATE TABLE IF NOT EXISTS swarm_scoped_transfers (transfer_id TEXT NOT NULL, direction TEXT NOT NULL, scope_hash TEXT NOT NULL, recipient_hash TEXT NOT NULL, sha256 TEXT NOT NULL, size_bytes INTEGER NOT NULL, expires INTEGER NOT NULL, state TEXT NOT NULL, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, PRIMARY KEY(transfer_id,direction));"},
			]
		},
		{
			macro:  "BF_STORE_SWARM_PEERS"
			source: "cmd/BonfyreSwarm/src/main.c"
			statements: [
				{kind: "table", name: "peers", sql: "CREATE TABLE IF NOT EXISTS peers (  peer_id TEXT NOT NULL,  addr TEXT NOT NULL,  port INTEGER NOT NULL,  info_hash TEXT NOT NULL,  bytes_uploaded INTEGER DEFAULT 0,  bytes_downloaded INTEGER DEFAULT 0,  last_seen TEXT DEFAULT (datetime('now')),  gate_token TEXT,  PRIMARY KEY (peer_id, info_hash));"},
			]
		},
		{
			macro:  "BF_STORE_SWARM_SWARM_ARTIFACTS"
			source: "cmd/BonfyreSwarm/src/main.c"
			statements: [
				{kind: "table", name: "swarm_artifacts", sql: "CREATE TABLE IF NOT EXISTS swarm_artifacts (  info_hash TEXT PRIMARY KEY,  family_key TEXT,  artifact_type TEXT,  size INTEGER,  piece_count INTEGER,  piece_hashes BLOB,  seeders INTEGER DEFAULT 0,  leechers INTEGER DEFAULT 0,  created_at TEXT DEFAULT (datetime('now')));"},
			]
		},
		{
			macro:  "BF_STORE_SWARM_SWARM_METER"
			source: "cmd/BonfyreSwarm/src/main.c"
			statements: [
				{kind: "table", name: "swarm_meter", sql: "CREATE TABLE IF NOT EXISTS swarm_meter (  id INTEGER PRIMARY KEY AUTOINCREMENT,  peer_id TEXT NOT NULL,  info_hash TEXT NOT NULL,  direction TEXT NOT NULL,  bytes INTEGER NOT NULL,  recorded_at TEXT DEFAULT (datetime('now')));"},
			]
		},
	]
}
