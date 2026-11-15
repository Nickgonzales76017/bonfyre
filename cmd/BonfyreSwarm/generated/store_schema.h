/* Generated from schemas/storage/stores by scripts/compile-store-schemas.sh. Do not edit. */
#ifndef BONFYRE_STORE_SCHEMA_CMD_BONFYRESWARM_H
#define BONFYRE_STORE_SCHEMA_CMD_BONFYRESWARM_H
/* cmd/BonfyreSwarm/src/scoped_transfer.h */
#define BF_STORE_SWARM_SWARM_SCOPED_TRANSFERS \
    "CREATE TABLE IF NOT EXISTS swarm_scoped_transfers (transfer_id TEXT NOT NULL, direction TEXT NOT NULL, scope_hash TEXT NOT NULL, recipient_hash TEXT NOT NULL, sha256 TEXT NOT NULL, size_bytes INTEGER NOT NULL, expires INTEGER NOT NULL, state TEXT NOT NULL, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, PRIMARY KEY(transfer_id,direction));"
/* cmd/BonfyreSwarm/src/main.c */
#define BF_STORE_SWARM_PEERS \
    "CREATE TABLE IF NOT EXISTS peers (  peer_id TEXT NOT NULL,  addr TEXT NOT NULL,  port INTEGER NOT NULL,  info_hash TEXT NOT NULL,  bytes_uploaded INTEGER DEFAULT 0,  bytes_downloaded INTEGER DEFAULT 0,  last_seen TEXT DEFAULT (datetime('now')),  gate_token TEXT,  PRIMARY KEY (peer_id, info_hash));"
/* cmd/BonfyreSwarm/src/main.c */
#define BF_STORE_SWARM_SWARM_ARTIFACTS \
    "CREATE TABLE IF NOT EXISTS swarm_artifacts (  info_hash TEXT PRIMARY KEY,  family_key TEXT,  artifact_type TEXT,  size INTEGER,  piece_count INTEGER,  piece_hashes BLOB,  seeders INTEGER DEFAULT 0,  leechers INTEGER DEFAULT 0,  created_at TEXT DEFAULT (datetime('now')));"
/* cmd/BonfyreSwarm/src/main.c */
#define BF_STORE_SWARM_SWARM_METER \
    "CREATE TABLE IF NOT EXISTS swarm_meter (  id INTEGER PRIMARY KEY AUTOINCREMENT,  peer_id TEXT NOT NULL,  info_hash TEXT NOT NULL,  direction TEXT NOT NULL,  bytes INTEGER NOT NULL,  recorded_at TEXT DEFAULT (datetime('now')));"
#endif
