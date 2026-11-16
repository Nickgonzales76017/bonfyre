package stores

// cmd/BonfyreBitNet: store DDL, byte-for-byte what the owner executes.
// Compiled to cmd/BonfyreBitNet/generated/store_schema.h by scripts/compile-store-schemas.sh.
store_schemas: "cmd/BonfyreBitNet": {
	fragments: [
		{
			macro:  "BF_STORE_BIT_NET_BITNET_RANK_RUN"
			source: "cmd/BonfyreBitNet/src/main.c"
			statements: [
				{kind: "other", name: "", sql: "PRAGMA journal_mode=WAL;"},
				{kind: "table", name: "bitnet_rank_run", sql: "CREATE TABLE IF NOT EXISTS bitnet_rank_run(run_id TEXT PRIMARY KEY,representation_id TEXT NOT NULL,scope TEXT NOT NULL,query_digest TEXT NOT NULL,gate_state TEXT NOT NULL,dimensions INTEGER NOT NULL,candidate_count INTEGER NOT NULL,created_at INTEGER NOT NULL);"},
				{kind: "table", name: "embedding_projection", sql: "CREATE TABLE IF NOT EXISTS embedding_projection(run_id TEXT NOT NULL,candidate_identity TEXT NOT NULL,representation_id TEXT NOT NULL,source_digest TEXT NOT NULL,vector_digest TEXT NOT NULL,vector BLOB NOT NULL,score REAL NOT NULL,exact_verifier TEXT NOT NULL,lowerer_ref TEXT NOT NULL,causal_frontier TEXT NOT NULL,PRIMARY KEY(run_id,candidate_identity));"},
				{kind: "table", name: "ranked_candidate_envelope", sql: "CREATE TABLE IF NOT EXISTS ranked_candidate_envelope(run_id TEXT NOT NULL,rank INTEGER NOT NULL,candidate_identity TEXT NOT NULL,candidate_kind TEXT NOT NULL,owner_ref TEXT NOT NULL,exact_verifier TEXT NOT NULL,lowerer_ref TEXT NOT NULL,PRIMARY KEY(run_id,rank),UNIQUE(run_id,candidate_identity));"},
				{kind: "other", name: "", sql: "BEGIN IMMEDIATE;"},
			]
		},
		{
			macro:  "BF_STORE_BIT_NET_Q"
			source: "cmd/BonfyreBitNet/src/main.c"
			statements: [
				{kind: "table", name: "q", sql: "CREATE TABLE q(identity TEXT PRIMARY KEY,observed INTEGER NOT NULL)"},
			]
		},
	]
}

// The rank store: a persisted BitNet run orders an AddressPlane survivor
// frontier, and the frontier table keeps every survivor at its rank so a later
// slice widens the attempt without rerunning the model. Retention evaluation
// attaches it read-only beside its private validation table.
store_schemas: "cmd/BonfyreBitNet": stores: rank: {
	kind:  "ranker"
	macro: "BITNET_RANK"
	attach: {as: "rank_source"}
	ranker: {
		key_space:      "AddressPlane survivor candidate identities of one forwarded frontier"
		run_table:      "bitnet_rank_run"
		frontier_table: "ranked_candidate_envelope"
		key_column:     "candidate_identity"
		order_column:   "rank"
		weights:        "the ModelRepresentationPassport named by bitnet_rank_run.representation_id (BitNet embedding 270M I2_S, SHADOW unless ADMIT)"
	}
}
