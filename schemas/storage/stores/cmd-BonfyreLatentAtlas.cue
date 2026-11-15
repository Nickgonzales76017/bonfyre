package stores

// cmd/BonfyreLatentAtlas: store DDL, byte-for-byte what the owner executes.
// Compiled to cmd/BonfyreLatentAtlas/generated/store_schema.h by scripts/compile-store-schemas.sh.
store_schemas: "cmd/BonfyreLatentAtlas": {
	fragments: [
		{
			macro:  "BF_STORE_LATENT_ATLAS_CHART"
			source: "cmd/BonfyreLatentAtlas/src/main.c"
			statements: [
				{kind: "other", name: "", sql: "PRAGMA journal_mode=WAL;"},
				{kind: "table", name: "chart", sql: "CREATE TABLE IF NOT EXISTS chart(chart_id TEXT PRIMARY KEY, family TEXT NOT NULL CHECK(family IN ('dense','hdc','hyperbolic','geo','circular','distribution','subspace','fingerprint','trajectory')), dims INTEGER NOT NULL, version TEXT NOT NULL, encoder TEXT NOT NULL, universe TEXT NOT NULL DEFAULT '');"},
				{kind: "table", name: "projection", sql: "CREATE TABLE IF NOT EXISTS projection(address TEXT NOT NULL, chart_id TEXT NOT NULL REFERENCES chart(chart_id), coordinate BLOB NOT NULL, source_digest TEXT NOT NULL, frontier TEXT NOT NULL DEFAULT '', updated_at TEXT NOT NULL, PRIMARY KEY(address, chart_id));"},
				{kind: "table", name: "label", sql: "CREATE TABLE IF NOT EXISTS label(address TEXT PRIMARY KEY, label TEXT NOT NULL);"},
				{kind: "table", name: "bridge", sql: "CREATE TABLE IF NOT EXISTS bridge(from_chart TEXT NOT NULL, to_chart TEXT NOT NULL, operator TEXT NOT NULL, calibration_error REAL NOT NULL, loss_envelope TEXT NOT NULL, validity_frontier TEXT NOT NULL, provenance TEXT NOT NULL, PRIMARY KEY(from_chart, to_chart));"},
				{kind: "table", name: "relation", sql: "CREATE TABLE IF NOT EXISTS relation(src TEXT NOT NULL, relation TEXT NOT NULL CHECK(relation IN ('same_subject_as','materializes','observed_as','performance_of', 'depicts','mentions','derived_from','fingerprints_to','projected_into','member_of','located_in','version_of','happens_at')), dst TEXT NOT NULL, evidence TEXT NOT NULL, PRIMARY KEY(src, relation, dst));"},
				{kind: "table", name: "neighbourhood", sql: "CREATE TABLE IF NOT EXISTS neighbourhood(logical TEXT PRIMARY KEY, lens TEXT NOT NULL, level INTEGER NOT NULL, chart_id TEXT NOT NULL, representation_type TEXT NOT NULL, current_snapshot TEXT, parent TEXT);"},
				{kind: "table", name: "snapshot", sql: "CREATE TABLE IF NOT EXISTS snapshot(snapshot_id TEXT PRIMARY KEY, logical TEXT NOT NULL, universe TEXT NOT NULL, lens TEXT NOT NULL, chart_id TEXT NOT NULL, member_closure TEXT NOT NULL, children TEXT NOT NULL DEFAULT '', representation TEXT NOT NULL, created_at TEXT NOT NULL);"},
				{kind: "table", name: "snapshot_member", sql: "CREATE TABLE IF NOT EXISTS snapshot_member(snapshot_id TEXT NOT NULL, address TEXT NOT NULL, source_digest TEXT NOT NULL, PRIMARY KEY(snapshot_id, address));"},
				{kind: "table", name: "latent_delta", sql: "CREATE TABLE IF NOT EXISTS latent_delta(address TEXT NOT NULL, chart_id TEXT NOT NULL, from_digest TEXT NOT NULL, to_digest TEXT NOT NULL, moved REAL NOT NULL, at TEXT NOT NULL);"},
				{kind: "table", name: "envelope_hold", sql: "CREATE TABLE IF NOT EXISTS envelope_hold(parent TEXT NOT NULL, child TEXT NOT NULL, child_from TEXT NOT NULL, child_to TEXT NOT NULL, delta REAL NOT NULL, envelope REAL NOT NULL, at TEXT NOT NULL);"},
			]
		},
		{
			macro:  "BF_STORE_LATENT_ATLAS_LINKER_SOURCE_GENERATION"
			source: "cmd/BonfyreLatentAtlas/src/main.c"
			statements: [
				{kind: "table", name: "_linker_source_generation", sql: "CREATE TABLE IF NOT EXISTS _linker_source_generation(source TEXT PRIMARY KEY,input_digest TEXT NOT NULL,context_digest TEXT NOT NULL,relation_digest TEXT NOT NULL);"},
			]
		},
		{
			macro:  "BF_STORE_LATENT_ATLAS_RELATION"
			source: "cmd/BonfyreLatentAtlas/src/main.c"
			statements: [
				{kind: "other", name: "", sql: "BEGIN;"},
				{kind: "other", name: "", sql: "ALTER TABLE relation RENAME TO relation_before_happens_at;"},
				{kind: "table", name: "relation", sql: "CREATE TABLE relation(src TEXT NOT NULL, relation TEXT NOT NULL CHECK(relation IN ('same_subject_as','materializes','observed_as','performance_of', 'depicts','mentions','derived_from','fingerprints_to','projected_into','member_of','located_in','version_of','happens_at')), dst TEXT NOT NULL, evidence TEXT NOT NULL, PRIMARY KEY(src, relation, dst));"},
				{kind: "other", name: "", sql: "INSERT OR IGNORE INTO relation(src,relation,dst,evidence) SELECT src,relation,dst,evidence FROM relation_before_happens_at;"},
				{kind: "other", name: "", sql: "DROP TABLE relation_before_happens_at;"},
				{kind: "other", name: "", sql: "COMMIT;"},
			]
		},
	]
}
