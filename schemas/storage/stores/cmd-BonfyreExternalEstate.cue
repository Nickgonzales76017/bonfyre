package stores

// cmd/BonfyreExternalEstate: store DDL, byte-for-byte what the owner executes.
// Compiled to cmd/BonfyreExternalEstate/generated/store_schema.h by scripts/compile-store-schemas.sh.
store_schemas: "cmd/BonfyreExternalEstate": {
	fragments: [
		{
			macro:  "BF_STORE_EXTERNAL_ESTATE_OCCURRENCE"
			source: "cmd/BonfyreExternalEstate/src/main.c"
			statements: [
				{kind: "other", name: "", sql: "PRAGMA journal_mode=WAL;"},
				{kind: "table", name: "occurrence", sql: "CREATE TABLE IF NOT EXISTS occurrence( id TEXT PRIMARY KEY,observed_name TEXT NOT NULL,source_ref TEXT NOT NULL, primary_url TEXT NOT NULL,source_kind TEXT NOT NULL,section TEXT NOT NULL,raw_json TEXT NOT NULL);"},
				{kind: "table", name: "occurrence_temporal", sql: "CREATE TABLE IF NOT EXISTS occurrence_temporal( occurrence_id TEXT PRIMARY KEY,state TEXT NOT NULL CHECK(state IN ('bearing','not_evident')),basis TEXT NOT NULL) WITHOUT ROWID;"},
				{kind: "table", name: "occurrence_temporal_mode", sql: "CREATE TABLE IF NOT EXISTS occurrence_temporal_mode( occurrence_id TEXT NOT NULL,mode TEXT NOT NULL CHECK(mode IN ('feed','live_update','history','forecast','schedule','telemetry')), PRIMARY KEY(occurrence_id,mode)) WITHOUT ROWID;"},
				{kind: "index", name: "occurrence_temporal_mode_lookup", sql: "CREATE INDEX IF NOT EXISTS occurrence_temporal_mode_lookup ON occurrence_temporal_mode(mode,occurrence_id);"},
				{kind: "table", name: "occurrence_term", sql: "CREATE TABLE IF NOT EXISTS occurrence_term(occurrence_id TEXT NOT NULL,axis TEXT NOT NULL,term TEXT NOT NULL, PRIMARY KEY(occurrence_id,axis,term)) WITHOUT ROWID;"},
				{kind: "index", name: "occurrence_term_lookup", sql: "CREATE INDEX IF NOT EXISTS occurrence_term_lookup ON occurrence_term(axis,term,occurrence_id);"},
				{kind: "table", name: "address_bit", sql: "CREATE TABLE IF NOT EXISTS address_bit(axis TEXT NOT NULL,term TEXT NOT NULL,word INTEGER NOT NULL,bit INTEGER NOT NULL, PRIMARY KEY(axis,term)) WITHOUT ROWID;"},
				{kind: "table", name: "candidate_mask", sql: "CREATE TABLE IF NOT EXISTS candidate_mask(occurrence_id TEXT NOT NULL,word INTEGER NOT NULL,provides INTEGER NOT NULL, PRIMARY KEY(occurrence_id,word)) WITHOUT ROWID;"},
				{kind: "index", name: "candidate_mask_word", sql: "CREATE INDEX IF NOT EXISTS candidate_mask_word ON candidate_mask(word);"},
				{kind: "table", name: "candidate_advertisement", sql: "CREATE TABLE IF NOT EXISTS candidate_advertisement( identity TEXT PRIMARY KEY,candidate_kind TEXT NOT NULL,owner_ref TEXT NOT NULL, exact_verifier TEXT NOT NULL,lowerer_ref TEXT NOT NULL,validity_frontier TEXT NOT NULL DEFAULT '', canonical_json TEXT NOT NULL) WITHOUT ROWID;"},
				{kind: "table", name: "candidate_advertisement_term", sql: "CREATE TABLE IF NOT EXISTS candidate_advertisement_term( identity TEXT NOT NULL,axis TEXT NOT NULL,term TEXT NOT NULL, PRIMARY KEY(identity,axis,term), FOREIGN KEY(identity) REFERENCES candidate_advertisement(identity)) WITHOUT ROWID;"},
				{kind: "index", name: "candidate_advertisement_term_lookup", sql: "CREATE INDEX IF NOT EXISTS candidate_advertisement_term_lookup ON candidate_advertisement_term(axis,term,identity);"},
				{kind: "table", name: "candidate_advertisement_mask", sql: "CREATE TABLE IF NOT EXISTS candidate_advertisement_mask( identity TEXT NOT NULL,word INTEGER NOT NULL,provides INTEGER NOT NULL, PRIMARY KEY(identity,word), FOREIGN KEY(identity) REFERENCES candidate_advertisement(identity)) WITHOUT ROWID;"},
				{kind: "index", name: "candidate_advertisement_mask_word", sql: "CREATE INDEX IF NOT EXISTS candidate_advertisement_mask_word ON candidate_advertisement_mask(word);"},
				{kind: "table", name: "candidate_advertisement_dependency", sql: "CREATE TABLE IF NOT EXISTS candidate_advertisement_dependency( identity TEXT NOT NULL,dependency_ref TEXT NOT NULL, PRIMARY KEY(identity,dependency_ref), FOREIGN KEY(identity) REFERENCES candidate_advertisement(identity)) WITHOUT ROWID;"},
				{kind: "index", name: "candidate_advertisement_dependency_lookup", sql: "CREATE INDEX IF NOT EXISTS candidate_advertisement_dependency_lookup ON candidate_advertisement_dependency(dependency_ref,identity);"},
				{kind: "table", name: "candidate_lowering_queue", sql: "CREATE TABLE IF NOT EXISTS candidate_lowering_queue( delta_id TEXT NOT NULL,identity TEXT NOT NULL,candidate_kind TEXT NOT NULL, owner_ref TEXT NOT NULL,lowerer_ref TEXT NOT NULL,state TEXT NOT NULL DEFAULT 'queued', causal_writes_json TEXT NOT NULL DEFAULT '[]', queued_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')), PRIMARY KEY(delta_id,identity)) WITHOUT ROWID;"},
				{kind: "table", name: "address_delta_receipt", sql: "CREATE TABLE IF NOT EXISTS address_delta_receipt( delta_id TEXT PRIMARY KEY,source_path TEXT NOT NULL,processed_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')), affected_count INTEGER NOT NULL) WITHOUT ROWID;"},
				{kind: "table", name: "occurrence_experience", sql: "CREATE TABLE IF NOT EXISTS occurrence_experience(occurrence_id TEXT PRIMARY KEY,experience_json TEXT NOT NULL,stage_json TEXT NOT NULL DEFAULT '{}') WITHOUT ROWID;"},
				{kind: "table", name: "experience_chain", sql: "CREATE TABLE IF NOT EXISTS experience_chain(chain_id TEXT PRIMARY KEY,chain_json TEXT NOT NULL) WITHOUT ROWID;"},
				{kind: "table", name: "presentation_cell", sql: "CREATE TABLE IF NOT EXISTS presentation_cell(grammar TEXT NOT NULL,presentation_class TEXT NOT NULL,cell_json TEXT NOT NULL, PRIMARY KEY(grammar,presentation_class)) WITHOUT ROWID;"},
				{kind: "table", name: "presentation_demand", sql: "CREATE TABLE IF NOT EXISTS presentation_demand(grammar TEXT NOT NULL,presentation_class TEXT NOT NULL,axis TEXT NOT NULL,term TEXT NOT NULL, PRIMARY KEY(grammar,presentation_class,axis,term)) WITHOUT ROWID;"},
				{kind: "index", name: "presentation_demand_lookup", sql: "CREATE INDEX IF NOT EXISTS presentation_demand_lookup ON presentation_demand(grammar,presentation_class,axis,term);"},
			]
		},
		{
			macro:  "BF_STORE_EXTERNAL_ESTATE_TEMPORAL_SOURCE_TEXT"
			source: "cmd/BonfyreExternalEstate/src/main.c"
			statements: [
				{kind: "other", name: "", sql: "DELETE FROM occurrence_temporal_mode;"},
				{kind: "other", name: "", sql: "DELETE FROM occurrence_temporal;"},
				{kind: "other", name: "", sql: "DROP VIEW IF EXISTS temp.temporal_source_text;"},
				{kind: "view", name: "temporal_source_text", sql: "CREATE TEMP VIEW temporal_source_text AS SELECT id,source_kind,lower(observed_name||' '||section||' '||coalesce(json_extract(raw_json,'$.raw_source_text'),'')) AS text FROM occurrence;"},
				{kind: "other", name: "", sql: "INSERT INTO occurrence_temporal_mode SELECT id,'feed' FROM temporal_source_text WHERE source_kind='rss';"},
				{kind: "other", name: "", sql: "INSERT OR IGNORE INTO occurrence_temporal_mode SELECT id,'live_update' FROM temporal_source_text WHERE text LIKE '%real-time%' OR text LIKE '%realtime%' OR text LIKE '%real time%' OR text LIKE '%websocket%' OR text LIKE '%web socket%' OR text LIKE '%websub%' OR text LIKE '%mqtt%' OR text LIKE '%server-sent events%' OR text LIKE '%eventsource%' OR text LIKE '%event stream%' OR text LIKE '%event-stream%' OR text LIKE '%notification%' OR text LIKE '%alert%' OR text LIKE '%updated every%' OR text LIKE '%updates every%' OR text LIKE '%refresh every%';"},
				{kind: "other", name: "", sql: "INSERT OR IGNORE INTO occurrence_temporal_mode SELECT id,'history' FROM temporal_source_text WHERE text LIKE '%historical%' OR text LIKE '%time series%' OR text LIKE '%time-series%' OR text LIKE '%timeseries%';"},
				{kind: "other", name: "", sql: "INSERT OR IGNORE INTO occurrence_temporal_mode SELECT id,'forecast' FROM temporal_source_text WHERE text LIKE '%forecast%' OR text LIKE '%nowcast%';"},
				{kind: "other", name: "", sql: "INSERT OR IGNORE INTO occurrence_temporal_mode SELECT id,'schedule' FROM temporal_source_text WHERE text LIKE '%schedule%' OR text LIKE '%calendar%';"},
				{kind: "other", name: "", sql: "INSERT OR IGNORE INTO occurrence_temporal_mode SELECT id,'telemetry' FROM temporal_source_text WHERE text LIKE '%telemetry%';"},
				{kind: "other", name: "", sql: "INSERT INTO occurrence_temporal(occurrence_id,state,basis) SELECT id,CASE WHEN EXISTS(SELECT 1 FROM occurrence_temporal_mode m WHERE m.occurrence_id=temporal_source_text.id) THEN 'bearing' ELSE 'not_evident' END,CASE WHEN EXISTS(SELECT 1 FROM occurrence_temporal_mode m WHERE m.occurrence_id=temporal_source_text.id) THEN 'source_description_or_kind' ELSE 'listing_no_temporal_evidence' END FROM temporal_source_text;"},
				{kind: "other", name: "", sql: "DROP VIEW temporal_source_text;"},
			]
		},
		{
			macro:  "BF_STORE_EXTERNAL_ESTATE_ADDRESS_DELTA_CUT"
			source: "cmd/BonfyreExternalEstate/src/main.c"
			statements: [
				{kind: "table", name: "address_delta_cut", sql: "CREATE TEMP TABLE IF NOT EXISTS address_delta_cut(identity TEXT PRIMARY KEY,candidate_kind TEXT NOT NULL,owner_ref TEXT NOT NULL) WITHOUT ROWID;"},
				{kind: "other", name: "", sql: "DELETE FROM address_delta_cut;"},
			]
		},
		{
			macro:  "BF_STORE_EXTERNAL_ESTATE_P"
			source: "cmd/BonfyreExternalEstate/src/main.c"
			statements: [
				{kind: "table", name: "p", sql: "CREATE TABLE p(j TEXT)"},
			]
		},
	]
}
