PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL,updated_at REAL NOT NULL);
CREATE TABLE IF NOT EXISTS tokenizer_descriptors(id TEXT PRIMARY KEY,version TEXT NOT NULL,implementation TEXT NOT NULL,
 exact INTEGER NOT NULL,model_compatible INTEGER NOT NULL DEFAULT 0,metadata_json TEXT NOT NULL DEFAULT '{}',created_at REAL NOT NULL,updated_at REAL NOT NULL);
CREATE TABLE IF NOT EXISTS artifacts(id TEXT PRIMARY KEY,kind TEXT NOT NULL,source_path TEXT,source_digest TEXT NOT NULL,
 canonical_digest TEXT NOT NULL,canonical_path TEXT NOT NULL,byte_count INTEGER NOT NULL,classification TEXT NOT NULL DEFAULT 'internal',
 schema_name TEXT,provenance_json TEXT NOT NULL DEFAULT '{}',created_at REAL NOT NULL);
CREATE TABLE IF NOT EXISTS token_artifacts(id TEXT PRIMARY KEY,artifact_id TEXT NOT NULL REFERENCES artifacts(id),
 tokenizer_id TEXT NOT NULL REFERENCES tokenizer_descriptors(id),tokenizer_version TEXT NOT NULL,token_count INTEGER NOT NULL,
 chunk_count INTEGER NOT NULL,manifest_path TEXT NOT NULL,spans_path TEXT NOT NULL,canonical_path TEXT NOT NULL,
 manifest_digest TEXT NOT NULL,prefix_identity TEXT NOT NULL,created_at REAL NOT NULL,UNIQUE(artifact_id,tokenizer_id,tokenizer_version));
CREATE TABLE IF NOT EXISTS token_chunks(token_artifact_id TEXT NOT NULL REFERENCES token_artifacts(id) ON DELETE CASCADE,
 chunk_index INTEGER NOT NULL,byte_start INTEGER NOT NULL,byte_end INTEGER NOT NULL,token_start INTEGER NOT NULL,token_end INTEGER NOT NULL,
 digest TEXT NOT NULL,PRIMARY KEY(token_artifact_id,chunk_index));
CREATE TABLE IF NOT EXISTS flux_events(seq INTEGER PRIMARY KEY AUTOINCREMENT,id TEXT UNIQUE NOT NULL,stream TEXT NOT NULL,
 kind TEXT NOT NULL,subject_kind TEXT,subject_id TEXT,payload_json TEXT NOT NULL,previous_digest TEXT,digest TEXT NOT NULL,created_at REAL NOT NULL);
CREATE TABLE IF NOT EXISTS deltas(id TEXT PRIMARY KEY,kind TEXT NOT NULL,source_id TEXT,target_id TEXT,logical_ops_json TEXT NOT NULL,
 physical_alternatives_json TEXT NOT NULL DEFAULT '[]',invalidations_json TEXT NOT NULL DEFAULT '[]',manifest_path TEXT,digest TEXT NOT NULL,created_at REAL NOT NULL);
CREATE TABLE IF NOT EXISTS dependency_edges(source_kind TEXT NOT NULL,source_id TEXT NOT NULL,target_kind TEXT NOT NULL,
 target_id TEXT NOT NULL,edge_kind TEXT NOT NULL,metadata_json TEXT NOT NULL DEFAULT '{}',created_at REAL NOT NULL,
 PRIMARY KEY(source_kind,source_id,target_kind,target_id,edge_kind));
CREATE TABLE IF NOT EXISTS affected_closures(id TEXT PRIMARY KEY,root_kind TEXT NOT NULL,root_id TEXT NOT NULL,max_depth INTEGER NOT NULL,
 nodes_json TEXT NOT NULL,edges_json TEXT NOT NULL,digest TEXT NOT NULL,created_at REAL NOT NULL);
CREATE TABLE IF NOT EXISTS ir_objects(id TEXT PRIMARY KEY,ir_type TEXT NOT NULL,subject_kind TEXT NOT NULL,subject_id TEXT NOT NULL,
 schema_version TEXT NOT NULL,payload_json TEXT NOT NULL,dependencies_json TEXT NOT NULL DEFAULT '[]',digest TEXT NOT NULL,created_at REAL NOT NULL);
CREATE TABLE IF NOT EXISTS lambda_motifs(id TEXT PRIMARY KEY,name TEXT NOT NULL,structure_json TEXT NOT NULL,digest TEXT NOT NULL UNIQUE,
 instance_count INTEGER NOT NULL DEFAULT 0,reconstruction_checks INTEGER NOT NULL DEFAULT 0,created_at REAL NOT NULL,updated_at REAL NOT NULL);
CREATE TABLE IF NOT EXISTS provider_token_bids(id TEXT PRIMARY KEY,mission_id TEXT,provider_id TEXT NOT NULL,tokenizer_id TEXT NOT NULL,
 required_tokens INTEGER NOT NULL,reusable_tokens INTEGER NOT NULL,uncached_tokens INTEGER NOT NULL,expected_output_tokens INTEGER NOT NULL,
 latency_ms REAL,cost_usd REAL,privacy_class TEXT,quality_score REAL,proof_score REAL,compatible INTEGER NOT NULL,payload_json TEXT NOT NULL,created_at REAL NOT NULL);
CREATE TABLE IF NOT EXISTS graph_projections(id TEXT PRIMARY KEY,graph_kind TEXT NOT NULL,root_kind TEXT NOT NULL,root_id TEXT NOT NULL,
 nodes_json TEXT NOT NULL,edges_json TEXT NOT NULL,digest TEXT NOT NULL,created_at REAL NOT NULL);
CREATE TABLE IF NOT EXISTS runtime_images(id TEXT PRIMARY KEY,mission_id TEXT,active_bfp_root TEXT,graph_roots_json TEXT NOT NULL DEFAULT '{}',
 token_roots_json TEXT NOT NULL DEFAULT '{}',provider_routes_json TEXT NOT NULL DEFAULT '{}',session_pointers_json TEXT NOT NULL DEFAULT '[]',
 authority_state_json TEXT NOT NULL DEFAULT '{}',placement_json TEXT NOT NULL DEFAULT '{}',proof_head TEXT,payload_json TEXT NOT NULL,digest TEXT NOT NULL,created_at REAL NOT NULL);
CREATE TABLE IF NOT EXISTS bfp_successor_plans(id TEXT PRIMARY KEY,source_root TEXT,target_root TEXT,changed_sections_json TEXT NOT NULL,
 token_chunks_json TEXT NOT NULL,logical_delta_ids_json TEXT NOT NULL,physical_candidates_json TEXT NOT NULL,rollback_root TEXT,
 payload_json TEXT NOT NULL,digest TEXT NOT NULL,created_at REAL NOT NULL);
CREATE TABLE IF NOT EXISTS receipts(id TEXT PRIMARY KEY,kind TEXT NOT NULL,subject_kind TEXT,subject_id TEXT,state TEXT NOT NULL,
 payload_json TEXT NOT NULL,previous_digest TEXT,digest TEXT NOT NULL,path TEXT NOT NULL,created_at REAL NOT NULL);
CREATE INDEX IF NOT EXISTS recursive_flux_stream_seq ON flux_events(stream,seq);
CREATE INDEX IF NOT EXISTS recursive_dependency_source ON dependency_edges(source_kind,source_id,target_kind,target_id,edge_kind);
