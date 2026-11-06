PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS manifold_meta(k TEXT PRIMARY KEY,v TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS semantic_object(
 id TEXT PRIMARY KEY,kind TEXT NOT NULL,owner TEXT NOT NULL,
 source_coordinate TEXT,discovered_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS occurrence(
 id TEXT PRIMARY KEY,kind TEXT NOT NULL,subject_id TEXT,
 observed_at TEXT NOT NULL,valid_from TEXT,valid_to TEXT,
 evidence_ref TEXT,source_coordinate TEXT);
CREATE TABLE IF NOT EXISTS representation_instance(
 id TEXT PRIMARY KEY,subject_id TEXT,kind TEXT NOT NULL,
 authority TEXT NOT NULL DEFAULT 'derived',frontier TEXT,digest TEXT,
 schema_ref TEXT,created_at TEXT NOT NULL,reconstructable INTEGER NOT NULL DEFAULT 1,
 state TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS physical_realization(
 id TEXT PRIMARY KEY,representation_id TEXT NOT NULL,carrier TEXT NOT NULL,
 coordinate TEXT NOT NULL,bytes INTEGER,digest TEXT,temperature TEXT NOT NULL,
 available INTEGER NOT NULL DEFAULT 1,checked_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS transformation_occurrence(
 id TEXT PRIMARY KEY,transform TEXT NOT NULL,compiler TEXT NOT NULL,
 input_ref TEXT NOT NULL,output_ref TEXT NOT NULL,input_digest TEXT,
 output_digest TEXT,occurred_at TEXT NOT NULL,proof_state TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS capability_unlock(
 representation_kind TEXT NOT NULL,capability TEXT NOT NULL,
 PRIMARY KEY(representation_kind,capability));
CREATE TABLE IF NOT EXISTS source_binding(
 source_db TEXT NOT NULL,source_table TEXT NOT NULL,semantic_kind TEXT NOT NULL,
 representation_kind TEXT NOT NULL,primary_coordinate TEXT,
 PRIMARY KEY(source_db,source_table));
CREATE TABLE IF NOT EXISTS representation_target(
 semantic_kind TEXT NOT NULL,representation_kind TEXT NOT NULL,status TEXT NOT NULL,
 substrate_path TEXT,reason TEXT,PRIMARY KEY(semantic_kind,representation_kind));
CREATE TABLE IF NOT EXISTS frontier_proof(
 id TEXT PRIMARY KEY,semantic_kind TEXT NOT NULL,frontier TEXT NOT NULL,
 member_count INTEGER NOT NULL,digest TEXT NOT NULL,created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS transport_binding(
 semantic_kind TEXT NOT NULL,transport TEXT NOT NULL,coordinate TEXT NOT NULL,
 state TEXT NOT NULL,proof_digest TEXT,checked_at TEXT NOT NULL,
 PRIMARY KEY(semantic_kind,transport));
CREATE TABLE IF NOT EXISTS execution_binding(
 semantic_kind TEXT NOT NULL,engine TEXT NOT NULL,coordinate TEXT NOT NULL,
 state TEXT NOT NULL,proof_digest TEXT,checked_at TEXT NOT NULL,
 PRIMARY KEY(semantic_kind,engine));
CREATE TABLE IF NOT EXISTS executable_projection(
 id TEXT PRIMARY KEY,kind TEXT NOT NULL,coordinate TEXT NOT NULL,digest TEXT,
 state TEXT NOT NULL,created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS forge_event(
 id TEXT PRIMARY KEY,kind TEXT NOT NULL,subject TEXT NOT NULL,state TEXT NOT NULL,
 detail TEXT,started_at TEXT NOT NULL,finished_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS representation_edge(
 parent_representation_id TEXT NOT NULL,child_representation_id TEXT NOT NULL,
 transform TEXT NOT NULL,proof_state TEXT NOT NULL,created_at TEXT NOT NULL,
 PRIMARY KEY(parent_representation_id,child_representation_id,transform));
CREATE TABLE IF NOT EXISTS graph_projection(
 semantic_kind TEXT NOT NULL,source_column TEXT NOT NULL,target_column TEXT NOT NULL,
 coordinate TEXT NOT NULL,rows INTEGER,digest TEXT,checked_at TEXT NOT NULL,
 PRIMARY KEY(semantic_kind,source_column,target_column));
CREATE TABLE IF NOT EXISTS virtual_graph_projection_v2(
 semantic_kind TEXT NOT NULL,source_column TEXT NOT NULL,target_column TEXT NOT NULL,
 source_representation_id TEXT NOT NULL,source_coordinate TEXT NOT NULL,
 source_digest TEXT NOT NULL,frontier TEXT NOT NULL,sql_text TEXT NOT NULL,
 proof_digest TEXT NOT NULL,state TEXT NOT NULL,checked_at TEXT NOT NULL,
 PRIMARY KEY(semantic_kind,source_column,target_column));
CREATE TABLE IF NOT EXISTS eviction_receipt(
 id TEXT PRIMARY KEY,representation_id TEXT NOT NULL,kind TEXT NOT NULL,
 coordinate TEXT NOT NULL,digest TEXT,bytes INTEGER NOT NULL,
 reason TEXT NOT NULL,reconstruct_from TEXT,evicted_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS byte_economics(
 representation_kind TEXT PRIMARY KEY,byte_policy TEXT NOT NULL,
 preferred_temperature TEXT NOT NULL,canonical_body_kind TEXT,
 rationale TEXT NOT NULL,updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS carrier_binding(
 representation_id TEXT NOT NULL,carrier TEXT NOT NULL,logical_path TEXT NOT NULL,
 physical_root TEXT NOT NULL,state TEXT NOT NULL,capability_json TEXT NOT NULL,
 checked_at TEXT NOT NULL,PRIMARY KEY(representation_id,carrier));
CREATE TABLE IF NOT EXISTS plan_representation(
 semantic_kind TEXT NOT NULL,plan_kind TEXT NOT NULL,coordinate TEXT NOT NULL,
 digest TEXT NOT NULL,state TEXT NOT NULL,created_at TEXT NOT NULL,
 PRIMARY KEY(semantic_kind,plan_kind));
CREATE TABLE IF NOT EXISTS metadata_fabric(
 kind TEXT NOT NULL,coordinate TEXT NOT NULL,state TEXT NOT NULL,
 detail TEXT,checked_at TEXT NOT NULL,PRIMARY KEY(kind,coordinate));
CREATE TABLE IF NOT EXISTS fabric_event(
 id TEXT PRIMARY KEY,kind TEXT NOT NULL,state TEXT NOT NULL,
 detail TEXT,occurred_at TEXT NOT NULL);
