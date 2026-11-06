PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS metadata(
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL,
  updated_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS receipts(
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  id TEXT UNIQUE NOT NULL,
  release TEXT NOT NULL,
  kind TEXT NOT NULL,
  subject_kind TEXT,
  subject_id TEXT,
  state TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  previous_digest TEXT,
  digest TEXT NOT NULL,
  path TEXT NOT NULL,
  created_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS flux_streams(
  name TEXT PRIMARY KEY,
  head_digest TEXT,
  event_count INTEGER NOT NULL DEFAULT 0,
  created_at REAL NOT NULL,
  updated_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS flux_events(
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  id TEXT UNIQUE NOT NULL,
  stream TEXT NOT NULL,
  kind TEXT NOT NULL,
  subject_kind TEXT NOT NULL,
  subject_id TEXT NOT NULL,
  operation TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  dependencies_json TEXT NOT NULL,
  previous_digest TEXT,
  digest TEXT NOT NULL,
  segment_path TEXT NOT NULL,
  created_at REAL NOT NULL
);

-- Ordered per-stream verification must not rescan every other stream.
CREATE INDEX IF NOT EXISTS flux_events_stream_seq ON flux_events(stream,seq);

CREATE TABLE IF NOT EXISTS object_state(
  subject_kind TEXT NOT NULL,
  subject_id TEXT NOT NULL,
  state_json TEXT NOT NULL,
  deleted INTEGER NOT NULL DEFAULT 0,
  last_event_id TEXT NOT NULL,
  last_digest TEXT NOT NULL,
  updated_at REAL NOT NULL,
  PRIMARY KEY(subject_kind, subject_id)
);

CREATE TABLE IF NOT EXISTS deltas(
  id TEXT PRIMARY KEY,
  source_digest TEXT,
  target_digest TEXT NOT NULL,
  subject_kind TEXT NOT NULL,
  subject_id TEXT NOT NULL,
  logical_ops_json TEXT NOT NULL,
  native_manifest_path TEXT,
  invalidations_json TEXT NOT NULL,
  digest TEXT NOT NULL,
  created_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS dependency_edges(
  source_kind TEXT NOT NULL,
  source_id TEXT NOT NULL,
  target_kind TEXT NOT NULL,
  target_id TEXT NOT NULL,
  edge_kind TEXT NOT NULL,
  metadata_json TEXT NOT NULL,
  created_at REAL NOT NULL,
  PRIMARY KEY(source_kind,source_id,target_kind,target_id,edge_kind)
);

CREATE TABLE IF NOT EXISTS closures(
  id TEXT PRIMARY KEY,
  root_kind TEXT NOT NULL,
  root_id TEXT NOT NULL,
  max_depth INTEGER NOT NULL,
  nodes_json TEXT NOT NULL,
  edges_json TEXT NOT NULL,
  digest TEXT NOT NULL,
  created_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS materialized_views(
  name TEXT PRIMARY KEY,
  schema_name TEXT NOT NULL,
  state_json TEXT NOT NULL,
  digest TEXT NOT NULL,
  backend TEXT NOT NULL,
  updated_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS parity_runs(
  id TEXT PRIMARY KEY,
  incremental_digest TEXT NOT NULL,
  rebuild_digest TEXT NOT NULL,
  equal INTEGER NOT NULL,
  details_json TEXT NOT NULL,
  proof_path TEXT NOT NULL,
  created_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS graph_partitions(
  id TEXT PRIMARY KEY,
  graph_kind TEXT NOT NULL,
  root_kind TEXT NOT NULL,
  root_id TEXT NOT NULL,
  nodes_json TEXT NOT NULL,
  edges_json TEXT NOT NULL,
  digest TEXT NOT NULL,
  path TEXT NOT NULL,
  created_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS rewrite_rules(
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  lhs TEXT NOT NULL,
  rhs TEXT NOT NULL,
  guard_json TEXT NOT NULL,
  enabled INTEGER NOT NULL,
  digest TEXT NOT NULL,
  created_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS saturation_runs(
  id TEXT PRIMARY KEY,
  input_term TEXT NOT NULL,
  selected_term TEXT NOT NULL,
  terms_json TEXT NOT NULL,
  iterations INTEGER NOT NULL,
  truncated INTEGER NOT NULL,
  rules_json TEXT NOT NULL,
  digest TEXT NOT NULL,
  created_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS helix_artifacts(
  id TEXT PRIMARY KEY,
  source_run_id TEXT,
  term TEXT NOT NULL,
  resource_bounds_json TEXT NOT NULL,
  path TEXT NOT NULL,
  digest TEXT NOT NULL,
  created_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS reductions(
  id TEXT PRIMARY KEY,
  helix_id TEXT NOT NULL,
  backend TEXT NOT NULL,
  state TEXT NOT NULL,
  witness_json TEXT NOT NULL,
  witness_path TEXT NOT NULL,
  digest TEXT NOT NULL,
  created_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS authority_decisions(
  id TEXT PRIMARY KEY,
  actor TEXT NOT NULL,
  effect_class TEXT NOT NULL,
  resource TEXT,
  decision TEXT NOT NULL,
  reason TEXT NOT NULL,
  proof_refs_json TEXT NOT NULL,
  requires_zig INTEGER NOT NULL,
  digest TEXT NOT NULL,
  created_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS crux_proofs(
  id TEXT PRIMARY KEY,
  claim TEXT NOT NULL,
  inputs_json TEXT NOT NULL,
  witnesses_json TEXT NOT NULL,
  authority_decision_id TEXT,
  classification TEXT NOT NULL,
  path TEXT NOT NULL,
  digest TEXT NOT NULL,
  created_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS hardware_census(
  id TEXT PRIMARY KEY,
  node_id TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  digest TEXT NOT NULL,
  created_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS model_inventory(
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  path TEXT NOT NULL,
  kind TEXT NOT NULL,
  version_text TEXT,
  capabilities_json TEXT NOT NULL,
  digest TEXT NOT NULL,
  discovered_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS phenotype_plans(
  id TEXT PRIMARY KEY,
  mission_id TEXT,
  model_family TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  digest TEXT NOT NULL,
  path TEXT NOT NULL,
  created_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS kernel_parity_runs(
  id TEXT PRIMARY KEY,
  backend TEXT NOT NULL,
  state TEXT NOT NULL,
  reference_json TEXT NOT NULL,
  candidate_json TEXT NOT NULL,
  tolerances_json TEXT NOT NULL,
  digest TEXT NOT NULL,
  path TEXT NOT NULL,
  created_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS placement_nodes(
  id TEXT PRIMARY KEY,
  kind TEXT NOT NULL,
  trust TEXT NOT NULL,
  authority_zone TEXT NOT NULL,
  capabilities_json TEXT NOT NULL,
  constraints_json TEXT NOT NULL,
  health TEXT NOT NULL,
  updated_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS placement_plans(
  id TEXT PRIMARY KEY,
  mission_id TEXT,
  phenotype_id TEXT,
  selected_node TEXT NOT NULL,
  alternatives_json TEXT NOT NULL,
  reason TEXT NOT NULL,
  distributed_ir_json TEXT NOT NULL,
  digest TEXT NOT NULL,
  path TEXT NOT NULL,
  created_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS fuse_plans(
  id TEXT PRIMARY KEY,
  mission_id TEXT,
  phenotype_id TEXT,
  placement_id TEXT,
  axon_json TEXT NOT NULL,
  kore_json TEXT NOT NULL,
  fuse_json TEXT NOT NULL,
  path TEXT NOT NULL,
  digest TEXT NOT NULL,
  created_at REAL NOT NULL
);
